package broker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/secret"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// Error is a broker error that maps directly onto an HTTP response.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newErr(status int, code, msg string) *Error {
	return &Error{Status: status, Code: code, Message: msg}
}

// Errors returned by the store.
var (
	ErrUnauthorized = newErr(http.StatusUnauthorized, protocol.CodeUnauthorized, "unknown or expired session")
	ErrBadName      = newErr(http.StatusBadRequest, protocol.CodeBadName, "a name is 1 to 64 characters: letters, digits, '.', '_' and '-' only")
	ErrReservedName = newErr(http.StatusBadRequest, protocol.CodeBadName, "that name is reserved (agent-bus and admin, in any case); choose another")
	ErrNameTaken    = newErr(http.StatusConflict, protocol.CodeNameTaken, "that name is already in use by another session")
	ErrNoName       = newErr(http.StatusConflict, protocol.CodeNoName,
		"you have not claimed a name: claim one with claim_name first. If this agent-bus has no claim_name tool, it is older than the broker and must be upgraded")
	ErrSelfSend    = newErr(http.StatusBadRequest, protocol.CodeSelfSend, "you cannot send a message to your own name")
	ErrPeerLeft    = newErr(http.StatusGone, protocol.CodePeerLeft, "that peer released its name or its session ended; sends to it fail. If someone claims the name later it is a different session")
	ErrPeerRemoved = newErr(http.StatusGone, protocol.CodePeerRemoved, "that peer's account was removed by the hostmaster; sends to it fail")
	ErrEmpty       = newErr(http.StatusBadRequest, protocol.CodeEmpty, "message text is empty")
)

// errNameHeld is the refusal to claim a second, different name.
func errNameHeld(name string) *Error {
	return newErr(http.StatusConflict, protocol.CodeNameHeld,
		"this session already holds the name "+quote(name)+"; release_name first to change it")
}

func errTooLarge() *Error {
	return newErr(http.StatusRequestEntityTooLarge, protocol.CodeTooLarge,
		fmt.Sprintf("message text exceeds the limit of %d bytes", protocol.MaxMessageBytes))
}

// Clock returns the current time. Tests inject a fake one.
type Clock func() time.Time

// Store owns the SQLite database and the notify hub (I3).
type Store struct {
	db     *sql.DB
	notify *Notify
	now    Clock
	claims *claims
	log    *slog.Logger
}

// SetLogger sets the logger used for registration refusals and similar
// store-level audit lines. nil restores the default logger.
func (s *Store) SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.Default()
	}
	s.log = l
}

// schemaV2 is migration 2 (multi-user M2): accounts own sessions. The old
// channel and session tables are dropped first; nothing survives the upgrade.
const schemaV2 = `
DROP TABLE IF EXISTS inbox;
DROP TABLE IF EXISTS held;
DROP TABLE IF EXISTS members;
DROP TABLE IF EXISTS channels;
DROP TABLE IF EXISTS sessions;
CREATE TABLE accounts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  name_key TEXT NOT NULL UNIQUE,
  cred_hash BLOB NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('pending','active')),
  is_hostmaster INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  decided_at INTEGER NULL);
CREATE UNIQUE INDEX accounts_one_hostmaster ON accounts(is_hostmaster) WHERE is_hostmaster = 1;
CREATE TABLE retired(
  cred_hash BLOB PRIMARY KEY,
  name TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('rejected','expired','removed')),
  at INTEGER NOT NULL);
CREATE TABLE settings(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL);
CREATE TABLE sessions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_hash BLOB UNIQUE NOT NULL,
  account_id INTEGER NOT NULL REFERENCES accounts ON DELETE RESTRICT,
  created_at INTEGER NOT NULL,
  disconnected_at INTEGER NULL,
  version INTEGER NOT NULL DEFAULT 0,
  admin_sub INTEGER NOT NULL DEFAULT 0);
CREATE INDEX sessions_account ON sessions(account_id);
CREATE TABLE channels(
  id INTEGER PRIMARY KEY,
  id_hash BLOB UNIQUE NOT NULL,
  created_at INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('open','joined','ended','expired','removed')));
CREATE TABLE members(
  id INTEGER PRIMARY KEY,
  channel_id INTEGER NOT NULL REFERENCES channels ON DELETE RESTRICT,
  session_id INTEGER NOT NULL REFERENCES sessions ON DELETE RESTRICT,
  label TEXT NOT NULL,
  handle TEXT NOT NULL,
  peer_label TEXT NULL,
  account TEXT NOT NULL,
  peer_account TEXT NULL,
  UNIQUE(session_id, handle),
  UNIQUE(channel_id, label));
CREATE TABLE held(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id INTEGER NOT NULL REFERENCES channels ON DELETE CASCADE,
  from_label TEXT NOT NULL,
  from_account TEXT NOT NULL,
  text TEXT NOT NULL,
  at INTEGER NOT NULL);
CREATE TABLE inbox(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL REFERENCES sessions ON DELETE CASCADE,
  handle TEXT NOT NULL,
  from_label TEXT NULL,
  from_account TEXT NULL,
  kind TEXT NOT NULL,
  text TEXT NOT NULL,
  at INTEGER NOT NULL);
CREATE INDEX inbox_session ON inbox(session_id, id);
CREATE INDEX members_session ON members(session_id);
CREATE INDEX held_channel ON held(channel_id);
`

// schemaV3 is migration 3 (name addressing): channels, members and held
// messages go away, a session carries its own name, and two small tables
// replace them. Accounts, retired rows and settings survive; every session and
// every unread message is discarded. The drop order matters, because foreign
// keys are enforced.
const schemaV3 = `
DROP TABLE IF EXISTS inbox;
DROP TABLE IF EXISTS held;
DROP TABLE IF EXISTS members;
DROP TABLE IF EXISTS channels;
DROP TABLE IF EXISTS sessions;
CREATE TABLE sessions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_hash BLOB UNIQUE NOT NULL,
  account_id INTEGER NOT NULL REFERENCES accounts ON DELETE RESTRICT,
  created_at INTEGER NOT NULL,
  disconnected_at INTEGER NULL,
  version INTEGER NOT NULL DEFAULT 0,
  admin_sub INTEGER NOT NULL DEFAULT 0,
  name TEXT NULL,
  name_key TEXT NULL UNIQUE);
CREATE INDEX sessions_account ON sessions(account_id);
CREATE TABLE partners(
  lo INTEGER NOT NULL REFERENCES sessions ON DELETE CASCADE,
  hi INTEGER NOT NULL REFERENCES sessions ON DELETE CASCADE,
  PRIMARY KEY(lo, hi),
  CHECK (lo < hi));
CREATE INDEX partners_hi ON partners(hi);
CREATE TABLE departed(
  session_id INTEGER NOT NULL REFERENCES sessions ON DELETE CASCADE,
  name_key TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('peer_left','peer_removed')),
  PRIMARY KEY(session_id, name_key));
CREATE INDEX departed_name_key ON departed(name_key);
CREATE TABLE inbox(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL REFERENCES sessions ON DELETE CASCADE,
  from_name TEXT NULL,
  from_account TEXT NULL,
  kind TEXT NOT NULL,
  text TEXT NOT NULL,
  at INTEGER NOT NULL,
  first_contact INTEGER NOT NULL DEFAULT 0);
CREATE INDEX inbox_session ON inbox(session_id, id);
`

// OpenStore opens (and migrates) the database at path. now may be nil.
func OpenStore(path string, notify *Notify, now Clock) (*Store, error) {
	if now == nil {
		now = time.Now
	}
	if notify == nil {
		notify = NewNotify()
	}
	q := url.Values{}
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Set("_txlock", "immediate")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("open database %s: %w", path, err)
	}
	db.SetMaxOpenConns(4)
	s := &Store{db: db, notify: notify, now: now, claims: &claims{}, log: slog.Default()}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Notify returns the hub that the store broadcasts to after commits.
func (s *Store) Notify() *Notify { return s.notify }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer tx.Rollback()
	var v int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if v >= 3 {
		return nil
	}
	if v < 2 {
		if _, err := tx.ExecContext(ctx, schemaV2); err != nil {
			return fmt.Errorf("migration 2: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, schemaV3); err != nil {
		return fmt.Errorf("migration 3: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 3"); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return tx.Commit()
}

// maxAccounts returns the active-account cap: the settings row, or the
// default when it is absent.
func maxAccounts(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}) (int, error) {
	var v string
	err := q.QueryRowContext(ctx, "SELECT value FROM settings WHERE key = 'max_accounts'").Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.DefaultMaxAccounts, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return protocol.DefaultMaxAccounts, nil
	}
	return n, nil
}

// Tx is a write transaction that records which sessions' versions it bumped.
type Tx struct {
	*sql.Tx
	ctx    context.Context
	now    int64
	bumped map[int64]struct{}
}

// update runs fn in a transaction. After a successful commit it broadcasts to
// every session in the bump set; after a rollback it does nothing (I3).
func (s *Store) update(ctx context.Context, fn func(tx *Tx) error) error {
	sqlTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	tx := &Tx{Tx: sqlTx, ctx: ctx, now: s.now().Unix(), bumped: map[int64]struct{}{}}
	if err := fn(tx); err != nil {
		sqlTx.Rollback()
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return err
	}
	if len(tx.bumped) > 0 {
		ids := make([]int64, 0, len(tx.bumped))
		for id := range tx.bumped {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		s.notify.Broadcast(ids...)
	}
	return nil
}

// bump increments a session's version and records it for broadcast.
func (tx *Tx) bump(session int64) error {
	if _, err := tx.ExecContext(tx.ctx, "UPDATE sessions SET version = version + 1 WHERE id = ?", session); err != nil {
		return err
	}
	tx.bumped[session] = struct{}{}
	return nil
}

// wakeOnly records a session for broadcast without bumping a version (used
// when the session itself is being deleted, so its long-polls wake and 401).
func (tx *Tx) wakeOnly(session int64) { tx.bumped[session] = struct{}{} }

// enqueue inserts a row into a session's inbox and bumps its version. from and
// fromAccount are the sender's name and verified account name (nil for rows
// with no sender). firstContact marks the first text row from a new sender.
func (tx *Tx) enqueue(session int64, from, fromAccount *string, kind, text string, firstContact bool) error {
	fc := 0
	if firstContact {
		fc = 1
	}
	if _, err := tx.ExecContext(tx.ctx,
		"INSERT INTO inbox(session_id, from_name, from_account, kind, text, at, first_contact) VALUES(?,?,?,?,?,?,?)",
		session, from, fromAccount, kind, text, tx.now, fc); err != nil {
		return err
	}
	return tx.bump(session)
}

// removeReason says why a session stops holding its name (M8).
type removeReason int

const (
	reasonLeft    removeReason = iota // the session released its name, ended, or was reaped
	reasonRemoved                     // the session's account was removed by the hostmaster
)

// CreateSession registers a session owned by an active account and returns its
// token and ID. The account is re-checked inside the transaction, so a
// session can never outlive the removal of its account (M1).
func (s *Store) CreateSession(ctx context.Context, accountID int64) (token string, id int64, err error) {
	token = secret.NewToken()
	err = s.update(ctx, func(tx *Tx) error {
		var status string
		err := tx.QueryRowContext(ctx, "SELECT status FROM accounts WHERE id = ?", accountID).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			return errAccountRemoved
		}
		if err != nil {
			return err
		}
		if status != protocol.AccountActive {
			return errAccountPending
		}
		res, err := tx.ExecContext(ctx, "INSERT INTO sessions(token_hash, account_id, created_at) VALUES(?,?,?)", secret.Hash(token), accountID, tx.now)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		return err
	})
	if err != nil {
		return "", 0, err
	}
	return token, id, nil
}

// SessionByToken looks up a session by the hash of its token.
func (s *Store) SessionByToken(ctx context.Context, token string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, "SELECT id FROM sessions WHERE token_hash = ?", secret.Hash(token)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrUnauthorized
	}
	return id, err
}

// DeleteSelf is the clean exit: tell the partners, then delete the session
// (its name, inbox and partner rows go with it).
func (s *Store) DeleteSelf(ctx context.Context, session int64) error {
	return s.update(ctx, func(tx *Tx) error {
		return tx.deleteSession(session, reasonLeft)
	})
}

func (tx *Tx) deleteSession(session int64, reason removeReason) error {
	if err := tx.depart(session, reason); err != nil {
		return err
	}
	if _, err := tx.ExecContext(tx.ctx, "DELETE FROM sessions WHERE id = ?", session); err != nil {
		return err
	}
	tx.wakeOnly(session)
	return nil
}

// Sync runs the sync transaction (D5). ack > 0 deletes the caller's inbox rows
// with id <= ack. It returns ErrUnauthorized if the session no longer exists.
func (s *Store) Sync(ctx context.Context, session int64, req protocol.SyncRequest) (protocol.SyncResponse, error) {
	var resp protocol.SyncResponse
	err := s.update(ctx, func(tx *Tx) error {
		if req.Ack > 0 {
			if _, err := tx.ExecContext(ctx, "DELETE FROM inbox WHERE session_id = ? AND id <= ?", session, req.Ack); err != nil {
				return err
			}
		}
		var adminSub int
		var name sql.NullString
		err := tx.QueryRowContext(ctx, "SELECT version, admin_sub, name FROM sessions WHERE id = ?", session).
			Scan(&resp.Version, &adminSub, &name)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrUnauthorized
		}
		if err != nil {
			return err
		}
		resp.AdminSubscribed = adminSub == 1
		resp.Name = name.String
		resp.Messages = []protocol.Message{}
		rows, err := tx.QueryContext(ctx,
			"SELECT id, from_name, from_account, kind, text, at, first_contact FROM inbox WHERE session_id = ? AND id > ? ORDER BY id",
			session, req.After)
		if err != nil {
			return err
		}
		size := 0
		for rows.Next() {
			var m protocol.Message
			var from, fromAccount sql.NullString
			var firstContact int
			if err := rows.Scan(&m.ID, &from, &fromAccount, &m.Kind, &m.Text, &m.At, &firstContact); err != nil {
				rows.Close()
				return err
			}
			rowSize := len(m.Text) + 256
			if len(resp.Messages) > 0 && size+rowSize > protocol.SyncPageBytes {
				resp.More = true
				break
			}
			m.FirstContact = firstContact == 1
			if from.Valid {
				f := from.String
				m.From = &f
			}
			if fromAccount.Valid {
				a := fromAccount.String
				m.FromAccount = &a
			}
			size += rowSize
			resp.Messages = append(resp.Messages, m)
		}
		err = rows.Err()
		rows.Close()
		return err
	})
	return resp, err
}
