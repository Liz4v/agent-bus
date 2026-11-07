package broker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/secret"
)

// Errors for the account layer.
var (
	// ErrRegistrationRefused is the one refusal /v1/register returns (M7). It
	// never says why; the real reason goes to the log.
	ErrRegistrationRefused = newErr(http.StatusForbidden, protocol.CodeRegistrationRefused,
		"registration refused: the name may be taken or the broker may be full; try another name or ask the hostmaster")
	ErrBadCredential = newErr(http.StatusUnauthorized, protocol.CodeUnauthorized, "unknown credential")
	ErrForbidden     = newErr(http.StatusForbidden, protocol.CodeForbidden, "this action is for the hostmaster only")
	ErrNoSuchAccount = newErr(http.StatusNotFound, protocol.CodeNoSuchAccount, "no pending or active account with that name")
	ErrAccountLimit  = newErr(http.StatusForbidden, protocol.CodeAccountLimit, "the active account limit is reached; raise it or remove an account first")

	errAccountPending  = newErr(http.StatusForbidden, protocol.CodeAccountPending, "this account is waiting for the hostmaster's approval")
	errAccountRejected = newErr(http.StatusForbidden, protocol.CodeAccountRejected, "the hostmaster rejected this account")
	errAccountExpired  = newErr(http.StatusForbidden, protocol.CodeAccountExpired, "this registration expired before the hostmaster approved it")
	errAccountRemoved  = newErr(http.StatusForbidden, protocol.CodeAccountRemoved, "the hostmaster removed this account")
)

func errWrongState(msg string) *Error {
	return newErr(http.StatusConflict, protocol.CodeWrongState, msg)
}

// statusErr maps a non-active account status to its 403 error.
func statusErr(status string) *Error {
	switch status {
	case protocol.AccountPending:
		return errAccountPending
	case protocol.AccountRejected:
		return errAccountRejected
	case protocol.AccountExpired:
		return errAccountExpired
	default:
		return errAccountRemoved
	}
}

// RegisterResult is what Register returns on success. DeletedSessions lists
// the sessions deleted by a recovery claim, for the handler to forget.
type RegisterResult struct {
	Credential      string
	Name            string
	Status          string
	Hostmaster      bool
	DeletedSessions []int64
}

// loggableName returns attributes for logging a requested name without letting
// an unauthenticated caller put arbitrary text in the journal.
func loggableName(name string) []any {
	if protocol.ValidName(name) {
		return []any{"name", name}
	}
	return []any{"name_len", len(name)}
}

// Register creates an account. Without claimCode it creates a pending account
// (M7); with a live claim code it creates the hostmaster, replacing the old
// one in recover mode (M4, M6). Every refusal is ErrRegistrationRefused.
func (s *Store) Register(ctx context.Context, name, claimCode string) (RegisterResult, error) {
	var res RegisterResult
	if claimCode != "" {
		s.claims.mu.Lock()
		defer s.claims.mu.Unlock()
	}
	refuse := func(reason string) error {
		s.log.Info("registration refused", append([]any{"reason", reason}, loggableName(name)...)...)
		return ErrRegistrationRefused
	}
	if err := s.ExpirePending(ctx); err != nil {
		return RegisterResult{}, err
	}
	var replaced string
	claimMode := ""
	err := s.update(ctx, func(tx *Tx) error {
		res = RegisterResult{}
		replaced = ""
		claimMode = ""
		// 1. lazy expiry, so counts are exact.
		if err := tx.expirePending(); err != nil {
			return err
		}
		// 2. claim code or hostmaster present.
		has, err := tx.hasHostmaster()
		if err != nil {
			return err
		}
		if claimCode != "" {
			mode, ok := s.claims.checkLocked(claimCode)
			if !ok {
				return refuse("claim code wrong, used or not issued")
			}
			switch {
			case mode == protocol.ClaimModeClaim && has:
				return refuse("claim code for an already claimed broker")
			case mode == protocol.ClaimModeRecover && has:
				var id int64
				if err := tx.QueryRowContext(ctx, "SELECT id, name FROM accounts WHERE is_hostmaster = 1").Scan(&id, &replaced); err != nil {
					return err
				}
				if res.DeletedSessions, err = tx.deleteAccount(id, protocol.AccountRemoved); err != nil {
					return err
				}
			}
			claimMode = mode
		} else if !has {
			return refuse("broker is unclaimed and no claim code was given")
		}
		// 3. name pattern.
		if !protocol.ValidName(name) {
			return refuse("invalid name")
		}
		// 4. name taken, case-insensitively, by an active or pending account.
		key := strings.ToLower(name)
		var one int
		err = tx.QueryRowContext(ctx, "SELECT 1 FROM accounts WHERE name_key = ?", key).Scan(&one)
		if err == nil {
			return refuse("name taken")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		cred := secret.NewToken()
		if claimMode != "" {
			// A claim never adds to the active count beyond the one hostmaster.
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO accounts(name, name_key, cred_hash, status, is_hostmaster, created_at, decided_at) VALUES(?,?,?,'active',1,?,?)",
				name, key, secret.Hash(cred), tx.now, tx.now); err != nil {
				return err
			}
			res.Credential, res.Name, res.Status, res.Hostmaster = cred, name, protocol.AccountActive, true
			return nil
		}
		// 5. pending limit.
		var pending, active int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts WHERE status = 'pending'").Scan(&pending); err != nil {
			return err
		}
		if pending >= protocol.PendingLimit {
			return refuse("too many pending registrations")
		}
		// 6. active cap.
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts WHERE status = 'active'").Scan(&active); err != nil {
			return err
		}
		max, err := maxAccounts(ctx, tx)
		if err != nil {
			return err
		}
		if active >= max {
			return refuse("active account limit reached")
		}
		// 7. insert and notify subscribed hostmaster sessions.
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO accounts(name, name_key, cred_hash, status, is_hostmaster, created_at) VALUES(?,?,?,'pending',0,?)",
			name, key, secret.Hash(cred), tx.now); err != nil {
			return err
		}
		res.Credential, res.Name, res.Status = cred, name, protocol.AccountPending
		return tx.notifyAdmins(name)
	})
	if err != nil {
		return RegisterResult{}, err
	}
	if claimMode != "" {
		s.claims.consumeLocked()
		if replaced != "" {
			s.log.Info("hostmaster replaced by recovery", "action", "recover", "account", res.Name, "replaced", replaced)
		} else {
			s.log.Info("hostmaster claimed", "action", "claim", "account", res.Name)
		}
	} else {
		s.log.Info("registration pending", "action", "register", "account", res.Name)
	}
	return res, nil
}

// notifyAdmins enqueues a registration_pending event to every session that
// subscribed to admin notifications.
func (tx *Tx) notifyAdmins(name string) error {
	rows, err := tx.QueryContext(tx.ctx, "SELECT id FROM sessions WHERE admin_sub = 1 ORDER BY id")
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := tx.enqueue(id, protocol.AdminHandle, nil, nil, protocol.KindRegistrationPending, name); err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) hasHostmaster() (bool, error) {
	var one int
	err := tx.QueryRowContext(tx.ctx, "SELECT 1 FROM accounts WHERE is_hostmaster = 1").Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// deleteAccount removes an account row, deleting its sessions through the
// removal path (M8) and leaving a retired row keyed by its credential hash. It
// returns the IDs of the deleted sessions.
func (tx *Tx) deleteAccount(id int64, outcome string) ([]int64, error) {
	var name string
	var credHash []byte
	if err := tx.QueryRowContext(tx.ctx, "SELECT name, cred_hash FROM accounts WHERE id = ?", id).Scan(&name, &credHash); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(tx.ctx, "SELECT id FROM sessions WHERE account_id = ? ORDER BY id", id)
	if err != nil {
		return nil, err
	}
	var sessions []int64
	for rows.Next() {
		var sid int64
		if err := rows.Scan(&sid); err != nil {
			rows.Close()
			return nil, err
		}
		sessions = append(sessions, sid)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, sid := range sessions {
		if err := tx.deleteSession(sid, reasonRemoved); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(tx.ctx, "INSERT OR REPLACE INTO retired(cred_hash, name, outcome, at) VALUES(?,?,?,?)",
		credHash, name, outcome, tx.now); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(tx.ctx, "DELETE FROM accounts WHERE id = ?", id); err != nil {
		return nil, err
	}
	return sessions, nil
}

// expirePending retires pending accounts older than PendingExpiry.
func (tx *Tx) expirePending() error {
	cutoff := time.Unix(tx.now, 0).Add(-protocol.PendingExpiry).Unix()
	rows, err := tx.QueryContext(tx.ctx, "SELECT id FROM accounts WHERE status = 'pending' AND created_at <= ? ORDER BY id", cutoff)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := tx.deleteAccount(id, protocol.AccountExpired); err != nil {
			return err
		}
	}
	return nil
}

// pruneRetired deletes retired rows older than RetiredKeep.
func (tx *Tx) pruneRetired() error {
	cutoff := time.Unix(tx.now, 0).Add(-protocol.RetiredKeep).Unix()
	_, err := tx.ExecContext(tx.ctx, "DELETE FROM retired WHERE at <= ?", cutoff)
	return err
}

// ExpirePending is the sweeper entry point for pending expiry.
func (s *Store) ExpirePending(ctx context.Context) error {
	return s.update(ctx, func(tx *Tx) error { return tx.expirePending() })
}

// PruneRetired is the sweeper entry point for pruning retired rows.
func (s *Store) PruneRetired(ctx context.Context) error {
	return s.update(ctx, func(tx *Tx) error { return tx.pruneRetired() })
}

// AccountInfo is the state of the account behind a credential.
type AccountInfo struct {
	ID         int64
	Name       string
	Status     string
	Hostmaster bool
}

// AccountByCredential looks up a credential. Live accounts report active or
// pending (a pending one past its expiry reports expired even before the
// sweeper retires it); retired rows report their outcome until pruned. An
// unknown credential is ErrBadCredential.
func (s *Store) AccountByCredential(ctx context.Context, cred string) (AccountInfo, error) {
	h := secret.Hash(cred)
	var a AccountInfo
	var host int
	var created int64
	err := s.db.QueryRowContext(ctx,
		"SELECT id, name, status, is_hostmaster, created_at FROM accounts WHERE cred_hash = ?", h).
		Scan(&a.ID, &a.Name, &a.Status, &host, &created)
	switch {
	case err == nil:
		a.Hostmaster = host == 1
		if a.Status == protocol.AccountPending && !time.Unix(created, 0).After(s.now().Add(-protocol.PendingExpiry)) {
			a.Status = protocol.AccountExpired
		}
		return a, nil
	case !errors.Is(err, sql.ErrNoRows):
		return AccountInfo{}, err
	}
	var at int64
	err = s.db.QueryRowContext(ctx, "SELECT name, outcome, at FROM retired WHERE cred_hash = ?", h).Scan(&a.Name, &a.Status, &at)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !time.Unix(at, 0).After(s.now().Add(-protocol.RetiredKeep))) {
		return AccountInfo{}, ErrBadCredential
	}
	return a, err
}

// SessionAccount is the POST /v1/sessions auth path: it returns the account ID
// and name for an active credential, or the 403 error for its status.
func (s *Store) SessionAccount(ctx context.Context, cred string) (id int64, name string, err error) {
	a, err := s.AccountByCredential(ctx, cred)
	if err != nil {
		return 0, "", err
	}
	if a.Status != protocol.AccountActive {
		return 0, "", statusErr(a.Status)
	}
	return a.ID, a.Name, nil
}

// SessionIdentity returns the account name and hostmaster flag behind a
// session, for the admin gate.
func (s *Store) SessionIdentity(ctx context.Context, session int64) (name string, hostmaster bool, err error) {
	var host int
	err = s.db.QueryRowContext(ctx,
		"SELECT a.name, a.is_hostmaster FROM sessions s JOIN accounts a ON a.id = s.account_id WHERE s.id = ?", session).
		Scan(&name, &host)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, ErrUnauthorized
	}
	return name, host == 1, err
}

// ListAccounts returns the live accounts and the limits.
func (s *Store) ListAccounts(ctx context.Context) (protocol.ListAccountsResponse, error) {
	resp := protocol.ListAccountsResponse{Accounts: []protocol.AccountInfo{}, PendingLimit: protocol.PendingLimit}
	err := s.update(ctx, func(tx *Tx) error {
		resp.Accounts, resp.Active, resp.Pending = []protocol.AccountInfo{}, 0, 0
		if err := tx.expirePending(); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx,
			"SELECT name, status, is_hostmaster, created_at FROM accounts ORDER BY is_hostmaster DESC, created_at, id")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a protocol.AccountInfo
			var host int
			if err := rows.Scan(&a.Name, &a.Status, &host, &a.CreatedAt); err != nil {
				return err
			}
			a.Hostmaster = host == 1
			switch a.Status {
			case protocol.AccountActive:
				resp.Active++
			case protocol.AccountPending:
				resp.Pending++
				exp := time.Unix(a.CreatedAt, 0).Add(protocol.PendingExpiry).Unix()
				a.ExpiresAt = &exp
			}
			resp.Accounts = append(resp.Accounts, a)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		resp.MaxAccounts, err = maxAccounts(ctx, tx)
		return err
	})
	return resp, err
}

// accountByName finds a live account by name, case-insensitively.
func (tx *Tx) accountByName(name string) (id int64, canonical, status string, hostmaster bool, err error) {
	var host int
	err = tx.QueryRowContext(tx.ctx, "SELECT id, name, status, is_hostmaster FROM accounts WHERE name_key = ?",
		strings.ToLower(name)).Scan(&id, &canonical, &status, &host)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", "", false, ErrNoSuchAccount
	}
	return id, canonical, status, host == 1, err
}

// Approve activates one pending account, re-checking the active cap.
func (s *Store) Approve(ctx context.Context, name string) (string, error) {
	// Expiry commits in its own transaction, so it sticks even when the
	// request that triggered it is refused.
	if err := s.ExpirePending(ctx); err != nil {
		return "", err
	}
	var canonical string
	err := s.update(ctx, func(tx *Tx) error {
		if err := tx.expirePending(); err != nil {
			return err
		}
		id, n, status, _, err := tx.accountByName(name)
		if err != nil {
			return err
		}
		canonical = n
		if status != protocol.AccountPending {
			return errWrongState("account " + strconv.Quote(n) + " is not pending")
		}
		var active int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts WHERE status = 'active'").Scan(&active); err != nil {
			return err
		}
		max, err := maxAccounts(ctx, tx)
		if err != nil {
			return err
		}
		if active >= max {
			return ErrAccountLimit
		}
		_, err = tx.ExecContext(ctx, "UPDATE accounts SET status = 'active', decided_at = ? WHERE id = ?", tx.now, id)
		return err
	})
	return canonical, err
}

// Reject retires one pending account as rejected.
func (s *Store) Reject(ctx context.Context, name string) (string, error) {
	if err := s.ExpirePending(ctx); err != nil {
		return "", err
	}
	var canonical string
	err := s.update(ctx, func(tx *Tx) error {
		if err := tx.expirePending(); err != nil {
			return err
		}
		id, n, status, _, err := tx.accountByName(name)
		if err != nil {
			return err
		}
		canonical = n
		if status != protocol.AccountPending {
			return errWrongState("account " + strconv.Quote(n) + " is not pending")
		}
		_, err = tx.deleteAccount(id, protocol.AccountRejected)
		return err
	})
	return canonical, err
}

// Remove deletes one active, non-hostmaster account and all its sessions. It
// returns the IDs of the deleted sessions for the handler to forget (M8).
func (s *Store) Remove(ctx context.Context, name string) (canonical string, deleted []int64, err error) {
	err = s.update(ctx, func(tx *Tx) error {
		deleted = nil
		id, n, status, host, err := tx.accountByName(name)
		if err != nil {
			return err
		}
		canonical = n
		switch {
		case host:
			return errWrongState("the hostmaster cannot be removed; replacing it is a host-side operation")
		case status != protocol.AccountActive:
			return errWrongState("account " + strconv.Quote(n) + " is not active; use reject for a pending request")
		}
		deleted, err = tx.deleteAccount(id, protocol.AccountRemoved)
		return err
	})
	if err != nil {
		return "", nil, err
	}
	return canonical, deleted, nil
}

// SetMaxAccounts stores the active-account cap (1..MaxAccountsCeiling).
func (s *Store) SetMaxAccounts(ctx context.Context, n int) (active int, err error) {
	if n < 1 || n > protocol.MaxAccountsCeiling {
		return 0, newErr(http.StatusBadRequest, protocol.CodeBadRequest,
			fmt.Sprintf("max must be between 1 and %d", protocol.MaxAccountsCeiling))
	}
	err = s.update(ctx, func(tx *Tx) error {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO settings(key, value) VALUES('max_accounts', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
			strconv.Itoa(n)); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts WHERE status = 'active'").Scan(&active)
	})
	return active, err
}

// SetAdminSub subscribes or unsubscribes a hostmaster session to
// registration_pending events and returns the current pending count. It bumps
// the session's version so its mirror learns the new state at once (M11).
func (s *Store) SetAdminSub(ctx context.Context, session int64, on bool) (pending int, err error) {
	v := 0
	if on {
		v = 1
	}
	err = s.update(ctx, func(tx *Tx) error {
		if err := tx.expirePending(); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			"UPDATE sessions SET admin_sub = ? WHERE id = ? AND account_id IN (SELECT id FROM accounts WHERE is_hostmaster = 1)",
			v, session)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrForbidden
		}
		if err := tx.bump(session); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts WHERE status = 'pending'").Scan(&pending)
	})
	return pending, err
}
