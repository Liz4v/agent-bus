package broker

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// sessionAccount returns the account name that owns a session, or
// ErrUnauthorized if the session no longer exists.
func (tx *Tx) sessionAccount(session int64) (string, error) {
	var name string
	err := tx.QueryRowContext(tx.ctx,
		"SELECT a.name FROM sessions s JOIN accounts a ON a.id = s.account_id WHERE s.id = ?", session).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrUnauthorized
	}
	return name, err
}

// sessionName returns the name a session holds and its case-folded key. ok is
// false when the session holds no name. It returns ErrUnauthorized if the
// session no longer exists.
func (tx *Tx) sessionName(session int64) (name, key string, ok bool, err error) {
	var n, k sql.NullString
	err = tx.QueryRowContext(tx.ctx, "SELECT name, name_key FROM sessions WHERE id = ?", session).Scan(&n, &k)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, ErrUnauthorized
	}
	if err != nil {
		return "", "", false, err
	}
	return n.String, k.String, n.Valid, nil
}

func quote(s string) string {
	if len(s) > 80 {
		s = s[:80] + "..."
	}
	return `"` + strings.ReplaceAll(s, `"`, `'`) + `"`
}

// isUnique reports whether err is a UNIQUE constraint violation.
func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// ClaimName gives the session a broker-wide unique name (N8). It returns the
// name the session holds afterwards. Claiming the name already held (in any
// case) is a no-op that returns the held spelling.
func (s *Store) ClaimName(ctx context.Context, session int64, name string) (string, error) {
	var held, account string
	changed := false
	err := s.update(ctx, func(tx *Tx) error {
		// 1. invalid, 2. reserved: both depend only on the request.
		if !protocol.ValidName(name) {
			return ErrBadName
		}
		if protocol.ReservedName(name) {
			return ErrReservedName
		}
		key := protocol.NameKey(name)
		// 3. the session already holds a name.
		cur, curKey, ok, err := tx.sessionName(session)
		if err != nil {
			return err
		}
		if ok {
			if curKey == key {
				held = cur
				return nil
			}
			return errNameHeld(cur)
		}
		// 4. another session holds the key.
		var one int
		err = tx.QueryRowContext(ctx, "SELECT 1 FROM sessions WHERE name_key = ?", key).Scan(&one)
		if err == nil {
			return ErrNameTaken
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// 5. take it. The UNIQUE constraint is the backstop for a race.
		if account, err = tx.sessionAccount(session); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE sessions SET name = ?, name_key = ? WHERE id = ?", name, key, session); err != nil {
			if isUnique(err) {
				return ErrNameTaken
			}
			return err
		}
		// Whoever is told "bob left" no longer gets that answer: bob is held again.
		if _, err := tx.ExecContext(ctx, "DELETE FROM departed WHERE name_key = ?", key); err != nil {
			return err
		}
		held, changed = name, true
		return tx.bump(session)
	})
	if err != nil {
		return "", err
	}
	if changed {
		s.log.Info("name claimed", "session", session, "account", account, "name", held)
	}
	return held, nil
}

// ReleaseName frees the session's name (N4, N10). Partners are told, and the
// session's unread peer rows are discarded; admin rows stay. It returns the
// released name, or "" if the session held none (idempotent).
func (s *Store) ReleaseName(ctx context.Context, session int64) (string, error) {
	var released, account string
	err := s.update(ctx, func(tx *Tx) error {
		name, _, ok, err := tx.sessionName(session)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if account, err = tx.sessionAccount(session); err != nil {
			return err
		}
		if err := tx.depart(session, reasonLeft); err != nil {
			return err
		}
		if _, err := tx.ExecContext(tx.ctx, "UPDATE sessions SET name = NULL, name_key = NULL WHERE id = ?", session); err != nil {
			return err
		}
		if _, err := tx.ExecContext(tx.ctx, "DELETE FROM inbox WHERE session_id = ? AND kind <> ?",
			session, protocol.KindRegistrationPending); err != nil {
			return err
		}
		released = name
		return tx.bump(session)
	})
	if err != nil {
		return "", err
	}
	if released != "" {
		s.log.Info("name released", "session", session, "account", account, "name", released)
	}
	return released, nil
}

// Send delivers text to the session that holds the name `to` (N7).
func (s *Store) Send(ctx context.Context, session int64, to, text string) error {
	if text == "" {
		return ErrEmpty
	}
	if len(text) > protocol.MaxMessageBytes {
		return errTooLarge()
	}
	return s.update(ctx, func(tx *Tx) error {
		name, key, ok, err := tx.sessionName(session)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNoName
		}
		if !protocol.ValidName(to) {
			return ErrBadName
		}
		toKey := protocol.NameKey(to)
		if toKey == key {
			return ErrSelfSend
		}
		var rcpt int64
		err = tx.QueryRowContext(ctx, "SELECT id FROM sessions WHERE name_key = ?", toKey).Scan(&rcpt)
		if errors.Is(err, sql.ErrNoRows) {
			return tx.unknownPeer(session, toKey, to)
		}
		if err != nil {
			return err
		}
		account, err := tx.sessionAccount(session)
		if err != nil {
			return err
		}
		// The partner row comes first: whether it was new sets first_contact.
		lo, hi := session, rcpt
		if hi < lo {
			lo, hi = hi, lo
		}
		res, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO partners(lo, hi) VALUES(?,?)", lo, hi)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		from, fromAccount := name, account
		return tx.enqueue(rcpt, &from, &fromAccount, protocol.KindText, text, n == 1)
	})
}

// unknownPeer builds the error for a send to a name nobody holds. Only a
// former partner learns that the name was once held (N5); everyone else gets
// the same answer for every unheld name.
func (tx *Tx) unknownPeer(session int64, key, to string) error {
	var kind string
	err := tx.QueryRowContext(tx.ctx, "SELECT kind FROM departed WHERE session_id = ? AND name_key = ?", session, key).Scan(&kind)
	switch {
	case err == nil && kind == protocol.KindPeerRemoved:
		return ErrPeerRemoved
	case err == nil:
		return ErrPeerLeft
	case errors.Is(err, sql.ErrNoRows):
		return newErr(http.StatusNotFound, protocol.CodeUnknownPeer, "no session called "+quote(to))
	default:
		return err
	}
}

// depart tells every partner of the session that it stopped holding its name,
// records that in each partner's departed table, and forgets the partnerships
// (N4). It is the one code path for release, reap, clean exit and account
// removal, and does nothing for a session that holds no name or is already
// gone.
func (tx *Tx) depart(session int64, reason removeReason) error {
	name, key, ok, err := tx.sessionName(session)
	if errors.Is(err, ErrUnauthorized) || (err == nil && !ok) {
		return nil
	}
	if err != nil {
		return err
	}
	account, err := tx.sessionAccount(session)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(tx.ctx,
		"SELECT CASE WHEN lo = ? THEN hi ELSE lo END AS other FROM partners WHERE lo = ? OR hi = ? ORDER BY other",
		session, session, session)
	if err != nil {
		return err
	}
	var others []int64
	for rows.Next() {
		var o int64
		if err := rows.Scan(&o); err != nil {
			rows.Close()
			return err
		}
		others = append(others, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	kind := protocol.KindPeerLeft
	if reason == reasonRemoved {
		kind = protocol.KindPeerRemoved
	}
	for _, o := range others {
		from, fromAccount := name, account
		if err := tx.enqueue(o, &from, &fromAccount, kind, "", false); err != nil {
			return err
		}
		if _, err := tx.ExecContext(tx.ctx, "INSERT OR REPLACE INTO departed(session_id, name_key, kind) VALUES(?,?,?)",
			o, key, kind); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(tx.ctx, "DELETE FROM partners WHERE lo = ? OR hi = ?", session, session)
	return err
}
