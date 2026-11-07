package broker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/secret"
)

// newHandle returns a handle unique within the session. The transaction holds
// the write lock, so checking first and inserting after cannot race; a
// conflict is still retried.
func (tx *Tx) newHandle(session int64) (string, error) {
	for try := 0; ; try++ {
		n := 2
		if try >= 20 {
			n = 4
		}
		b := make([]byte, n)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}
		h := "ch-" + hex.EncodeToString(b)
		var one int
		err := tx.QueryRowContext(tx.ctx, "SELECT 1 FROM members WHERE session_id = ? AND handle = ?", session, h).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return h, nil
		}
		if err != nil {
			return "", err
		}
	}
}

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

// CreateChannel makes a new open channel with the caller as its only member.
func (s *Store) CreateChannel(ctx context.Context, session int64, label string) (channelID, handle string, err error) {
	if !protocol.ValidLabel(label) {
		return "", "", ErrBadLabel
	}
	channelID = secret.NewChannelID()
	err = s.update(ctx, func(tx *Tx) error {
		res, err := tx.ExecContext(ctx, "INSERT INTO channels(id_hash, created_at, state) VALUES(?,?,'open')",
			secret.Hash(channelID), tx.now)
		if err != nil {
			return err
		}
		cid, _ := res.LastInsertId()
		account, err := tx.sessionAccount(session)
		if err != nil {
			return err
		}
		handle, err = tx.newHandle(session)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO members(channel_id, session_id, label, handle, account) VALUES(?,?,?,?,?)",
			cid, session, label, handle, account); err != nil {
			return err
		}
		return tx.bump(session)
	})
	if err != nil {
		return "", "", err
	}
	return channelID, handle, nil
}

// JoinChannel adds the caller as the second member of an open channel.
func (s *Store) JoinChannel(ctx context.Context, session int64, channelID, label string) (handle, peerLabel, peerAccount string, err error) {
	err = s.update(ctx, func(tx *Tx) error {
		// 1. bad_label depends only on the request.
		if !protocol.ValidLabel(label) {
			return ErrBadLabel
		}
		// 2. the channel must exist and be open; every other case is the same generic error.
		var cid int64
		var state string
		err := tx.QueryRowContext(ctx, "SELECT id, state FROM channels WHERE id_hash = ?", secret.Hash(channelID)).Scan(&cid, &state)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && state != "open") {
			return ErrNoOpen
		}
		if err != nil {
			return err
		}
		var creator member
		if err := tx.QueryRowContext(ctx,
			"SELECT id, channel_id, session_id, label, handle, account FROM members WHERE channel_id = ?", cid).
			Scan(&creator.id, &creator.channelID, &creator.sessionID, &creator.label, &creator.handle, &creator.account); err != nil {
			return err
		}
		// 3. same session or same label.
		if creator.sessionID == session || creator.label == label {
			return ErrSameLabel
		}
		// 4. duplicate peer, counting only joined channels on both sides.
		for _, c := range []struct {
			sess int64
			peer string
		}{{session, creator.label}, {creator.sessionID, label}} {
			var one int
			err := tx.QueryRowContext(ctx, `
SELECT 1 FROM members m JOIN channels c ON c.id = m.channel_id
WHERE m.session_id = ? AND m.peer_label = ? AND c.state = 'joined' LIMIT 1`, c.sess, c.peer).Scan(&one)
			if err == nil {
				return ErrDuplicate
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		// 5. join.
		account, err := tx.sessionAccount(session)
		if err != nil {
			return err
		}
		handle, err = tx.newHandle(session)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO members(channel_id, session_id, label, handle, peer_label, account, peer_account) VALUES(?,?,?,?,?,?,?)",
			cid, session, label, handle, creator.label, account, creator.account); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE members SET peer_label = ?, peer_account = ? WHERE id = ?", label, account, creator.id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE channels SET state = 'joined' WHERE id = ?", cid); err != nil {
			return err
		}
		// Held rows get their inbox IDs only now (I1), in their original order.
		if _, err := tx.ExecContext(ctx, `
INSERT INTO inbox(session_id, handle, from_label, from_account, kind, text, at)
SELECT ?, ?, from_label, from_account, 'text', text, at FROM held WHERE channel_id = ? ORDER BY id`,
			session, handle, cid); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM held WHERE channel_id = ?", cid); err != nil {
			return err
		}
		if err := tx.bump(session); err != nil {
			return err
		}
		peerLabel, peerAccount = creator.label, creator.account
		return tx.enqueue(creator.sessionID, creator.handle, &label, &account, protocol.KindPeerJoined, "")
	})
	if err != nil {
		return "", "", "", err
	}
	return handle, peerLabel, peerAccount, nil
}

type resolved struct {
	member
	state string
}

// resolve finds the membership that `to` names, inside the caller's session
// (D4). `to` is a full label (contains '/'), a channel handle, or a bare peer
// name (the part of the label after the host).
func (tx *Tx) resolve(session int64, to string) (resolved, error) {
	const cols = `SELECT m.id, m.channel_id, m.session_id, m.label, m.handle, m.account, c.state, COALESCE(m.peer_label, '')
FROM members m JOIN channels c ON c.id = m.channel_id `
	const order = ` ORDER BY (c.state = 'joined') DESC, m.id DESC`
	scan := func(rows *sql.Rows) ([]resolved, []string, error) {
		defer rows.Close()
		var out []resolved
		var labels []string
		for rows.Next() {
			var r resolved
			var pl string
			if err := rows.Scan(&r.id, &r.channelID, &r.sessionID, &r.label, &r.handle, &r.account, &r.state, &pl); err != nil {
				return nil, nil, err
			}
			out = append(out, r)
			labels = append(labels, pl)
		}
		return out, labels, rows.Err()
	}
	var q string
	var args []any
	switch {
	case strings.Contains(to, "/"):
		q, args = cols+"WHERE m.session_id = ? AND m.peer_label = ?"+order, []any{session, to}
	default:
		// A handle wins over a bare name.
		rows, err := tx.QueryContext(tx.ctx, cols+"WHERE m.session_id = ? AND m.handle = ?", session, to)
		if err != nil {
			return resolved{}, err
		}
		got, _, err := scan(rows)
		if err != nil {
			return resolved{}, err
		}
		if len(got) > 0 {
			return got[0], nil
		}
		q, args = cols+"WHERE m.session_id = ? AND substr(m.peer_label, instr(m.peer_label, '/') + 1) = ?"+order, []any{session, to}
	}
	rows, err := tx.QueryContext(tx.ctx, q, args...)
	if err != nil {
		return resolved{}, err
	}
	got, labels, err := scan(rows)
	if err != nil {
		return resolved{}, err
	}
	if len(got) == 0 {
		return resolved{}, tx.unknownPeer(session, to)
	}
	for _, l := range labels {
		if l != labels[0] {
			return resolved{}, newErr(409, protocol.CodeUnknownPeer, "more than one peer is called "+quote(to)+" ("+quote(labels[0])+", "+quote(l)+"); use the full host/name label")
		}
	}
	return got[0], nil
}

func (tx *Tx) unknownPeer(session int64, to string) error {
	e := newErr(404, protocol.CodeUnknownPeer, "no channel with peer "+quote(to)+" in this session")
	chans, err := tx.channelList(session)
	if err != nil {
		return err
	}
	for _, c := range chans {
		kp := protocol.KnownPeer{Handle: c.Handle, State: c.State}
		if c.PeerLabel != nil {
			kp.Label = *c.PeerLabel
		}
		if c.PeerAccount != nil {
			kp.Account = *c.PeerAccount
		}
		e.KnownPeers = append(e.KnownPeers, kp)
	}
	return e
}

func quote(s string) string {
	if len(s) > 80 {
		s = s[:80] + "..."
	}
	return `"` + strings.ReplaceAll(s, `"`, `'`) + `"`
}

// Send delivers text to the peer named by `to`, according to the channel state.
func (s *Store) Send(ctx context.Context, session int64, to, text string) error {
	if text == "" {
		return ErrEmpty
	}
	if len(text) > protocol.MaxMessageBytes {
		return errTooLarge()
	}
	return s.update(ctx, func(tx *Tx) error {
		r, err := tx.resolve(session, to)
		if err != nil {
			return err
		}
		switch r.state {
		case "open":
			_, err := tx.ExecContext(ctx, "INSERT INTO held(channel_id, from_label, from_account, text, at) VALUES(?,?,?,?,?)",
				r.channelID, r.label, r.account, text, tx.now)
			return err
		case "joined":
			var other member
			if err := tx.QueryRowContext(ctx,
				"SELECT id, channel_id, session_id, label, handle, account FROM members WHERE channel_id = ? AND id <> ?",
				r.channelID, r.id).Scan(&other.id, &other.channelID, &other.sessionID, &other.label, &other.handle, &other.account); err != nil {
				return err
			}
			from, account := r.label, r.account
			return tx.enqueue(other.sessionID, other.handle, &from, &account, protocol.KindText, text)
		case "ended":
			return ErrPeerLeft
		case "removed":
			return ErrPeerRemoved
		default:
			return ErrExpired
		}
	})
}

// Leave ends the caller's membership in the channel named by `to`.
func (s *Store) Leave(ctx context.Context, session int64, to string) error {
	return s.update(ctx, func(tx *Tx) error {
		r, err := tx.resolve(session, to)
		if err != nil {
			return err
		}
		return tx.removeMember(r.member, reasonLeft)
	})
}
