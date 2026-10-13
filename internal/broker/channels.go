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
		handle, err = tx.newHandle(session)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO members(channel_id, session_id, label, handle) VALUES(?,?,?,?)",
			cid, session, label, handle); err != nil {
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
func (s *Store) JoinChannel(ctx context.Context, session int64, channelID, label string) (handle, peerLabel string, err error) {
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
			"SELECT id, channel_id, session_id, label, handle FROM members WHERE channel_id = ?", cid).
			Scan(&creator.id, &creator.channelID, &creator.sessionID, &creator.label, &creator.handle); err != nil {
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
		handle, err = tx.newHandle(session)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO members(channel_id, session_id, label, handle, peer_label) VALUES(?,?,?,?,?)",
			cid, session, label, handle, creator.label); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE members SET peer_label = ? WHERE id = ?", label, creator.id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE channels SET state = 'joined' WHERE id = ?", cid); err != nil {
			return err
		}
		// Held rows get their inbox IDs only now (I1), in their original order.
		if _, err := tx.ExecContext(ctx, `
INSERT INTO inbox(session_id, handle, from_label, kind, text, at)
SELECT ?, ?, from_label, 'text', text, at FROM held WHERE channel_id = ? ORDER BY id`,
			session, handle, cid); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM held WHERE channel_id = ?", cid); err != nil {
			return err
		}
		if err := tx.bump(session); err != nil {
			return err
		}
		peerLabel = creator.label
		return tx.enqueue(creator.sessionID, creator.handle, &label, protocol.KindPeerJoined, "")
	})
	if err != nil {
		return "", "", err
	}
	return handle, peerLabel, nil
}

type resolved struct {
	member
	state string
}

// resolve finds the membership that `to` (a handle, or a label if it contains
// '/') names, inside the caller's session (D4).
func (tx *Tx) resolve(session int64, to string) (resolved, error) {
	var r resolved
	var row *sql.Row
	if strings.Contains(to, "/") {
		row = tx.QueryRowContext(tx.ctx, `
SELECT m.id, m.channel_id, m.session_id, m.label, m.handle, c.state
FROM members m JOIN channels c ON c.id = m.channel_id
WHERE m.session_id = ? AND m.peer_label = ?
ORDER BY (c.state = 'joined') DESC, m.id DESC LIMIT 1`, session, to)
	} else {
		row = tx.QueryRowContext(tx.ctx, `
SELECT m.id, m.channel_id, m.session_id, m.label, m.handle, c.state
FROM members m JOIN channels c ON c.id = m.channel_id
WHERE m.session_id = ? AND m.handle = ?`, session, to)
	}
	err := row.Scan(&r.id, &r.channelID, &r.sessionID, &r.label, &r.handle, &r.state)
	if errors.Is(err, sql.ErrNoRows) {
		return r, tx.unknownPeer(session, to)
	}
	return r, err
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
			_, err := tx.ExecContext(ctx, "INSERT INTO held(channel_id, from_label, text, at) VALUES(?,?,?,?)",
				r.channelID, r.label, text, tx.now)
			return err
		case "joined":
			var other member
			if err := tx.QueryRowContext(ctx,
				"SELECT id, channel_id, session_id, label, handle FROM members WHERE channel_id = ? AND id <> ?",
				r.channelID, r.id).Scan(&other.id, &other.channelID, &other.sessionID, &other.label, &other.handle); err != nil {
				return err
			}
			from := r.label
			return tx.enqueue(other.sessionID, other.handle, &from, protocol.KindText, text)
		case "ended":
			return ErrPeerLeft
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
		return tx.removeMember(r.member)
	})
}
