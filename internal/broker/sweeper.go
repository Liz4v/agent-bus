package broker

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// Tracker holds each session's last_seen time in memory only (I7, D10).
type Tracker struct {
	mu        sync.Mutex
	now       Clock
	seen      map[int64]time.Time
	forgotten map[int64]struct{}
}

// NewTracker returns an empty tracker on the given clock.
func NewTracker(now Clock) *Tracker {
	if now == nil {
		now = time.Now
	}
	return &Tracker{now: now, seen: map[int64]time.Time{}, forgotten: map[int64]struct{}{}}
}

// Init sets last_seen to now for every session that is not already
// disconnected. It must run before the sweeper starts and before the listener
// accepts requests.
func (t *Tracker) Init(ctx context.Context, s *Store) error {
	rows, err := s.db.QueryContext(ctx, "SELECT id FROM sessions WHERE disconnected_at IS NULL")
	if err != nil {
		return err
	}
	defer rows.Close()
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		t.seen[id] = now
	}
	return rows.Err()
}

// Touch records activity for a session. It is ignored for a session that was
// forgotten (deleted), so the map holds only existing sessions.
func (t *Tracker) Touch(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, gone := t.forgotten[id]; gone {
		return
	}
	t.seen[id] = t.now()
}

// Forget removes a session's entry and remembers that it is gone. This is
// sound only because session IDs are never reused (AUTOINCREMENT).
func (t *Tracker) Forget(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.seen, id)
	t.forgotten[id] = struct{}{}
}

// Get returns the last time the session was seen. A missing entry is the zero
// time ("not seen since boot"), never now.
func (t *Tracker) Get(id int64) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen[id]
}

// size is used by tests.
func (t *Tracker) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.seen)
}

// Sweeper owns every time-driven transition (I7).
type Sweeper struct {
	store   *Store
	tracker *Tracker
	log     *slog.Logger
	// Interval is the time between sweeps (default protocol.SweepInterval).
	Interval time.Duration
}

// NewSweeper builds a sweeper. log may be nil.
func NewSweeper(store *Store, tracker *Tracker, log *slog.Logger) *Sweeper {
	if log == nil {
		log = slog.Default()
	}
	return &Sweeper{store: store, tracker: tracker, log: log, Interval: protocol.SweepInterval}
}

// Run sweeps every protocol.SweepInterval until ctx is done.
func (sw *Sweeper) Run(ctx context.Context) {
	tick := time.NewTicker(sw.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := sw.Sweep(ctx); err != nil && ctx.Err() == nil {
				sw.log.Error("sweep failed", "err", err)
			}
		}
	}
}

// Sweep runs one pass: per session reconnect, disconnect, reap (in that
// order), then the channel TTL expiry, pending-registration expiry and
// retired-row pruning.
func (sw *Sweeper) Sweep(ctx context.Context) error {
	type sess struct {
		id   int64
		disc sql.NullInt64
	}
	rows, err := sw.store.db.QueryContext(ctx, "SELECT id, disconnected_at FROM sessions ORDER BY id")
	if err != nil {
		return err
	}
	var all []sess
	for rows.Next() {
		var s sess
		if err := rows.Scan(&s.id, &s.disc); err != nil {
			rows.Close()
			return err
		}
		all = append(all, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, s := range all {
		if err := sw.sweepSession(ctx, s.id, s.disc); err != nil {
			return err
		}
	}
	if err := sw.expireChannels(ctx); err != nil {
		return err
	}
	if err := sw.store.ExpirePending(ctx); err != nil {
		return err
	}
	return sw.store.PruneRetired(ctx)
}

func (sw *Sweeper) sweepSession(ctx context.Context, id int64, disc sql.NullInt64) error {
	now := sw.store.now()
	last := sw.tracker.Get(id)

	// 1. reconnect
	if disc.Valid && last.After(time.Unix(disc.Int64, 0)) {
		ok, err := sw.store.setDisconnected(ctx, id, nil, protocol.KindPeerReconnected)
		if err != nil {
			return err
		}
		if ok {
			sw.log.Info("session reconnected", "session", id)
			disc = sql.NullInt64{}
		}
	}
	// 2. disconnect
	if !disc.Valid && last.Before(now.Add(-protocol.DisconnectAfter)) {
		ts := now.Unix()
		ok, err := sw.store.setDisconnected(ctx, id, &ts, protocol.KindPeerDisconnected)
		if err != nil {
			return err
		}
		if ok {
			sw.log.Info("session disconnected", "session", id)
			disc = sql.NullInt64{Int64: ts, Valid: true}
		}
	}
	// 3. reap, re-checking last_seen just before the transaction
	if disc.Valid && time.Unix(disc.Int64, 0).Before(now.Add(-protocol.DisconnectGrace)) {
		if sw.tracker.Get(id).After(time.Unix(disc.Int64, 0)) {
			return nil // a request arrived since: the next sweep reconnects it
		}
		err := sw.store.update(ctx, func(tx *Tx) error {
			var still sql.NullInt64
			err := tx.QueryRowContext(ctx, "SELECT disconnected_at FROM sessions WHERE id = ?", id).Scan(&still)
			if err == sql.ErrNoRows || (err == nil && !still.Valid) {
				return nil
			}
			if err != nil {
				return err
			}
			return tx.deleteSession(id, reasonLeft)
		})
		if err != nil {
			return err
		}
		sw.tracker.Forget(id)
		sw.log.Info("session reaped", "session", id)
	}
	return nil
}

// setDisconnected flips disconnected_at (to ts, or NULL when ts is nil) guarded
// by the column's current value, and emits kind to the peer of each joined
// channel in the same transaction. It reports whether it changed the row.
func (s *Store) setDisconnected(ctx context.Context, id int64, ts *int64, kind string) (bool, error) {
	changed := false
	err := s.update(ctx, func(tx *Tx) error {
		var q string
		var args []any
		if ts == nil {
			q, args = "UPDATE sessions SET disconnected_at = NULL WHERE id = ? AND disconnected_at IS NOT NULL", []any{id}
		} else {
			q, args = "UPDATE sessions SET disconnected_at = ? WHERE id = ? AND disconnected_at IS NULL", []any{*ts, id}
		}
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil
		}
		changed = true
		rows, err := tx.QueryContext(ctx, `
SELECT o.session_id, o.handle, m.label, m.account
FROM members m
JOIN channels c ON c.id = m.channel_id AND c.state = 'joined'
JOIN members o ON o.channel_id = m.channel_id AND o.id <> m.id
WHERE m.session_id = ? ORDER BY m.id`, id)
		if err != nil {
			return err
		}
		type target struct {
			session                int64
			handle, label, account string
		}
		var targets []target
		for rows.Next() {
			var t target
			if err := rows.Scan(&t.session, &t.handle, &t.label, &t.account); err != nil {
				rows.Close()
				return err
			}
			targets = append(targets, t)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, t := range targets {
			label, account := t.label, t.account
			if err := tx.enqueue(t.session, t.handle, &label, &account, kind, ""); err != nil {
				return err
			}
		}
		return nil
	})
	return changed, err
}

// expireChannels moves open channels older than the TTL to expired, deletes
// their held rows and tells the creator. The creator's membership stays.
func (sw *Sweeper) expireChannels(ctx context.Context) error {
	return sw.store.expireChannels(ctx)
}

func (s *Store) expireChannels(ctx context.Context) error {
	cutoff := s.now().Add(-protocol.ChannelTTL).Unix()
	rows, err := s.db.QueryContext(ctx, "SELECT id FROM channels WHERE state = 'open' AND created_at < ? ORDER BY id", cutoff)
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
	for _, cid := range ids {
		err := s.update(ctx, func(tx *Tx) error {
			res, err := tx.ExecContext(ctx, "UPDATE channels SET state = 'expired' WHERE id = ? AND state = 'open'", cid)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return nil
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM held WHERE channel_id = ?", cid); err != nil {
				return err
			}
			var sess int64
			var handle string
			if err := tx.QueryRowContext(ctx, "SELECT session_id, handle FROM members WHERE channel_id = ?", cid).Scan(&sess, &handle); err != nil {
				return err
			}
			return tx.enqueue(sess, handle, nil, nil, protocol.KindChannelExpired, "")
		})
		if err != nil {
			return err
		}
	}
	return nil
}
