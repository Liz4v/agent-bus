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
// order), then pending-registration expiry and retired-row pruning.
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
// by the column's current value, and emits kind to every partner of the session
// in the same transaction. It reports whether it changed the row. A session
// that holds no name has no partners, so nobody is told.
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
		var name sql.NullString
		var account string
		err = tx.QueryRowContext(ctx, `
SELECT s.name, a.name FROM sessions s JOIN accounts a ON a.id = s.account_id WHERE s.id = ?`, id).Scan(&name, &account)
		if err == sql.ErrNoRows || (err == nil && !name.Valid) {
			return nil
		}
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx,
			"SELECT CASE WHEN lo = ? THEN hi ELSE lo END AS other FROM partners WHERE lo = ? OR hi = ? ORDER BY other", id, id, id)
		if err != nil {
			return err
		}
		var targets []int64
		for rows.Next() {
			var o int64
			if err := rows.Scan(&o); err != nil {
				rows.Close()
				return err
			}
			targets = append(targets, o)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, o := range targets {
			from, fromAccount := name.String, account
			if err := tx.enqueue(o, &from, &fromAccount, kind, "", false); err != nil {
				return err
			}
		}
		return nil
	})
	return changed, err
}
