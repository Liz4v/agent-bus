package shim

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/protocol"
)

// MirrorConfig holds the loop's tunables. Zero values mean the defaults.
type MirrorConfig struct {
	SyncWait   time.Duration // long-poll length (default protocol.SyncWait)
	BackoffMin time.Duration // default 1 s
	BackoffMax time.Duration // default 15 s
}

func (c MirrorConfig) withDefaults() MirrorConfig {
	if c.SyncWait == 0 {
		c.SyncWait = protocol.SyncWait
	}
	if c.BackoffMin == 0 {
		c.BackoffMin = time.Second
	}
	if c.BackoffMax == 0 {
		c.BackoffMax = 15 * time.Second
	}
	return c
}

// Snapshot is a consistent copy of the mirror's state.
type Snapshot struct {
	Gen      int64 // loop generation that supplied Rows
	Running  bool
	Rows     []protocol.Message // unread rows, id > cursor, in ID order
	Channels []protocol.Channel
	Health   error
	Cursor   int64 // read cursor, taken under the same lock as Rows
	// AdminSubscribed is the broker-confirmed admin subscription of the session.
	AdminSubscribed bool
}

// Summary is the wake-socket view of the mirror (D7).
type Summary struct {
	Count        int      // unread rows of waking kinds
	From         []string // sender labels of those rows, de-duplicated
	LiveChannels int      // channels in state waiting, connected or peer_disconnected
	Health       error
	Cursor       int64 // read cursor; changes after a read
	// AdminSubscribed is the broker-confirmed admin subscription of the session.
	AdminSubscribed bool
}

type refreshWaiter struct{ done chan error }

// Mirror is the shim's only caller of POST /v1/sync (I2). It keeps a local
// copy of the session's inbox (rows above the cursor) and channel list.
type Mirror struct {
	sess *Session
	c    *client.Client
	cfg  MirrorConfig

	mu       sync.Mutex
	gen      int64
	running  bool
	stop     context.CancelFunc
	cursor   int64
	after    int64
	version  int64
	rows     []protocol.Message
	channels []protocol.Channel
	health   error
	adminSub bool

	waiters    []*refreshWaiter
	dropWaiter []*refreshWaiter   // waiters of a dropped loop, released once the notice is set
	kick       bool               // run an immediate sync (a refresh, commit or more=true)
	cancelPoll context.CancelFunc // non-nil only while the loop is in a cancellable wait
	changed    chan struct{}      // closed and replaced on every change
}

// NewMirror creates the mirror and installs its hooks on the session.
func NewMirror(sess *Session, cfg MirrorConfig) *Mirror {
	m := &Mirror{sess: sess, c: sess.Client(), cfg: cfg.withDefaults(), changed: make(chan struct{})}
	sess.SetCallbacks(m.start, m.drop, m.dropped)
	return m
}

// notifyLocked wakes everything waiting on the mirror. Callers hold m.mu.
func (m *Mirror) notifyLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

// Changed returns a channel that is closed at the next change.
func (m *Mirror) Changed() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.changed
}

// resetLocked empties the mirror and its cursors.
func (m *Mirror) resetLocked() {
	m.cursor, m.after, m.version = 0, 0, 0
	m.rows, m.channels, m.health = nil, nil, nil
	m.adminSub = false
}

// releaseWaitersLocked completes every waiting Refresh with nil.
func (m *Mirror) releaseWaitersLocked() {
	for _, w := range m.waiters {
		w.done <- nil
	}
	m.waiters = nil
}

// start is the session's onRegister hook: it begins a new loop generation.
func (m *Mirror) start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stop != nil {
		m.stop()
	}
	m.gen++
	m.running = true
	m.resetLocked()
	m.kick = false
	m.cancelPoll = nil
	ctx, cancel := context.WithCancel(context.Background())
	m.stop = cancel
	m.releaseWaitersLocked()
	m.notifyLocked()
	go m.loop(ctx, m.gen, m.sess.Token())
}

// haltLocked stops the loop and releases waiters. It does not wait for the
// loop goroutine, because the loop itself may be the caller.
func (m *Mirror) haltLocked() {
	if m.stop != nil {
		m.stop()
		m.stop = nil
	}
	m.gen++ // a stopped loop applies nothing, even an in-flight response
	m.running = false
	m.cancelPoll = nil
	m.kick = false
	m.releaseWaitersLocked()
}

// drop is the session's onDrop hook: stop, empty, and report what was lost.
func (m *Mirror) drop() DropReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	rep := DropReport{HadState: len(m.channels) > 0 || len(m.rows) > 0}
	for _, r := range m.rows {
		if protocol.IsWaking(r.Kind) {
			rep.UnreadCount++
			rep.UnreadLabels = append(rep.UnreadLabels, SenderOf(r))
		}
	}
	held := m.waiters
	m.waiters = nil
	m.haltLocked()
	m.dropWaiter = append(m.dropWaiter, held...)
	m.resetLocked()
	m.notifyLocked()
	return rep
}

// dropped is the session's onDropped hook: it releases the Refresh calls that
// were waiting when the session was dropped, now that the notice is set.
func (m *Mirror) dropped() {
	m.mu.Lock()
	ws := m.dropWaiter
	m.dropWaiter = nil
	m.mu.Unlock()
	for _, w := range ws {
		w.done <- nil
	}
}

// Stop halts the loop without touching the session (clean exit).
func (m *Mirror) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.haltLocked()
	m.notifyLocked()
}

// Snapshot returns a copy of the current state.
func (m *Mirror) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Snapshot{
		Gen:      m.gen,
		Running:  m.running,
		Rows:     append([]protocol.Message(nil), m.rows...),
		Channels: append([]protocol.Channel(nil), m.channels...),
		Health:   m.health,
		Cursor:   m.cursor,

		AdminSubscribed: m.adminSub,
	}
}

// Poke wakes everything waiting on Changed without changing any state. The
// shim calls it when it clears the local admin intent, so a wait that was
// armed only because of that intent re-evaluates (M12).
func (m *Mirror) Poke() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifyLocked()
}

// Unread returns the unread rows and the loop generation that supplied them.
func (m *Mirror) Unread() ([]protocol.Message, int64) {
	s := m.Snapshot()
	return s.Rows, s.Gen
}

// Summary computes the wake-socket summary.
func (m *Mirror) Summary() Summary {
	s := m.Snapshot()
	var sum Summary
	seen := map[string]bool{}
	for _, r := range s.Rows {
		if !protocol.IsWaking(r.Kind) {
			continue
		}
		sum.Count++
		if l := SenderOf(r); !seen[l] {
			seen[l] = true
			sum.From = append(sum.From, l)
		}
	}
	for _, c := range s.Channels {
		switch c.State {
		case protocol.StateWaiting, protocol.StateConnected, protocol.StatePeerDisconnected:
			sum.LiveChannels++
		}
	}
	sum.Health = s.Health
	sum.Cursor = s.Cursor
	sum.AdminSubscribed = s.AdminSubscribed
	return sum
}

// WaitChange blocks until pred(snapshot) is true or ctx is done. It checks
// pred first, then after every change.
func (m *Mirror) WaitChange(ctx context.Context, pred func(Snapshot) bool) (Snapshot, error) {
	for {
		ch := m.Changed()
		s := m.Snapshot()
		if pred(s) {
			return s, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return s, ctx.Err()
		}
	}
}

// Commit records that rows up to maxID were shown to the agent: it advances the
// cursor, drops those rows and nudges the loop so the ack goes out. It is a
// no-op if gen is not the current loop generation.
func (m *Mirror) Commit(gen, maxID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running || gen != m.gen {
		return
	}
	if maxID > m.cursor {
		m.cursor = maxID
	}
	keep := m.rows[:0:0]
	for _, r := range m.rows {
		if r.ID > m.cursor {
			keep = append(keep, r)
		}
	}
	m.rows = keep
	m.kickLocked()
	m.notifyLocked()
}

// kickLocked asks the loop for an immediate sync and interrupts a cancellable wait.
func (m *Mirror) kickLocked() {
	m.kick = true
	if m.cancelPoll != nil {
		m.cancelPoll()
	}
}

// Refresh makes the loop run one wait=0 sync and returns its error. It returns
// nil at once if there is no session, and nil if the loop stops (a drop from
// any source, or Close) before that sync completes.
func (m *Mirror) Refresh(ctx context.Context) error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}
	w := &refreshWaiter{done: make(chan error, 1)}
	m.waiters = append(m.waiters, w)
	m.kickLocked()
	m.mu.Unlock()
	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Mirror) loop(ctx context.Context, gen int64, token string) {
	backoff := m.cfg.BackoffMin
	for {
		if ctx.Err() != nil {
			return
		}
		m.mu.Lock()
		if m.gen != gen {
			m.mu.Unlock()
			return
		}
		waiters := m.waiters
		m.waiters = nil
		immediate := m.kick || len(waiters) > 0
		m.kick = false
		req := protocol.SyncRequest{Ack: m.cursor, After: m.after, Version: m.version}
		pollCtx, cancel := context.WithCancel(ctx)
		if immediate {
			req.Wait = 0
		} else {
			req.Wait = int((m.cfg.SyncWait + time.Second - 1) / time.Second)
			m.cancelPoll = cancel // a refresh or commit may interrupt this wait
		}
		m.mu.Unlock()

		resp, err := m.c.Sync(pollCtx, token, req)

		m.mu.Lock()
		m.cancelPoll = nil
		interrupted := pollCtx.Err() != nil // before our own cancel() below
		cancel()
		if ctx.Err() != nil || m.gen != gen {
			for _, w := range waiters {
				w.done <- nil
			}
			m.mu.Unlock()
			return
		}
		if err != nil && interrupted {
			// Interrupted by a refresh or commit: discard the response and
			// keep any waiters for the next attempt.
			m.waiters = append(waiters, m.waiters...)
			m.mu.Unlock()
			continue
		}
		if err != nil {
			if errors.Is(err, client.ErrUnauthorized) {
				m.mu.Unlock()
				m.sess.DropAfter401(ctx, token) // stops this loop, empties the mirror, sets the notice
				for _, w := range waiters {
					w.done <- nil // only now: the notice is visible to whoever wakes
				}
				return
			}
			m.health = err
			for _, w := range waiters {
				w.done <- err
			}
			m.notifyLocked()
			sleep := backoff
			if backoff *= 2; backoff > m.cfg.BackoffMax {
				backoff = m.cfg.BackoffMax
			}
			if m.kick || len(m.waiters) > 0 {
				m.mu.Unlock()
				continue
			}
			sctx, stopSleep := context.WithCancel(ctx)
			m.cancelPoll = stopSleep // a refresh or commit may cut the backoff short
			m.mu.Unlock()
			t := time.NewTimer(sleep)
			select {
			case <-t.C:
			case <-sctx.Done():
			}
			t.Stop()
			stopSleep()
			m.mu.Lock()
			m.cancelPoll = nil
			m.mu.Unlock()
			continue
		}
		backoff = m.cfg.BackoffMin
		m.applyLocked(resp)
		for _, w := range waiters {
			w.done <- nil
		}
		if resp.More {
			m.kick = true
		}
		m.notifyLocked()
		m.mu.Unlock()
	}
}

// applyLocked applies one response. It appends only rows above `after`.
func (m *Mirror) applyLocked(resp protocol.SyncResponse) {
	for _, r := range resp.Messages {
		if r.ID > m.after && r.ID > m.cursor {
			m.rows = append(m.rows, r)
			m.after = r.ID
		}
	}
	m.version = resp.Version
	m.channels = resp.Channels
	m.adminSub = resp.AdminSubscribed
	m.health = nil
}
