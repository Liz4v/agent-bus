package hermeswake

import (
	"context"
	"log/slog"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// Default durations (Architect: sidecar state machine).
const (
	DefaultPoll            = 5 * time.Second
	DefaultStuck           = 10 * time.Minute
	DefaultMinSpacing      = 10 * time.Second
	DefaultNoChannelsSleep = 10 * time.Second
	DefaultBackoffMin      = time.Second
	DefaultBackoffMax      = 60 * time.Second
)

// Shim is the wake-socket client.
type Shim interface {
	Wait(ctx context.Context) (protocol.WaitReply, error)
	Status(ctx context.Context) (protocol.StatusReply, error)
}

// Notifier sends one wake notification; nil means HTTP 202.
type Notifier interface {
	Post(ctx context.Context, count int, from []string) error
}

// Clock is injectable time. Sleep returns ctx.Err() if ctx ends first.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// Config configures Run. Zero durations mean the defaults.
type Config struct {
	Shim     Shim
	Notifier Notifier
	Clock    Clock // nil means the real clock
	Log      *slog.Logger

	Poll            time.Duration // HOLD STATUS poll interval
	Stuck           time.Duration // HOLD re-fire when count stays above zero
	MinSpacing      time.Duration // minimum between two accepted POSTs
	NoChannelsSleep time.Duration
	BackoffMin      time.Duration
	BackoffMax      time.Duration
}

// SocketShim is a Shim over the wake socket at Path.
type SocketShim struct{ Path string }

func (s SocketShim) Wait(ctx context.Context) (protocol.WaitReply, error) {
	return Wait(ctx, s.Path)
}

func (s SocketShim) Status(ctx context.Context) (protocol.StatusReply, error) {
	return Status(ctx, s.Path)
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c Config) withDefaults() Config {
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	def := func(p *time.Duration, v time.Duration) {
		if *p == 0 {
			*p = v
		}
	}
	def(&c.Poll, DefaultPoll)
	def(&c.Stuck, DefaultStuck)
	def(&c.MinSpacing, DefaultMinSpacing)
	def(&c.NoChannelsSleep, DefaultNoChannelsSleep)
	def(&c.BackoffMin, DefaultBackoffMin)
	def(&c.BackoffMax, DefaultBackoffMax)
	return c
}

type state int

const (
	stIdle state = iota
	stFire
	stHold
)

type runner struct {
	Config
	cur      time.Duration // current backoff, 0 after a successful exchange
	lastPost time.Time     // last accepted POST
	c0       int64         // cursor remembered from the STATUS before that POST
	holdFrom time.Time
}

// Run drives IDLE/FIRE/HOLD until ctx is cancelled, then returns ctx.Err().
func Run(ctx context.Context, cfg Config) error {
	r := &runner{Config: cfg.withDefaults()}
	st := stIdle
	for ctx.Err() == nil {
		var err error
		switch st {
		case stIdle:
			st, err = r.idle(ctx)
		case stFire:
			st, err = r.fire(ctx)
		case stHold:
			st, err = r.hold(ctx)
		}
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r *runner) backoff(ctx context.Context) error {
	if r.cur == 0 {
		r.cur = r.BackoffMin
	} else if r.cur *= 2; r.cur > r.BackoffMax {
		r.cur = r.BackoffMax
	}
	return r.Clock.Sleep(ctx, r.cur)
}

func (r *runner) idle(ctx context.Context) (state, error) {
	rep, err := r.Shim.Wait(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return stIdle, ctx.Err()
		}
		r.Log.Warn("wake socket unavailable; backing off", "err", err)
		return stIdle, r.backoff(ctx)
	}
	r.cur = 0
	switch rep.Status {
	case protocol.WaitPending:
		return stFire, nil
	case protocol.WaitNoChannels:
		r.Log.Info("no channels to wait for")
		return stIdle, r.Clock.Sleep(ctx, r.NoChannelsSleep)
	case protocol.WaitRearm:
		return stIdle, nil
	}
	r.Log.Warn("unexpected WAIT status", "status", rep.Status)
	return stIdle, r.backoff(ctx)
}

func (r *runner) fire(ctx context.Context) (state, error) {
	if !r.lastPost.IsZero() {
		if wait := r.MinSpacing - r.Clock.Now().Sub(r.lastPost); wait > 0 {
			if err := r.Clock.Sleep(ctx, wait); err != nil {
				return stFire, err
			}
		}
	}
	st, err := r.Shim.Status(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return stFire, ctx.Err()
		}
		r.Log.Warn("wake socket status failed; backing off", "err", err)
		return stIdle, r.backoff(ctx)
	}
	if st.Count == 0 {
		r.cur = 0
		return stIdle, nil // read during the debounce or spacing wait
	}
	// The backoff is not reset here: a STATUS that precedes a failing POST is
	// part of the failed attempt, and resetting would keep retrying at 1 s.
	if err := r.Notifier.Post(ctx, st.Count, st.From); err != nil {
		if ctx.Err() != nil {
			return stFire, ctx.Err()
		}
		r.Log.Warn("wake POST failed; backing off", "err", err)
		return stFire, r.backoff(ctx)
	}
	r.cur = 0
	now := r.Clock.Now()
	r.lastPost, r.holdFrom, r.c0 = now, now, st.Cursor
	r.Log.Info("wake posted", "count", st.Count)
	return stHold, nil
}

func (r *runner) hold(ctx context.Context) (state, error) {
	if err := r.Clock.Sleep(ctx, r.Poll); err != nil {
		return stHold, err
	}
	st, err := r.Shim.Status(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return stHold, ctx.Err()
		}
		r.Log.Warn("wake socket status failed; backing off", "err", err)
		return stIdle, r.backoff(ctx)
	}
	r.cur = 0
	switch {
	case st.Count == 0:
		return stIdle, nil
	case st.Cursor != r.c0:
		r.Log.Info("new unread messages after a read; waking again")
		return stFire, nil
	case r.Clock.Now().Sub(r.holdFrom) >= r.Stuck:
		r.Log.Warn("unread messages not read; waking again")
		return stFire, nil
	}
	return stHold, nil
}
