package hermeswake

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// harness scripts the three dependencies of Run. Every call is recorded in
// events, in order. When a script runs dry the next call cancels the run, so
// each test ends deterministically.
type harness struct {
	mu     sync.Mutex
	now    time.Time
	events []string
	sleeps []time.Duration
	posts  []int // counts posted

	waits    []any // protocol.WaitReply or error
	statuses []any // protocol.StatusReply or error
	postErrs []error

	cancel context.CancelFunc
}

func (h *harness) log(s string) { h.events = append(h.events, s) }

func (h *harness) Now() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *harness) Sleep(ctx context.Context, d time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	h.now = h.now.Add(d)
	h.sleeps = append(h.sleeps, d)
	h.log("sleep:" + d.String())
	return nil
}

func (h *harness) exhausted(ctx context.Context) error {
	h.cancel()
	return ctx.Err()
}

func (h *harness) Wait(ctx context.Context) (protocol.WaitReply, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log("wait")
	if len(h.waits) == 0 {
		return protocol.WaitReply{}, h.exhausted(ctx)
	}
	v := h.waits[0]
	h.waits = h.waits[1:]
	if err, ok := v.(error); ok {
		return protocol.WaitReply{}, err
	}
	return v.(protocol.WaitReply), nil
}

func (h *harness) Status(ctx context.Context) (protocol.StatusReply, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log("status")
	if len(h.statuses) == 0 {
		return protocol.StatusReply{}, h.exhausted(ctx)
	}
	v := h.statuses[0]
	h.statuses = h.statuses[1:]
	if err, ok := v.(error); ok {
		return protocol.StatusReply{}, err
	}
	return v.(protocol.StatusReply), nil
}

func (h *harness) Post(ctx context.Context, count int, from []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log("post")
	h.posts = append(h.posts, count)
	if len(h.postErrs) == 0 {
		return nil
	}
	err := h.postErrs[0]
	h.postErrs = h.postErrs[1:]
	return err
}

func (h *harness) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx, cancel2 := context.WithCancel(ctx)
	h.cancel = cancel2
	h.now = time.Unix(1_000_000, 0)
	err := Run(ctx, Config{Shim: h, Notifier: h, Clock: h})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v; events: %v", err, h.events)
	}
}

func (h *harness) count(prefix string) int {
	n := 0
	for _, e := range h.events {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

var (
	pending = protocol.WaitReply{Status: protocol.WaitPending}
	rearm   = protocol.WaitReply{Status: protocol.WaitRearm}
	noChan  = protocol.WaitReply{Status: protocol.WaitNoChannels}
)

func st(count int, cursor int64) protocol.StatusReply {
	return protocol.StatusReply{Count: count, From: []string{"h/a"}, Cursor: cursor}
}

func TestOnePostPerWakeAndNoneWhileUnreadUnchanged(t *testing.T) {
	h := &harness{
		waits:    []any{pending},
		statuses: []any{st(2, 5), st(2, 5), st(2, 5), st(2, 5), st(0, 5)},
	}
	h.run(t)
	if len(h.posts) != 1 || h.posts[0] != 2 {
		t.Fatalf("posts %v; events %v", h.posts, h.events)
	}
	// FIRE status, three HOLD polls with unread, one with zero, then IDLE waits again
	if h.count("status") != 5 || h.count("wait") != 2 {
		t.Fatalf("events %v", h.events)
	}
	if h.sleeps[0] != DefaultPoll {
		t.Fatalf("sleeps %v", h.sleeps)
	}
}

func TestChangedCursorWithUnreadFiresAgain(t *testing.T) {
	h := &harness{
		waits:    []any{pending},
		statuses: []any{st(1, 5), st(1, 9), st(1, 9), st(0, 9)},
	}
	h.run(t)
	if len(h.posts) != 2 {
		t.Fatalf("posts %v; events %v", h.posts, h.events)
	}
	// the re-fire waited out the rest of the 10 s spacing before its STATUS
	want := []string{"wait", "status", "post", "sleep:5s", "status", "sleep:5s", "status", "post"}
	if !slices.Equal(h.events[:len(want)], want) {
		t.Fatalf("events %v", h.events)
	}
}

func TestStuckRefireAfterTenMinutes(t *testing.T) {
	statuses := []any{st(1, 5)}
	for range 10 { // 10 polls of one minute each: the tenth reaches 10 min
		statuses = append(statuses, st(1, 5))
	}
	statuses = append(statuses, st(1, 5), st(0, 5))
	h := &harness{waits: []any{pending}, statuses: statuses}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.now = time.Unix(1_000_000, 0)
	Run(ctx, Config{Shim: h, Notifier: h, Clock: h, Poll: time.Minute})
	if len(h.posts) != 2 {
		t.Fatalf("posts %v; events %v", h.posts, h.events)
	}
	if h.count("sleep:1m0s") < 10 {
		t.Fatalf("events %v", h.events)
	}
}

func TestMinimumSpacingBetweenPosts(t *testing.T) {
	// WAIT keeps answering pending: no hot loop, the 10 s spacing holds
	h := &harness{
		waits:    []any{pending, pending, pending},
		statuses: []any{st(1, 1), st(0, 1), st(1, 2), st(0, 2), st(1, 3), st(0, 3)},
	}
	start := time.Unix(1_000_000, 0)
	h.run(t)
	if len(h.posts) != 3 {
		t.Fatalf("posts %v; events %v", h.posts, h.events)
	}
	if elapsed := h.now.Sub(start); elapsed < 2*DefaultMinSpacing {
		t.Fatalf("three posts within %v", elapsed)
	}
	// spacing is waited before the STATUS call
	i := slices.Index(h.events, "sleep:5s")
	if i < 0 || h.events[i+1] != "status" {
		t.Fatalf("events %v", h.events)
	}
}

func TestPostFailureAndIgnoredRetryWithBackoffThenRecover(t *testing.T) {
	h := &harness{
		waits:    []any{pending},
		statuses: []any{st(1, 1), st(1, 1), st(1, 1), st(0, 1)},
		postErrs: []error{errors.New("webhook request failed"), &StatusError{Code: 200, Word: "ignored"}, nil},
	}
	h.run(t)
	if len(h.posts) != 3 {
		t.Fatalf("posts %v; events %v", h.posts, h.events)
	}
	want := []string{"wait", "status", "post", "sleep:1s", "status", "post", "sleep:2s", "status", "post", "sleep:5s"}
	// the backoff grows across failed POSTs; only the accepted POST resets it
	if !slices.Equal(h.events[:len(want)], want) {
		t.Fatalf("events %v", h.events) // no HOLD poll before the third post
	}
}

func TestCountZeroAtFireGoesIdleWithoutPost(t *testing.T) {
	h := &harness{waits: []any{pending}, statuses: []any{st(0, 3)}}
	h.run(t)
	if len(h.posts) != 0 || h.count("wait") != 2 {
		t.Fatalf("posts %v events %v", h.posts, h.events)
	}
}

func TestBackoffGrowsCapsAndResets(t *testing.T) {
	errs := []any{}
	for range 8 {
		errs = append(errs, errors.New("refused"))
	}
	h := &harness{waits: append(errs, rearm, errors.New("refused"))}
	h.run(t)
	want := []time.Duration{1, 2, 4, 8, 16, 32, 60, 60, 1}
	for i, w := range want {
		if i < 8 && h.sleeps[i] != w*time.Second {
			t.Fatalf("sleeps %v", h.sleeps)
		}
	}
	if h.sleeps[8] != time.Second {
		t.Fatalf("backoff did not reset after a good exchange: %v", h.sleeps)
	}
}

func TestNoChannelsSleepsAndRearmRewaits(t *testing.T) {
	h := &harness{waits: []any{noChan, rearm, rearm}}
	h.run(t)
	want := []string{"wait", "sleep:10s", "wait", "wait", "wait"}
	if !slices.Equal(h.events, want) {
		t.Fatalf("events %v", h.events)
	}
}

func TestUnexpectedWaitStatusBacksOff(t *testing.T) {
	h := &harness{waits: []any{protocol.WaitReply{Status: "weird"}}}
	h.run(t)
	if len(h.sleeps) != 1 || h.sleeps[0] != time.Second {
		t.Fatalf("sleeps %v", h.sleeps)
	}
}

func TestStatusErrorAtFireBacksOffToIdle(t *testing.T) {
	h := &harness{waits: []any{pending}, statuses: []any{errors.New("gone")}}
	h.run(t)
	want := []string{"wait", "status", "sleep:1s", "wait"}
	if !slices.Equal(h.events, want) {
		t.Fatalf("events %v", h.events)
	}
}

func TestStatusErrorInHoldBacksOffToIdle(t *testing.T) {
	h := &harness{waits: []any{pending}, statuses: []any{st(1, 1), errors.New("gone")}}
	h.run(t)
	want := []string{"wait", "status", "post", "sleep:5s", "status", "sleep:1s", "wait"}
	if !slices.Equal(h.events, want) {
		t.Fatalf("events %v", h.events)
	}
}

// blocker blocks every call until ctx ends.
type blocker struct{ inWait, inStatus, inPost bool }

func (b blocker) Wait(ctx context.Context) (protocol.WaitReply, error) {
	if b.inWait {
		<-ctx.Done()
		return protocol.WaitReply{}, ctx.Err()
	}
	return pending, nil
}

func (b blocker) Status(ctx context.Context) (protocol.StatusReply, error) {
	if b.inStatus {
		<-ctx.Done()
		return protocol.StatusReply{}, ctx.Err()
	}
	return st(1, 1), nil
}

func (b blocker) Post(ctx context.Context, _ int, _ []string) error {
	if b.inPost {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func TestContextCancelExitsPromptlyInEveryState(t *testing.T) {
	hour := time.Hour
	cases := map[string]struct {
		b   blocker
		cfg Config
	}{
		"idle wait":         {blocker{inWait: true}, Config{}},
		"fire status":       {blocker{inStatus: true}, Config{}},
		"fire post":         {blocker{inPost: true}, Config{}},
		"hold sleep":        {blocker{}, Config{Poll: hour}},
		"spacing sleep":     {blocker{}, Config{Poll: time.Millisecond, MinSpacing: hour}},
		"backoff sleep":     {blocker{inWait: false}, Config{}},
		"no_channels sleep": {blocker{}, Config{NoChannelsSleep: hour}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var s Shim = c.b
			var n Notifier = c.b
			cfg := c.cfg
			switch name {
			case "backoff sleep":
				s = errShim{}
				cfg.BackoffMin, cfg.BackoffMax = hour, hour
			case "no_channels sleep":
				s = noChanShim{}
			}
			cfg.Shim, cfg.Notifier = s, n
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Run(ctx, cfg) }()
			time.Sleep(100 * time.Millisecond)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Run did not stop after cancel")
			}
		})
	}
}

type errShim struct{}

func (errShim) Wait(context.Context) (protocol.WaitReply, error) {
	return protocol.WaitReply{}, errors.New("refused")
}

func (errShim) Status(context.Context) (protocol.StatusReply, error) {
	return protocol.StatusReply{}, errors.New("refused")
}

type noChanShim struct{ errShim }

func (noChanShim) Wait(context.Context) (protocol.WaitReply, error) { return noChan, nil }

func TestSocketShimAndRealClock(t *testing.T) {
	path := fakeShim(t, func(cmd string, c net.Conn) {
		if cmd == protocol.WakeCmdWait {
			reply(c, pending)
			return
		}
		reply(c, st(4, 7))
	})
	s := SocketShim{Path: path}
	if r, err := s.Wait(context.Background()); err != nil || r.Status != protocol.WaitPending {
		t.Fatalf("wait: %+v %v", r, err)
	}
	if r, err := s.Status(context.Background()); err != nil || r.Count != 4 || r.Cursor != 7 {
		t.Fatalf("status: %+v %v", r, err)
	}
	var c Clock = realClock{}
	if err := c.Sleep(context.Background(), time.Millisecond); err != nil || c.Now().IsZero() {
		t.Fatalf("real clock: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestDefaultsAreTheDocumentedValues(t *testing.T) {
	c := Config{}.withDefaults()
	got := fmt.Sprint(c.Poll, c.Stuck, c.MinSpacing, c.NoChannelsSleep, c.BackoffMin, c.BackoffMax)
	if got != "5s 10m0s 10s 10s 1s 1m0s" {
		t.Fatalf("defaults: %s", got)
	}
}
