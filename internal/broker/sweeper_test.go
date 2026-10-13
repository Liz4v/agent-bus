package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type sweepEnv struct {
	t   *testing.T
	s   *Store
	clk *fakeClock
	tr  *Tracker
	sw  *Sweeper
}

func newSweepEnv(t *testing.T) *sweepEnv {
	s, clk := openTestStore(t)
	tr := NewTracker(clk.Now)
	return &sweepEnv{t: t, s: s, clk: clk, tr: tr, sw: NewSweeper(s, tr, quiet)}
}

// session registers a session and touches it, like POST /sessions.
func (e *sweepEnv) session() int64 {
	id := newSession(e.t, e.s)
	e.tr.Touch(id)
	return id
}

func (e *sweepEnv) sweep() {
	e.t.Helper()
	if err := e.sw.Sweep(bg); err != nil {
		e.t.Fatal(err)
	}
}

// pair returns two joined sessions a ("h/a") and b ("h/b") and a's handle for the channel.
func (e *sweepEnv) pair() (a, b int64, ha string) {
	a, b = e.session(), e.session()
	id, ha := mustCreate(e.t, e.s, a, "h/a")
	mustJoin(e.t, e.s, b, id, "h/b")
	return
}

func (e *sweepEnv) kinds(sess int64) []string {
	return kinds(sync0(e.t, e.s, sess, protocol.SyncRequest{}))
}

func (e *sweepEnv) count(sess int64, kind string) int {
	n := 0
	for _, k := range e.kinds(sess) {
		if k == kind {
			n++
		}
	}
	return n
}

func (e *sweepEnv) state(sess int64) string {
	ch := sync0(e.t, e.s, sess, protocol.SyncRequest{}).Channels
	if len(ch) != 1 {
		e.t.Fatalf("want 1 channel, got %+v", ch)
	}
	return ch[0].State
}

// disconnectA advances time so that only b stays seen, and sweeps once.
func (e *sweepEnv) disconnectA(b int64) {
	e.clk.Advance(protocol.DisconnectAfter + time.Second)
	e.tr.Touch(b)
	e.sweep()
}

func TestDisconnectEmittedExactlyOnce(t *testing.T) {
	e := newSweepEnv(t)
	_, b, _ := e.pair()
	e.disconnectA(b)
	for i := 0; i < 3; i++ {
		e.clk.Advance(15 * time.Second)
		e.tr.Touch(b)
		e.sweep()
	}
	if n := e.count(b, protocol.KindPeerDisconnected); n != 1 {
		t.Fatalf("peer_disconnected emitted %d times", n)
	}
	if e.state(b) != protocol.StatePeerDisconnected {
		t.Fatalf("state %s", e.state(b))
	}
}

func TestReconnectWithinGraceKeepsMembershipAndMessages(t *testing.T) {
	e := newSweepEnv(t)
	a, b, ha := e.pair()
	e.disconnectA(b)
	// b sends while a is disconnected: accepted and queued
	if err := e.s.Send(bg, b, "h/a", "while away"); err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(2 * time.Minute)
	e.tr.Touch(a)
	e.tr.Touch(b)
	e.sweep()
	if n := e.count(b, protocol.KindPeerReconnected); n != 1 {
		t.Fatalf("peer_reconnected emitted %d times", n)
	}
	if e.state(b) != protocol.StateConnected {
		t.Fatalf("state %s", e.state(b))
	}
	ks := e.kinds(a)
	if len(ks) != 2 || ks[0] != protocol.KindPeerJoined || ks[1] != protocol.KindText {
		t.Fatalf("a's queued rows: %v", ks)
	}
	if err := e.s.Send(bg, a, ha, "x"); err != nil {
		t.Fatalf("membership lost: %v", err)
	}
	// more sweeps do not emit again
	e.tr.Touch(a)
	e.tr.Touch(b)
	e.sweep()
	if n := e.count(b, protocol.KindPeerReconnected); n != 1 {
		t.Fatalf("peer_reconnected emitted %d times", n)
	}
}

func TestRequestBeforeReapDeadlineReconnectWins(t *testing.T) {
	e := newSweepEnv(t)
	a, b, _ := e.pair()
	e.disconnectA(b)
	e.clk.Advance(protocol.DisconnectGrace + time.Second) // past the reap deadline
	e.tr.Touch(a)                                         // a request arrives just before the sweep
	e.tr.Touch(b)
	e.sweep()
	if _, err := e.s.Sync(bg, a, protocol.SyncRequest{}); err != nil {
		t.Fatalf("session reaped from under a live request: %v", err)
	}
	if e.state(b) != protocol.StateConnected {
		t.Fatalf("state %s", e.state(b))
	}
}

func TestReapAfterGrace(t *testing.T) {
	e := newSweepEnv(t)
	a, b, _ := e.pair()
	e.disconnectA(b)
	e.clk.Advance(protocol.DisconnectGrace + time.Second)
	e.tr.Touch(b)
	e.sweep()
	if _, err := e.s.Sync(bg, a, protocol.SyncRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("reaped session still exists: %v", err)
	}
	if e.count(b, protocol.KindPeerLeft) != 1 || e.state(b) != protocol.StatePeerLeft {
		t.Fatalf("peer not told: %v / %s", e.kinds(b), e.state(b))
	}
	if !e.tr.Get(a).IsZero() {
		t.Fatal("tracker kept the reaped session")
	}
	e.tr.Touch(a) // a late touch
	if e.tr.size() != 1 || !e.tr.Get(a).IsZero() {
		t.Fatalf("late touch re-added the reaped session (size %d)", e.tr.size())
	}
}

func TestChannelTTLExpiry(t *testing.T) {
	e := newSweepEnv(t)
	a, b := e.session(), e.session()
	id, ha := mustCreate(t, e.s, a, "h/a")
	if err := e.s.Send(bg, a, ha, "held"); err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(protocol.ChannelTTL + time.Second)
	e.tr.Touch(a)
	e.tr.Touch(b)
	e.sweep()
	e.sweep() // exactly once
	if e.count(a, protocol.KindChannelExpired) != 1 {
		t.Fatalf("creator rows: %v", e.kinds(a))
	}
	if e.state(a) != protocol.StateExpired {
		t.Fatalf("state %s", e.state(a))
	}
	var held int
	e.s.db.QueryRow("SELECT COUNT(*) FROM held").Scan(&held)
	if held != 0 {
		t.Fatal("held rows survived expiry")
	}
	wantCode(t, e.s.Send(bg, a, ha, "x"), ErrExpired)
	_, _, err := e.s.JoinChannel(bg, b, id, "h/b")
	wantCode(t, err, ErrNoOpen)
	// The creator leaving removes the expired channel.
	if err := e.s.Leave(bg, a, ha); err != nil {
		t.Fatal(err)
	}
	var n int
	e.s.db.QueryRow("SELECT COUNT(*) FROM channels").Scan(&n)
	if n != 0 {
		t.Fatal("expired channel not deleted when the creator left")
	}
}

func TestRestartReapsNoLiveSession(t *testing.T) {
	e := newSweepEnv(t)
	a, b, _ := e.pair()
	e.clk.Advance(10 * time.Minute) // broker was down
	e.tr = NewTracker(e.clk.Now)    // restart
	e.sw = NewSweeper(e.s, e.tr, quiet)
	if err := e.tr.Init(bg, e.s); err != nil {
		t.Fatal(err)
	}
	e.sweep()
	for _, id := range []int64{a, b} {
		if _, err := e.s.Sync(bg, id, protocol.SyncRequest{}); err != nil {
			t.Fatalf("live session reaped after restart: %v", err)
		}
	}
	if e.state(a) != protocol.StateConnected || e.count(a, protocol.KindPeerDisconnected) != 0 {
		t.Fatal("live session disconnected on first sweep after restart")
	}
}

func TestRestartKeepsStoredDisconnect(t *testing.T) {
	e := newSweepEnv(t)
	_, b, _ := e.pair()
	e.disconnectA(b)
	e.clk.Advance(time.Second)
	e.tr = NewTracker(e.clk.Now) // restart
	e.sw = NewSweeper(e.s, e.tr, quiet)
	if err := e.tr.Init(bg, e.s); err != nil {
		t.Fatal(err)
	}
	e.sweep()
	if e.count(b, protocol.KindPeerReconnected) != 0 {
		t.Fatal("restart produced a spurious peer_reconnected")
	}
	if e.state(b) != protocol.StatePeerDisconnected {
		t.Fatalf("state %s", e.state(b))
	}
	e.clk.Advance(protocol.DisconnectGrace)
	e.tr.Touch(b)
	e.sweep()
	if e.state(b) != protocol.StatePeerLeft {
		t.Fatalf("not reaped on schedule: %s", e.state(b))
	}
}

func TestUnusedSessionDisconnectedThenReaped(t *testing.T) {
	e := newSweepEnv(t)
	c := e.session()
	e.clk.Advance(protocol.DisconnectAfter + time.Second)
	e.sweep()
	var disc *int64
	e.s.db.QueryRow("SELECT disconnected_at FROM sessions WHERE id = ?", c).Scan(&disc)
	if disc == nil {
		t.Fatal("unused session not disconnected after 60 s")
	}
	e.clk.Advance(protocol.DisconnectGrace + time.Second)
	e.sweep()
	if _, err := e.s.Sync(bg, c, protocol.SyncRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unused session immortal: %v", err)
	}
}

func TestSessionAfterHighestDeletedStaysConnected(t *testing.T) {
	e := newSweepEnv(t)
	e.session()
	hi := e.session()
	if err := e.s.DeleteSelf(bg, hi); err != nil {
		t.Fatal(err)
	}
	e.tr.Forget(hi)
	n := e.session()
	if n <= hi {
		t.Fatalf("session ID reused: %d <= %d", n, hi)
	}
	for i := 0; i < 6; i++ {
		e.clk.Advance(30 * time.Second)
		e.tr.Touch(n)
		e.sweep()
	}
	var disc *int64
	e.s.db.QueryRow("SELECT disconnected_at FROM sessions WHERE id = ?", n).Scan(&disc)
	if disc != nil {
		t.Fatal("active session was disconnected")
	}
}

func TestPeerStateMatchesLastEvent(t *testing.T) {
	e := newSweepEnv(t)
	a, b, _ := e.pair()
	last := func() string {
		ks := e.kinds(b)
		return ks[len(ks)-1]
	}
	if e.state(b) != protocol.StateConnected {
		t.Fatal("initial state")
	}
	e.disconnectA(b)
	if last() != protocol.KindPeerDisconnected || e.state(b) != protocol.StatePeerDisconnected {
		t.Fatalf("after disconnect: %s / %s", last(), e.state(b))
	}
	e.clk.Advance(time.Second)
	e.tr.Touch(a)
	e.sweep()
	if last() != protocol.KindPeerReconnected || e.state(b) != protocol.StateConnected {
		t.Fatalf("after reconnect: %s / %s", last(), e.state(b))
	}
	if err := e.s.Leave(bg, a, "h/b"); err != nil {
		t.Fatal(err)
	}
	if last() != protocol.KindPeerLeft || e.state(b) != protocol.StatePeerLeft {
		t.Fatalf("after leave: %s / %s", last(), e.state(b))
	}
}

func TestSweeperRunSweepsOnItsInterval(t *testing.T) {
	e := newSweepEnv(t)
	c := e.session()
	e.clk.Advance(protocol.DisconnectAfter + time.Second)
	e.sw.Interval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() { e.sw.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var disc *int64
		e.s.db.QueryRow("SELECT disconnected_at FROM sessions WHERE id = ?", c).Scan(&disc)
		if disc != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never swept")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}
