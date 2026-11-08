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

// pair returns two partnered, named sessions: a ("alice") and b ("bob"). One
// message went each way and both inboxes were acknowledged, so the tests see
// only the events that follow.
func (e *sweepEnv) pair() (a, b int64) {
	a, b = e.session(), e.session()
	claimAs(e.t, e.s, a, "alice")
	claimAs(e.t, e.s, b, "bob")
	mustSend(e.t, e.s, a, "bob", "hello")
	mustSend(e.t, e.s, b, "alice", "hello back")
	e.drain(a)
	e.drain(b)
	return a, b
}

// drain acknowledges everything in a session's inbox.
func (e *sweepEnv) drain(sess int64) {
	e.t.Helper()
	r := sync0(e.t, e.s, sess, protocol.SyncRequest{})
	if len(r.Messages) == 0 {
		return
	}
	sync0(e.t, e.s, sess, protocol.SyncRequest{Ack: lastID(r)})
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

// disconnected reports whether the broker has marked the session disconnected.
func (e *sweepEnv) disconnected(sess int64) bool {
	e.t.Helper()
	var disc *int64
	if err := e.s.db.QueryRow("SELECT disconnected_at FROM sessions WHERE id = ?", sess).Scan(&disc); err != nil {
		e.t.Fatal(err)
	}
	return disc != nil
}

// wantPeerLeft checks that a send from `from` to name now fails as peer_left.
func (e *sweepEnv) wantPeerLeft(from int64, name string) {
	e.t.Helper()
	if err := e.s.Send(bg, from, name, "x"); !errors.Is(err, ErrPeerLeft) {
		e.t.Fatalf("send to %q: %v, want ErrPeerLeft", name, err)
	}
}

// disconnectA advances time so that only b stays seen, and sweeps once.
func (e *sweepEnv) disconnectA(b int64) {
	e.clk.Advance(protocol.DisconnectAfter + time.Second)
	e.tr.Touch(b)
	e.sweep()
}

func TestDisconnectEmittedExactlyOnce(t *testing.T) {
	e := newSweepEnv(t)
	a, b := e.pair()
	e.disconnectA(b)
	for i := 0; i < 3; i++ {
		e.clk.Advance(15 * time.Second)
		e.tr.Touch(b)
		e.sweep()
	}
	if n := e.count(b, protocol.KindPeerDisconnected); n != 1 {
		t.Fatalf("peer_disconnected emitted %d times", n)
	}
	if !e.disconnected(a) {
		t.Fatal("a not marked disconnected")
	}
}

func TestReconnectWithinGraceKeepsNameAndMessages(t *testing.T) {
	e := newSweepEnv(t)
	a, b := e.pair()
	e.disconnectA(b)
	// b sends while a is disconnected: accepted and queued
	if err := e.s.Send(bg, b, "alice", "while away"); err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(2 * time.Minute)
	e.tr.Touch(a)
	e.tr.Touch(b)
	e.sweep()
	if n := e.count(b, protocol.KindPeerReconnected); n != 1 {
		t.Fatalf("peer_reconnected emitted %d times", n)
	}
	if e.disconnected(a) {
		t.Fatal("a still marked disconnected")
	}
	ks := e.kinds(a)
	if len(ks) != 1 || ks[0] != protocol.KindText {
		t.Fatalf("a's queued rows: %v", ks)
	}
	if err := e.s.Send(bg, a, "bob", "x"); err != nil {
		t.Fatalf("name lost: %v", err)
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
	a, b := e.pair()
	e.disconnectA(b)
	e.clk.Advance(protocol.DisconnectGrace + time.Second) // past the reap deadline
	e.tr.Touch(a)                                         // a request arrives just before the sweep
	e.tr.Touch(b)
	e.sweep()
	if _, err := e.s.Sync(bg, a, protocol.SyncRequest{}); err != nil {
		t.Fatalf("session reaped from under a live request: %v", err)
	}
	if e.disconnected(a) {
		t.Fatal("a still marked disconnected")
	}
}

func TestReapAfterGrace(t *testing.T) {
	e := newSweepEnv(t)
	a, b := e.pair()
	e.disconnectA(b)
	e.clk.Advance(protocol.DisconnectGrace + time.Second)
	e.tr.Touch(b)
	e.sweep()
	if _, err := e.s.Sync(bg, a, protocol.SyncRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("reaped session still exists: %v", err)
	}
	if e.count(b, protocol.KindPeerLeft) != 1 {
		t.Fatalf("peer not told: %v", e.kinds(b))
	}
	e.wantPeerLeft(b, "alice")
	if !e.tr.Get(a).IsZero() {
		t.Fatal("tracker kept the reaped session")
	}
	e.tr.Touch(a) // a late touch
	if e.tr.size() != 1 || !e.tr.Get(a).IsZero() {
		t.Fatalf("late touch re-added the reaped session (size %d)", e.tr.size())
	}
}

func TestRestartReapsNoLiveSession(t *testing.T) {
	e := newSweepEnv(t)
	a, b := e.pair()
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
	if e.disconnected(a) || e.count(a, protocol.KindPeerDisconnected) != 0 {
		t.Fatal("live session disconnected on first sweep after restart")
	}
}

func TestRestartKeepsStoredDisconnect(t *testing.T) {
	e := newSweepEnv(t)
	a, b := e.pair()
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
	if !e.disconnected(a) {
		t.Fatal("restart lost the stored disconnect")
	}
	e.clk.Advance(protocol.DisconnectGrace)
	e.tr.Touch(b)
	e.sweep()
	e.wantPeerLeft(b, "alice")
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
	a, b := e.pair()
	last := func() string {
		ks := e.kinds(b)
		return ks[len(ks)-1]
	}
	if e.disconnected(a) {
		t.Fatal("initial state")
	}
	e.disconnectA(b)
	if last() != protocol.KindPeerDisconnected || !e.disconnected(a) {
		t.Fatalf("after disconnect: %s / %v", last(), e.disconnected(a))
	}
	e.clk.Advance(time.Second)
	e.tr.Touch(a)
	e.sweep()
	if last() != protocol.KindPeerReconnected || e.disconnected(a) {
		t.Fatalf("after reconnect: %s / %v", last(), e.disconnected(a))
	}
	if _, err := e.s.ReleaseName(bg, a); err != nil {
		t.Fatal(err)
	}
	if last() != protocol.KindPeerLeft {
		t.Fatalf("after release: %s", last())
	}
	e.wantPeerLeft(b, "alice")
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

func TestSweepExpiresPendingAndPrunesRetired(t *testing.T) {
	e := newSweepEnv(t)
	code, _, err := e.s.IssueClaim(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.Register(bg, "boss", code); err != nil {
		t.Fatal(err)
	}
	p, err := e.s.Register(bg, "waiting", "")
	if err != nil {
		t.Fatal(err)
	}
	e.clk.Advance(protocol.PendingExpiry + time.Second)
	e.sweep()
	var n int
	e.s.db.QueryRow("SELECT COUNT(*) FROM accounts WHERE status = 'pending'").Scan(&n)
	if n != 0 {
		t.Fatalf("%d pending rows survived the sweep", n)
	}
	var outcome string
	if err := e.s.db.QueryRow("SELECT outcome FROM retired WHERE name = 'waiting'").Scan(&outcome); err != nil || outcome != "expired" {
		t.Fatalf("retired outcome %q, %v", outcome, err)
	}
	if a, err := e.s.AccountByCredential(bg, p.Credential); err != nil || a.Status != protocol.AccountExpired {
		t.Fatalf("status %+v %v", a, err)
	}
	e.clk.Advance(protocol.RetiredKeep + time.Second)
	e.sweep()
	e.s.db.QueryRow("SELECT COUNT(*) FROM retired").Scan(&n)
	if n != 0 {
		t.Fatalf("%d retired rows survived pruning", n)
	}
	if _, err := e.s.AccountByCredential(bg, p.Credential); err != ErrBadCredential {
		t.Fatalf("pruned credential: %v", err)
	}
}

func TestReapEmitsPeerLeftNotPeerRemoved(t *testing.T) {
	e := newSweepEnv(t)
	_, b := e.pair()
	e.disconnectA(b) // a goes quiet; b stays seen
	e.clk.Advance(protocol.DisconnectGrace + time.Second)
	e.tr.Touch(b)
	e.sweep()
	if e.count(b, protocol.KindPeerLeft) != 1 || e.count(b, protocol.KindPeerRemoved) != 0 {
		t.Fatalf("b rows: %v", e.kinds(b))
	}
	e.wantPeerLeft(b, "alice")
}

func TestDisconnectEventsCarryAccountAndName(t *testing.T) {
	e := newSweepEnv(t)
	_, b := e.pair()
	e.disconnectA(b)
	r := sync0(t, e.s, b, protocol.SyncRequest{})
	var found bool
	for _, m := range r.Messages {
		if m.Kind == protocol.KindPeerDisconnected {
			found = m.FromAccount != nil && *m.FromAccount != "" && strp(m.From) == "alice"
			if protocol.IsWaking(m.Kind) {
				t.Fatal("peer_disconnected must not wake")
			}
		}
	}
	if !found {
		t.Fatalf("peer_disconnected without from and from_account: %+v", r.Messages)
	}
}

func TestPresenceEventsGoToPartnersOnly(t *testing.T) {
	e := newSweepEnv(t)
	a, b := e.pair()
	c := e.session()
	claimAs(t, e.s, c, "carol") // named, but never exchanged a message with a
	e.clk.Advance(protocol.DisconnectAfter + time.Second)
	e.tr.Touch(b)
	e.tr.Touch(c)
	e.sweep()
	if !e.disconnected(a) {
		t.Fatal("setup: a should be disconnected")
	}
	if e.count(b, protocol.KindPeerDisconnected) != 1 {
		t.Fatalf("partner not told: %v", e.kinds(b))
	}
	if ks := e.kinds(c); len(ks) != 0 {
		t.Fatalf("a non-partner was told: %v", ks)
	}
	e.clk.Advance(time.Second)
	e.tr.Touch(a)
	e.sweep()
	if ks := e.kinds(c); len(ks) != 0 {
		t.Fatalf("a non-partner was told about the reconnect: %v", ks)
	}
}

func TestDisconnectOfAnUnnamedSessionTellsNobody(t *testing.T) {
	e := newSweepEnv(t)
	_, b := e.pair()
	c := e.session() // never claims a name
	e.clk.Advance(protocol.DisconnectAfter + time.Second)
	e.tr.Touch(b)
	e.sweep()
	if !e.disconnected(c) {
		t.Fatal("setup: c should be disconnected")
	}
	if ks := e.kinds(b); len(ks) != 1 || ks[0] != protocol.KindPeerDisconnected {
		t.Fatalf("b rows: %v (only a's disconnect expected)", ks)
	}
}

func TestReapFreesTheNameAndRecordsTheDeparture(t *testing.T) {
	e := newSweepEnv(t)
	_, b := e.pair()
	e.disconnectA(b)
	e.clk.Advance(protocol.DisconnectGrace + time.Second)
	e.tr.Touch(b)
	e.sweep()
	if e.count(b, protocol.KindPeerLeft) != 1 {
		t.Fatalf("partner not told: %v", e.kinds(b))
	}
	var kind string
	if err := e.s.db.QueryRow("SELECT kind FROM departed WHERE session_id = ? AND name_key = 'alice'", b).Scan(&kind); err != nil || kind != protocol.KindPeerLeft {
		t.Fatalf("departed row: %q, %v", kind, err)
	}
	// The name is free: another session claims it, and b's next send reaches it.
	d := e.session()
	claimAs(t, e.s, d, "alice")
	mustSend(t, e.s, b, "alice", "who is this?")
	if got := inboxOf(t, e.s, d); len(got) != 1 || !got[0].FirstContact {
		t.Fatalf("new holder rows: %+v", got)
	}
}
