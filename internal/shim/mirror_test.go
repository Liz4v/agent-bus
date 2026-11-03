package shim

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/protocol"
)

// mirrorEnv is a shim session ("h/me") linked to a raw peer session ("h/peer").
type mirrorEnv struct {
	t       *testing.T
	tb      *testBroker
	sess    *Session
	m       *Mirror
	peerTok string
	handle  string // the shim side's handle for the channel
}

func fastCfg() MirrorConfig {
	return MirrorConfig{SyncWait: 2 * time.Second, BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond}
}

func newMirrorEnv(t *testing.T, cfg MirrorConfig) *mirrorEnv {
	t.Helper()
	tb := newTestBroker(t)
	sess := NewSession(tb.c)
	m := NewMirror(sess, cfg)
	t.Cleanup(m.Stop)
	return &mirrorEnv{t: t, tb: tb, sess: sess, m: m}
}

// link registers the shim, creates a channel and joins a peer to it.
func (e *mirrorEnv) link() {
	e.t.Helper()
	tok, err := e.sess.Ensure(bg)
	if err != nil {
		e.t.Fatal(err)
	}
	ch, err := e.tb.c.CreateChannel(bg, tok, "h/me")
	if err != nil {
		e.t.Fatal(err)
	}
	e.handle = ch.Handle
	e.peerTok, _ = e.tb.c.CreateSession(bg)
	if _, err := e.tb.c.JoinChannel(bg, e.peerTok, ch.ChannelID, "h/peer"); err != nil {
		e.t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return len(s.Channels) == 1 && s.Channels[0].State == protocol.StateConnected })
}

func (e *mirrorEnv) eventually(pred func(Snapshot) bool) Snapshot {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	s, err := e.m.WaitChange(ctx, pred)
	if err != nil {
		e.t.Fatalf("condition not reached; mirror: %+v", s)
	}
	return s
}

func (e *mirrorEnv) peerSend(text string) {
	e.t.Helper()
	if err := e.tb.c.Send(bg, e.peerTok, "h/me", text); err != nil {
		e.t.Fatal(err)
	}
}

func rowsWithKind(s Snapshot, kind string) int {
	n := 0
	for _, r := range s.Rows {
		if r.Kind == kind {
			n++
		}
	}
	return n
}

func TestMirrorReceivesWithoutToolCall(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.peerSend("hello")
	s := e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	// the creator also has peer_joined unread
	if rowsWithKind(s, protocol.KindPeerJoined) != 1 {
		t.Fatalf("rows: %+v", s.Rows)
	}
}

func TestCommitRemovesRowsAndBrokerDeletes(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.peerSend("one")
	s := e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	maxID := s.Rows[len(s.Rows)-1].ID
	e.m.Commit(s.Gen, maxID)
	if rows, _ := e.m.Unread(); len(rows) != 0 {
		t.Fatalf("rows left after commit: %+v", rows)
	}
	tok := e.sess.Token()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := e.tb.c.Sync(bg, tok, protocol.SyncRequest{After: 0})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Messages) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker never deleted the acked rows: %+v", r.Messages)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestUncommittedRowsReturnedAgain(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.peerSend("keep")
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	a, _ := e.m.Unread()
	if err := e.m.Refresh(bg); err != nil {
		t.Fatal(err)
	}
	b, _ := e.m.Unread()
	if len(a) != len(b) {
		t.Fatalf("rows changed without a commit: %d -> %d", len(a), len(b))
	}
	r, _ := e.tb.c.Sync(bg, e.sess.Token(), protocol.SyncRequest{})
	if len(r.Messages) != len(a) {
		t.Fatalf("broker deleted uncommitted rows: %d vs %d", len(r.Messages), len(a))
	}
}

func TestInformationalEventsNotCounted(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	// consume peer_joined so only informational rows remain
	s := e.m.Snapshot()
	e.m.Commit(s.Gen, s.Rows[len(s.Rows)-1].ID)

	e.tb.clk.Advance(protocol.DisconnectAfter + time.Second)
	if err := e.m.Refresh(bg); err != nil { // touches the shim's session at the new time
		t.Fatal(err)
	}
	if err := e.tb.sweeper.Sweep(bg); err != nil {
		t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerDisconnected) == 1 })
	e.tb.clk.Advance(time.Second)
	if _, err := e.tb.c.Sync(bg, e.peerTok, protocol.SyncRequest{}); err != nil { // peer is seen again
		t.Fatal(err)
	}
	if err := e.tb.sweeper.Sweep(bg); err != nil {
		t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerReconnected) == 1 })
	sum := e.m.Summary()
	if sum.Count != 0 || len(sum.From) != 0 || sum.LiveChannels != 1 {
		t.Fatalf("summary: %+v", sum)
	}
	// a text message does count
	e.peerSend("x")
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	if sum := e.m.Summary(); sum.Count != 1 || len(sum.From) != 1 || sum.From[0] != "h/peer" {
		t.Fatalf("summary: %+v", sum)
	}
}

func TestBrokerStopSetsHealthAndKeepsMirror(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.peerSend("before outage")
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	e.tb.down.Store(true)
	if err := e.m.Refresh(bg); err == nil {
		t.Fatal("refresh against a stopped broker succeeded")
	}
	s := e.eventually(func(s Snapshot) bool { return s.Health != nil })
	if rowsWithKind(s, protocol.KindText) != 1 || len(s.Channels) != 1 {
		t.Fatalf("mirror lost during outage: %+v", s)
	}
	if !strings.Contains(s.Health.Error(), e.tb.ts.URL) {
		t.Fatalf("health does not name the broker: %v", s.Health)
	}
	e.tb.down.Store(false)
	e.eventually(func(s Snapshot) bool { return s.Health == nil })
}

func TestForced401EmptiesMirrorAndWakesWaiters(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	woken := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		e.m.WaitChange(ctx, func(s Snapshot) bool { return !s.Running })
		close(woken)
	}()
	e.tb.kill(e.sess.Token())
	if err := e.m.Refresh(bg); err != nil {
		t.Fatalf("refresh hit by the 401 must return nil, got %v", err)
	}
	s := e.m.Snapshot()
	if s.Running || len(s.Rows) != 0 || len(s.Channels) != 0 || s.Health != nil {
		t.Fatalf("mirror not emptied: %+v", s)
	}
	if e.sess.Token() != "" || e.sess.PeekNotice() == "" {
		t.Fatalf("token %q notice %q", e.sess.Token(), e.sess.PeekNotice())
	}
	select {
	case <-woken:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken by the drop")
	}
}

func TestForced401NoticeNamesUnreadWithoutContent(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.peerSend("TOPSECRETCONTENT")
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	e.tb.kill(e.sess.Token())
	e.m.Refresh(bg)
	n := e.sess.PeekNotice()
	if !strings.Contains(n, "2 unread") || !strings.Contains(n, "h/peer") || strings.Contains(n, "TOPSECRET") {
		t.Fatalf("notice: %q", n)
	}
}

func TestStaleResponseAfterNewRegistrationIgnored(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.tb.delay.Store(int64(500 * time.Millisecond))
	e.peerSend("late") // the in-flight long-poll returns, then the handler sleeps before responding
	time.Sleep(150 * time.Millisecond)
	old := e.sess.Token()
	e.sess.Drop(old)
	if _, err := e.sess.Ensure(bg); err != nil { // new registration, new loop generation
		t.Fatal(err)
	}
	time.Sleep(900 * time.Millisecond)
	s := e.m.Snapshot()
	if len(s.Rows) != 0 || len(s.Channels) != 0 {
		t.Fatalf("old session's data leaked into the new mirror: %+v", s)
	}
}

func TestNewLoopStartsFromZeroAndSeesLeaveQuickly(t *testing.T) {
	e := newMirrorEnv(t, MirrorConfig{SyncWait: 20 * time.Second, BackoffMin: 20 * time.Millisecond})
	e.link()
	e.peerSend("x")
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	s := e.m.Snapshot()
	e.m.Commit(s.Gen, s.Rows[len(s.Rows)-1].ID)
	e.sess.Drop(e.sess.Token())
	e.m.mu.Lock()
	zero := e.m.cursor == 0 && e.m.after == 0 && e.m.version == 0
	e.m.mu.Unlock()
	if !zero {
		t.Fatal("cursor, after or version survived the drop")
	}
	// fresh registration, new channel, peer joins then leaves
	tok, err := e.sess.Ensure(bg)
	if err != nil {
		t.Fatal(err)
	}
	ch, _ := e.tb.c.CreateChannel(bg, tok, "h/me")
	e.eventually(func(s Snapshot) bool { return len(s.Channels) == 1 && s.Channels[0].State == protocol.StateWaiting })
	e.peerTok, _ = e.tb.c.CreateSession(bg) // the old peer is still linked to the old session
	if _, err := e.tb.c.JoinChannel(bg, e.peerTok, ch.ChannelID, "h/peer"); err != nil {
		t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return len(s.Channels) == 1 && s.Channels[0].State == protocol.StateConnected })
	start := time.Now()
	e.tb.c.Leave(bg, e.peerTok, "h/me")
	e.eventually(func(s Snapshot) bool { return len(s.Channels) == 1 && s.Channels[0].State == protocol.StatePeerLeft })
	if time.Since(start) > 3*time.Second {
		t.Fatalf("leave took %v to show; the 20 s poll was waited out", time.Since(start))
	}
}

// scriptedBroker serves registration and then a fixed sequence of sync
// responses; after the script runs out it holds the connection open.
func scriptedBroker(t *testing.T, script []protocol.SyncResponse) *Session {
	t.Helper()
	var mu sync.Mutex
	next := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sessions":
			w.WriteHeader(201)
			w.Write([]byte(`{"session_token":"tok"}`))
		case "/v1/sync":
			mu.Lock()
			i := next
			next++
			mu.Unlock()
			if i >= len(script) {
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
				return
			}
			json.NewEncoder(w).Encode(script[i])
		}
	}))
	t.Cleanup(ts.Close)
	return NewSession(client.New(ts.URL))
}

func TestStaleResponseRowsNotAppended(t *testing.T) {
	row := func(id int64, text string) protocol.Message {
		return protocol.Message{ID: id, Handle: "ch-1", Kind: protocol.KindText, Text: text}
	}
	sess := scriptedBroker(t, []protocol.SyncResponse{
		{Version: 1, Messages: []protocol.Message{row(1, "one"), row(2, "two")}},
		{Version: 2, Messages: []protocol.Message{row(2, "stale duplicate"), row(1, "stale again")}}, // rows <= after
		{Version: 3, Messages: []protocol.Message{row(3, "three")}},
	})
	m := NewMirror(sess, MirrorConfig{SyncWait: time.Second, BackoffMin: 10 * time.Millisecond})
	defer m.Stop()
	if _, err := sess.Ensure(bg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	s, err := m.WaitChange(ctx, func(s Snapshot) bool { return len(s.Rows) == 3 })
	if err != nil {
		t.Fatalf("rows: %+v", s.Rows)
	}
	for i, want := range []string{"one", "two", "three"} {
		if s.Rows[i].Text != want {
			t.Fatalf("row %d = %q, want %q (stale rows were appended?)", i, s.Rows[i].Text, want)
		}
	}
}

func TestRefreshDuringBackoffReturnsPromptly(t *testing.T) {
	e := newMirrorEnv(t, MirrorConfig{SyncWait: 2 * time.Second, BackoffMin: 5 * time.Second, BackoffMax: 5 * time.Second})
	e.link()
	e.tb.down.Store(true)
	if err := e.m.Refresh(bg); err == nil {
		t.Fatal("expected an error")
	}
	time.Sleep(100 * time.Millisecond) // the loop is now in its 5 s backoff
	start := time.Now()
	err := e.m.Refresh(bg)
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("refresh during backoff: err=%v after %v", err, time.Since(start))
	}
	e.tb.down.Store(false)
	if err := e.m.Refresh(bg); err != nil {
		t.Fatalf("refresh after recovery: %v", err)
	}
}

func TestRefreshWaitingWhenSessionDroppedReturnsNil(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.tb.delay.Store(int64(2 * time.Second))
	done := make(chan error, 1)
	go func() { done <- e.m.Refresh(bg) }()
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	e.sess.Drop(e.sess.Token()) // as a tool's 401 would
	select {
	case err := <-done:
		if err != nil || time.Since(start) > time.Second {
			t.Fatalf("err=%v after %v", err, time.Since(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("refresh still waiting after the session was dropped")
	}
	e.tb.delay.Store(0)
}

func TestRefreshWithoutSessionReturnsNilAtOnce(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	start := time.Now()
	if err := e.m.Refresh(bg); err != nil || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("err=%v after %v", err, time.Since(start))
	}
	if e.tb.totalHits() != 0 {
		t.Fatal("refresh without a session contacted the broker")
	}
}

func TestMorePagingFetchesEverything(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	chunk := strings.Repeat("z", 60<<10)
	const n = 20 // about 1.2 MiB, more than one sync page
	for i := 0; i < n; i++ {
		e.peerSend(chunk)
	}
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == n })
}

func TestSyncAtLeastEveryIntervalKeepsSessionConnected(t *testing.T) {
	e := newMirrorEnv(t, MirrorConfig{SyncWait: time.Second, BackoffMin: 20 * time.Millisecond})
	e.link()
	before := e.tb.hitCount("POST /v1/sync")
	time.Sleep(3500 * time.Millisecond)
	if got := e.tb.hitCount("POST /v1/sync") - before; got < 3 {
		t.Fatalf("only %d syncs in 3.5 s with a 1 s wait", got)
	}
	// Two minutes of (fake) idle time: only the sync loop can keep the shim's
	// session seen. The peer does a request of its own, like a live peer would.
	e.tb.clk.Advance(2 * protocol.DisconnectAfter)
	deadline := time.Now().Add(5 * time.Second)
	for e.tb.tracker.Get(mustSessionID(t, e)).Before(e.tb.clk.Now()) {
		if time.Now().After(deadline) {
			t.Fatal("the sync loop did not touch the session after the clock moved")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := e.tb.c.Sync(bg, e.peerTok, protocol.SyncRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := e.tb.sweeper.Sweep(bg); err != nil {
		t.Fatal(err)
	}
	r, err := e.tb.c.Sync(bg, e.peerTok, protocol.SyncRequest{})
	if err != nil || len(r.Channels) != 1 || r.Channels[0].State != protocol.StateConnected {
		t.Fatalf("shim's session not connected: %+v %v", r.Channels, err)
	}
}

func mustSessionID(t *testing.T, e *mirrorEnv) int64 {
	t.Helper()
	id, err := e.tb.store.SessionByToken(bg, e.sess.Token())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSummaryCursor(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	if c := e.m.Summary().Cursor; c != 0 {
		t.Fatalf("initial cursor %d", c)
	}
	e.link()
	e.peerSend("one")
	s := e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	maxID := s.Rows[len(s.Rows)-1].ID
	e.m.Commit(s.Gen, maxID)
	sum := e.m.Summary()
	if sum.Cursor != maxID || sum.Count != 0 {
		t.Fatalf("after commit: cursor %d (want %d), count %d", sum.Cursor, maxID, sum.Count)
	}
	e.tb.kill(e.sess.Token())
	e.m.Refresh(bg)
	if c := e.m.Summary().Cursor; c != 0 {
		t.Fatalf("cursor %d after session reset", c)
	}
}
