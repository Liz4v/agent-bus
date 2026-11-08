package shim

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/testutil"
)

// mirrorEnv is a shim session (named "me") linked to a raw peer session
// (named "peer"): both hold their names and have exchanged a message, so they
// are partners.
type mirrorEnv struct {
	t       *testing.T
	tb      *testBroker
	sess    *Session
	m       *Mirror
	peerTok string
}

func fastCfg() MirrorConfig {
	return MirrorConfig{SyncWait: 2 * time.Second, BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond}
}

func newMirrorEnv(t *testing.T, cfg MirrorConfig) *mirrorEnv {
	t.Helper()
	tb := newTestBroker(t)
	return newMirrorEnvFor(t, tb, cfg, tb.c)
}

// newMirrorEnvFor builds a mirror env whose shim session uses client c.
func newMirrorEnvFor(t *testing.T, tb *testBroker, cfg MirrorConfig, c *client.Client) *mirrorEnv {
	t.Helper()
	sess := NewSession(c)
	m := NewMirror(sess, cfg)
	t.Cleanup(m.Stop)
	return &mirrorEnv{t: t, tb: tb, sess: sess, m: m}
}

// link registers the shim, claims "me" for it and "peer" for a raw peer, and
// makes them partners (the shim says hello; nothing is left unread locally).
func (e *mirrorEnv) link() {
	e.t.Helper()
	tok, err := e.sess.Ensure(bg)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := e.tb.c.ClaimName(bg, tok, "me"); err != nil {
		e.t.Fatal(err)
	}
	e.peerTok, _, _ = e.tb.c.CreateSession(bg)
	if _, err := e.tb.c.ClaimName(bg, e.peerTok, "peer"); err != nil {
		e.t.Fatal(err)
	}
	if err := e.tb.c.Send(bg, tok, "peer", "hello"); err != nil {
		e.t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return s.Name == "me" })
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
	if err := e.tb.c.Send(bg, e.peerTok, "me", text); err != nil {
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
	if s.Name != "me" || len(s.Rows) != 1 || s.Rows[0].From == nil || *s.Rows[0].From != "peer" || s.Rows[0].FirstContact {
		t.Fatalf("snapshot: %+v", s)
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
	if sum.Count != 0 || len(sum.From) != 0 || !sum.Named {
		t.Fatalf("summary: %+v", sum)
	}
	// a text message does count
	e.peerSend("x")
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	if sum := e.m.Summary(); sum.Count != 1 || len(sum.From) != 1 || sum.From[0] != "peer" {
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
	if rowsWithKind(s, protocol.KindText) != 1 || s.Name != "me" {
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
	if s.Running || len(s.Rows) != 0 || s.Name != "" || s.Health != nil {
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
	if !strings.Contains(n, "1 unread") || !strings.Contains(n, "peer") || strings.Contains(n, "TOPSECRET") {
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
	if len(s.Rows) != 0 || s.Name != "" {
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
	// fresh registration and a new name (the old session still holds "me" on
	// the broker), a new peer that becomes a partner and then releases its name
	tok, err := e.sess.Ensure(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.tb.c.ClaimName(bg, tok, "me2"); err != nil {
		t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return s.Name == "me2" })
	e.peerTok, _, _ = e.tb.c.CreateSession(bg)
	if _, err := e.tb.c.ClaimName(bg, e.peerTok, "peer2"); err != nil {
		t.Fatal(err)
	}
	if err := e.tb.c.Send(bg, tok, "peer2", "hi"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := e.tb.c.ReleaseName(bg, e.peerTok); err != nil {
		t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerLeft) == 1 })
	if time.Since(start) > 3*time.Second {
		t.Fatalf("peer_left took %v to show; the 20 s poll was waited out", time.Since(start))
	}
}

// scriptedBroker serves registration and then a fixed sequence of sync
// responses; after the script runs out it holds the connection open.
func scriptedBroker(t *testing.T, script []protocol.SyncResponse) *Session {
	t.Helper()
	var mu sync.Mutex
	next := 0
	ts, pin := testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sessions":
			w.WriteHeader(201)
			w.Write([]byte(`{"session_token":"tok","account":"a"}`))
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
	return NewSession(testClient(t, ts.URL, pin))
}

func TestStaleResponseRowsNotAppended(t *testing.T) {
	row := func(id int64, text string) protocol.Message {
		return protocol.Message{ID: id, Kind: protocol.KindText, Text: text}
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
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range r.Messages {
		if m.Kind == protocol.KindPeerDisconnected || m.Kind == protocol.KindPeerLeft {
			t.Fatalf("the shim's session was disconnected or reaped: %+v", r.Messages)
		}
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

func TestAdminSubscribedPropagates(t *testing.T) {
	tb := newTestBroker(t)
	sess := NewSession(tb.newClient(tb.bossCred))
	m := NewMirror(sess, fastCfg())
	t.Cleanup(m.Stop)
	tok, err := sess.Ensure(bg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	if s, _ := m.WaitChange(ctx, func(s Snapshot) bool { return s.Running }); s.AdminSubscribed {
		t.Fatal("subscribed before asking")
	}
	if _, err := sess.Client().SetAdminSubscription(bg, tok, true); err != nil {
		t.Fatal(err)
	}
	if _, err := m.WaitChange(ctx, func(s Snapshot) bool { return s.AdminSubscribed }); err != nil {
		t.Fatal("AdminSubscribed never reached the mirror")
	}
	if !m.Summary().AdminSubscribed {
		t.Fatal("Summary lacks AdminSubscribed")
	}
}

func eventuallyDropped(t *testing.T, s *Session) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.Token() != "" {
		if time.Now().After(deadline) {
			t.Fatal("session was not dropped")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMirror401WithRemovedAccountGivesRemovedNotice(t *testing.T) {
	tb := newTestBroker(t)
	sess := NewSession(tb.newClient(tb.user("victim")))
	m := NewMirror(sess, fastCfg())
	t.Cleanup(m.Stop)
	tok, _ := sess.Ensure(bg)
	if _, err := sess.Client().ClaimName(bg, tok, "victim-s"); err != nil {
		t.Fatal(err)
	}
	m.Refresh(bg)
	if _, _, err := tb.store.Remove(bg, "victim"); err != nil {
		t.Fatal(err)
	}
	eventuallyDropped(t, sess)
	n := sess.TakeNotice()
	if !strings.Contains(n, "removed by the hostmaster") || strings.Contains(n, "expired while this host was disconnected") {
		t.Fatalf("notice: %q", n)
	}
}

func TestMirror401WithActiveAccountKeepsExistingNotice(t *testing.T) {
	tb := newTestBroker(t)
	sess := NewSession(tb.newClient(tb.user("steady")))
	m := NewMirror(sess, fastCfg())
	t.Cleanup(m.Stop)
	tok, _ := sess.Ensure(bg)
	sess.Client().ClaimName(bg, tok, "steady-s")
	m.Refresh(bg)
	tb.kill(tok) // as a reap would
	eventuallyDropped(t, sess)
	if n := sess.TakeNotice(); !strings.Contains(n, "expired while this host was disconnected") {
		t.Fatalf("notice: %q", n)
	}
}

func TestSendHittingThe401FirstStillGetsAccountNotice(t *testing.T) {
	tb := newTestBroker(t)
	a := newShimWith(t, tb, tb.newClient(tb.user("victim")), nil, false)
	b := newShimWith(t, tb, tb.newClient(tb.user("friend")), nil, false)
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	b.mustOK("claim_name", map[string]any{"name": "bob"})
	a.mustOK("send", map[string]any{"peer": "bob", "text": "hello"}) // they are partners now
	a.eventually(func(s Snapshot) bool { return s.Name == "alice" })
	a.m.Stop() // the mirror cannot see the 401 first; the send does
	if _, _, err := tb.store.Remove(bg, "victim"); err != nil {
		t.Fatal(err)
	}
	text, isErr := a.call("send", map[string]any{"peer": "bob", "text": "hi"})
	if !isErr || !strings.Contains(text, "removed by the hostmaster") || strings.Contains(text, "expired while this host was disconnected") {
		t.Fatalf("send after removal: %v %q", isErr, text)
	}
	// And the peer sees peer_removed, naming the account.
	b.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerRemoved) == 1 })
	got := b.mustOK("read", nil)
	if !strings.Contains(got, `kind="peer_removed"`) || !strings.Contains(got, `account="victim"`) {
		t.Fatalf("peer read: %s", got)
	}
}

func TestDiscardPeerRowsKeepsAdminRowsAndTheCursor(t *testing.T) {
	row := func(id int64, kind, text string) protocol.Message {
		return protocol.Message{ID: id, Kind: kind, Text: text}
	}
	sess := scriptedBroker(t, []protocol.SyncResponse{
		{Version: 1, Name: "me", Messages: []protocol.Message{row(1, protocol.KindText, "one"), row(2, protocol.KindPeerLeft, ""),
			row(3, protocol.KindRegistrationPending, "newbie"), row(4, protocol.KindPeerDisconnected, "")}},
	})
	m := NewMirror(sess, MirrorConfig{SyncWait: time.Second, BackoffMin: 10 * time.Millisecond})
	defer m.Stop()
	if _, err := sess.Ensure(bg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	s, err := m.WaitChange(ctx, func(s Snapshot) bool { return len(s.Rows) == 4 })
	if err != nil {
		t.Fatalf("rows: %+v", s.Rows)
	}
	m.Commit(s.Gen, 1) // row 1 was read: the cursor is 1
	m.DiscardPeerRows()
	got := m.Snapshot()
	if len(got.Rows) != 1 || got.Rows[0].Kind != protocol.KindRegistrationPending || got.Rows[0].ID != 3 {
		t.Fatalf("after discard: %+v", got.Rows)
	}
	if got.Cursor != 1 {
		t.Fatalf("the cursor moved: %d", got.Cursor)
	}
	if sum := m.Summary(); sum.Count != 1 {
		t.Fatalf("the admin row must still count as unread and waking: %+v", sum)
	}
}

// After release_name the broker has deleted the session's peer rows, but the
// mirror already holds the ones it synced, and later syncs ask only for rows
// above `after`: only an explicit discard drops them, and they must not come
// back.
func TestDiscardPeerRowsRowsDoNotComeBackFromTheBroker(t *testing.T) {
	e := newMirrorEnv(t, fastCfg())
	e.link()
	e.peerSend("before release")
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	if _, err := e.tb.c.ReleaseName(bg, e.sess.Token()); err != nil {
		t.Fatal(err)
	}
	e.m.DiscardPeerRows()
	if err := e.m.Refresh(bg); err != nil {
		t.Fatal(err)
	}
	s := e.m.Snapshot()
	if len(s.Rows) != 0 || s.Name != "" {
		t.Fatalf("after release and discard: %+v", s)
	}
	if sum := e.m.Summary(); sum.Count != 0 || sum.Named {
		t.Fatalf("summary after release: %+v", sum)
	}
	// The discard did not wedge the loop: a new name receives new messages.
	if _, err := e.tb.c.ClaimName(bg, e.sess.Token(), "me-again"); err != nil {
		t.Fatal(err)
	}
	if err := e.tb.c.Send(bg, e.peerTok, "me-again", "after"); err != nil {
		t.Fatal(err)
	}
	s = e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindText) == 1 })
	if s.Name != "me-again" || s.Rows[0].Text != "after" {
		t.Fatalf("after the new claim: %+v", s)
	}
}

// A sync response in flight while DiscardPeerRows runs can carry a row the
// broker has since deleted, and the old name. It must apply neither: the
// mirror drops it and asks again. The in-flight request is an immediate
// (wait=0) one, which a kick cannot cancel, so only the epoch check can save
// the mirror. The server is scripted so the order is exact, not timed.
func TestDiscardPeerRowsDropsAResponseAlreadyInFlight(t *testing.T) {
	var mu sync.Mutex
	immediate := 0
	inflight := make(chan struct{})
	release := make(chan struct{})
	var lastAfter int64 = -1
	ts, pin := testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sessions":
			w.WriteHeader(201)
			w.Write([]byte(`{"session_token":"tok","account":"a"}`))
		case "/v1/sync":
			var req protocol.SyncRequest
			json.NewDecoder(r.Body).Decode(&req)
			switch {
			case req.Wait > 0 && req.Version == 0: // the first sync: the session holds "me"
				json.NewEncoder(w).Encode(protocol.SyncResponse{Version: 1, Name: "me"})
			case req.Wait > 0: // a parked long-poll: hold it until the client gives up
				<-r.Context().Done()
			default: // an immediate sync, which the Refresh below makes
				mu.Lock()
				immediate++
				k := immediate
				lastAfter = req.After
				mu.Unlock()
				if k == 1 { // held in flight, then stale
					close(inflight)
					<-release
					from := "peer"
					json.NewEncoder(w).Encode(protocol.SyncResponse{Version: 2, Name: "me",
						Messages: []protocol.Message{{ID: 7, From: &from, Kind: protocol.KindText, Text: "doomed"}}})
					return
				}
				// the retry: the broker released the name and deleted the row
				json.NewEncoder(w).Encode(protocol.SyncResponse{Version: 3, Name: ""})
			}
		}
	}))
	sess := NewSession(testClient(t, ts.URL, pin))
	m := NewMirror(sess, MirrorConfig{SyncWait: 5 * time.Second, BackoffMin: 10 * time.Millisecond})
	defer m.Stop()
	if _, err := sess.Ensure(bg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	if _, err := m.WaitChange(ctx, func(s Snapshot) bool { return s.Name == "me" }); err != nil {
		t.Fatal("first sync never applied")
	}
	refreshed := make(chan error, 1)
	go func() { refreshed <- m.Refresh(bg) }()
	select {
	case <-inflight:
	case <-time.After(5 * time.Second):
		t.Fatal("the immediate sync never started")
	}
	m.DiscardPeerRows() // the release happened: this response is now stale
	close(release)
	select {
	case err := <-refreshed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Refresh never returned")
	}
	s := m.Snapshot()
	if len(s.Rows) != 0 {
		t.Fatalf("the stale response resurrected a deleted row: %+v", s.Rows)
	}
	if s.Name != "" {
		t.Fatalf("the stale response brought the old name back: %q", s.Name)
	}
	mu.Lock()
	defer mu.Unlock()
	if immediate < 2 {
		t.Fatalf("the stale response was not asked for again (%d immediate syncs)", immediate)
	}
	if lastAfter != 0 {
		t.Fatalf("the retry changed `after`: %d", lastAfter)
	}
}
