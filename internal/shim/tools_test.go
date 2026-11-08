package shim

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Liz4v/agent-bus/internal/brokerfile"
	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/testutil"
)

// shimEnv is one complete shim (session, mirror, tools) connected to an MCP
// client over the in-memory transport.
type shimEnv struct {
	t     *testing.T
	tb    *testBroker
	sess  *Session
	m     *Mirror
	tools *Tools
	cs    *mcp.ClientSession
	done  chan error
}

func newShim(t *testing.T, tb *testBroker) *shimEnv {
	t.Helper()
	return newShimWith(t, tb, tb.c, nil, false)
}

// newShimWith builds a shim around a specific client, setup error and
// hostmaster hint.
func newShimWith(t *testing.T, tb *testBroker, c *client.Client, setupErr error, hostmaster bool) *shimEnv {
	t.Helper()
	sess := NewSession(c)
	m := NewMirror(sess, fastCfg())
	tools := NewTools(sess, m, nil, setupErr, hostmaster)
	srv := tools.NewMCPServer("test")
	t1, t2 := mcp.NewInMemoryTransports()
	e := &shimEnv{t: t, tb: tb, sess: sess, m: m, tools: tools, done: make(chan error, 1)}
	go func() { e.done <- tools.Run(bg, srv, t1) }()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil).Connect(bg, t2, nil)
	if err != nil {
		t.Fatal(err)
	}
	e.cs = cs
	t.Cleanup(func() { cs.Close(); m.Stop() })
	return e
}

func (e *shimEnv) call(name string, args map[string]any) (string, bool) {
	e.t.Helper()
	return e.callCtx(bg, name, args)
}

func (e *shimEnv) callCtx(ctx context.Context, name string, args map[string]any) (string, bool) {
	e.t.Helper()
	res, err := e.cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		e.t.Fatalf("%s: protocol error: %v", name, err)
	}
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(tc.Text)
		}
	}
	return sb.String(), res.IsError
}

func (e *shimEnv) mustOK(name string, args map[string]any) string {
	e.t.Helper()
	text, isErr := e.call(name, args)
	if isErr {
		e.t.Fatalf("%s failed: %s", name, text)
	}
	return text
}

// linked returns shims a ("alice") and b ("bob") that hold their names and are
// partners: alice has said hello, which is unread on bob's side.
func linked(t *testing.T) (*testBroker, *shimEnv, *shimEnv) {
	t.Helper()
	tb := newTestBroker(t)
	a, b := newShim(t, tb), newShim(t, tb)
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	b.mustOK("claim_name", map[string]any{"name": "bob"})
	a.mustOK("send", map[string]any{"peer": "bob", "text": "hello"})
	a.eventually(func(s Snapshot) bool { return s.Name == "alice" })
	b.eventually(func(s Snapshot) bool { return s.Name == "bob" && len(s.Rows) == 1 })
	return tb, a, b
}

func (e *shimEnv) eventually(pred func(Snapshot) bool) Snapshot {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	s, err := e.m.WaitChange(ctx, pred)
	if err != nil {
		e.t.Fatalf("condition not reached: %+v", s)
	}
	return s
}

// drain reads everything pending so later assertions see only new rows.
func (e *shimEnv) drain() {
	e.t.Helper()
	for {
		if text, _ := e.call("read", nil); strings.HasPrefix(text, "No new messages") {
			return
		}
	}
}

func TestClaimAndSendHappyPath(t *testing.T) {
	tb := newTestBroker(t)
	a, b := newShim(t, tb), newShim(t, tb)
	out := a.mustOK("claim_name", map[string]any{"name": "alice"})
	if !strings.Contains(out, "alice") || !strings.Contains(out, "wake_command") || !strings.Contains(out, "knows the name") {
		t.Fatalf("claim result: %s", out)
	}
	for _, gone := range []string{"CHANNEL ID", "channel", "ch-"} {
		if strings.Contains(out, gone) {
			t.Fatalf("claim result still talks about %q: %s", gone, out)
		}
	}
	b.mustOK("claim_name", map[string]any{"name": "bob"})
	a.eventually(func(s Snapshot) bool { return s.Name == "alice" })
	a.mustOK("send", map[string]any{"peer": "bob", "text": "hello bob"})
	got := b.mustOK("read", map[string]any{"wait_seconds": 5})
	for _, want := range []string{Preamble, `from="alice"`, `account="shared"`, `first-contact="true"`, FirstContactNote, "hello bob"} {
		if !strings.Contains(got, want) {
			t.Errorf("read lacks %q: %s", want, got)
		}
	}
	if strings.Contains(got, "handle=") {
		t.Fatalf("a handle is still shown: %s", got)
	}
	// the reply is not first contact
	b.mustOK("send", map[string]any{"peer": "alice", "text": "hello alice"})
	reply := a.mustOK("read", map[string]any{"wait_seconds": 5})
	if !strings.Contains(reply, "hello alice") || strings.Contains(reply, "first-contact") {
		t.Fatalf("reply read: %s", reply)
	}
}

func TestInvalidNamesAndBadArgs(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb)
	for _, n := range []string{"", "bad name", "a/b", strings.Repeat("x", 65), "admin", "Agent-Bus"} {
		if text, isErr := a.call("claim_name", map[string]any{"name": n}); !isErr || text == "" {
			t.Errorf("name %q accepted: %s", n, text)
		}
	}
	if tb.totalHits() != 0 {
		t.Fatal("invalid arguments reached the broker")
	}
	// Refusals from the broker are rendered as they are.
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	b := newShim(t, tb)
	if text, isErr := b.call("claim_name", map[string]any{"name": "ALICE"}); !isErr || !strings.Contains(text, "already in use") {
		t.Fatalf("taken name: %v %s", isErr, text)
	}
	if text, isErr := a.call("claim_name", map[string]any{"name": "carol"}); !isErr || !strings.Contains(text, "already holds the name") || !strings.Contains(text, "alice") {
		t.Fatalf("second name: %v %s", isErr, text)
	}
	// claiming the held name again is fine
	if text := a.mustOK("claim_name", map[string]any{"name": "Alice"}); !strings.Contains(text, "alice") {
		t.Fatalf("re-claim: %s", text)
	}
}

func TestSendByNameInAnyCaseThenPeerReleases(t *testing.T) {
	tb := newTestBroker(t)
	a, b := newShim(t, tb), newShim(t, tb)
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	b.mustOK("claim_name", map[string]any{"name": "bob"})
	// no other call in between
	a.mustOK("send", map[string]any{"peer": "bob", "text": "by name"})
	a.mustOK("send", map[string]any{"peer": "BOB", "text": "by name in capitals"})
	text := b.mustOK("read", map[string]any{"wait_seconds": 5})
	b.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 || strings.Contains(text, "capitals") })
	text += b.mustOK("read", map[string]any{"wait_seconds": 1})
	if !strings.Contains(text, "by name") || !strings.Contains(text, "capitals") || !strings.Contains(text, Preamble) ||
		!strings.Contains(text, `from="alice"`) {
		t.Fatalf("read: %s", text)
	}
	// bob releases: alice's next send says so
	b.mustOK("release_name", nil)
	if text, isErr := a.call("send", map[string]any{"peer": "bob", "text": "x"}); !isErr || !strings.Contains(text, "released its name") {
		t.Fatalf("send after release: %s", text)
	}
}

func TestReadCancelledBeforeCommitShowsRowsAgain(t *testing.T) {
	_, a, b := linked(t)
	b.drain()
	a.mustOK("send", map[string]any{"peer": "bob", "text": "survivor"})
	b.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 })
	ctx, cancel := context.WithCancel(bg)
	b.tools.beforeCommit = func() { cancel() }
	res, _, _ := b.tools.read(ctx, nil, readArgs{})
	b.tools.beforeCommit = nil
	if !res.IsError {
		t.Fatal("cancelled read reported success")
	}
	text := b.mustOK("read", nil)
	if !strings.Contains(text, "survivor") {
		t.Fatalf("rows lost by a cancelled read: %s", text)
	}
}

func TestConcurrentReadsNeverShowARowTwice(t *testing.T) {
	_, a, b := linked(t)
	b.drain()
	const n = 12
	for i := 0; i < n; i++ {
		a.mustOK("send", map[string]any{"peer": "bob", "text": "msg-" + string(rune('A'+i))})
	}
	b.eventually(func(s Snapshot) bool { return len(s.Rows) >= n })
	var wg sync.WaitGroup
	outs := make([]string, 6)
	for i := range outs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); outs[i], _ = b.call("read", nil) }(i)
	}
	wg.Wait()
	all := strings.Join(outs, "\n")
	for i := 0; i < n; i++ {
		if c := strings.Count(all, "msg-"+string(rune('A'+i))+"<"); c != 1 {
			t.Fatalf("msg-%c shown %d times", 'A'+i, c)
		}
	}
}

func TestReadZeroNotBlockedByLongWait(t *testing.T) {
	_, a, b := linked(t)
	b.drain()
	long := make(chan string, 1)
	go func() { text, _ := b.call("read", map[string]any{"wait_seconds": 30}); long <- text }()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	if text, _ := b.call("read", map[string]any{"wait_seconds": 0}); !strings.HasPrefix(text, "No new messages") || time.Since(start) > time.Second {
		t.Fatalf("read(0) = %q after %v", text, time.Since(start))
	}
	a.mustOK("send", map[string]any{"peer": "bob", "text": "wakeup"})
	select {
	case text := <-long:
		if !strings.Contains(text, "wakeup") {
			t.Fatalf("long read returned %q", text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting read not woken by the send")
	}
}

func TestWaitingReadKeepsWaitingWhenAnotherReadTookTheRows(t *testing.T) {
	_, a, b := linked(t)
	b.drain()
	// Park the waiting read right after its wait ends, before it takes the
	// render mutex, so a real read(0) can take the rows first.
	woke := make(chan struct{})
	release := make(chan struct{})
	var parked atomic.Bool
	b.tools.afterWait = func() {
		if parked.CompareAndSwap(false, true) { // only the first (waiting) read parks
			close(woke)
			<-release
		}
	}
	long := make(chan string, 1)
	go func() { text, _ := b.call("read", map[string]any{"wait_seconds": 3}); long <- text }()
	time.Sleep(300 * time.Millisecond)
	a.mustOK("send", map[string]any{"peer": "bob", "text": "taken elsewhere"})
	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatal("waiting read never woke")
	}
	if text := b.mustOK("read", map[string]any{"wait_seconds": 0}); !strings.Contains(text, "taken elsewhere") {
		t.Fatalf("read(0) did not take the rows: %q", text)
	}
	start := time.Now()
	close(release)
	select {
	case text := <-long:
		if !strings.HasPrefix(text, "No new messages") || strings.Contains(text, "taken elsewhere") {
			t.Fatalf("waiting read returned %q", text)
		}
		if time.Since(start) < 1000*time.Millisecond {
			t.Fatalf("waiting read gave up after %v instead of waiting out its deadline", time.Since(start))
		}
	case <-time.After(6 * time.Second):
		t.Fatal("waiting read never returned")
	}
}

func TestBrokerStoppedReadShowsMirrorRowsAndPeersErrors(t *testing.T) {
	tb, a, b := linked(t)
	b.drain()
	a.mustOK("send", map[string]any{"peer": "bob", "text": "arrived before outage"})
	b.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 })
	tb.down.Store(true)
	text, isErr := b.call("read", nil)
	if isErr || !strings.Contains(text, "arrived before outage") || !strings.Contains(text, tb.ts.URL) || !strings.Contains(text, "unreachable") {
		t.Fatalf("read during outage: err=%v %s", isErr, text)
	}
	// and a read with no rows at all returns the broker error
	text, isErr = b.call("read", nil)
	if !isErr || !strings.Contains(text, tb.ts.URL) {
		t.Fatalf("empty read during outage: err=%v %s", isErr, text)
	}
	tb.down.Store(false)
}

func TestUnregisteredToolsAnswerLocally(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb)
	start := time.Now()
	if text := a.mustOK("read", map[string]any{"wait_seconds": 30}); !strings.HasPrefix(text, "No new messages") || time.Since(start) > time.Second {
		t.Fatalf("unregistered read: %q after %v", text, time.Since(start))
	}
	text, isErr := a.call("send", map[string]any{"peer": "bob", "text": "t"})
	if !isErr || !strings.Contains(text, "claim_name") {
		t.Fatalf("send: err=%v %s", isErr, text)
	}
	text, isErr = a.call("release_name", nil)
	if isErr || !strings.Contains(text, "You hold no name") {
		t.Fatalf("release_name: err=%v %s", isErr, text)
	}
	a.call("wake_command", nil)
	if tb.totalHits() != 0 || tb.sessions() != 0 {
		t.Fatalf("an unregistered shim contacted the broker (%d requests)", tb.totalHits())
	}
}

func TestWaitingReadReturnsPromptlyWhenSessionDropped(t *testing.T) {
	tb, _, b := linked(t)
	b.drain()
	res := make(chan string, 1)
	go func() { text, _ := b.call("read", map[string]any{"wait_seconds": 60}); res <- text }()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	tb.kill(b.sess.Token())
	select {
	case text := <-res:
		if !strings.Contains(text, "expired") || time.Since(start) > 5*time.Second {
			t.Fatalf("after %v: %q", time.Since(start), text)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("read(60) still waiting after the session was dropped")
	}
}

func TestToolsHittingA401ReturnNoticeNotUnauthorized(t *testing.T) {
	tb, _, b := linked(t)
	tb.kill(b.sess.Token())
	text, _ := b.call("read", map[string]any{"wait_seconds": 1})
	if strings.Contains(strings.ToLower(text), "unauthorized") || !strings.Contains(text, "expired") || !strings.Contains(text, "the name bob was released") {
		t.Fatalf("read: %q", text)
	}

	tb2, _, b2 := linked(t)
	tb2.kill(b2.sess.Token())
	text, _ = b2.call("send", map[string]any{"peer": "alice", "text": "x"})
	if strings.Contains(strings.ToLower(text), "unauthorized") || !strings.Contains(text, "expired") || !strings.Contains(text, "claim_name") {
		t.Fatalf("send: %q", text)
	}

	tb3, _, b3 := linked(t)
	tb3.kill(b3.sess.Token())
	text, isErr := b3.call("release_name", nil)
	if isErr || strings.Contains(strings.ToLower(text), "unauthorized") || !strings.Contains(text, "expired") || !strings.Contains(text, "You hold no name") {
		t.Fatalf("release_name: err=%v %q", isErr, text)
	}
}

func TestClaimFailureAfterRegistrationReusesSession(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb)
	tb.failName.Store(1)
	if text, isErr := a.call("claim_name", map[string]any{"name": "alice"}); !isErr || !strings.Contains(text, tb.ts.URL) {
		t.Fatalf("first claim: err=%v %s", isErr, text)
	}
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	if tb.sessions() != 1 {
		t.Fatalf("%d broker sessions, want 1", tb.sessions())
	}
}

func TestWaitSecondsCappedAndReported(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb)
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	a.drain()
	a.tools.maxWait = time.Second
	start := time.Now()
	text := a.mustOK("read", map[string]any{"wait_seconds": 1000})
	if d := time.Since(start); d < 800*time.Millisecond || d > 4*time.Second || !strings.Contains(text, "capped") {
		t.Fatalf("read(1000) after %v: %q", d, text)
	}
}

func TestReadWaitReturnsWhenPeerSends(t *testing.T) {
	_, a, b := linked(t)
	b.drain()
	res := make(chan string, 1)
	go func() { text, _ := b.call("read", map[string]any{"wait_seconds": 30}); res <- text }()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	a.mustOK("send", map[string]any{"peer": "bob", "text": "hi there"})
	select {
	case text := <-res:
		if !strings.Contains(text, "hi there") || time.Since(start) > 3*time.Second {
			t.Fatalf("after %v: %q", time.Since(start), text)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("read did not return")
	}
}

func TestReadPageCutAt256KiB(t *testing.T) {
	_, a, b := linked(t)
	b.drain()
	chunk := strings.Repeat("p", 60<<10)
	for i := 0; i < 6; i++ {
		a.mustOK("send", map[string]any{"peer": "bob", "text": chunk})
	}
	b.eventually(func(s Snapshot) bool { return len(s.Rows) >= 6 })
	first := b.mustOK("read", nil)
	if n := strings.Count(first, "<peer-message"); n < 1 || n >= 6 || !strings.Contains(first, "more unread") {
		t.Fatalf("first page has %d rows: %.200s", n, first)
	}
	if len(first) > protocol.ReadPageBytes+4096 {
		t.Fatalf("page is %d bytes", len(first))
	}
	rest := b.mustOK("read", nil)
	if n1, n2 := strings.Count(first, "<peer-message"), strings.Count(rest, "<peer-message"); n1+n2 != 6 {
		t.Fatalf("pages show %d + %d rows, want 6", n1, n2)
	}
}

func TestClaimAgainstUnreachableBrokerCreatesNoState(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb)
	tb.down.Store(true)
	text, isErr := a.call("claim_name", map[string]any{"name": "alice"})
	if !isErr || !strings.Contains(text, tb.ts.URL) {
		t.Fatalf("err=%v %s", isErr, text)
	}
	if a.sess.Token() != "" || a.m.Snapshot().Running {
		t.Fatal("local state recorded for a failed registration")
	}
}

func TestForced401NoticeAndEmptyPeers(t *testing.T) {
	tb, _, b := linked(t)
	tb.kill(b.sess.Token())
	b.eventually(func(s Snapshot) bool { return !s.Running })
	text := b.mustOK("read", nil)
	if !strings.Contains(text, "expired") {
		t.Fatalf("first result after the drop: %q", text)
	}
	if text := b.mustOK("read", nil); strings.Contains(text, "expired") {
		t.Fatalf("notice repeated: %q", text)
	}
}

func TestPeerSessionEndShownAndSendReportsPeerLeft(t *testing.T) {
	tb, a, b := linked(t)
	a.drain()
	tb.kill(b.sess.Token()) // bob's session ends, as a reap would
	a.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerLeft) == 1 })
	if text, isErr := a.call("send", map[string]any{"peer": "bob", "text": "x"}); !isErr || !strings.Contains(text, "released its name") {
		t.Fatalf("send: %s", text)
	}
	read := a.mustOK("read", map[string]any{"wait_seconds": 2})
	for _, want := range []string{`kind="peer_left"`, `from="bob"`, "released its name or its session ended", "ask the user before messaging that name again"} {
		if !strings.Contains(read, want) {
			t.Errorf("read lacks %q: %s", want, read)
		}
	}
}

func TestReleaseNameWithAndWithoutAName(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb)
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	a.eventually(func(s Snapshot) bool { return s.Name == "alice" })
	text := a.mustOK("release_name", nil)
	if !strings.Contains(text, "Released alice") || !strings.Contains(text, "partners were told") {
		t.Fatalf("release: %s", text)
	}
	a.eventually(func(s Snapshot) bool { return s.Name == "" })
	text, isErr := a.call("release_name", nil)
	if isErr || !strings.Contains(text, "You hold no name") {
		t.Fatalf("second release: err=%v %s", isErr, text)
	}
	// the name is free again, and so is this session
	a.mustOK("claim_name", map[string]any{"name": "alice2"})
}

func TestReleaseDiscardsUnreadPeerRowsFromTheMirror(t *testing.T) {
	_, a, b := linked(t) // bob holds alice's unread "hello"
	w, err := StartWake(b.m, b.sess, WakeOptions{RuntimeDir: shortTempDir(t), Debounce: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if r := waitReply(t, w.Path(), 3*time.Second); r.Status != protocol.WaitPending {
		t.Fatalf("before the release: %+v", r)
	}
	b.mustOK("release_name", nil)
	// The unread rows are gone from read, and a WAIT has nothing to wait for.
	if text := b.mustOK("read", nil); !strings.HasPrefix(text, "No new messages") || strings.Contains(text, "hello") {
		t.Fatalf("read after release: %s", text)
	}
	if r := waitReply(t, w.Path(), 3*time.Second); r.Status != protocol.WaitNoName {
		t.Fatalf("wait after release: %+v", r)
	}
	// alice was told
	a.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerLeft) == 1 })
}

func TestSendWithATokenButNoNameGetsTheShimsOwnText(t *testing.T) {
	_, _, b := linked(t)
	b.mustOK("release_name", nil)
	text, isErr := b.call("send", map[string]any{"peer": "alice", "text": "x"})
	if !isErr || !strings.Contains(text, "claim_name") {
		t.Fatalf("send: err=%v %s", isErr, text)
	}
	// The broker's own wording has an upgrade hint for old shims; a new shim
	// must not show it.
	if strings.Contains(text, "older than the broker") || strings.Contains(text, "must be upgraded") {
		t.Fatalf("a new shim told the agent to upgrade: %s", text)
	}
}

func TestOldBrokerWithoutNamesGetsTheUpgradeText(t *testing.T) {
	ts, pin := testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sessions" {
			w.WriteHeader(201)
			w.Write([]byte(`{"session_token":"tok","account":"a"}`))
			return
		}
		http.NotFound(w, r) // an older mux: a plain-text 404
	}))
	tb := newTestBroker(t)
	e := newShimWith(t, tb, testClient(t, ts.URL, pin), nil, false)
	text, isErr := e.call("claim_name", map[string]any{"name": "alice"})
	if !isErr || !strings.Contains(text, "older than this agent-bus") || !strings.Contains(text, "must be upgraded") || !strings.Contains(text, ts.URL) {
		t.Fatalf("claim against an old broker: err=%v %s", isErr, text)
	}
}

func TestClosingTransportMakesPeerReadPeerLeftAtOnce(t *testing.T) {
	tb, a, b := linked(t)
	a.drain()
	_ = tb
	a.cs.Close()
	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the transport closed")
	}
	text := b.mustOK("read", map[string]any{"wait_seconds": 5})
	if !strings.Contains(text, `kind="peer_left"`) || strings.Contains(text, "peer_disconnected") {
		t.Fatalf("peer read: %s", text)
	}
}

type fakeWake struct {
	path  string
	armed bool
}

func (f fakeWake) Path() string { return f.path }
func (f fakeWake) Armed() bool  { return f.armed }

func TestWakeCommandHappyPath(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb)
	a.tools.wake = fakeWake{path: "/run/user/1000/agent-bus/abc.sock", armed: false}
	a.tools.SetExecutable(func() (string, error) { return "/nix/store/x-agent-bus/bin/agent-bus", nil })
	text := a.mustOK("wake_command", nil)
	want := "/nix/store/x-agent-bus/bin/agent-bus wait --socket /run/user/1000/agent-bus/abc.sock"
	if !strings.Contains(text, "\n"+want) || !strings.Contains(text, "run_in_background") || !strings.Contains(text, "Wake not armed") {
		t.Fatalf("not armed: %s", text)
	}
	a.tools.wake = fakeWake{path: "/run/user/1000/agent-bus/abc.sock", armed: true}
	if text := a.mustOK("wake_command", nil); strings.Contains(text, "Wake not armed") || !strings.Contains(text, want) {
		t.Fatalf("armed: %s", text)
	}
	// paths with spaces or quotes are shell-quoted
	a.tools.SetExecutable(func() (string, error) { return "/opt/my tools/agent-bus", nil })
	if text := a.mustOK("wake_command", nil); !strings.Contains(text, "'/opt/my tools/agent-bus' wait --socket") {
		t.Fatalf("quoting: %s", text)
	}
	if got := shellQuote("it's"); got != `'it'\''s'` {
		t.Fatalf("shellQuote = %s", got)
	}
	// no wake socket at all
	a.tools.wake = nil
	if text, isErr := a.call("wake_command", nil); !isErr || !strings.Contains(text, "not available") {
		t.Fatalf("no socket: %v %s", isErr, text)
	}
	if tb.totalHits() != 0 {
		t.Fatal("wake_command contacted the broker")
	}
}

func TestSendErrorsRenderedForTheAgent(t *testing.T) {
	_, a, _ := linked(t)
	text, isErr := a.call("send", map[string]any{"peer": "nobody", "text": "x"})
	if !isErr || !strings.Contains(text, `no session called "nobody"`) || strings.Contains(text, "Known peers") {
		t.Fatalf("unknown peer: %v %s", isErr, text)
	}
	text, isErr = a.call("send", map[string]any{"peer": "ALICE", "text": "x"})
	if !isErr || !strings.Contains(text, "your own name") {
		t.Fatalf("self send: %v %s", isErr, text)
	}
	text, isErr = a.call("send", map[string]any{"peer": "a/b", "text": "x"})
	if !isErr || !strings.Contains(text, "letters, digits") {
		t.Fatalf("bad name: %v %s", isErr, text)
	}
	text, isErr = a.call("send", map[string]any{"peer": "bob", "text": ""})
	if !isErr || !strings.Contains(text, "empty") {
		t.Fatalf("empty: %v %s", isErr, text)
	}
	text, isErr = a.call("send", map[string]any{"peer": "bob", "text": strings.Repeat("x", protocol.MaxMessageBytes+1)})
	if !isErr || !strings.Contains(text, "65536") || !strings.Contains(text, "limit") {
		t.Fatalf("too large: %v %s", isErr, text)
	}
	// the exact limit still goes through
	a.mustOK("send", map[string]any{"peer": "bob", "text": strings.Repeat("x", protocol.MaxMessageBytes)})
}

func toolNames(t *testing.T, e *shimEnv) map[string]bool {
	t.Helper()
	res, err := e.cs.ListTools(bg, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, tl := range res.Tools {
		out[tl.Name] = true
	}
	return out
}

var adminToolNames = []string{"admin_list_accounts", "admin_approve", "admin_reject", "admin_remove", "admin_set_max_accounts", "admin_subscribe"}

func TestAdminToolsOnlyWithHostmasterHint(t *testing.T) {
	tb := newTestBroker(t)
	plain := toolNames(t, newShimWith(t, tb, tb.c, nil, false))
	host := toolNames(t, newShimWith(t, tb, tb.newClient(tb.bossCred), nil, true))
	for _, n := range []string{"claim_name", "send", "read", "release_name", "wake_command"} {
		if !plain[n] || !host[n] {
			t.Errorf("tool %s missing (plain %v, host %v)", n, plain[n], host[n])
		}
	}
	for _, n := range []string{"create_channel", "join_channel", "leave"} {
		if plain[n] || host[n] {
			t.Errorf("the channel tool %s still exists", n)
		}
	}
	for _, n := range adminToolNames {
		if plain[n] {
			t.Errorf("admin tool %s visible without the hostmaster hint", n)
		}
		if !host[n] {
			t.Errorf("admin tool %s missing for the hostmaster", n)
		}
	}
	if len(plain) != 5 || len(host) != 11 {
		t.Errorf("tool counts: plain %d, host %d", len(plain), len(host))
	}
}

func TestEveryToolReturnsTheSetupErrorWhenUnconfigured(t *testing.T) {
	tb := newTestBroker(t)
	_, _, setupErr := CheckSetup("broker-host:7700", t.TempDir())
	if setupErr == nil {
		t.Fatal("no setup error")
	}
	e := newShimWith(t, tb, nil, setupErr, true) // even with the hint set
	calls := map[string]map[string]any{
		"claim_name": {"name": "alice"}, "release_name": nil,
		"send": {"peer": "bob", "text": "hi"}, "read": nil, "wake_command": nil,
		"admin_list_accounts": nil, "admin_approve": {"name": "x"}, "admin_reject": {"name": "x"},
		"admin_remove": {"name": "x"}, "admin_set_max_accounts": {"max": 3}, "admin_subscribe": {"subscribed": true},
	}
	for name, args := range calls {
		text, isErr := e.call(name, args)
		if !isErr || text != setupErr.Error() {
			t.Errorf("%s: %v %q", name, isErr, text)
		}
	}
	for _, want := range []string{"agent-bus register --broker https://broker-host:7700", "restart this Claude Code / Hermes session", "Do not run the command yourself"} {
		if !strings.Contains(setupErr.Error(), want) {
			t.Errorf("setup message lacks %q: %s", want, setupErr)
		}
	}
	if tb.totalHits() != 0 {
		t.Fatal("an unconfigured shim contacted the broker")
	}
}

func TestCheckSetupCases(t *testing.T) {
	dir := t.TempDir()
	check := func(url string) (client.Config, bool, string) {
		cfg, hm, err := CheckSetup(url, dir)
		if err != nil {
			var se *SetupError
			if !errors.As(err, &se) {
				t.Fatalf("not a SetupError: %T", err)
			}
			return cfg, hm, err.Error()
		}
		return cfg, hm, ""
	}
	if _, _, msg := check(""); !strings.Contains(msg, "no broker URL") {
		t.Errorf("empty URL: %s", msg)
	}
	if _, _, msg := check("http://broker-host:7700"); !strings.Contains(msg, "https://") || !strings.Contains(msg, "plain http://") {
		t.Errorf("http URL: %s", msg)
	}
	if _, _, msg := check("broker-host"); !strings.Contains(msg, "not set up") || !strings.Contains(msg, "register") {
		t.Errorf("missing file: %s", msg)
	}
	brokerfile.Save(dir, brokerfile.File{URL: "broker-host"})
	if _, _, msg := check("broker-host"); !strings.Contains(msg, "not pinned") || !strings.Contains(msg, "agent-bus pin") {
		t.Errorf("unpinned: %s", msg)
	}
	brokerfile.Save(dir, brokerfile.File{URL: "broker-host", Fingerprint: "SHA256:x"})
	if _, _, msg := check("broker-host"); !strings.Contains(msg, "no account") {
		t.Errorf("no credential: %s", msg)
	}
	brokerfile.Save(dir, brokerfile.File{URL: "broker-host", Fingerprint: "SHA256:x", Account: "a", Credential: "c", Hostmaster: true})
	cfg, hm, msg := check("Broker-Host:7700")
	if msg != "" || !hm || cfg.Pin != "SHA256:x" || cfg.Credential != "c" || cfg.BaseURL != "https://broker-host:7700" {
		t.Errorf("good file: %+v %v %q", cfg, hm, msg)
	}
}

func TestPinMismatchMessageNamesBothFingerprintsAndForbidsSelfRepair(t *testing.T) {
	tb := newTestBroker(t)
	const wrong = "SHA256:WRONGWRONGWRONGWRONGWRONGWRONGWRONGWRONGWR"
	c, err := client.New(client.Config{BaseURL: tb.ts.URL, Pin: wrong, Credential: "c"})
	if err != nil {
		t.Fatal(err)
	}
	e := newShimWith(t, tb, c, nil, false)
	before := tb.totalHits()
	text, isErr := e.call("claim_name", map[string]any{"name": "alice"})
	if !isErr {
		t.Fatalf("pin mismatch accepted: %s", text)
	}
	for _, want := range []string{wrong, tb.pin, "agent-bus unpin", "agent-bus pin", "restart this Claude Code / Hermes session", "Do not run those commands yourself"} {
		if !strings.Contains(text, want) {
			t.Errorf("message lacks %q: %s", want, text)
		}
	}
	if tb.totalHits() != before {
		t.Fatal("a request reached the broker through a wrong pin")
	}
}

func TestAccountsAreShownOnMessagesAndFirstContactOnlyOnce(t *testing.T) {
	tb := newTestBroker(t)
	a := newShimWith(t, tb, tb.newClient(tb.user("alicia")), nil, false)
	b := newShimWith(t, tb, tb.newClient(tb.user("bobby")), nil, false)
	a.mustOK("claim_name", map[string]any{"name": "alice"})
	b.mustOK("claim_name", map[string]any{"name": "bob"})
	b.mustOK("send", map[string]any{"peer": "alice", "text": "hello"})
	a.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 })
	got := a.mustOK("read", nil)
	if !strings.Contains(got, `account="bobby"`) || !strings.Contains(got, "hello") || !strings.Contains(got, `first-contact="true"`) {
		t.Fatalf("first read: %s", got)
	}
	b.mustOK("send", map[string]any{"peer": "alice", "text": "again"})
	a.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 })
	got = a.mustOK("read", nil)
	if !strings.Contains(got, `account="bobby"`) || !strings.Contains(got, "again") || strings.Contains(got, "first-contact") {
		t.Fatalf("second read: %s", got)
	}
	a.mustOK("send", map[string]any{"peer": "bob", "text": "reply"})
	b.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 })
	got = b.mustOK("read", nil)
	if !strings.Contains(got, `account="alicia"`) || strings.Contains(got, "first-contact") {
		t.Fatalf("reply read: %s", got)
	}
}
