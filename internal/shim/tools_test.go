package shim

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Liz4v/agent-bus/internal/protocol"
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

func newShim(t *testing.T, tb *testBroker, host string) *shimEnv {
	t.Helper()
	sess := NewSession(tb.c)
	m := NewMirror(sess, fastCfg())
	tools := NewTools(sess, m, func() (string, error) { return host, nil }, nil)
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

var uuidRE = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)

// linked returns shims a ("hostA/alice") and b ("hostB/bob") joined to each other.
func linked(t *testing.T) (*testBroker, *shimEnv, *shimEnv) {
	t.Helper()
	tb := newTestBroker(t)
	a, b := newShim(t, tb, "hostA"), newShim(t, tb, "hostB")
	out := a.mustOK("create_channel", map[string]any{"my_name": "alice"})
	id := uuidRE.FindString(out)
	b.mustOK("join_channel", map[string]any{"channel_id": id, "my_name": "bob"})
	// both sides see the link
	for _, e := range []*shimEnv{a, b} {
		e.eventually(func(s Snapshot) bool { return len(s.Channels) == 1 && s.Channels[0].State == protocol.StateConnected })
	}
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

func TestHappyPathsAndChannelIDSetApart(t *testing.T) {
	tb := newTestBroker(t)
	a, b := newShim(t, tb, "hostA"), newShim(t, tb, "hostB")
	out := a.mustOK("create_channel", map[string]any{"my_name": "alice"})
	id := uuidRE.FindString(out)
	if id == "" || len(uuidRE.FindAllString(out, -1)) != 1 {
		t.Fatalf("channel ID not found exactly once: %s", out)
	}
	var line string
	for _, l := range strings.Split(out, "\n") {
		if l == id {
			line = l
		}
	}
	if line == "" || !strings.Contains(out, "CHANNEL ID") || !strings.Contains(out, "ch-") {
		t.Fatalf("channel ID is not on a line of its own, or unmarked: %s", out)
	}
	join := b.mustOK("join_channel", map[string]any{"channel_id": id, "my_name": "bob"})
	if !strings.Contains(join, "hostA/alice") {
		t.Fatalf("join result: %s", join)
	}
	if !strings.Contains(b.mustOK("peers", nil), "hostA/alice") {
		t.Fatal("peers lacks the peer")
	}
	a.eventually(func(s Snapshot) bool { return len(s.Channels) == 1 && s.Channels[0].State == protocol.StateConnected })
	if got := a.mustOK("peers", nil); !strings.Contains(got, "hostB/bob") || !strings.Contains(got, "connected") {
		t.Fatalf("creator's peers: %s", got)
	}
}

func TestInvalidMyNameAndBadArgs(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb, "hostA")
	for _, n := range []string{"", "bad name", "a/b", strings.Repeat("x", 65)} {
		if text, isErr := a.call("create_channel", map[string]any{"my_name": n}); !isErr || text == "" {
			t.Errorf("my_name %q accepted: %s", n, text)
		}
	}
	if _, isErr := a.call("join_channel", map[string]any{"channel_id": "", "my_name": "x"}); !isErr {
		t.Error("empty channel_id accepted")
	}
	if tb.totalHits() != 0 {
		t.Fatal("invalid arguments reached the broker")
	}
	_, b := tb, newShim(t, tb, "hostB")
	if text, isErr := b.call("join_channel", map[string]any{"channel_id": "00000000-0000-4000-8000-000000000000", "my_name": "bob"}); !isErr || !strings.Contains(text, "no such open channel") {
		t.Fatalf("bad channel id: %s", text)
	}
}

func TestSendRightAfterJoinByLabelAndHandle(t *testing.T) {
	tb := newTestBroker(t)
	a, b := newShim(t, tb, "hostA"), newShim(t, tb, "hostB")
	out := a.mustOK("create_channel", map[string]any{"my_name": "alice"})
	id := uuidRE.FindString(out)
	handle := regexp.MustCompile(`ch-[0-9a-f]+`).FindString(out)
	b.mustOK("join_channel", map[string]any{"channel_id": id, "my_name": "bob"})
	// no other call in between
	a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": "by label"})
	a.mustOK("send", map[string]any{"peer": handle, "text": "by handle"})
	text := b.mustOK("read", map[string]any{"wait_seconds": 5})
	b.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 || strings.Contains(text, "by handle") })
	text += b.mustOK("read", map[string]any{"wait_seconds": 1})
	if !strings.Contains(text, "by label") || !strings.Contains(text, "by handle") || !strings.Contains(text, Preamble) ||
		!strings.Contains(text, `from="hostA/alice"`) {
		t.Fatalf("read: %s", text)
	}
	// leave by handle
	bh := regexp.MustCompile(`handle (ch-[0-9a-f]+)`).FindStringSubmatch(b.mustOK("peers", nil))[1]
	b.mustOK("leave", map[string]any{"peer": bh})
	if text, isErr := a.call("send", map[string]any{"peer": "hostB/bob", "text": "x"}); !isErr || !strings.Contains(text, "left") {
		t.Fatalf("send after leave: %s", text)
	}
}

func TestReadCancelledBeforeCommitShowsRowsAgain(t *testing.T) {
	_, a, b := linked(t)
	b.drain()
	a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": "survivor"})
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
		a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": "msg-" + string(rune('A'+i))})
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
	a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": "wakeup"})
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
	a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": "taken elsewhere"})
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
	a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": "arrived before outage"})
	b.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 })
	tb.down.Store(true)
	text, isErr := b.call("read", nil)
	if isErr || !strings.Contains(text, "arrived before outage") || !strings.Contains(text, tb.ts.URL) || !strings.Contains(text, "unreachable") {
		t.Fatalf("read during outage: err=%v %s", isErr, text)
	}
	text, isErr = b.call("peers", nil)
	if !isErr || !strings.Contains(text, tb.ts.URL) || !strings.Contains(text, "stale") || !strings.Contains(text, "hostA/alice") {
		t.Fatalf("peers during outage: err=%v %s", isErr, text)
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
	a := newShim(t, tb, "hostA")
	start := time.Now()
	if text := a.mustOK("read", map[string]any{"wait_seconds": 30}); !strings.HasPrefix(text, "No new messages") || time.Since(start) > time.Second {
		t.Fatalf("unregistered read: %q after %v", text, time.Since(start))
	}
	if text := a.mustOK("peers", nil); !strings.Contains(text, "no channels") {
		t.Fatalf("peers: %s", text)
	}
	for tool, args := range map[string]map[string]any{
		"send":  {"peer": "x/y", "text": "t"},
		"leave": {"peer": "x/y"},
	} {
		text, isErr := a.call(tool, args)
		if !isErr || !strings.Contains(text, "no channels") {
			t.Fatalf("%s: err=%v %s", tool, isErr, text)
		}
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
	if strings.Contains(strings.ToLower(text), "unauthorized") || !strings.Contains(text, "expired") {
		t.Fatalf("read: %q", text)
	}

	tb2, _, b2 := linked(t)
	tb2.kill(b2.sess.Token())
	text, _ = b2.call("send", map[string]any{"peer": "hostA/alice", "text": "x"})
	if strings.Contains(strings.ToLower(text), "unauthorized") || !strings.Contains(text, "expired") || !strings.Contains(text, "no channels") {
		t.Fatalf("send: %q", text)
	}
}

func TestCreateFailureAfterRegistrationReusesSession(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb, "hostA")
	tb.failChannels.Store(1)
	if text, isErr := a.call("create_channel", map[string]any{"my_name": "alice"}); !isErr || !strings.Contains(text, tb.ts.URL) {
		t.Fatalf("first create: err=%v %s", isErr, text)
	}
	a.mustOK("create_channel", map[string]any{"my_name": "alice"})
	if tb.sessions() != 1 {
		t.Fatalf("%d broker sessions, want 1", tb.sessions())
	}
}

func TestWaitSecondsCappedAndReported(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb, "hostA")
	a.mustOK("create_channel", map[string]any{"my_name": "alice"})
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
	a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": "hi there"})
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
		a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": chunk})
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

func TestCreateAgainstUnreachableBrokerCreatesNoState(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb, "hostA")
	tb.down.Store(true)
	text, isErr := a.call("create_channel", map[string]any{"my_name": "alice"})
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
	text := b.mustOK("peers", nil)
	if !strings.Contains(text, "expired") || !strings.Contains(text, "no channels") {
		t.Fatalf("first result after the drop: %q", text)
	}
	if text := b.mustOK("peers", nil); strings.Contains(text, "expired") {
		t.Fatalf("notice repeated: %q", text)
	}
}

func TestExpiredChannelShownAndSendReportsExpiry(t *testing.T) {
	tb := newTestBroker(t)
	a := newShim(t, tb, "hostA")
	out := a.mustOK("create_channel", map[string]any{"my_name": "alice"})
	handle := regexp.MustCompile(`ch-[0-9a-f]+`).FindString(out)
	tb.clk.Advance(protocol.ChannelTTL + time.Second)
	if err := a.m.Refresh(bg); err != nil { // keeps a's session alive at the new time
		t.Fatal(err)
	}
	if err := tb.sweeper.Sweep(bg); err != nil {
		t.Fatal(err)
	}
	a.eventually(func(s Snapshot) bool { return len(s.Channels) == 1 && s.Channels[0].State == protocol.StateExpired })
	if text := a.mustOK("peers", nil); !strings.Contains(text, "expired") {
		t.Fatalf("peers: %s", text)
	}
	if text, isErr := a.call("send", map[string]any{"peer": handle, "text": "x"}); !isErr || !strings.Contains(text, "expired") {
		t.Fatalf("send: %s", text)
	}
	read := a.mustOK("read", map[string]any{"wait_seconds": 2})
	if !strings.Contains(read, `kind="channel_expired"`) || !strings.Contains(read, `from="agent-bus"`) {
		t.Fatalf("read: %s", read)
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
	a := newShim(t, tb, "hostA")
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
	_, a, b := linked(t)
	_ = b
	text, isErr := a.call("send", map[string]any{"peer": "hostZ/nobody", "text": "x"})
	if !isErr || !strings.Contains(text, "Known peers: hostB/bob [handle ch-") || !strings.Contains(text, "connected]") {
		t.Fatalf("unknown peer: %v %s", isErr, text)
	}
	text, isErr = a.call("send", map[string]any{"peer": "hostB/bob", "text": ""})
	if !isErr || !strings.Contains(text, "empty") {
		t.Fatalf("empty: %v %s", isErr, text)
	}
	text, isErr = a.call("send", map[string]any{"peer": "hostB/bob", "text": strings.Repeat("x", protocol.MaxMessageBytes+1)})
	if !isErr || !strings.Contains(text, "65536") || !strings.Contains(text, "limit") {
		t.Fatalf("too large: %v %s", isErr, text)
	}
	// the exact limit still goes through
	a.mustOK("send", map[string]any{"peer": "hostB/bob", "text": strings.Repeat("x", protocol.MaxMessageBytes)})
}
