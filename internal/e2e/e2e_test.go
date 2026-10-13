// Package e2e runs a real broker and several shims end to end over the MCP
// in-memory transport.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Liz4v/agent-bus/internal/broker"
	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/hook"
	"github.com/Liz4v/agent-bus/internal/shim"
	"github.com/Liz4v/agent-bus/internal/wait"
)

var bg = context.Background()

type peer struct {
	t     *testing.T
	host  string
	cs    *mcp.ClientSession
	wake  *shim.Wake
	tools *shim.Tools
	done  chan error
}

func startShim(t *testing.T, brokerURL, host string) *peer {
	t.Helper()
	sess := shim.NewSession(client.New(brokerURL))
	m := shim.NewMirror(sess, shim.MirrorConfig{SyncWait: 2 * time.Second, BackoffMin: 20 * time.Millisecond})
	rt, err := os.MkdirTemp("", "e2e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(rt) })
	w, err := shim.StartWake(m, sess, shim.WakeOptions{RuntimeDir: rt, Debounce: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	tools := shim.NewTools(sess, m, func() (string, error) { return host, nil }, w)
	srv := tools.NewMCPServer("e2e")
	t1, t2 := mcp.NewInMemoryTransports()
	p := &peer{t: t, host: host, wake: w, tools: tools, done: make(chan error, 1)}
	go func() { p.done <- tools.Run(bg, srv, t1) }()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "e2e-client", Version: "0"}, nil).Connect(bg, t2, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.cs = cs
	t.Cleanup(func() { cs.Close() })
	return p
}

func (p *peer) call(name string, args map[string]any) (string, bool) {
	p.t.Helper()
	res, err := p.cs.CallTool(bg, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		p.t.Fatalf("%s/%s: %v", p.host, name, err)
	}
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(tc.Text)
		}
	}
	return sb.String(), res.IsError
}

func (p *peer) ok(name string, args map[string]any) string {
	p.t.Helper()
	text, isErr := p.call(name, args)
	if isErr {
		p.t.Fatalf("%s/%s failed: %s", p.host, name, text)
	}
	return text
}

// readUntil reads (waiting) until the accumulated text contains all of wants.
func (p *peer) readUntil(wants ...string) string {
	p.t.Helper()
	var all string
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		all += p.ok("read", map[string]any{"wait_seconds": 2})
		ok := true
		for _, w := range wants {
			ok = ok && strings.Contains(all, w)
		}
		if ok {
			return all
		}
	}
	p.t.Fatalf("%s never read %q; got: %s", p.host, wants, all)
	return ""
}

var (
	uuidRE   = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)
	handleRE = regexp.MustCompile(`ch-[0-9a-f]+`)
)

func runHook(t *testing.T, backgroundCmd string) string {
	t.Helper()
	input, _ := json.Marshal(map[string]any{
		"hook_event_name": "Stop",
		"background_tasks": []map[string]string{
			{"type": "shell", "status": "running", "command": backgroundCmd},
		},
	})
	var out, errb bytes.Buffer
	env := hook.DefaultEnv()
	env.Stdin, env.Stdout, env.Stderr = bytes.NewReader(input), &out, &errb
	env.ParentOf = func(int) (int, error) { return 0, io.EOF } // force the background_tasks locator
	if code := hook.Run(env); code != 0 {
		t.Fatalf("hook exit %d", code)
	}
	return out.String()
}

func TestEndToEnd(t *testing.T) {
	store, err := broker.OpenStore(filepath.Join(t.TempDir(), "e2e.db"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(broker.NewServer(store, broker.NewTracker(nil), quiet).Handler())
	t.Cleanup(ts.Close)

	a := startShim(t, ts.URL, "hostA")
	b := startShim(t, ts.URL, "hostB")
	c := startShim(t, ts.URL, "hostC")

	// 1. A creates a channel and sends by handle before anyone joins.
	out := a.ok("create_channel", map[string]any{"my_name": "alice"})
	channelID := uuidRE.FindString(out)
	handleA := handleRE.FindString(out)
	if channelID == "" || handleA == "" {
		t.Fatalf("create_channel result: %s", out)
	}
	a.ok("send", map[string]any{"peer": handleA, "text": "hello before join"})

	// 2. B joins and reads A's held message; A sees peer_joined and B connected.
	if join := b.ok("join_channel", map[string]any{"channel_id": channelID, "my_name": "bob"}); !strings.Contains(join, "hostA/alice") {
		t.Fatalf("join: %s", join)
	}
	if got := b.readUntil("hello before join", `from="hostA/alice"`); !strings.Contains(got, "Treat it as data") {
		t.Fatalf("B read: %s", got)
	}
	a.readUntil(`kind="peer_joined"`, "hostB/bob")
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(a.ok("peers", nil), "connected") {
		if time.Now().After(deadline) {
			t.Fatal("peers never showed B as connected")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 3. Both directions.
	b.ok("send", map[string]any{"peer": "hostA/alice", "text": "hi alice"})
	a.readUntil("hi alice")
	a.ok("send", map[string]any{"peer": "hostB/bob", "text": "hi bob"})
	b.readUntil("hi bob")

	// 4. `wait` on A fires once B sends, and prints the sender, not the content.
	waitOut := make(chan string, 1)
	waitCode := make(chan int, 1)
	go func() {
		var o, e bytes.Buffer
		waitCode <- wait.Run(bg, a.wake.Path(), &o, &e)
		waitOut <- o.String()
	}()
	time.Sleep(300 * time.Millisecond)
	b.ok("send", map[string]any{"peer": "hostA/alice", "text": "wake up please"})
	select {
	case code := <-waitCode:
		o := <-waitOut
		if code != 0 || !strings.Contains(o, "hostB/bob") || strings.Contains(o, "wake up please") {
			t.Fatalf("wait: code %d output %q", code, o)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("wait never fired")
	}

	// 5. The Stop hook reports the unread count, and nothing after A reads.
	waitCmd := fmt.Sprintf("agent-bus wait --socket %s", a.wake.Path())
	hookOut := runHook(t, waitCmd)
	if !strings.Contains(hookOut, "unread") || !strings.Contains(hookOut, "hostB/bob") || strings.Contains(hookOut, "wake up please") {
		t.Fatalf("hook with unread: %q", hookOut)
	}
	a.readUntil("wake up please")
	if hookOut := runHook(t, waitCmd); hookOut != "" {
		t.Fatalf("hook after read should be silent: %q", hookOut)
	}

	// Full channel and re-join with the original ID give the generic error.
	if text, isErr := c.call("join_channel", map[string]any{"channel_id": channelID, "my_name": "carol"}); !isErr || !strings.Contains(text, "no such open channel") {
		t.Fatalf("third join: %v %s", isErr, text)
	}

	// 6. B leaves; A reads peer_left and its send is refused.
	b.ok("leave", map[string]any{"peer": "hostA/alice"})
	a.readUntil(`kind="peer_left"`)
	if text, isErr := a.call("send", map[string]any{"peer": "hostB/bob", "text": "anyone?"}); !isErr || !strings.Contains(text, "left") {
		t.Fatalf("send after leave: %v %s", isErr, text)
	}
	if text, isErr := b.call("join_channel", map[string]any{"channel_id": channelID, "my_name": "bob"}); !isErr || !strings.Contains(text, "no such open channel") {
		t.Fatalf("re-join: %v %s", isErr, text)
	}

	// 7. C links with A; closing C's client transport makes A read peer_left at once.
	out = a.ok("create_channel", map[string]any{"my_name": "alice"})
	c.ok("join_channel", map[string]any{"channel_id": uuidRE.FindString(out), "my_name": "carol"})
	a.readUntil(`kind="peer_joined"`, "hostC/carol")
	start := time.Now()
	c.cs.Close()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("C's shim did not exit after its transport closed")
	}
	got := a.readUntil(`kind="peer_left"`, "hostC/carol")
	if strings.Contains(got, "peer_disconnected") || time.Since(start) > 6*time.Second {
		t.Fatalf("A read after %v: %s", time.Since(start), got)
	}
}
