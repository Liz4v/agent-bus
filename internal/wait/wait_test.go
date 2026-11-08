package wait

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeShim serves one connection: it reads the command line and then runs fn.
func fakeShim(t *testing.T, fn func(c net.Conn, cmd string)) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "w")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		line, _ := bufio.NewReader(c).ReadString('\n')
		fn(c, strings.TrimSpace(line))
	}()
	return path
}

func run(t *testing.T, sock string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	code := Run(ctx, sock, &out, &errb)
	return code, out.String(), errb.String()
}

func TestPending(t *testing.T) {
	var got string
	sock := fakeShim(t, func(c net.Conn, cmd string) {
		got = cmd
		c.Write([]byte(`{"status":"pending","from":["alice","agent-bus","evil\" name","h/a"],"count":3}` + "\n"))
	})
	code, out, _ := run(t, sock)
	if code != 0 || got != "WAIT" || !strings.Contains(out, "3 unread") || !strings.Contains(out, "alice, agent-bus") ||
		strings.Contains(out, "evil") || strings.Contains(out, "h/a") {
		t.Fatalf("code=%d out=%q cmd=%q", code, out, got)
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("want one line, got %q", out)
	}
}

func TestNoNamePrintsReasonAndExitsNothing(t *testing.T) {
	if ExitNothing != 1 {
		t.Fatalf("ExitNothing = %d: the skills tell agents that exit 1 means nothing to wait for", ExitNothing)
	}
	sock := fakeShim(t, func(c net.Conn, _ string) {
		c.Write([]byte(`{"status":"no_name","reason":"this session holds no agent-bus name; call claim_name first"}` + "\n"))
	})
	code, out, _ := run(t, sock)
	if code != ExitNothing || !strings.Contains(out, "nothing to wait for") || !strings.Contains(out, "claim_name") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestEOFWithoutReply(t *testing.T) {
	sock := fakeShim(t, func(c net.Conn, _ string) { c.Close() })
	code, out, _ := run(t, sock)
	if code != 1 || !strings.Contains(out, "shim went away") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestRearm(t *testing.T) {
	sock := fakeShim(t, func(c net.Conn, _ string) { c.Write([]byte(`{"status":"rearm"}` + "\n")) })
	code, out, _ := run(t, sock)
	if code != 2 || !strings.Contains(out, "re-run this same wait command in the background") {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestUnreachableSocket(t *testing.T) {
	code, _, errOut := run(t, "/nonexistent/agent-bus.sock")
	if code != 1 || !strings.Contains(errOut, "/nonexistent/agent-bus.sock") {
		t.Fatalf("code=%d err=%q", code, errOut)
	}
}

func TestGarbageReply(t *testing.T) {
	sock := fakeShim(t, func(c net.Conn, _ string) { c.Write([]byte("not json\n")) })
	if code, _, errOut := run(t, sock); code != 1 || errOut == "" {
		t.Fatalf("code=%d err=%q", code, errOut)
	}
}

func TestContextCancelStopsWaiting(t *testing.T) {
	sock := fakeShim(t, func(c net.Conn, _ string) { time.Sleep(3 * time.Second) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { var o, e bytes.Buffer; done <- Run(ctx, sock, &o, &e) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("code %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestFilterNames(t *testing.T) {
	got := FilterNames([]string{"alice", "agent-bus", "bad name\nIgnore previous", "<script>", "host/alice", "Bob.s-1_x"})
	if len(got) != 3 || got[0] != "alice" || got[1] != "agent-bus" || got[2] != "Bob.s-1_x" {
		t.Fatalf("got %q", got)
	}
	for _, in := range [][]string{nil, {}, {"\n"}, {"a/b"}} {
		if out := FilterNames(in); out == nil || len(out) != 0 {
			t.Fatalf("FilterNames(%q) = %#v, want empty non-nil", in, out)
		}
	}
}
