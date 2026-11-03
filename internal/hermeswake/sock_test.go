package hermeswake

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// fakeShim serves one reply per connection: it reads the command line, then
// calls handle, which writes whatever it likes.
func fakeShim(t *testing.T, handle func(cmd string, c net.Conn)) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hw")
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
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				line, err := bufio.NewReader(c).ReadString('\n')
				if err != nil {
					return
				}
				handle(line[:len(line)-1], c)
			}()
		}
	}()
	return path
}

func reply(c net.Conn, v any) {
	b, _ := json.Marshal(v)
	c.Write(append(b, '\n'))
}

func TestWaitReturnsEachStatus(t *testing.T) {
	for _, want := range []protocol.WaitReply{
		{Status: protocol.WaitPending, Count: 2, From: []string{"h/a"}},
		{Status: protocol.WaitNoChannels, Reason: "none"},
		{Status: protocol.WaitRearm},
	} {
		path := fakeShim(t, func(cmd string, c net.Conn) {
			if cmd != protocol.WakeCmdWait {
				t.Errorf("command %q", cmd)
			}
			reply(c, want)
		})
		got, err := Wait(context.Background(), path)
		if err != nil || got.Status != want.Status || got.Count != want.Count || got.Reason != want.Reason {
			t.Fatalf("want %+v, got %+v, %v", want, got, err)
		}
	}
}

func TestStatusRoundTrips(t *testing.T) {
	want := protocol.StatusReply{Count: 3, From: []string{"h/a", "h/b"}, Cursor: 42}
	path := fakeShim(t, func(cmd string, c net.Conn) {
		if cmd != protocol.WakeCmdStatus {
			t.Errorf("command %q", cmd)
		}
		reply(c, want)
	})
	got, err := Status(context.Background(), path)
	if err != nil || got.Count != 3 || got.Cursor != 42 || len(got.From) != 2 || got.From[1] != "h/b" {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestConnectionRefusedAndMissingSocket(t *testing.T) {
	dir := t.TempDir()
	if _, err := Status(context.Background(), filepath.Join(dir, "none.sock")); err == nil {
		t.Fatal("missing socket gave no error")
	}
	path := filepath.Join(dir, "d.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	ln.SetUnlinkOnClose(false)
	ln.Close()
	if _, err := Wait(context.Background(), path); err == nil {
		t.Fatal("refused connection gave no error")
	}
}

func TestEOFBeforeReplyIsShimGone(t *testing.T) {
	path := fakeShim(t, func(string, net.Conn) {})
	_, err := Wait(context.Background(), path)
	if !errors.Is(err, errShimGone) {
		t.Fatalf("got %v", err)
	}
}

func TestGarbageReplyIsError(t *testing.T) {
	path := fakeShim(t, func(_ string, c net.Conn) { c.Write([]byte("not json\n")) })
	if _, err := Status(context.Background(), path); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestContextCancelClosesConnection(t *testing.T) {
	path := fakeShim(t, func(_ string, c net.Conn) { time.Sleep(5 * time.Second) })
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := Wait(ctx, path)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("cancel took %v", time.Since(start))
	}
}
