package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/broker"
	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/testutil"
)

var bg = context.Background()

// testBroker is a real broker handler on a TLS test server.
type testBroker struct {
	store *broker.Store
	ts    *httptest.Server
	pin   string
	hits  atomic.Int64
}

func startBroker(t *testing.T) *testBroker {
	t.Helper()
	store, err := broker.OpenStore(filepath.Join(t.TempDir(), "c.db"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	srv := broker.NewServer(store, broker.NewTracker(nil), slog.New(slog.NewTextHandler(io.Discard, nil)))
	tb := &testBroker{store: store}
	h := srv.Handler()
	tb.ts, tb.pin = testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tb.hits.Add(1)
		h.ServeHTTP(w, r)
	}))
	return tb
}

// client returns a pinned client with the given credential.
func (tb *testBroker) client(t *testing.T, cred string) *Client {
	t.Helper()
	c, err := New(Config{BaseURL: tb.ts.URL, Pin: tb.pin, Credential: cred})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

// hostmaster claims the broker and returns the hostmaster credential.
func (tb *testBroker) hostmaster(t *testing.T, name string) string {
	t.Helper()
	code, _, err := tb.store.IssueClaim(bg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := tb.store.Register(bg, name, code)
	if err != nil {
		t.Fatal(err)
	}
	return res.Credential
}

// user registers and approves an account, returning its credential.
func (tb *testBroker) user(t *testing.T, name string) string {
	t.Helper()
	res, err := tb.store.Register(bg, name, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tb.store.Approve(bg, name); err != nil {
		t.Fatal(err)
	}
	return res.Credential
}

// newBroker starts a broker with a hostmaster and one active user, and returns
// a client for the user.
func newBroker(t *testing.T) (*Client, *testBroker) {
	t.Helper()
	tb := startBroker(t)
	tb.hostmaster(t, "boss")
	return tb.client(t, tb.user(t, "alice")), tb
}

// session creates another active user and opens a session for it.
func session(t *testing.T, tb *testBroker, name string) string {
	t.Helper()
	tok, _, err := tb.client(t, tb.user(t, name)).CreateSession(bg)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestRoundTrip(t *testing.T) {
	c, tb := newBroker(t)
	a, account, err := c.CreateSession(bg)
	if err != nil || account != "alice" {
		t.Fatalf("create session: %q %v", account, err)
	}
	b := session(t, tb, "bob")
	ch, err := c.CreateChannel(bg, a, "h/a")
	if err != nil || ch.ChannelID == "" || ch.Handle == "" {
		t.Fatalf("create: %+v %v", ch, err)
	}
	j, err := c.JoinChannel(bg, b, ch.ChannelID, "h/b")
	if err != nil || j.PeerLabel != "h/a" || j.PeerAccount != "alice" {
		t.Fatalf("join: %+v %v", j, err)
	}
	if err := c.Send(bg, a, "h/b", "hello"); err != nil {
		t.Fatal(err)
	}
	r, err := c.Sync(bg, b, protocol.SyncRequest{})
	if err != nil || len(r.Messages) != 1 || r.Messages[0].Text != "hello" || len(r.Channels) != 1 {
		t.Fatalf("sync: %+v %v", r, err)
	}
	// typed errors
	err = c.Send(bg, a, "h/nobody", "x")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != protocol.CodeUnknownPeer || len(ae.KnownPeers) != 1 {
		t.Fatalf("unknown peer: %v", err)
	}
	if err := c.Leave(bg, a, "h/b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(bg, b, "h/a", "x"); !errors.As(err, &ae) || ae.Code != protocol.CodePeerLeft {
		t.Fatalf("peer left: %v", err)
	}
	if err := c.DeleteSelf(bg, a); err != nil {
		t.Fatal(err)
	}
}

func TestUnauthorizedMapping(t *testing.T) {
	c, _ := newBroker(t)
	tok, _, _ := c.CreateSession(bg)
	if err := c.DeleteSelf(bg, tok); err != nil {
		t.Fatal(err)
	}
	_, err := c.Sync(bg, tok, protocol.SyncRequest{})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("got %v, want ErrUnauthorized", err)
	}
	if err := c.Leave(bg, "garbage", "h/x"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("got %v, want ErrUnauthorized", err)
	}
}

func TestUnreachableBrokerNamesAddress(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	c, err := New(Config{BaseURL: "https://" + addr, Pin: "SHA256:x", Credential: "c"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.CreateSession(bg)
	if err == nil || !strings.Contains(err.Error(), addr) {
		t.Fatalf("error does not name the broker: %v", err)
	}
	if errors.Is(err, ErrUnauthorized) {
		t.Fatal("unreachable mapped to unauthorized")
	}
}

func TestConnectionClosedMidLongPoll(t *testing.T) {
	ts, pin := testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}))
	c, err := New(Config{BaseURL: ts.URL, Pin: pin})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.Sync(bg, "tok", protocol.SyncRequest{Wait: 20})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("want a prompt error, got %v after %v", err, time.Since(start))
	}
	if !strings.Contains(err.Error(), c.BaseURL()) {
		t.Fatalf("error does not name the broker: %v", err)
	}
}

func TestCancelledContextAbortsSync(t *testing.T) {
	c, _ := newBroker(t)
	tok, _, _ := c.CreateSession(bg)
	first, _ := c.Sync(bg, tok, protocol.SyncRequest{})
	ctx, cancel := context.WithCancel(bg)
	errc := make(chan error, 1)
	go func() {
		_, err := c.Sync(ctx, tok, protocol.SyncRequest{Version: first.Version, Wait: 20})
		errc <- err
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled sync did not return")
	}
}
