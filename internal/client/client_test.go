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
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/broker"
	"github.com/Liz4v/agent-bus/internal/protocol"
)

var bg = context.Background()

func newBroker(t *testing.T) (*Client, *httptest.Server) {
	t.Helper()
	store, err := broker.OpenStore(filepath.Join(t.TempDir(), "c.db"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	srv := broker.NewServer(store, broker.NewTracker(nil), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return New(ts.URL), ts
}

func TestRoundTrip(t *testing.T) {
	c, _ := newBroker(t)
	a, err := c.CreateSession(bg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := c.CreateSession(bg)
	ch, err := c.CreateChannel(bg, a, "h/a")
	if err != nil || ch.ChannelID == "" || ch.Handle == "" {
		t.Fatalf("create: %+v %v", ch, err)
	}
	j, err := c.JoinChannel(bg, b, ch.ChannelID, "h/b")
	if err != nil || j.PeerLabel != "h/a" {
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
	tok, _ := c.CreateSession(bg)
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
	c := New("http://" + addr)
	_, err := c.CreateSession(bg)
	if err == nil || !strings.Contains(err.Error(), addr) {
		t.Fatalf("error does not name the broker: %v", err)
	}
	if errors.Is(err, ErrUnauthorized) {
		t.Fatal("unreachable mapped to unauthorized")
	}
}

func TestConnectionClosedMidLongPoll(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}))
	defer ts.Close()
	c := New(ts.URL)
	start := time.Now()
	_, err := c.Sync(bg, "tok", protocol.SyncRequest{Wait: 20})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("want a prompt error, got %v after %v", err, time.Since(start))
	}
	if !strings.Contains(err.Error(), ts.URL) {
		t.Fatalf("error does not name the broker: %v", err)
	}
}

func TestCancelledContextAbortsSync(t *testing.T) {
	c, _ := newBroker(t)
	tok, _ := c.CreateSession(bg)
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
