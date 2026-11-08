package shim

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/broker"
	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/testutil"
)

var bg = context.Background()

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// testBroker is an in-process broker that can be switched off and counts requests.
type testBroker struct {
	t            *testing.T
	store        *broker.Store
	ts           *httptest.Server
	pin          string
	c            *client.Client // a client for the default active account "shared"
	bossCred     string
	down         atomic.Bool
	failSessions atomic.Bool  // POST /v1/sessions answers 503 (counted)
	failName     atomic.Int32 // number of upcoming POST /v1/name requests to drop
	delay        atomic.Int64 // nanoseconds slept after each /v1/sync is handled, before the handler returns

	clk     *fakeClock
	tracker *broker.Tracker
	sweeper *broker.Sweeper

	mu   sync.Mutex
	hits map[string]int
}

func newTestBroker(t *testing.T) *testBroker {
	t.Helper()
	clk := &fakeClock{t: time.Now()}
	store, err := broker.OpenStore(filepath.Join(t.TempDir(), "shim.db"), nil, clk.Now)
	if err != nil {
		t.Fatal(err)
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store.SetLogger(quiet)
	tracker := broker.NewTracker(clk.Now)
	tb := &testBroker{t: t, store: store, hits: map[string]int{}, clk: clk, tracker: tracker,
		sweeper: broker.NewSweeper(store, tracker, quiet)}
	inner := broker.NewServer(store, tracker, quiet).Handler()
	tb.ts, tb.pin = testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tb.down.Load() {
			panic(http.ErrAbortHandler) // drops the connection
		}
		if r.URL.Path == "/v1/sessions" && tb.failSessions.Load() {
			tb.mu.Lock()
			tb.hits[r.Method+" "+r.URL.Path]++
			tb.mu.Unlock()
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/v1/name" && r.Method == "POST" && tb.failName.Add(-1) >= 0 {
			panic(http.ErrAbortHandler)
		} else if r.URL.Path == "/v1/name" && r.Method == "POST" {
			tb.failName.Store(0)
		}
		tb.mu.Lock()
		tb.hits[r.Method+" "+r.URL.Path]++
		tb.mu.Unlock()
		inner.ServeHTTP(w, r)
		if d := time.Duration(tb.delay.Load()); d > 0 && r.URL.Path == "/v1/sync" {
			time.Sleep(d)
		}
	}))
	tb.bossCred = tb.claim("boss")
	tb.c = tb.newClient(tb.user("shared"))
	t.Cleanup(func() { store.Close() })
	return tb
}

// claim makes name the hostmaster and returns its credential.
func (tb *testBroker) claim(name string) string {
	tb.t.Helper()
	code, _, err := tb.store.IssueClaim(bg)
	if err != nil {
		tb.t.Fatal(err)
	}
	res, err := tb.store.Register(bg, name, code)
	if err != nil {
		tb.t.Fatal(err)
	}
	return res.Credential
}

// user registers and approves an account and returns its credential.
func (tb *testBroker) user(name string) string {
	tb.t.Helper()
	res, err := tb.store.Register(bg, name, "")
	if err != nil {
		tb.t.Fatal(err)
	}
	if _, err := tb.store.Approve(bg, name); err != nil {
		tb.t.Fatal(err)
	}
	return res.Credential
}

// newClient returns a pinned client for the given credential.
func (tb *testBroker) newClient(cred string) *client.Client {
	tb.t.Helper()
	c, err := client.New(client.Config{BaseURL: tb.ts.URL, Pin: tb.pin, Credential: cred})
	if err != nil {
		tb.t.Fatal(err)
	}
	tb.t.Cleanup(c.CloseIdleConnections)
	return c
}

func (tb *testBroker) hitCount(key string) int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.hits[key]
}

func (tb *testBroker) totalHits() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	n := 0
	for _, v := range tb.hits {
		n += v
	}
	return n
}

func (tb *testBroker) sessions() int { return tb.hitCount("POST /v1/sessions") }

// kill deletes a session on the broker side, as a reap would, so the next
// request carrying its token gets 401.
func (tb *testBroker) kill(token string) {
	tb.t.Helper()
	id, err := tb.store.SessionByToken(bg, token)
	if err != nil {
		tb.t.Fatal(err)
	}
	if err := tb.store.DeleteSelf(bg, id); err != nil {
		tb.t.Fatal(err)
	}
}

func TestEnsureRegistersOnceUnderConcurrency(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	var registered atomic.Int32
	s.SetCallbacks(func() { registered.Add(1) }, nil, nil)
	var wg sync.WaitGroup
	toks := make([]string, 12)
	for i := range toks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			if toks[i], err = s.Ensure(bg); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if tb.sessions() != 1 || registered.Load() != 1 {
		t.Fatalf("sessions=%d onRegister=%d", tb.sessions(), registered.Load())
	}
	for _, tk := range toks {
		if tk != toks[0] || tk == "" {
			t.Fatal("callers got different tokens")
		}
	}
}

func TestConcurrentDropClearsOnceAndSetsNoticeOnce(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	var drops atomic.Int32
	s.SetCallbacks(nil, func() DropReport {
		drops.Add(1)
		return DropReport{HadState: true, Name: "alice", UnreadCount: 2, UnreadLabels: []string{"bob", "bob"}}
	}, nil)
	tok, _ := s.Ensure(bg)
	var wg sync.WaitGroup
	var won atomic.Int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Drop(tok) {
				won.Add(1)
			}
		}()
	}
	wg.Wait()
	if won.Load() != 1 || drops.Load() != 1 || s.Token() != "" {
		t.Fatalf("won=%d drops=%d token=%q", won.Load(), drops.Load(), s.Token())
	}
	n := s.TakeNotice()
	if !strings.Contains(n, "expired") || !strings.Contains(n, "2 unread") || !strings.Contains(n, "bob") ||
		strings.Count(n, "bob") != 1 || !strings.Contains(n, "the name alice was released") || !strings.Contains(n, "claim_name") {
		t.Fatalf("notice: %q", n)
	}
	if s.TakeNotice() != "" {
		t.Fatal("notice not consumed")
	}
}

func TestDropWithoutStateSetsNoNotice(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	s.SetCallbacks(nil, func() DropReport { return DropReport{} }, nil)
	tok, _ := s.Ensure(bg)
	if !s.Drop(tok) {
		t.Fatal("drop did nothing")
	}
	if s.PeekNotice() != "" {
		t.Fatal("notice set for a session that never had state")
	}
}

func TestPeekNoticeDoesNotConsume(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	s.SetCallbacks(nil, func() DropReport { return DropReport{HadState: true} }, nil)
	tok, _ := s.Ensure(bg)
	s.Drop(tok)
	a, b := s.PeekNotice(), s.PeekNotice()
	if a == "" || a != b {
		t.Fatal("peek consumed the notice")
	}
	if s.TakeNotice() != a || s.PeekNotice() != "" {
		t.Fatal("take did not consume")
	}
}

func TestDropWithOldTokenAfterNewRegistrationDoesNothing(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	var drops atomic.Int32
	s.SetCallbacks(nil, func() DropReport { drops.Add(1); return DropReport{HadState: true} }, nil)
	old, _ := s.Ensure(bg)
	s.Drop(old)
	s.TakeNotice()
	fresh, _ := s.Ensure(bg)
	if fresh == old {
		t.Fatal("same token")
	}
	if s.Drop(old) || drops.Load() != 1 || s.Token() != fresh || s.PeekNotice() != "" {
		t.Fatal("stale drop affected the new session")
	}
}

func TestCloseMakesPeerReadPeerLeftImmediately(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	tokA, _ := s.Ensure(bg)
	tokB, _, _ := tb.c.CreateSession(bg)
	if _, err := tb.c.ClaimName(bg, tokA, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := tb.c.ClaimName(bg, tokB, "b"); err != nil {
		t.Fatal(err)
	}
	if err := tb.c.Send(bg, tokA, "b", "hello"); err != nil { // they are partners now
		t.Fatal(err)
	}
	s.Close(bg)
	r, err := tb.c.Sync(bg, tokB, protocol.SyncRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Messages) != 2 || r.Messages[1].Kind != protocol.KindPeerLeft || r.Messages[1].From == nil || *r.Messages[1].From != "a" {
		t.Fatalf("peer saw %+v", r)
	}
	if s.Token() != "" {
		t.Fatal("token kept after close")
	}
}

func TestCloseUnreachableBrokerReturnsQuickly(t *testing.T) {
	release := make(chan struct{})
	ts, pin := testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(201)
			w.Write([]byte(`{"session_token":"tok","account":"a"}`))
			return
		}
		<-release // DELETE hangs
	}))
	defer close(release)
	s := NewSession(testClient(t, ts.URL, pin))
	if _, err := s.Ensure(bg); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	s.Close(bg)
	if d := time.Since(start); d > 4*time.Second || d < time.Second {
		t.Fatalf("Close took %v, want about 2s", d)
	}
}

func TestCloseWithNoTokenMakesNoRequest(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	s.Close(bg)
	if tb.totalHits() != 0 {
		t.Fatalf("%d requests made", tb.totalHits())
	}
}

// testClient returns a pinned client with a dummy credential, for scripted
// servers that do not check it.
func testClient(t *testing.T, url, pin string) *client.Client {
	t.Helper()
	c, err := client.New(client.Config{BaseURL: url, Pin: pin, Credential: "test-credential"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func TestEnsureAccountErrors(t *testing.T) {
	tb := newTestBroker(t)
	anon := tb.newClient("")
	// No credential at all, and an unknown one: a 401 from POST /v1/sessions
	// is an AccountError, never a bare client.ErrUnauthorized.
	for _, c := range []*client.Client{anon, tb.newClient("not-a-credential")} {
		s := NewSession(c)
		_, err := s.Ensure(bg)
		var acc *AccountError
		if !errors.As(err, &acc) || acc.Code != CodeUnknownCredential || errors.Is(err, client.ErrUnauthorized) {
			t.Fatalf("got %T %v", err, err)
		}
		if !strings.Contains(err.Error(), "agent-bus status") || !strings.Contains(err.Error(), "do not run it yourself") {
			t.Fatalf("not human-directed: %v", err)
		}
	}
	// 403s for each account state.
	pend, err := tb.store.Register(bg, "waiting", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, code, text string
		prep             func()
	}{
		{"waiting", protocol.CodeAccountPending, "waiting for the hostmaster", func() {}},
	} {
		c.prep()
		_, err := NewSession(tb.newClient(pend.Credential)).Ensure(bg)
		var acc *AccountError
		if !errors.As(err, &acc) || acc.Code != c.code || !strings.Contains(err.Error(), c.text) {
			t.Fatalf("%s: %v", c.name, err)
		}
	}
	tb.store.Reject(bg, "waiting")
	_, err = NewSession(tb.newClient(pend.Credential)).Ensure(bg)
	var acc *AccountError
	if !errors.As(err, &acc) || acc.Code != protocol.CodeAccountRejected {
		t.Fatalf("rejected: %v", err)
	}
	rem := tb.user("goner")
	tb.store.Remove(bg, "goner")
	_, err = NewSession(tb.newClient(rem)).Ensure(bg)
	if !errors.As(err, &acc) || acc.Code != protocol.CodeAccountRemoved || !strings.Contains(err.Error(), "removed by the hostmaster") {
		t.Fatalf("removed: %v", err)
	}
	// A network error is still a plain error.
	tb.down.Store(true)
	_, err = NewSession(tb.c).Ensure(bg)
	if err == nil || errors.As(err, &acc) {
		t.Fatalf("network error became %v", err)
	}
}

func TestDropWithNoticeAppends(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	m := NewMirror(s, fastCfg())
	t.Cleanup(m.Stop)
	tok, err := s.Ensure(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tb.c.ClaimName(bg, tok, "me"); err != nil {
		t.Fatal(err)
	}
	m.Refresh(bg)
	if !s.DropWithNotice(tok, "First notice.") {
		t.Fatal("drop did not happen")
	}
	if s.DropWithNotice(tok, "ignored") {
		t.Fatal("a stale token dropped the session")
	}
	s.AppendNotice("Second notice.")
	n := s.TakeNotice()
	if !strings.HasPrefix(n, "First notice.") || !strings.HasSuffix(n, "Second notice.") {
		t.Fatalf("notices not appended: %q", n)
	}
	if s.TakeNotice() != "" {
		t.Fatal("notice not cleared")
	}
	// A normal Drop after a pending notice appends too.
	tok, _ = s.Ensure(bg)
	tb.c.ClaimName(bg, tok, "me2")
	m.Refresh(bg)
	s.AppendNotice("Pending.")
	s.Drop(tok)
	n = s.TakeNotice()
	if !strings.HasPrefix(n, "Pending.") || !strings.Contains(n, "expired while this host was disconnected") || !strings.Contains(n, "the name me2 was released") {
		t.Fatalf("Drop overwrote the pending notice: %q", n)
	}
}

func TestKeeperHookOnlyWhenWantAdmin(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	var calls atomic.Int32
	s.SetKeeperHooks(func() { calls.Add(1) }, nil)
	tok, _ := s.Ensure(bg)
	s.Drop(tok)
	if calls.Load() != 0 {
		t.Fatal("keeper hook ran without wantAdmin")
	}
	s.SetWantAdmin(true)
	tok, _ = s.Ensure(bg)
	s.Drop(tok)
	if calls.Load() != 1 {
		t.Fatalf("keeper hook ran %d times, want 1", calls.Load())
	}
	// A drop of a stale token does nothing and does not call the hook.
	s.Drop("stale")
	if calls.Load() != 1 {
		t.Fatal("hook ran for a no-op drop")
	}
}

func TestSessionCloseRunsCloseHookFirst(t *testing.T) {
	tb := newTestBroker(t)
	s := NewSession(tb.c)
	var order []string
	s.SetKeeperHooks(nil, func() { order = append(order, "hook") })
	s.Ensure(bg)
	s.Close(bg)
	if len(order) != 1 || s.Token() != "" {
		t.Fatalf("order %v token %q", order, s.Token())
	}
}
