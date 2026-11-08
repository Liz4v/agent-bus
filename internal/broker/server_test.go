package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/identity"
	"github.com/Liz4v/agent-bus/internal/protocol"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type httpEnv struct {
	t    *testing.T
	ts   *httptest.Server
	s    *Store
	logs *syncBuffer
	tr   *Tracker
}

func newHTTPEnv(t *testing.T) *httpEnv {
	s, clk := openTestStore(t)
	logs := &syncBuffer{}
	tr := NewTracker(clk.Now)
	srv := NewServer(s, tr, slog.New(slog.NewTextHandler(logs, nil)))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &httpEnv{t: t, ts: ts, s: s, logs: logs, tr: tr}
}

func (e *httpEnv) do(token, method, path string, body any) (int, []byte) {
	e.t.Helper()
	var rd io.Reader
	if raw, ok := body.(string); ok {
		rd = strings.NewReader(raw)
	} else if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.ts.URL+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

var httpSeq atomic.Int64

// credential creates an active account and returns its credential.
func (e *httpEnv) credential() string {
	e.t.Helper()
	_, cred := addAccount(e.t, e.s, fmt.Sprintf("http%d", httpSeq.Add(1)), protocol.AccountActive, false)
	return cred
}

func (e *httpEnv) session() string {
	e.t.Helper()
	code, b := e.do(e.credential(), "POST", "/v1/sessions", struct{}{})
	if code != 201 {
		e.t.Fatalf("create session: %d %s", code, b)
	}
	var r protocol.CreateSessionResponse
	json.Unmarshal(b, &r)
	return r.SessionToken
}

func errCode(b []byte) string {
	var e protocol.ErrorBody
	json.Unmarshal(b, &e)
	return e.Error.Code
}

func (e *httpEnv) claim(tok, name string) (int, []byte) {
	return e.do(tok, "POST", "/v1/name", protocol.ClaimNameRequest{Name: name})
}

// claimOK claims name for the session and fails the test unless it is granted.
func (e *httpEnv) claimOK(tok, name string) {
	e.t.Helper()
	code, b := e.claim(tok, name)
	if code != 200 {
		e.t.Fatalf("claim %q: %d %s", name, code, b)
	}
}

func (e *httpEnv) send(tok, to, text string) (int, []byte) {
	return e.do(tok, "POST", "/v1/messages", protocol.SendRequest{To: to, Text: text})
}
func (e *httpEnv) sync(tok string, req protocol.SyncRequest) (int, protocol.SyncResponse, []byte) {
	code, b := e.do(tok, "POST", "/v1/sync", req)
	var r protocol.SyncResponse
	if code == 200 {
		json.Unmarshal(b, &r)
	}
	return code, r, b
}

func TestRoutesSuccessAndErrors(t *testing.T) {
	e := newHTTPEnv(t)
	a, b := e.session(), e.session()
	if code, body := e.claim(a, "alice"); code != 200 || !strings.Contains(string(body), `"name":"alice"`) {
		t.Fatalf("claim: %d %s", code, body)
	}
	e.claimOK(b, "bob")
	if code, body := e.claim(a, "Alice"); code != 200 || !strings.Contains(string(body), `"name":"alice"`) {
		t.Fatalf("claiming the held name again: %d %s", code, body)
	}
	if code, body := e.send(a, "bob", "hi"); code != 202 {
		t.Fatalf("send: %d %s", code, body)
	}
	code, raw := e.do(b, "POST", "/v1/sync", protocol.SyncRequest{})
	if code != 200 {
		t.Fatalf("sync: %d %s", code, raw)
	}
	var r protocol.SyncResponse
	json.Unmarshal(raw, &r)
	if len(r.Messages) != 1 || r.Messages[0].Text != "hi" || strp(r.Messages[0].From) != "alice" ||
		strp(r.Messages[0].FromAccount) == "<nil>" || !r.Messages[0].FirstContact || r.Name != "bob" {
		t.Fatalf("sync: %+v", r)
	}
	// The wire format has no channel or handle left in it.
	for _, gone := range []string{`"handle"`, `"channels"`, `"from_label"`, `"peer_label"`} {
		if strings.Contains(string(raw), gone) {
			t.Fatalf("sync JSON still carries %s: %s", gone, raw)
		}
	}
	for _, want := range []string{`"from":"alice"`, `"first_contact":true`, `"name":"bob"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("sync JSON lacks %s: %s", want, raw)
		}
	}
	// claim errors
	if code, body := e.claim(b, "alice"); code != 409 || errCode(body) != protocol.CodeNameHeld {
		t.Fatalf("claim a second name: %d %s", code, body)
	}
	c := e.session()
	if code, body := e.claim(c, "ALICE"); code != 409 || errCode(body) != protocol.CodeNameTaken {
		t.Fatalf("claim a taken name: %d %s", code, body)
	}
	if code, body := e.claim(c, "admin"); code != 400 || errCode(body) != protocol.CodeBadName {
		t.Fatalf("claim a reserved name: %d %s", code, body)
	}
	if code, body := e.claim(c, "a/b"); code != 400 || errCode(body) != protocol.CodeBadName {
		t.Fatalf("claim an invalid name: %d %s", code, body)
	}
	if code, body := e.do(c, "POST", "/v1/name", "{"); code != 400 || errCode(body) != protocol.CodeBadRequest {
		t.Fatalf("claim malformed: %d %s", code, body)
	}
	// send errors
	if code, body := e.send(c, "alice", "x"); code != 409 || errCode(body) != protocol.CodeNoName {
		t.Fatalf("send without a name: %d %s", code, body)
	}
	if code, body := e.send(a, "zzz", "x"); code != 404 || errCode(body) != protocol.CodeUnknownPeer || strings.Contains(string(body), "known_peers") {
		t.Fatalf("unknown peer: %d %s", code, body)
	}
	if code, body := e.send(a, "bob", ""); code != 400 || errCode(body) != protocol.CodeEmpty {
		t.Fatalf("empty: %d %s", code, body)
	}
	if code, body := e.send(a, "ALICE", "x"); code != 400 || errCode(body) != protocol.CodeSelfSend {
		t.Fatalf("self send: %d %s", code, body)
	}
	if code, body := e.send(a, "b/b", "x"); code != 400 || errCode(body) != protocol.CodeBadName {
		t.Fatalf("send to an invalid name: %d %s", code, body)
	}
	if code, body := e.do(a, "POST", "/v1/messages", "{"); code != 400 || errCode(body) != protocol.CodeBadRequest {
		t.Fatalf("malformed: %d %s", code, body)
	}
	// release
	if code, body := e.do(b, "DELETE", "/v1/name", nil); code != 200 || !strings.Contains(string(body), `"released":"bob"`) {
		t.Fatalf("release: %d %s", code, body)
	}
	if code, body := e.do(b, "DELETE", "/v1/name", nil); code != 200 || !strings.Contains(string(body), `"released":""`) {
		t.Fatalf("second release: %d %s", code, body)
	}
	if code, body := e.send(a, "bob", "x"); code != 410 || errCode(body) != protocol.CodePeerLeft {
		t.Fatalf("send to a peer that released: %d %s", code, body)
	}
	// clean exit
	if code, _ := e.do(a, "DELETE", "/v1/sessions/self", nil); code != 204 {
		t.Fatalf("delete self: %d", code)
	}
}

func TestRemovedChannelRoutesAnswerUpgradeRequired(t *testing.T) {
	e := newHTTPEnv(t)
	tok := e.session()
	for _, rt := range [][2]string{{"POST", "/v1/channels"}, {"POST", "/v1/channels/join"}, {"POST", "/v1/leave"}} {
		for _, token := range []string{"", "not-a-token", tok} {
			code, body := e.do(token, rt[0], rt[1], struct{}{})
			if code != 410 || errCode(body) != protocol.CodeUpgradeRequired || !strings.Contains(string(body), "upgrade agent-bus") {
				t.Errorf("%s %s with token %q: %d %s", rt[0], rt[1], token, code, body)
			}
		}
	}
}
func TestUnauthorized(t *testing.T) {
	e := newHTTPEnv(t)
	for _, rt := range [][2]string{{"POST", "/v1/name"}, {"DELETE", "/v1/name"}, {"POST", "/v1/messages"},
		{"POST", "/v1/sync"}, {"DELETE", "/v1/sessions/self"}} {
		if code, body := e.do("", rt[0], rt[1], struct{}{}); code != 401 || errCode(body) != protocol.CodeUnauthorized {
			t.Errorf("%s %s without token: %d %s", rt[0], rt[1], code, body)
		}
		if code, _ := e.do("not-a-token", rt[0], rt[1], struct{}{}); code != 401 {
			t.Errorf("%s %s bad token: %d", rt[0], rt[1], code)
		}
	}
	tok := e.session()
	e.do(tok, "DELETE", "/v1/sessions/self", nil)
	if code, _, _ := e.sync(tok, protocol.SyncRequest{}); code != 401 {
		t.Fatalf("sync after delete: %d", code)
	}
}

func asyncSync(e *httpEnv, tok string, req protocol.SyncRequest) <-chan syncResult {
	ch := make(chan syncResult, 1)
	go func() {
		code, r, _ := e.sync(tok, req)
		ch <- syncResult{code, r}
	}()
	return ch
}

type syncResult struct {
	code int
	resp protocol.SyncResponse
}

func await(t *testing.T, ch <-chan syncResult) syncResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("sync did not return")
		return syncResult{}
	}
}

func notYet(t *testing.T, ch <-chan syncResult) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("sync returned too early")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSyncReturnsImmediatelyWhenVersionBehind(t *testing.T) {
	e := newHTTPEnv(t)
	a := e.session()
	e.claimOK(a, "alice") // bumps the version to 1
	start := time.Now()
	code, r, _ := e.sync(a, protocol.SyncRequest{Version: 0, Wait: 20})
	if code != 200 || r.Version != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("code %d version %d after %v", code, r.Version, time.Since(start))
	}
}

func TestSyncWakesOnSendReleaseAndClaim(t *testing.T) {
	e := newHTTPEnv(t)
	a, b := e.session(), e.session()
	e.claimOK(a, "alice")
	e.claimOK(b, "bob")

	// a send mid-wait wakes the receiver
	_, rb, _ := e.sync(b, protocol.SyncRequest{})
	w := asyncSync(e, b, protocol.SyncRequest{After: lastID(rb), Version: rb.Version, Wait: 20})
	notYet(t, w)
	e.send(a, "bob", "ping")
	got := await(t, w)
	if len(got.resp.Messages) != 1 || got.resp.Messages[0].Text != "ping" {
		t.Fatalf("send wake: %+v", got)
	}

	// a partner's release wakes the other side with peer_left
	w = asyncSync(e, b, protocol.SyncRequest{After: lastID(got.resp), Version: got.resp.Version, Wait: 20})
	notYet(t, w)
	e.do(a, "DELETE", "/v1/name", nil)
	got = await(t, w)
	if got.code != 200 || len(got.resp.Messages) != 1 || got.resp.Messages[0].Kind != protocol.KindPeerLeft {
		t.Fatalf("release wake: %+v", got)
	}

	// the caller's own claim wakes its own long-poll (the name changed)
	c := e.session()
	_, base, _ := e.sync(c, protocol.SyncRequest{})
	w = asyncSync(e, c, protocol.SyncRequest{Version: base.Version, Wait: 20})
	notYet(t, w)
	e.claimOK(c, "carol")
	got = await(t, w)
	if got.code != 200 || got.resp.Name != "carol" {
		t.Fatalf("own claim wake: %+v", got)
	}
}
func lastID(r protocol.SyncResponse) int64 {
	if len(r.Messages) == 0 {
		return 0
	}
	return r.Messages[len(r.Messages)-1].ID
}

func TestSyncEmptyAtTimeout(t *testing.T) {
	e := newHTTPEnv(t)
	a := e.session()
	_, base, _ := e.sync(a, protocol.SyncRequest{})
	start := time.Now()
	code, r, _ := e.sync(a, protocol.SyncRequest{Version: base.Version, Wait: 1})
	d := time.Since(start)
	if code != 200 || len(r.Messages) != 0 || d < 900*time.Millisecond || d > 3*time.Second {
		t.Fatalf("code %d msgs %d after %v", code, len(r.Messages), d)
	}
}

func TestTwoConcurrentLongPollsBothReturn(t *testing.T) {
	e := newHTTPEnv(t)
	a, b := e.session(), e.session()
	e.claimOK(a, "alice")
	e.claimOK(b, "bob")
	_, base, _ := e.sync(b, protocol.SyncRequest{})
	w1 := asyncSync(e, b, protocol.SyncRequest{Version: base.Version, Wait: 20})
	w2 := asyncSync(e, b, protocol.SyncRequest{Version: base.Version, Wait: 20})
	notYet(t, w1)
	e.send(a, "bob", "one")
	for _, w := range []<-chan syncResult{w1, w2} {
		if got := await(t, w); len(got.resp.Messages) != 1 {
			t.Fatalf("long-poll: %+v", got)
		}
	}
}

func TestLongPollGets401WhenSessionDeleted(t *testing.T) {
	e := newHTTPEnv(t)
	a := e.session()
	_, base, _ := e.sync(a, protocol.SyncRequest{})
	w := asyncSync(e, a, protocol.SyncRequest{Version: base.Version, Wait: 20})
	notYet(t, w)
	// delete through the store, as the sweeper's reap would
	id, _ := e.s.SessionByToken(bg, a)
	if err := e.s.DeleteSelf(bg, id); err != nil {
		t.Fatal(err)
	}
	if got := await(t, w); got.code != 401 {
		t.Fatalf("code %d, want 401", got.code)
	}
}

func TestMessageSizeLimits(t *testing.T) {
	e := newHTTPEnv(t)
	a, b := e.session(), e.session()
	e.claimOK(a, "alice")
	e.claimOK(b, "bob")
	// 60 KiB of quotes: the JSON body is ~120 KiB, but the decoded text is under the limit
	quotes := strings.Repeat(`"`, 60<<10)
	code, body := e.send(a, "bob", quotes)
	if code != 202 {
		t.Fatalf("60 KiB text: %d %s", code, body)
	}
	over := strings.Repeat("x", protocol.MaxMessageBytes+1)
	code, body = e.send(a, "bob", over)
	if code != 413 || errCode(body) != protocol.CodeTooLarge || !strings.Contains(string(body), "65536") {
		t.Fatalf("over limit: %d %s", code, body)
	}
	code, _ = e.send(a, "bob", strings.Repeat("y", 2<<20))
	if code != 413 {
		t.Fatalf("over body limit: %d", code)
	}
}
func TestBadNames(t *testing.T) {
	e := newHTTPEnv(t)
	a := e.session()
	for _, n := range []string{`na"me`, `<x>`, "no/slash", "", "a b", "é", strings.Repeat("x", 65), "admin", "Agent-Bus"} {
		if code, body := e.claim(a, n); code != 400 || errCode(body) != protocol.CodeBadName {
			t.Errorf("claim %q: %d %s", n, code, body)
		}
	}
	if _, r, _ := e.sync(a, protocol.SyncRequest{}); r.Name != "" {
		t.Fatalf("a refused claim left name %q", r.Name)
	}
}
func TestLogsHoldNoSecrets(t *testing.T) {
	e := newHTTPEnv(t)
	a, b := e.session(), e.session()
	e.claimOK(a, "alice")
	e.claimOK(b, "bob")
	e.send(a, "bob", "secret text")
	e.do("bogus-token-value", "POST", "/v1/sync", protocol.SyncRequest{})
	logs := e.logs.String()
	if logs == "" {
		t.Fatal("no logs captured")
	}
	for _, secretStr := range []string{a, b, "bogus-token-value", "secret text"} {
		if strings.Contains(logs, secretStr) {
			t.Fatalf("log contains secret %q", secretStr)
		}
	}
}
func TestSendToAPeerWhoseSessionEndedIs410(t *testing.T) {
	e := newHTTPEnv(t)
	a, b := e.session(), e.session()
	e.claimOK(a, "alice")
	e.claimOK(b, "bob")
	e.send(a, "bob", "hello")
	if code, _ := e.do(b, "DELETE", "/v1/sessions/self", nil); code != 204 {
		t.Fatalf("delete self: %d", code)
	}
	code, body := e.send(a, "bob", "x")
	if code != 410 || errCode(body) != protocol.CodePeerLeft {
		t.Fatalf("send to an ended session: %d %s", code, body)
	}
	// Someone who never talked to bob learns nothing.
	c := e.session()
	e.claimOK(c, "carol")
	if code, body := e.send(c, "bob", "x"); code != 404 || errCode(body) != protocol.CodeUnknownPeer {
		t.Fatalf("stranger: %d %s", code, body)
	}
}
func TestInternalErrorIs500AndHidesDetails(t *testing.T) {
	e := newHTTPEnv(t)
	a := e.session()
	e.s.db.Close() // every store call now fails with a non-*Error
	code, body := e.claim(a, "alice")
	if code != 500 && code != 401 {
		t.Fatalf("got %d %s", code, body)
	}
	if code == 500 && (errCode(body) != protocol.CodeInternal || strings.Contains(string(body), "closed")) {
		t.Fatalf("internal error leaks details: %s", body)
	}
	// writeErr with a plain error, directly
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/x", nil)
	NewServer(e.s, NewTracker(nil), nil).writeErr(rec, req, io.ErrUnexpectedEOF)
	if rec.Code != 500 || errCode(rec.Body.Bytes()) != protocol.CodeInternal || strings.Contains(rec.Body.String(), "EOF") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	// a cancelled request writes nothing
	ctx, cancel := context.WithCancel(bg)
	cancel()
	rec = httptest.NewRecorder()
	NewServer(e.s, NewTracker(nil), nil).writeErr(rec, req.WithContext(ctx), io.ErrUnexpectedEOF)
	if rec.Body.Len() != 0 {
		t.Fatalf("wrote %q for a vanished client", rec.Body)
	}
}

func TestErrorStringAndHTTPServerTimeouts(t *testing.T) {
	if got := ErrNameTaken.Error(); got != "name_taken: that name is already in use by another session" {
		t.Fatalf("Error() = %q", got)
	}
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	srv := HTTPServer(":0", http.NewServeMux(), ctx)
	if srv.ReadHeaderTimeout != 10*time.Second || srv.IdleTimeout == 0 {
		t.Fatalf("timeouts: header %v idle %v", srv.ReadHeaderTimeout, srv.IdleTimeout)
	}
	if srv.WriteTimeout != 0 && srv.WriteTimeout <= protocol.SyncWaitCap {
		t.Fatalf("WriteTimeout %v would cut long-polls", srv.WriteTimeout)
	}
	if srv.BaseContext(nil) != ctx {
		t.Fatal("BaseContext is not the run context")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func TestRunServesAndInitBeforeSweeper(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "run.db")
	// Seed a live (never disconnected) session, as after a broker restart.
	seed, err := OpenStore(dbPath, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	seedAcc, seedCred := addAccount(t, seed, "seed", protocol.AccountActive, false)
	if _, _, err := seed.CreateSession(bg, seedAcc); err != nil {
		t.Fatal(err)
	}
	seed.Close()

	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{Addr: addr, DBPath: dbPath, SweepInterval: 20 * time.Millisecond,
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
	var resp *http.Response
	tlsClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	for i := 0; i < 100; i++ {
		req, _ := http.NewRequest("POST", "https://"+addr+"/v1/sessions", strings.NewReader("{}"))
		req.Header.Set("Authorization", "Bearer "+seedCred)
		resp, err = tlsClient.Do(req)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || resp.StatusCode != 201 {
		t.Fatalf("broker not serving: %v %v", err, resp)
	}
	resp.Body.Close()
	time.Sleep(300 * time.Millisecond) // many sweeps
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	// If the sweeper had run before Init, the seeded session (zero last_seen)
	// would have been disconnected. Init came first, so it was not.
	check, err := OpenStore(dbPath, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var n int
	check.db.QueryRow("SELECT COUNT(*) FROM sessions WHERE disconnected_at IS NOT NULL").Scan(&n)
	if n != 0 {
		t.Fatalf("%d sessions disconnected right after startup", n)
	}
}

func TestRunFailsOnBadListenAddress(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	err := Run(bg, Config{Addr: ln.Addr().String(), DBPath: filepath.Join(t.TempDir(), "x.db"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err == nil {
		t.Fatal("Run succeeded on an address already in use")
	}
}

func TestCreateSessionCredentialAuth(t *testing.T) {
	e := newHTTPEnv(t)
	if code, body := e.do("", "POST", "/v1/sessions", struct{}{}); code != 401 || errCode(body) != protocol.CodeUnauthorized {
		t.Fatalf("no credential: %d %s", code, body)
	}
	if code, body := e.do("not-a-credential", "POST", "/v1/sessions", struct{}{}); code != 401 {
		t.Fatalf("bad credential: %d %s", code, body)
	}
	_, pend := addAccount(t, e.s, "waiting", protocol.AccountPending, false)
	if code, body := e.do(pend, "POST", "/v1/sessions", struct{}{}); code != 403 || errCode(body) != protocol.CodeAccountPending {
		t.Fatalf("pending: %d %s", code, body)
	}
	cred := e.credential()
	code, body := e.do(cred, "POST", "/v1/sessions", struct{}{})
	var r protocol.CreateSessionResponse
	json.Unmarshal(body, &r)
	if code != 201 || r.SessionToken == "" || r.Account == "" {
		t.Fatalf("active: %d %s", code, body)
	}
	// A session token is not a credential, and a credential is not a session token.
	if code, _ := e.do(r.SessionToken, "POST", "/v1/sessions", struct{}{}); code != 401 {
		t.Fatalf("session token as credential: %d", code)
	}
	if code, _ := e.claim(cred, "alice"); code != 401 {
		t.Fatalf("credential as session token: %d", code)
	}
}

func TestLoggingNeverHoldsCredentialsAndLabelsThemCred(t *testing.T) {
	e := newHTTPEnv(t)
	cred := e.credential()
	e.do(cred, "POST", "/v1/sessions", struct{}{})
	e.do(cred, "GET", "/v1/account", nil)
	tok := e.session()
	e.do(tok, "POST", "/v1/sync", protocol.SyncRequest{})
	e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: "someone", ClaimCode: "ABCDE-FGHIJ-KLMNO-PQRST"})
	logs := e.logs.String()
	for _, sec := range []string{cred, tok, "ABCDE-FGHIJ-KLMNO-PQRST"} {
		if strings.Contains(logs, sec) {
			t.Fatalf("log contains %q", sec)
		}
	}
	if !strings.Contains(logs, "cred=") || !strings.Contains(logs, "session=") {
		t.Fatalf("expected cred= and session= attributes:\n%s", logs)
	}
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "/v1/sessions") && strings.Contains(line, "session=") && !strings.Contains(line, "DELETE") {
			t.Fatalf("a credential-authenticated route is labelled session: %s", line)
		}
	}
}

func TestAnnounceClaimOnlyWhenUnclaimed(t *testing.T) {
	s, _ := openTestStore(t)
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	issued, err := announceClaim(bg, s, log)
	if err != nil || !issued {
		t.Fatalf("unclaimed: %v %v", issued, err)
	}
	var code string
	for _, f := range strings.Fields(logs.String()) {
		if strings.HasPrefix(f, "claim_code=") {
			code = strings.TrimPrefix(f, "claim_code=")
		}
	}
	if !claimFormat.MatchString(code) {
		t.Fatalf("logged code %q", code)
	}
	if strings.Count(logs.String(), "claim_code=") != 1 || !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("log: %s", logs.String())
	}
	if _, err := s.Register(bg, "boss", code); err != nil {
		t.Fatalf("the logged code does not work: %v", err)
	}
	// Claimed: a start issues and logs nothing.
	logs2 := &syncBuffer{}
	issued, err = announceClaim(bg, s, slog.New(slog.NewTextHandler(logs2, nil)))
	if err != nil || issued || logs2.String() != "" {
		t.Fatalf("claimed store: issued=%v err=%v log=%q", issued, err, logs2.String())
	}
}

func TestRunServesTLSOnlyWithControlSocketAndLogsOneClaimCode(t *testing.T) {
	dir, err := os.MkdirTemp("", "run")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	logs := &syncBuffer{}
	ready := make(chan string, 1)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	cfg := Config{Addr: "127.0.0.1:0", DBPath: filepath.Join(dir, "a.db"), Ready: func(a string) { ready <- a },
		Log: slog.New(slog.NewTextHandler(logs, nil)), SweepInterval: time.Hour}
	go func() { done <- Run(ctx, cfg) }()
	var addr string
	select {
	case addr = <-ready:
	case err := <-done:
		t.Fatalf("Run: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not start")
	}
	// Defaults sit next to the database; the key is private.
	if fi, err := os.Stat(filepath.Join(dir, "broker-key.pem")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file: %v %v", fi, err)
	}
	// Plain HTTP is refused.
	resp, err := http.Post("http://"+addr+"/v1/register", "application/json", strings.NewReader("{}"))
	if err == nil {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 400 || !strings.Contains(string(b), "HTTP request to an HTTPS server") {
			t.Fatalf("plain HTTP got %d %s", resp.StatusCode, b)
		}
	}
	// TLS 1.3 with the identity's fingerprint, which the control socket reports.
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatal(err)
	}
	fp := identity.FingerprintSPKI(conn.ConnectionState().PeerCertificates[0].RawSubjectPublicKeyInfo)
	conn.Close()
	if got, err := ControlFingerprint(DefaultControlPath(cfg.DBPath)); err != nil || got != fp {
		t.Fatalf("control fingerprint %q %v, served %q", got, err, fp)
	}
	if !strings.Contains(logs.String(), fp) {
		t.Fatal("the fingerprint was not logged at start")
	}
	if n := strings.Count(logs.String(), "claim_code="); n != 1 {
		t.Fatalf("%d claim codes logged at start of an unclaimed broker", n)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	if _, err := os.Stat(DefaultControlPath(cfg.DBPath)); err == nil {
		t.Fatal("the control socket was left behind")
	}

	// A second start with a hostmaster logs no code, and keeps the same key.
	store, err := OpenStore(cfg.DBPath, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	addAccount(t, store, "boss", protocol.AccountActive, true)
	store.Close()
	logs2 := &syncBuffer{}
	ready2 := make(chan string, 1)
	ctx2, cancel2 := context.WithCancel(bg)
	cfg.Log, cfg.Ready = slog.New(slog.NewTextHandler(logs2, nil)), func(a string) { ready2 <- a }
	go func() { done <- Run(ctx2, cfg) }()
	select {
	case addr = <-ready2:
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not restart")
	}
	if strings.Contains(logs2.String(), "claim_code") {
		t.Fatalf("a claimed broker logged a claim code:\n%s", logs2.String())
	}
	if !strings.Contains(logs2.String(), fp) {
		t.Fatal("the fingerprint changed across a restart")
	}
	cancel2()
	<-done
}
