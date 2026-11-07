package broker

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// hostmasterEnv is an HTTP env with a claimed broker and a hostmaster session.
type hostmasterEnv struct {
	*httpEnv
	hostCred, hostTok string
}

func newHostmasterEnv(t *testing.T) *hostmasterEnv {
	e := newHTTPEnv(t)
	e.s.SetLogger(slog.New(slog.NewTextHandler(e.logs, nil)))
	code, _, err := e.s.IssueClaim(bg)
	if err != nil {
		t.Fatal(err)
	}
	c, body := e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: "boss", ClaimCode: code})
	var rr protocol.RegisterResponse
	json.Unmarshal(body, &rr)
	if c != 201 || !rr.Hostmaster {
		t.Fatalf("claim: %d %s", c, body)
	}
	c, body = e.do(rr.Credential, "POST", "/v1/sessions", struct{}{})
	var sr protocol.CreateSessionResponse
	json.Unmarshal(body, &sr)
	if c != 201 {
		t.Fatalf("session: %d %s", c, body)
	}
	return &hostmasterEnv{httpEnv: e, hostCred: rr.Credential, hostTok: sr.SessionToken}
}

func TestAdminRoutesForbiddenForNonHostmasterBeforeBodyValidation(t *testing.T) {
	e := newHostmasterEnv(t)
	user := e.session()
	for _, rt := range [][2]string{
		{"GET", "/v1/admin/accounts"}, {"POST", "/v1/admin/approve"}, {"POST", "/v1/admin/reject"},
		{"POST", "/v1/admin/remove"}, {"POST", "/v1/admin/max-accounts"}, {"POST", "/v1/admin/subscribe"},
	} {
		// A malformed body and a wildcard name both give 403, never 400.
		for _, body := range []any{"{", map[string]string{"name": "*"}, nil} {
			if code, b := e.do(user, rt[0], rt[1], body); code != 403 || errCode(b) != protocol.CodeForbidden {
				t.Errorf("%s %s: %d %s", rt[0], rt[1], code, b)
			}
		}
		if code, _ := e.do("", rt[0], rt[1], nil); code != 401 {
			t.Errorf("%s %s without a token: %d", rt[0], rt[1], code)
		}
		if code, _ := e.do(e.hostCred, rt[0], rt[1], nil); code != 401 {
			t.Errorf("%s %s with a credential: %d", rt[0], rt[1], code)
		}
	}
}

func TestHostmasterFlowsOverHTTP(t *testing.T) {
	e := newHostmasterEnv(t)
	reg := func(name string) (int, protocol.RegisterResponse, []byte) {
		c, b := e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: name})
		var r protocol.RegisterResponse
		json.Unmarshal(b, &r)
		return c, r, b
	}
	c, alice, b := reg("alice")
	if c != 201 || alice.Status != protocol.AccountPending || alice.Hostmaster || alice.Credential == "" {
		t.Fatalf("register: %d %s", c, b)
	}
	// status over the credential
	var acct protocol.AccountResponse
	code, b := e.do(alice.Credential, "GET", "/v1/account", nil)
	json.Unmarshal(b, &acct)
	if code != 200 || acct.Status != protocol.AccountPending || acct.Name != "alice" {
		t.Fatalf("account: %d %s", code, b)
	}
	// list
	var l protocol.ListAccountsResponse
	code, b = e.do(e.hostTok, "GET", "/v1/admin/accounts", nil)
	json.Unmarshal(b, &l)
	if code != 200 || l.Pending != 1 || l.Active != 1 || l.MaxAccounts != protocol.DefaultMaxAccounts || len(l.Accounts) != 2 {
		t.Fatalf("list: %d %s", code, b)
	}
	// approve
	code, b = e.do(e.hostTok, "POST", "/v1/admin/approve", protocol.AdminNameRequest{Name: "alice"})
	if code != 200 || !strings.Contains(string(b), `"status":"active"`) {
		t.Fatalf("approve: %d %s", code, b)
	}
	if code, b = e.do(e.hostTok, "POST", "/v1/admin/approve", protocol.AdminNameRequest{Name: "alice"}); code != 409 || errCode(b) != protocol.CodeWrongState {
		t.Fatalf("approve twice: %d %s", code, b)
	}
	if code, b = e.do(e.hostTok, "POST", "/v1/admin/approve", protocol.AdminNameRequest{Name: "ghost"}); code != 404 || errCode(b) != protocol.CodeNoSuchAccount {
		t.Fatalf("approve missing: %d %s", code, b)
	}
	// reject
	_, bob, _ := reg("bob")
	if code, b = e.do(e.hostTok, "POST", "/v1/admin/reject", protocol.AdminNameRequest{Name: "bob"}); code != 204 {
		t.Fatalf("reject: %d %s", code, b)
	}
	code, b = e.do(bob.Credential, "GET", "/v1/account", nil)
	json.Unmarshal(b, &acct)
	if acct.Status != protocol.AccountRejected {
		t.Fatalf("rejected status: %s", b)
	}
	if code, b = e.do(bob.Credential, "POST", "/v1/sessions", struct{}{}); code != 403 || errCode(b) != protocol.CodeAccountRejected {
		t.Fatalf("rejected session: %d %s", code, b)
	}
	// remove
	if code, b = e.do(e.hostTok, "POST", "/v1/admin/remove", protocol.AdminNameRequest{Name: "boss"}); code != 409 || errCode(b) != protocol.CodeWrongState {
		t.Fatalf("remove hostmaster: %d %s", code, b)
	}
	if code, b = e.do(e.hostTok, "POST", "/v1/admin/remove", protocol.AdminNameRequest{Name: "alice"}); code != 204 {
		t.Fatalf("remove: %d %s", code, b)
	}
	code, b = e.do(alice.Credential, "GET", "/v1/account", nil)
	json.Unmarshal(b, &acct)
	if acct.Status != protocol.AccountRemoved {
		t.Fatalf("removed status: %s", b)
	}
	// validation
	for _, name := range []string{"", "*", "a b", strings.Repeat("a", 65)} {
		if code, b = e.do(e.hostTok, "POST", "/v1/admin/approve", protocol.AdminNameRequest{Name: name}); code != 400 || errCode(b) != protocol.CodeBadRequest {
			t.Errorf("name %q: %d %s", name, code, b)
		}
	}
	// max accounts
	var mr protocol.MaxAccountsResponse
	code, b = e.do(e.hostTok, "POST", "/v1/admin/max-accounts", protocol.MaxAccountsRequest{Max: 7})
	json.Unmarshal(b, &mr)
	if code != 200 || mr.MaxAccounts != 7 || mr.Active != 1 {
		t.Fatalf("max-accounts: %d %s", code, b)
	}
	if code, b = e.do(e.hostTok, "POST", "/v1/admin/max-accounts", protocol.MaxAccountsRequest{Max: 0}); code != 400 || errCode(b) != protocol.CodeBadRequest {
		t.Fatalf("max-accounts 0: %d %s", code, b)
	}
	// subscribe
	var sub protocol.SubscribeResponse
	reg("carol")
	code, b = e.do(e.hostTok, "POST", "/v1/admin/subscribe", protocol.SubscribeRequest{Subscribed: true})
	json.Unmarshal(b, &sub)
	if code != 200 || !sub.Subscribed || sub.Pending != 1 {
		t.Fatalf("subscribe: %d %s", code, b)
	}
	_, r, _ := e.sync(e.hostTok, protocol.SyncRequest{})
	if !r.AdminSubscribed {
		t.Fatal("sync does not report admin_subscribed")
	}
	// unknown credential
	if code, _ = e.do("nope", "GET", "/v1/account", nil); code != 401 {
		t.Fatalf("unknown credential: %d", code)
	}
	if code, _ = e.do("", "GET", "/v1/account", nil); code != 401 {
		t.Fatalf("no credential: %d", code)
	}
}

func TestRegisterRefusalsAreIdenticalOverHTTP(t *testing.T) {
	e := newHostmasterEnv(t)
	e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: "taken"})
	var first []byte
	var firstCode int
	for i, name := range []string{"taken", "TAKEN", "bad name", "", "x/y"} {
		code, b := e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: name})
		if i == 0 {
			first, firstCode = b, code
			if code != 403 || errCode(b) != protocol.CodeRegistrationRefused {
				t.Fatalf("%d %s", code, b)
			}
			continue
		}
		if code != firstCode || string(b) != string(first) {
			t.Errorf("%q: %d %s differs from %d %s", name, code, b, firstCode, first)
		}
	}
	// The same body for a claim code sent to a claimed broker with no recovery pending.
	code, b := e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: "zed", ClaimCode: "AAAAA-AAAAA-AAAAA-AAAAA"})
	if code != firstCode || string(b) != string(first) {
		t.Errorf("claim code on a claimed broker: %d %s", code, b)
	}
}

func TestRemoveForgetsDeletedSessionsInTracker(t *testing.T) {
	e := newHostmasterEnv(t)
	_, b := e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: "user"})
	var user protocol.RegisterResponse
	json.Unmarshal(b, &user)
	e.do(e.hostTok, "POST", "/v1/admin/approve", protocol.AdminNameRequest{Name: "user"})
	if code, b := e.do(user.Credential, "POST", "/v1/sessions", struct{}{}); code != 201 {
		t.Fatalf("session: %d %s", code, b)
	}
	var userSess int64
	if err := e.s.db.QueryRow("SELECT s.id FROM sessions s JOIN accounts a ON a.id = s.account_id WHERE a.name = 'user'").Scan(&userSess); err != nil {
		t.Fatal(err)
	}
	if e.tr.size() != 2 {
		t.Fatalf("tracker size %d, want 2", e.tr.size())
	}
	if code, b := e.do(e.hostTok, "POST", "/v1/admin/remove", protocol.AdminNameRequest{Name: "user"}); code != 204 {
		t.Fatalf("remove: %d %s", code, b)
	}
	if e.tr.size() != 1 {
		t.Fatalf("tracker still holds the removed session: size %d", e.tr.size())
	}
	e.tr.Touch(userSess) // a late deferred Touch must not re-add it
	if e.tr.size() != 1 {
		t.Fatalf("late Touch re-added a deleted session: size %d", e.tr.size())
	}
}

func TestAdminMutationsLogOneInfoLineWithoutSecrets(t *testing.T) {
	e := newHostmasterEnv(t)
	_, b := e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: "alice"})
	var alice protocol.RegisterResponse
	json.Unmarshal(b, &alice)
	before := e.logs.String()
	e.do(e.hostTok, "POST", "/v1/admin/approve", protocol.AdminNameRequest{Name: "alice"})
	e.do(e.hostTok, "POST", "/v1/admin/max-accounts", protocol.MaxAccountsRequest{Max: 9})
	e.do(e.hostTok, "POST", "/v1/admin/subscribe", protocol.SubscribeRequest{Subscribed: true})
	e.do(e.hostTok, "POST", "/v1/admin/subscribe", protocol.SubscribeRequest{Subscribed: false})
	e.do("", "POST", "/v1/register", protocol.RegisterRequest{Name: "bob"})
	e.do(e.hostTok, "POST", "/v1/admin/reject", protocol.AdminNameRequest{Name: "bob"})
	e.do(e.hostTok, "POST", "/v1/admin/remove", protocol.AdminNameRequest{Name: "alice"})
	added := strings.TrimPrefix(e.logs.String(), before)
	var audit []string
	for _, line := range strings.Split(added, "\n") {
		if strings.Contains(line, "admin action") {
			audit = append(audit, line)
		}
	}
	if len(audit) != 6 {
		t.Fatalf("want 6 audit lines, got %d:\n%s", len(audit), added)
	}
	for i, want := range []string{"action=approve target=alice", "action=max-accounts target=9", "action=subscribe", "action=unsubscribe", "action=reject target=bob", "action=remove target=alice"} {
		if !strings.Contains(audit[i], want) || !strings.Contains(audit[i], "actor=boss") || !strings.Contains(audit[i], "level=INFO") {
			t.Errorf("audit line %d = %s, want %s", i, audit[i], want)
		}
	}
	for _, sec := range []string{alice.Credential, e.hostCred, e.hostTok} {
		if strings.Contains(e.logs.String(), sec) {
			t.Fatalf("log holds a secret")
		}
	}
}
