package client

import (
	"errors"
	"testing"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

func wantAPI(t *testing.T, err error, status int, code string) {
	t.Helper()
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != status || ae.Code != code {
		t.Fatalf("got %v, want %d %s", err, status, code)
	}
}

func TestRegisterAndAccountStatus(t *testing.T) {
	tb := startBroker(t)
	anon := tb.client(t, "")
	// Unclaimed: registration is refused.
	_, err := anon.Register(bg, "alice", "")
	wantAPI(t, err, 403, protocol.CodeRegistrationRefused)

	code, _, _ := tb.store.IssueClaim(bg)
	boss, err := anon.Register(bg, "boss", code)
	if err != nil || !boss.Hostmaster || boss.Status != protocol.AccountActive || boss.Credential == "" {
		t.Fatalf("claim: %+v %v", boss, err)
	}
	alice, err := anon.Register(bg, "alice", "")
	if err != nil || alice.Status != protocol.AccountPending || alice.Hostmaster {
		t.Fatalf("register: %+v %v", alice, err)
	}
	st, err := tb.client(t, alice.Credential).AccountStatus(bg)
	if err != nil || st.Status != protocol.AccountPending || st.Name != "alice" || st.Hostmaster {
		t.Fatalf("status: %+v %v", st, err)
	}
	st, err = tb.client(t, boss.Credential).AccountStatus(bg)
	if err != nil || !st.Hostmaster {
		t.Fatalf("hostmaster status: %+v %v", st, err)
	}
	_, err = tb.client(t, "bogus").AccountStatus(bg)
	wantAPI(t, err, 401, protocol.CodeUnauthorized)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatal("a credential 401 should match ErrUnauthorized at the client layer")
	}
	// A pending account cannot open a session.
	_, _, err = tb.client(t, alice.Credential).CreateSession(bg)
	wantAPI(t, err, 403, protocol.CodeAccountPending)
	// No credential at all.
	_, _, err = anon.CreateSession(bg)
	wantAPI(t, err, 401, protocol.CodeUnauthorized)
}

func TestAdminMethods(t *testing.T) {
	tb := startBroker(t)
	bossCred := tb.hostmaster(t, "boss")
	boss := tb.client(t, bossCred)
	tok, account, err := boss.CreateSession(bg)
	if err != nil || account != "boss" {
		t.Fatal(account, err)
	}
	anon := tb.client(t, "")
	a, _ := anon.Register(bg, "alice", "")
	b, _ := anon.Register(bg, "bob", "")

	l, err := boss.ListAccounts(bg, tok)
	if err != nil || l.Pending != 2 || l.Active != 1 || len(l.Accounts) != 3 || l.MaxAccounts != protocol.DefaultMaxAccounts {
		t.Fatalf("list: %+v %v", l, err)
	}
	ap, err := boss.Approve(bg, tok, "alice")
	if err != nil || ap.Name != "alice" || ap.Status != protocol.AccountActive {
		t.Fatalf("approve: %+v %v", ap, err)
	}
	_, err = boss.Approve(bg, tok, "alice")
	wantAPI(t, err, 409, protocol.CodeWrongState)
	_, err = boss.Approve(bg, tok, "ghost")
	wantAPI(t, err, 404, protocol.CodeNoSuchAccount)
	if err := boss.Reject(bg, tok, "bob"); err != nil {
		t.Fatal(err)
	}
	st, _ := tb.client(t, b.Credential).AccountStatus(bg)
	if st.Status != protocol.AccountRejected {
		t.Fatalf("bob = %s", st.Status)
	}
	mr, err := boss.SetMaxAccounts(bg, tok, 3)
	if err != nil || mr.MaxAccounts != 3 || mr.Active != 2 {
		t.Fatalf("max: %+v %v", mr, err)
	}
	_, err = boss.SetMaxAccounts(bg, tok, 0)
	wantAPI(t, err, 400, protocol.CodeBadRequest)
	sub, err := boss.SetAdminSubscription(bg, tok, true)
	if err != nil || !sub.Subscribed || sub.Pending != 0 {
		t.Fatalf("subscribe: %+v %v", sub, err)
	}
	sr, err := boss.Sync(bg, tok, protocol.SyncRequest{})
	if err != nil || !sr.AdminSubscribed {
		t.Fatalf("sync: %+v %v", sr, err)
	}
	// A non-hostmaster session is forbidden on every admin call.
	utok, _, err := tb.client(t, a.Credential).CreateSession(bg)
	if err != nil {
		t.Fatal(err)
	}
	u := tb.client(t, a.Credential)
	_, err = u.ListAccounts(bg, utok)
	wantAPI(t, err, 403, protocol.CodeForbidden)
	_, err = u.Approve(bg, utok, "x")
	wantAPI(t, err, 403, protocol.CodeForbidden)
	wantAPI(t, u.Reject(bg, utok, "x"), 403, protocol.CodeForbidden)
	wantAPI(t, u.Remove(bg, utok, "x"), 403, protocol.CodeForbidden)
	_, err = u.SetMaxAccounts(bg, utok, 2)
	wantAPI(t, err, 403, protocol.CodeForbidden)
	_, err = u.SetAdminSubscription(bg, utok, true)
	wantAPI(t, err, 403, protocol.CodeForbidden)
	// Remove ends the removed account's session.
	if err := boss.Remove(bg, tok, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := u.Sync(bg, utok, protocol.SyncRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("removed account's session still works: %v", err)
	}
	wantAPI(t, boss.Remove(bg, tok, "boss"), 409, protocol.CodeWrongState)
}
