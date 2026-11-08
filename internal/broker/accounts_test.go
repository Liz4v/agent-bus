package broker

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

type acctEnv struct {
	t    *testing.T
	s    *Store
	clk  *fakeClock
	logs *syncBuffer
}

func newAcctEnv(t *testing.T) *acctEnv {
	s, clk := openTestStore(t)
	logs := &syncBuffer{}
	s.SetLogger(slog.New(slog.NewTextHandler(logs, nil)))
	return &acctEnv{t: t, s: s, clk: clk, logs: logs}
}

// claim claims the broker as hostmaster name using a freshly issued code.
func (e *acctEnv) claim(name string) RegisterResult {
	e.t.Helper()
	code, _, err := e.s.IssueClaim(bg)
	if err != nil {
		e.t.Fatal(err)
	}
	res, err := e.s.Register(bg, name, code)
	if err != nil {
		e.t.Fatalf("claim %s: %v", name, err)
	}
	return res
}

func (e *acctEnv) pending(name string) RegisterResult {
	e.t.Helper()
	res, err := e.s.Register(bg, name, "")
	if err != nil {
		e.t.Fatalf("register %s: %v", name, err)
	}
	if res.Status != protocol.AccountPending || res.Hostmaster {
		e.t.Fatalf("register %s: %+v", name, res)
	}
	return res
}

func (e *acctEnv) active(name string) RegisterResult {
	e.t.Helper()
	res := e.pending(name)
	if _, err := e.s.Approve(bg, name); err != nil {
		e.t.Fatalf("approve %s: %v", name, err)
	}
	return res
}

func (e *acctEnv) session(cred string) int64 {
	e.t.Helper()
	acc, _, err := e.s.SessionAccount(bg, cred)
	if err != nil {
		e.t.Fatal(err)
	}
	_, id, err := e.s.CreateSession(bg, acc)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *acctEnv) status(cred string) string {
	e.t.Helper()
	a, err := e.s.AccountByCredential(bg, cred)
	if err != nil {
		e.t.Fatal(err)
	}
	return a.Status
}

func TestClaimOnUnclaimedBroker(t *testing.T) {
	e := newAcctEnv(t)
	res := e.claim("boss")
	if !res.Hostmaster || res.Status != protocol.AccountActive || res.Credential == "" {
		t.Fatalf("claim result %+v", res)
	}
	if has, _ := e.s.HasHostmaster(bg); !has {
		t.Fatal("no hostmaster after a claim")
	}
	if !strings.Contains(e.logs.String(), "hostmaster claimed") || strings.Contains(e.logs.String(), res.Credential) {
		t.Fatalf("log: %s", e.logs.String())
	}
	// The code was consumed: the same code no longer works.
	code, _, _ := e.s.IssueClaim(bg)
	if _, err := e.s.Register(bg, "other", code); err != nil {
		t.Fatalf("recovery claim with a fresh code: %v", err)
	}
}

func TestConcurrentClaimsExactlyOneWins(t *testing.T) {
	e := newAcctEnv(t)
	code, _, err := e.s.IssueClaim(bg)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = e.s.Register(bg, fmt.Sprintf("boss%d", i), code)
		}()
	}
	wg.Wait()
	wins := 0
	for _, err := range errs {
		if err == nil {
			wins++
		} else if err != ErrRegistrationRefused {
			t.Fatalf("unexpected error %v", err)
		}
	}
	var hosts int
	e.s.db.QueryRow("SELECT COUNT(*) FROM accounts WHERE is_hostmaster = 1").Scan(&hosts)
	if wins != 1 || hosts != 1 {
		t.Fatalf("wins=%d hostmasters=%d", wins, hosts)
	}
}

func TestRegisterWithoutCodeWhenUnclaimedRefused(t *testing.T) {
	e := newAcctEnv(t)
	if _, err := e.s.Register(bg, "alice", ""); err != ErrRegistrationRefused {
		t.Fatalf("got %v", err)
	}
}

func TestPendingApproveRejectRemoveFlow(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	a := e.pending("alice")
	b := e.pending("bob")
	c := e.active("carol")
	if _, _, err := e.s.SessionAccount(bg, a.Credential); err != errAccountPending {
		t.Fatalf("pending session: %v", err)
	}
	if name, err := e.s.Approve(bg, "ALICE"); err != nil || name != "alice" {
		t.Fatalf("approve: %q %v", name, err)
	}
	if e.status(a.Credential) != protocol.AccountActive {
		t.Fatal("alice not active")
	}
	if _, err := e.s.Reject(bg, "bob"); err != nil {
		t.Fatal(err)
	}
	if got := e.status(b.Credential); got != protocol.AccountRejected {
		t.Fatalf("bob = %s", got)
	}
	if _, _, err := e.s.SessionAccount(bg, b.Credential); err != errAccountRejected {
		t.Fatalf("rejected session: %v", err)
	}
	sess := e.session(c.Credential)
	name, deleted, err := e.s.Remove(bg, "carol")
	if err != nil || name != "carol" || len(deleted) != 1 || deleted[0] != sess {
		t.Fatalf("remove: %q %v %v", name, deleted, err)
	}
	if got := e.status(c.Credential); got != protocol.AccountRemoved {
		t.Fatalf("carol = %s", got)
	}
	if _, _, err := e.s.SessionAccount(bg, c.Credential); err != errAccountRemoved {
		t.Fatalf("removed session: %v", err)
	}
	if _, err := e.s.Sync(bg, sess, protocol.SyncRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("a removed account's session still syncs: %v", err)
	}
}

func TestWrongStateAndMissingAccounts(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	e.pending("pend")
	e.active("act")
	want := func(err error, e2 *Error) {
		t.Helper()
		var be *Error
		if !errors.As(err, &be) || be.Code != e2.Code {
			t.Fatalf("got %v, want %s", err, e2.Code)
		}
	}
	wrong := errWrongState("")
	_, err := e.s.Approve(bg, "act")
	want(err, wrong)
	_, err = e.s.Reject(bg, "act")
	want(err, wrong)
	_, _, err = e.s.Remove(bg, "pend")
	want(err, wrong)
	_, _, err = e.s.Remove(bg, "boss") // the hostmaster cannot be removed
	want(err, wrong)
	_, err = e.s.Approve(bg, "boss")
	want(err, wrong)
	_, err = e.s.Approve(bg, "ghost")
	want(err, ErrNoSuchAccount)
	_, err = e.s.Reject(bg, "ghost")
	want(err, ErrNoSuchAccount)
	_, _, err = e.s.Remove(bg, "ghost")
	want(err, ErrNoSuchAccount)
	if has, _ := e.s.HasHostmaster(bg); !has {
		t.Fatal("hostmaster vanished")
	}
}

func TestCapAtRegistrationAndApproval(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	if _, err := e.s.SetMaxAccounts(bg, 2); err != nil {
		t.Fatal(err)
	}
	e.active("a") // boss + a = 2 = cap
	if _, err := e.s.Register(bg, "b", ""); err != ErrRegistrationRefused {
		t.Fatalf("register at cap: %v", err)
	}
	// Approval re-checks the cap: raise it, register two, lower it again.
	e.s.SetMaxAccounts(bg, 4)
	e.pending("b")
	e.pending("c")
	e.s.SetMaxAccounts(bg, 3)
	if _, err := e.s.Approve(bg, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.Approve(bg, "c"); err != ErrAccountLimit {
		t.Fatalf("approve past the cap: %v", err)
	}
	// Lowering the cap below the current count removes nobody but blocks more.
	if _, err := e.s.SetMaxAccounts(bg, 1); err != nil {
		t.Fatal(err)
	}
	var n int
	e.s.db.QueryRow("SELECT COUNT(*) FROM accounts WHERE status = 'active'").Scan(&n)
	if n != 3 {
		t.Fatalf("active = %d", n)
	}
	if _, err := e.s.Register(bg, "d", ""); err != ErrRegistrationRefused {
		t.Fatalf("register over a lowered cap: %v", err)
	}
	if _, err := e.s.Approve(bg, "c"); err != ErrAccountLimit {
		t.Fatalf("approve over a lowered cap: %v", err)
	}
}

func TestDefaultCapIsFive(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	for i := 1; i <= 4; i++ {
		e.active(fmt.Sprintf("u%d", i))
	}
	if _, err := e.s.Register(bg, "u5", ""); err != ErrRegistrationRefused {
		t.Fatalf("the 6th active account was allowed: %v", err)
	}
}

func TestPendingLimit(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	e.s.SetMaxAccounts(bg, 100)
	for i := 0; i < protocol.PendingLimit; i++ {
		e.pending(fmt.Sprintf("p%d", i))
	}
	if _, err := e.s.Register(bg, "extra", ""); err != ErrRegistrationRefused {
		t.Fatalf("pending limit not enforced: %v", err)
	}
	if !strings.Contains(e.logs.String(), "too many pending") {
		t.Fatalf("reason not logged: %s", e.logs.String())
	}
}

func TestPendingExpiryWithFakeClock(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	p := e.pending("late")
	e.clk.Advance(protocol.PendingExpiry - time.Minute)
	if got := e.status(p.Credential); got != protocol.AccountPending {
		t.Fatalf("before expiry: %s", got)
	}
	e.clk.Advance(2 * time.Minute)
	// Reported as expired even before any sweep.
	if got := e.status(p.Credential); got != protocol.AccountExpired {
		t.Fatalf("after expiry: %s", got)
	}
	// Approving past expiry, before any sweep, is no_such_account and retires it.
	if _, err := e.s.Approve(bg, "late"); err != ErrNoSuchAccount {
		t.Fatalf("approve after expiry: %v", err)
	}
	var outcome string
	if err := e.s.db.QueryRow("SELECT outcome FROM retired").Scan(&outcome); err != nil || outcome != "expired" {
		t.Fatalf("retired outcome %q %v", outcome, err)
	}
	// The name is free again.
	e.pending("late")
}

func TestExpiryFreesPendingSlotsOnRegister(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	e.s.SetMaxAccounts(bg, 100)
	for i := 0; i < protocol.PendingLimit; i++ {
		e.pending(fmt.Sprintf("p%d", i))
	}
	e.clk.Advance(protocol.PendingExpiry + time.Second)
	e.pending("fresh") // lazy expiry makes room without a sweep
}

func TestIdenticalRefusalsAndLoggedReasons(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	e.active("alice")
	e.s.SetMaxAccounts(bg, 100)
	for i := 0; i < protocol.PendingLimit; i++ {
		e.pending(fmt.Sprintf("p%d", i))
	}
	cases := []struct{ name, reason string }{
		{"bad name!", "invalid name"},
		{"ALICE", "name taken"},
		{"p0", "name taken"},
		{"newcomer", "too many pending"},
	}
	var refusals []*Error
	for _, c := range cases {
		_, err := e.s.Register(bg, c.name, "")
		var be *Error
		if !errors.As(err, &be) {
			t.Fatalf("%s: %v", c.name, err)
		}
		refusals = append(refusals, be)
		if !strings.Contains(e.logs.String(), c.reason) {
			t.Errorf("%s: reason %q not logged", c.name, c.reason)
		}
	}
	// Active cap, from a state with room for pending rows.
	e2 := newAcctEnv(t)
	e2.claim("boss")
	e2.s.SetMaxAccounts(bg, 1)
	_, err := e2.s.Register(bg, "late", "")
	var be *Error
	if !errors.As(err, &be) {
		t.Fatal(err)
	}
	refusals = append(refusals, be)
	if !strings.Contains(e2.logs.String(), "active account limit") {
		t.Errorf("cap reason not logged: %s", e2.logs.String())
	}
	for _, r := range refusals {
		if r.Status != 403 || r.Code != protocol.CodeRegistrationRefused || r.Message != refusals[0].Message {
			t.Fatalf("refusals differ: %+v vs %+v", r, refusals[0])
		}
	}
	// An invalid name is logged by length, never by content.
	if strings.Contains(e.logs.String(), "bad name!") {
		t.Fatal("an invalid name was written to the log")
	}
	if !strings.Contains(e.logs.String(), "name_len=9") {
		t.Fatalf("name length not logged: %s", e.logs.String())
	}
}

func TestNameReuseAfterRetirement(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	e.s.SetMaxAccounts(bg, 100)
	e.pending("rej")
	e.s.Reject(bg, "rej")
	e.pending("rej")
	e.s.Reject(bg, "rej")
	e.pending("exp")
	e.clk.Advance(protocol.PendingExpiry + time.Second)
	e.pending("exp")
	e.active("rem")
	e.s.Remove(bg, "rem")
	e.pending("rem")
}

func TestRetiredStatusAndPruning(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	r := e.pending("x")
	e.s.Reject(bg, "x")
	if got := e.status(r.Credential); got != protocol.AccountRejected {
		t.Fatal(got)
	}
	e.clk.Advance(protocol.RetiredKeep - time.Hour)
	if got := e.status(r.Credential); got != protocol.AccountRejected {
		t.Fatalf("still within keep: %s", got)
	}
	e.clk.Advance(2 * time.Hour)
	if _, err := e.s.AccountByCredential(bg, r.Credential); err != ErrBadCredential {
		t.Fatalf("past keep, before prune: %v", err)
	}
	if err := e.s.PruneRetired(bg); err != nil {
		t.Fatal(err)
	}
	var n int
	e.s.db.QueryRow("SELECT COUNT(*) FROM retired").Scan(&n)
	if n != 0 {
		t.Fatalf("%d retired rows left", n)
	}
	if _, err := e.s.AccountByCredential(bg, "nonsense"); err != ErrBadCredential {
		t.Fatal(err)
	}
}

func TestRecoveryReplacesHostmaster(t *testing.T) {
	e := newAcctEnv(t)
	old := e.claim("boss")
	bob := e.active("bob")
	sBoss, sBob := e.session(old.Credential), e.session(bob.Credential)
	claimAs(t, e.s, sBoss, "boss-s")
	claimAs(t, e.s, sBob, "bob-s")
	mustSend(t, e.s, sBoss, "bob-s", "hello")
	e.pending("pend")

	code, mode, err := e.s.IssueClaim(bg)
	if err != nil || mode != protocol.ClaimModeRecover {
		t.Fatalf("mode %q %v", mode, err)
	}
	res, err := e.s.Register(bg, "boss", code) // the same name is allowed
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !res.Hostmaster || len(res.DeletedSessions) != 1 || res.DeletedSessions[0] != sBoss {
		t.Fatalf("recover result %+v", res)
	}
	if got := e.status(old.Credential); got != protocol.AccountRemoved {
		t.Fatalf("old hostmaster = %s", got)
	}
	r := sync0(t, e.s, sBob, protocol.SyncRequest{})
	if len(r.Messages) == 0 || r.Messages[len(r.Messages)-1].Kind != protocol.KindPeerRemoved {
		t.Fatalf("peer of the old hostmaster: %+v", r)
	}
	if err := e.s.Send(bg, sBob, "boss-s", "x"); !errors.Is(err, ErrPeerRemoved) {
		t.Fatalf("send to the removed hostmaster's name: %v", err)
	}
	var hosts int
	e.s.db.QueryRow("SELECT COUNT(*) FROM accounts WHERE is_hostmaster = 1").Scan(&hosts)
	if hosts != 1 {
		t.Fatalf("%d hostmasters", hosts)
	}
	if !strings.Contains(e.logs.String(), "hostmaster replaced by recovery") {
		t.Fatalf("recovery not logged: %s", e.logs.String())
	}
	// The recovery code is single-use.
	if _, err := e.s.Register(bg, "again", code); err != ErrRegistrationRefused {
		t.Fatalf("reused recovery code: %v", err)
	}
}

func TestClaimNameChecks(t *testing.T) {
	e := newAcctEnv(t)
	code, _, _ := e.s.IssueClaim(bg)
	if _, err := e.s.Register(bg, "bad name", code); err != ErrRegistrationRefused {
		t.Fatalf("invalid name in a claim: %v", err)
	}
	// A refused claim does not consume the code.
	if _, err := e.s.Register(bg, "boss", code); err != nil {
		t.Fatalf("code consumed by a refused claim: %v", err)
	}
	e.pending("carol")
	code, _, _ = e.s.IssueClaim(bg)
	if _, err := e.s.Register(bg, "CAROL", code); err != ErrRegistrationRefused {
		t.Fatalf("recovery claim with a pending account's name: %v", err)
	}
	// The failed recovery rolled back: the old hostmaster is intact.
	var n int
	e.s.db.QueryRow("SELECT COUNT(*) FROM accounts WHERE name = 'boss' AND is_hostmaster = 1").Scan(&n)
	if n != 1 {
		t.Fatal("a refused recovery claim removed the hostmaster")
	}
	e.active("dave")
	code, _, _ = e.s.IssueClaim(bg)
	if _, err := e.s.Register(bg, "dave", code); err != ErrRegistrationRefused {
		t.Fatalf("recovery claim with an active account's name: %v", err)
	}
}

func TestRegistrationPendingOnlyToSubscribedSessions(t *testing.T) {
	e := newAcctEnv(t)
	boss := e.claim("boss")
	sub, other := e.session(boss.Credential), e.session(boss.Credential)
	user := e.active("user")
	userSess := e.session(user.Credential)
	if _, err := e.s.SetAdminSub(bg, userSess, true); err != ErrForbidden {
		t.Fatalf("non-hostmaster subscribe: %v", err)
	}
	if n, err := e.s.SetAdminSub(bg, sub, true); err != nil || n != 0 {
		t.Fatalf("subscribe: %d %v", n, err)
	}
	e.pending("newbie")
	rs := sync0(t, e.s, sub, protocol.SyncRequest{})
	if len(rs.Messages) != 1 || rs.Messages[0].Kind != protocol.KindRegistrationPending ||
		rs.Messages[0].Text != "newbie" || rs.Messages[0].From != nil || rs.Messages[0].FromAccount != nil {
		t.Fatalf("subscribed: %+v", rs.Messages)
	}
	if !rs.AdminSubscribed {
		t.Fatal("AdminSubscribed not set")
	}
	if ro := sync0(t, e.s, other, protocol.SyncRequest{}); len(ro.Messages) != 0 || ro.AdminSubscribed {
		t.Fatalf("unsubscribed session: %+v", ro)
	}
	// The count of earlier requests comes back on subscribe, events are not replayed.
	if n, _ := e.s.SetAdminSub(bg, other, true); n != 1 {
		t.Fatalf("pending count = %d", n)
	}
	if ro := sync0(t, e.s, other, protocol.SyncRequest{}); len(ro.Messages) != 0 {
		t.Fatalf("replayed: %+v", ro.Messages)
	}
	// Unsubscribing stops delivery.
	e.s.SetAdminSub(bg, sub, false)
	e.pending("another")
	rs = sync0(t, e.s, sub, protocol.SyncRequest{After: rs.Messages[0].ID})
	if len(rs.Messages) != 0 || rs.AdminSubscribed {
		t.Fatalf("after unsubscribe: %+v", rs)
	}
}

func TestSetAdminSubBumpsSessionVersion(t *testing.T) {
	e := newAcctEnv(t)
	boss := e.claim("boss")
	sess := e.session(boss.Credential)
	before := sync0(t, e.s, sess, protocol.SyncRequest{}).Version
	wake, cancel := e.s.Notify().Subscribe(sess)
	defer cancel()
	if _, err := e.s.SetAdminSub(bg, sess, true); err != nil {
		t.Fatal(err)
	}
	recv(t, wake)
	if after := sync0(t, e.s, sess, protocol.SyncRequest{}).Version; after <= before {
		t.Fatalf("version %d -> %d", before, after)
	}
}

func TestListAccounts(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	e.active("a")
	e.pending("b")
	l, err := e.s.ListAccounts(bg)
	if err != nil {
		t.Fatal(err)
	}
	if l.MaxAccounts != protocol.DefaultMaxAccounts || l.Active != 2 || l.Pending != 1 || l.PendingLimit != protocol.PendingLimit || len(l.Accounts) != 3 {
		t.Fatalf("%+v", l)
	}
	if !l.Accounts[0].Hostmaster || l.Accounts[0].Name != "boss" {
		t.Fatalf("hostmaster first: %+v", l.Accounts)
	}
	var sawPending bool
	for _, a := range l.Accounts {
		if a.Status == protocol.AccountPending {
			sawPending = a.ExpiresAt != nil
		} else if a.ExpiresAt != nil {
			t.Fatalf("expires_at on %+v", a)
		}
	}
	if !sawPending {
		t.Fatal("pending account without expires_at")
	}
	e.clk.Advance(protocol.PendingExpiry + time.Second)
	if l, _ = e.s.ListAccounts(bg); l.Pending != 0 || len(l.Accounts) != 2 {
		t.Fatalf("expired entry still listed: %+v", l)
	}
}

func TestSetMaxAccountsBounds(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	for _, n := range []int{0, -3, protocol.MaxAccountsCeiling + 1} {
		_, err := e.s.SetMaxAccounts(bg, n)
		var be *Error
		if !errors.As(err, &be) || be.Code != protocol.CodeBadRequest {
			t.Errorf("SetMaxAccounts(%d) = %v", n, err)
		}
	}
	for _, n := range []int{1, protocol.MaxAccountsCeiling} {
		if _, err := e.s.SetMaxAccounts(bg, n); err != nil {
			t.Errorf("SetMaxAccounts(%d) = %v", n, err)
		}
	}
}

func TestUnknownCredentialAndBadSessionAccount(t *testing.T) {
	e := newAcctEnv(t)
	if _, _, err := e.s.SessionAccount(bg, "nope"); err != ErrBadCredential {
		t.Fatal(err)
	}
	boss := e.claim("boss")
	// A session token never authenticates as a credential.
	sess := e.session(boss.Credential)
	var tok string
	_ = sess
	e.s.db.QueryRow("SELECT hex(token_hash) FROM sessions WHERE id = ?", sess).Scan(&tok)
	if _, err := e.s.AccountByCredential(bg, tok); err != ErrBadCredential {
		t.Fatal(err)
	}
}

func TestRemoveDiscardsNameAndInbox(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	u := e.active("user")
	su := e.session(u.Credential)
	claimAs(t, e.s, su, "user-s")
	other := e.active("other")
	so := e.session(other.Credential)
	claimAs(t, e.s, so, "other-s")
	mustSend(t, e.s, so, "user-s", "queued text")
	if _, _, err := e.s.Remove(bg, "user"); err != nil {
		t.Fatal(err)
	}
	var n int
	e.s.db.QueryRow("SELECT COUNT(*) FROM inbox WHERE session_id = ?", su).Scan(&n)
	if n != 0 {
		t.Fatalf("%d inbox rows left behind", n)
	}
	e.s.db.QueryRow("SELECT COUNT(*) FROM sessions WHERE name_key = 'user-s'").Scan(&n)
	if n != 0 {
		t.Fatal("the removed account's name is still held")
	}
	claimAs(t, e.s, e.session(e.active("third").Credential), "user-s")
}

func TestRemoveTellsPartnersPeerRemovedAndFreesTheirNames(t *testing.T) {
	e := newAcctEnv(t)
	e.claim("boss")
	u1, u2, u3 := e.active("u1"), e.active("u2"), e.active("u3")
	s1, s2, s3 := e.session(u1.Credential), e.session(u2.Credential), e.session(u3.Credential)
	claimAs(t, e.s, s1, "u1-s")
	claimAs(t, e.s, s2, "u2-s")
	claimAs(t, e.s, s3, "u3-s") // never talks to u1
	mustSend(t, e.s, s1, "u2-s", "hello")
	mustSend(t, e.s, s2, "u1-s", "hi")
	if _, _, err := e.s.Remove(bg, "u1"); err != nil {
		t.Fatal(err)
	}
	got := inboxOf(t, e.s, s2)
	last := got[len(got)-1]
	if last.Kind != protocol.KindPeerRemoved || !protocol.IsWaking(last.Kind) || strp(last.From) != "u1-s" || strp(last.FromAccount) != "u1" {
		t.Fatalf("partner not told: %+v", got)
	}
	if got := inboxOf(t, e.s, s3); len(got) != 0 {
		t.Fatalf("a stranger was told: %+v", got)
	}
	if err := e.s.Send(bg, s2, "u1-s", "x"); !errors.Is(err, ErrPeerRemoved) {
		t.Fatalf("former partner: %v, want ErrPeerRemoved", err)
	}
	wantCode(t, e.s.Send(bg, s3, "u1-s", "x"), &Error{Code: protocol.CodeUnknownPeer})
	// The name is free again.
	claimAs(t, e.s, e.session(e.active("u4").Credential), "u1-s")
}
