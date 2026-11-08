package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

var bg = context.Background()

func wantCode(t *testing.T, err error, want *Error) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != want.Code {
		t.Fatalf("got error %v, want code %s", err, want.Code)
	}
}

func kinds(r protocol.SyncResponse) []string {
	var out []string
	for _, m := range r.Messages {
		out = append(out, m.Kind)
	}
	return out
}

func strp(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// claimAs claims name for the session and checks that it was granted as asked.
func claimAs(t *testing.T, s *Store, session int64, name string) {
	t.Helper()
	got, err := s.ClaimName(bg, session, name)
	if err != nil {
		t.Fatalf("claim %q: %v", name, err)
	}
	if got != name {
		t.Fatalf("claim %q returned %q", name, got)
	}
}

// named creates an account called "acct-<name>" with a session that holds name.
func named(t *testing.T, s *Store, name string) int64 {
	t.Helper()
	id, _ := newSessionFor(t, s, "acct-"+name)
	claimAs(t, s, id, name)
	return id
}

func mustSend(t *testing.T, s *Store, from int64, to, text string) {
	t.Helper()
	if err := s.Send(bg, from, to, text); err != nil {
		t.Fatalf("send to %q: %v", to, err)
	}
}

func inboxOf(t *testing.T, s *Store, session int64) []protocol.Message {
	t.Helper()
	return sync0(t, s, session, protocol.SyncRequest{}).Messages
}

func countKind(msgs []protocol.Message, kind string) int {
	n := 0
	for _, m := range msgs {
		if m.Kind == kind {
			n++
		}
	}
	return n
}

func TestClaimNameGrantsAndSyncReportsIt(t *testing.T) {
	s, _ := openTestStore(t)
	a, _ := newSessionFor(t, s, "liz")
	if r := sync0(t, s, a, protocol.SyncRequest{}); r.Name != "" {
		t.Fatalf("an unnamed session reports name %q", r.Name)
	}
	claimAs(t, s, a, "Bob")
	if r := sync0(t, s, a, protocol.SyncRequest{}); r.Name != "Bob" {
		t.Fatalf("sync name = %q, want the holder's spelling Bob", r.Name)
	}
}

func TestClaimNameSameNameIsANoOpInAnyCase(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "bob")
	before := sync0(t, s, a, protocol.SyncRequest{}).Version
	for _, again := range []string{"bob", "Bob", "BOB"} {
		got, err := s.ClaimName(bg, a, again)
		if err != nil || got != "bob" {
			t.Fatalf("claim %q again = %q, %v; want the held spelling bob", again, got, err)
		}
	}
	if after := sync0(t, s, a, protocol.SyncRequest{}).Version; after != before {
		t.Fatalf("a no-op claim bumped the version: %d -> %d", before, after)
	}
}

func TestClaimNameDifferentNameWhileHoldingIsRefused(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "bob")
	_, err := s.ClaimName(bg, a, "carol")
	wantCode(t, err, &Error{Code: protocol.CodeNameHeld})
	var e *Error
	errors.As(err, &e)
	if e.Status != 409 || !strings.Contains(e.Message, `"bob"`) {
		t.Fatalf("name_held must be a 409 naming the held name: %+v", e)
	}
	if r := sync0(t, s, a, protocol.SyncRequest{}); r.Name != "bob" {
		t.Fatalf("a refused claim changed the name to %q", r.Name)
	}
}

func TestClaimNameTakenIsCaseInsensitiveAndRevealsNoAccount(t *testing.T) {
	s, _ := openTestStore(t)
	named(t, s, "bob") // account acct-bob
	b, _ := newSessionFor(t, s, "mallory")
	for _, name := range []string{"bob", "Bob", "BOB"} {
		_, err := s.ClaimName(bg, b, name)
		wantCode(t, err, &Error{Code: protocol.CodeNameTaken})
		var e *Error
		errors.As(err, &e)
		if e.Status != 409 || strings.Contains(e.Message, "acct-bob") || strings.Contains(e.Message, "mallory") {
			t.Fatalf("name_taken must be 409 and name no account: %+v", e)
		}
	}
}

func TestClaimNameRefusesReservedAndInvalidNames(t *testing.T) {
	s, _ := openTestStore(t)
	a, _ := newSessionFor(t, s, "liz")
	for _, name := range []string{"admin", "Admin", "ADMIN", "agent-bus", "Agent-Bus"} {
		_, err := s.ClaimName(bg, a, name)
		if !errors.Is(err, ErrReservedName) {
			t.Errorf("claim %q: %v, want ErrReservedName", name, err)
		}
	}
	for _, name := range []string{"", "a b", "a/b", "é", "a<b", strings.Repeat("x", 65), "bob\n"} {
		_, err := s.ClaimName(bg, a, name)
		if !errors.Is(err, ErrBadName) {
			t.Errorf("claim %q: %v, want ErrBadName", name, err)
		}
	}
	if r := sync0(t, s, a, protocol.SyncRequest{}); r.Name != "" {
		t.Fatalf("a refused claim left name %q", r.Name)
	}
}

func TestConcurrentNameClaimsExactlyOneWins(t *testing.T) {
	s, _ := openTestStore(t)
	const n = 8
	sessions := make([]int64, n)
	for i := range sessions {
		sessions[i] = newSession(t, s)
	}
	var wg sync.WaitGroup
	results := make([]error, n)
	for i := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Alternate the case: Race and race are the same name.
			name := "race"
			if i%2 == 1 {
				name = "RACE"
			}
			_, results[i] = s.ClaimName(bg, sessions[i], name)
		}()
	}
	wg.Wait()
	wins := 0
	for _, err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrNameTaken):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("%d claims won, want exactly 1", wins)
	}
	var held int
	s.db.QueryRow("SELECT COUNT(*) FROM sessions WHERE name_key = 'race'").Scan(&held)
	if held != 1 {
		t.Fatalf("%d sessions hold the name", held)
	}
}

func TestSendWithoutANameIsRefused(t *testing.T) {
	s, _ := openTestStore(t)
	named(t, s, "bob")
	a, _ := newSessionFor(t, s, "liz")
	err := s.Send(bg, a, "bob", "hi")
	if !errors.Is(err, ErrNoName) {
		t.Fatalf("send without a name: %v", err)
	}
	var e *Error
	errors.As(err, &e)
	if e.Status != 409 || !strings.Contains(e.Message, "claim_name") {
		t.Fatalf("no_name must be a 409 pointing at claim_name: %+v", e)
	}
	if got := inboxOf(t, s, a); len(got) != 0 {
		t.Fatalf("unnamed session got rows: %+v", got)
	}
}

func TestSendToSelfIsRefusedInAnyCase(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "bob")
	for _, to := range []string{"bob", "BOB", "Bob"} {
		if err := s.Send(bg, a, to, "x"); !errors.Is(err, ErrSelfSend) {
			t.Errorf("send to %q: %v, want ErrSelfSend", to, err)
		}
	}
	if got := inboxOf(t, s, a); len(got) != 0 {
		t.Fatalf("self-send queued rows: %+v", got)
	}
}

func TestSendToAnUnheldNameFailsAndStoresNothing(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	err := s.Send(bg, a, "ghost", "hello?")
	wantCode(t, err, &Error{Code: protocol.CodeUnknownPeer})
	var e *Error
	errors.As(err, &e)
	if e.Status != 404 || !strings.Contains(e.Message, `"ghost"`) {
		t.Fatalf("unknown_peer must be a 404 naming the name: %+v", e)
	}
	for _, q := range []string{"SELECT COUNT(*) FROM inbox", "SELECT COUNT(*) FROM partners", "SELECT COUNT(*) FROM departed"} {
		var n int
		s.db.QueryRow(q).Scan(&n)
		if n != 0 {
			t.Fatalf("%s = %d after a failed send", q, n)
		}
	}
	// Reserved and malformed targets: reserved names are simply never held.
	wantCode(t, s.Send(bg, a, "admin", "x"), &Error{Code: protocol.CodeUnknownPeer})
	if err := s.Send(bg, a, "a/b", "x"); !errors.Is(err, ErrBadName) {
		t.Fatalf("send to a malformed name: %v", err)
	}
	if err := s.Send(bg, a, "ghost", ""); !errors.Is(err, ErrEmpty) {
		t.Fatalf("empty text: %v", err)
	}
	wantCode(t, s.Send(bg, a, "ghost", strings.Repeat("x", protocol.MaxMessageBytes+1)), &Error{Code: protocol.CodeTooLarge})
}

func TestSendDeliversWithSenderAccountAndFirstContactOnce(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	b := named(t, s, "bob")

	mustSend(t, s, a, "bob", "one")
	got := inboxOf(t, s, b)
	if len(got) != 1 {
		t.Fatalf("bob rows: %+v", got)
	}
	m := got[0]
	if m.Kind != protocol.KindText || m.Text != "one" || strp(m.From) != "alice" || strp(m.FromAccount) != "acct-alice" || !m.FirstContact {
		t.Fatalf("first delivery: %+v (from %s, account %s)", m, strp(m.From), strp(m.FromAccount))
	}

	mustSend(t, s, a, "BOB", "two") // the name is matched in any case
	mustSend(t, s, b, "alice", "reply")
	bob := inboxOf(t, s, b)
	if len(bob) != 2 || bob[1].Text != "two" || bob[1].FirstContact {
		t.Fatalf("second delivery must not be first contact: %+v", bob)
	}
	alice := inboxOf(t, s, a)
	if len(alice) != 1 || alice[0].Text != "reply" || alice[0].FirstContact || strp(alice[0].From) != "bob" || strp(alice[0].FromAccount) != "acct-bob" {
		t.Fatalf("the reply must not be first contact: %+v", alice)
	}
	var partners int
	s.db.QueryRow("SELECT COUNT(*) FROM partners").Scan(&partners)
	if partners != 1 {
		t.Fatalf("%d partner rows for one pair", partners)
	}
}

func TestSendWakesTheRecipient(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	b := named(t, s, "bob")
	wake, cancel := s.Notify().Subscribe(b)
	defer cancel()
	mustSend(t, s, a, "bob", "hi")
	recv(t, wake)
}

func TestFirstContactIsPerSenderSession(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	c := named(t, s, "carol")
	b := named(t, s, "bob")
	mustSend(t, s, a, "bob", "from a")
	mustSend(t, s, c, "bob", "from c")
	got := inboxOf(t, s, b)
	if len(got) != 2 || !got[0].FirstContact || !got[1].FirstContact {
		t.Fatalf("each new sender must be first contact: %+v", got)
	}
}

func TestReleaseNameTellsPartnersAndDiscardsPeerRows(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	b := named(t, s, "bob")
	c := named(t, s, "carol") // never talked to alice
	mustSend(t, s, a, "bob", "hello")
	mustSend(t, s, b, "alice", "hi back") // unread on alice's side
	// An admin row on alice's inbox belongs to the hostmaster subscription, not
	// to her name, so release must keep it (N10).
	if err := s.update(bg, func(tx *Tx) error {
		return tx.enqueue(a, nil, nil, protocol.KindRegistrationPending, "newcomer", false)
	}); err != nil {
		t.Fatal(err)
	}

	released, err := s.ReleaseName(bg, a)
	if err != nil || released != "alice" {
		t.Fatalf("release = %q, %v", released, err)
	}

	bob := inboxOf(t, s, b)
	last := bob[len(bob)-1]
	if last.Kind != protocol.KindPeerLeft || !protocol.IsWaking(last.Kind) || strp(last.From) != "alice" || strp(last.FromAccount) != "acct-alice" {
		t.Fatalf("partner did not get a waking peer_left from alice: %+v", bob)
	}
	if got := inboxOf(t, s, c); len(got) != 0 {
		t.Fatalf("a stranger was told: %+v", got)
	}
	alice := inboxOf(t, s, a)
	if len(alice) != 1 || alice[0].Kind != protocol.KindRegistrationPending {
		t.Fatalf("release must discard peer rows and keep the admin row: %+v", alice)
	}
	if r := sync0(t, s, a, protocol.SyncRequest{}); r.Name != "" {
		t.Fatalf("name after release = %q", r.Name)
	}
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM partners").Scan(&n)
	if n != 0 {
		t.Fatalf("%d partner rows survived the release", n)
	}
	// The released name is free at once, for the same session or another.
	if err := s.Send(bg, a, "bob", "x"); !errors.Is(err, ErrNoName) {
		t.Fatalf("send after release: %v", err)
	}
	claimAs(t, s, a, "alice2")
	claimAs(t, s, newSession(t, s), "alice")
}

func TestReleaseNameIsIdempotent(t *testing.T) {
	s, _ := openTestStore(t)
	a, _ := newSessionFor(t, s, "liz")
	before := sync0(t, s, a, protocol.SyncRequest{}).Version
	released, err := s.ReleaseName(bg, a)
	if err != nil || released != "" {
		t.Fatalf("release with no name = %q, %v", released, err)
	}
	claimAs(t, s, a, "bob")
	if released, err = s.ReleaseName(bg, a); err != nil || released != "bob" {
		t.Fatalf("release = %q, %v", released, err)
	}
	if released, err = s.ReleaseName(bg, a); err != nil || released != "" {
		t.Fatalf("second release = %q, %v", released, err)
	}
	if v := sync0(t, s, a, protocol.SyncRequest{}).Version; v <= before {
		t.Fatalf("claim and release must bump the version: %d -> %d", before, v)
	}
	if _, err := s.ReleaseName(bg, 9999); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("release for a missing session: %v", err)
	}
}

func TestFormerPartnerGetsPeerLeftOthersGetUnknownPeerAndNewHolderGetsFirstContact(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	b := named(t, s, "bob")
	stranger := named(t, s, "stranger")
	mustSend(t, s, a, "bob", "hello")
	if _, err := s.ReleaseName(bg, b); err != nil {
		t.Fatal(err)
	}

	// A former partner learns that bob left; anyone else learns nothing.
	if err := s.Send(bg, a, "bob", "still there?"); !errors.Is(err, ErrPeerLeft) {
		t.Fatalf("former partner: %v, want ErrPeerLeft", err)
	}
	wantCode(t, s.Send(bg, stranger, "bob", "hi"), &Error{Code: protocol.CodeUnknownPeer})

	// A different session claims bob. The former partner's next send reaches it
	// (it is a new session), flagged as first contact so its agent checks first.
	d, _ := newSessionFor(t, s, "dave")
	claimAs(t, s, d, "bob")
	mustSend(t, s, a, "bob", "hello again")
	got := inboxOf(t, s, d)
	if len(got) != 1 || got[0].Text != "hello again" || !got[0].FirstContact || strp(got[0].FromAccount) != "acct-alice" {
		t.Fatalf("new holder: %+v", got)
	}
	var departed int
	s.db.QueryRow("SELECT COUNT(*) FROM departed").Scan(&departed)
	if departed != 0 {
		t.Fatalf("%d departed rows survived the new claim", departed)
	}
}

func TestNewHolderIsNotAPartnerOfTheOldHoldersPartners(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	b := named(t, s, "bob")
	mustSend(t, s, a, "bob", "hello")
	if _, err := s.ReleaseName(bg, b); err != nil {
		t.Fatal(err)
	}
	if n := countKind(inboxOf(t, s, a), protocol.KindPeerLeft); n != 1 {
		t.Fatalf("alice got %d peer_left rows, want 1", n)
	}
	d, _ := newSessionFor(t, s, "dave")
	claimAs(t, s, d, "bob")
	if _, err := s.ReleaseName(bg, d); err != nil {
		t.Fatal(err)
	}
	if n := countKind(inboxOf(t, s, a), protocol.KindPeerLeft); n != 1 {
		t.Fatalf("the new holder's release reached an old partner: %d peer_left rows", n)
	}
}

func TestSessionEndTellsPartnersLikeARelease(t *testing.T) {
	s, _ := openTestStore(t)
	a := named(t, s, "alice")
	b := named(t, s, "bob")
	mustSend(t, s, a, "bob", "hello")
	if err := s.DeleteSelf(bg, a); err != nil {
		t.Fatal(err)
	}
	bob := inboxOf(t, s, b)
	if countKind(bob, protocol.KindPeerLeft) != 1 {
		t.Fatalf("partner not told: %+v", bob)
	}
	if err := s.Send(bg, b, "alice", "x"); !errors.Is(err, ErrPeerLeft) {
		t.Fatalf("send to an ended session: %v", err)
	}
	// The name is free again.
	claimAs(t, s, newSession(t, s), "alice")
}

func TestManyNamesDoNotCollide(t *testing.T) {
	s, _ := openTestStore(t)
	for i := 0; i < 20; i++ {
		named(t, s, fmt.Sprintf("n%d", i))
	}
	var n int
	s.db.QueryRow("SELECT COUNT(DISTINCT name_key) FROM sessions").Scan(&n)
	if n != 20 {
		t.Fatalf("%d distinct names", n)
	}
}
