package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

var bg = context.Background()

func mustCreate(t *testing.T, s *Store, sess int64, label string) (string, string) {
	t.Helper()
	id, h, err := s.CreateChannel(bg, sess, label)
	if err != nil {
		t.Fatal(err)
	}
	return id, h
}

func mustJoin(t *testing.T, s *Store, sess int64, id, label string) (string, string) {
	t.Helper()
	h, p, err := s.JoinChannel(bg, sess, id, label)
	if err != nil {
		t.Fatal(err)
	}
	return h, p
}

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

func TestConcurrentJoinsExactlyOneSucceeds(t *testing.T) {
	s, _ := openTestStore(t)
	creator := newSession(t, s)
	id, _ := mustCreate(t, s, creator, "h/creator")
	const n = 6
	sessions := make([]int64, n)
	for i := range sessions {
		sessions[i] = newSession(t, s)
	}
	var wg sync.WaitGroup
	results := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, results[i] = s.JoinChannel(bg, sessions[i], id, "h/joiner"+string(rune('a'+i)))
		}(i)
	}
	wg.Wait()
	ok := 0
	for _, err := range results {
		if err == nil {
			ok++
		} else {
			wantCode(t, err, ErrNoOpen)
		}
	}
	if ok != 1 {
		t.Fatalf("%d joins succeeded, want 1", ok)
	}
}

func TestGenericJoinErrors(t *testing.T) {
	s, _ := openTestStore(t)
	a, b, c := newSession(t, s), newSession(t, s), newSession(t, s)
	wantCode(t, func() error { _, _, e := s.JoinChannel(bg, b, "00000000-0000-4000-8000-000000000000", "h/b"); return e }(), ErrNoOpen)

	// expired
	idExp, _ := mustCreate(t, s, a, "h/a")
	if _, err := s.db.Exec("UPDATE channels SET state = 'expired'"); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.JoinChannel(bg, b, idExp, "h/b")
	wantCode(t, err, ErrNoOpen)

	// joined (full), including with the creator's label
	a2 := newSession(t, s)
	idJ, _ := mustCreate(t, s, a2, "h/a2")
	mustJoin(t, s, b, idJ, "h/b")
	_, _, err = s.JoinChannel(bg, c, idJ, "h/c")
	wantCode(t, err, ErrNoOpen)
	_, _, err = s.JoinChannel(bg, c, idJ, "h/a2")
	wantCode(t, err, ErrNoOpen)

	// ended
	if err := s.Leave(bg, b, "h/a2"); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.JoinChannel(bg, c, idJ, "h/c")
	wantCode(t, err, ErrNoOpen)

	// deleted: both left
	if err := s.Leave(bg, a2, "h/b"); err != nil {
		t.Fatal(err)
	}
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM channels WHERE state IN ('joined','ended')").Scan(&n)
	if n != 0 {
		t.Fatal("channel not deleted")
	}
	_, _, err = s.JoinChannel(bg, c, idJ, "h/c")
	wantCode(t, err, ErrNoOpen)
}

func TestSameLabelRules(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id, _ := mustCreate(t, s, a, "h/a")
	_, _, err := s.JoinChannel(bg, a, id, "h/other")
	wantCode(t, err, ErrSameLabel)
	_, _, err = s.JoinChannel(bg, b, id, "h/a")
	wantCode(t, err, ErrSameLabel)
	_, _, err = s.JoinChannel(bg, b, id, `bad"label/x`)
	wantCode(t, err, ErrBadLabel)
	if _, _, err = s.CreateChannel(bg, a, "nolabel"); err == nil {
		t.Fatal("bad label accepted on create")
	}
	// still joinable afterwards
	mustJoin(t, s, b, id, "h/b")
}

func TestDuplicatePeerCreatorSide(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id1, _ := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id1, "h/b") // a's session now has a joined channel with peer h/b
	id3, _ := mustCreate(t, s, a, "h/a3")
	_, _, err := s.JoinChannel(bg, b, id3, "h/b")
	wantCode(t, err, ErrDuplicate)
}

func TestDuplicatePeerJoinerSide(t *testing.T) {
	s, _ := openTestStore(t)
	a, b, c := newSession(t, s), newSession(t, s), newSession(t, s)
	id1, _ := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id1, "h/b") // b's peer label is h/a
	idC, _ := mustCreate(t, s, c, "h/a")
	_, _, err := s.JoinChannel(bg, b, idC, "h/b2")
	wantCode(t, err, ErrDuplicate)
}

func TestRelinkAfterLeaveAndLabelResolution(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id1, ha1 := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id1, "h/b")
	if err := s.Leave(bg, b, "h/a"); err != nil {
		t.Fatal(err)
	}
	// a sees peer_left; sending to the ended channel fails and stores nothing
	if err := s.Send(bg, a, "h/b", "x"); err == nil {
		t.Fatal("send to ended channel succeeded")
	} else {
		wantCode(t, err, ErrPeerLeft)
	}
	// the same two sessions link again
	id2, ha2 := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id2, "h/b")
	if ha1 == ha2 {
		t.Fatal("handles collided within a session")
	}
	// label now resolves to the joined channel
	if err := s.Send(bg, a, "h/b", "hello"); err != nil {
		t.Fatalf("send by label: %v", err)
	}
	// the ended one is reachable by handle
	wantCode(t, s.Send(bg, a, ha1, "x"), ErrPeerLeft)
	r := sync0(t, s, b, protocol.SyncRequest{})
	var texts int
	for _, m := range r.Messages {
		if m.Kind == protocol.KindText {
			texts++
			if m.Text != "hello" {
				t.Fatalf("unexpected text %q", m.Text)
			}
		}
	}
	if texts != 1 {
		t.Fatalf("b got %d texts, want 1", texts)
	}
}

func TestHeldMessagesDeliveredAfterJoin(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id, ha := mustCreate(t, s, a, "h/a")
	// b has already seen some inbox rows from another channel
	other := newSession(t, s)
	idO, _ := mustCreate(t, s, other, "h/o")
	mustJoin(t, s, b, idO, "h/b0")
	for i := 0; i < 3; i++ {
		if err := s.Send(bg, other, "h/b0", "earlier"); err != nil {
			t.Fatal(err)
		}
	}
	seen := sync0(t, s, b, protocol.SyncRequest{})
	hi := seen.Messages[len(seen.Messages)-1].ID

	if err := s.Send(bg, a, ha, "first"); err != nil {
		t.Fatal(err)
	}
	if err := s.Send(bg, a, ha, "second"); err != nil {
		t.Fatal(err)
	}
	mustJoin(t, s, b, id, "h/b")
	r := sync0(t, s, b, protocol.SyncRequest{After: hi})
	if len(r.Messages) != 2 || r.Messages[0].Text != "first" || r.Messages[1].Text != "second" ||
		r.Messages[0].ID <= hi || r.Messages[1].ID <= r.Messages[0].ID {
		t.Fatalf("held rows: %+v (hi %d)", r.Messages, hi)
	}
	if r.Messages[0].FromLabel == nil || *r.Messages[0].FromLabel != "h/a" {
		t.Fatal("held row lost its sender")
	}
	// a got peer_joined, and nothing is left in held
	ra := sync0(t, s, a, protocol.SyncRequest{})
	if k := kinds(ra); len(k) != 1 || k[0] != protocol.KindPeerJoined {
		t.Fatalf("creator rows: %v", k)
	}
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM held").Scan(&n)
	if n != 0 {
		t.Fatal("held rows left behind")
	}
}

func TestCreatorLeavingOpenChannelDeletesIt(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id, ha := mustCreate(t, s, a, "h/a")
	if err := s.Send(bg, a, ha, "held"); err != nil {
		t.Fatal(err)
	}
	if err := s.Leave(bg, a, ha); err != nil {
		t.Fatal(err)
	}
	var nc, nh int
	s.db.QueryRow("SELECT COUNT(*) FROM channels").Scan(&nc)
	s.db.QueryRow("SELECT COUNT(*) FROM held").Scan(&nh)
	if nc != 0 || nh != 0 {
		t.Fatalf("channels=%d held=%d after creator left", nc, nh)
	}
	_, _, err := s.JoinChannel(bg, b, id, "h/b")
	wantCode(t, err, ErrNoOpen)
}

func TestSendToEndedStoresNothing(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id, _ := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id, "h/b")
	if err := s.Leave(bg, a, "h/b"); err != nil {
		t.Fatal(err)
	}
	wantCode(t, s.Send(bg, b, "h/a", "hi"), ErrPeerLeft)
	var held int
	s.db.QueryRow("SELECT COUNT(*) FROM held").Scan(&held)
	if held != 0 {
		t.Fatal("held row stored")
	}
	after := sync0(t, s, a, protocol.SyncRequest{})
	for _, m := range after.Messages {
		if m.Kind == protocol.KindText {
			t.Fatal("text stored for the leaver")
		}
	}
}

func TestSendByHandleAndLabelRightAfterJoin(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id, ha := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id, "h/b")
	if err := s.Send(bg, a, "h/b", "by label"); err != nil {
		t.Fatal(err)
	}
	if err := s.Send(bg, a, ha, "by handle"); err != nil {
		t.Fatal(err)
	}
	r := sync0(t, s, b, protocol.SyncRequest{})
	if len(r.Messages) != 2 || r.Messages[0].Text != "by label" || r.Messages[1].Text != "by handle" {
		t.Fatalf("rows: %+v", r.Messages)
	}
}

func TestUnknownPeerListsKnownPeers(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	id, ha := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id, "h/b")
	err := s.Send(bg, a, "h/nobody", "x")
	var e *Error
	if !errors.As(err, &e) || e.Code != protocol.CodeUnknownPeer {
		t.Fatalf("got %v", err)
	}
	if len(e.KnownPeers) != 1 || e.KnownPeers[0].Handle != ha || e.KnownPeers[0].Label != "h/b" ||
		e.KnownPeers[0].State != protocol.StateConnected {
		t.Fatalf("known peers: %+v", e.KnownPeers)
	}
	wantCode(t, s.Leave(bg, a, "nonexistent"), &Error{Code: protocol.CodeUnknownPeer})
}

func TestSendValidation(t *testing.T) {
	s, _ := openTestStore(t)
	a := newSession(t, s)
	_, ha := mustCreate(t, s, a, "h/a")
	wantCode(t, s.Send(bg, a, ha, ""), ErrEmpty)
	big := make([]byte, protocol.MaxMessageBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	wantCode(t, s.Send(bg, a, ha, string(big)), errTooLarge())
	if err := s.Send(bg, a, ha, string(big[:protocol.MaxMessageBytes])); err != nil {
		t.Fatalf("exact-limit text rejected: %v", err)
	}
}

func TestCreateChannelBumpsVersion(t *testing.T) {
	s, _ := openTestStore(t)
	a := newSession(t, s)
	v0 := sync0(t, s, a, protocol.SyncRequest{}).Version
	_, ha := mustCreate(t, s, a, "h/a")
	r := sync0(t, s, a, protocol.SyncRequest{})
	if r.Version != v0+1 || len(r.Channels) != 1 || r.Channels[0].Handle != ha ||
		r.Channels[0].State != protocol.StateWaiting || r.Channels[0].PeerLabel != nil {
		t.Fatalf("version %d->%d channels %+v", v0, r.Version, r.Channels)
	}
}

func TestSendByBareName(t *testing.T) {
	s, _ := openTestStore(t)
	a, b, c := newSession(t, s), newSession(t, s), newSession(t, s)
	id1, _ := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, b, id1, "h1/bob")
	if err := s.Send(bg, a, "bob", "hi"); err != nil {
		t.Fatalf("bare name: %v", err)
	}
	if r := sync0(t, s, b, protocol.SyncRequest{}); len(r.Messages) != 1 || r.Messages[0].Text != "hi" {
		t.Fatalf("rows: %+v", r.Messages)
	}
	// a second peer with the same name on another host is ambiguous
	id2, _ := mustCreate(t, s, a, "h/a")
	mustJoin(t, s, c, id2, "h2/bob")
	var e *Error
	if err := s.Send(bg, a, "bob", "x"); !errors.As(err, &e) || e.Code != protocol.CodeUnknownPeer || !strings.Contains(e.Message, "host/name") {
		t.Fatalf("ambiguous: %v", err)
	}
	if err := s.Send(bg, a, "h2/bob", "ok"); err != nil {
		t.Fatalf("full label: %v", err)
	}
}
