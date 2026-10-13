package broker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/secret"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_800_000_000, 0)} }
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

func openTestStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"), NewNotify(), clk.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, clk
}

func newSession(t *testing.T, s *Store) int64 {
	t.Helper()
	_, id, err := s.CreateSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// rawJoined creates a joined channel between two sessions with raw SQL, so the
// store tests do not depend on channels.go. It returns the member rows.
func rawJoined(t *testing.T, s *Store, a, b int64, la, lb string) (ma, mb member, channel int64) {
	t.Helper()
	err := s.update(context.Background(), func(tx *Tx) error {
		res, err := tx.Exec("INSERT INTO channels(id_hash, created_at, state) VALUES(?,?,'joined')", secret.Hash(secret.NewChannelID()), tx.now)
		if err != nil {
			return err
		}
		channel, _ = res.LastInsertId()
		for i, x := range []struct {
			sess        int64
			label, peer string
			handle      string
		}{{a, la, lb, "ch-a"}, {b, lb, la, "ch-b"}} {
			r, err := tx.Exec("INSERT INTO members(channel_id, session_id, label, handle, peer_label) VALUES(?,?,?,?,?)",
				channel, x.sess, x.label, x.handle, x.peer)
			if err != nil {
				return err
			}
			id, _ := r.LastInsertId()
			m := member{id: id, channelID: channel, sessionID: x.sess, label: x.label, handle: x.handle}
			if i == 0 {
				ma = m
			} else {
				mb = m
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}

func enqueueText(t *testing.T, s *Store, session int64, text string) {
	t.Helper()
	from := "h/x"
	err := s.update(context.Background(), func(tx *Tx) error {
		return tx.enqueue(session, "ch-a", &from, protocol.KindText, text)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func sync0(t *testing.T, s *Store, session int64, req protocol.SyncRequest) protocol.SyncResponse {
	t.Helper()
	r, err := s.Sync(context.Background(), session, req)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestMigrationIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.db")
	s, err := OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := newSession(t, s)
	s.Close()
	s, err = OpenStore(path, nil, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	var v int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != 1 {
		t.Fatalf("user_version = %d, %v", v, err)
	}
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM sessions WHERE id = ?", id).Scan(&n)
	if n != 1 {
		t.Fatal("data lost across reopen")
	}
}

func TestForeignKeysOnEveryConnection(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	var conns []interface{ Close() error }
	for i := 0; i < 4; i++ {
		c, err := s.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		var fk int
		if err := c.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil || fk != 1 {
			t.Fatalf("conn %d: foreign_keys = %d, %v", i, fk, err)
		}
	}
	for _, c := range conns {
		c.Close()
	}
}

func TestRestrictBlocksDeletes(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	_, _, ch := rawJoined(t, s, a, b, "h/a", "h/b")
	if _, err := s.db.Exec("DELETE FROM sessions WHERE id = ?", a); err == nil {
		t.Fatal("deleting a session with members must fail")
	}
	if _, err := s.db.Exec("DELETE FROM channels WHERE id = ?", ch); err == nil {
		t.Fatal("deleting a channel with members must fail")
	}
}

func TestInboxIDsNeverReused(t *testing.T) {
	s, _ := openTestStore(t)
	a := newSession(t, s)
	enqueueText(t, s, a, "1")
	enqueueText(t, s, a, "2")
	r := sync0(t, s, a, protocol.SyncRequest{})
	last := r.Messages[1].ID
	r = sync0(t, s, a, protocol.SyncRequest{Ack: last, After: last}) // deletes both
	enqueueText(t, s, a, "3")
	r = sync0(t, s, a, protocol.SyncRequest{After: last})
	if len(r.Messages) != 1 || r.Messages[0].ID <= last {
		t.Fatalf("inbox ID reused: %+v (last %d)", r.Messages, last)
	}
}

func TestSessionIDsNeverReused(t *testing.T) {
	s, _ := openTestStore(t)
	newSession(t, s)
	hi := newSession(t, s)
	if err := s.DeleteSelf(context.Background(), hi); err != nil {
		t.Fatal(err)
	}
	next := newSession(t, s)
	if next <= hi {
		t.Fatalf("session ID reused: new %d, deleted %d", next, hi)
	}
}

func TestSyncCommitOrderedVisibility(t *testing.T) {
	s, _ := openTestStore(t)
	a := newSession(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				from := "h/x"
				if err := s.update(ctx, func(tx *Tx) error {
					return tx.enqueue(a, "ch-a", &from, protocol.KindText, "x")
				}); err != nil && ctx.Err() == nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	seen := map[int64]bool{}
	var prevMax int64
	check := func() {
		r, err := s.Sync(context.Background(), a, protocol.SyncRequest{})
		if err != nil {
			t.Fatal(err)
		}
		var mx int64
		for _, m := range r.Messages {
			if m.ID < prevMax && !seen[m.ID] {
				t.Fatalf("row %d appeared after a sync already returned %d", m.ID, prevMax)
			}
			if m.ID > mx {
				mx = m.ID
			}
		}
		for _, m := range r.Messages {
			seen[m.ID] = true
		}
		if mx > prevMax {
			prevMax = mx
		}
	}
loop:
	for {
		select {
		case <-done:
			break loop
		default:
			check()
			time.Sleep(time.Millisecond) // SQLite's write lock is unfair; don't starve the writers
		}
	}
	cancel()
	check()
	if len(seen) != 240 {
		t.Fatalf("saw %d rows, want 240", len(seen))
	}
}

func TestSyncAckScopedToCaller(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	enqueueText(t, s, a, "for a")
	enqueueText(t, s, b, "for b")
	sync0(t, s, b, protocol.SyncRequest{Ack: 1 << 40})
	r := sync0(t, s, a, protocol.SyncRequest{})
	if len(r.Messages) != 1 || r.Messages[0].Text != "for a" {
		t.Fatalf("a's rows were deleted by b's ack: %+v", r.Messages)
	}
	if r := sync0(t, s, b, protocol.SyncRequest{}); len(r.Messages) != 0 {
		t.Fatalf("b's own rows should be acked: %+v", r.Messages)
	}
}

func TestSyncAfterOrderAndMore(t *testing.T) {
	s, _ := openTestStore(t)
	a := newSession(t, s)
	chunk := strings.Repeat("x", 400<<10)
	for i := 0; i < 4; i++ {
		enqueueText(t, s, a, chunk)
	}
	r := sync0(t, s, a, protocol.SyncRequest{})
	if !r.More || len(r.Messages) < 1 || len(r.Messages) >= 4 {
		t.Fatalf("expected a partial page with more=true, got %d rows more=%v", len(r.Messages), r.More)
	}
	var total int
	after := int64(0)
	for {
		r = sync0(t, s, a, protocol.SyncRequest{After: after})
		for _, m := range r.Messages {
			if m.ID <= after {
				t.Fatalf("row %d not after %d", m.ID, after)
			}
			after = m.ID
			total++
		}
		if !r.More {
			break
		}
	}
	if total != 4 {
		t.Fatalf("paged %d rows, want 4", total)
	}
	// After the last row nothing is returned.
	if r := sync0(t, s, a, protocol.SyncRequest{After: after}); len(r.Messages) != 0 || r.More {
		t.Fatalf("unexpected rows: %+v", r)
	}
}

func TestSyncVersionAndRowsSameSnapshot(t *testing.T) {
	s, _ := openTestStore(t)
	a := newSession(t, s)
	r0 := sync0(t, s, a, protocol.SyncRequest{})
	enqueueText(t, s, a, "hi")
	r1 := sync0(t, s, a, protocol.SyncRequest{})
	if r1.Version != r0.Version+1 || len(r1.Messages) != 1 {
		t.Fatalf("version %d -> %d with %d rows", r0.Version, r1.Version, len(r1.Messages))
	}
	// An ack alone does not bump the version.
	r2 := sync0(t, s, a, protocol.SyncRequest{Ack: r1.Messages[0].ID})
	if r2.Version != r1.Version {
		t.Fatal("ack bumped the version")
	}
}

func TestTxBumpsAndBroadcastsExactlyTheBumped(t *testing.T) {
	s, _ := openTestStore(t)
	a, b, c := newSession(t, s), newSession(t, s), newSession(t, s)
	ca, cancelA := s.Notify().Subscribe(a)
	cb, cancelB := s.Notify().Subscribe(b)
	cc, cancelC := s.Notify().Subscribe(c)
	defer cancelA()
	defer cancelB()
	defer cancelC()

	boom := errors.New("boom")
	err := s.update(context.Background(), func(tx *Tx) error {
		if err := tx.bump(a); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	for name, ch := range map[string]<-chan struct{}{"a": ca, "b": cb, "c": cc} {
		select {
		case <-ch:
			t.Fatalf("rolled-back tx woke %s", name)
		default:
		}
	}
	if v := sync0(t, s, a, protocol.SyncRequest{}).Version; v != 0 {
		t.Fatalf("rolled-back bump persisted: version %d", v)
	}

	if err := s.update(context.Background(), func(tx *Tx) error {
		if err := tx.bump(a); err != nil {
			return err
		}
		return tx.bump(b)
	}); err != nil {
		t.Fatal(err)
	}
	recv(t, ca)
	recv(t, cb)
	select {
	case <-cc:
		t.Fatal("unbumped session woken")
	default:
	}
	if v := sync0(t, s, a, protocol.SyncRequest{}).Version; v != 1 {
		t.Fatalf("a version = %d", v)
	}
	if v := sync0(t, s, c, protocol.SyncRequest{}).Version; v != 0 {
		t.Fatalf("c version = %d", v)
	}
}

func TestDeleteSelf(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	a, b := newSession(t, s), newSession(t, s)
	rawJoined(t, s, a, b, "h/a", "h/b")
	enqueueText(t, s, a, "queued for a")
	wake, cancel := s.Notify().Subscribe(a)
	defer cancel()

	if err := s.DeleteSelf(ctx, a); err != nil {
		t.Fatal(err)
	}
	recv(t, wake)
	r := sync0(t, s, b, protocol.SyncRequest{})
	if len(r.Messages) != 1 || r.Messages[0].Kind != protocol.KindPeerLeft ||
		r.Messages[0].FromLabel == nil || *r.Messages[0].FromLabel != "h/a" || r.Messages[0].Handle != "ch-b" {
		t.Fatalf("peer did not get peer_left: %+v", r.Messages)
	}
	if len(r.Channels) != 1 || r.Channels[0].State != protocol.StatePeerLeft {
		t.Fatalf("channel list: %+v", r.Channels)
	}
	if _, err := s.Sync(ctx, a, protocol.SyncRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("deleted session sync: %v", err)
	}
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM inbox WHERE session_id = ?", a).Scan(&n)
	if n != 0 {
		t.Fatal("inbox survived session deletion")
	}
	// b leaves too: the channel is deleted with it.
	if err := s.DeleteSelf(ctx, b); err != nil {
		t.Fatal(err)
	}
	s.db.QueryRow("SELECT COUNT(*) FROM channels").Scan(&n)
	if n != 0 {
		t.Fatalf("%d channels left behind", n)
	}
}

func TestInboxRowsOutliveMembershipAndChannel(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	a, b := newSession(t, s), newSession(t, s)
	ma, mb, _ := rawJoined(t, s, a, b, "h/a", "h/b")
	enqueueText(t, s, a, "queued before leave")
	// Both sides remove their membership; the channel is deleted.
	if err := s.update(ctx, func(tx *Tx) error { return tx.removeMember(mb) }); err != nil {
		t.Fatal(err)
	}
	if err := s.update(ctx, func(tx *Tx) error { return tx.removeMember(ma) }); err != nil {
		t.Fatal(err)
	}
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM channels").Scan(&n)
	if n != 0 {
		t.Fatal("channel not deleted")
	}
	r := sync0(t, s, a, protocol.SyncRequest{})
	var texts []string
	for _, m := range r.Messages {
		texts = append(texts, m.Kind+":"+m.Text)
	}
	found := map[string]bool{}
	for _, m := range r.Messages {
		found[m.Kind] = true
	}
	if !found[protocol.KindText] || !found[protocol.KindPeerLeft] {
		t.Fatalf("inbox rows lost with the channel: %v", texts)
	}
}
