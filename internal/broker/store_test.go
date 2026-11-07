package broker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

var accountSeq atomic.Int64

// addAccount inserts an account row with raw SQL (so store tests do not depend
// on Register) and returns its ID and credential.
func addAccount(t *testing.T, s *Store, name, status string, hostmaster bool) (int64, string) {
	t.Helper()
	cred := secret.NewToken()
	hm := 0
	if hostmaster {
		hm = 1
	}
	res, err := s.db.Exec(
		"INSERT INTO accounts(name, name_key, cred_hash, status, is_hostmaster, created_at) VALUES(?,?,?,?,?,?)",
		name, strings.ToLower(name), secret.Hash(cred), status, hm, s.now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id, cred
}

// newSession creates a fresh active account and a session owned by it.
func newSession(t *testing.T, s *Store) int64 {
	t.Helper()
	id, _ := newSessionFor(t, s, fmt.Sprintf("user%d", accountSeq.Add(1)))
	return id
}

// newSessionFor creates an active account called name and a session for it.
func newSessionFor(t *testing.T, s *Store, name string) (session int64, account int64) {
	t.Helper()
	acc, _ := addAccount(t, s, name, protocol.AccountActive, false)
	_, id, err := s.CreateSession(context.Background(), acc)
	if err != nil {
		t.Fatal(err)
	}
	return id, acc
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
		accounts := map[int64]string{}
		for _, sess := range []int64{a, b} {
			var n string
			if err := tx.QueryRow("SELECT a.name FROM sessions s JOIN accounts a ON a.id = s.account_id WHERE s.id = ?", sess).Scan(&n); err != nil {
				return err
			}
			accounts[sess] = n
		}
		for i, x := range []struct {
			sess        int64
			label, peer string
			handle      string
		}{{a, la, lb, "ch-a"}, {b, lb, la, "ch-b"}} {
			other := a
			if x.sess == a {
				other = b
			}
			r, err := tx.Exec("INSERT INTO members(channel_id, session_id, label, handle, peer_label, account, peer_account) VALUES(?,?,?,?,?,?,?)",
				channel, x.sess, x.label, x.handle, x.peer, accounts[x.sess], accounts[other])
			if err != nil {
				return err
			}
			id, _ := r.LastInsertId()
			m := member{id: id, channelID: channel, sessionID: x.sess, label: x.label, handle: x.handle, account: accounts[x.sess]}
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
		return tx.enqueue(session, "ch-a", &from, nil, protocol.KindText, text)
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
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != 2 {
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
					return tx.enqueue(a, "ch-a", &from, nil, protocol.KindText, "x")
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
	if err := s.update(ctx, func(tx *Tx) error { return tx.removeMember(mb, reasonLeft) }); err != nil {
		t.Fatal(err)
	}
	if err := s.update(ctx, func(tx *Tx) error { return tx.removeMember(ma, reasonLeft) }); err != nil {
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

func TestDSNPragmasOnEveryPooledConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pragmas.db")
	s, err := OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	var conns []*sql.Conn
	for i := 0; i < 4; i++ {
		c, err := s.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		var fk, busy int
		var mode string
		c.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk)
		c.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busy)
		c.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode)
		if fk != 1 || busy != 5000 || mode != "wal" {
			t.Fatalf("conn %d: foreign_keys=%d busy_timeout=%d journal_mode=%s", i, fk, busy, mode)
		}
	}
	for _, c := range conns {
		c.Close()
	}

	// _txlock=immediate: a transaction takes the write lock at BEGIN, so a
	// second writer with no busy timeout is refused at once. A deferred
	// transaction would not hold the lock yet.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	other, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(0)")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.ExecContext(ctx, "BEGIN IMMEDIATE"); err == nil {
		t.Fatal("second writer got the lock: transactions are not immediate")
	}
}

// schemaV1 is the pre-multi-user schema, to seed a v1 database.
const schemaV1 = `
CREATE TABLE sessions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_hash BLOB UNIQUE NOT NULL,
  created_at INTEGER NOT NULL,
  disconnected_at INTEGER NULL,
  version INTEGER NOT NULL DEFAULT 0);
CREATE TABLE channels(
  id INTEGER PRIMARY KEY,
  id_hash BLOB UNIQUE NOT NULL,
  created_at INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('open','joined','ended','expired')));
CREATE TABLE members(
  id INTEGER PRIMARY KEY,
  channel_id INTEGER NOT NULL REFERENCES channels ON DELETE RESTRICT,
  session_id INTEGER NOT NULL REFERENCES sessions ON DELETE RESTRICT,
  label TEXT NOT NULL,
  handle TEXT NOT NULL,
  peer_label TEXT NULL,
  UNIQUE(session_id, handle),
  UNIQUE(channel_id, label));
CREATE TABLE held(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_id INTEGER NOT NULL REFERENCES channels ON DELETE CASCADE,
  from_label TEXT NOT NULL,
  text TEXT NOT NULL,
  at INTEGER NOT NULL);
CREATE TABLE inbox(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL REFERENCES sessions ON DELETE CASCADE,
  handle TEXT NOT NULL,
  from_label TEXT NULL,
  kind TEXT NOT NULL,
  text TEXT NOT NULL,
  at INTEGER NOT NULL);
`

func seedV1(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		schemaV1,
		"INSERT INTO sessions(token_hash, created_at) VALUES(x'01', 1)",
		"INSERT INTO channels(id_hash, created_at, state) VALUES(x'02', 1, 'open')",
		"INSERT INTO members(channel_id, session_id, label, handle) VALUES(1, 1, 'h/a', 'ch-a')",
		"INSERT INTO inbox(session_id, handle, kind, text, at) VALUES(1, 'ch-a', 'text', 'old', 1)",
		"PRAGMA user_version = 1",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
}

func TestMigrationV1ToV2DropsDataAndSetsVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	seedV1(t, path)
	s, err := OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v int
	s.db.QueryRow("PRAGMA user_version").Scan(&v)
	if v != 2 {
		t.Fatalf("user_version = %d, want 2", v)
	}
	for _, tbl := range []string{"sessions", "channels", "members", "held", "inbox", "accounts", "retired", "settings"} {
		var n int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&n); err != nil || n != 0 {
			t.Errorf("table %s: count %d, err %v (want present and empty)", tbl, n, err)
		}
	}
	if has, err := s.HasHostmaster(bg); err != nil || has {
		t.Fatalf("a migrated broker must start unclaimed: %v %v", has, err)
	}
}

func TestMigrationV2ReopenIsNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v2.db")
	s, err := OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	addAccount(t, s, "keeper", protocol.AccountActive, true)
	s.Close()
	s, err = OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if has, _ := s.HasHostmaster(bg); !has {
		t.Fatal("reopening a v2 database dropped its accounts")
	}
}

func TestFailedMigrationRollsBackFully(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	seedV1(t, path)
	db, _ := sql.Open("sqlite", "file:"+path)
	// A table the migration wants to create blocks it half-way through.
	if _, err := db.Exec("CREATE TABLE retired(x)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if s, err := OpenStore(path, nil, nil); err == nil {
		s.Close()
		t.Fatal("expected the migration to fail")
	}
	db, _ = sql.Open("sqlite", "file:"+path)
	defer db.Close()
	var v, n int
	db.QueryRow("PRAGMA user_version").Scan(&v)
	if v != 1 {
		t.Fatalf("user_version = %d after a failed migration, want 1", v)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM inbox").Scan(&n); err != nil || n != 1 {
		t.Fatalf("old data not restored: %d, %v", n, err)
	}
}

func TestRemoveMemberReasonRemoved(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	a, _ := newSessionFor(t, s, "alice")
	b, _ := newSessionFor(t, s, "bob")
	ma, _, _ := rawJoined(t, s, a, b, "h/a", "h/b")
	if err := s.update(ctx, func(tx *Tx) error { return tx.removeMember(ma, reasonRemoved) }); err != nil {
		t.Fatal(err)
	}
	r := sync0(t, s, b, protocol.SyncRequest{})
	if len(r.Messages) != 1 || r.Messages[0].Kind != protocol.KindPeerRemoved ||
		r.Messages[0].FromAccount == nil || *r.Messages[0].FromAccount != "alice" ||
		r.Messages[0].FromLabel == nil || *r.Messages[0].FromLabel != "h/a" {
		t.Fatalf("peer_removed row: %+v", r.Messages)
	}
	if len(r.Channels) != 1 || r.Channels[0].State != protocol.StatePeerRemoved {
		t.Fatalf("channel list: %+v", r.Channels)
	}
}

func TestRemoveMemberReasonLeftKeepsPeerLeft(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	a, _ := newSessionFor(t, s, "alice")
	b, _ := newSessionFor(t, s, "bob")
	ma, _, _ := rawJoined(t, s, a, b, "h/a", "h/b")
	if err := s.update(ctx, func(tx *Tx) error { return tx.removeMember(ma, reasonLeft) }); err != nil {
		t.Fatal(err)
	}
	r := sync0(t, s, b, protocol.SyncRequest{})
	if len(r.Messages) != 1 || r.Messages[0].Kind != protocol.KindPeerLeft ||
		r.Messages[0].FromAccount == nil || *r.Messages[0].FromAccount != "alice" {
		t.Fatalf("peer_left row: %+v", r.Messages)
	}
	if r.Channels[0].State != protocol.StatePeerLeft {
		t.Fatalf("state = %s", r.Channels[0].State)
	}
}

func TestCreateSessionRequiresActiveAccount(t *testing.T) {
	s, _ := openTestStore(t)
	pending, _ := addAccount(t, s, "p", protocol.AccountPending, false)
	if _, _, err := s.CreateSession(bg, pending); err == nil {
		t.Fatal("a pending account got a session")
	}
	if _, _, err := s.CreateSession(bg, 9999); err == nil {
		t.Fatal("a missing account got a session")
	}
}
