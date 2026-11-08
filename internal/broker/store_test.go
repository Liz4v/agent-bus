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

func enqueueText(t *testing.T, s *Store, session int64, text string) {
	t.Helper()
	from := "x"
	err := s.update(context.Background(), func(tx *Tx) error {
		return tx.enqueue(session, &from, nil, protocol.KindText, text, false)
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
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil || v != 3 {
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

func TestRestrictBlocksAccountDeleteAndSessionDeleteCascades(t *testing.T) {
	s, _ := openTestStore(t)
	a, accA := newSessionFor(t, s, "alice")
	b := named(t, s, "bob")
	claimAs(t, s, a, "alice")
	mustSend(t, s, a, "bob", "hi")
	if _, err := s.db.Exec("DELETE FROM accounts WHERE id = ?", accA); err == nil {
		t.Fatal("deleting an account that owns a session must fail")
	}
	// A session's inbox, partner rows and departed rows go with it.
	if _, err := s.db.Exec("INSERT INTO departed(session_id, name_key, kind) VALUES(?, 'x', 'peer_left')", a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("DELETE FROM sessions WHERE id = ?", a); err != nil {
		t.Fatalf("deleting a session: %v", err)
	}
	for _, q := range []string{"SELECT COUNT(*) FROM partners", "SELECT COUNT(*) FROM departed", "SELECT COUNT(*) FROM inbox WHERE session_id = " + fmt.Sprint(a)} {
		var n int
		s.db.QueryRow(q).Scan(&n)
		if n != 0 {
			t.Fatalf("%s = %d after the session was deleted", q, n)
		}
	}
	if got := inboxOf(t, s, b); len(got) != 1 {
		t.Fatalf("the peer's own row must survive: %+v", got)
	}
}

func TestSessionNameKeyIsUnique(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := newSession(t, s), newSession(t, s)
	if _, err := s.db.Exec("UPDATE sessions SET name = 'Bob', name_key = 'bob' WHERE id = ?", a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE sessions SET name = 'bob', name_key = 'bob' WHERE id = ?", b); err == nil {
		t.Fatal("two sessions share a name key")
	}
	// Unnamed sessions do not collide: NULL keys are distinct.
	c, d := newSession(t, s), newSession(t, s)
	if c == d {
		t.Fatal("setup")
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
				from := "x"
				if err := s.update(ctx, func(tx *Tx) error {
					return tx.enqueue(a, &from, nil, protocol.KindText, "x", false)
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
	a, b := named(t, s, "alice"), named(t, s, "bob")
	mustSend(t, s, a, "bob", "hi")
	enqueueText(t, s, a, "queued for a")
	wake, cancel := s.Notify().Subscribe(a)
	defer cancel()

	if err := s.DeleteSelf(ctx, a); err != nil {
		t.Fatal(err)
	}
	recv(t, wake)
	r := sync0(t, s, b, protocol.SyncRequest{})
	last := r.Messages[len(r.Messages)-1]
	if last.Kind != protocol.KindPeerLeft || strp(last.From) != "alice" || strp(last.FromAccount) != "acct-alice" {
		t.Fatalf("peer did not get peer_left: %+v", r.Messages)
	}
	if _, err := s.Sync(ctx, a, protocol.SyncRequest{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("deleted session sync: %v", err)
	}
	var n int
	s.db.QueryRow("SELECT COUNT(*) FROM inbox WHERE session_id = ?", a).Scan(&n)
	if n != 0 {
		t.Fatal("inbox survived session deletion")
	}
	s.db.QueryRow("SELECT COUNT(*) FROM partners").Scan(&n)
	if n != 0 {
		t.Fatalf("%d partner rows left behind", n)
	}
	// Deleting twice is harmless (the session may already be gone).
	if err := s.DeleteSelf(ctx, a); err != nil {
		t.Fatalf("second DeleteSelf: %v", err)
	}
	// b leaves too: nobody is left to tell.
	if err := s.DeleteSelf(ctx, b); err != nil {
		t.Fatal(err)
	}
}

func TestInboxRowsOutliveAPartnersDeparture(t *testing.T) {
	s, _ := openTestStore(t)
	a, b := named(t, s, "alice"), named(t, s, "bob")
	mustSend(t, s, b, "alice", "queued before leaving")
	if _, err := s.ReleaseName(bg, b); err != nil {
		t.Fatal(err)
	}
	r := sync0(t, s, a, protocol.SyncRequest{})
	found := map[string]bool{}
	for _, m := range r.Messages {
		found[m.Kind] = true
	}
	if !found[protocol.KindText] || !found[protocol.KindPeerLeft] {
		t.Fatalf("rows lost when the sender left: %v", kinds(r))
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

func TestMigrationV1ToCurrentDropsDataAndSetsVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	seedV1(t, path)
	s, err := OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v int
	s.db.QueryRow("PRAGMA user_version").Scan(&v)
	if v != 3 {
		t.Fatalf("user_version = %d, want 3", v)
	}
	for _, tbl := range []string{"sessions", "inbox", "partners", "departed", "accounts", "retired", "settings"} {
		var n int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&n); err != nil || n != 0 {
			t.Errorf("table %s: count %d, err %v (want present and empty)", tbl, n, err)
		}
	}
	for _, tbl := range []string{"channels", "members", "held"} {
		if _, err := s.db.Exec("SELECT 1 FROM " + tbl); err == nil {
			t.Errorf("table %s still exists after the migration", tbl)
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

func TestDepartReasonRemoved(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	a, _ := newSessionFor(t, s, "alice")
	b, _ := newSessionFor(t, s, "bob")
	claimAs(t, s, a, "alice-s")
	claimAs(t, s, b, "bob-s")
	mustSend(t, s, a, "bob-s", "hi")
	if err := s.update(ctx, func(tx *Tx) error { return tx.depart(a, reasonRemoved) }); err != nil {
		t.Fatal(err)
	}
	r := sync0(t, s, b, protocol.SyncRequest{})
	last := r.Messages[len(r.Messages)-1]
	if last.Kind != protocol.KindPeerRemoved || strp(last.FromAccount) != "alice" || strp(last.From) != "alice-s" {
		t.Fatalf("peer_removed row: %+v", r.Messages)
	}
	var kind string
	if err := s.db.QueryRow("SELECT kind FROM departed WHERE session_id = ? AND name_key = 'alice-s'", b).Scan(&kind); err != nil || kind != protocol.KindPeerRemoved {
		t.Fatalf("departed row: %q, %v", kind, err)
	}
}

func TestDepartReasonLeftKeepsPeerLeft(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	a, _ := newSessionFor(t, s, "alice")
	b, _ := newSessionFor(t, s, "bob")
	claimAs(t, s, a, "alice-s")
	claimAs(t, s, b, "bob-s")
	mustSend(t, s, a, "bob-s", "hi")
	if err := s.update(ctx, func(tx *Tx) error { return tx.depart(a, reasonLeft) }); err != nil {
		t.Fatal(err)
	}
	r := sync0(t, s, b, protocol.SyncRequest{})
	last := r.Messages[len(r.Messages)-1]
	if last.Kind != protocol.KindPeerLeft || strp(last.FromAccount) != "alice" {
		t.Fatalf("peer_left row: %+v", r.Messages)
	}
	var kind string
	s.db.QueryRow("SELECT kind FROM departed WHERE session_id = ? AND name_key = 'alice-s'", b).Scan(&kind)
	if kind != protocol.KindPeerLeft {
		t.Fatalf("departed kind = %q", kind)
	}
}

func TestDepartDoesNothingForAnUnnamedOrMissingSession(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	a := newSession(t, s)
	b := named(t, s, "bob")
	for _, session := range []int64{a, 9999} {
		if err := s.update(ctx, func(tx *Tx) error { return tx.depart(session, reasonLeft) }); err != nil {
			t.Fatalf("depart(%d): %v", session, err)
		}
	}
	if got := inboxOf(t, s, b); len(got) != 0 {
		t.Fatalf("a bystander was told: %+v", got)
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

// seedV2 builds a version 2 database (accounts, plus channels, held rows and
// handles) the way the multi-user release left it.
func seedV2(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		schemaV2,
		"INSERT INTO accounts(name, name_key, cred_hash, status, is_hostmaster, created_at) VALUES('keeper','keeper',x'0a','active',1,1)",
		"INSERT INTO accounts(name, name_key, cred_hash, status, is_hostmaster, created_at) VALUES('liz','liz',x'0b','active',0,1)",
		"INSERT INTO retired(cred_hash, name, outcome, at) VALUES(x'0c','gone','removed',1)",
		"INSERT INTO settings(key, value) VALUES('max_accounts','7')",
		"INSERT INTO sessions(token_hash, account_id, created_at) VALUES(x'01', 2, 1)",
		"INSERT INTO channels(id_hash, created_at, state) VALUES(x'02', 1, 'open')",
		"INSERT INTO members(channel_id, session_id, label, handle, account) VALUES(1, 1, 'h/a', 'ch-a', 'liz')",
		"INSERT INTO held(channel_id, from_label, from_account, text, at) VALUES(1, 'h/a', 'liz', 'held', 1)",
		"INSERT INTO inbox(session_id, handle, kind, text, at) VALUES(1, 'ch-a', 'text', 'old', 1)",
		"PRAGMA user_version = 2",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
}

func TestMigrationV2ToV3KeepsAccountsAndDropsChannelData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v2.db")
	seedV2(t, path)
	s, err := OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v int
	s.db.QueryRow("PRAGMA user_version").Scan(&v)
	if v != 3 {
		t.Fatalf("user_version = %d, want 3", v)
	}
	counts := map[string]int{"accounts": 2, "retired": 1, "settings": 1, "sessions": 0, "inbox": 0, "partners": 0, "departed": 0}
	for tbl, want := range counts {
		var n int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&n); err != nil || n != want {
			t.Errorf("table %s: count %d, err %v (want %d)", tbl, n, err, want)
		}
	}
	for _, tbl := range []string{"channels", "members", "held"} {
		if _, err := s.db.Exec("SELECT 1 FROM " + tbl); err == nil {
			t.Errorf("table %s survived the migration", tbl)
		}
	}
	if has, err := s.HasHostmaster(bg); err != nil || !has {
		t.Fatalf("the hostmaster must survive: %v %v", has, err)
	}
	var max string
	s.db.QueryRow("SELECT value FROM settings WHERE key = 'max_accounts'").Scan(&max)
	if max != "7" {
		t.Fatalf("settings lost: max_accounts = %q", max)
	}
	// The upgraded database works: an old account opens a session and claims a name.
	var id int64
	if err := s.db.QueryRow("SELECT id FROM accounts WHERE name = 'liz'").Scan(&id); err != nil {
		t.Fatal(err)
	}
	_, sess, err := s.CreateSession(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	claimAs(t, s, sess, "liz-s")
}

func TestMigrationFreshDatabaseIsV3(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "fresh.db"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var v int
	s.db.QueryRow("PRAGMA user_version").Scan(&v)
	if v != 3 {
		t.Fatalf("user_version = %d, want 3", v)
	}
	for _, tbl := range []string{"accounts", "retired", "settings", "sessions", "partners", "departed", "inbox"} {
		if _, err := s.db.Exec("SELECT 1 FROM " + tbl); err != nil {
			t.Errorf("table %s missing: %v", tbl, err)
		}
	}
}

func TestMigrationV3ReopenIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v3.db")
	s, err := OpenStore(path, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := newSessionFor(t, s, "liz")
	claimAs(t, s, a, "bob")
	s.Close()
	s, err = OpenStore(path, nil, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	if r := sync0(t, s, a, protocol.SyncRequest{}); r.Name != "bob" {
		t.Fatalf("a v3 reopen dropped the session or its name: %q", r.Name)
	}
}

func TestFailedV3MigrationRollsBackFully(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad3.db")
	seedV2(t, path)
	db, _ := sql.Open("sqlite", "file:"+path)
	// A table the migration wants to create blocks it after the drops.
	if _, err := db.Exec("CREATE TABLE partners(x)"); err != nil {
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
	if v != 2 {
		t.Fatalf("user_version = %d after a failed migration, want 2", v)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM channels").Scan(&n); err != nil || n != 1 {
		t.Fatalf("channels not restored: %d, %v", n, err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM inbox").Scan(&n); err != nil || n != 1 {
		t.Fatalf("inbox not restored: %d, %v", n, err)
	}
}
