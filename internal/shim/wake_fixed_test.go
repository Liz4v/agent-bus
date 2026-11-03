package shim

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatusCarriesCursorAndItChangesAfterRead(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	before := statusOf(t, e.w.Path())
	if before.Count < 1 {
		t.Fatalf("status: %+v", before)
	}
	e.commitAll()
	after := statusOf(t, e.w.Path())
	if after.Count != 0 || after.Cursor == before.Cursor || after.Cursor == 0 {
		t.Fatalf("cursor before %d, after %d (count %d)", before.Cursor, after.Cursor, after.Count)
	}
}

// startFixed starts a fixed-path wake on a fresh mirror.
func startFixed(t *testing.T, path string, mut func(*WakeOptions)) (*Wake, error) {
	t.Helper()
	me := newMirrorEnv(t, fastCfg())
	opt := WakeOptions{Socket: path, Debounce: 150 * time.Millisecond}
	if mut != nil {
		mut(&opt)
	}
	w, err := StartWake(me.m, me.sess, opt)
	if err == nil {
		t.Cleanup(w.Close)
	}
	return w, err
}

func fastHandover(o *WakeOptions) {
	o.HandoverWait = 300 * time.Millisecond
	o.HandoverPoll = 30 * time.Millisecond
}

func TestFixedSocketFreshPathCreatesParent(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "sub", "w.sock")
	w, err := startFixed(t, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if w.Path() != path || w.IndexPath() != "" {
		t.Fatalf("path %q index %q", w.Path(), w.IndexPath())
	}
	fi, err := os.Stat(filepath.Dir(path))
	if err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("parent: %v %v", fi, err)
	}
	si, err := os.Stat(path)
	if err != nil || si.Mode().Perm() != 0o600 {
		t.Fatalf("socket: %v %v", si, err)
	}
	if r := statusOf(t, path); r.Count != 0 {
		t.Fatalf("status %+v", r)
	}
	ents, _ := os.ReadDir(filepath.Dir(path))
	if len(ents) != 1 {
		t.Fatalf("expected only the socket in the directory, got %v", ents)
	}
	w.Close()
	w.Close() // a second Close is safe
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket left behind: %v", err)
	}
}

func TestFixedSocketStaleSocketRemoved(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "w.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	ln.SetUnlinkOnClose(false)
	ln.Close() // leaves a dead socket file: dialing gives ECONNREFUSED
	if _, err := startFixed(t, path, fastHandover); err != nil {
		t.Fatalf("stale socket not reclaimed: %v", err)
	}
	statusOf(t, path)
}

func TestFixedSocketLiveSocketRefusedAfterHandoverWindow(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "w.sock")
	first, err := startFixed(t, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := startFixed(t, path, fastHandover); err == nil {
		t.Fatal("second shim claimed a live socket")
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Fatalf("gave up after %v, before the handover window", time.Since(start))
	}
	statusOf(t, first.Path()) // the first one still serves
}

func TestFixedSocketReleasedWithinWindowIsClaimed(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "w.sock")
	first, err := startFixed(t, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(150 * time.Millisecond)
		first.Close()
	}()
	if _, err := startFixed(t, path, func(o *WakeOptions) {
		o.HandoverWait = 3 * time.Second
		o.HandoverPoll = 30 * time.Millisecond
	}); err != nil {
		t.Fatalf("handover failed: %v", err)
	}
	statusOf(t, path)
}

func TestFixedSocketRefusals(t *testing.T) {
	dir := shortTempDir(t)
	reg := filepath.Join(dir, "file.sock")
	if err := os.WriteFile(reg, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	open := filepath.Join(dir, "open")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"regular file":    reg,
		"relative":        "rel/w.sock",
		"too long":        filepath.Join(dir, strings.Repeat("a", 120), "w.sock"),
		"bad parent mode": filepath.Join(open, "w.sock"),
	}
	for name, p := range cases {
		if _, err := startFixed(t, p, fastHandover); err == nil {
			t.Errorf("%s: path %q accepted", name, p)
		}
	}
	if b, err := os.ReadFile(reg); err != nil || string(b) != "x" {
		t.Fatalf("regular file was touched: %q %v", b, err)
	}
}

func TestFixedSocketOtherDialErrorRefusesWithoutRemoving(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores socket permissions")
	}
	path := filepath.Join(shortTempDir(t), "w.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := os.Chmod(path, 0); err != nil { // dialing now fails with EACCES
		t.Fatal(err)
	}
	if _, err := startFixed(t, path, fastHandover); err == nil {
		t.Fatal("accepted a socket it could not probe")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("socket removed: %v", err)
	}
}

func TestFixedSocketCloseLeavesSuccessorSocket(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "w.sock")
	w, err := startFixed(t, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.ln.Close() // unlinks the path, like a dying predecessor
	succ, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer succ.Close()
	w.Close() // must not delete the successor's socket
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("successor's socket was removed: %v", err)
	}
	w.Close() // the cleanup's second Close is safe
}
