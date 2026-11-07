package shim

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

type wakeEnv struct {
	*mirrorEnv
	w   *Wake
	dir string
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "ab")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func newWakeEnv(t *testing.T, opt WakeOptions) *wakeEnv {
	t.Helper()
	return newWakeEnvFrom(t, opt, newMirrorEnv(t, fastCfg()))
}

// newHostmasterWakeEnv is a wake env whose shim session is the hostmaster's.
func newHostmasterWakeEnv(t *testing.T, opt WakeOptions) *wakeEnv {
	t.Helper()
	tb := newTestBroker(t)
	return newWakeEnvFrom(t, opt, newMirrorEnvFor(t, tb, fastCfg(), tb.newClient(tb.bossCred)))
}

func newWakeEnvFrom(t *testing.T, opt WakeOptions, me *mirrorEnv) *wakeEnv {
	t.Helper()
	if opt.RuntimeDir == "" {
		opt.RuntimeDir = shortTempDir(t)
	}
	if opt.Debounce == 0 {
		opt.Debounce = 150 * time.Millisecond
	}
	w, err := StartWake(me.m, me.sess, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return &wakeEnv{mirrorEnv: me, w: w, dir: WakeDir(opt.withDefaults())}
}

func dial(t *testing.T, path, cmd string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if _, err := c.Write([]byte(cmd + "\n")); err != nil {
		t.Fatal(err)
	}
	return c, bufio.NewReader(c)
}

func readLine(c net.Conn, br *bufio.Reader, d time.Duration) (string, error) {
	c.SetReadDeadline(time.Now().Add(d))
	return br.ReadString('\n')
}

func waitReply(t *testing.T, path string, d time.Duration) protocol.WaitReply {
	t.Helper()
	c, br := dial(t, path, "WAIT")
	line, err := readLine(c, br, d)
	if err != nil {
		t.Fatalf("no WAIT reply within %v: %v", d, err)
	}
	var r protocol.WaitReply
	if err := json.Unmarshal([]byte(line), &r); err != nil {
		t.Fatalf("bad reply %q", line)
	}
	return r
}

func noReply(t *testing.T, c net.Conn, br *bufio.Reader, d time.Duration) {
	t.Helper()
	if line, err := readLine(c, br, d); err == nil {
		t.Fatalf("unexpected reply: %s", line)
	}
}

func statusOf(t *testing.T, path string) protocol.StatusReply {
	t.Helper()
	c, br := dial(t, path, "STATUS")
	line, err := readLine(c, br, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var r protocol.StatusReply
	if err := json.Unmarshal([]byte(line), &r); err != nil {
		t.Fatalf("bad reply %q", line)
	}
	return r
}

// commitAll reads everything in the mirror, like a read tool call.
func (e *wakeEnv) commitAll() {
	rows, gen := e.m.Unread()
	if len(rows) > 0 {
		e.m.Commit(gen, rows[len(rows)-1].ID)
	}
}

func TestWaitPendingImmediatelyWhenUnreadExists(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link() // the creator has an unread peer_joined
	start := time.Now()
	r := waitReply(t, e.w.Path(), 3*time.Second)
	if r.Status != protocol.WaitPending || r.Count < 1 || len(r.From) != 1 || r.From[0] != "h/peer" {
		t.Fatalf("reply: %+v", r)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %v", time.Since(start))
	}
}

func TestBurstGivesOneReplyWithCount(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{Debounce: 500 * time.Millisecond})
	e.link()
	e.commitAll()
	c, br := dial(t, e.w.Path(), "WAIT")
	for i := 0; i < 5; i++ {
		e.peerSend("burst")
	}
	line, err := readLine(c, br, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var r protocol.WaitReply
	json.Unmarshal([]byte(line), &r)
	if r.Status != protocol.WaitPending || r.Count != 5 {
		t.Fatalf("reply: %+v", r)
	}
}

func TestReadDuringDebounceCancelsTheWake(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{Debounce: 400 * time.Millisecond})
	e.link()
	e.commitAll()
	e.peerSend("hi")
	e.eventually(func(s Snapshot) bool { return len(s.Rows) > 0 })
	c, br := dial(t, e.w.Path(), "WAIT")
	time.Sleep(100 * time.Millisecond)
	e.commitAll() // a read inside the window
	noReply(t, c, br, 900*time.Millisecond)
	if !e.w.Armed() {
		t.Fatal("WAIT should still be open")
	}
}

func TestInformationalEventsNeverWake(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	e.commitAll()
	c, br := dial(t, e.w.Path(), "WAIT")
	e.tb.clk.Advance(protocol.DisconnectAfter + time.Second)
	if err := e.m.Refresh(bg); err != nil {
		t.Fatal(err)
	}
	e.tb.sweeper.Sweep(bg)
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerDisconnected) == 1 })
	e.tb.clk.Advance(time.Second)
	e.tb.c.Sync(bg, e.peerTok, protocol.SyncRequest{})
	e.tb.sweeper.Sweep(bg)
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerReconnected) == 1 })
	noReply(t, c, br, 600*time.Millisecond)
	if st := statusOf(t, e.w.Path()); st.Count != 0 || st.LiveChannels != 1 {
		t.Fatalf("status: %+v", st)
	}
}

func TestLastChannelEndedThenNoChannelsAfterRead(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	e.commitAll()
	if err := e.tb.c.Leave(bg, e.peerTok, "h/me"); err != nil {
		t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return rowsWithKind(s, protocol.KindPeerLeft) == 1 })
	r := waitReply(t, e.w.Path(), 3*time.Second)
	if r.Status != protocol.WaitPending || len(r.From) != 1 || r.From[0] != "h/peer" {
		t.Fatalf("reply: %+v", r)
	}
	e.commitAll()
	r = waitReply(t, e.w.Path(), 3*time.Second)
	if r.Status != protocol.WaitNoChannels || r.Reason == "" {
		t.Fatalf("reply: %+v", r)
	}
}

func TestUnregisteredShimNoChannelsAtOnce(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	r := waitReply(t, e.w.Path(), time.Second)
	if r.Status != protocol.WaitNoChannels || r.Reason == "" {
		t.Fatalf("reply: %+v", r)
	}
	st := statusOf(t, e.w.Path())
	if st.Count != 0 || st.LiveChannels != 0 || st.Armed || st.Error != nil || len(st.From) != 0 {
		t.Fatalf("status: %+v", st)
	}
	if e.tb.totalHits() != 0 {
		t.Fatal("wake socket caused broker requests")
	}
}

func TestWaitWhenSessionDroppedGetsNoticeReason(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	e.commitAll()
	c, br := dial(t, e.w.Path(), "WAIT")
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	e.tb.kill(e.sess.Token())
	line, err := readLine(c, br, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var r protocol.WaitReply
	json.Unmarshal([]byte(line), &r)
	if r.Status != protocol.WaitNoChannels || r.Reason == "" || time.Since(start) > 4*time.Second {
		t.Fatalf("reply %+v after %v", r, time.Since(start))
	}
	if e.sess.PeekNotice() != r.Reason {
		t.Fatalf("reason %q is not the notice %q", r.Reason, e.sess.PeekNotice())
	}
	if e.sess.TakeNotice() == "" {
		t.Fatal("WAIT consumed the notice")
	}
}

func TestExpiredChannelReportedFromAgentBus(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	tok, _ := e.sess.Ensure(bg)
	if _, err := e.tb.c.CreateChannel(bg, tok, "h/me"); err != nil {
		t.Fatal(err)
	}
	e.tb.clk.Advance(protocol.ChannelTTL + time.Second)
	e.m.Refresh(bg)
	e.tb.sweeper.Sweep(bg)
	r := waitReply(t, e.w.Path(), 3*time.Second)
	if r.Status != protocol.WaitPending || len(r.From) != 1 || r.From[0] != protocol.SystemSender {
		t.Fatalf("reply: %+v", r)
	}
}

func TestUntrustedSocketDirectoryRefused(t *testing.T) {
	me := newMirrorEnv(t, fastCfg())
	rt := shortTempDir(t)
	dir := rt + "/agent-bus"
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// wrong owner: pretend we are another uid
	if _, err := StartWake(me.m, me.sess, WakeOptions{RuntimeDir: rt, UID: os.Getuid() + 1}); err == nil {
		t.Fatal("directory owned by another uid accepted")
	}
	// wrong mode
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := StartWake(me.m, me.sess, WakeOptions{RuntimeDir: rt}); err == nil {
		t.Fatal("mode 0777 directory accepted")
	}
	// a symlink in place of the directory
	rt2 := shortTempDir(t)
	os.Mkdir(rt2+"/real", 0o700)
	os.Symlink(rt2+"/real", rt2+"/agent-bus")
	if _, err := StartWake(me.m, me.sess, WakeOptions{RuntimeDir: rt2}); err == nil {
		t.Fatal("symlinked directory accepted")
	}
}

func TestTwoConcurrentWaitsGetTheSameReply(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	c1, b1 := dial(t, e.w.Path(), "WAIT")
	c2, b2 := dial(t, e.w.Path(), "WAIT")
	l1, err1 := readLine(c1, b1, 3*time.Second)
	l2, err2 := readLine(c2, b2, 3*time.Second)
	if err1 != nil || err2 != nil || l1 != l2 {
		t.Fatalf("%q (%v) vs %q (%v)", l1, err1, l2, err2)
	}
}

func TestClientHangUpClearsArmed(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	e.commitAll()
	c, _ := dial(t, e.w.Path(), "WAIT")
	deadline := time.Now().Add(3 * time.Second)
	for !statusOf(t, e.w.Path()).Armed {
		if time.Now().After(deadline) {
			t.Fatal("never armed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.Close()
	for statusOf(t, e.w.Path()).Armed {
		if time.Now().After(deadline.Add(3 * time.Second)) {
			t.Fatal("armed stayed true after the client hung up")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRearmAfterTimeout(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{Rearm: 300 * time.Millisecond})
	e.link()
	e.commitAll()
	r := waitReply(t, e.w.Path(), 3*time.Second)
	if r.Status != protocol.WaitRearm {
		t.Fatalf("reply: %+v", r)
	}
}

func TestStatusErrorWhileBrokerDown(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	e.tb.down.Store(true)
	e.m.Refresh(bg)
	st := statusOf(t, e.w.Path())
	if st.Error == nil || st.LiveChannels != 1 {
		t.Fatalf("status: %+v", st)
	}
	e.tb.down.Store(false)
	e.eventually(func(s Snapshot) bool { return s.Health == nil })
	if st := statusOf(t, e.w.Path()); st.Error != nil {
		t.Fatalf("error not cleared: %v", *st.Error)
	}
}

func TestSocketAndIndexPermissions(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{PPID: 4242})
	for path, want := range map[string]os.FileMode{e.dir: 0o700, e.w.Path(): 0o600, e.w.IndexPath(): 0o600} {
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != want {
			t.Errorf("%s has mode %o, want %o", path, fi.Mode().Perm(), want)
		}
	}
	if b, _ := os.ReadFile(e.dir + "/ppid-4242"); string(b) != e.w.Path() {
		t.Fatalf("index holds %q", b)
	}
}

func TestIndexFileKeptWhenAnotherShimRewroteIt(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{PPID: 4243})
	idx := e.w.IndexPath()
	if err := writeFileAtomic(idx, []byte("/some/other/shim.sock")); err != nil {
		t.Fatal(err)
	}
	e.w.Close()
	if b, err := os.ReadFile(idx); err != nil || string(b) != "/some/other/shim.sock" {
		t.Fatalf("index removed or changed: %q %v", b, err)
	}
	// and our own is removed
	e2 := newWakeEnv(t, WakeOptions{PPID: 4244})
	idx2 := e2.w.IndexPath()
	e2.w.Close()
	if _, err := os.Stat(idx2); err == nil {
		t.Fatal("own index file left behind")
	}
	if _, err := os.Stat(e2.w.Path()); err == nil {
		t.Fatal("socket left behind")
	}
}

func TestClientsGetEOFOnShutdown(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	e.commitAll()
	c, br := dial(t, e.w.Path(), "WAIT")
	time.Sleep(200 * time.Millisecond)
	e.w.Close()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if line, err := br.ReadString('\n'); err == nil {
		t.Fatalf("got %q, want EOF", line)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("client was not disconnected on shutdown")
	}
}

func TestWakeOptionsFromEnvHonoursXDGRuntimeDir(t *testing.T) {
	rt := shortTempDir(t)
	t.Setenv("XDG_RUNTIME_DIR", rt)
	opt := WakeOptionsFromEnv()
	if opt.RuntimeDir != rt {
		t.Fatalf("RuntimeDir = %q, want %q", opt.RuntimeDir, rt)
	}
	me := newMirrorEnv(t, fastCfg())
	w, err := StartWake(me.m, me.sess, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if want := filepath.Join(rt, "agent-bus") + string(os.PathSeparator); !strings.HasPrefix(w.Path(), want) {
		t.Fatalf("socket %q is not under %s", w.Path(), want)
	}
	// without it, the fallback is os.TempDir()/agent-bus-<uid>
	t.Setenv("XDG_RUNTIME_DIR", "")
	got := WakeDir(WakeOptionsFromEnv().withDefaults())
	if want := filepath.Join(os.TempDir(), fmt.Sprintf("agent-bus-%d", os.Getuid())); got != want {
		t.Fatalf("fallback dir %q, want %q", got, want)
	}
}

func TestWaitStaysArmedByDefault(t *testing.T) {
	e := newWakeEnv(t, WakeOptions{})
	e.link()
	e.commitAll()
	c, err := net.Dial("unix", e.w.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte(protocol.WakeCmdWait + "\n")); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(700 * time.Millisecond))
	if line, err := bufio.NewReader(c).ReadString('\n'); err == nil {
		t.Fatalf("wait replied without a message or a timeout option: %q", line)
	}
}

func TestSubscribedSessionWithNoChannelsStaysArmedAndWakesOnRegistration(t *testing.T) {
	e := newHostmasterWakeEnv(t, WakeOptions{})
	tok, err := e.sess.Ensure(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.sess.Client().SetAdminSubscription(bg, tok, true); err != nil {
		t.Fatal(err)
	}
	e.eventually(func(s Snapshot) bool { return s.AdminSubscribed })
	if st := statusOf(t, e.w.Path()); !st.Subscribed || st.LiveChannels != 0 {
		t.Fatalf("status: %+v", st)
	}
	c, br := dial(t, e.w.Path(), "WAIT")
	noReply(t, c, br, 400*time.Millisecond) // no channels, but subscribed: still armed
	if _, err := e.tb.store.Register(bg, "newbie", ""); err != nil {
		t.Fatal(err)
	}
	line, err := readLine(c, br, 3*time.Second)
	if err != nil {
		t.Fatalf("not woken by the registration: %v", err)
	}
	var r protocol.WaitReply
	json.Unmarshal([]byte(line), &r)
	if r.Status != protocol.WaitPending || r.Count != 1 || len(r.From) != 1 || r.From[0] != protocol.SystemSender {
		t.Fatalf("reply %+v", r)
	}
}

func TestWaitDuringKeeperGapStaysArmedUntilIntentCleared(t *testing.T) {
	e := newHostmasterWakeEnv(t, WakeOptions{})
	tok, _ := e.sess.Ensure(bg)
	e.sess.SetWantAdmin(true)
	e.sess.Drop(tok) // the mirror is now reset to empty; the keeper is "re-establishing"

	c, br := dial(t, e.w.Path(), "WAIT")
	noReply(t, c, br, 400*time.Millisecond) // wantAdmin keeps it armed: no no_channels
	if st := statusOf(t, e.w.Path()); !st.Subscribed || !st.Armed {
		t.Fatalf("status: %+v", st)
	}

	// The keeper gives up: it clears the intent, leaves a notice and pokes.
	e.sess.SetWantAdmin(false)
	e.sess.AppendNotice("Your admin subscription could not be restored: test reason.")
	e.m.Poke()
	line, err := readLine(c, br, 3*time.Second)
	if err != nil {
		t.Fatalf("armed wait was not re-evaluated after the poke: %v", err)
	}
	var r protocol.WaitReply
	json.Unmarshal([]byte(line), &r)
	if r.Status != protocol.WaitNoChannels || !strings.Contains(r.Reason, "could not be restored") {
		t.Fatalf("reply %+v", r)
	}
	// A new WAIT now gets no_channels at once.
	if r := waitReply(t, e.w.Path(), 3*time.Second); r.Status != protocol.WaitNoChannels {
		t.Fatalf("after clearing: %+v", r)
	}
}

func TestUnsubscribedNoChannelsStillReplyNoChannels(t *testing.T) {
	e := newHostmasterWakeEnv(t, WakeOptions{})
	e.sess.Ensure(bg)
	if r := waitReply(t, e.w.Path(), 3*time.Second); r.Status != protocol.WaitNoChannels {
		t.Fatalf("%+v", r)
	}
	if st := statusOf(t, e.w.Path()); st.Subscribed {
		t.Fatalf("status: %+v", st)
	}
}
