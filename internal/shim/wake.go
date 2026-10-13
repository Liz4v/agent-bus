package shim

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// WakeOptions configures the wake socket. Zero values mean the defaults.
type WakeOptions struct {
	RuntimeDir string // $XDG_RUNTIME_DIR; empty means use TempDir
	TempDir    string // default os.TempDir()
	UID        int    // default os.Getuid()
	PPID       int    // default os.Getppid()
	Debounce   time.Duration
	Rearm      time.Duration
	Log        *slog.Logger
}

// Wake serves WAIT and STATUS over a Unix socket, answering from the mirror
// only (D7). It never causes a broker request.
type Wake struct {
	m    *Mirror
	sess *Session
	opt  WakeOptions

	dir, sock, index string
	ln               net.Listener
	armed            atomic.Int32

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// CheckSocketDir verifies that dir is a real directory owned by uid with no
// group or other permission bits (D7).
func CheckSocketDir(dir string, uid int) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.Mode().IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s is not a real directory", dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != uid {
		return fmt.Errorf("%s is not owned by uid %d", dir, uid)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s has mode %o; group and other access is not allowed", dir, fi.Mode().Perm())
	}
	return nil
}

// WakeDir returns the directory the shim uses for its socket and index file.
func WakeDir(opt WakeOptions) string {
	if opt.RuntimeDir != "" {
		return filepath.Join(opt.RuntimeDir, "agent-bus")
	}
	return filepath.Join(opt.TempDir, fmt.Sprintf("agent-bus-%d", opt.UID))
}

func (o WakeOptions) withDefaults() WakeOptions {
	if o.TempDir == "" {
		o.TempDir = os.TempDir()
	}
	if o.UID == 0 {
		o.UID = os.Getuid()
	}
	if o.PPID == 0 {
		o.PPID = os.Getppid()
	}
	if o.Debounce == 0 {
		o.Debounce = protocol.WakeDebounce
	}
	if o.Rearm == 0 {
		o.Rearm = protocol.WaitRearmAfter
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return o
}

// StartWake creates the socket directory, listens and writes the index file.
// If the directory cannot be trusted it returns an error and the shim runs
// without a wake socket.
func StartWake(m *Mirror, sess *Session, opt WakeOptions) (*Wake, error) {
	opt = opt.withDefaults()
	w := &Wake{m: m, sess: sess, opt: opt, conns: map[net.Conn]struct{}{}}
	w.dir = WakeDir(opt)
	if _, err := os.Lstat(w.dir); errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(w.dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create wake directory: %w", err)
		}
	}
	if err := CheckSocketDir(w.dir, opt.UID); err != nil {
		return nil, fmt.Errorf("refusing wake directory: %w", err)
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, err
	}
	w.sock = filepath.Join(w.dir, hex.EncodeToString(rnd[:])+".sock")
	if len(w.sock) > 100 {
		return nil, fmt.Errorf("wake socket path %q is too long for a Unix socket (limit about 108 bytes); set a shorter TMPDIR or XDG_RUNTIME_DIR", w.sock)
	}
	ln, err := net.Listen("unix", w.sock)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", w.sock, err)
	}
	if err := os.Chmod(w.sock, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	w.ln = ln
	w.index = filepath.Join(w.dir, fmt.Sprintf("ppid-%d", opt.PPID))
	if err := writeFileAtomic(w.index, []byte(w.sock)); err != nil {
		ln.Close()
		os.Remove(w.sock)
		return nil, fmt.Errorf("write index file: %w", err)
	}
	w.wg.Add(1)
	go w.acceptLoop()
	return w, nil
}

func writeFileAtomic(path string, data []byte) error {
	tmp := path + fmt.Sprintf(".%d.tmp", os.Getpid())
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// Path returns the socket path.
func (w *Wake) Path() string { return w.sock }

// IndexPath returns the ancestry index file path.
func (w *Wake) IndexPath() string { return w.index }

// Armed reports whether at least one WAIT connection is open.
func (w *Wake) Armed() bool { return w.armed.Load() > 0 }

// Close stops serving, closes every client connection and removes the socket
// and (if it still holds our path) the index file.
func (w *Wake) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	for c := range w.conns {
		c.Close()
	}
	w.mu.Unlock()
	w.ln.Close()
	w.wg.Wait()
	os.Remove(w.sock)
	if b, err := os.ReadFile(w.index); err == nil && string(b) == w.sock {
		os.Remove(w.index)
	}
}

func (w *Wake) acceptLoop() {
	defer w.wg.Done()
	for {
		c, err := w.ln.Accept()
		if err != nil {
			return
		}
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			c.Close()
			return
		}
		w.conns[c] = struct{}{}
		w.wg.Add(1)
		w.mu.Unlock()
		go func() {
			defer w.wg.Done()
			defer func() {
				c.Close()
				w.mu.Lock()
				delete(w.conns, c)
				w.mu.Unlock()
			}()
			w.serve(c)
		}()
	}
}

func (w *Wake) serve(c net.Conn) {
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		return
	}
	c.SetReadDeadline(time.Time{})
	switch strings.TrimSpace(line) {
	case protocol.WakeCmdStatus:
		w.reply(c, w.status())
	case protocol.WakeCmdWait:
		w.handleWait(c, br)
	default:
		w.reply(c, map[string]string{"error": "unknown command"})
	}
}

func (w *Wake) reply(c net.Conn, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	c.Write(append(b, '\n'))
}

func (w *Wake) status() protocol.StatusReply {
	sum := w.m.Summary()
	r := protocol.StatusReply{Count: sum.Count, From: sum.From, LiveChannels: sum.LiveChannels, Armed: w.Armed()}
	if r.From == nil {
		r.From = []string{}
	}
	if sum.Health != nil {
		msg := sum.Health.Error()
		r.Error = &msg
	}
	return r
}

func (w *Wake) handleWait(c net.Conn, br *bufio.Reader) {
	w.armed.Add(1)
	defer w.armed.Add(-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { // hang-up detection: EOF means the wait process is gone
		io.Copy(io.Discard, br)
		cancel()
	}()
	rearm := time.NewTimer(w.opt.Rearm)
	defer rearm.Stop()
	for {
		changed := w.m.Changed()
		sum := w.m.Summary()
		if sum.Count > 0 {
			d := time.NewTimer(w.opt.Debounce)
			select {
			case <-d.C:
			case <-ctx.Done():
				d.Stop()
				return
			}
			if again := w.m.Summary(); again.Count > 0 {
				w.reply(c, protocol.WaitReply{Status: protocol.WaitPending, From: again.From, Count: again.Count})
				return
			}
			continue // read during the debounce window: stay quiet
		}
		if sum.LiveChannels == 0 {
			w.reply(c, protocol.WaitReply{Status: protocol.WaitNoChannels, Reason: w.noChannelsReason()})
			return
		}
		select {
		case <-changed:
		case <-rearm.C:
			w.reply(c, protocol.WaitReply{Status: protocol.WaitRearm})
			return
		case <-ctx.Done():
			return
		}
	}
}

func (w *Wake) noChannelsReason() string {
	if n := w.sess.PeekNotice(); n != "" {
		return n
	}
	if w.sess.Token() == "" {
		return "this session has no agent-bus channels; use create_channel or join_channel first"
	}
	return "all of this session's channels have ended (the peer left or the channel expired)"
}
