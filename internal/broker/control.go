package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// maxSocketPath is the longest Unix socket path accepted (the OS limit is
// about 108 bytes; a longer path is a clear start error, not a truncation).
const maxSocketPath = 100

// ControlServer is the host-side control socket (M5). Being able to reach it
// is the proof of host access.
type ControlServer struct {
	path string
	ln   net.Listener
	wg   sync.WaitGroup
}

// ListenControl binds the control socket at path with mode 0600 and serves
// RECOVER and FINGERPRINT, one request per connection. A stale socket (dial
// gives exactly ECONNREFUSED) is replaced; a live one is an error.
func ListenControl(path string, store *Store, fingerprint string, log *slog.Logger) (*ControlServer, error) {
	if log == nil {
		log = slog.Default()
	}
	if len(path) > maxSocketPath {
		return nil, fmt.Errorf("control socket path %q is too long for a Unix socket (limit %d bytes); use --control with a shorter path", path, maxSocketPath)
	}
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", path)
		}
		c, err := net.DialTimeout("unix", path, time.Second)
		switch {
		case err == nil:
			c.Close()
			return nil, fmt.Errorf("control socket %s is served by another process", path)
		case errors.Is(err, syscall.ECONNREFUSED):
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("probe %s: %w", path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on control socket %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		os.Remove(path)
		return nil, err
	}
	cs := &ControlServer{path: path, ln: ln}
	cs.wg.Add(1)
	go cs.accept(store, fingerprint, log)
	return cs, nil
}

// Path returns the socket path.
func (cs *ControlServer) Path() string { return cs.path }

// Close stops serving and removes the socket.
func (cs *ControlServer) Close() {
	cs.ln.Close()
	cs.wg.Wait()
	os.Remove(cs.path)
}

func (cs *ControlServer) accept(store *Store, fingerprint string, log *slog.Logger) {
	defer cs.wg.Done()
	for {
		c, err := cs.ln.Accept()
		if err != nil {
			return
		}
		cs.wg.Add(1)
		go func() {
			defer cs.wg.Done()
			defer c.Close()
			serveControl(c, store, fingerprint, log)
		}()
	}
}

func serveControl(c net.Conn, store *Store, fingerprint string, log *slog.Logger) {
	c.SetDeadline(time.Now().Add(5 * time.Second))
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return
	}
	var reply any
	switch strings.TrimSpace(line) {
	case protocol.ControlCmdFingerprint:
		reply = protocol.ControlFingerprintReply{Fingerprint: fingerprint}
	case protocol.ControlCmdRecover:
		code, mode, err := store.IssueClaim(context.Background())
		if err != nil {
			log.Error("control: issue claim code", "err", err)
			reply = protocol.ControlError{Error: "internal error"}
			break
		}
		// The code goes to the caller only, never to the log.
		log.Info("claim code issued over the control socket", "mode", mode)
		reply = protocol.ControlRecoverReply{ClaimCode: code, Mode: mode}
	default:
		reply = protocol.ControlError{Error: "unknown command"}
	}
	b, err := json.Marshal(reply)
	if err != nil {
		return
	}
	c.Write(append(b, '\n'))
}

// ControlRequest sends one command to the control socket at path and returns
// the raw JSON reply line. It is the client used by the CLI.
func ControlRequest(path, cmd string) ([]byte, error) {
	c, err := net.DialTimeout("unix", path, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("cannot reach the broker's control socket %s (is the broker running, and are you root or the broker's user?): %w", path, err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(cmd + "\n")); err != nil {
		return nil, err
	}
	line, err := bufio.NewReader(c).ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read control reply: %w", err)
	}
	var e protocol.ControlError
	if json.Unmarshal(line, &e) == nil && e.Error != "" {
		return nil, errors.New("control socket: " + e.Error)
	}
	return line, nil
}

// ControlRecover asks the running broker for a fresh claim or recovery code.
func ControlRecover(path string) (protocol.ControlRecoverReply, error) {
	var r protocol.ControlRecoverReply
	b, err := ControlRequest(path, protocol.ControlCmdRecover)
	if err != nil {
		return r, err
	}
	return r, json.Unmarshal(b, &r)
}

// ControlFingerprint asks the running broker for its public-key fingerprint.
func ControlFingerprint(path string) (string, error) {
	var r protocol.ControlFingerprintReply
	b, err := ControlRequest(path, protocol.ControlCmdFingerprint)
	if err != nil {
		return "", err
	}
	err = json.Unmarshal(b, &r)
	return r.Fingerprint, err
}
