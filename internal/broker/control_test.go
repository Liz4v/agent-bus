package broker

import (
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

func startControl(t *testing.T, s *Store, log *syncBuffer) *ControlServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.sock")
	cs, err := ListenControl(path, s, "SHA256:testprint", slogTo(log))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cs.Close)
	return cs
}

func TestControlFingerprint(t *testing.T) {
	s, _ := openTestStore(t)
	cs := startControl(t, s, &syncBuffer{})
	fp, err := ControlFingerprint(cs.Path())
	if err != nil || fp != "SHA256:testprint" {
		t.Fatalf("%q %v", fp, err)
	}
}

func TestControlRecoverModesAndSingleUse(t *testing.T) {
	s, _ := openTestStore(t)
	logs := &syncBuffer{}
	cs := startControl(t, s, logs)

	r, err := ControlRecover(cs.Path())
	if err != nil || r.Mode != protocol.ClaimModeClaim || !claimFormat.MatchString(r.ClaimCode) {
		t.Fatalf("unclaimed: %+v %v", r, err)
	}
	if _, err := s.Register(bg, "boss", r.ClaimCode); err != nil {
		t.Fatalf("the issued code does not work: %v", err)
	}
	if _, err := s.Register(bg, "again", r.ClaimCode); err != ErrRegistrationRefused {
		t.Fatalf("code reuse: %v", err)
	}

	r, err = ControlRecover(cs.Path())
	if err != nil || r.Mode != protocol.ClaimModeRecover {
		t.Fatalf("claimed: %+v %v", r, err)
	}
	if strings.Contains(logs.String(), r.ClaimCode) || strings.Contains(logs.String(), strings.ReplaceAll(r.ClaimCode, "-", "")) {
		t.Fatal("the recovery code was logged")
	}
	res, err := s.Register(bg, "boss2", r.ClaimCode)
	if err != nil || !res.Hostmaster {
		t.Fatalf("recover: %+v %v", res, err)
	}
}

func TestControlUnknownCommand(t *testing.T) {
	s, _ := openTestStore(t)
	cs := startControl(t, s, &syncBuffer{})
	if _, err := ControlRequest(cs.Path(), "BOGUS"); err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("got %v", err)
	}
	c, err := net.Dial("unix", cs.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Write([]byte("BOGUS\n"))
	var m map[string]string
	if err := json.NewDecoder(c).Decode(&m); err != nil || m["error"] != "unknown command" {
		t.Fatalf("%v %v", m, err)
	}
}

func TestControlSocketModeAndStaleReplacement(t *testing.T) {
	s, _ := openTestStore(t)
	path := filepath.Join(t.TempDir(), "control.sock")
	cs, err := ListenControl(path, s, "fp", nil)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v %v", fi.Mode(), err)
	}
	// A live socket is not replaced.
	if _, err := ListenControl(path, s, "fp", nil); err == nil {
		t.Fatal("a live control socket was replaced")
	}
	// Simulate a crash: close the listener without removing the file.
	cs.ln.(*net.UnixListener).SetUnlinkOnClose(false)
	cs.ln.Close()
	cs.wg.Wait()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("socket file vanished: %v", err)
	}
	cs2, err := ListenControl(path, s, "fp2", nil)
	if err != nil {
		t.Fatalf("stale socket not replaced: %v", err)
	}
	defer cs2.Close()
	if fp, err := ControlFingerprint(path); err != nil || fp != "fp2" {
		t.Fatalf("%q %v", fp, err)
	}
	// A regular file in the way is refused and kept.
	file := filepath.Join(t.TempDir(), "afile")
	os.WriteFile(file, []byte("x"), 0o600)
	if _, err := ListenControl(file, s, "fp", nil); err == nil {
		t.Fatal("a regular file was replaced")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal("regular file removed")
	}
}

func TestControlPathTooLong(t *testing.T) {
	s, _ := openTestStore(t)
	long := filepath.Join(t.TempDir(), strings.Repeat("x", 120), "control.sock")
	_, err := ListenControl(long, s, "fp", nil)
	if err == nil || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("got %v", err)
	}
}

func TestControlRequestUnreachable(t *testing.T) {
	_, err := ControlRequest(filepath.Join(t.TempDir(), "none.sock"), "RECOVER")
	if err == nil || !strings.Contains(err.Error(), "control socket") {
		t.Fatalf("got %v", err)
	}
}

func slogTo(b *syncBuffer) *slog.Logger { return slog.New(slog.NewTextHandler(b, nil)) }
