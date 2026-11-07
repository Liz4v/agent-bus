package identity

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreateThenReloadSameKeyAndFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "key.pem")
	k1, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if !k1.Equal(k2) {
		t.Fatal("reloaded key differs")
	}
	c1, _ := SelfSignedCert(k1, time.Now())
	c2, _ := SelfSignedCert(k2, time.Now())
	f1, _ := Fingerprint(c1)
	f2, _ := Fingerprint(c2)
	if f1 != f2 {
		t.Fatalf("fingerprints differ: %s vs %s", f1, f2)
	}
}

func TestTwoCertsFromOneKeyShareFingerprint(t *testing.T) {
	k, err := LoadOrCreateKey(filepath.Join(t.TempDir(), "k.pem"))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := SelfSignedCert(k, time.Now())
	b, _ := SelfSignedCert(k, time.Now().Add(48*time.Hour))
	if string(a.Certificate[0]) == string(b.Certificate[0]) {
		t.Fatal("expected distinct certificates")
	}
	fa, _ := Fingerprint(a)
	fb, _ := Fingerprint(b)
	if fa != fb {
		t.Fatal("fingerprint changed between certs of one key")
	}
}

func TestKeyFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k.pem")
	if _, err := LoadOrCreateKey(path); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 600", fi.Mode().Perm())
	}
}

func TestCorruptKeyIsErrorAndNotOverwritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k.pem")
	if err := os.WriteFile(path, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateKey(path); err == nil {
		t.Fatal("expected error for a corrupt key")
	}
	b, _ := os.ReadFile(path)
	if string(b) != "not a key" {
		t.Fatalf("corrupt file was overwritten: %q", b)
	}
}

func TestUnreadableKeyIsErrorAndNotOverwritten(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can read everything")
	}
	path := filepath.Join(t.TempDir(), "k.pem")
	if err := os.WriteFile(path, []byte("secret"), 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateKey(path); err == nil {
		t.Fatal("expected error for an unreadable key")
	}
	os.Chmod(path, 0o600)
	if b, _ := os.ReadFile(path); string(b) != "secret" {
		t.Fatalf("unreadable file was overwritten: %q", b)
	}
}

func TestFingerprintFormat(t *testing.T) {
	k, _ := LoadOrCreateKey(filepath.Join(t.TempDir(), "k.pem"))
	c, _ := SelfSignedCert(k, time.Now())
	f, err := Fingerprint(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(f, "SHA256:") {
		t.Fatalf("missing prefix: %s", f)
	}
	rest := strings.TrimPrefix(f, "SHA256:")
	if strings.Contains(rest, "=") || len(rest) != 43 {
		t.Fatalf("want 43 unpadded base64 chars, got %q", rest)
	}
	if b, err := base64.RawStdEncoding.DecodeString(rest); err != nil || len(b) != 32 {
		t.Fatalf("not a raw base64 SHA-256: %v", err)
	}
}

func TestCertLifetimeAndTLSConfig(t *testing.T) {
	k, _ := LoadOrCreateKey(filepath.Join(t.TempDir(), "k.pem"))
	now := time.Now()
	c, err := SelfSignedCert(k, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Leaf.NotAfter.Sub(now); got < 364*24*time.Hour || got > 366*24*time.Hour {
		t.Fatalf("validity = %v, want about 1 year", got)
	}
	cfg := ServerTLSConfig(c)
	if cfg.MinVersion != 0x0304 {
		t.Fatalf("MinVersion = %x, want TLS 1.3", cfg.MinVersion)
	}
}
