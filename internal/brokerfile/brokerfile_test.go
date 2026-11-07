package brokerfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeURL(t *testing.T) {
	ok := map[string]string{
		"broker-host":                 "https://broker-host:7700",
		"broker-host:7700":            "https://broker-host:7700",
		"https://broker-host":         "https://broker-host:7700",
		"https://Broker-Host:9000/x":  "https://broker-host:9000",
		"HTTPS://broker-host/a?b=c":   "https://broker-host:7700",
		"https://[::1]:8443":          "https://[::1]:8443",
		"[2001:db8::1]":               "https://[2001:db8::1]:7700",
		"  https://broker-host:7700 ": "https://broker-host:7700",
	}
	for in, want := range ok {
		got, err := NormalizeURL(in)
		if err != nil || got != want {
			t.Errorf("NormalizeURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := NormalizeURL("http://broker-host:7700"); !errors.Is(err, ErrInsecureURL) {
		t.Errorf("http:// error = %v, want ErrInsecureURL", err)
	}
	for _, bad := range []string{"", "ftp://x", "https://"} {
		if _, err := NormalizeURL(bad); err == nil {
			t.Errorf("NormalizeURL(%q) succeeded", bad)
		}
	}
}

func TestMixedCaseHostSharesPath(t *testing.T) {
	a, _ := PathFor("https://Broker-Host:7700")
	b, _ := PathFor("broker-host")
	if a != b || a != filepath.Join("brokers", "broker-host_7700.json") {
		t.Fatalf("paths %q and %q", a, b)
	}
	v6, _ := PathFor("https://[::1]:7700")
	if strings.ContainsAny(v6, ":[]") {
		t.Fatalf("unsafe IPv6 path %q", v6)
	}
}

func TestDirPrecedence(t *testing.T) {
	t.Setenv("AGENT_BUS_CONFIG_DIR", "/a")
	t.Setenv("XDG_CONFIG_HOME", "/b")
	if d, _ := Dir(); d != "/a" {
		t.Errorf("Dir = %q, want /a", d)
	}
	t.Setenv("AGENT_BUS_CONFIG_DIR", "")
	if d, _ := Dir(); d != "/b/agent-bus" {
		t.Errorf("Dir = %q, want /b/agent-bus", d)
	}
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "/home/u")
	if d, _ := Dir(); d != "/home/u/.config/agent-bus" {
		t.Errorf("Dir = %q", d)
	}
}

func TestRoundTripAndModes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cfg")
	in := File{URL: "https://Broker-Host", Fingerprint: "SHA256:abc", Account: "alice", Credential: "secret", Hostmaster: true}
	if err := Save(dir, in); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir, "broker-host:7700")
	if err != nil {
		t.Fatal(err)
	}
	in.URL = "https://broker-host:7700"
	if got != in {
		t.Fatalf("got %+v, want %+v", got, in)
	}
	path := filepath.Join(dir, "brokers", "broker-host_7700.json")
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("file mode %o", fi.Mode().Perm())
	}
	for _, d := range []string{dir, filepath.Join(dir, "brokers")} {
		if fi, _ := os.Stat(d); fi.Mode().Perm() != 0o700 {
			t.Errorf("%s mode %o", d, fi.Mode().Perm())
		}
	}
	ents, _ := os.ReadDir(filepath.Join(dir, "brokers"))
	if len(ents) != 1 {
		t.Errorf("leftover temp files: %v", ents)
	}
}

func TestLoadRefusesPermissiveMode(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, File{URL: "broker-host", Fingerprint: "SHA256:x"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "brokers", "broker-host_7700.json")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, "broker-host"); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("Load = %v, want a mode error", err)
	}
}

func TestLoadMissing(t *testing.T) {
	_, err := Load(t.TempDir(), "broker-host")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want ErrNotExist", err)
	}
}

func TestSaveRefusesHTTP(t *testing.T) {
	if err := Save(t.TempDir(), File{URL: "http://broker-host"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestCheckWritable(t *testing.T) {
	dir := t.TempDir()
	if err := CheckWritable(dir, "broker-host"); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(filepath.Join(dir, "brokers"))
	if len(ents) != 0 {
		t.Fatalf("temp file left behind: %v", ents)
	}
	if os.Getuid() == 0 {
		return
	}
	ro := t.TempDir()
	os.Chmod(ro, 0o500)
	defer os.Chmod(ro, 0o700)
	if err := CheckWritable(filepath.Join(ro, "x"), "broker-host"); err == nil {
		t.Fatal("expected an error for an unwritable directory")
	}
}
