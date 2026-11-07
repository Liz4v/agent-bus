// Package brokerfile reads and writes the client-side broker file: the pinned
// broker fingerprint and, once registered, the account credential (multi-user
// M14). One file per broker, mode 0600. Copying the file to another host
// carries both the trust decision and the account.
package brokerfile

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// DefaultPort is the broker port used when a URL names none.
const DefaultPort = "7700"

// File is the content of one broker file.
type File struct {
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Account     string `json:"account,omitempty"`
	Credential  string `json:"credential,omitempty"`
	// Hostmaster is a local hint only: it decides whether the shim registers
	// the admin tools. The broker always enforces.
	Hostmaster bool `json:"hostmaster,omitempty"`
}

// ErrInsecureURL is returned for an http:// broker URL.
var ErrInsecureURL = errors.New("the broker URL must use https:// (plain http:// is no longer supported); change it to https://")

// Dir returns the configuration directory: $AGENT_BUS_CONFIG_DIR, else
// $XDG_CONFIG_HOME/agent-bus, else ~/.config/agent-bus.
func Dir() (string, error) {
	if d := os.Getenv("AGENT_BUS_CONFIG_DIR"); d != "" {
		return d, nil
	}
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "agent-bus"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot find a configuration directory: %w", err)
	}
	return filepath.Join(home, ".config", "agent-bus"), nil
}

// NormalizeURL returns the canonical form https://host:port. A missing scheme
// means https, a missing port means 7700, the host is lowercased and any path
// is dropped. http:// is refused.
func NormalizeURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("no broker URL given")
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid broker URL %q: %w", raw, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
	case "http":
		return "", ErrInsecureURL
	default:
		return "", fmt.Errorf("invalid broker URL scheme %q: use https://", u.Scheme)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", fmt.Errorf("invalid broker URL %q: no host", raw)
	}
	port := u.Port()
	if port == "" {
		port = DefaultPort
	}
	return "https://" + net.JoinHostPort(host, port), nil
}

// PathFor returns the broker file's path relative to Dir: brokers/<host>_<port>.json.
func PathFor(rawURL string) (string, error) {
	n, err := NormalizeURL(rawURL)
	if err != nil {
		return "", err
	}
	u, _ := url.Parse(n)
	var b strings.Builder
	for _, r := range u.Hostname() + "_" + u.Port() {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return filepath.Join("brokers", b.String()+".json"), nil
}

// Load reads the broker file for rawURL under dir. A missing file is an error
// satisfying errors.Is(err, os.ErrNotExist). A file readable by group or others
// is refused, because it may hold a credential.
func Load(dir, rawURL string) (File, error) {
	rel, err := PathFor(rawURL)
	if err != nil {
		return File{}, err
	}
	path := filepath.Join(dir, rel)
	fi, err := os.Stat(path)
	if err != nil {
		return File{}, err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return File{}, fmt.Errorf("%s has mode %o; it holds a credential and must not be readable by group or others (chmod 600)", path, fi.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return File{}, fmt.Errorf("%s is not valid: %w", path, err)
	}
	return f, nil
}

// Save writes f atomically with the directory at 0700 and the file at 0600.
// f.URL is normalised.
func Save(dir string, f File) error {
	n, err := NormalizeURL(f.URL)
	if err != nil {
		return err
	}
	f.URL = n
	rel, _ := PathFor(n)
	path := filepath.Join(dir, rel)
	if err := mkdirPrivate(dir, filepath.Dir(path)); err != nil {
		return err
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(path, append(b, '\n'))
}

// CheckWritable verifies that a broker file for rawURL could be saved under
// dir: it creates the directory, then writes and removes a temporary file
// next to the target.
func CheckWritable(dir, rawURL string) error {
	rel, err := PathFor(rawURL)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, rel)
	if err := mkdirPrivate(dir, filepath.Dir(path)); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".check-*")
	if err != nil {
		return fmt.Errorf("cannot write to %s: %w", filepath.Dir(path), err)
	}
	name := tmp.Name()
	tmp.Close()
	return os.Remove(name)
}

func mkdirPrivate(dirs ...string) error {
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", d, err)
		}
		if err := os.Chmod(d, 0o700); err != nil {
			return fmt.Errorf("secure %s: %w", d, err)
		}
	}
	return nil
}

func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	name := tmp.Name()
	fail := func(err error) error { tmp.Close(); os.Remove(name); return fmt.Errorf("write %s: %w", path, err) }
	if err := tmp.Chmod(0o600); err != nil {
		return fail(err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
