package enroll

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Liz4v/agent-bus/internal/broker"
	"github.com/Liz4v/agent-bus/internal/brokerfile"
	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/testutil"
)

var bg = context.Background()

type fixture struct {
	t      *testing.T
	store  *broker.Store
	url    string
	pin    string
	hits   atomic.Int64
	probes atomic.Int64
	dir    string
	out    *bytes.Buffer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	store, err := broker.OpenStore(filepath.Join(t.TempDir(), "e.db"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	f := &fixture{t: t, store: store, dir: filepath.Join(t.TempDir(), "cfg"), out: &bytes.Buffer{}}
	h := broker.NewServer(store, broker.NewTracker(nil), slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
	ts, pin := testutil.TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		h.ServeHTTP(w, r)
	}))
	f.url, f.pin = ts.URL, pin
	return f
}

// env builds an Env reading the given scripted input.
func (f *fixture) env(tty bool, input string) *Env {
	return &Env{
		Stdin: strings.NewReader(input), Stdout: f.out, Stderr: f.out,
		IsTTY:     func() bool { return tty },
		ConfigDir: f.dir,
		Probe: func(ctx context.Context, url string) (string, error) {
			f.probes.Add(1)
			return client.Probe(ctx, url)
		},
	}
}

func (f *fixture) file() brokerfile.File {
	f.t.Helper()
	bf, err := brokerfile.Load(f.dir, f.url)
	if err != nil {
		f.t.Fatal(err)
	}
	return bf
}

func (f *fixture) savePinned() {
	f.t.Helper()
	if err := brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: f.pin}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) claimCode() string {
	f.t.Helper()
	code, _, err := f.store.IssueClaim(bg)
	if err != nil {
		f.t.Fatal(err)
	}
	return code
}

func TestNonTTYRefusals(t *testing.T) {
	f := newFixture(t)
	e := f.env(false, "yes\nyes\n")
	for name, err := range map[string]error{
		"pin":      e.Pin(bg, f.url),
		"unpin":    e.Unpin(bg, f.url),
		"register": e.Register(bg, f.url, "alice"),
		"claim":    e.Claim(bg, f.url, "alice"),
	} {
		if err == nil || !strings.Contains(err.Error(), "human-only") {
			t.Errorf("%s: %v", name, err)
		}
	}
	if f.hits.Load() != 0 || f.probes.Load() != 0 {
		t.Fatal("a refused command made network calls")
	}
	if _, err := os.Stat(f.dir); err == nil {
		t.Fatal("a refused command wrote files")
	}
}

func TestPinAcceptAndDecline(t *testing.T) {
	f := newFixture(t)
	if err := f.env(true, "no\n").Pin(bg, f.url); !errors.Is(err, ErrDeclined) {
		t.Fatalf("decline: %v", err)
	}
	if _, err := brokerfile.Load(f.dir, f.url); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a declined pin was saved")
	}
	if err := f.env(true, "YES\n").Pin(bg, f.url); err != nil {
		t.Fatal(err)
	}
	if got := f.file().Fingerprint; got != f.pin {
		t.Fatalf("pinned %q, want %q", got, f.pin)
	}
	if !strings.Contains(f.out.String(), f.pin) {
		t.Fatal("the fingerprint was not shown")
	}
	// Pinning again with the same key is a no-op that asks nothing.
	if err := f.env(true, "").Pin(bg, f.url); err != nil {
		t.Fatalf("re-pin: %v", err)
	}
}

func TestPinConflict(t *testing.T) {
	f := newFixture(t)
	if err := brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: "SHA256:OLDOLDOLD"}); err != nil {
		t.Fatal(err)
	}
	err := f.env(true, "yes\n").Pin(bg, f.url)
	if err == nil || !strings.Contains(err.Error(), "different key") || !strings.Contains(err.Error(), "unpin") {
		t.Fatalf("got %v", err)
	}
	if f.file().Fingerprint != "SHA256:OLDOLDOLD" {
		t.Fatal("the existing pin was overwritten")
	}
}

func TestUnpinKeepsCredential(t *testing.T) {
	f := newFixture(t)
	brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: f.pin, Account: "alice", Credential: "cred-value"})
	if err := f.env(true, "no\n").Unpin(bg, f.url); !errors.Is(err, ErrDeclined) {
		t.Fatal(err)
	}
	if f.file().Fingerprint == "" {
		t.Fatal("unpin without confirmation")
	}
	if err := f.env(true, "yes\n").Unpin(bg, f.url); err != nil {
		t.Fatal(err)
	}
	got := f.file()
	if got.Fingerprint != "" || got.Credential != "cred-value" || got.Account != "alice" {
		t.Fatalf("after unpin: %+v", got)
	}
}

func TestRegisterWritesCredentialAt0600(t *testing.T) {
	f := newFixture(t)
	// A claimed broker is needed for pending registrations.
	if _, err := f.store.Register(bg, "boss", f.claimCode()); err != nil {
		t.Fatal(err)
	}
	if err := f.env(true, "yes\n").Register(bg, f.url, "alice"); err != nil {
		t.Fatal(err)
	}
	got := f.file()
	if got.Account != "alice" || got.Credential == "" || got.Fingerprint != f.pin || got.Hostmaster {
		t.Fatalf("file: %+v", got)
	}
	rel, _ := brokerfile.PathFor(f.url)
	path := filepath.Join(f.dir, rel)
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode: %v %v", fi, err)
	}
	if !strings.Contains(f.out.String(), "waiting for the hostmaster") {
		t.Fatalf("output: %s", f.out)
	}
	if strings.Contains(f.out.String(), got.Credential) {
		t.Fatal("register printed the credential")
	}
}

func TestRegisterRefusedMessage(t *testing.T) {
	f := newFixture(t) // unclaimed: every registration is refused
	f.savePinned()
	err := f.env(true, "").Register(bg, f.url, "alice")
	if err == nil || !strings.Contains(err.Error(), "the name may be taken or the broker may be full") {
		t.Fatalf("got %v", err)
	}
	if f.file().Credential != "" {
		t.Fatal("a refused registration stored a credential")
	}
}

func TestClaimReadsCodeFromStdinAndSetsHostmaster(t *testing.T) {
	f := newFixture(t)
	code := f.claimCode()
	if err := f.env(true, "yes\n"+code+"\n").Claim(bg, f.url, "boss"); err != nil {
		t.Fatal(err)
	}
	got := f.file()
	if !got.Hostmaster || got.Account != "boss" || got.Credential == "" {
		t.Fatalf("file: %+v", got)
	}
	if strings.Contains(f.out.String(), code) {
		t.Fatal("the claim code was echoed")
	}
	// A wrong code is a generic refusal and stores nothing new.
	f2 := newFixture(t)
	f2.claimCode()
	err := f2.env(true, "yes\nAAAAA-AAAAA-AAAAA-AAAAA\n").Claim(bg, f2.url, "boss")
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("wrong code: %v", err)
	}
}

func TestMalformedNameRefusedLocallyWithNoRequest(t *testing.T) {
	f := newFixture(t)
	for _, name := range []string{"", "a b", "a/b", strings.Repeat("x", 65), "é"} {
		for _, fn := range []func(*Env) error{
			func(e *Env) error { return e.Register(bg, f.url, name) },
			func(e *Env) error { return e.Claim(bg, f.url, name) },
		} {
			err := fn(f.env(true, "yes\nCODE\n"))
			if err == nil || !strings.Contains(err.Error(), "not a valid account name") {
				t.Errorf("name %q: %v", name, err)
			}
		}
	}
	if f.hits.Load() != 0 || f.probes.Load() != 0 {
		t.Fatal("a malformed name caused a network call")
	}
}

func TestStatusForEachState(t *testing.T) {
	f := newFixture(t)
	code := f.claimCode()
	boss, _ := f.store.Register(bg, "boss", code)
	pending, _ := f.store.Register(bg, "pend", "")
	rej, _ := f.store.Register(bg, "rej", "")
	f.store.Reject(bg, "rej")
	act, _ := f.store.Register(bg, "act", "")
	f.store.Approve(bg, "act")
	rem, _ := f.store.Register(bg, "rem", "")
	f.store.Approve(bg, "rem")
	f.store.Remove(bg, "rem")

	for _, c := range []struct {
		cred, want string
		hint       bool // stored hostmaster hint before status
	}{
		{boss.Credential, "hostmaster: true", false},
		{pending.Credential, "pending", false},
		{rej.Credential, "rejected", false},
		{act.Credential, "active", false},
		{rem.Credential, "removed", false},
	} {
		f.out.Reset()
		brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: f.pin, Account: "stale", Credential: c.cred, Hostmaster: c.hint})
		if err := f.env(false, "").Status(bg, f.url); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(f.out.String(), c.want) {
			t.Errorf("status output lacks %q:\n%s", c.want, f.out)
		}
		if strings.Contains(f.out.String(), c.cred) {
			t.Errorf("status printed the credential:\n%s", f.out)
		}
	}
	// The hostmaster hint was refreshed from the broker.
	brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: f.pin, Account: "boss", Credential: boss.Credential})
	f.env(false, "").Status(bg, f.url)
	if !f.file().Hostmaster {
		t.Fatal("hostmaster hint not refreshed")
	}
}

func TestStatusWithoutSetup(t *testing.T) {
	f := newFixture(t)
	e := f.env(false, "")
	if err := e.Status(bg, f.url); err != nil || !strings.Contains(f.out.String(), "not set up") {
		t.Fatalf("%v %s", err, f.out)
	}
	f.out.Reset()
	brokerfile.Save(f.dir, brokerfile.File{URL: f.url})
	e.Status(bg, f.url)
	if !strings.Contains(f.out.String(), "not pinned") {
		t.Fatal(f.out.String())
	}
	f.out.Reset()
	f.savePinned()
	e.Status(bg, f.url)
	if !strings.Contains(f.out.String(), "no account yet") {
		t.Fatal(f.out.String())
	}
	if f.hits.Load() != 0 {
		t.Fatal("status contacted the broker without a credential")
	}
	f.out.Reset()
	brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: f.pin, Account: "ghost", Credential: "unknown"})
	if err := e.Status(bg, f.url); err != nil || !strings.Contains(f.out.String(), "does not recognise") {
		t.Fatalf("%v %s", err, f.out)
	}
}

func TestUnwritableConfigDirFailsBeforeAnyRequest(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can write anywhere")
	}
	f := newFixture(t)
	ro := t.TempDir()
	os.Chmod(ro, 0o500)
	defer os.Chmod(ro, 0o700)
	f.dir = filepath.Join(ro, "cfg")
	if err := f.env(true, "yes\n").Register(bg, f.url, "alice"); err == nil {
		t.Fatal("register succeeded with an unwritable directory")
	}
	if f.hits.Load() != 0 || f.probes.Load() != 0 {
		t.Fatal("a request was sent before the write check")
	}
}

func TestRegisterOverExistingCredentialNeedsConfirmation(t *testing.T) {
	f := newFixture(t)
	f.store.Register(bg, "boss", f.claimCode())
	brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: f.pin, Account: "old", Credential: "old-cred"})

	err := f.env(true, "no\n").Register(bg, f.url, "alice")
	if !errors.Is(err, ErrDeclined) {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(f.out.String(), `"old"`) {
		t.Fatalf("the stored account name was not shown:\n%s", f.out)
	}
	if f.hits.Load() != 0 || f.probes.Load() != 0 {
		t.Fatal("a request was sent without confirmation")
	}
	if got := f.file(); got.Credential != "old-cred" {
		t.Fatalf("credential replaced without confirmation: %+v", got)
	}
	if err := f.env(true, "yes\n").Register(bg, f.url, "alice"); err != nil {
		t.Fatal(err)
	}
	if got := f.file(); got.Account != "alice" || got.Credential == "old-cred" || got.Credential == "" {
		t.Fatalf("not replaced after yes: %+v", got)
	}
}

func TestWrongPinSurfacesGuidance(t *testing.T) {
	f := newFixture(t)
	f.store.Register(bg, "boss", f.claimCode())
	brokerfile.Save(f.dir, brokerfile.File{URL: f.url, Fingerprint: "SHA256:WRONGWRONG"})
	err := f.env(true, "").Register(bg, f.url, "alice")
	var pe *client.PinError
	if !errors.As(err, &pe) || !strings.Contains(err.Error(), "unpin") {
		t.Fatalf("got %v", err)
	}
	if f.hits.Load() != 0 {
		t.Fatal("a request reached the broker through a wrong pin")
	}
}
