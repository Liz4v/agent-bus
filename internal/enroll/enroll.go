// Package enroll implements the human-only setup commands: pin, unpin,
// register and claim, plus the read-only status (multi-user M15). The
// interactive ones refuse to run unless stdin is a terminal and require typed
// confirmation, so an agent using a non-interactive shell cannot complete them
// by accident. All I/O is injected so the flows can be tested.
package enroll

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Liz4v/agent-bus/internal/brokerfile"
	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/protocol"
)

// ErrDeclined is returned when the human declines a confirmation.
var ErrDeclined = errors.New("cancelled: nothing was changed")

// Env is everything the flows need from the outside world.
type Env struct {
	Stdin          io.Reader
	Stdout, Stderr io.Writer
	// IsTTY reports whether stdin is an interactive terminal.
	IsTTY func() bool
	// ConfigDir is the brokerfile directory.
	ConfigDir string
	// Probe returns a broker's fingerprint (default client.Probe).
	Probe func(ctx context.Context, url string) (string, error)
	// NewClient builds a pinned client (default client.New).
	NewClient func(client.Config) (*client.Client, error)

	br *bufio.Reader
}

// DefaultEnv returns an Env wired to the real process.
func DefaultEnv() (*Env, error) {
	dir, err := brokerfile.Dir()
	if err != nil {
		return nil, err
	}
	return &Env{
		Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr,
		IsTTY:     StdinIsTTY,
		ConfigDir: dir,
	}, nil
}

// IsTTYReader reports whether r is a terminal: an *os.File that is a
// character device.
func IsTTYReader(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// StdinIsTTY reports whether os.Stdin is a character device (a terminal).
func StdinIsTTY() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func (e *Env) probe(ctx context.Context, url string) (string, error) {
	if e.Probe != nil {
		return e.Probe(ctx, url)
	}
	return client.Probe(ctx, url)
}

func (e *Env) newClient(cfg client.Config) (*client.Client, error) {
	if e.NewClient != nil {
		return e.NewClient(cfg)
	}
	return client.New(cfg)
}

func (e *Env) requireTTY(cmd string) error {
	if e.IsTTY == nil || !e.IsTTY() {
		return fmt.Errorf("agent-bus %s is a human-only command and needs an interactive terminal (stdin is not a terminal); a person must run it, not an agent", cmd)
	}
	return nil
}

func (e *Env) printf(format string, a ...any) { fmt.Fprintf(e.Stdout, format, a...) }

// prompt prints msg and reads one trimmed line from stdin.
func (e *Env) prompt(msg string) (string, error) {
	if e.br == nil {
		e.br = bufio.NewReader(e.Stdin)
	}
	fmt.Fprint(e.Stdout, msg)
	line, err := e.br.ReadString('\n')
	if err != nil && (err != io.EOF || line == "") {
		return "", fmt.Errorf("no answer read from the terminal: %w", err)
	}
	return strings.TrimSpace(line), nil
}

// confirm asks the human to type "yes".
func (e *Env) confirm(msg string) (bool, error) {
	ans, err := e.prompt(msg + " Type yes to continue: ")
	if err != nil {
		return false, err
	}
	return strings.EqualFold(ans, "yes"), nil
}

// load reads the broker file; a missing file is an empty File for url.
func (e *Env) load(url string) (brokerfile.File, error) {
	f, err := brokerfile.Load(e.ConfigDir, url)
	if errors.Is(err, os.ErrNotExist) {
		return brokerfile.File{URL: url}, nil
	}
	if err != nil {
		return brokerfile.File{}, err
	}
	f.URL = url
	return f, nil
}

// pinFlow makes sure f has a pin, probing the broker and asking the human to
// confirm the fingerprint on first contact (TOFU). It saves nothing itself.
func (e *Env) pinFlow(ctx context.Context, f *brokerfile.File) error {
	if f.Fingerprint != "" {
		return nil
	}
	fp, err := e.probe(ctx, f.URL)
	if err != nil {
		return err
	}
	e.printf("First contact with the broker at %s.\nIts key fingerprint is:\n\n    %s\n\n", f.URL, fp)
	e.printf("Compare it with the output of `sudo agent-bus broker fingerprint` on the broker host.\nIf it does not match, answer anything but yes.\n")
	ok, err := e.confirm("Trust this broker?")
	if err != nil {
		return err
	}
	if !ok {
		return ErrDeclined
	}
	f.Fingerprint = fp
	return nil
}

// Pin pins the broker's fingerprint after the human confirms it.
func (e *Env) Pin(ctx context.Context, rawURL string) error {
	if err := e.requireTTY("pin"); err != nil {
		return err
	}
	url, err := brokerfile.NormalizeURL(rawURL)
	if err != nil {
		return err
	}
	f, err := e.load(url)
	if err != nil {
		return err
	}
	if f.Fingerprint != "" {
		fp, err := e.probe(ctx, url)
		if err != nil {
			return err
		}
		if fp == f.Fingerprint {
			e.printf("%s is already pinned to %s.\n", url, fp)
			return nil
		}
		return fmt.Errorf("%s is already pinned to a different key (%s; the broker now shows %s). If you trust the new key, run `agent-bus unpin --broker %s` first and then pin again", url, f.Fingerprint, fp, url)
	}
	if err := brokerfile.CheckWritable(e.ConfigDir, url); err != nil {
		return err
	}
	if err := e.pinFlow(ctx, &f); err != nil {
		return err
	}
	if err := brokerfile.Save(e.ConfigDir, f); err != nil {
		return err
	}
	e.printf("Pinned %s. Restart any running Claude Code or Hermes session that uses this broker.\n", url)
	return nil
}

// Unpin removes the pin and keeps the credential.
func (e *Env) Unpin(ctx context.Context, rawURL string) error {
	if err := e.requireTTY("unpin"); err != nil {
		return err
	}
	url, err := brokerfile.NormalizeURL(rawURL)
	if err != nil {
		return err
	}
	f, err := brokerfile.Load(e.ConfigDir, url)
	if errors.Is(err, os.ErrNotExist) || (err == nil && f.Fingerprint == "") {
		e.printf("%s is not pinned.\n", url)
		return nil
	}
	if err != nil {
		return err
	}
	ok, err := e.confirm(fmt.Sprintf("Remove the pin %s for %s? The account credential is kept.", f.Fingerprint, url))
	if err != nil {
		return err
	}
	if !ok {
		return ErrDeclined
	}
	f.Fingerprint = ""
	f.URL = url
	if err := brokerfile.Save(e.ConfigDir, f); err != nil {
		return err
	}
	e.printf("Unpinned %s. Pin it again with `agent-bus pin --broker %s`.\n", url, url)
	return nil
}

// Register asks the broker for a normal account called name.
func (e *Env) Register(ctx context.Context, rawURL, name string) error {
	return e.register(ctx, "register", rawURL, name, false)
}

// Claim claims (or recovers) the hostmaster account. The claim code is read
// from a prompt, never from a flag or the environment.
func (e *Env) Claim(ctx context.Context, rawURL, name string) error {
	return e.register(ctx, "claim", rawURL, name, true)
}

func (e *Env) register(ctx context.Context, cmd, rawURL, name string, withCode bool) error {
	if err := e.requireTTY(cmd); err != nil {
		return err
	}
	if !protocol.ValidName(name) {
		return fmt.Errorf("%q is not a valid account name: use 1 to 64 letters, digits, '.', '_' or '-'", name)
	}
	url, err := brokerfile.NormalizeURL(rawURL)
	if err != nil {
		return err
	}
	f, err := e.load(url)
	if err != nil {
		return err
	}
	// Everything that can fail locally comes before any network call, so a
	// successful registration can never lose its credential to a write error.
	if err := brokerfile.CheckWritable(e.ConfigDir, url); err != nil {
		return err
	}
	if f.Credential != "" {
		who := f.Account
		if who == "" {
			who = "(unnamed)"
		}
		ok, err := e.confirm(fmt.Sprintf("%s already holds a credential for the account %q. Replacing it cannot be undone.", url, who))
		if err != nil {
			return err
		}
		if !ok {
			return ErrDeclined
		}
	}
	if err := e.pinFlow(ctx, &f); err != nil {
		return err
	}
	code := ""
	if withCode {
		if code, err = e.prompt("Claim code (from the broker's journal, or `sudo agent-bus broker recover`): "); err != nil {
			return err
		}
		if code == "" {
			return errors.New("no claim code entered")
		}
	}
	c, err := e.newClient(client.Config{BaseURL: url, Pin: f.Fingerprint})
	if err != nil {
		return err
	}
	res, err := c.Register(ctx, name, code)
	if err != nil {
		var ae *client.APIError
		if errors.As(err, &ae) && ae.Code == protocol.CodeRegistrationRefused {
			return errors.New("refused: the name may be taken or the broker may be full; try another name or ask the hostmaster")
		}
		var pe *client.PinError
		if errors.As(err, &pe) {
			return fmt.Errorf("%w; run `agent-bus unpin --broker %s` only if you know the broker's key changed", pe, url)
		}
		return err
	}
	f.Account, f.Credential, f.Hostmaster = res.Name, res.Credential, res.Hostmaster
	if err := brokerfile.Save(e.ConfigDir, f); err != nil {
		return fmt.Errorf("the broker created the account %q but the credential could not be saved: %w", res.Name, err)
	}
	switch {
	case res.Hostmaster:
		e.printf("You are the hostmaster of %s as %q. The admin tools appear after you restart Claude Code or Hermes.\n", url, res.Name)
	case res.Status == protocol.AccountPending:
		e.printf("Registered %q on %s. It is waiting for the hostmaster's approval; check with `agent-bus status --broker %s`, then restart your Claude Code or Hermes session.\n", res.Name, url, url)
	default:
		e.printf("Registered %q on %s. Restart your Claude Code or Hermes session.\n", res.Name, url)
	}
	return nil
}

// Status shows the account name and state without printing the credential, and
// refreshes the local hostmaster hint. It needs no terminal, so agents may run it.
func (e *Env) Status(ctx context.Context, rawURL string) error {
	url, err := brokerfile.NormalizeURL(rawURL)
	if err != nil {
		return err
	}
	f, err := brokerfile.Load(e.ConfigDir, url)
	switch {
	case errors.Is(err, os.ErrNotExist):
		e.printf("Broker %s: not set up. A person must run `agent-bus register --broker %s --name NAME` (or `claim`) in a terminal.\n", url, url)
		return nil
	case err != nil:
		return err
	}
	if f.Fingerprint == "" {
		e.printf("Broker %s: not pinned. A person must run `agent-bus pin --broker %s` in a terminal.\n", url, url)
		return nil
	}
	if f.Credential == "" {
		e.printf("Broker %s: pinned (%s), but no account yet. A person must run `agent-bus register --broker %s --name NAME` in a terminal.\n", url, f.Fingerprint, url)
		return nil
	}
	c, err := e.newClient(client.Config{BaseURL: url, Pin: f.Fingerprint, Credential: f.Credential})
	if err != nil {
		return err
	}
	st, err := c.AccountStatus(ctx)
	if err != nil {
		var ae *client.APIError
		if errors.As(err, &ae) && ae.Status == 401 {
			e.printf("Broker %s: the broker does not recognise the stored credential for %q (a retired account is forgotten after a week). A person must register again.\n", url, f.Account)
			return nil
		}
		return err
	}
	e.printf("Broker %s\n  account:    %s\n  status:     %s\n  hostmaster: %v\n", url, st.Name, st.Status, st.Hostmaster)
	switch st.Status {
	case protocol.AccountPending:
		e.printf("The hostmaster has not approved this account yet.\n")
	case protocol.AccountRejected, protocol.AccountExpired, protocol.AccountRemoved:
		e.printf("This account can no longer be used. A person must register again.\n")
	}
	if f.Hostmaster != st.Hostmaster || f.Account != st.Name {
		f.Hostmaster, f.Account = st.Hostmaster, st.Name
		f.URL = url
		if err := brokerfile.Save(e.ConfigDir, f); err != nil {
			e.printf("(could not refresh the local hostmaster hint: %v)\n", err)
		} else if st.Hostmaster {
			e.printf("Restart Claude Code or Hermes so the admin tools appear.\n")
		}
	}
	return nil
}
