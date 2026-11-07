package broker

import (
	"regexp"
	"strings"
	"testing"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

var claimFormat = regexp.MustCompile(`^[A-Z2-7]{5}(-[A-Z2-7]{5}){3}$`)

func TestClaimCodeFormatAndNormalisation(t *testing.T) {
	code := newClaimCode()
	if !claimFormat.MatchString(code) {
		t.Fatalf("bad format %q", code)
	}
	if code == newClaimCode() {
		t.Fatal("two codes were identical")
	}
	raw := strings.ReplaceAll(code, "-", "")
	for _, in := range []string{code, strings.ToLower(code), raw, strings.ReplaceAll(code, "-", " "), " " + code + " "} {
		if got := normalizeClaimCode(in); got != raw {
			t.Errorf("normalizeClaimCode(%q) = %q, want %q", in, got, raw)
		}
	}
}

func TestClaimSingleUse(t *testing.T) {
	c := &claims{}
	code := c.issue(protocol.ClaimModeClaim)
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.checkLocked(strings.ToLower(code)); !ok || m != protocol.ClaimModeClaim {
		t.Fatalf("check = %q, %v", m, ok)
	}
	c.consumeLocked()
	if _, ok := c.checkLocked(code); ok {
		t.Fatal("a consumed code still works")
	}
}

func TestClaimReissueInvalidatesPrevious(t *testing.T) {
	c := &claims{}
	first := c.issue(protocol.ClaimModeClaim)
	second := c.issue(protocol.ClaimModeRecover)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.checkLocked(first); ok {
		t.Fatal("the previous code survived a re-issue")
	}
	if m, ok := c.checkLocked(second); !ok || m != protocol.ClaimModeRecover {
		t.Fatalf("new code: %q %v", m, ok)
	}
}

func TestClaimWrongAndMissingIndistinguishable(t *testing.T) {
	// With no code live, and with a code live but a wrong one given, the
	// answer is the same, and so is the broker's error through Register.
	c := &claims{}
	c.mu.Lock()
	_, okNone := c.checkLocked("AAAAA-AAAAA-AAAAA-AAAAA")
	c.mu.Unlock()
	c.issue(protocol.ClaimModeClaim)
	c.mu.Lock()
	_, okWrong := c.checkLocked("AAAAA-AAAAA-AAAAA-AAAAA")
	c.mu.Unlock()
	if okNone || okWrong {
		t.Fatal("a wrong code was accepted")
	}

	s, _ := openTestStore(t)
	_, err1 := s.Register(bg, "alice", "")
	_, err2 := s.Register(bg, "alice", "AAAAA-AAAAA-AAAAA-AAAAA")
	if _, _, err := s.IssueClaim(bg); err != nil {
		t.Fatal(err)
	}
	_, err3 := s.Register(bg, "alice", "AAAAA-AAAAA-AAAAA-AAAAA")
	for _, err := range []error{err1, err2, err3} {
		if err != ErrRegistrationRefused {
			t.Fatalf("got %v, want the generic refusal", err)
		}
	}
}

func TestIssueClaimModeFollowsHostmaster(t *testing.T) {
	s, _ := openTestStore(t)
	_, mode, err := s.IssueClaim(bg)
	if err != nil || mode != protocol.ClaimModeClaim {
		t.Fatalf("unclaimed: %q %v", mode, err)
	}
	addAccount(t, s, "boss", protocol.AccountActive, true)
	_, mode, err = s.IssueClaim(bg)
	if err != nil || mode != protocol.ClaimModeRecover {
		t.Fatalf("claimed: %q %v", mode, err)
	}
}
