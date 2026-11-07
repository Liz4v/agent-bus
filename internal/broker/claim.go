package broker

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base32"
	"strings"
	"sync"

	"github.com/Liz4v/agent-bus/internal/protocol"
	"github.com/Liz4v/agent-bus/internal/secret"
)

// claims holds the single live claim code in memory (M4). Only its hash is
// kept. mu is held by Register across the store transaction so a code is
// consumed only if the insert commits, and two simultaneous claims cannot
// both win.
type claims struct {
	mu   sync.Mutex
	hash []byte
	mode string
}

var claimEnc = base32.StdEncoding.WithPadding(base32.NoPadding)

// newClaimCode returns a fresh code: 20 base32 characters (100 bits) in four
// dash-separated groups.
func newClaimCode() string {
	var b [13]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("agent-bus: crypto/rand failed: " + err.Error())
	}
	raw := claimEnc.EncodeToString(b[:])[:20]
	return raw[0:5] + "-" + raw[5:10] + "-" + raw[10:15] + "-" + raw[15:20]
}

// normalizeClaimCode uppercases and removes dashes and spaces.
func normalizeClaimCode(code string) string {
	code = strings.ToUpper(code)
	code = strings.ReplaceAll(code, "-", "")
	return strings.ReplaceAll(code, " ", "")
}

// issue replaces the live code with a new one of the given mode. It takes mu
// itself, so it must not be called while the caller holds it.
func (c *claims) issue(mode string) string {
	code := newClaimCode()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hash = secret.Hash(normalizeClaimCode(code))
	c.mode = mode
	return code
}

// checkLocked reports the mode of the live code if code matches it. The
// caller holds mu. The comparison is constant-time on the hashes.
func (c *claims) checkLocked(code string) (string, bool) {
	if c.hash == nil {
		// Still do a comparison so the work does not depend on state.
		subtle.ConstantTimeCompare(secret.Hash(normalizeClaimCode(code)), make([]byte, 32))
		return "", false
	}
	if subtle.ConstantTimeCompare(secret.Hash(normalizeClaimCode(code)), c.hash) != 1 {
		return "", false
	}
	return c.mode, true
}

// consumeLocked invalidates the live code. The caller holds mu.
func (c *claims) consumeLocked() { c.hash, c.mode = nil, "" }

// HasHostmaster reports whether a hostmaster account exists.
func (s *Store) HasHostmaster(ctx context.Context) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM accounts WHERE is_hostmaster = 1").Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// IssueClaim issues a fresh claim code, invalidating any earlier one. The mode
// is "claim" when no hostmaster exists and "recover" when one does (M4). It is
// the single entry point for Run, the control socket and tests. The code is
// never logged here.
func (s *Store) IssueClaim(ctx context.Context) (code, mode string, err error) {
	has, err := s.HasHostmaster(ctx)
	if err != nil {
		return "", "", err
	}
	mode = protocol.ClaimModeClaim
	if has {
		mode = protocol.ClaimModeRecover
	}
	return s.claims.issue(mode), mode, nil
}
