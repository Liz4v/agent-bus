package shim

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Liz4v/agent-bus/internal/client"
)

// DropReport is what the mirror tells the session when it is emptied by a drop.
type DropReport struct {
	// HadState is true if the mirror held channels or unread rows.
	HadState bool
	// UnreadCount and UnreadLabels describe the unread waking rows discarded
	// (counts and sender labels only, never content).
	UnreadCount  int
	UnreadLabels []string
}

// Session holds the broker session token (D2). The token never leaves the shim.
type Session struct {
	c *client.Client

	reg sync.Mutex // serialises registration and dropping

	mu     sync.Mutex // guards token and notice
	token  string
	notice string

	onRegister func()
	onDrop     func() DropReport
	onDropped  func()
}

// NewSession returns an unregistered session.
func NewSession(c *client.Client) *Session { return &Session{c: c} }

// SetCallbacks installs the mirror hooks. It must be called before use.
// onRegister runs after a successful registration; onDrop must stop the sync
// loop, empty the mirror and clear its health, and report what it discarded.
// onDropped runs last, after the one-time notice is set, so anything it wakes
// can already see the notice. No callback may call Ensure or Drop.
func (s *Session) SetCallbacks(onRegister func(), onDrop func() DropReport, onDropped func()) {
	s.onRegister, s.onDrop, s.onDropped = onRegister, onDrop, onDropped
}

// Client returns the broker client.
func (s *Session) Client() *client.Client { return s.c }

// Token returns the current token, or "" when unregistered.
func (s *Session) Token() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

// Ensure returns the current token, registering a new session if there is
// none. Registration happens under a mutex, so concurrent callers register
// once. The mirror loop starts (onRegister) before Ensure returns.
func (s *Session) Ensure(ctx context.Context) (string, error) {
	s.reg.Lock()
	defer s.reg.Unlock()
	if t := s.Token(); t != "" {
		return t, nil
	}
	tok, err := s.c.CreateSession(ctx)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.token = tok
	s.mu.Unlock()
	if s.onRegister != nil {
		s.onRegister()
	}
	return tok, nil
}

// Drop is compare-and-clear: it clears the session only if token is still the
// current one. It reports whether it did so.
func (s *Session) Drop(token string) bool {
	s.reg.Lock()
	defer s.reg.Unlock()
	s.mu.Lock()
	if token == "" || s.token != token {
		s.mu.Unlock()
		return false
	}
	s.token = ""
	s.mu.Unlock()
	var rep DropReport
	if s.onDrop != nil {
		rep = s.onDrop()
	}
	if rep.HadState {
		s.mu.Lock()
		s.notice = dropNotice(rep)
		s.mu.Unlock()
	}
	if s.onDropped != nil {
		s.onDropped()
	}
	return true
}

func dropNotice(rep DropReport) string {
	msg := "Your agent-bus memberships expired while this host was disconnected; ask the human for a new channel ID."
	if rep.UnreadCount > 0 {
		labels := dedupe(rep.UnreadLabels)
		msg += fmt.Sprintf(" %d unread message(s)/event(s) from %s were discarded.", rep.UnreadCount, strings.Join(labels, ", "))
	}
	return msg
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// TakeNotice returns the pending one-time notice and clears it.
func (s *Session) TakeNotice() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.notice
	s.notice = ""
	return n
}

// PeekNotice returns the pending notice without clearing it.
func (s *Session) PeekNotice() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notice
}

// Close is the clean exit: DELETE /v1/sessions/self with a 2 s timeout, errors
// ignored. With no token it makes no broker call.
func (s *Session) Close(ctx context.Context) {
	s.reg.Lock()
	defer s.reg.Unlock()
	s.mu.Lock()
	tok := s.token
	s.token = ""
	s.mu.Unlock()
	if tok == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = s.c.DeleteSelf(ctx, tok)
}
