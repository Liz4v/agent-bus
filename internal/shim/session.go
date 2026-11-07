package shim

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Liz4v/agent-bus/internal/client"
	"github.com/Liz4v/agent-bus/internal/protocol"
)

// accountCheckTimeout bounds the GET /v1/account made after a 401.
const accountCheckTimeout = 5 * time.Second

// AccountError is a problem with the account behind the credential, rendered
// as text for the agent that tells it to hand the problem to the human.
type AccountError struct {
	// Code is the broker's error code (account_pending, ...), or
	// "unknown_credential" for a 401 from POST /v1/sessions.
	Code string
}

// CodeUnknownCredential is AccountError.Code for a credential the broker does
// not recognise.
const CodeUnknownCredential = "unknown_credential"

func (e *AccountError) Error() string {
	switch e.Code {
	case protocol.CodeAccountPending:
		return "Your agent-bus account is waiting for the hostmaster's approval. Tell the user; do not retry in a loop. They can check with `agent-bus status`. Once approved, the tools work again without a restart."
	case protocol.CodeAccountRejected:
		return "The hostmaster rejected your agent-bus account. Tell the user. A person can register again with `agent-bus register` and then restart this session; do not run it yourself."
	case protocol.CodeAccountExpired:
		return "Your agent-bus registration expired before the hostmaster approved it. Tell the user. A person can register again with `agent-bus register` and then restart this session; do not run it yourself."
	case protocol.CodeAccountRemoved:
		return "Your agent-bus account was removed by the hostmaster. Tell the user; do not investigate or retry. A person can register again with `agent-bus register` and then restart this session; do not run it yourself."
	default:
		return "The broker does not recognise this account's credential (a rejected, expired or removed account is forgotten after a week). Tell the user to run `agent-bus status`, and to register again with `agent-bus register` and restart this session if needed; do not run it yourself."
	}
}

// accountGoneNotice is the one-time notice for a session dropped because its
// account is no longer active.
func accountGoneNotice(status string) string {
	switch status {
	case protocol.AccountRejected:
		return "Your agent-bus account was rejected by the hostmaster, so your channels are gone."
	case protocol.AccountExpired:
		return "Your agent-bus registration expired, so your channels are gone."
	case protocol.AccountRemoved:
		return "Your agent-bus account was removed by the hostmaster, so your channels are gone. Tell the user; do not investigate."
	default:
		return "Your agent-bus account is no longer active (" + status + "), so your channels are gone. Tell the user."
	}
}

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

	// wantAdmin records that the agent subscribed to admin notifications; the
	// keeper (admin.go) restores the subscription after a drop (M12).
	wantAdmin  atomic.Bool
	keeperHook func() // runs after a drop when wantAdmin is set; must not call Ensure
	closeHook  func() // runs first in Close; stops the keeper
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

// Client returns the broker client (nil when the shim is not set up).
func (s *Session) Client() *client.Client { return s.c }

// WantAdmin reports whether the agent asked for admin notifications.
func (s *Session) WantAdmin() bool { return s.wantAdmin.Load() }

// SetWantAdmin records the agent's local subscription intent.
func (s *Session) SetWantAdmin(v bool) { s.wantAdmin.Store(v) }

// SetKeeperHooks installs the keeper hook, called after a drop while wantAdmin
// is set (Drop holds the registration mutex, so it must only signal), and the
// close hook, called first in Close.
func (s *Session) SetKeeperHooks(afterDrop, onClose func()) {
	s.keeperHook, s.closeHook = afterDrop, onClose
}

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
	if s.c == nil {
		return "", errors.New("agent-bus is not set up: no broker client")
	}
	tok, _, err := s.c.CreateSession(ctx)
	if err != nil {
		var ae *client.APIError
		if errors.As(err, &ae) {
			switch {
			case ae.Status == 403 && isAccountCode(ae.Code):
				return "", &AccountError{Code: ae.Code}
			case ae.Status == 401:
				// A credential 401 must never look like a session 401.
				return "", &AccountError{Code: CodeUnknownCredential}
			}
		}
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

func isAccountCode(code string) bool {
	switch code {
	case protocol.CodeAccountPending, protocol.CodeAccountRejected, protocol.CodeAccountExpired, protocol.CodeAccountRemoved:
		return true
	}
	return false
}

// Drop is compare-and-clear: it clears the session only if token is still the
// current one. It reports whether it did so.
func (s *Session) Drop(token string) bool { return s.drop(token, "") }

// DropWithNotice is Drop with a specific one-time notice. The notice is
// appended to any pending one, never written over it.
func (s *Session) DropWithNotice(token, notice string) bool { return s.drop(token, notice) }

// DropAfter401 is the only way to drop a session after a 401. It asks the
// broker whether the account is still active: if not, the one-time notice says
// so instead of "expired while disconnected" (M16). Callers must not hold
// the registration mutex.
func (s *Session) DropAfter401(ctx context.Context, token string) bool {
	if token == "" || s.Token() != token || s.c == nil {
		return s.Drop(token)
	}
	cctx, cancel := context.WithTimeout(ctx, accountCheckTimeout)
	defer cancel()
	st, err := s.c.AccountStatus(cctx)
	var ae *client.APIError
	switch {
	case err == nil && st.Status != protocol.AccountActive:
		return s.DropWithNotice(token, accountGoneNotice(st.Status))
	case errors.As(err, &ae) && ae.Status == 401:
		return s.DropWithNotice(token, "The broker no longer recognises this account's credential, so your channels are gone. Tell the user; a person must run `agent-bus status`.")
	}
	return s.Drop(token)
}

func (s *Session) drop(token, extra string) bool {
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
	switch {
	case extra != "":
		if rep.UnreadCount > 0 {
			extra += fmt.Sprintf(" %d unread message(s)/event(s) from %s were discarded.", rep.UnreadCount, strings.Join(dedupe(rep.UnreadLabels), ", "))
		}
		s.AppendNotice(extra)
	case rep.HadState:
		s.AppendNotice(dropNotice(rep))
	}
	if s.onDropped != nil {
		s.onDropped()
	}
	if s.wantAdmin.Load() && s.keeperHook != nil {
		s.keeperHook()
	}
	return true
}

// AppendNotice adds text to the pending one-time notice.
func (s *Session) AppendNotice(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notice != "" {
		s.notice += "\n\n"
	}
	s.notice += text
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
	if s.closeHook != nil {
		s.closeHook() // stop the keeper before the session goes away
	}
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
