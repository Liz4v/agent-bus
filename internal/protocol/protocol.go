// Package protocol holds the wire types, error codes and shared constants used
// by the broker, the client, the shim, the wait command and the Stop hook.
package protocol

import (
	"time"
)

// Shared constants (Architect D11).
const (
	MaxMessageBytes = 64 << 10 // decoded text limit
	MaxBodyBytes    = 1 << 20  // request body limit
	SyncPageBytes   = 1 << 20  // rows returned by one sync
	ReadPageBytes   = 256 << 10

	DisconnectAfter = 60 * time.Second
	DisconnectGrace = 5 * time.Minute
	SweepInterval   = 15 * time.Second

	MaxReadWait = 300 * time.Second
	SyncWait    = 20 * time.Second // shim long-poll length
	SyncWaitCap = 30 * time.Second // broker-side cap on any sync wait

	WakeDebounce = 2 * time.Second
)

// Error codes carried in ErrorBody.
const (
	CodeBadName         = "bad_name"
	CodeNameTaken       = "name_taken"
	CodeNameHeld        = "name_held"
	CodeNoName          = "no_name"
	CodeSelfSend        = "self_send"
	CodeUpgradeRequired = "upgrade_required"
	CodeUnknownPeer     = "unknown_peer"
	CodePeerLeft        = "peer_left"
	CodePeerRemoved     = "peer_removed"
	CodeTooLarge        = "too_large"
	CodeEmpty           = "empty"
	CodeUnauthorized    = "unauthorized"
	CodeBadRequest      = "bad_request"
	CodeInternal        = "internal"
)

// Message kinds.
const (
	KindText             = "text"
	KindPeerDisconnected = "peer_disconnected"
	KindPeerReconnected  = "peer_reconnected"
	KindPeerLeft         = "peer_left"
	KindPeerRemoved      = "peer_removed"
	// KindRegistrationPending is an admin event for a subscribed hostmaster
	// session; its text is the registrant's name.
	KindRegistrationPending = "registration_pending"
)

// IsWaking reports whether an unread row of this kind should wake an idle
// session (D6). Disconnect and reconnect events are informational only.
func IsWaking(kind string) bool {
	switch kind {
	case KindText, KindPeerLeft, KindPeerRemoved, KindRegistrationPending:
		return true
	}
	return false
}

// SystemSender is the sender shown for rows that carry no name.
const SystemSender = "agent-bus"

// ErrorBody is the JSON error shape: {"error":{...}}.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes one error.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// POST /v1/sessions
type CreateSessionRequest struct{}
type CreateSessionResponse struct {
	SessionToken string `json:"session_token"`
	Account      string `json:"account"`
}

// POST /v1/name
type ClaimNameRequest struct {
	Name string `json:"name"`
}
type ClaimNameResponse struct {
	Name string `json:"name"`
}

// DELETE /v1/name. Released is the name that was held, or "" if none was.
type ReleaseNameResponse struct {
	Released string `json:"released"`
}

// POST /v1/messages
type SendRequest struct {
	To   string `json:"to"`
	Text string `json:"text"`
}
type SendResponse struct{}

// POST /v1/sync. Wait is in seconds.
type SyncRequest struct {
	Ack     int64 `json:"ack"`
	After   int64 `json:"after"`
	Version int64 `json:"version"`
	Wait    int   `json:"wait"`
}

// Message is one inbox row. At is Unix seconds.
type Message struct {
	ID int64 `json:"id"`
	// From is the sender's name (nil for rows with no sender).
	From *string `json:"from"`
	// FromAccount is the verified account of the sender (nil for admin rows).
	FromAccount *string `json:"from_account,omitempty"`
	Kind        string  `json:"kind"`
	Text        string  `json:"text"`
	At          int64   `json:"at"`
	// FirstContact is set on the first text row a session receives from a
	// given sender session.
	FirstContact bool `json:"first_contact,omitempty"`
}

type SyncResponse struct {
	Version  int64     `json:"version"`
	Messages []Message `json:"messages"`
	More     bool      `json:"more"`
	// Name is the name the session holds, or "" when it holds none.
	Name string `json:"name"`
	// AdminSubscribed reports the session's admin notification subscription.
	AdminSubscribed bool `json:"admin_subscribed,omitempty"`
}

// Wake socket protocol (D7). The client sends "WAIT\n" or "STATUS\n" and the
// shim answers with one JSON line.
const (
	WakeCmdWait   = "WAIT"
	WakeCmdStatus = "STATUS"
)

// WAIT reply statuses.
const (
	WaitPending = "pending"
	WaitNoName  = "no_name"
	WaitRearm   = "rearm"
)

type WaitReply struct {
	Status string   `json:"status"`
	From   []string `json:"from,omitempty"`
	Count  int      `json:"count,omitempty"`
	Reason string   `json:"reason,omitempty"`
}

type StatusReply struct {
	Count      int      `json:"count"`
	From       []string `json:"from"`
	Named      bool     `json:"named"`
	Armed      bool     `json:"armed"`
	Error      *string  `json:"error"`
	Cursor     int64    `json:"cursor"` // read cursor; changes after a read (additive)
	Subscribed bool     `json:"subscribed"`
}
