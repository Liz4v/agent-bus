// Package protocol holds the wire types, error codes and shared constants used
// by the broker, the client, the shim, the wait command and the Stop hook.
package protocol

import (
	"regexp"
	"time"
)

// Shared constants (Architect D11).
const (
	MaxMessageBytes = 64 << 10 // decoded text limit
	MaxBodyBytes    = 1 << 20  // request body limit
	SyncPageBytes   = 1 << 20  // rows returned by one sync
	ReadPageBytes   = 256 << 10

	ChannelTTL      = time.Hour
	DisconnectAfter = 60 * time.Second
	DisconnectGrace = 5 * time.Minute
	SweepInterval   = 15 * time.Second

	MaxReadWait = 300 * time.Second
	SyncWait    = 20 * time.Second // shim long-poll length
	SyncWaitCap = 30 * time.Second // broker-side cap on any sync wait

	WaitRearmAfter = time.Hour
	WakeDebounce   = 2 * time.Second
)

// Error codes carried in ErrorBody.
const (
	CodeBadLabel      = "bad_label"
	CodeNoOpenChannel = "no_open_channel"
	CodeSameLabel     = "same_label"
	CodeDuplicatePeer = "duplicate_peer"
	CodeUnknownPeer   = "unknown_peer"
	CodePeerLeft      = "peer_left"
	CodeChannelExpire = "channel_expired"
	CodeTooLarge      = "too_large"
	CodeEmpty         = "empty"
	CodeUnauthorized  = "unauthorized"
	CodeBadRequest    = "bad_request"
	CodeInternal      = "internal"
)

// Message kinds.
const (
	KindText             = "text"
	KindPeerJoined       = "peer_joined"
	KindPeerDisconnected = "peer_disconnected"
	KindPeerReconnected  = "peer_reconnected"
	KindPeerLeft         = "peer_left"
	KindChannelExpired   = "channel_expired"
)

// IsWaking reports whether an unread row of this kind should wake an idle
// session (D6). Disconnect and reconnect events are informational only.
func IsWaking(kind string) bool {
	switch kind {
	case KindText, KindPeerJoined, KindPeerLeft, KindChannelExpired:
		return true
	}
	return false
}

// Peer states shown to a member (D10).
const (
	StateWaiting          = "waiting"
	StateConnected        = "connected"
	StatePeerDisconnected = "peer_disconnected"
	StatePeerLeft         = "peer_left"
	StateExpired          = "expired"
)

// LabelPattern is the pattern every label must match (API notes).
const LabelPattern = `^[A-Za-z0-9._-]{1,253}/[A-Za-z0-9._-]{1,64}$`

var labelRE = regexp.MustCompile(LabelPattern)

// ValidLabel reports whether s is a well-formed label.
func ValidLabel(s string) bool { return labelRE.MatchString(s) }

// SystemSender is the sender shown for rows that carry no label.
const SystemSender = "agent-bus"

// ErrorBody is the JSON error shape: {"error":{...}}.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes one error.
type ErrorDetail struct {
	Code       string      `json:"code"`
	Message    string      `json:"message"`
	KnownPeers []KnownPeer `json:"known_peers,omitempty"`
}

// KnownPeer is one entry of the known_peers list in an unknown_peer error.
type KnownPeer struct {
	Handle string `json:"handle"`
	Label  string `json:"label,omitempty"`
	State  string `json:"state"`
}

// POST /v1/sessions
type CreateSessionRequest struct{}
type CreateSessionResponse struct {
	SessionToken string `json:"session_token"`
}

// POST /v1/channels
type CreateChannelRequest struct {
	Label string `json:"label"`
}
type CreateChannelResponse struct {
	ChannelID string `json:"channel_id"`
	Handle    string `json:"handle"`
}

// POST /v1/channels/join
type JoinChannelRequest struct {
	ChannelID string `json:"channel_id"`
	Label     string `json:"label"`
}
type JoinChannelResponse struct {
	Handle    string `json:"handle"`
	PeerLabel string `json:"peer_label"`
}

// POST /v1/messages
type SendRequest struct {
	To   string `json:"to"`
	Text string `json:"text"`
}
type SendResponse struct{}

// POST /v1/leave
type LeaveRequest struct {
	To string `json:"to"`
}

// POST /v1/sync. Wait is in seconds.
type SyncRequest struct {
	Ack     int64 `json:"ack"`
	After   int64 `json:"after"`
	Version int64 `json:"version"`
	Wait    int   `json:"wait"`
}

// Message is one inbox row. At is Unix seconds.
type Message struct {
	ID        int64   `json:"id"`
	Handle    string  `json:"handle"`
	FromLabel *string `json:"from_label"`
	Kind      string  `json:"kind"`
	Text      string  `json:"text"`
	At        int64   `json:"at"`
}

// Channel is one entry of a session's channel list. CreatedAt is Unix seconds.
type Channel struct {
	Handle    string  `json:"handle"`
	Label     string  `json:"label"`
	PeerLabel *string `json:"peer_label"`
	State     string  `json:"state"`
	CreatedAt int64   `json:"created_at"`
}

type SyncResponse struct {
	Version  int64     `json:"version"`
	Messages []Message `json:"messages"`
	More     bool      `json:"more"`
	Channels []Channel `json:"channels"`
}

// Wake socket protocol (D7). The client sends "WAIT\n" or "STATUS\n" and the
// shim answers with one JSON line.
const (
	WakeCmdWait   = "WAIT"
	WakeCmdStatus = "STATUS"
)

// WAIT reply statuses.
const (
	WaitPending    = "pending"
	WaitNoChannels = "no_channels"
	WaitRearm      = "rearm"
)

type WaitReply struct {
	Status string   `json:"status"`
	From   []string `json:"from,omitempty"`
	Count  int      `json:"count,omitempty"`
	Reason string   `json:"reason,omitempty"`
}

type StatusReply struct {
	Count        int      `json:"count"`
	From         []string `json:"from"`
	LiveChannels int      `json:"live_channels"`
	Armed        bool     `json:"armed"`
	Error        *string  `json:"error"`
}
