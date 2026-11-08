package protocol

import (
	"regexp"
	"strings"
	"time"
)

// Account limits and retention (multi-user M1, M7).
const (
	DefaultMaxAccounts = 5
	MaxAccountsCeiling = 1000
	PendingLimit       = 5
	PendingExpiry      = 2 * time.Hour
	RetiredKeep        = 7 * 24 * time.Hour
)

// Account statuses. "active" and "pending" are live rows; the others are
// outcomes reported from the retired table.
const (
	AccountActive   = "active"
	AccountPending  = "pending"
	AccountRejected = "rejected"
	AccountExpired  = "expired"
	AccountRemoved  = "removed"
)

// Account-related error codes.
const (
	// CodeRegistrationRefused is the only refusal POST /v1/register returns
	// (M7): it never says why.
	CodeRegistrationRefused = "registration_refused"
	// CodeAccountLimit is returned by approve only.
	CodeAccountLimit    = "account_limit"
	CodeAccountPending  = "account_pending"
	CodeAccountRejected = "account_rejected"
	CodeAccountExpired  = "account_expired"
	CodeAccountRemoved  = "account_removed"
	CodeForbidden       = "forbidden"
	CodeWrongState      = "wrong_state"
	CodeNoSuchAccount   = "no_such_account"
)

// NamePattern is the pattern account names and session names must match.
const NamePattern = `^[A-Za-z0-9._-]{1,64}$`

var nameRE = regexp.MustCompile(NamePattern)

// ValidName reports whether s is a well-formed account name or session name
// (the name a session claims and peers address it by).
func ValidName(s string) bool { return nameRE.MatchString(s) }

// NameKey is the case-folded form of a name. Session names are unique by key,
// so Bob and bob are the same name. The charset is ASCII, so ToLower is exact.
func NameKey(s string) string { return strings.ToLower(s) }

// ReservedName reports whether a session may not claim s: the system sender
// and the admin inbox, in any case.
func ReservedName(s string) bool {
	switch NameKey(s) {
	case SystemSender, "admin":
		return true
	}
	return false
}

// POST /v1/register. ClaimCode is set only by the human claiming or
// recovering the hostmaster account.
type RegisterRequest struct {
	Name      string `json:"name"`
	ClaimCode string `json:"claim_code,omitempty"`
}
type RegisterResponse struct {
	Credential string `json:"credential"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Hostmaster bool   `json:"hostmaster"`
}

// GET /v1/account (credential bearer).
type AccountResponse struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Hostmaster bool   `json:"hostmaster"`
}

// AccountInfo is one entry of GET /v1/admin/accounts. Times are Unix seconds.
type AccountInfo struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Hostmaster bool   `json:"hostmaster"`
	CreatedAt  int64  `json:"created_at"`
	ExpiresAt  *int64 `json:"expires_at,omitempty"`
}

// GET /v1/admin/accounts.
type ListAccountsResponse struct {
	Accounts     []AccountInfo `json:"accounts"`
	MaxAccounts  int           `json:"max_accounts"`
	Active       int           `json:"active"`
	Pending      int           `json:"pending"`
	PendingLimit int           `json:"pending_limit"`
}

// POST /v1/admin/approve, /reject and /remove name exactly one account.
type AdminNameRequest struct {
	Name string `json:"name"`
}
type ApproveResponse struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// POST /v1/admin/max-accounts.
type MaxAccountsRequest struct {
	Max int `json:"max"`
}
type MaxAccountsResponse struct {
	MaxAccounts int `json:"max_accounts"`
	Active      int `json:"active"`
}

// POST /v1/admin/subscribe.
type SubscribeRequest struct {
	Subscribed bool `json:"subscribed"`
}
type SubscribeResponse struct {
	Subscribed bool `json:"subscribed"`
	Pending    int  `json:"pending"`
}

// Control socket (host-side). One command line in, one JSON line out.
const (
	ControlCmdRecover     = "RECOVER"
	ControlCmdFingerprint = "FINGERPRINT"

	ClaimModeClaim   = "claim"
	ClaimModeRecover = "recover"
)

type ControlRecoverReply struct {
	ClaimCode string `json:"claim_code"`
	Mode      string `json:"mode"`
}
type ControlFingerprintReply struct {
	Fingerprint string `json:"fingerprint"`
}
type ControlError struct {
	Error string `json:"error"`
}
