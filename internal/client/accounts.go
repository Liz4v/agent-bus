package client

import (
	"context"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// Register asks the broker for an account called name. claimCode is set only
// when claiming or recovering the hostmaster account. It needs no
// authentication; a refusal is an *APIError with code registration_refused.
func (c *Client) Register(ctx context.Context, name, claimCode string) (protocol.RegisterResponse, error) {
	var out protocol.RegisterResponse
	err := c.do(ctx, "", "POST", "/v1/register", protocol.RegisterRequest{Name: name, ClaimCode: claimCode}, &out, callTimeout)
	return out, err
}

// AccountStatus reports the state of the client's own account, authenticated
// by its credential.
func (c *Client) AccountStatus(ctx context.Context) (protocol.AccountResponse, error) {
	var out protocol.AccountResponse
	err := c.do(ctx, c.cred, "GET", "/v1/account", nil, &out, callTimeout)
	return out, err
}

// ListAccounts lists the live accounts and limits (hostmaster session token).
func (c *Client) ListAccounts(ctx context.Context, token string) (protocol.ListAccountsResponse, error) {
	var out protocol.ListAccountsResponse
	err := c.do(ctx, token, "GET", "/v1/admin/accounts", nil, &out, callTimeout)
	return out, err
}

// Approve activates the pending account called name.
func (c *Client) Approve(ctx context.Context, token, name string) (protocol.ApproveResponse, error) {
	var out protocol.ApproveResponse
	err := c.do(ctx, token, "POST", "/v1/admin/approve", protocol.AdminNameRequest{Name: name}, &out, callTimeout)
	return out, err
}

// Reject rejects the pending account called name.
func (c *Client) Reject(ctx context.Context, token, name string) error {
	return c.do(ctx, token, "POST", "/v1/admin/reject", protocol.AdminNameRequest{Name: name}, nil, callTimeout)
}

// Remove removes the active account called name and ends its sessions.
func (c *Client) Remove(ctx context.Context, token, name string) error {
	return c.do(ctx, token, "POST", "/v1/admin/remove", protocol.AdminNameRequest{Name: name}, nil, callTimeout)
}

// SetMaxAccounts sets the active-account cap.
func (c *Client) SetMaxAccounts(ctx context.Context, token string, max int) (protocol.MaxAccountsResponse, error) {
	var out protocol.MaxAccountsResponse
	err := c.do(ctx, token, "POST", "/v1/admin/max-accounts", protocol.MaxAccountsRequest{Max: max}, &out, callTimeout)
	return out, err
}

// SetAdminSubscription turns registration notifications for this session on
// or off and returns the current pending count.
func (c *Client) SetAdminSubscription(ctx context.Context, token string, on bool) (protocol.SubscribeResponse, error) {
	var out protocol.SubscribeResponse
	err := c.do(ctx, token, "POST", "/v1/admin/subscribe", protocol.SubscribeRequest{Subscribed: on}, &out, callTimeout)
	return out, err
}
