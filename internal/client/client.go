// Package client is the typed HTTP client for the broker API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Liz4v/agent-bus/internal/protocol"
)

// ErrUnauthorized means the broker no longer knows the session token.
var ErrUnauthorized = errors.New("agent-bus: session unknown to the broker (expired or reaped)")

// APIError is a broker error response. It maps the broker's error codes onto a
// typed value; callers switch on Code.
type APIError struct {
	Broker     string
	Status     int
	Code       string
	Message    string
	KnownPeers []protocol.KnownPeer
}

func (e *APIError) Error() string {
	return fmt.Sprintf("broker %s: %s (%s)", e.Broker, e.Message, e.Code)
}

// Is lets errors.Is(err, ErrUnauthorized) match a 401 APIError too.
func (e *APIError) Is(target error) bool {
	return target == ErrUnauthorized && e.Status == http.StatusUnauthorized
}

// Client talks to one broker. It is safe for concurrent use.
type Client struct {
	base string
	http *http.Client
}

// New returns a client for the broker at baseURL (for example http://broker-host:7700).
func New(baseURL string) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), http: &http.Client{}}
}

// BaseURL returns the broker address.
func (c *Client) BaseURL() string { return c.base }

func (c *Client) do(ctx context.Context, token, method, path string, in, out any, timeout time.Duration) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return fmt.Errorf("broker %s: %w", c.base, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("broker %s: %w", c.base, ctx.Err())
		}
		return fmt.Errorf("broker %s unreachable: %w", c.base, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("broker %s: reading response: %w", c.base, err)
	}
	if resp.StatusCode >= 300 {
		var eb protocol.ErrorBody
		ae := &APIError{Broker: c.base, Status: resp.StatusCode}
		if json.Unmarshal(data, &eb) == nil && eb.Error.Code != "" {
			ae.Code, ae.Message, ae.KnownPeers = eb.Error.Code, eb.Error.Message, eb.Error.KnownPeers
		} else {
			ae.Code, ae.Message = "http_"+fmt.Sprint(resp.StatusCode), strings.TrimSpace(string(data))
			if ae.Message == "" {
				ae.Message = resp.Status
			}
		}
		return ae
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("broker %s: bad response: %w", c.base, err)
		}
	}
	return nil
}

const callTimeout = 15 * time.Second

// CreateSession registers a session and returns its token.
func (c *Client) CreateSession(ctx context.Context) (string, error) {
	var out protocol.CreateSessionResponse
	if err := c.do(ctx, "", "POST", "/v1/sessions", struct{}{}, &out, callTimeout); err != nil {
		return "", err
	}
	return out.SessionToken, nil
}

// DeleteSelf ends the session (clean exit).
func (c *Client) DeleteSelf(ctx context.Context, token string) error {
	return c.do(ctx, token, "DELETE", "/v1/sessions/self", nil, nil, callTimeout)
}

// CreateChannel creates a channel under label.
func (c *Client) CreateChannel(ctx context.Context, token, label string) (protocol.CreateChannelResponse, error) {
	var out protocol.CreateChannelResponse
	err := c.do(ctx, token, "POST", "/v1/channels", protocol.CreateChannelRequest{Label: label}, &out, callTimeout)
	return out, err
}

// JoinChannel joins the channel with the given secret ID under label.
func (c *Client) JoinChannel(ctx context.Context, token, channelID, label string) (protocol.JoinChannelResponse, error) {
	var out protocol.JoinChannelResponse
	err := c.do(ctx, token, "POST", "/v1/channels/join",
		protocol.JoinChannelRequest{ChannelID: channelID, Label: label}, &out, callTimeout)
	return out, err
}

// Send sends text to a peer label or handle.
func (c *Client) Send(ctx context.Context, token, to, text string) error {
	return c.do(ctx, token, "POST", "/v1/messages", protocol.SendRequest{To: to, Text: text}, nil, callTimeout)
}

// Leave ends the membership named by a peer label or handle.
func (c *Client) Leave(ctx context.Context, token, to string) error {
	return c.do(ctx, token, "POST", "/v1/leave", protocol.LeaveRequest{To: to}, nil, callTimeout)
}

// Sync runs one sync. The HTTP timeout is the wait plus 10 s.
func (c *Client) Sync(ctx context.Context, token string, req protocol.SyncRequest) (protocol.SyncResponse, error) {
	var out protocol.SyncResponse
	timeout := time.Duration(req.Wait)*time.Second + 10*time.Second
	err := c.do(ctx, token, "POST", "/v1/sync", req, &out, timeout)
	return out, err
}
