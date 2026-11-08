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

	"github.com/Liz4v/agent-bus/internal/brokerfile"
	"github.com/Liz4v/agent-bus/internal/protocol"
)

// ErrUnauthorized means the broker no longer knows the bearer value it was
// sent (a session token, or for CreateSession and AccountStatus the credential).
var ErrUnauthorized = errors.New("agent-bus: session unknown to the broker (expired or reaped)")

// APIError is a broker error response. It maps the broker's error codes onto a
// typed value; callers switch on Code.
type APIError struct {
	Broker  string
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("broker %s: %s (%s)", e.Broker, e.Message, e.Code)
}

// Is lets errors.Is(err, ErrUnauthorized) match a 401 APIError too.
func (e *APIError) Is(target error) bool {
	return target == ErrUnauthorized && e.Status == http.StatusUnauthorized
}

// Config configures a client. BaseURL must be https and Pin is required; the
// Credential may be empty for calls that need none (Register).
type Config struct {
	BaseURL    string
	Pin        string // broker key fingerprint ("SHA256:...")
	Credential string // account credential
}

// Client talks to one broker over a pinned TLS connection. It is safe for
// concurrent use.
type Client struct {
	base string
	cred string
	http *http.Client
}

// New returns a client for the broker in cfg. It refuses a non-https URL and
// an empty pin (ErrNotPinned).
func New(cfg Config) (*Client, error) {
	base, err := brokerfile.NormalizeURL(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.Pin == "" {
		return nil, ErrNotPinned
	}
	return &Client{base: base, cred: cfg.Credential, http: &http.Client{Transport: pinnedTransport(cfg.Pin)}}, nil
}

// BaseURL returns the broker address.
func (c *Client) BaseURL() string { return c.base }

// CloseIdleConnections drops pooled connections.
func (c *Client) CloseIdleConnections() { c.http.CloseIdleConnections() }

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
		var pe *PinError
		if errors.As(err, &pe) {
			return pe
		}
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
			ae.Code, ae.Message = eb.Error.Code, eb.Error.Message
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

// CreateSession opens a session under the client's account credential and
// returns its token and the account name.
func (c *Client) CreateSession(ctx context.Context) (token, account string, err error) {
	var out protocol.CreateSessionResponse
	if err := c.do(ctx, c.cred, "POST", "/v1/sessions", struct{}{}, &out, callTimeout); err != nil {
		return "", "", err
	}
	return out.SessionToken, out.Account, nil
}

// DeleteSelf ends the session (clean exit).
func (c *Client) DeleteSelf(ctx context.Context, token string) error {
	return c.do(ctx, token, "DELETE", "/v1/sessions/self", nil, nil, callTimeout)
}

// ClaimName claims a broker-wide name for the session. It returns the name the
// session holds afterwards.
func (c *Client) ClaimName(ctx context.Context, token, name string) (protocol.ClaimNameResponse, error) {
	var out protocol.ClaimNameResponse
	err := c.do(ctx, token, "POST", "/v1/name", protocol.ClaimNameRequest{Name: name}, &out, callTimeout)
	return out, err
}

// ReleaseName releases the session's name. Released is "" if it held none.
func (c *Client) ReleaseName(ctx context.Context, token string) (protocol.ReleaseNameResponse, error) {
	var out protocol.ReleaseNameResponse
	err := c.do(ctx, token, "DELETE", "/v1/name", nil, &out, callTimeout)
	return out, err
}

// Send sends text to the session that holds the name `to`.
func (c *Client) Send(ctx context.Context, token, to, text string) error {
	return c.do(ctx, token, "POST", "/v1/messages", protocol.SendRequest{To: to, Text: text}, nil, callTimeout)
}

// Sync runs one sync. The HTTP timeout is the wait plus 10 s.
func (c *Client) Sync(ctx context.Context, token string, req protocol.SyncRequest) (protocol.SyncResponse, error) {
	var out protocol.SyncResponse
	timeout := time.Duration(req.Wait)*time.Second + 10*time.Second
	err := c.do(ctx, token, "POST", "/v1/sync", req, &out, timeout)
	return out, err
}
