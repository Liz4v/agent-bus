package hermeswake

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Liz4v/agent-bus/internal/wait"
)

// EventType is the webhook payload's event_type; the Hermes route filters on it.
const EventType = "agent_bus_wake"

const postTimeout = 10 * time.Second

// payload is the whole body: a count and sender labels, never content.
type payload struct {
	EventType string   `json:"event_type"`
	Count     int      `json:"count"`
	From      []string `json:"from"`
}

// StatusError is a POST that Hermes did not accept with 202. It carries the
// status code and, for a 200, the JSON "status" word, and nothing else.
type StatusError struct {
	Code int
	Word string
}

func (e *StatusError) Error() string {
	if e.Word != "" {
		return fmt.Sprintf("hermes webhook answered HTTP %d (status %q), not 202", e.Code, e.Word)
	}
	return fmt.Sprintf("hermes webhook answered HTTP %d, not 202", e.Code)
}

// Poster sends signed wake notifications.
type Poster struct {
	URL    string
	Secret string
	Client *http.Client     // nil means a client with a 10 s timeout
	Now    func() time.Time // nil means time.Now
}

// Post sends one notification. Only HTTP 202 is success: Hermes answers 200
// "ignored"/"duplicate" without starting a turn.
func (p *Poster) Post(ctx context.Context, count int, from []string) error {
	body, err := json.Marshal(payload{EventType: EventType, Count: count, From: wait.FilterLabels(from)})
	if err != nil {
		return err
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	ts := strconv.FormatInt(now().Unix(), 10)
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid webhook URL")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Timestamp", ts)
	req.Header.Set("X-Webhook-Signature-V2", Sign(p.Secret, ts, body))
	req.Header.Set("X-Request-ID", hex.EncodeToString(id[:]))
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: postTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return transportError(ctx, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode == http.StatusAccepted {
		return nil
	}
	se := &StatusError{Code: resp.StatusCode}
	if resp.StatusCode == http.StatusOK {
		var r struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(data, &r) == nil && validWord(r.Status) {
			se.Word = r.Status
		}
	}
	return se
}

// Sign returns hex(HMAC-SHA256(secret, "<ts>.<body>")), Hermes' V2 signature.
func Sign(secret, ts string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(ts))
	m.Write([]byte("."))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// validWord keeps only a short lowercase word, so reply text cannot leak.
func validWord(s string) bool {
	if s == "" || len(s) > 32 {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && r != '_' {
			return false
		}
	}
	return true
}

// transportError reports only the class of a transport failure: the underlying
// error text can contain the URL.
func transportError(ctx context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, context.DeadlineExceeded) || isTimeout(err):
		return errors.New("webhook request timed out")
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	if strings.Contains(strings.ToLower(err.Error()), "refused") {
		return errors.New("webhook connection refused")
	}
	return errors.New("webhook request failed")
}

func isTimeout(err error) bool {
	var t interface{ Timeout() bool }
	return errors.As(err, &t) && t.Timeout()
}

// LoadSecret reads the webhook secret file. It refuses a missing or empty file
// and any file that group or other can access.
func LoadSecret(path string) (string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("secret file: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("secret file %s is not a regular file", path)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("secret file %s has mode %o; group and other access is not allowed (chmod 600)", path, fi.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("secret file: %w", err)
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", fmt.Errorf("secret file %s is empty", path)
	}
	return s, nil
}
