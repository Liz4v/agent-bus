package client

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Liz4v/agent-bus/internal/brokerfile"
	"github.com/Liz4v/agent-bus/internal/identity"
)

// ErrNotPinned is returned by New when no broker fingerprint is configured.
var ErrNotPinned = errors.New("agent-bus: the broker is not pinned (no fingerprint on record)")

// PinError reports that the broker presented a different public key than the
// pinned one. The handshake fails before any bearer value is sent.
type PinError struct {
	Expected, Got string
}

func (e *PinError) Error() string {
	return fmt.Sprintf("the broker's key fingerprint %s does not match the pinned %s", e.Got, e.Expected)
}

// pinnedTLS returns a TLS configuration that ignores the CA chain and accepts
// exactly one public key: the one whose fingerprint is pin (M13).
func pinnedTLS(pin string) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // the pin below replaces chain verification
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("the broker presented no certificate")
			}
			got := identity.FingerprintSPKI(cs.PeerCertificates[0].RawSubjectPublicKeyInfo)
			if subtle.ConstantTimeCompare([]byte(got), []byte(pin)) != 1 {
				return &PinError{Expected: pin, Got: got}
			}
			return nil
		},
	}
}

func pinnedTransport(pin string) *http.Transport {
	return &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     pinnedTLS(pin),
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
	}
}

// Probe performs a TLS handshake with the broker at rawURL and returns its
// public-key fingerprint. It sends no request and is for the human pin flow
// only: the result must be confirmed by the human before it is trusted.
func Probe(ctx context.Context, rawURL string) (string, error) {
	n, err := brokerfile.NormalizeURL(rawURL)
	if err != nil {
		return "", err
	}
	host := n[len("https://"):]
	var fp string
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("the broker presented no certificate")
			}
			fp = identity.FingerprintSPKI(cs.PeerCertificates[0].RawSubjectPublicKeyInfo)
			return nil
		},
	}
	d := tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: cfg}
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return "", fmt.Errorf("cannot reach the broker at %s over TLS: %w", n, err)
	}
	conn.Close()
	return fp, nil
}
