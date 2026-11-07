// Package testutil holds helpers shared by tests in several packages. It
// imports only identity, so any package's tests can use it without an import
// cycle.
package testutil

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Liz4v/agent-bus/internal/identity"
)

// TLSServer starts an HTTPS test server for h with a fresh self-signed
// identity and returns it with its pin (the fingerprint clients must use).
// The server is closed when the test ends.
func TLSServer(t testing.TB, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	key, err := identity.LoadOrCreateKey(filepath.Join(t.TempDir(), "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := identity.SelfSignedCert(key, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pin, err := identity.Fingerprint(cert)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = identity.ServerTLSConfig(cert)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, pin
}
