package client

import (
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Liz4v/agent-bus/internal/identity"
)

func TestRightPinConnects(t *testing.T) {
	c, tb := newBroker(t)
	if _, _, err := c.CreateSession(bg); err != nil {
		t.Fatal(err)
	}
	if tb.hits.Load() == 0 {
		t.Fatal("no request reached the broker")
	}
}

func TestWrongPinIsPinErrorAndNothingReachesHandler(t *testing.T) {
	tb := startBroker(t)
	cred := tb.hostmaster(t, "boss")
	const wrong = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	c, err := New(Config{BaseURL: tb.ts.URL, Pin: wrong, Credential: cred})
	if err != nil {
		t.Fatal(err)
	}
	before := tb.hits.Load()
	_, _, err = c.CreateSession(bg)
	var pe *PinError
	if !errors.As(err, &pe) {
		t.Fatalf("got %T %v, want *PinError", err, err)
	}
	if pe.Expected != wrong || pe.Got != tb.pin {
		t.Fatalf("PinError = %+v, want expected %s got %s", pe, wrong, tb.pin)
	}
	if !strings.Contains(pe.Error(), wrong) || !strings.Contains(pe.Error(), tb.pin) {
		t.Fatalf("message lacks fingerprints: %v", pe)
	}
	if _, err := c.AccountStatus(bg); !errors.As(err, &pe) {
		t.Fatalf("AccountStatus: %v", err)
	}
	if got := tb.hits.Load(); got != before {
		t.Fatalf("%d requests reached the handler through a wrong pin", got-before)
	}
}

func TestNewRefusesEmptyPinAndHTTP(t *testing.T) {
	if _, err := New(Config{BaseURL: "https://broker-host:7700"}); !errors.Is(err, ErrNotPinned) {
		t.Fatalf("empty pin: %v", err)
	}
	if _, err := New(Config{BaseURL: "http://broker-host:7700", Pin: "SHA256:x"}); err == nil {
		t.Fatal("http:// accepted")
	}
	if _, err := New(Config{BaseURL: "", Pin: "SHA256:x"}); err == nil {
		t.Fatal("empty URL accepted")
	}
}

func TestProbeReturnsServerFingerprintWithoutRequest(t *testing.T) {
	tb := startBroker(t)
	before := tb.hits.Load()
	fp, err := Probe(bg, tb.ts.URL)
	if err != nil || fp != tb.pin {
		t.Fatalf("Probe = %q, %v; want %q", fp, err, tb.pin)
	}
	if tb.hits.Load() != before {
		t.Fatal("Probe sent an HTTP request")
	}
	if _, err := Probe(bg, "http://127.0.0.1:1"); err == nil {
		t.Fatal("Probe accepted http://")
	}
	if _, err := Probe(bg, "https://127.0.0.1:1"); err == nil {
		t.Fatal("Probe of a closed port succeeded")
	}
}

func TestTLS12ServerIsRefused(t *testing.T) {
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	ts.TLS = &tls.Config{MaxVersion: tls.VersionTLS12}
	ts.StartTLS()
	defer ts.Close()
	fp := identity.FingerprintSPKI(ts.Certificate().RawSubjectPublicKeyInfo)
	c, err := New(Config{BaseURL: ts.URL, Pin: fp})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Register(bg, "x", ""); err == nil {
		t.Fatal("a TLS 1.2 server was accepted")
	}
}
