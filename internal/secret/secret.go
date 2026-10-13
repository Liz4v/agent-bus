// Package secret generates and hashes the bearer secrets (D3).
package secret

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// NewChannelID returns a random UUIDv4 string.
func NewChannelID() string {
	var b [16]byte
	mustRead(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// NewToken returns a session token: 32 random bytes, hex-encoded.
func NewToken() string {
	var b [32]byte
	mustRead(b[:])
	return hex.EncodeToString(b[:])
}

// Hash returns the SHA-256 of s.
func Hash(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

// Short returns the first 8 hex characters of the hash of s, for logs.
func Short(s string) string { return ShortHash(Hash(s)) }

// ShortHash returns the first 8 hex characters of an already-computed hash.
func ShortHash(h []byte) string {
	e := hex.EncodeToString(h)
	if len(e) > 8 {
		e = e[:8]
	}
	return e
}

func mustRead(b []byte) {
	if _, err := rand.Read(b); err != nil {
		panic("agent-bus: crypto/rand failed: " + err.Error())
	}
}
