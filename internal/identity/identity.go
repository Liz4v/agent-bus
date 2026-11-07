// Package identity holds the broker's TLS identity: a long-lived ECDSA key,
// self-signed certificates built from it, and the fingerprint clients pin
// (multi-user M13). Certificate validity is never checked by clients; only
// the public-key fingerprint matters.
package identity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// FingerprintPrefix starts every fingerprint string.
const FingerprintPrefix = "SHA256:"

// CertLifetime is how long a freshly built certificate is valid.
const CertLifetime = 365 * 24 * time.Hour

// LoadOrCreateKey loads the ECDSA P-256 key at path (PKCS#8 PEM). If the file
// does not exist it creates it with O_EXCL and mode 0600. An existing file
// that cannot be read or parsed is an error and is never overwritten.
func LoadOrCreateKey(path string) (*ecdsa.PrivateKey, error) {
	key, err := loadKey(path)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create key directory: %w", err)
	}
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return loadKey(path) // lost a creation race; use the winner's key
		}
		return nil, fmt.Errorf("create key file %s: %w", path, err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: der}); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, err
	}
	return key, nil
}

func loadKey(path string) (*ecdsa.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("read key file %s (it is not regenerated automatically): %w", path, err)
	}
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("key file %s is not a PKCS#8 PEM private key (it is not regenerated automatically)", path)
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("key file %s is invalid (it is not regenerated automatically): %w", path, err)
	}
	ec, ok := k.(*ecdsa.PrivateKey)
	if !ok || ec.Curve != elliptic.P256() {
		return nil, fmt.Errorf("key file %s is not an ECDSA P-256 key", path)
	}
	return ec, nil
}

// SelfSignedCert builds a self-signed certificate for key, valid from now for
// CertLifetime. The same key always yields the same fingerprint.
func SelfSignedCert(key *ecdsa.PrivateKey, now time.Time) (tls.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 126))
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "agent-bus broker"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(CertLifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}

// FingerprintSPKI returns "SHA256:" plus the unpadded standard base64 of the
// SHA-256 of a DER SubjectPublicKeyInfo (the OpenSSH style).
func FingerprintSPKI(spki []byte) string {
	h := sha256.Sum256(spki)
	return FingerprintPrefix + base64.RawStdEncoding.EncodeToString(h[:])
}

// Fingerprint returns the fingerprint of a certificate's public key.
func Fingerprint(cert tls.Certificate) (string, error) {
	leaf := cert.Leaf
	if leaf == nil {
		if len(cert.Certificate) == 0 {
			return "", errors.New("certificate has no data")
		}
		var err error
		if leaf, err = x509.ParseCertificate(cert.Certificate[0]); err != nil {
			return "", err
		}
	}
	return FingerprintSPKI(leaf.RawSubjectPublicKeyInfo), nil
}

// ServerTLSConfig returns a TLS 1.3 server configuration for cert.
func ServerTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
}
