package tunnels

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// Secret mirrors CortexTerminal.Gateway.Tunnels.TunnelSecret.
//
// GenerateSecret returns 32 random bytes URL-safe base64 (no padding).
// Used as the ?k= visitor credential. GenerateTunnelKey returns 5 random
// bytes as 10 lowercase hex characters (40 bits of entropy), DNS-safe
// for <key>.<RootDomain>. Hash returns SHA-256(secret) lowercase hex;
// plaintext secret is never persisted. Verify compares Hash(secret)
// to the stored hash.
const (
	secretByteLength = 32
	keyByteLength    = 5
)

// GenerateSecret returns the ?k= visitor credential.
func GenerateSecret() string {
	b := make([]byte, secretByteLength)
	_, _ = rand.Read(b)
	return urlSafeBase64(b)
}

// GenerateTunnelKey returns the public DNS subdomain label.
func GenerateTunnelKey() string {
	b := make([]byte, keyByteLength)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Hash returns SHA-256(secret) as lowercase hex.
func Hash(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// Verify returns true iff Hash(secret) == storedHash.
func Verify(secret, storedHash string) bool {
	if secret == "" {
		return false
	}
	return Hash(secret) == storedHash
}

func urlSafeBase64(b []byte) string {
	s := base64.StdEncoding.EncodeToString(b)
	s = strings.ReplaceAll(s, "+", "-")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.TrimRight(s, "=")
	return s
}