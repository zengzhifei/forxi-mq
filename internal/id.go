package internal

import (
	"crypto/rand"
	"encoding/hex"
)

// GenerateID returns a random 24-character hex string.
func GenerateID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}
