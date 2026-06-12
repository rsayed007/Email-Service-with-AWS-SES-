package auth

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// generateRandomHex returns n random bytes encoded as a lowercase hex string
// (length = 2*n).
func generateRandomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generateRandomHex: %w", err)
	}
	return hex.EncodeToString(b), nil
}
