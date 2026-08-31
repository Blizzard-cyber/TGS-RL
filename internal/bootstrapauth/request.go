package bootstrapauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
)

// SignPayload authenticates a deterministic internal request payload. Callers
// must clear the authentication field before marshaling the payload.
func SignPayload(key, payload []byte) ([]byte, error) {
	if len(key) < 32 {
		return nil, errors.New("worker registry signing key must contain at least 32 bytes")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil), nil
}

// VerifyPayload compares a supplied request tag in constant time.
func VerifyPayload(key, payload, supplied []byte) bool {
	expected, err := SignPayload(key, payload)
	return err == nil && len(expected) == len(supplied) && hmac.Equal(expected, supplied)
}
