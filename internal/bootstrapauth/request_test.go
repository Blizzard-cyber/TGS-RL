package bootstrapauth

import (
	"strings"
	"testing"
)

func TestPayloadAuthenticationRejectsTampering(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	tag, err := SignPayload(key, []byte("request-a"))
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPayload(key, []byte("request-a"), tag) {
		t.Fatal("valid request tag was rejected")
	}
	if VerifyPayload(key, []byte("request-b"), tag) {
		t.Fatal("request tag authenticated modified content")
	}
	if VerifyPayload([]byte("short"), []byte("request-a"), tag) {
		t.Fatal("request tag authenticated under an invalid key")
	}
}
