package bootstrapauth

import (
	"strings"
	"testing"
)

func TestScopedTokenNormalizesDevicesAndRejectsChangedIdentity(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	claims := Claims{
		RunID: "run-a", JobID: "job-a", RuntimeUnitID: "unit-a",
		SandboxID: "sandbox-a", BindingID: "binding-a", Generation: 4,
		DeviceIDs: []string{"GPU-b", "GPU-a"},
	}
	token, err := Sign(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	reordered := claims
	reordered.DeviceIDs = []string{"GPU-a", "GPU-b"}
	if !Verify(key, reordered, token) {
		t.Fatal("device ordering changed a scoped token")
	}
	tampered := claims
	tampered.Generation++
	if Verify(key, tampered, token) {
		t.Fatal("token authenticated a different generation")
	}
	if Verify([]byte(strings.Repeat("x", 32)), claims, token) {
		t.Fatal("token authenticated under a different signing key")
	}
}
