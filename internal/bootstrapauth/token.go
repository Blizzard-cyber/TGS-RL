package bootstrapauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"strings"
)

type Claims struct {
	RunID         string   `json:"run_id"`
	JobID         string   `json:"job_id"`
	RuntimeUnitID string   `json:"runtime_unit_id"`
	SandboxID     string   `json:"sandbox_id"`
	BindingID     string   `json:"binding_id"`
	Generation    uint64   `json:"generation"`
	DeviceIDs     []string `json:"device_ids"`
}

func Sign(key []byte, claims Claims) (string, error) {
	if len(key) < 32 {
		return "", errors.New("worker registry signing key must contain at least 32 bytes")
	}
	normalized, err := normalize(claims)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func Verify(key []byte, claims Claims, token string) bool {
	expected, err := Sign(key, claims)
	if err != nil {
		return false
	}
	provided, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return false
	}
	want, _ := base64.RawURLEncoding.DecodeString(expected)
	return hmac.Equal(provided, want)
}

func normalize(claims Claims) (Claims, error) {
	claims.RunID = strings.TrimSpace(claims.RunID)
	claims.JobID = strings.TrimSpace(claims.JobID)
	claims.RuntimeUnitID = strings.TrimSpace(claims.RuntimeUnitID)
	claims.SandboxID = strings.TrimSpace(claims.SandboxID)
	claims.BindingID = strings.TrimSpace(claims.BindingID)
	if claims.RunID == "" || claims.JobID == "" || claims.RuntimeUnitID == "" || claims.SandboxID == "" || claims.BindingID == "" || claims.Generation == 0 {
		return Claims{}, errors.New("worker registration claims are incomplete")
	}
	seen := make(map[string]bool, len(claims.DeviceIDs))
	devices := make([]string, 0, len(claims.DeviceIDs))
	for _, deviceID := range claims.DeviceIDs {
		deviceID = strings.TrimSpace(deviceID)
		if deviceID == "" || seen[deviceID] {
			return Claims{}, errors.New("worker registration device identities are invalid")
		}
		seen[deviceID] = true
		devices = append(devices, deviceID)
	}
	sort.Strings(devices)
	claims.DeviceIDs = devices
	return claims, nil
}
