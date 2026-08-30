package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

var (
	labelKeySanitizer = regexp.MustCompile(`[^a-z0-9A-Z./_-]+`)
	nameSanitizer     = regexp.MustCompile(`[^a-z0-9-]+`)
)

func bundleFingerprint(bundle *api.Bundle) (string, error) {
	payload, err := json.Marshal(bundle)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func bundleKey(input *normalizedInput) string {
	return fmt.Sprintf("%s/%s", input.Namespace, buildStableName("bundle", input))
}

func buildStableName(kind string, input *normalizedInput) string {
	parts := []string{
		kind,
		compactName(input.Run.GetJobId()),
		compactName(input.Run.GetRunId()),
		compactName(input.Plan.GetExecutionId()),
		compactName(input.Plan.GetStageId()),
		compactName(input.workloadUnitID),
	}
	return boundedName(parts)
}

func buildObjectName(kind string, input *normalizedInput) string {
	return boundedName([]string{buildStableName(kind, input), fmt.Sprintf("g%d", input.Generation)})
}

func boundedName(parts []string) string {
	joined := strings.Join(parts, "-")
	if len(joined) > 63 {
		sum := sha256.Sum256([]byte(joined))
		joined = joined[:52] + "-" + hex.EncodeToString(sum[:])[:10]
	}
	return strings.Trim(joined, "-")
}

func compactName(value string) string {
	value = strings.ToLower(value)
	value = nameSanitizer.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return "x"
	}
	return value
}

func sanitizeLabelKey(value string) string {
	value = labelKeySanitizer.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return "x"
	}
	return value
}

func sanitizeLabelValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "x"
	}
	value = labelKeySanitizer.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return "x"
	}
	return value
}

func sanitizeEnvName(value string) string {
	value = strings.ToUpper(value)
	value = strings.NewReplacer(".", "_", "-", "_", "/", "_").Replace(value)
	if value == "" {
		return "X"
	}
	return value
}

func sortedProtoLabelKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
