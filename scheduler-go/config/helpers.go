package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func componentRef(raw any, path string) (*ComponentRef, error) {
	data, err := mapping(raw, path)
	if err != nil {
		return nil, err
	}
	name, err := mustString(data["name"], path+".name")
	if err != nil {
		return nil, err
	}
	version, err := optionalString(data["version"], path+".version")
	if err != nil {
		return nil, err
	}
	return &ComponentRef{
		Name:    name,
		Version: version,
	}, nil
}

func resourceProviderRef(raw any, path string) (*ResourceProviderRef, error) {
	data, err := mapping(raw, path)
	if err != nil {
		return nil, err
	}
	name, err := mustString(data["name"], path+".name")
	if err != nil {
		return nil, err
	}
	source, err := mustString(data["source"], path+".source")
	if err != nil {
		return nil, err
	}
	return &ResourceProviderRef{
		Name:   name,
		Source: source,
	}, nil
}

func kubernetesRef(raw any, path string) (*KubernetesRef, error) {
	data, err := mapping(raw, path)
	if err != nil {
		return nil, err
	}
	enabled, err := mustBool(data["enabled"], path+".enabled")
	if err != nil {
		return nil, err
	}
	version, err := optionalString(data["version"], path+".version")
	if err != nil {
		return nil, err
	}
	return &KubernetesRef{
		Enabled: enabled,
		Version: version,
	}, nil
}

func dependencyRecords(raw any, path string) ([]DependencyRecord, error) {
	items, err := list(raw, path)
	if err != nil {
		return nil, err
	}
	records := make([]DependencyRecord, 0, len(items))
	for _, item := range items {
		data, err := mapping(item, path)
		if err != nil {
			return nil, err
		}
		name, err := mustString(data["name"], path+".name")
		if err != nil {
			return nil, err
		}
		version, err := mustString(data["version"], path+".version")
		if err != nil {
			return nil, err
		}
		source, err := mustString(data["source"], path+".source")
		if err != nil {
			return nil, err
		}
		sourceCommit, err := optionalString(data["source_commit"], path+".source_commit")
		if err != nil {
			return nil, err
		}
		artifactDigest, err := optionalString(data["artifact_digest"], path+".artifact_digest")
		if err != nil {
			return nil, err
		}
		records = append(records, DependencyRecord{
			Name:           name,
			Version:        version,
			Source:         source,
			SourceCommit:   sourceCommit,
			ArtifactDigest: artifactDigest,
		})
	}
	return records, nil
}

func developmentImages(raw any, path string) ([]DevelopmentImage, error) {
	items, err := list(raw, path)
	if err != nil {
		return nil, err
	}
	images := make([]DevelopmentImage, 0, len(items))
	for _, item := range items {
		data, err := mapping(item, path)
		if err != nil {
			return nil, err
		}
		name, err := mustString(data["name"], path+".name")
		if err != nil {
			return nil, err
		}
		image, err := mustString(data["image"], path+".image")
		if err != nil {
			return nil, err
		}
		imageDigest, err := mustString(data["image_digest"], path+".image_digest")
		if err != nil {
			return nil, err
		}
		images = append(images, DevelopmentImage{
			Name:        name,
			Image:       image,
			ImageDigest: imageDigest,
		})
	}
	return images, nil
}

func mapping(value any, path string) (map[string]any, error) {
	mapped, ok := value.(map[string]any)
	if !ok {
		return nil, configError(path+" must be a mapping", "schema_validation", "", path, nil)
	}
	return mapped, nil
}

func rejectUnknownFields(data map[string]any, path string, allowed ...string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key := range data {
		if _, ok := allowedSet[key]; ok {
			continue
		}
		return configError(path+" contains unknown field "+strconv.Quote(key), "schema_validation", "", path+"."+key, nil)
	}
	return nil
}

func list(value any, path string) ([]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, configError(path+" must be a list", "schema_validation", "", path, nil)
	}
	return items, nil
}

func stringSlice(value any, path string, allowEmpty bool) ([]string, error) {
	items, err := list(value, path)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 && !allowEmpty {
		return nil, configError(path+" must not be empty", "schema_validation", "", path, nil)
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		text, err := mustString(item, path)
		if err != nil {
			return nil, err
		}
		result = append(result, text)
	}
	return result, nil
}

func mustString(value any, path string) (string, error) {
	text, ok := value.(string)
	if !ok || text == "" {
		return "", configError(path+" must be a string", "schema_validation", "", path, nil)
	}
	return text, nil
}

func optionalString(value any, path string) (*string, error) {
	if value == nil {
		return nil, nil
	}
	text, ok := value.(string)
	if !ok {
		return nil, configError(path+" must be a string or null", "schema_validation", "", path, nil)
	}
	return &text, nil
}

func optionalTimestamp(value any, path string) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	text, err := mustString(value, path)
	if err != nil {
		return nil, err
	}
	parsed, err := time.Parse(time.RFC3339, text)
	if err != nil {
		return nil, configError(path+" must be an RFC3339 timestamp or null", "schema_validation", "", path, nil)
	}
	return &parsed, nil
}

func mustInt(value any, path string) (int, error) {
	number, ok := value.(int)
	if !ok {
		return 0, configError(path+" must be an integer", "schema_validation", "", path, nil)
	}
	return number, nil
}

func mustBool(value any, path string) (bool, error) {
	boolean, ok := value.(bool)
	if !ok {
		return false, configError(path+" must be a boolean", "schema_validation", "", path, nil)
	}
	return boolean, nil
}

func mustFloat(value any, path string) (float64, error) {
	switch number := value.(type) {
	case float64:
		return number, nil
	case int:
		return float64(number), nil
	default:
		return 0, configError(path+" must be a number", "schema_validation", "", path, nil)
	}
}

func mustDuration(value any, path string) (time.Duration, error) {
	text, err := mustString(value, path)
	if err != nil {
		return 0, err
	}
	duration, err := time.ParseDuration(text)
	if err != nil {
		return 0, configError(path+" must be a Go duration", "schema_validation", "", path, nil)
	}
	return duration, nil
}

func ensurePinnedVersion(version string, label string, source string) error {
	if floatingTokenPattern.MatchString(version) || unpinnedVersionPattern.MatchString(version) {
		return configError(fmt.Sprintf("%s must be pinned and must not use floating markers: %q", label, version), "schema_validation", source, label, nil)
	}
	return nil
}

func ensurePinnedImage(image string, label string, source string) error {
	if floatingTokenPattern.MatchString(image) {
		return configError(label+" must not use latest or another floating tag", "schema_validation", source, label, nil)
	}
	remainder := image
	if slash := strings.LastIndex(remainder, "/"); slash >= 0 {
		remainder = remainder[slash+1:]
	}
	if strings.Contains(remainder, "@") {
		return nil
	}
	if !strings.Contains(remainder, ":") {
		return configError(label+" must include an explicit tag", "schema_validation", source, label, nil)
	}
	return nil
}

func dataOriginSupports(dataOrigin string, dataKind string) bool {
	if dataOrigin == "" {
		return false
	}
	for _, part := range strings.Split(dataOrigin, "-or-") {
		if strings.TrimSpace(part) == dataKind {
			return true
		}
	}
	return false
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func resolvePath(repoRoot string, baseFile string, reference string) string {
	candidate := filepath.Clean(filepath.Join(repoRoot, reference))
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	return filepath.Clean(filepath.Join(filepath.Dir(baseFile), reference))
}

func requireExistingPath(repoRoot string, baseFile string, reference string, label string) error {
	resolved := resolvePath(repoRoot, baseFile, reference)
	if _, err := os.Stat(resolved); err != nil {
		return configError(fmt.Sprintf("%s does not exist: %s", label, reference), "missing_reference", baseFile, label, nil)
	}
	return nil
}

func resolveExistingPath(repoRoot string, baseFile string, reference string) (string, error) {
	resolved := resolvePath(repoRoot, baseFile, reference)
	if _, err := os.Stat(resolved); err != nil {
		return "", configError(fmt.Sprintf("referenced config path does not exist: %s", reference), "missing_reference", baseFile, reference, nil)
	}
	return resolved, nil
}

func absPath(path string) string {
	resolved, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return resolved
}

func cloneEnv(env map[string]string) map[string]string {
	if env == nil {
		env = make(map[string]string)
		for _, pair := range os.Environ() {
			if index := strings.IndexByte(pair, '='); index >= 0 {
				env[pair[:index]] = pair[index+1:]
			}
		}
	}
	cloned := make(map[string]string, len(env))
	for key, value := range env {
		cloned[key] = value
	}
	return cloned
}

func readEnvOverrides(prefix string, env map[string]string) AppliedOverrides {
	keys := map[string]string{
		"manifest":     prefix + "MANIFEST_PATH",
		"bom":          prefix + "BOM_PATH",
		"profile":      prefix + "PROFILE_PATH",
		"capabilities": prefix + "CAPABILITIES_PATH",
		"policy":       prefix + "POLICY_PATH",
		"scenario":     prefix + "SCENARIO_PATH",
	}
	redacted := make(map[string]string)
	for key, value := range env {
		if strings.HasPrefix(key, prefix) {
			redacted[key] = fmt.Sprint(redactValue(key, value))
		}
	}
	return AppliedOverrides{
		ManifestPath:     env[keys["manifest"]],
		BOMPath:          env[keys["bom"]],
		ProfilePath:      env[keys["profile"]],
		CapabilitiesPath: env[keys["capabilities"]],
		PolicyPath:       env[keys["policy"]],
		ScenarioPath:     env[keys["scenario"]],
		Env:              redacted,
	}
}

func expectSchema(actual string, kind string, source string) error {
	expected := expectedSchemas[kind]
	if actual != expected {
		return configError(fmt.Sprintf("%s schema_version must be %q, got %q", kind, expected, actual), "schema_version_mismatch", source, "schema_version", nil)
	}
	return nil
}

func redactValue(key string, value any) any {
	if sensitiveTokenPattern.MatchString(key) {
		return "<redacted>"
	}
	if text, ok := value.(string); ok && sensitiveTokenPattern.MatchString(text) {
		return "<redacted>"
	}
	return value
}

func parseScalar(value string) any {
	switch value {
	case "{}":
		return map[string]any{}
	case "[]":
		return []any{}
	case "null", "~":
		return nil
	case "true":
		return true
	case "false":
		return false
	}
	if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
		return value[1 : len(value)-1]
	}
	if number, err := strconv.Atoi(value); err == nil {
		return number
	}
	if floatNumber, err := strconv.ParseFloat(value, 64); err == nil && strings.Contains(value, ".") {
		return floatNumber
	}
	return value
}
