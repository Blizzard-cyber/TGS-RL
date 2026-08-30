package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/preemption"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/scheduler"
)

func TestLoadBundleReadsRealConfigGraph(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	bundle, err := LoadBundle(repoRoot)
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	if bundle.Manifest.ManifestID != "cpu-mock" {
		t.Fatalf("manifest id = %q, want cpu-mock", bundle.Manifest.ManifestID)
	}
	if bundle.BOM.BOMID != "runtime" {
		t.Fatalf("bom id = %q, want runtime", bundle.BOM.BOMID)
	}
	if bundle.Profile.ProfileID != "cpu-mock-v1" {
		t.Fatalf("profile id = %q, want cpu-mock-v1", bundle.Profile.ProfileID)
	}
	if bundle.Capabilities.CapabilityID != "cpu-mock-v1" {
		t.Fatalf("capability id = %q, want cpu-mock-v1", bundle.Capabilities.CapabilityID)
	}
	if bundle.Policy.PolicyID != "static" {
		t.Fatalf("policy id = %q, want static", bundle.Policy.PolicyID)
	}
	if !bundle.Policy.Protection.Enabled || bundle.Policy.Protection.Cooldown <= 0 || bundle.Policy.Protection.MaxActionsPerWindow != 8 {
		t.Fatalf("protection policy = %+v", bundle.Policy.Protection)
	}
	if bundle.Policy.Preemption.Enabled || bundle.Policy.Preemption.Strategy != "noop" || !bundle.Policy.Preemption.RequireSafePoint {
		t.Fatalf("preemption policy = %+v", bundle.Policy.Preemption)
	}
	if !bundle.Policy.Constraints.RequireCapacity || !bundle.Policy.Constraints.RequireCapability || !bundle.Policy.Constraints.RequireSafePoint {
		t.Fatalf("constraint policy = %+v", bundle.Policy.Constraints)
	}
	if !bundle.Policy.Determinism.ExplicitSeedRequired || !bundle.Policy.Determinism.VirtualClockRequiredForReplay || !bundle.Policy.Determinism.DeterministicProtoSerialization || bundle.Policy.Determinism.WallClockInCanonicalOutput {
		t.Fatalf("determinism policy = %+v", bundle.Policy.Determinism)
	}
	if bundle.Policy.Safety.RewardValueAllowed || bundle.Policy.Safety.StaleIntentAllowed || bundle.Policy.Safety.StaleSnapshotCommitAllowed || bundle.Policy.Safety.PartialFailureIsSuccess {
		t.Fatalf("safety policy = %+v", bundle.Policy.Safety)
	}
	projection := bundle.Policy.SchedulerPolicy()
	if projection.PolicyID != "static" || projection.Strategy != "score_first" || projection.TopK != 1 || !projection.Protection.Enabled || projection.Preemption.Strategy != "noop" {
		t.Fatalf("scheduler policy projection = %+v", projection)
	}
	if bundle.Scenario.ScenarioID != "tool-wait-v1" {
		t.Fatalf("scenario id = %q, want tool-wait-v1", bundle.Scenario.ScenarioID)
	}
	if bundle.Overrides.Env == nil || len(bundle.Overrides.Env) != 0 {
		t.Fatalf("unexpected default overrides env: %#v", bundle.Overrides.Env)
	}
}

func TestLoadBundleWithManifestOverrideAndRedactedEnv(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	tempRoot := t.TempDir()
	copyTree(t, filepath.Join(repoRoot, "compatibility"), filepath.Join(tempRoot, "compatibility"))
	copyTree(t, filepath.Join(repoRoot, "configs"), filepath.Join(tempRoot, "configs"))
	copyTree(t, filepath.Join(repoRoot, "upstream"), filepath.Join(tempRoot, "upstream"))

	manifestPath := filepath.Join(tempRoot, "compatibility", "manifests", "override.yaml")
	manifestBytes, err := os.ReadFile(filepath.Join(repoRoot, "compatibility", "manifests", "cpu-mock.yaml"))
	if err != nil {
		t.Fatalf("read source manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, manifestBytes, 0o644); err != nil {
		t.Fatalf("write override manifest: %v", err)
	}
	scenarioPath := filepath.Join(tempRoot, "configs", "scenarios", "tool-wait.yaml")
	scenarioBytes, err := os.ReadFile(scenarioPath)
	if err != nil {
		t.Fatalf("read scenario: %v", err)
	}
	updatedScenario := strings.ReplaceAll(
		string(scenarioBytes),
		"compatibility/manifests/cpu-mock.yaml",
		"compatibility/manifests/override.yaml",
	)
	if err := os.WriteFile(scenarioPath, []byte(updatedScenario), 0o644); err != nil {
		t.Fatalf("write scenario: %v", err)
	}

	bundle, err := LoadBundleWithOptions(LoadOptions{
		Root: tempRoot,
		Env: map[string]string{
			"TGSRL_CONFIG_MANIFEST_PATH": "compatibility/manifests/override.yaml",
			"TGSRL_CONFIG_API_TOKEN":     "super-secret-token",
		},
	})
	if err != nil {
		t.Fatalf("LoadBundleWithOptions() error = %v", err)
	}
	if filepath.Base(bundle.Manifest.Path) != "override.yaml" {
		t.Fatalf("manifest path = %q, want override.yaml", bundle.Manifest.Path)
	}
	if bundle.Overrides.ManifestPath != "compatibility/manifests/override.yaml" {
		t.Fatalf("override manifest path = %q", bundle.Overrides.ManifestPath)
	}
	if got := bundle.Overrides.Env["TGSRL_CONFIG_API_TOKEN"]; got != "<redacted>" {
		t.Fatalf("redacted env = %q, want <redacted>", got)
	}
}

func TestMissingOverrideReturnsSecretSafeDiagnostic(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	tempRoot := t.TempDir()
	copyTree(t, filepath.Join(repoRoot, "compatibility"), filepath.Join(tempRoot, "compatibility"))
	copyTree(t, filepath.Join(repoRoot, "configs"), filepath.Join(tempRoot, "configs"))
	copyTree(t, filepath.Join(repoRoot, "upstream"), filepath.Join(tempRoot, "upstream"))

	_, err := LoadBundleWithOptions(LoadOptions{
		Root: tempRoot,
		Env: map[string]string{
			"TGSRL_CONFIG_SCENARIO_PATH": "configs/scenarios/missing.yaml",
			"TGSRL_CONFIG_SECRET_KEY":    "abc123",
		},
	})
	if err == nil {
		t.Fatal("LoadBundleWithOptions() error = nil, want missing_reference")
	}
	configErr, ok := err.(*ConfigError)
	if !ok {
		t.Fatalf("error type = %T, want *ConfigError", err)
	}
	diagnostic := configErr.Diagnostic()
	if diagnostic["code"] != "missing_reference" {
		t.Fatalf("diagnostic code = %#v, want missing_reference", diagnostic["code"])
	}
	if strings.Contains(fmt.Sprint(diagnostic), "abc123") {
		t.Fatalf("diagnostic leaked secret: %#v", diagnostic)
	}
}

func TestLoadPolicyMalformedYAMLReturnsConfigErrorWithoutPanic(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte("schema_version:\n\t\"tgsrl.io/policy-bundle/v1alpha1\"\n"), 0o644); err != nil {
		t.Fatalf("write malformed policy: %v", err)
	}

	assertNoPanic(t, func() {
		_, err := LoadPolicy(path)
		if err == nil {
			t.Fatal("LoadPolicy() error = nil, want yaml_parse_error")
		}
		configErr, ok := err.(*ConfigError)
		if !ok {
			t.Fatalf("error type = %T, want *ConfigError", err)
		}
		if configErr.CodeOrDefault() != "yaml_parse_error" {
			t.Fatalf("error code = %q, want yaml_parse_error", configErr.CodeOrDefault())
		}
	})
}

func TestLoadPolicyTypeMismatchReturnsConfigErrorWithoutPanic(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	path := filepath.Join(t.TempDir(), "policy.yaml")
	policyBytes, err := os.ReadFile(filepath.Join(repoRoot, "configs", "policies", "static.yaml"))
	if err != nil {
		t.Fatalf("read source policy: %v", err)
	}
	invalidPolicy := strings.Replace(string(policyBytes), "top_k: 1", "top_k: \"oops\"", 1)
	if err := os.WriteFile(path, []byte(invalidPolicy), 0o644); err != nil {
		t.Fatalf("write invalid policy: %v", err)
	}

	assertNoPanic(t, func() {
		_, err := LoadPolicy(path)
		if err == nil {
			t.Fatal("LoadPolicy() error = nil, want schema_validation")
		}
		configErr, ok := err.(*ConfigError)
		if !ok {
			t.Fatalf("error type = %T, want *ConfigError", err)
		}
		if configErr.CodeOrDefault() != "schema_validation" {
			t.Fatalf("error code = %q, want schema_validation", configErr.CodeOrDefault())
		}
		if configErr.Field != "policy.selection.top_k" {
			t.Fatalf("error field = %q, want policy.selection.top_k", configErr.Field)
		}
		if !strings.Contains(configErr.Message, "must be an integer") {
			t.Fatalf("error message = %q, want integer validation", configErr.Message)
		}
	})
}

func TestLoadBundleAllowsPolicyTopKGreaterThanOne(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	tempRoot := t.TempDir()
	copyTree(t, filepath.Join(repoRoot, "compatibility"), filepath.Join(tempRoot, "compatibility"))
	copyTree(t, filepath.Join(repoRoot, "configs"), filepath.Join(tempRoot, "configs"))
	copyTree(t, filepath.Join(repoRoot, "upstream"), filepath.Join(tempRoot, "upstream"))

	policyPath := filepath.Join(tempRoot, "configs", "policies", "static.yaml")
	policyBytes, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatalf("read copied policy: %v", err)
	}
	updatedPolicy := strings.Replace(string(policyBytes), "top_k: 1", "top_k: 3", 1)
	if err := os.WriteFile(policyPath, []byte(updatedPolicy), 0o644); err != nil {
		t.Fatalf("write updated policy: %v", err)
	}

	bundle, err := LoadBundle(tempRoot)
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	if bundle.Policy.Selection.TopK != 3 {
		t.Fatalf("bundle policy top_k = %d, want 3", bundle.Policy.Selection.TopK)
	}
}

func TestLoadPolicyRejectsUnknownTopLevelField(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	path := filepath.Join(t.TempDir(), "policy.yaml")
	policyBytes, err := os.ReadFile(filepath.Join(repoRoot, "configs", "policies", "static.yaml"))
	if err != nil {
		t.Fatalf("read source policy: %v", err)
	}
	invalidPolicy := string(policyBytes) + "\nfuture_toggle: true\n"
	if err := os.WriteFile(path, []byte(invalidPolicy), 0o644); err != nil {
		t.Fatalf("write invalid policy: %v", err)
	}

	_, err = LoadPolicy(path)
	if err == nil {
		t.Fatal("LoadPolicy() error = nil, want schema_validation")
	}
	configErr, ok := err.(*ConfigError)
	if !ok {
		t.Fatalf("error type = %T, want *ConfigError", err)
	}
	if configErr.Field != "policy.future_toggle" {
		t.Fatalf("error field = %q, want policy.future_toggle", configErr.Field)
	}
	if !strings.Contains(configErr.Message, "unknown field") {
		t.Fatalf("error message = %q, want unknown field", configErr.Message)
	}
}

func TestLoadPolicyRejectsUnknownNestedField(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	path := filepath.Join(t.TempDir(), "policy.yaml")
	policyBytes, err := os.ReadFile(filepath.Join(repoRoot, "configs", "policies", "static.yaml"))
	if err != nil {
		t.Fatalf("read source policy: %v", err)
	}
	invalidPolicy := strings.Replace(string(policyBytes), "  top_k: 1", "  top_k: 1\n  unknown_mode: true", 1)
	if err := os.WriteFile(path, []byte(invalidPolicy), 0o644); err != nil {
		t.Fatalf("write invalid policy: %v", err)
	}

	_, err = LoadPolicy(path)
	if err == nil {
		t.Fatal("LoadPolicy() error = nil, want schema_validation")
	}
	configErr, ok := err.(*ConfigError)
	if !ok {
		t.Fatalf("error type = %T, want *ConfigError", err)
	}
	if configErr.Field != "policy.selection.unknown_mode" {
		t.Fatalf("error field = %q, want policy.selection.unknown_mode", configErr.Field)
	}
	if !strings.Contains(configErr.Message, "unknown field") {
		t.Fatalf("error message = %q, want unknown field", configErr.Message)
	}
}

func TestLoadPolicyRejectsUnsupportedConstraintRelaxation(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	path := filepath.Join(t.TempDir(), "policy.yaml")
	policyBytes, err := os.ReadFile(filepath.Join(repoRoot, "configs", "policies", "static.yaml"))
	if err != nil {
		t.Fatalf("read source policy: %v", err)
	}
	invalidPolicy := strings.Replace(string(policyBytes), "  require_current_intent: true", "  require_current_intent: false", 1)
	if err := os.WriteFile(path, []byte(invalidPolicy), 0o644); err != nil {
		t.Fatalf("write invalid policy: %v", err)
	}

	_, err = LoadPolicy(path)
	if err == nil {
		t.Fatal("LoadPolicy() error = nil, want schema_validation")
	}
	configErr, ok := err.(*ConfigError)
	if !ok {
		t.Fatalf("error type = %T, want *ConfigError", err)
	}
	if configErr.Field != "policy.constraints.require_current_intent" {
		t.Fatalf("error field = %q, want policy.constraints.require_current_intent", configErr.Field)
	}
	if !strings.Contains(configErr.Message, "not supported") {
		t.Fatalf("error message = %q, want not supported", configErr.Message)
	}
}

func TestLoadPolicyAllowsSupportedPreemptionEnablement(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	path := filepath.Join(t.TempDir(), "policy.yaml")
	policyBytes, err := os.ReadFile(filepath.Join(repoRoot, "configs", "policies", "static.yaml"))
	if err != nil {
		t.Fatalf("read source policy: %v", err)
	}
	invalidPolicy := strings.Replace(string(policyBytes), "  enabled: false\n  strategy: \"noop\"", "  enabled: true\n  strategy: \"low_priority_first\"", 1)
	if err := os.WriteFile(path, []byte(invalidPolicy), 0o644); err != nil {
		t.Fatalf("write invalid policy: %v", err)
	}

	policyBundle, err := LoadPolicy(path)
	if err != nil {
		t.Fatalf("LoadPolicy() error = %v", err)
	}
	if !policyBundle.Preemption.Enabled {
		t.Fatalf("preemption enabled = %v, want true", policyBundle.Preemption.Enabled)
	}
	if policyBundle.Preemption.Strategy != "low_priority_first" {
		t.Fatalf("preemption strategy = %q, want low_priority_first", policyBundle.Preemption.Strategy)
	}
	if !policyBundle.Preemption.RequireSafePoint {
		t.Fatalf("preemption require_safe_point = %v, want true", policyBundle.Preemption.RequireSafePoint)
	}

	projected, err := policyBundle.ApplyToScheduler(scheduler.Config{})
	if err != nil {
		t.Fatalf("ApplyToScheduler() error = %v", err)
	}
	if !projected.Policy.AllowPreemption {
		t.Fatalf("projected policy allow_preemption = %v, want true", projected.Policy.AllowPreemption)
	}
	if projected.Policy.PreemptionPolicy != "low_priority_first" {
		t.Fatalf("projected policy preemption = %q, want low_priority_first", projected.Policy.PreemptionPolicy)
	}
	if !projected.Policy.RequireSafePoint {
		t.Fatalf("projected policy require_safe_point = %v, want true", projected.Policy.RequireSafePoint)
	}
	if _, ok := projected.Preemption.(preemption.LowPriorityFirst); !ok {
		t.Fatalf("projected preemption strategy type = %T, want preemption.LowPriorityFirst", projected.Preemption)
	}
}

func TestLoadPolicyRejectsUnsupportedDeterminismAndSafetyDeviation(t *testing.T) {
	t.Helper()
	repoRoot := repoRootFromTest(t)
	path := filepath.Join(t.TempDir(), "policy.yaml")
	policyBytes, err := os.ReadFile(filepath.Join(repoRoot, "configs", "policies", "static.yaml"))
	if err != nil {
		t.Fatalf("read source policy: %v", err)
	}
	invalidPolicy := strings.Replace(string(policyBytes), "  wall_clock_in_canonical_output: false", "  wall_clock_in_canonical_output: true", 1)
	invalidPolicy = strings.Replace(invalidPolicy, "  reward_value_allowed: false", "  reward_value_allowed: true", 1)
	if err := os.WriteFile(path, []byte(invalidPolicy), 0o644); err != nil {
		t.Fatalf("write invalid policy: %v", err)
	}

	_, err = LoadPolicy(path)
	if err == nil {
		t.Fatal("LoadPolicy() error = nil, want schema_validation")
	}
	configErr, ok := err.(*ConfigError)
	if !ok {
		t.Fatalf("error type = %T, want *ConfigError", err)
	}
	if configErr.Field != "policy.determinism.wall_clock_in_canonical_output" {
		t.Fatalf("error field = %q, want policy.determinism.wall_clock_in_canonical_output", configErr.Field)
	}
	if !strings.Contains(configErr.Message, "not supported") {
		t.Fatalf("error message = %q, want not supported", configErr.Message)
	}
}

func assertNoPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("unexpected panic: %v", recovered)
		}
	}()
	fn()
}

func repoRootFromTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

func copyTree(t *testing.T, src string, dst string) {
	t.Helper()
	if err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		bytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, bytes, 0o644)
	}); err != nil {
		t.Fatalf("copyTree(%s, %s) error = %v", src, dst, err)
	}
}
