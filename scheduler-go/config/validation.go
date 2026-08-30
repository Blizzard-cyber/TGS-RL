package config

import (
	"fmt"
	"time"
)

func validateBundle(bundle *Bundle) error {
	if err := requireExistingPath(bundle.Root, bundle.Manifest.Path, bundle.Manifest.References.PatchLedger, "manifest.references.patch_ledger"); err != nil {
		return err
	}
	if err := requireExistingPath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.CompatibilityManifest, "scenario.references.compatibility_manifest"); err != nil {
		return err
	}
	if err := requireExistingPath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.ResourceProfile, "scenario.references.resource_profile"); err != nil {
		return err
	}
	if err := requireExistingPath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.Capabilities, "scenario.references.capabilities"); err != nil {
		return err
	}
	if err := requireExistingPath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.Policy, "scenario.references.policy"); err != nil {
		return err
	}
	if resolvePath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.CompatibilityManifest) != bundle.Manifest.Path {
		return configError("scenario.references.compatibility_manifest must resolve to the loaded manifest", "cross_field_validation", bundle.Scenario.Path, "scenario.references.compatibility_manifest", nil)
	}
	if resolvePath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.ResourceProfile) != bundle.Profile.Path {
		return configError("scenario.references.resource_profile must resolve to the loaded profile", "cross_field_validation", bundle.Scenario.Path, "scenario.references.resource_profile", nil)
	}
	if resolvePath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.Capabilities) != bundle.Capabilities.Path {
		return configError("scenario.references.capabilities must resolve to the loaded capabilities", "cross_field_validation", bundle.Scenario.Path, "scenario.references.capabilities", nil)
	}
	if resolvePath(bundle.Root, bundle.Scenario.Path, bundle.Scenario.References.Policy) != bundle.Policy.Path {
		return configError("scenario.references.policy must resolve to the loaded policy", "cross_field_validation", bundle.Scenario.Path, "scenario.references.policy", nil)
	}
	if bundle.Manifest.Combination.ResourceProvider.Source != bundle.Profile.Provider.Source {
		return configError("provider source mismatch between manifest and profile", "cross_field_validation", bundle.Manifest.Path, "resource_provider.source", nil)
	}
	if bundle.Manifest.Combination.ResourceProvider.Source != bundle.Capabilities.Source {
		return configError("provider source mismatch between manifest and capabilities", "cross_field_validation", bundle.Manifest.Path, "resource_provider.source", nil)
	}
	if bundle.Manifest.Combination.ResourceProvider.Source != bundle.Scenario.Topology.ProviderSource {
		return configError("provider source mismatch between manifest and scenario topology", "cross_field_validation", bundle.Scenario.Path, "scenario.topology.provider_source", nil)
	}
	if !contains(bundle.Manifest.DeclaredMatrix.DataKinds, bundle.Scenario.DataKind) {
		return configError("scenario data_kind is not declared by the manifest", "cross_field_validation", bundle.Scenario.Path, "scenario.data_kind", nil)
	}
	if !contains(bundle.Profile.RuntimeScope.DataKinds, bundle.Scenario.DataKind) {
		return configError("scenario data_kind is not allowed by the profile", "cross_field_validation", bundle.Scenario.Path, "scenario.data_kind", nil)
	}
	if !dataOriginSupports(bundle.Capabilities.Attributes["data_origin"], bundle.Scenario.DataKind) {
		return configError("capabilities.attributes.data_origin is inconsistent with the scenario data_kind", "cross_field_validation", bundle.Capabilities.Path, "capabilities.attributes.data_origin", nil)
	}
	if !contains(bundle.Manifest.DeclaredMatrix.Algorithms, bundle.Scenario.Workload.Algorithm) {
		return configError("scenario algorithm is not declared by the manifest", "cross_field_validation", bundle.Scenario.Path, "scenario.workload.algorithm", nil)
	}
	if !contains(bundle.Profile.RuntimeScope.Algorithms, bundle.Scenario.Workload.Algorithm) {
		return configError("scenario algorithm is not allowed by the profile", "cross_field_validation", bundle.Scenario.Path, "scenario.workload.algorithm", nil)
	}
	if !contains(bundle.Capabilities.Algorithms, bundle.Scenario.Workload.Algorithm) {
		return configError("scenario algorithm is not supported by capabilities", "cross_field_validation", bundle.Scenario.Path, "scenario.workload.algorithm", nil)
	}
	if !contains(bundle.Manifest.DeclaredMatrix.RolloutModes, bundle.Scenario.Workload.RolloutMode) {
		return configError("scenario rollout_mode is not declared by the manifest", "cross_field_validation", bundle.Scenario.Path, "scenario.workload.rollout_mode", nil)
	}
	if !contains(bundle.Profile.RuntimeScope.RolloutModes, bundle.Scenario.Workload.RolloutMode) {
		return configError("scenario rollout_mode is not allowed by the profile", "cross_field_validation", bundle.Scenario.Path, "scenario.workload.rollout_mode", nil)
	}
	if !contains(bundle.Capabilities.RolloutModes, bundle.Scenario.Workload.RolloutMode) {
		return configError("scenario rollout_mode is not supported by capabilities", "cross_field_validation", bundle.Scenario.Path, "scenario.workload.rollout_mode", nil)
	}
	for _, action := range bundle.Profile.RuntimeScope.LifecycleActions {
		if !contains(bundle.Capabilities.SupportedActions, action) {
			return configError("capabilities.supported_actions must include all profile lifecycle actions", "cross_field_validation", bundle.Capabilities.Path, "capabilities.supported_actions", nil)
		}
	}
	if (contains(bundle.Capabilities.SupportedActions, "rebind") || contains(bundle.Capabilities.SupportedActions, "recreate")) && !bundle.Policy.Actions.RequireGenerationFenceForL4 {
		return configError("policy.actions.require_generation_fence_for_l4 must be true when L4 actions are enabled", "cross_field_validation", bundle.Policy.Path, "policy.actions.require_generation_fence_for_l4", nil)
	}
	if bundle.Policy.Actions.UnsupportedAction != "reject" {
		return configError("policy.actions.unsupported_action must be reject", "schema_validation", bundle.Policy.Path, "policy.actions.unsupported_action", nil)
	}
	if bundle.Policy.PolicyVersion != "1" {
		return configError("policy.policy_version must be a pinned concrete version", "schema_validation", bundle.Policy.Path, "policy.policy_version", nil)
	}
	return nil
}

func validateBOM(bom *RuntimeBOM) error {
	if bom.PinningPolicy.FloatingVersionsAllowed {
		return configError("bom.pinning_policy.floating_versions_allowed must be false", "schema_validation", bom.Path, "bom.pinning_policy.floating_versions_allowed", nil)
	}
	if !bom.PinningPolicy.ImmutableImageDigestRequiredForRelease {
		return configError("bom.pinning_policy.immutable_image_digest_required_for_release must be true", "schema_validation", bom.Path, "bom.pinning_policy.immutable_image_digest_required_for_release", nil)
	}
	for _, record := range append(append([]DependencyRecord{}, bom.Toolchain...), bom.RuntimeDependencies...) {
		if err := ensurePinnedVersion(record.Version, record.Name+" version", bom.Path); err != nil {
			return err
		}
	}
	for _, image := range bom.DevelopmentImages {
		if err := ensurePinnedImage(image.Image, image.Name+" image", bom.Path); err != nil {
			return err
		}
		if !imageDigestPattern.MatchString(image.ImageDigest) {
			return configError(fmt.Sprintf("%s image_digest must be an immutable sha256 digest", image.Name), "schema_validation", bom.Path, "bom.development_images.image_digest", nil)
		}
	}
	return nil
}

func validateProfile(profile *CompatibilityProfile) error {
	gpu := profile.GPUManagementProfile
	if gpu.MaximumActiveProfiles != 1 {
		return configError("profile.gpu_management_profile.maximum_active_profiles must be 1", "schema_validation", profile.Path, "profile.gpu_management_profile.maximum_active_profiles", nil)
	}
	if len(gpu.EnabledProfiles) > 1 {
		return configError("profile.gpu_management_profile.enabled_profiles must contain at most one active profile", "schema_validation", profile.Path, "profile.gpu_management_profile.enabled_profiles", nil)
	}
	if !contains(gpu.AllowedValues, gpu.Selected) {
		return configError("profile.gpu_management_profile.selected must be in allowed_values", "schema_validation", profile.Path, "profile.gpu_management_profile.selected", nil)
	}
	if len(gpu.EnabledProfiles) == 1 && gpu.EnabledProfiles[0] != gpu.Selected {
		return configError("the enabled GPU profile must match gpu_management_profile.selected", "cross_field_validation", profile.Path, "profile.gpu_management_profile.enabled_profiles", nil)
	}
	if len(gpu.EnabledProfiles) == 0 && gpu.Selected != "none" {
		return configError("gpu_management_profile.selected must be none when no profile is enabled", "cross_field_validation", profile.Path, "profile.gpu_management_profile.selected", nil)
	}
	return nil
}

func validatePolicy(policy *PolicyBundle) error {
	if policy.Selection.TopK < 1 {
		return configError("policy.selection.top_k must be at least 1", "schema_validation", policy.Path, "policy.selection.top_k", nil)
	}
	if len(policy.Selection.Tiebreakers) == 0 {
		return configError("policy.selection.tiebreakers must not be empty", "schema_validation", policy.Path, "policy.selection.tiebreakers", nil)
	}
	if len(policy.Fallback.Order) == 0 {
		return configError("policy.fallback.order must not be empty", "schema_validation", policy.Path, "policy.fallback.order", nil)
	}
	allowed := map[string]bool{"last-valid-intent": true, "no-op": true, "static": true}
	for _, value := range policy.Fallback.Order {
		if !allowed[value] {
			return configError(fmt.Sprintf("policy.fallback.order contains unsupported fallback values: [%s]", value), "schema_validation", policy.Path, "policy.fallback.order", nil)
		}
	}
	if !policy.Actions.RequireIdempotencyKey {
		return configError("policy.actions.require_idempotency_key must be true", "schema_validation", policy.Path, "policy.actions.require_idempotency_key", nil)
	}
	if !policy.Actions.RequireExplicitRollback {
		return configError("policy.actions.require_explicit_rollback must be true", "schema_validation", policy.Path, "policy.actions.require_explicit_rollback", nil)
	}
	if policy.Actions.UnsupportedAction != "reject" {
		return configError("policy.actions.unsupported_action must be reject", "schema_validation", policy.Path, "policy.actions.unsupported_action", nil)
	}
	if !policy.Constraints.RequireCapacity {
		return configError("policy.constraints.require_capacity=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.constraints.require_capacity", nil)
	}
	if !policy.Constraints.RequireCapability {
		return configError("policy.constraints.require_capability=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.constraints.require_capability", nil)
	}
	if !policy.Constraints.RequireSafePoint {
		return configError("policy.constraints.require_safe_point=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.constraints.require_safe_point", nil)
	}
	if !policy.Constraints.RequireCurrentIntent {
		return configError("policy.constraints.require_current_intent=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.constraints.require_current_intent", nil)
	}
	if !policy.Constraints.RequireSnapshotRevisionMatch {
		return configError("policy.constraints.require_snapshot_revision_match=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.constraints.require_snapshot_revision_match", nil)
	}
	if !policy.Constraints.RejectUnknownCapability {
		return configError("policy.constraints.reject_unknown_capability=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.constraints.reject_unknown_capability", nil)
	}
	if policy.Protection.Cooldown < 0 || policy.Protection.Hysteresis < 0 || policy.Protection.MaxActionsPerWindow < 1 || policy.Protection.Window <= 0 || policy.Protection.BreakerThreshold < 1 || policy.Protection.BreakerResetAfter <= 0 {
		return configError("policy.protection contains invalid cooldown, hysteresis, budget, or breaker settings", "schema_validation", policy.Path, "policy.protection", nil)
	}
	if policy.Preemption.RequireSafePoint != policy.Constraints.RequireSafePoint {
		return configError("policy.preemption.require_safe_point must match policy.constraints.require_safe_point", "cross_field_validation", policy.Path, "policy.preemption.require_safe_point", nil)
	}
	if policy.Preemption.Enabled && policy.Preemption.Strategy != "low_priority_first" {
		return configError("policy.preemption.strategy must be low_priority_first when enabled", "schema_validation", policy.Path, "policy.preemption.strategy", nil)
	}
	if !policy.Preemption.Enabled && policy.Preemption.Strategy != "noop" {
		return configError("policy.preemption.strategy must be noop when disabled", "schema_validation", policy.Path, "policy.preemption.strategy", nil)
	}
	if !policy.Determinism.ExplicitSeedRequired {
		return configError("policy.determinism.explicit_seed_required=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.determinism.explicit_seed_required", nil)
	}
	if !policy.Determinism.VirtualClockRequiredForReplay {
		return configError("policy.determinism.virtual_clock_required_for_replay=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.determinism.virtual_clock_required_for_replay", nil)
	}
	if !policy.Determinism.DeterministicProtoSerialization {
		return configError("policy.determinism.deterministic_proto_serialization=false is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.determinism.deterministic_proto_serialization", nil)
	}
	if policy.Determinism.WallClockInCanonicalOutput {
		return configError("policy.determinism.wall_clock_in_canonical_output=true is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.determinism.wall_clock_in_canonical_output", nil)
	}
	if policy.Safety.RewardValueAllowed {
		return configError("policy.safety.reward_value_allowed=true is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.safety.reward_value_allowed", nil)
	}
	if policy.Safety.StaleIntentAllowed {
		return configError("policy.safety.stale_intent_allowed=true is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.safety.stale_intent_allowed", nil)
	}
	if policy.Safety.StaleSnapshotCommitAllowed {
		return configError("policy.safety.stale_snapshot_commit_allowed=true is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.safety.stale_snapshot_commit_allowed", nil)
	}
	if policy.Safety.PartialFailureIsSuccess {
		return configError("policy.safety.partial_failure_is_success=true is not supported by the authoritative scheduler", "schema_validation", policy.Path, "policy.safety.partial_failure_is_success", nil)
	}
	return nil
}

func deriveFallbackMode(policy PolicyBundle) (string, error) {
	if override := policy.Path; override == "" {
		// No-op; keeps source available for diagnostics below.
	}
	for _, candidate := range policy.Fallback.Order {
		switch normalizeToken(candidate) {
		case "lastvalidintent", "static":
			return "static", nil
		case "noop", "no_op":
			return "noop", nil
		}
	}
	return "", configError(
		fmt.Sprintf("policy.fallback.order does not map to a supported startup fallback: %v", policy.Fallback.Order),
		"schema_validation",
		policy.Path,
		"policy.fallback.order",
		nil,
	)
}

func deriveRuntimeIntervals(env map[string]string) (RuntimeIntervals, error) {
	intervals := RuntimeIntervals{
		Fast:   25 * time.Millisecond,
		Medium: 100 * time.Millisecond,
		Slow:   250 * time.Millisecond,
	}
	fields := []struct {
		key    string
		target *time.Duration
	}{
		{key: DefaultEnvPrefix + "FAST_INTERVAL", target: &intervals.Fast},
		{key: DefaultEnvPrefix + "MEDIUM_INTERVAL", target: &intervals.Medium},
		{key: DefaultEnvPrefix + "SLOW_INTERVAL", target: &intervals.Slow},
	}
	for _, field := range fields {
		value := env[field.key]
		if value == "" {
			continue
		}
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return RuntimeIntervals{}, configError(
				fmt.Sprintf("%s must be a positive Go duration, got %q", field.key, value),
				"schema_validation",
				"",
				field.key,
				nil,
			)
		}
		*field.target = parsed
	}
	return intervals, nil
}

func isSupportedStrategy(strategy string) bool {
	switch normalizeToken(strategy) {
	case "stablefirstfit", "scorefirst", "binpack", "traceaware":
		return true
	default:
		return false
	}
}
