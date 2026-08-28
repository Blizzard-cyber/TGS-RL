package config

func LoadPolicy(path string) (*PolicyBundle, error) {
	raw, err := loadYAMLSubset(path)
	if err != nil {
		return nil, err
	}
	data, err := mapping(raw, "policy")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(data, "policy", "schema_version", "policy_id", "policy_version", "status", "runtime_loader", "description", "selection", "constraints", "fallback", "actions", "protection", "preemption", "determinism", "safety"); err != nil {
		return nil, err
	}
	selection, err := mapping(data["selection"], "policy.selection")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(selection, "policy.selection", "strategy", "tiebreakers", "top_k"); err != nil {
		return nil, err
	}
	constraintsConfig, err := mapping(data["constraints"], "policy.constraints")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(constraintsConfig, "policy.constraints", "require_capacity", "require_capability", "require_safe_point", "require_current_intent", "require_snapshot_revision_match", "reject_unknown_capability"); err != nil {
		return nil, err
	}
	fallback, err := mapping(data["fallback"], "policy.fallback")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(fallback, "policy.fallback", "order", "permit_binpack", "reason_required"); err != nil {
		return nil, err
	}
	actions, err := mapping(data["actions"], "policy.actions")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(actions, "policy.actions", "unsupported_action", "require_idempotency_key", "require_explicit_rollback", "require_generation_fence_for_l4"); err != nil {
		return nil, err
	}
	protectionConfig, err := mapping(data["protection"], "policy.protection")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(protectionConfig, "policy.protection", "enabled", "cooldown", "hysteresis", "max_actions_per_window", "window", "breaker_threshold", "breaker_reset_after"); err != nil {
		return nil, err
	}
	preemptionConfig, err := mapping(data["preemption"], "policy.preemption")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(preemptionConfig, "policy.preemption", "enabled", "strategy", "require_safe_point"); err != nil {
		return nil, err
	}
	determinismConfig, err := mapping(data["determinism"], "policy.determinism")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(determinismConfig, "policy.determinism", "explicit_seed_required", "virtual_clock_required_for_replay", "deterministic_proto_serialization", "wall_clock_in_canonical_output"); err != nil {
		return nil, err
	}
	safetyConfig, err := mapping(data["safety"], "policy.safety")
	if err != nil {
		return nil, err
	}
	if err := rejectUnknownFields(safetyConfig, "policy.safety", "reward_value_allowed", "stale_intent_allowed", "stale_snapshot_commit_allowed", "partial_failure_is_success"); err != nil {
		return nil, err
	}
	tiebreakers, err := stringSlice(selection["tiebreakers"], "policy.selection.tiebreakers", false)
	if err != nil {
		return nil, err
	}
	order, err := stringSlice(fallback["order"], "policy.fallback.order", false)
	if err != nil {
		return nil, err
	}
	schemaVersion, err := mustString(data["schema_version"], "policy.schema_version")
	if err != nil {
		return nil, err
	}
	policyID, err := mustString(data["policy_id"], "policy.policy_id")
	if err != nil {
		return nil, err
	}
	policyVersion, err := mustString(data["policy_version"], "policy.policy_version")
	if err != nil {
		return nil, err
	}
	status, err := mustString(data["status"], "policy.status")
	if err != nil {
		return nil, err
	}
	runtimeLoader, err := mustBool(data["runtime_loader"], "policy.runtime_loader")
	if err != nil {
		return nil, err
	}
	description, err := mustString(data["description"], "policy.description")
	if err != nil {
		return nil, err
	}
	strategy, err := mustString(selection["strategy"], "policy.selection.strategy")
	if err != nil {
		return nil, err
	}
	topK, err := mustInt(selection["top_k"], "policy.selection.top_k")
	if err != nil {
		return nil, err
	}
	requireCapacity, err := mustBool(constraintsConfig["require_capacity"], "policy.constraints.require_capacity")
	if err != nil {
		return nil, err
	}
	requireCapability, err := mustBool(constraintsConfig["require_capability"], "policy.constraints.require_capability")
	if err != nil {
		return nil, err
	}
	constraintsRequireSafePoint, err := mustBool(constraintsConfig["require_safe_point"], "policy.constraints.require_safe_point")
	if err != nil {
		return nil, err
	}
	requireCurrentIntent, err := mustBool(constraintsConfig["require_current_intent"], "policy.constraints.require_current_intent")
	if err != nil {
		return nil, err
	}
	requireSnapshotRevisionMatch, err := mustBool(constraintsConfig["require_snapshot_revision_match"], "policy.constraints.require_snapshot_revision_match")
	if err != nil {
		return nil, err
	}
	rejectUnknownCapability, err := mustBool(constraintsConfig["reject_unknown_capability"], "policy.constraints.reject_unknown_capability")
	if err != nil {
		return nil, err
	}
	permitBinpack, err := mustBool(fallback["permit_binpack"], "policy.fallback.permit_binpack")
	if err != nil {
		return nil, err
	}
	reasonRequired, err := mustBool(fallback["reason_required"], "policy.fallback.reason_required")
	if err != nil {
		return nil, err
	}
	unsupportedAction, err := mustString(actions["unsupported_action"], "policy.actions.unsupported_action")
	if err != nil {
		return nil, err
	}
	requireIdempotencyKey, err := mustBool(actions["require_idempotency_key"], "policy.actions.require_idempotency_key")
	if err != nil {
		return nil, err
	}
	requireExplicitRollback, err := mustBool(actions["require_explicit_rollback"], "policy.actions.require_explicit_rollback")
	if err != nil {
		return nil, err
	}
	requireGenerationFenceForL4, err := mustBool(actions["require_generation_fence_for_l4"], "policy.actions.require_generation_fence_for_l4")
	if err != nil {
		return nil, err
	}
	protectionEnabled, err := mustBool(protectionConfig["enabled"], "policy.protection.enabled")
	if err != nil {
		return nil, err
	}
	protectionCooldown, err := mustDuration(protectionConfig["cooldown"], "policy.protection.cooldown")
	if err != nil {
		return nil, err
	}
	protectionHysteresis, err := mustFloat(protectionConfig["hysteresis"], "policy.protection.hysteresis")
	if err != nil {
		return nil, err
	}
	maxActionsPerWindow, err := mustInt(protectionConfig["max_actions_per_window"], "policy.protection.max_actions_per_window")
	if err != nil {
		return nil, err
	}
	protectionWindow, err := mustDuration(protectionConfig["window"], "policy.protection.window")
	if err != nil {
		return nil, err
	}
	breakerThreshold, err := mustInt(protectionConfig["breaker_threshold"], "policy.protection.breaker_threshold")
	if err != nil {
		return nil, err
	}
	breakerResetAfter, err := mustDuration(protectionConfig["breaker_reset_after"], "policy.protection.breaker_reset_after")
	if err != nil {
		return nil, err
	}
	preemptionEnabled, err := mustBool(preemptionConfig["enabled"], "policy.preemption.enabled")
	if err != nil {
		return nil, err
	}
	preemptionStrategy, err := mustString(preemptionConfig["strategy"], "policy.preemption.strategy")
	if err != nil {
		return nil, err
	}
	requireSafePoint, err := mustBool(preemptionConfig["require_safe_point"], "policy.preemption.require_safe_point")
	if err != nil {
		return nil, err
	}
	explicitSeedRequired, err := mustBool(determinismConfig["explicit_seed_required"], "policy.determinism.explicit_seed_required")
	if err != nil {
		return nil, err
	}
	virtualClockRequiredForReplay, err := mustBool(determinismConfig["virtual_clock_required_for_replay"], "policy.determinism.virtual_clock_required_for_replay")
	if err != nil {
		return nil, err
	}
	deterministicProtoSerialization, err := mustBool(determinismConfig["deterministic_proto_serialization"], "policy.determinism.deterministic_proto_serialization")
	if err != nil {
		return nil, err
	}
	wallClockInCanonicalOutput, err := mustBool(determinismConfig["wall_clock_in_canonical_output"], "policy.determinism.wall_clock_in_canonical_output")
	if err != nil {
		return nil, err
	}
	rewardValueAllowed, err := mustBool(safetyConfig["reward_value_allowed"], "policy.safety.reward_value_allowed")
	if err != nil {
		return nil, err
	}
	staleIntentAllowed, err := mustBool(safetyConfig["stale_intent_allowed"], "policy.safety.stale_intent_allowed")
	if err != nil {
		return nil, err
	}
	staleSnapshotCommitAllowed, err := mustBool(safetyConfig["stale_snapshot_commit_allowed"], "policy.safety.stale_snapshot_commit_allowed")
	if err != nil {
		return nil, err
	}
	partialFailureIsSuccess, err := mustBool(safetyConfig["partial_failure_is_success"], "policy.safety.partial_failure_is_success")
	if err != nil {
		return nil, err
	}
	policy := &PolicyBundle{
		Path:          absPath(path),
		SchemaVersion: schemaVersion,
		PolicyID:      policyID,
		PolicyVersion: policyVersion,
		Status:        status,
		RuntimeLoader: runtimeLoader,
		Description:   description,
		Selection: SelectionPolicy{
			Strategy:    strategy,
			Tiebreakers: tiebreakers,
			TopK:        topK,
		},
		Constraints: ConstraintPolicy{
			RequireCapacity:              requireCapacity,
			RequireCapability:            requireCapability,
			RequireSafePoint:             constraintsRequireSafePoint,
			RequireCurrentIntent:         requireCurrentIntent,
			RequireSnapshotRevisionMatch: requireSnapshotRevisionMatch,
			RejectUnknownCapability:      rejectUnknownCapability,
		},
		Fallback: FallbackPolicy{
			Order:          order,
			PermitBinpack:  permitBinpack,
			ReasonRequired: reasonRequired,
		},
		Actions: ActionPolicy{
			UnsupportedAction:           unsupportedAction,
			RequireIdempotencyKey:       requireIdempotencyKey,
			RequireExplicitRollback:     requireExplicitRollback,
			RequireGenerationFenceForL4: requireGenerationFenceForL4,
		},
		Protection: ProtectionPolicy{
			Enabled:             protectionEnabled,
			Cooldown:            protectionCooldown,
			Hysteresis:          protectionHysteresis,
			MaxActionsPerWindow: maxActionsPerWindow,
			Window:              protectionWindow,
			BreakerThreshold:    breakerThreshold,
			BreakerResetAfter:   breakerResetAfter,
		},
		Preemption: PreemptionPolicy{
			Enabled:          preemptionEnabled,
			Strategy:         preemptionStrategy,
			RequireSafePoint: requireSafePoint,
		},
		Determinism: DeterminismPolicy{
			ExplicitSeedRequired:            explicitSeedRequired,
			VirtualClockRequiredForReplay:   virtualClockRequiredForReplay,
			DeterministicProtoSerialization: deterministicProtoSerialization,
			WallClockInCanonicalOutput:      wallClockInCanonicalOutput,
		},
		Safety: SafetyPolicy{
			RewardValueAllowed:         rewardValueAllowed,
			StaleIntentAllowed:         staleIntentAllowed,
			StaleSnapshotCommitAllowed: staleSnapshotCommitAllowed,
			PartialFailureIsSuccess:    partialFailureIsSuccess,
		},
	}
	if err := expectSchema(policy.SchemaVersion, "policy", policy.Path); err != nil {
		return nil, err
	}
	if err := validatePolicy(policy); err != nil {
		return nil, err
	}
	return policy, nil
}
