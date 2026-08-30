from __future__ import annotations

import os
from pathlib import Path

from .parser import _load_yaml_subset
from .types import (
    DEFAULT_ENV_PREFIX,
    DEFAULT_MANIFEST,
    DEFAULT_REPO_ROOT,
    ActionPolicy,
    CapabilityClaims,
    CapabilitySetConfig,
    Combination,
    CompatibilityManifest,
    CompatibilityProfile,
    ComponentRef,
    ConfigBundle,
    ConstraintPolicy,
    DeclaredMatrix,
    DependencyRecord,
    DeterminismPolicy,
    DevelopmentImage,
    FallbackPolicy,
    GPUManagementProfile,
    KubernetesRef,
    LoadOptions,
    Node,
    PinningPolicy,
    PolicyBundle,
    PreemptionPolicy,
    ProtectionPolicy,
    ProviderConfig,
    ReferenceSet,
    ResourceProviderRef,
    RuntimeBOM,
    RuntimeProjection,
    RuntimeScope,
    SafetyPolicy,
    ScenarioReferences,
    ScenarioWorkload,
    SelectionPolicy,
    SyntheticScenarioConfig,
    Topology,
)
from .validation import (
    _bool,
    _expect_schema,
    _float,
    _int,
    _list,
    _mapping,
    _optional_string,
    _read_env_overrides,
    _reject_unknown_fields,
    _resolve_existing_path,
    _runtime_component_value,
    _runtime_strategy,
    _string,
    _string_tuple,
    _validate_bom,
    _validate_bundle,
    _validate_policy,
    _validate_profile,
)


def load_bundle(
    root: str | Path = DEFAULT_REPO_ROOT,
    manifest_path: str = DEFAULT_MANIFEST,
    *,
    env_prefix: str = DEFAULT_ENV_PREFIX,
    env: dict[str, str] | None = None,
) -> ConfigBundle:
    return load_bundle_with_options(
        LoadOptions(root=Path(root), manifest_path=manifest_path, env_prefix=env_prefix, env=env)
    )


def load_bundle_with_options(options: LoadOptions) -> ConfigBundle:
    repo_root = Path(options.root).resolve()
    env_values = dict(os.environ if options.env is None else options.env)
    overrides = _read_env_overrides(options.env_prefix, env_values)
    manifest_ref = overrides.manifest_path or options.manifest_path
    manifest = load_manifest(
        _resolve_existing_path(repo_root, repo_root / manifest_ref, manifest_ref)
    )
    bom_ref = overrides.bom_path or manifest.references.bom
    profile_ref = overrides.profile_path or manifest.references.profile
    capabilities_ref = overrides.capabilities_path or manifest.references.capabilities
    policy_ref = overrides.policy_path or manifest.references.policy
    scenario_ref = overrides.scenario_path or manifest.references.representative_scenario
    bom = load_bom(_resolve_existing_path(repo_root, manifest.path, bom_ref))
    profile = load_profile(_resolve_existing_path(repo_root, manifest.path, profile_ref))
    capabilities = load_capabilities(
        _resolve_existing_path(repo_root, manifest.path, capabilities_ref)
    )
    policy = load_policy(_resolve_existing_path(repo_root, manifest.path, policy_ref))
    scenario = load_scenario(_resolve_existing_path(repo_root, manifest.path, scenario_ref))
    bundle = ConfigBundle(
        root=repo_root,
        manifest=manifest,
        bom=bom,
        profile=profile,
        capabilities=capabilities,
        policy=policy,
        scenario=scenario,
        overrides=overrides,
    )
    _validate_bundle(bundle)
    return bundle


def runtime_projection(bundle: ConfigBundle) -> RuntimeProjection:
    selection_strategy = _runtime_strategy(bundle.policy.selection.strategy, bundle.policy.path)
    return RuntimeProjection(
        framework=_runtime_component_value(
            "framework",
            bundle.manifest.combination.framework.name,
            bundle.manifest.path,
        ),
        execution_backend=_runtime_component_value(
            "execution_backend",
            bundle.manifest.combination.execution_backend.name,
            bundle.manifest.path,
        ),
        trainer=_runtime_component_value(
            "trainer",
            bundle.manifest.combination.trainer.name,
            bundle.manifest.path,
        ),
        rollout_engine=_runtime_component_value(
            "rollout_engine",
            bundle.manifest.combination.rollout_engine.name,
            bundle.manifest.path,
        ),
        compatibility_profile=bundle.profile.profile_id,
        provider_kind=bundle.profile.provider.kind,
        provider_source=bundle.profile.provider.source,
        selection_strategy=selection_strategy,
        policy_version=bundle.policy.policy_version,
        data_kind=bundle.scenario.data_kind,
        algorithm=bundle.scenario.workload.algorithm,
        rollout_mode=bundle.scenario.workload.rollout_mode,
        desired_units=bundle.scenario.workload.unit_count,
    )


def load_manifest(path: str | Path) -> CompatibilityManifest:
    raw = _load_yaml_subset(path)
    data = _mapping(raw, "manifest")
    references = _mapping(data["references"], "manifest.references")
    combination = _mapping(data["combination"], "manifest.combination")
    matrix = _mapping(data["declared_matrix"], "manifest.declared_matrix")
    manifest = CompatibilityManifest(
        path=Path(path).resolve(),
        schema_version=_string(data["schema_version"], "manifest.schema_version"),
        manifest_id=_string(data["manifest_id"], "manifest.manifest_id"),
        revision=_int(data["revision"], "manifest.revision"),
        status=_string(data["status"], "manifest.status"),
        scope=_string(data["scope"], "manifest.scope"),
        references=ReferenceSet(
            bom=_string(references["bom"], "manifest.references.bom"),
            profile=_string(references["profile"], "manifest.references.profile"),
            capabilities=_string(references["capabilities"], "manifest.references.capabilities"),
            policy=_string(references["policy"], "manifest.references.policy"),
            representative_scenario=_string(
                references["representative_scenario"], "manifest.references.representative_scenario"
            ),
            patch_ledger=_string(references["patch_ledger"], "manifest.references.patch_ledger"),
        ),
        combination=Combination(
            framework=_component_ref(combination["framework"], "manifest.combination.framework"),
            execution_backend=_component_ref(
                combination["execution_backend"], "manifest.combination.execution_backend"
            ),
            trainer=_component_ref(combination["trainer"], "manifest.combination.trainer"),
            rollout_engine=_component_ref(
                combination["rollout_engine"], "manifest.combination.rollout_engine"
            ),
            resource_provider=_resource_provider_ref(
                combination["resource_provider"], "manifest.combination.resource_provider"
            ),
            kubernetes=_kubernetes_ref(
                combination["kubernetes"], "manifest.combination.kubernetes"
            ),
            gpu_management_profile=_string(
                combination["gpu_management_profile"], "manifest.combination.gpu_management_profile"
            ),
        ),
        declared_matrix=DeclaredMatrix(
            algorithms=_string_tuple(matrix["algorithms"], "manifest.declared_matrix.algorithms"),
            rollout_modes=_string_tuple(
                matrix["rollout_modes"], "manifest.declared_matrix.rollout_modes"
            ),
            data_kinds=_string_tuple(matrix["data_kinds"], "manifest.declared_matrix.data_kinds"),
            evidence_status=_string(
                matrix["evidence_status"], "manifest.declared_matrix.evidence_status"
            ),
            evidence=_string_tuple(matrix["evidence"], "manifest.declared_matrix.evidence"),
        ),
    )
    _expect_schema(manifest.schema_version, "manifest", manifest.path)
    return manifest


def load_bom(path: str | Path) -> RuntimeBOM:
    raw = _load_yaml_subset(path)
    data = _mapping(raw, "bom")
    pinning = _mapping(data["pinning_policy"], "bom.pinning_policy")
    toolchain = tuple(
        _dependency_record(item, "bom.toolchain")
        for item in _list(data["toolchain"], "bom.toolchain")
    )
    runtime_dependencies = tuple(
        _dependency_record(item, "bom.runtime_dependencies")
        for item in _list(data["runtime_dependencies"], "bom.runtime_dependencies")
    )
    development_images = tuple(
        _development_image(item, "bom.development_images")
        for item in _list(data["development_images"], "bom.development_images")
    )
    bom = RuntimeBOM(
        path=Path(path).resolve(),
        schema_version=_string(data["schema_version"], "bom.schema_version"),
        bom_id=_string(data["bom_id"], "bom.bom_id"),
        revision=_int(data["revision"], "bom.revision"),
        status=_string(data["status"], "bom.status"),
        scope=_string(data["scope"], "bom.scope"),
        pinning_policy=PinningPolicy(
            floating_versions_allowed=_bool(
                pinning["floating_versions_allowed"], "bom.pinning_policy.floating_versions_allowed"
            ),
            immutable_image_digest_required_for_release=_bool(
                pinning["immutable_image_digest_required_for_release"],
                "bom.pinning_policy.immutable_image_digest_required_for_release",
            ),
            unknown_commit_or_digest_value=_optional_string(
                pinning["unknown_commit_or_digest_value"],
                "bom.pinning_policy.unknown_commit_or_digest_value",
            ),
            note=_string(pinning["note"], "bom.pinning_policy.note"),
        ),
        toolchain=toolchain,
        runtime_dependencies=runtime_dependencies,
        development_images=development_images,
    )
    _expect_schema(bom.schema_version, "bom", bom.path)
    _validate_bom(bom)
    return bom


def load_profile(path: str | Path) -> CompatibilityProfile:
    raw = _load_yaml_subset(path)
    data = _mapping(raw, "profile")
    provider = _mapping(data["provider"], "profile.provider")
    runtime_scope = _mapping(data["runtime_scope"], "profile.runtime_scope")
    gpu = _mapping(data["gpu_management_profile"], "profile.gpu_management_profile")
    profile = CompatibilityProfile(
        path=Path(path).resolve(),
        schema_version=_string(data["schema_version"], "profile.schema_version"),
        profile_id=_string(data["profile_id"], "profile.profile_id"),
        revision=_int(data["revision"], "profile.revision"),
        status=_string(data["status"], "profile.status"),
        description=_string(data["description"], "profile.description"),
        provider=ProviderConfig(
            kind=_string(provider["kind"], "profile.provider.kind"),
            source=_string(provider["source"], "profile.provider.source"),
            live_hardware=_bool(provider["live_hardware"], "profile.provider.live_hardware"),
            authoritative_for=_string(
                provider["authoritative_for"], "profile.provider.authoritative_for"
            ),
        ),
        runtime_scope=RuntimeScope(
            required_host_resources=_string_tuple(
                runtime_scope["required_host_resources"],
                "profile.runtime_scope.required_host_resources",
            ),
            data_kinds=_string_tuple(
                runtime_scope["data_kinds"], "profile.runtime_scope.data_kinds"
            ),
            algorithms=_string_tuple(
                runtime_scope["algorithms"], "profile.runtime_scope.algorithms"
            ),
            rollout_modes=_string_tuple(
                runtime_scope["rollout_modes"], "profile.runtime_scope.rollout_modes"
            ),
            lifecycle_actions=_string_tuple(
                runtime_scope["lifecycle_actions"], "profile.runtime_scope.lifecycle_actions"
            ),
        ),
        gpu_management_profile=GPUManagementProfile(
            selected=_string(gpu["selected"], "profile.gpu_management_profile.selected"),
            allowed_values=_string_tuple(
                gpu["allowed_values"], "profile.gpu_management_profile.allowed_values"
            ),
            mutually_exclusive=_bool(
                gpu["mutually_exclusive"], "profile.gpu_management_profile.mutually_exclusive"
            ),
            maximum_active_profiles=_int(
                gpu["maximum_active_profiles"],
                "profile.gpu_management_profile.maximum_active_profiles",
            ),
            admission_rejects_multiple_profiles=_bool(
                gpu["admission_rejects_multiple_profiles"],
                "profile.gpu_management_profile.admission_rejects_multiple_profiles",
            ),
            enabled_profiles=_string_tuple(
                gpu["enabled_profiles"],
                "profile.gpu_management_profile.enabled_profiles",
                allow_empty=True,
            ),
        ),
    )
    _expect_schema(profile.schema_version, "profile", profile.path)
    _validate_profile(profile)
    return profile


def load_capabilities(path: str | Path) -> CapabilitySetConfig:
    raw = _load_yaml_subset(path)
    data = _mapping(raw, "capabilities")
    claims = _mapping(data["claims"], "capabilities.claims")
    attributes = {
        key: _string(value, f"capabilities.attributes.{key}")
        for key, value in _mapping(data["attributes"], "capabilities.attributes").items()
    }
    capability = CapabilitySetConfig(
        path=Path(path).resolve(),
        schema_version=_string(data["schema_version"], "capabilities.schema_version"),
        capability_id=_string(data["capability_id"], "capabilities.capability_id"),
        revision=_int(data["revision"], "capabilities.revision"),
        source=_string(data["source"], "capabilities.source"),
        semantics=_string(data["semantics"], "capabilities.semantics"),
        verification_status=_string(
            data["verification_status"], "capabilities.verification_status"
        ),
        runtime_loader=_bool(data["runtime_loader"], "capabilities.runtime_loader"),
        names=_string_tuple(data["names"], "capabilities.names"),
        attributes=attributes,
        algorithms=_string_tuple(data["algorithms"], "capabilities.algorithms"),
        rollout_modes=_string_tuple(data["rollout_modes"], "capabilities.rollout_modes"),
        supported_actions=_string_tuple(
            data["supported_actions"], "capabilities.supported_actions"
        ),
        claims=CapabilityClaims(
            mock_only=_bool(claims["mock_only"], "capabilities.claims.mock_only"),
            live_gpu=_bool(claims["live_gpu"], "capabilities.claims.live_gpu"),
            hardware_latency=_bool(
                claims["hardware_latency"], "capabilities.claims.hardware_latency"
            ),
            performance=_bool(claims["performance"], "capabilities.claims.performance"),
        ),
    )
    _expect_schema(capability.schema_version, "capabilities", capability.path)
    return capability


def load_policy(path: str | Path) -> PolicyBundle:
    raw = _load_yaml_subset(path)
    data = _mapping(raw, "policy")
    _reject_unknown_fields(
        data,
        "policy",
        {
            "schema_version",
            "policy_id",
            "policy_version",
            "status",
            "runtime_loader",
            "description",
            "selection",
            "constraints",
            "fallback",
            "actions",
            "protection",
            "preemption",
            "determinism",
            "safety",
        },
    )
    selection = _mapping(data["selection"], "policy.selection")
    _reject_unknown_fields(selection, "policy.selection", {"strategy", "tiebreakers", "top_k"})
    constraints = _mapping(data["constraints"], "policy.constraints")
    _reject_unknown_fields(
        constraints,
        "policy.constraints",
        {
            "require_capacity",
            "require_capability",
            "require_safe_point",
            "require_current_intent",
            "require_snapshot_revision_match",
            "reject_unknown_capability",
        },
    )
    fallback = _mapping(data["fallback"], "policy.fallback")
    _reject_unknown_fields(
        fallback, "policy.fallback", {"order", "permit_binpack", "reason_required"}
    )
    actions = _mapping(data["actions"], "policy.actions")
    _reject_unknown_fields(
        actions,
        "policy.actions",
        {
            "unsupported_action",
            "require_idempotency_key",
            "require_explicit_rollback",
            "require_generation_fence_for_l4",
            "max_actions_per_tick",
            "max_affected_sandboxes",
            "max_gpu_reconfigurations",
            "max_recovery_cost_nanos",
            "disable_l4",
        },
    )
    protection = _mapping(data["protection"], "policy.protection")
    _reject_unknown_fields(
        protection,
        "policy.protection",
        {
            "enabled",
            "cooldown",
            "hysteresis",
            "max_actions_per_window",
            "window",
            "breaker_threshold",
            "breaker_reset_after",
        },
    )
    preemption = _mapping(data["preemption"], "policy.preemption")
    _reject_unknown_fields(
        preemption, "policy.preemption", {"enabled", "strategy", "require_safe_point"}
    )
    determinism = _mapping(data["determinism"], "policy.determinism")
    _reject_unknown_fields(
        determinism,
        "policy.determinism",
        {
            "explicit_seed_required",
            "virtual_clock_required_for_replay",
            "deterministic_proto_serialization",
            "wall_clock_in_canonical_output",
        },
    )
    safety = _mapping(data["safety"], "policy.safety")
    _reject_unknown_fields(
        safety,
        "policy.safety",
        {
            "reward_value_allowed",
            "stale_intent_allowed",
            "stale_snapshot_commit_allowed",
            "partial_failure_is_success",
        },
    )
    policy = PolicyBundle(
        path=Path(path).resolve(),
        schema_version=_string(data["schema_version"], "policy.schema_version"),
        policy_id=_string(data["policy_id"], "policy.policy_id"),
        policy_version=_string(data["policy_version"], "policy.policy_version"),
        status=_string(data["status"], "policy.status"),
        runtime_loader=_bool(data["runtime_loader"], "policy.runtime_loader"),
        description=_string(data["description"], "policy.description"),
        selection=SelectionPolicy(
            strategy=_string(selection["strategy"], "policy.selection.strategy"),
            tiebreakers=_string_tuple(selection["tiebreakers"], "policy.selection.tiebreakers"),
            top_k=_int(selection["top_k"], "policy.selection.top_k"),
        ),
        constraints=ConstraintPolicy(
            require_capacity=_bool(
                constraints["require_capacity"], "policy.constraints.require_capacity"
            ),
            require_capability=_bool(
                constraints["require_capability"], "policy.constraints.require_capability"
            ),
            require_safe_point=_bool(
                constraints["require_safe_point"], "policy.constraints.require_safe_point"
            ),
            require_current_intent=_bool(
                constraints["require_current_intent"], "policy.constraints.require_current_intent"
            ),
            require_snapshot_revision_match=_bool(
                constraints["require_snapshot_revision_match"],
                "policy.constraints.require_snapshot_revision_match",
            ),
            reject_unknown_capability=_bool(
                constraints["reject_unknown_capability"],
                "policy.constraints.reject_unknown_capability",
            ),
        ),
        fallback=FallbackPolicy(
            order=_string_tuple(fallback["order"], "policy.fallback.order"),
            permit_binpack=_bool(fallback["permit_binpack"], "policy.fallback.permit_binpack"),
            reason_required=_bool(fallback["reason_required"], "policy.fallback.reason_required"),
        ),
        actions=ActionPolicy(
            unsupported_action=_string(
                actions["unsupported_action"], "policy.actions.unsupported_action"
            ),
            require_idempotency_key=_bool(
                actions["require_idempotency_key"], "policy.actions.require_idempotency_key"
            ),
            require_explicit_rollback=_bool(
                actions["require_explicit_rollback"], "policy.actions.require_explicit_rollback"
            ),
            require_generation_fence_for_l4=_bool(
                actions["require_generation_fence_for_l4"],
                "policy.actions.require_generation_fence_for_l4",
            ),
            max_actions_per_tick=_int(
                actions.get("max_actions_per_tick", 1),
                "policy.actions.max_actions_per_tick",
            ),
            max_affected_sandboxes=_int(
                actions.get("max_affected_sandboxes", 1),
                "policy.actions.max_affected_sandboxes",
            ),
            max_gpu_reconfigurations=_int(
                actions.get("max_gpu_reconfigurations", 1),
                "policy.actions.max_gpu_reconfigurations",
            ),
            max_recovery_cost_nanos=_int(
                actions.get("max_recovery_cost_nanos", 2**63 - 1),
                "policy.actions.max_recovery_cost_nanos",
            ),
            disable_l4=_bool(actions.get("disable_l4", False), "policy.actions.disable_l4"),
        ),
        protection=ProtectionPolicy(
            enabled=_bool(protection["enabled"], "policy.protection.enabled"),
            cooldown=_string(protection["cooldown"], "policy.protection.cooldown"),
            hysteresis=_float(protection["hysteresis"], "policy.protection.hysteresis"),
            max_actions_per_window=_int(
                protection["max_actions_per_window"], "policy.protection.max_actions_per_window"
            ),
            window=_string(protection["window"], "policy.protection.window"),
            breaker_threshold=_int(
                protection["breaker_threshold"], "policy.protection.breaker_threshold"
            ),
            breaker_reset_after=_string(
                protection["breaker_reset_after"], "policy.protection.breaker_reset_after"
            ),
        ),
        preemption=PreemptionPolicy(
            enabled=_bool(preemption["enabled"], "policy.preemption.enabled"),
            strategy=_string(preemption["strategy"], "policy.preemption.strategy"),
            require_safe_point=_bool(
                preemption["require_safe_point"], "policy.preemption.require_safe_point"
            ),
        ),
        determinism=DeterminismPolicy(
            explicit_seed_required=_bool(
                determinism["explicit_seed_required"],
                "policy.determinism.explicit_seed_required",
            ),
            virtual_clock_required_for_replay=_bool(
                determinism["virtual_clock_required_for_replay"],
                "policy.determinism.virtual_clock_required_for_replay",
            ),
            deterministic_proto_serialization=_bool(
                determinism["deterministic_proto_serialization"],
                "policy.determinism.deterministic_proto_serialization",
            ),
            wall_clock_in_canonical_output=_bool(
                determinism["wall_clock_in_canonical_output"],
                "policy.determinism.wall_clock_in_canonical_output",
            ),
        ),
        safety=SafetyPolicy(
            reward_value_allowed=_bool(
                safety["reward_value_allowed"], "policy.safety.reward_value_allowed"
            ),
            stale_intent_allowed=_bool(
                safety["stale_intent_allowed"], "policy.safety.stale_intent_allowed"
            ),
            stale_snapshot_commit_allowed=_bool(
                safety["stale_snapshot_commit_allowed"],
                "policy.safety.stale_snapshot_commit_allowed",
            ),
            partial_failure_is_success=_bool(
                safety["partial_failure_is_success"], "policy.safety.partial_failure_is_success"
            ),
        ),
    )
    _expect_schema(policy.schema_version, "policy", policy.path)
    _validate_policy(policy)
    return policy


def load_scenario(path: str | Path) -> SyntheticScenarioConfig:
    raw = _load_yaml_subset(path)
    data = _mapping(raw, "scenario")
    refs = _mapping(data["references"], "scenario.references")
    workload = _mapping(data["workload"], "scenario.workload")
    topology = _mapping(data["topology"], "scenario.topology")
    scenario = SyntheticScenarioConfig(
        path=Path(path).resolve(),
        schema_version=_string(data["schema_version"], "scenario.schema_version"),
        scenario_id=_string(data["scenario_id"], "scenario.scenario_id"),
        scenario_revision=_int(data["scenario_revision"], "scenario.scenario_revision"),
        status=_string(data["status"], "scenario.status"),
        runtime_loader=_bool(data["runtime_loader"], "scenario.runtime_loader"),
        data_kind=_string(data["data_kind"], "scenario.data_kind"),
        seed=_int(data["seed"], "scenario.seed"),
        references=ScenarioReferences(
            compatibility_manifest=_string(
                refs["compatibility_manifest"], "scenario.references.compatibility_manifest"
            ),
            resource_profile=_string(
                refs["resource_profile"], "scenario.references.resource_profile"
            ),
            capabilities=_string(refs["capabilities"], "scenario.references.capabilities"),
            policy=_string(refs["policy"], "scenario.references.policy"),
        ),
        workload=ScenarioWorkload(
            algorithm=_string(workload["algorithm"], "scenario.workload.algorithm"),
            rollout_mode=_string(workload["rollout_mode"], "scenario.workload.rollout_mode"),
            execution_id=_string(workload["execution_id"], "scenario.workload.execution_id"),
            stage_id=_string(workload["stage_id"], "scenario.workload.stage_id"),
            unit_count=_int(workload["unit_count"], "scenario.workload.unit_count"),
        ),
        topology=Topology(
            provider_source=_string(
                topology["provider_source"], "scenario.topology.provider_source"
            )
        ),
    )
    _expect_schema(scenario.schema_version, "scenario", scenario.path)
    return scenario


def _component_ref(raw: Node, path: str) -> ComponentRef:
    data = _mapping(raw, path)
    return ComponentRef(
        name=_string(data["name"], f"{path}.name"),
        version=_optional_string(data["version"], f"{path}.version"),
    )


def _resource_provider_ref(raw: Node, path: str) -> ResourceProviderRef:
    data = _mapping(raw, path)
    return ResourceProviderRef(
        name=_string(data["name"], f"{path}.name"),
        source=_string(data["source"], f"{path}.source"),
    )


def _kubernetes_ref(raw: Node, path: str) -> KubernetesRef:
    data = _mapping(raw, path)
    return KubernetesRef(
        enabled=_bool(data["enabled"], f"{path}.enabled"),
        version=_optional_string(data["version"], f"{path}.version"),
    )


def _dependency_record(raw: Node, path: str) -> DependencyRecord:
    data = _mapping(raw, path)
    return DependencyRecord(
        name=_string(data["name"], f"{path}.name"),
        version=_string(data["version"], f"{path}.version"),
        source=_string(data["source"], f"{path}.source"),
        source_commit=_optional_string(data.get("source_commit"), f"{path}.source_commit"),
        artifact_digest=_optional_string(data.get("artifact_digest"), f"{path}.artifact_digest"),
    )


def _development_image(raw: Node, path: str) -> DevelopmentImage:
    data = _mapping(raw, path)
    return DevelopmentImage(
        name=_string(data["name"], f"{path}.name"),
        image=_string(data["image"], f"{path}.image"),
        image_digest=_string(data["image_digest"], f"{path}.image_digest"),
    )
