from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

type Scalar = str | int | float | bool | None
type Node = Scalar | list["Node"] | dict[str, "Node"]

DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST = "compatibility/manifests/cpu-mock.yaml"
DEFAULT_ENV_PREFIX = "TGSRL_CONFIG_"

_IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FLOATING_TOKEN_RE = re.compile(
    r"(?i)(?:^|[^a-z])(latest|stable|main|master|head|edge)(?:[^a-z]|$)"
)
_UNPINNED_VERSION_RE = re.compile(r"[*xX]|>=|<=|~=|!=|\^")
_SENSITIVE_TOKEN_RE = re.compile(
    r"(?i)(secret|token|password|credential|authorization|cookie|session|key)"
)
_EXPECTED_SCHEMAS = {
    "manifest": "tgsrl.io/compatibility-manifest/v1alpha1",
    "bom": "tgsrl.io/bom/v1alpha1",
    "profile": "tgsrl.io/compatibility-profile/v1alpha1",
    "capabilities": "tgsrl.io/capability-set/v1alpha1",
    "policy": "tgsrl.io/policy-bundle/v1alpha1",
    "scenario": "tgsrl.io/synthetic-scenario/v1alpha1",
}


class ConfigError(ValueError):
    """Raised when a config file cannot be parsed, overridden, or validated."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "config_error",
        source: str | Path | None = None,
        field: str | None = None,
        context: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.source = str(source) if source is not None else None
        self.field = field
        self.context = dict(context or {})

    def diagnostic(self) -> dict[str, object]:
        payload: dict[str, object] = {"code": self.code, "message": self.message}
        if self.source is not None:
            payload["source"] = self.source
        if self.field is not None:
            payload["field"] = self.field
        if self.context:
            payload["context"] = {
                key: _redact_value(key, value) for key, value in self.context.items()
            }
        return payload

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class LoadOptions:
    root: Path = DEFAULT_REPO_ROOT
    manifest_path: str = DEFAULT_MANIFEST
    env_prefix: str = DEFAULT_ENV_PREFIX
    env: Mapping[str, str] | None = None


@dataclass(frozen=True, slots=True)
class ReferenceSet:
    bom: str
    profile: str
    capabilities: str
    policy: str
    representative_scenario: str
    patch_ledger: str


@dataclass(frozen=True, slots=True)
class ResourceProviderRef:
    name: str
    source: str


@dataclass(frozen=True, slots=True)
class KubernetesRef:
    enabled: bool
    version: str | None


@dataclass(frozen=True, slots=True)
class ComponentRef:
    name: str
    version: str | None


@dataclass(frozen=True, slots=True)
class Combination:
    framework: ComponentRef
    execution_backend: ComponentRef
    trainer: ComponentRef
    rollout_engine: ComponentRef
    resource_provider: ResourceProviderRef
    kubernetes: KubernetesRef
    gpu_management_profile: str


@dataclass(frozen=True, slots=True)
class DeclaredMatrix:
    algorithms: tuple[str, ...]
    rollout_modes: tuple[str, ...]
    data_kinds: tuple[str, ...]
    evidence_status: str
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CompatibilityManifest:
    path: Path
    schema_version: str
    manifest_id: str
    revision: int
    status: str
    scope: str
    references: ReferenceSet
    combination: Combination
    declared_matrix: DeclaredMatrix


@dataclass(frozen=True, slots=True)
class PinningPolicy:
    floating_versions_allowed: bool
    immutable_image_digest_required_for_release: bool
    unknown_commit_or_digest_value: str | None
    note: str


@dataclass(frozen=True, slots=True)
class DependencyRecord:
    name: str
    version: str
    source: str
    source_commit: str | None
    artifact_digest: str | None


@dataclass(frozen=True, slots=True)
class DevelopmentImage:
    name: str
    image: str
    image_digest: str


@dataclass(frozen=True, slots=True)
class RuntimeBOM:
    path: Path
    schema_version: str
    bom_id: str
    revision: int
    status: str
    scope: str
    pinning_policy: PinningPolicy
    toolchain: tuple[DependencyRecord, ...]
    runtime_dependencies: tuple[DependencyRecord, ...]
    development_images: tuple[DevelopmentImage, ...]


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    kind: str
    source: str
    live_hardware: bool
    authoritative_for: str


@dataclass(frozen=True, slots=True)
class RuntimeScope:
    required_host_resources: tuple[str, ...]
    data_kinds: tuple[str, ...]
    algorithms: tuple[str, ...]
    rollout_modes: tuple[str, ...]
    lifecycle_actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GPUManagementProfile:
    selected: str
    allowed_values: tuple[str, ...]
    mutually_exclusive: bool
    maximum_active_profiles: int
    admission_rejects_multiple_profiles: bool
    enabled_profiles: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CompatibilityProfile:
    path: Path
    schema_version: str
    profile_id: str
    revision: int
    status: str
    description: str
    provider: ProviderConfig
    runtime_scope: RuntimeScope
    gpu_management_profile: GPUManagementProfile


@dataclass(frozen=True, slots=True)
class CapabilityClaims:
    mock_only: bool
    live_gpu: bool
    hardware_latency: bool
    performance: bool


@dataclass(frozen=True, slots=True)
class CapabilitySetConfig:
    path: Path
    schema_version: str
    capability_id: str
    revision: int
    source: str
    semantics: str
    verification_status: str
    runtime_loader: bool
    names: tuple[str, ...]
    attributes: dict[str, str]
    algorithms: tuple[str, ...]
    rollout_modes: tuple[str, ...]
    supported_actions: tuple[str, ...]
    claims: CapabilityClaims


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    strategy: str
    tiebreakers: tuple[str, ...]
    top_k: int


@dataclass(frozen=True, slots=True)
class FallbackPolicy:
    order: tuple[str, ...]
    permit_binpack: bool
    reason_required: bool


@dataclass(frozen=True, slots=True)
class ActionPolicy:
    unsupported_action: str
    require_idempotency_key: bool
    require_explicit_rollback: bool
    require_generation_fence_for_l4: bool


@dataclass(frozen=True, slots=True)
class ConstraintPolicy:
    require_capacity: bool
    require_capability: bool
    require_safe_point: bool
    require_current_intent: bool
    require_snapshot_revision_match: bool
    reject_unknown_capability: bool


@dataclass(frozen=True, slots=True)
class ProtectionPolicy:
    enabled: bool
    cooldown: str
    hysteresis: float
    max_actions_per_window: int
    window: str
    breaker_threshold: int
    breaker_reset_after: str


@dataclass(frozen=True, slots=True)
class PreemptionPolicy:
    enabled: bool
    strategy: str
    require_safe_point: bool


@dataclass(frozen=True, slots=True)
class DeterminismPolicy:
    explicit_seed_required: bool
    virtual_clock_required_for_replay: bool
    deterministic_proto_serialization: bool
    wall_clock_in_canonical_output: bool


@dataclass(frozen=True, slots=True)
class SafetyPolicy:
    reward_value_allowed: bool
    stale_intent_allowed: bool
    stale_snapshot_commit_allowed: bool
    partial_failure_is_success: bool


@dataclass(frozen=True, slots=True)
class PolicyBundle:
    path: Path
    schema_version: str
    policy_id: str
    policy_version: str
    status: str
    runtime_loader: bool
    description: str
    selection: SelectionPolicy
    constraints: ConstraintPolicy
    fallback: FallbackPolicy
    actions: ActionPolicy
    protection: ProtectionPolicy
    preemption: PreemptionPolicy
    determinism: DeterminismPolicy
    safety: SafetyPolicy


@dataclass(frozen=True, slots=True)
class ScenarioReferences:
    compatibility_manifest: str
    resource_profile: str
    capabilities: str
    policy: str


@dataclass(frozen=True, slots=True)
class ScenarioWorkload:
    algorithm: str
    rollout_mode: str
    execution_id: str
    stage_id: str
    unit_count: int


@dataclass(frozen=True, slots=True)
class Topology:
    provider_source: str


@dataclass(frozen=True, slots=True)
class SyntheticScenarioConfig:
    path: Path
    schema_version: str
    scenario_id: str
    scenario_revision: int
    status: str
    runtime_loader: bool
    data_kind: str
    seed: int
    references: ScenarioReferences
    workload: ScenarioWorkload
    topology: Topology


@dataclass(frozen=True, slots=True)
class AppliedOverrides:
    manifest_path: str | None = None
    bom_path: str | None = None
    profile_path: str | None = None
    capabilities_path: str | None = None
    policy_path: str | None = None
    scenario_path: str | None = None
    env: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConfigBundle:
    root: Path
    manifest: CompatibilityManifest
    bom: RuntimeBOM
    profile: CompatibilityProfile
    capabilities: CapabilitySetConfig
    policy: PolicyBundle
    scenario: SyntheticScenarioConfig
    overrides: AppliedOverrides


@dataclass(frozen=True, slots=True)
class RuntimeProjection:
    framework: str
    execution_backend: str
    trainer: str
    rollout_engine: str
    compatibility_profile: str
    provider_kind: str
    provider_source: str
    selection_strategy: str
    policy_version: str
    data_kind: str
    algorithm: str
    rollout_mode: str
    desired_units: int


def _redact_value(key: str, value: object) -> object:
    if value is None:
        return None
    if _SENSITIVE_TOKEN_RE.search(key):
        return "<redacted>"
    if isinstance(value, str) and _SENSITIVE_TOKEN_RE.search(value):
        return "<redacted>"
    return value
