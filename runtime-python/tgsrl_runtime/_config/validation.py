from __future__ import annotations

from pathlib import Path

from .types import (
    _EXPECTED_SCHEMAS,
    _FLOATING_TOKEN_RE,
    _IMAGE_DIGEST_RE,
    _SENSITIVE_TOKEN_RE,
    _UNPINNED_VERSION_RE,
    AppliedOverrides,
    CompatibilityProfile,
    ConfigBundle,
    ConfigError,
    Node,
    PolicyBundle,
    RuntimeBOM,
)


def _mapping(value: Node, path: str) -> dict[str, Node]:
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must be a mapping", field=path)
    return value


def _reject_unknown_fields(value: dict[str, Node], path: str, allowed: set[str]) -> None:
    for key in value:
        if key in allowed:
            continue
        raise ConfigError(f"{path} contains unknown field {key!r}", field=f"{path}.{key}")


def _list(value: Node, path: str) -> list[Node]:
    if not isinstance(value, list):
        raise ConfigError(f"{path} must be a list", field=path)
    return value


def _string(value: Node, path: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{path} must be a string", field=path)
    if not value:
        raise ConfigError(f"{path} must not be empty", field=path)
    return value


def _optional_string(value: Node, path: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{path} must be a string or null", field=path)
    return value


def _int(value: Node, path: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ConfigError(f"{path} must be an integer", field=path)
    return value


def _bool(value: Node, path: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{path} must be a boolean", field=path)
    return value


def _float(value: Node, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f"{path} must be a number", field=path)
    return float(value)


def _string_tuple(value: Node, path: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    items = tuple(_string(item, path) for item in _list(value, path))
    if not items and not allow_empty:
        raise ConfigError(f"{path} must not be empty", field=path)
    return items


def _ensure_pinned_version(version: str, label: str) -> None:
    if _FLOATING_TOKEN_RE.search(version) or _UNPINNED_VERSION_RE.search(version):
        raise ConfigError(f"{label} must be pinned and must not use floating markers: {version!r}")


def _ensure_pinned_image(image: str, label: str) -> None:
    if _FLOATING_TOKEN_RE.search(image):
        raise ConfigError(f"{label} must not use latest or another floating tag")
    remainder = image.rsplit("/", 1)[-1]
    if "@" in remainder:
        return
    if ":" not in remainder:
        raise ConfigError(f"{label} must include an explicit tag")


def _data_origin_supports(data_origin: str | None, data_kind: str) -> bool:
    if data_origin is None:
        return False
    tokens = tuple(part.strip() for part in data_origin.split("-or-"))
    return data_kind in tokens


def _normalize_token(value: str) -> str:
    return value.strip().lower().replace("-", "").replace("_", "").replace(" ", "")


def _runtime_component_value(field: str, value: str, source: Path) -> str:
    normalized = _normalize_token(value)
    if not normalized:
        raise ConfigError(
            f"runtime supervisor does not support {field}={value!r}",
            field=field,
            source=source,
        )
    return normalized


def _runtime_strategy(strategy: str, source: Path) -> str:
    normalized = _normalize_token(strategy)
    if normalized not in {"stablefirstfit", "scorefirst", "binpack", "traceaware"}:
        raise ConfigError(
            f"runtime supervisor does not support policy.selection.strategy={strategy!r}",
            field="policy.selection.strategy",
            source=source,
        )
    return normalized


def _resolve_path(repo_root: Path, base_file: Path, reference: str) -> Path:
    candidate = (repo_root / reference).resolve()
    if candidate.exists():
        return candidate
    return (base_file.parent / reference).resolve()


def _require_existing_path(repo_root: Path, base_file: Path, reference: str, label: str) -> Path:
    resolved = _resolve_path(repo_root, base_file, reference)
    if not resolved.exists():
        raise ConfigError(
            f"{label} does not exist: {reference}", code="missing_reference", field=label
        )
    return resolved


def _resolve_existing_path(repo_root: Path, base_file: Path, reference: str) -> Path:
    resolved = _resolve_path(repo_root, base_file, reference)
    if not resolved.exists():
        raise ConfigError(
            f"referenced config path does not exist: {reference}",
            code="missing_reference",
            source=base_file,
            field=reference,
        )
    return resolved


def _read_env_overrides(prefix: str, env: dict[str, str]) -> AppliedOverrides:
    keys = {
        "manifest_path": prefix + "MANIFEST_PATH",
        "bom_path": prefix + "BOM_PATH",
        "profile_path": prefix + "PROFILE_PATH",
        "capabilities_path": prefix + "CAPABILITIES_PATH",
        "policy_path": prefix + "POLICY_PATH",
        "scenario_path": prefix + "SCENARIO_PATH",
    }
    values = {name: env_value for name, env_key in keys.items() if (env_value := env.get(env_key))}
    redacted_env = {
        env_key: "<redacted>"
        if _SENSITIVE_TOKEN_RE.search(env_key)
        or (isinstance(env_value, str) and _SENSITIVE_TOKEN_RE.search(env_value))
        else str(env_value)
        for env_key, env_value in env.items()
        if env_key.startswith(prefix)
    }
    return AppliedOverrides(
        manifest_path=values.get("manifest_path"),
        bom_path=values.get("bom_path"),
        profile_path=values.get("profile_path"),
        capabilities_path=values.get("capabilities_path"),
        policy_path=values.get("policy_path"),
        scenario_path=values.get("scenario_path"),
        env=redacted_env,
    )


def _expect_schema(actual: str, kind: str, source: Path) -> None:
    expected = _EXPECTED_SCHEMAS[kind]
    if actual != expected:
        raise ConfigError(
            f"{kind} schema_version must be {expected!r}, got {actual!r}",
            code="schema_version_mismatch",
            source=source,
            field="schema_version",
        )


def _validate_bundle(bundle: ConfigBundle) -> None:
    manifest = bundle.manifest
    scenario = bundle.scenario
    profile = bundle.profile
    capabilities = bundle.capabilities
    policy = bundle.policy

    _require_existing_path(
        bundle.root,
        manifest.path,
        manifest.references.patch_ledger,
        "manifest.references.patch_ledger",
    )
    _require_existing_path(
        bundle.root,
        scenario.path,
        scenario.references.compatibility_manifest,
        "scenario.references.compatibility_manifest",
    )
    _require_existing_path(
        bundle.root,
        scenario.path,
        scenario.references.resource_profile,
        "scenario.references.resource_profile",
    )
    _require_existing_path(
        bundle.root,
        scenario.path,
        scenario.references.capabilities,
        "scenario.references.capabilities",
    )
    _require_existing_path(
        bundle.root, scenario.path, scenario.references.policy, "scenario.references.policy"
    )

    if (
        _resolve_path(bundle.root, scenario.path, scenario.references.compatibility_manifest)
        != manifest.path
    ):
        raise ConfigError(
            "scenario.references.compatibility_manifest must resolve to the loaded manifest"
        )
    if (
        _resolve_path(bundle.root, scenario.path, scenario.references.resource_profile)
        != profile.path
    ):
        raise ConfigError("scenario.references.resource_profile must resolve to the loaded profile")
    if (
        _resolve_path(bundle.root, scenario.path, scenario.references.capabilities)
        != capabilities.path
    ):
        raise ConfigError(
            "scenario.references.capabilities must resolve to the loaded capabilities"
        )
    if _resolve_path(bundle.root, scenario.path, scenario.references.policy) != policy.path:
        raise ConfigError("scenario.references.policy must resolve to the loaded policy")

    if manifest.combination.resource_provider.source != profile.provider.source:
        raise ConfigError("provider source mismatch between manifest and profile")
    if manifest.combination.resource_provider.source != capabilities.source:
        raise ConfigError("provider source mismatch between manifest and capabilities")
    if manifest.combination.resource_provider.source != scenario.topology.provider_source:
        raise ConfigError("provider source mismatch between manifest and scenario topology")

    if scenario.data_kind not in manifest.declared_matrix.data_kinds:
        raise ConfigError("scenario data_kind is not declared by the manifest")
    if scenario.data_kind not in profile.runtime_scope.data_kinds:
        raise ConfigError("scenario data_kind is not allowed by the profile")
    if not _data_origin_supports(capabilities.attributes.get("data_origin"), scenario.data_kind):
        raise ConfigError(
            "capabilities.attributes.data_origin is inconsistent with the scenario data_kind"
        )

    if scenario.workload.algorithm not in manifest.declared_matrix.algorithms:
        raise ConfigError("scenario algorithm is not declared by the manifest")
    if scenario.workload.algorithm not in profile.runtime_scope.algorithms:
        raise ConfigError("scenario algorithm is not allowed by the profile")
    if scenario.workload.algorithm not in capabilities.algorithms:
        raise ConfigError("scenario algorithm is not supported by capabilities")

    if scenario.workload.rollout_mode not in manifest.declared_matrix.rollout_modes:
        raise ConfigError("scenario rollout_mode is not declared by the manifest")
    if scenario.workload.rollout_mode not in profile.runtime_scope.rollout_modes:
        raise ConfigError("scenario rollout_mode is not allowed by the profile")
    if scenario.workload.rollout_mode not in capabilities.rollout_modes:
        raise ConfigError("scenario rollout_mode is not supported by capabilities")

    supported_actions = set(capabilities.supported_actions)
    required_lifecycle = set(profile.runtime_scope.lifecycle_actions)
    if not required_lifecycle.issubset(supported_actions):
        raise ConfigError(
            "capabilities.supported_actions must include all profile lifecycle actions"
        )
    if {
        "rebind",
        "recreate",
    } & supported_actions and not policy.actions.require_generation_fence_for_l4:
        raise ConfigError(
            "policy.actions.require_generation_fence_for_l4 must be true "
            "when L4 actions are enabled"
        )
    if policy.actions.unsupported_action != "reject":
        raise ConfigError("policy.actions.unsupported_action must be reject")
    if policy.policy_version != "1":
        raise ConfigError("policy.policy_version must be a pinned concrete version")
    if not scenario.references.compatibility_manifest or not scenario.references.resource_profile:
        raise ConfigError("scenario references must not be empty")


def _validate_bom(bom: RuntimeBOM) -> None:
    if bom.pinning_policy.floating_versions_allowed:
        raise ConfigError("bom.pinning_policy.floating_versions_allowed must be false")
    if not bom.pinning_policy.immutable_image_digest_required_for_release:
        raise ConfigError(
            "bom.pinning_policy.immutable_image_digest_required_for_release must be true"
        )
    for record in (*bom.toolchain, *bom.runtime_dependencies):
        _ensure_pinned_version(record.version, f"{record.name} version")
    for image in bom.development_images:
        _ensure_pinned_image(image.image, f"{image.name} image")
        if not _IMAGE_DIGEST_RE.match(image.image_digest):
            raise ConfigError(f"{image.name} image_digest must be an immutable sha256 digest")


def _validate_profile(profile: CompatibilityProfile) -> None:
    gpu = profile.gpu_management_profile
    if gpu.maximum_active_profiles != 1:
        raise ConfigError("profile.gpu_management_profile.maximum_active_profiles must be 1")
    if len(gpu.enabled_profiles) > 1:
        raise ConfigError(
            "profile.gpu_management_profile.enabled_profiles must contain "
            "at most one active profile"
        )
    if gpu.selected not in gpu.allowed_values:
        raise ConfigError("profile.gpu_management_profile.selected must be in allowed_values")
    if len(gpu.enabled_profiles) == 1 and gpu.enabled_profiles[0] != gpu.selected:
        raise ConfigError("the enabled GPU profile must match gpu_management_profile.selected")
    if len(gpu.enabled_profiles) == 0 and gpu.selected != "none":
        raise ConfigError("gpu_management_profile.selected must be none when no profile is enabled")


def _validate_policy(policy: PolicyBundle) -> None:
    if policy.selection.top_k < 1:
        raise ConfigError("policy.selection.top_k must be at least 1")
    if not policy.selection.tiebreakers:
        raise ConfigError("policy.selection.tiebreakers must not be empty")
    if not policy.fallback.order:
        raise ConfigError("policy.fallback.order must not be empty")
    allowed_fallback = {"last-valid-intent", "no-op", "static"}
    unknown_fallback = [value for value in policy.fallback.order if value not in allowed_fallback]
    if unknown_fallback:
        raise ConfigError(
            f"policy.fallback.order contains unsupported fallback values: {unknown_fallback}"
        )
    if not policy.actions.require_idempotency_key:
        raise ConfigError("policy.actions.require_idempotency_key must be true")
    if not policy.actions.require_explicit_rollback:
        raise ConfigError("policy.actions.require_explicit_rollback must be true")
    if policy.actions.unsupported_action not in {"reject"}:
        raise ConfigError("policy.actions.unsupported_action must be reject")
