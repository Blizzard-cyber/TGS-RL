"""Deterministic adapter registry and manifest-level runtime compilation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import metadata

from tgsrl.v1 import execution_pb2, job_pb2, resource_pb2, runtime_pb2, trace_pb2

from adapters.compliance.runtime import (
    ExecutionBackendAdapter,
    FrameworkAdapter,
    LifecycleAction,
    ManifestValidationError,
    RolloutEngineAdapter,
    RuntimeAdapterError,
    SupportReport,
    TrainerAdapter,
)
from adapters.contracts import validate_component_version
from adapters.execution import FakeExecutionBackendAdapter, RayExecutionBackendAdapter
from adapters.frameworks import FakeFrameworkAdapter, OpenRLHFFrameworkAdapter, VerlFrameworkAdapter
from adapters.rollout_engines import (
    FakeRolloutEngineAdapter,
    SGLangRolloutEngineAdapter,
    VLLMRolloutEngineAdapter,
)
from adapters.trainers import FakeTrainerAdapter, PyTorchTrainerAdapter


def _clone_manifest(manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
    clone = runtime_pb2.RuntimeManifest()
    clone.CopyFrom(manifest)
    return clone


_COMPONENT_FIELDS: tuple[tuple[str, str, int], ...] = (
    ("framework", "framework_version", execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER),
    (
        "execution_backend",
        "execution_backend_version",
        execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND,
    ),
    ("trainer", "trainer_version", execution_pb2.COMPONENT_KIND_TRAINER),
    (
        "rollout_engine",
        "rollout_engine_version",
        execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE,
    ),
)
_ADAPTER_DISTRIBUTIONS: dict[tuple[int, str], str] = {
    (execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER, "fake"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER, "mock"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER, "verl"): "verl",
    (execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER, "openrlhf"): "openrlhf",
    (execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND, "fake"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND, "mock"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND, "ray"): "ray",
    (execution_pb2.COMPONENT_KIND_TRAINER, "fake"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_TRAINER, "mock"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_TRAINER, "pytorch"): "torch",
    (execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE, "fake"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE, "mock"): "tgsrl-runtime",
    (execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE, "vllm"): "vllm",
    (execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE, "sglang"): "sglang",
}
_RUNTIME_DISTRIBUTION = "tgsrl-runtime"
_DECLARED_COMPONENT_FIELDS: dict[int, str] = {
    kind: name_field for name_field, _version_field, kind in _COMPONENT_FIELDS
}
_DECLARED_SOURCE = "job.runtime"
_DECLARED_PROVENANCE = "declared"
_RESOLVED_SOURCE = "runtime-adapter-registry"
_RESOLVED_PROVENANCE = "observed/resolved"


def _component_version(
    *,
    kind: int,
    name: str,
    version: str,
    observed_at: datetime,
    source: str,
    provenance: str,
    attributes: dict[str, str] | None = None,
) -> execution_pb2.ComponentVersion:
    if observed_at.tzinfo is None:
        raise ValueError("component version observed_at must be timezone-aware")
    component = execution_pb2.ComponentVersion(
        kind=kind,
        name=name,
        version=version,
        source=source,
        revision=1,
        attributes={"provenance": provenance} | (attributes or {}),
    )
    component.observed_at.FromDatetime(observed_at.astimezone(UTC))
    validate_component_version(component)
    return component


def _declared_component_versions(
    job: job_pb2.RLTrainingJob, observed_at: datetime
) -> list[execution_pb2.ComponentVersion]:
    versions: list[execution_pb2.ComponentVersion] = []
    for name_field, version_field, kind in _COMPONENT_FIELDS:
        name = getattr(job.runtime, name_field) or "fake"
        version = getattr(job.runtime, version_field)
        if not version:
            continue
        versions.append(
            _component_version(
                kind=kind,
                name=name,
                version=version,
                observed_at=observed_at,
                source="job.runtime",
                provenance="declared",
                attributes={"field": version_field},
            )
        )
    return versions


def _installed_distribution_version(distribution: str) -> str | None:
    try:
        version = metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return None
    return version if version and version == version.strip() else None


def _validate_manifest_component_versions(manifest: runtime_pb2.RuntimeManifest) -> None:
    seen: dict[tuple[int, str], execution_pb2.ComponentVersion] = {}
    for index, component in enumerate(manifest.component_versions):
        field = f"manifest.component_versions[{index}]"
        try:
            validate_component_version(component, field=field)
        except ValueError as error:
            raise ManifestValidationError(str(error)) from error
        identity = (component.kind, component.name.casefold())
        existing = seen.get(identity)
        if existing is not None:
            detail = "duplicate" if existing.version == component.version else "conflicting"
            raise ManifestValidationError(
                f"{field} has a {detail} kind/name identity for {component.name!r}"
            )
        seen[identity] = component

        provenance = component.attributes.get("provenance", "")
        if component.source != _DECLARED_SOURCE or provenance != _DECLARED_PROVENANCE:
            raise ManifestValidationError(
                f"{field} accepts only declared versions with source={_DECLARED_SOURCE!r} "
                f"and provenance={_DECLARED_PROVENANCE!r}; resolved provenance is "
                "added only by the trusted runtime adapter registry"
            )
        selected_field = _DECLARED_COMPONENT_FIELDS.get(component.kind)
        if selected_field is None:
            raise ManifestValidationError(
                f"{field} declared kind is not a selectable runtime component"
            )
        selected_name = getattr(manifest, selected_field)
        if component.name.casefold() != selected_name.casefold():
            raise ManifestValidationError(
                f"{field} name {component.name!r} does not match selected "
                f"{selected_field}={selected_name!r}"
            )


def runtime_manifest_from_job(
    job: job_pb2.RLTrainingJob, *, observed_at: datetime | None = None
) -> runtime_pb2.RuntimeManifest:
    """Build a runtime manifest from the latest job/runtime product inputs."""
    runtime = job.runtime
    has_version_pins = any(
        getattr(runtime, version_field) for _, version_field, _ in _COMPONENT_FIELDS
    )
    declaration_time = observed_at
    if has_version_pins:
        if declaration_time is None and job.HasField("created_at"):
            declaration_time = job.created_at.ToDatetime(tzinfo=UTC)
        if declaration_time is None:
            raise ValueError(
                "job.created_at or an explicit observed_at is required for version pins"
            )
        if declaration_time.tzinfo is None:
            raise ValueError("component version observed_at must be timezone-aware")
    semantic_context = trace_pb2.SemanticEnvelope(
        envelope_id=f"manifest:{job.job_id}",
        schema_version="v1",
        type_name="tgsrl.v1.RuntimeManifest",
    )
    return runtime_pb2.RuntimeManifest(
        manifest_id=f"manifest:{job.job_id}",
        run_id=f"run:{job.job_id}",
        job_id=job.job_id,
        trace_id=f"trace:{job.job_id}",
        framework=runtime.framework or "fake",
        execution_backend=runtime.execution_backend or "fake",
        trainer=runtime.trainer or "fake",
        rollout_engine=runtime.rollout_engine or "fake",
        image_digests=[runtime.image_digest] if runtime.image_digest else [],
        patch_set=list(runtime.patch_set),
        compatibility_profile=runtime.compatibility_profile,
        annotations=dict(runtime.annotations) | {"algorithm": job.algorithm},
        data_kind=job.data_kind,
        semantic_context=semantic_context,
        execution_contract=job.execution_contract,
        resources_per_unit=job.resources_per_unit,
        required_capabilities=job.required_capabilities,
        desired_units=job.desired_units or 1,
        priority=job.priority,
        queue=job.queue,
        rollout_mode=job.rollout_mode,
        policy_version=job.policy_ref,
        command=list(runtime.command),
        args=list(runtime.args),
        environment=dict(runtime.environment),
        working_directory=runtime.working_directory,
        component_versions=(
            _declared_component_versions(job, declaration_time)
            if declaration_time is not None
            else []
        ),
    )


def _unit_kind_for_phase(phase_kind: int) -> int:
    return {
        execution_pb2.PHASE_KIND_PREFILL: runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        execution_pb2.PHASE_KIND_DECODE: runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        execution_pb2.PHASE_KIND_REFERENCE: runtime_pb2.RUNTIME_UNIT_KIND_REFERENCE,
        execution_pb2.PHASE_KIND_REWARD: runtime_pb2.RUNTIME_UNIT_KIND_REWARD,
        execution_pb2.PHASE_KIND_ACTOR: runtime_pb2.RUNTIME_UNIT_KIND_ACTOR,
        execution_pb2.PHASE_KIND_OPTIMIZER: runtime_pb2.RUNTIME_UNIT_KIND_OPTIMIZER,
        execution_pb2.PHASE_KIND_WEIGHT_SYNC: runtime_pb2.RUNTIME_UNIT_KIND_TRAINER,
    }.get(phase_kind, runtime_pb2.RUNTIME_UNIT_KIND_TRAINER)


@dataclass(frozen=True)
class RuntimeAdapterBundle:
    """All adapters required to materialize one manifest."""

    framework: FrameworkAdapter
    execution: ExecutionBackendAdapter
    trainer: TrainerAdapter
    rollout_engine: RolloutEngineAdapter

    def support_reports(self, manifest: runtime_pb2.RuntimeManifest) -> dict[str, SupportReport]:
        return {
            "framework": self.framework.describe_support(manifest),
            "execution": self.execution.describe_support(manifest),
            "trainer": self.trainer.describe_support(manifest),
            "rollout_engine": self.rollout_engine.describe_support(manifest),
        }

    def diagnostics(self, manifest: runtime_pb2.RuntimeManifest) -> list[str]:
        output: list[str] = []
        for component, report in self.support_reports(manifest).items():
            output.append(f"{component}:{report.status}:{report.summary}")
            executable = self._executable_action_names(component, manifest)
            unavailable = tuple(
                action.value for action in LifecycleAction if action.value not in executable
            )
            output.append(f"{component}:executable_actions={','.join(executable)}")
            output.append(f"{component}:unavailable_actions={','.join(unavailable)}")
            output.extend(f"{component}:{detail}" for detail in report.diagnostics)
        return output

    def _executable_action_names(
        self, component: str, manifest: runtime_pb2.RuntimeManifest
    ) -> tuple[str, ...]:
        mapping = {
            "framework": self.framework,
            "execution": self.execution,
            "trainer": self.trainer,
            "rollout_engine": self.rollout_engine,
        }
        adapter = mapping[component]
        executable: list[str] = []
        for action in adapter.supported_actions:
            try:
                adapter.lifecycle_call(manifest, action)
            except RuntimeAdapterError:
                continue
            executable.append(action.value)
        return tuple(executable)

    def normalized_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        _validate_manifest_component_versions(manifest)
        normalized = _clone_manifest(manifest)
        normalized.CopyFrom(self.framework.validate_manifest(normalized))
        normalized.CopyFrom(self.execution.validate_manifest(normalized))
        normalized.CopyFrom(self.trainer.validate_manifest(normalized))
        normalized.CopyFrom(self.rollout_engine.validate_manifest(normalized))
        return normalized

    def runtime_units(self, manifest: runtime_pb2.RuntimeManifest) -> list[runtime_pb2.RuntimeUnit]:
        normalized = self.normalized_manifest(manifest)
        support = self.support_reports(normalized)
        units: list[runtime_pb2.RuntimeUnit] = []
        desired_units = max(normalized.desired_units, 1)
        resources = resource_pb2.ResourceVector()
        if normalized.HasField("resources_per_unit"):
            resources.CopyFrom(normalized.resources_per_unit)
        else:
            resources.cpu_millis = 1000
            resources.memory_bytes = 1 << 30
        priority = normalized.priority
        queue = normalized.queue or "default"
        for phase in normalized.execution_contract.phase_graph.phases:
            kind = _unit_kind_for_phase(phase.kind)
            component = (
                "rollout_engine" if kind == runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT else "trainer"
            )
            units.append(
                runtime_pb2.RuntimeUnit(
                    runtime_unit_id=f"{normalized.run_id}:{phase.phase_id}",
                    run_id=normalized.run_id,
                    job_id=normalized.job_id,
                    trace_id=normalized.trace_id,
                    kind=kind,
                    phase_id=phase.phase_id,
                    phase_kind=phase.kind,
                    state=runtime_pb2.RUNTIME_STATE_REQUESTED,
                    generation=1,
                    requested_resources=resources,
                    required_capabilities=(
                        normalized.required_capabilities
                        if normalized.HasField("required_capabilities")
                        else resource_pb2.CapabilitySet()
                    ),
                    execution_id=normalized.run_id,
                    stage_id=phase.phase_id,
                    data_kind=normalized.data_kind,
                    semantic_context=normalized.semantic_context,
                    annotations={
                        "component": component,
                        "component_support": support[component].status,
                        "display_name": phase.display_name,
                        "unit_count": str(desired_units),
                        "priority": str(priority),
                        "queue": queue,
                    },
                )
            )
        return units


class RuntimeAdapterRegistry:
    """Stable component registry for fake and real-boundary adapters."""

    def __init__(
        self,
        *,
        version_resolver: Callable[[str], str | None] = _installed_distribution_version,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._version_resolver = version_resolver
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._frameworks: dict[str, FrameworkAdapter] = {
            adapter.component_name: adapter
            for adapter in (
                FakeFrameworkAdapter(),
                VerlFrameworkAdapter(),
                OpenRLHFFrameworkAdapter(),
            )
        }
        self._frameworks["mock"] = self._frameworks["fake"]
        self._execution: dict[str, ExecutionBackendAdapter] = {
            adapter.component_name: adapter
            for adapter in (FakeExecutionBackendAdapter(), RayExecutionBackendAdapter())
        }
        self._execution["mock"] = self._execution["fake"]
        self._trainers: dict[str, TrainerAdapter] = {
            adapter.component_name: adapter
            for adapter in (FakeTrainerAdapter(), PyTorchTrainerAdapter())
        }
        self._trainers["mock"] = self._trainers["fake"]
        self._rollout: dict[str, RolloutEngineAdapter] = {
            adapter.component_name: adapter
            for adapter in (
                FakeRolloutEngineAdapter(),
                VLLMRolloutEngineAdapter(),
                SGLangRolloutEngineAdapter(),
            )
        }
        self._rollout["mock"] = self._rollout["fake"]

    def resolved_component_versions(
        self,
        manifest: runtime_pb2.RuntimeManifest,
        *,
        observed_at: datetime | None = None,
    ) -> list[execution_pb2.ComponentVersion]:
        """Resolve installed component versions from the explicit registry only."""
        timestamp = observed_at or self._clock()
        if timestamp.tzinfo is None:
            raise ValueError("component version clock must be timezone-aware")
        selected = [
            (execution_pb2.COMPONENT_KIND_RUNTIME, "tgsrl-runtime", _RUNTIME_DISTRIBUTION),
            *(
                (kind, getattr(manifest, name_field), distribution)
                for name_field, _version_field, kind in _COMPONENT_FIELDS
                if (
                    distribution := _ADAPTER_DISTRIBUTIONS.get(
                        (kind, getattr(manifest, name_field).casefold())
                    )
                )
            ),
        ]
        resolved: list[execution_pb2.ComponentVersion] = []
        for kind, name, distribution in selected:
            version = self._version_resolver(distribution)
            if version is None:
                continue
            resolved.append(
                _component_version(
                    kind=kind,
                    name=name,
                    version=version,
                    observed_at=timestamp,
                    source="runtime-adapter-registry",
                    provenance="observed/resolved",
                    attributes={"distribution": distribution},
                )
            )
        return resolved

    def bundle_for(self, manifest: runtime_pb2.RuntimeManifest) -> RuntimeAdapterBundle:
        return RuntimeAdapterBundle(
            framework=self._frameworks[manifest.framework.casefold()],
            execution=self._execution[manifest.execution_backend.casefold()],
            trainer=self._trainers[manifest.trainer.casefold()],
            rollout_engine=self._rollout[manifest.rollout_engine.casefold()],
        )

    def validate(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> tuple[runtime_pb2.RuntimeManifest, list[str]]:
        normalized, _reports, diagnostics = self.evaluate(manifest)
        return normalized, diagnostics

    def evaluate(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> tuple[
        runtime_pb2.RuntimeManifest,
        dict[str, SupportReport],
        list[str],
    ]:
        """Return normalized input and structured adapter availability evidence."""
        bundle = self.bundle_for(manifest)
        normalized = bundle.normalized_manifest(manifest)
        return normalized, bundle.support_reports(normalized), bundle.diagnostics(normalized)

    def compile(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> tuple[runtime_pb2.RuntimeManifest, list[runtime_pb2.RuntimeUnit], list[str]]:
        bundle = self.bundle_for(manifest)
        normalized = bundle.normalized_manifest(manifest)
        return normalized, bundle.runtime_units(normalized), bundle.diagnostics(normalized)
