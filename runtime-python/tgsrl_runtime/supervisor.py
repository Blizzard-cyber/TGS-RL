"""Runtime supervisor core and compatibility re-exports."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol, cast

from tgsrl.v1 import (
    control_pb2,
    execution_pb2,
    experiment_pb2,
    resource_pb2,
    runtime_pb2,
    scheduling_pb2,
    semantic_pb2,
    trace_pb2,
)

import tgsrl_runtime.config as runtime_config
from adapters import RuntimeAdapterRegistry
from adapters.compliance.runtime import LifecycleAction
from tgsrl_runtime.aggregation import TraceAggregator
from tgsrl_runtime.checkpoints import CheckpointStore
from tgsrl_runtime.duration import to_timestamp
from tgsrl_runtime.executor import (
    FakeCommandDriver,
    FakeProcessDriver,
    RuntimeExecutionResult,
    RuntimeExecutor,
    SubprocessDriver,
)
from tgsrl_runtime.experiments import ExperimentCoordinator, ReplayExperimentStore
from tgsrl_runtime.intent_coordinator import IntentCoordinator
from tgsrl_runtime.oracle import RuntimeOracle
from tgsrl_runtime.persistence_adapter import (
    NullPersistenceHook,
    PersistenceHook,
    PersistenceHydration,
    restore_hydrated_state,
)
from tgsrl_runtime.proto_utils import decode_page_token, encode_page_token
from tgsrl_runtime.replay import (
    REPLAY_STEP_ARTIFACT_KIND,
    ReplayScheduler,
    SchedulerReplayRunner,
    decode_replay_steps,
    replay_start_key_digest,
    replay_step_progress_digests,
)
from tgsrl_runtime.runtime_errors import RuntimeLifecycleError
from tgsrl_runtime.runtime_lifecycle import RuntimeLifecycleCoordinator
from tgsrl_runtime.storage import (
    ReplayScheduleStep,
    RuntimeObservationBatch,
    RuntimeObservationCommit,
)
from tgsrl_runtime.stores import (
    RuntimeManifestStore,
    RuntimeUnitStore,
    SandboxEventStore,
    SandboxStore,
)
from tgsrl_runtime.trace_ingest import TraceIngestor
from tgsrl_runtime.version_store import VersionStore

if TYPE_CHECKING:
    from tgsrl_runtime.runtime_transport import (
        ExperimentServicer as ExperimentServicer,
    )
    from tgsrl_runtime.runtime_transport import (
        RuntimeControlServicer as RuntimeControlServicer,
    )


class SchedulerClientProtocol(Protocol):
    """Scheduler client interface required by the runtime supervisor."""

    async def publish_intent(
        self, intent: scheduling_pb2.SchedulingIntent
    ) -> scheduling_pb2.PublishIntentResponse | None:
        """Publish one scheduling intent downstream."""


def _digest_payload(parts: Sequence[str]) -> str:
    material = "\0".join(parts).encode()
    return hashlib.sha256(material).hexdigest()


def _cursor(scope: str, *parts: object) -> str:
    payload = json.dumps([scope, *parts], separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def _sequence_cursor(sequence: int, event_id: str) -> str:
    return f"{sequence}:{event_id}"


def _decode_sequence_cursor(cursor: str) -> tuple[int, str]:
    sequence, _, event_id = cursor.partition(":")
    try:
        return int(sequence), event_id
    except ValueError as error:
        raise ValueError("invalid runtime event cursor") from error


def _event_type_for_state(state: int) -> int:
    return {
        runtime_pb2.RUNTIME_STATE_REQUESTED: runtime_pb2.SANDBOX_EVENT_TYPE_REQUESTED,
        runtime_pb2.RUNTIME_STATE_BOUND: runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        runtime_pb2.RUNTIME_STATE_RUNNING: runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        runtime_pb2.RUNTIME_STATE_PAUSED: runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED,
        runtime_pb2.RUNTIME_STATE_SLEEPING: runtime_pb2.SANDBOX_EVENT_TYPE_SLEEPING,
        runtime_pb2.RUNTIME_STATE_FAILED: runtime_pb2.SANDBOX_EVENT_TYPE_FAILED,
        runtime_pb2.RUNTIME_STATE_TERMINATED: runtime_pb2.SANDBOX_EVENT_TYPE_TERMINATED,
    }[state]


def _is_terminal_runtime_state(state: int) -> bool:
    return state in {
        runtime_pb2.RUNTIME_STATE_FAILED,
        runtime_pb2.RUNTIME_STATE_TERMINATED,
    }


def _is_allowed_same_generation_transition(current_state: int, next_state: int) -> bool:
    if current_state == next_state:
        return True
    if current_state == runtime_pb2.RUNTIME_STATE_UNKNOWN:
        return True
    if current_state == runtime_pb2.RUNTIME_STATE_TERMINATED:
        return next_state == runtime_pb2.RUNTIME_STATE_TERMINATED
    if next_state in {
        runtime_pb2.RUNTIME_STATE_FAILED,
        runtime_pb2.RUNTIME_STATE_TERMINATED,
    }:
        return not _is_terminal_runtime_state(current_state)
    allowed: dict[int, set[int]] = {
        runtime_pb2.RUNTIME_STATE_REQUESTED: {
            runtime_pb2.RUNTIME_STATE_BOUND,
            runtime_pb2.RUNTIME_STATE_RUNNING,
            runtime_pb2.RUNTIME_STATE_SLEEPING,
            runtime_pb2.RUNTIME_STATE_PAUSED,
        },
        runtime_pb2.RUNTIME_STATE_BOUND: {
            runtime_pb2.RUNTIME_STATE_RUNNING,
            runtime_pb2.RUNTIME_STATE_PAUSED,
            runtime_pb2.RUNTIME_STATE_SLEEPING,
        },
        runtime_pb2.RUNTIME_STATE_RUNNING: {
            runtime_pb2.RUNTIME_STATE_PAUSED,
            runtime_pb2.RUNTIME_STATE_SLEEPING,
        },
        runtime_pb2.RUNTIME_STATE_PAUSED: {
            runtime_pb2.RUNTIME_STATE_RUNNING,
            runtime_pb2.RUNTIME_STATE_SLEEPING,
        },
        runtime_pb2.RUNTIME_STATE_SLEEPING: {
            runtime_pb2.RUNTIME_STATE_RUNNING,
            runtime_pb2.RUNTIME_STATE_PAUSED,
        },
        runtime_pb2.RUNTIME_STATE_FAILED: set(),
    }
    return next_state in allowed.get(current_state, set())


def _same_message_payload(
    left: runtime_pb2.SandboxEvent,
    right: runtime_pb2.SandboxEvent,
) -> bool:
    return left.SerializeToString(deterministic=True) == right.SerializeToString(deterministic=True)


def _binding_share(binding: scheduling_pb2.Binding) -> float | None:
    if binding.HasField("resources"):
        return binding.resources.accelerator_units
    return None


def _runtime_priority(
    runtime_unit: runtime_pb2.RuntimeUnit,
    manifest: runtime_pb2.RuntimeManifest,
) -> int:
    value = runtime_unit.annotations.get("priority", "").strip()
    if value:
        try:
            return int(value)
        except ValueError:
            pass
    return manifest.priority


def _event_or_default_share(
    event: runtime_pb2.SandboxEvent,
    *,
    binding: scheduling_pb2.Binding,
    runtime_unit: runtime_pb2.RuntimeUnit,
    manifest: runtime_pb2.RuntimeManifest,
    existing: runtime_pb2.Sandbox | None,
) -> float:
    if event.HasField("share"):
        return event.share
    if existing is not None:
        return existing.share
    binding_share = _binding_share(binding)
    if binding_share is not None:
        return binding_share
    requested = runtime_unit.requested_resources.accelerator_units
    if requested:
        return requested
    return manifest.resources_per_unit.accelerator_units


def _event_or_default_priority(
    event: runtime_pb2.SandboxEvent,
    *,
    runtime_unit: runtime_pb2.RuntimeUnit,
    manifest: runtime_pb2.RuntimeManifest,
    existing: runtime_pb2.Sandbox | None,
) -> int:
    if event.HasField("priority"):
        return event.priority
    if existing is not None:
        return existing.priority
    return _runtime_priority(runtime_unit, manifest)


def _event_or_default_offloaded(
    event: runtime_pb2.SandboxEvent,
    *,
    existing: runtime_pb2.Sandbox | None,
) -> bool:
    if event.HasField("offloaded"):
        return event.offloaded
    if existing is not None:
        return existing.offloaded
    return event.state == runtime_pb2.RUNTIME_STATE_SLEEPING


def _trace_event(
    *,
    run_id: str,
    manifest: runtime_pb2.RuntimeManifest,
    runtime_unit: runtime_pb2.RuntimeUnit,
    event_id: str,
    event_type: int,
    safe_point: bool = False,
    buffer_level: int = 0,
) -> trace_pb2.TraceEvent:
    occurred_at = to_timestamp(datetime.now(tz=UTC))
    attributes = {"runtime_unit_id": runtime_unit.runtime_unit_id}
    for key in (
        "provider_source",
        "provider_kind",
        "selection_strategy",
        "algorithm",
        "rollout_mode",
        "policy_version",
    ):
        value = runtime_unit.annotations.get(key, "")
        if value:
            attributes[key] = value
    event = trace_pb2.TraceEvent(
        event_id=event_id,
        job_id=manifest.job_id,
        execution_id=runtime_unit.execution_id,
        phase_id=runtime_unit.phase_id,
        occurred_at=occurred_at,
        event_type=event_type,
        algorithm=manifest.annotations.get("algorithm", "grpo"),
        rollout_mode=manifest.rollout_mode or trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version=manifest.policy_version
        or manifest.annotations.get("policy_version", "policy-1"),
        buffer_level=buffer_level,
        safe_point=safe_point,
        decision_id=f"decision:{run_id}:{runtime_unit.phase_id}",
        sequence=runtime_unit.generation,
        attributes=attributes,
        phase_kind=runtime_unit.phase_kind,
        raw_phase_label=runtime_unit.phase_id,
        sandbox_id=runtime_unit.sandbox_id,
        stage_id=runtime_unit.stage_id,
        run_id=run_id,
        data_kind=manifest.data_kind or trace_pb2.DATA_KIND_SYNTHETIC,
    )
    event.contract_observation.CopyFrom(
        execution_pb2.ContractObservation(
            observed_at=occurred_at,
            buffer_level=buffer_level,
            safe_point=safe_point,
            source="runtime-supervisor",
            event_id=event_id,
            phase_id=runtime_unit.phase_id,
            policy_version=event.policy_version,
            typed_facts=[
                semantic_pb2.SemanticField(
                    key="buffer.level.current",
                    value=semantic_pb2.SemanticValue(uint64_value=buffer_level),
                ),
                semantic_pb2.SemanticField(
                    key="runtime.safe_point",
                    value=semantic_pb2.SemanticValue(bool_value=safe_point),
                ),
            ],
            fact_observations=[
                execution_pb2.ObservedFact(
                    fact=semantic_pb2.SemanticField(
                        key="buffer.level.current",
                        value=semantic_pb2.SemanticValue(uint64_value=buffer_level),
                    ),
                    observed_at=occurred_at,
                    source="runtime-supervisor",
                    revision=max(runtime_unit.generation, 1),
                ),
                execution_pb2.ObservedFact(
                    fact=semantic_pb2.SemanticField(
                        key="runtime.safe_point",
                        value=semantic_pb2.SemanticValue(bool_value=safe_point),
                    ),
                    observed_at=occurred_at,
                    source="runtime-supervisor",
                    revision=max(runtime_unit.generation, 1),
                ),
            ],
        )
    )
    return event


@dataclass
class RuntimeSupervisor:
    """Own manifests, runtime units, sandboxes, intents, and experiments."""

    config_bundle: runtime_config.ConfigBundle | None = None
    config_projection: runtime_config.RuntimeProjection | None = None
    scheduler_client: SchedulerClientProtocol | None = None
    registry: RuntimeAdapterRegistry = field(default_factory=RuntimeAdapterRegistry)
    versions: VersionStore = field(default_factory=VersionStore)
    manifests: RuntimeManifestStore = field(default_factory=RuntimeManifestStore)
    runtime_units: RuntimeUnitStore = field(default_factory=RuntimeUnitStore)
    sandboxes: SandboxStore = field(default_factory=SandboxStore)
    sandbox_events: SandboxEventStore = field(default_factory=SandboxEventStore)
    checkpoints: CheckpointStore = field(default_factory=CheckpointStore)
    trace_ingestor: TraceIngestor = field(default_factory=TraceIngestor)
    aggregator: TraceAggregator = field(default_factory=TraceAggregator)
    intent_coordinator: IntentCoordinator = field(default_factory=IntentCoordinator)
    oracle: RuntimeOracle = field(default_factory=RuntimeOracle)
    experiments: ExperimentCoordinator = field(
        default_factory=lambda: ExperimentCoordinator(
            trace_ingestor=TraceIngestor(),
            store=ReplayExperimentStore(),
        )
    )
    persistence: PersistenceHook = field(default_factory=NullPersistenceHook)
    clock: Callable[[], datetime] = field(
        default=lambda: datetime.now(tz=UTC), repr=False
    )
    published_intents: list[scheduling_pb2.SchedulingIntent] = field(default_factory=list)
    component_statuses: dict[tuple[str, str], control_pb2.ComponentStatus] = field(
        default_factory=dict
    )
    fake_process_driver: FakeProcessDriver = field(default_factory=FakeProcessDriver)
    real_process_driver: SubprocessDriver = field(default_factory=SubprocessDriver)
    fake_command_driver: FakeCommandDriver = field(init=False)
    fake_executor: RuntimeExecutor = field(init=False)
    real_executor: RuntimeExecutor = field(init=False)
    lifecycle: RuntimeLifecycleCoordinator = field(init=False)

    def __post_init__(self) -> None:
        self.fake_command_driver = FakeCommandDriver(process_driver=self.fake_process_driver)
        self.fake_executor = RuntimeExecutor(
            registry=self.registry,
            command_driver=self.fake_command_driver,
            process_driver=self.fake_process_driver,
        )
        self.real_executor = RuntimeExecutor(
            registry=self.registry,
            command_driver=self.real_process_driver,
            process_driver=self.real_process_driver,
        )
        self.lifecycle = RuntimeLifecycleCoordinator(self)
        self.experiments.trace_ingestor = self.trace_ingestor
        if self.config_bundle is not None and self.config_projection is None:
            self.config_projection = runtime_config.runtime_projection(self.config_bundle)
        hydration = self.persistence.hydrate_state()
        if hydration is not None:
            restore_hydrated_state(
                hydration=hydration,
                manifests=self.manifests,
                runtime_units=self.runtime_units,
                sandboxes=self.sandboxes,
                sandbox_events=self.sandbox_events,
                trace_ingestor=self.trace_ingestor,
                checkpoints=self.checkpoints,
                versions=self.versions,
                experiments_store=self.experiments.store,
                component_statuses=self.component_statuses,
                aggregator=self.aggregator,
            )
            self.published_intents.extend(hydration.adapter_state.intents)
            self.intent_coordinator.restore_versions(hydration.adapter_state.intents)
            self._hydrate_executors(hydration)

    def _ensure_run(self, run_id: str) -> runtime_pb2.RuntimeManifest:
        if not self.manifests.has(run_id):
            raise RuntimeLifecycleError(f"unknown run_id: {run_id}")
        return self.manifests.get(run_id)

    def _executor_for_manifest(self, manifest: runtime_pb2.RuntimeManifest) -> RuntimeExecutor:
        fake_components = {manifest.framework, manifest.execution_backend, manifest.trainer}
        fake_components.add(manifest.rollout_engine)
        if all(component in {"fake", "mock"} for component in fake_components):
            return self.fake_executor
        return self.real_executor

    def _sync_executor(
        self,
        run_id: str,
        *,
        compile_if_missing: bool,
    ) -> tuple[RuntimeExecutor, runtime_pb2.RuntimeManifest]:
        manifest = self._apply_config_projection(self._ensure_run(run_id))
        self.manifests.put(manifest)
        runtime_units = self.runtime_units.list(run_id)
        if not runtime_units and compile_if_missing:
            _, runtime_units = self._compile_with_projection(manifest)
            self.runtime_units.replace(run_id, runtime_units)
        executor = self._executor_for_manifest(manifest)
        executor.register_runtime(manifest, runtime_units)
        return executor, manifest

    def _hydrate_executors(self, hydration: PersistenceHydration) -> None:
        """Restore executor-side generation watermarks from persisted runtime state."""
        for recovery in hydration.runtime_recoveries:
            manifest = self.manifests.get(recovery.manifest.run_id)
            runtime_units = self.runtime_units.list(recovery.manifest.run_id)
            executor = self._executor_for_manifest(manifest)
            executor.register_runtime(manifest, runtime_units)
            executor.restore_runtime_generation(
                recovery.manifest.run_id, recovery.latest_generation
            )

    def _trace_event_for_start(
        self,
        *,
        manifest: runtime_pb2.RuntimeManifest,
        runtime_unit: runtime_pb2.RuntimeUnit,
    ) -> trace_pb2.TraceEvent:
        event = _trace_event(
            run_id=manifest.run_id,
            manifest=manifest,
            runtime_unit=runtime_unit,
            event_id=(
                f"trace:{runtime_unit.runtime_unit_id}:generation:{runtime_unit.generation}:start"
            ),
            event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
        )
        event.contract_observation.component_versions.extend(
            self.registry.resolved_component_versions(
                manifest,
                observed_at=event.occurred_at.ToDatetime(tzinfo=UTC),
            )
        )
        return event

    def _cursor_for_intent(self, intent: scheduling_pb2.SchedulingIntent) -> str:
        return _cursor("intent", intent.execution_id, intent.stage_id, intent.version)

    def _state_reason_for(self, state: int) -> str:
        return {
            runtime_pb2.RUNTIME_STATE_UNKNOWN: "unknown",
            runtime_pb2.RUNTIME_STATE_REQUESTED: "requested",
            runtime_pb2.RUNTIME_STATE_BOUND: "bound",
            runtime_pb2.RUNTIME_STATE_RUNNING: "running",
            runtime_pb2.RUNTIME_STATE_PAUSED: "paused",
            runtime_pb2.RUNTIME_STATE_SLEEPING: "sleeping",
            runtime_pb2.RUNTIME_STATE_FAILED: "failed",
            runtime_pb2.RUNTIME_STATE_TERMINATED: "terminated",
            runtime_pb2.RUNTIME_STATE_VALIDATING: "validating",
            runtime_pb2.RUNTIME_STATE_COMPILING: "compiling",
            runtime_pb2.RUNTIME_STATE_PREPARING: "preparing",
            runtime_pb2.RUNTIME_STATE_STARTING: "starting",
            runtime_pb2.RUNTIME_STATE_PAUSING: "pausing",
            runtime_pb2.RUNTIME_STATE_RESUMING: "resuming",
            runtime_pb2.RUNTIME_STATE_CHECKPOINTING: "checkpointing",
            runtime_pb2.RUNTIME_STATE_STOPPING: "stopping",
            runtime_pb2.RUNTIME_STATE_TERMINATING: "terminating",
        }.get(state, "unknown")

    def _require_executor_success(
        self,
        result: RuntimeExecutionResult,
        *,
        action: LifecycleAction,
    ) -> RuntimeExecutionResult:
        if result.ok:
            return result
        first_error = next(
            (item.error for item in result.component_results if item.error is not None),
            None,
        )
        detail = first_error.summary if first_error is not None else f"{action.value} failed"
        raise RuntimeLifecycleError(detail)

    def _update_runtime_status(
        self,
        run_id: str,
    ) -> control_pb2.ComponentStatus:
        return self.project_runtime_status(run_id)

    def project_runtime_status(self, run_id: str) -> control_pb2.ComponentStatus:
        """Persist the authoritative projection of all observed runtime targets."""
        manifest = self._ensure_run(run_id)
        sequenced_events = self.sandbox_events.list_with_sequences(run_id)
        latest_event = sequenced_events[-1][1] if sequenced_events else None
        projection = self.oracle.project(
            component="runtime",
            run_id=run_id,
            job_id=manifest.job_id,
            trace_id=manifest.trace_id,
            runtime_units=self.runtime_units.list(run_id),
            sandboxes=self.sandboxes.list(run_id),
            revision=sequenced_events[-1][0] if sequenced_events else 0,
            observed_at=(
                latest_event.occurred_at
                if latest_event is not None and latest_event.HasField("occurred_at")
                else None
            ),
        )
        projected_units = list(projection.runtime_units)
        self.runtime_units.put_many(run_id, projected_units)
        self.persistence.save_runtime_units(projected_units)
        status = projection.component_status
        status.data_kind = (
            latest_event.data_kind
            if latest_event is not None and latest_event.data_kind
            else manifest.data_kind
        )
        if latest_event is not None and latest_event.HasField("semantic_context"):
            status.semantic_context.CopyFrom(latest_event.semantic_context)
        elif manifest.HasField("semantic_context"):
            status.semantic_context.CopyFrom(manifest.semantic_context)
        self.component_statuses[(run_id, "runtime")] = status
        self.persistence.record_component_status(status)
        result = control_pb2.ComponentStatus()
        result.CopyFrom(status)
        return result

    def get_component_status(self, run_id: str, component: str) -> control_pb2.ComponentStatus:
        """Return one component status using the durable run/component key."""
        status = control_pb2.ComponentStatus()
        status.CopyFrom(self.component_statuses[(run_id, component)])
        return status

    def _project_runtime_status_candidate(
        self,
        *,
        run_id: str,
        runtime_units: list[runtime_pb2.RuntimeUnit],
        sandboxes: list[runtime_pb2.Sandbox],
        sequence: int,
        latest_event: runtime_pb2.SandboxEvent,
    ) -> tuple[list[runtime_pb2.RuntimeUnit], control_pb2.ComponentStatus]:
        manifest = self._ensure_run(run_id)
        projection = self.oracle.project(
            component="runtime",
            run_id=run_id,
            job_id=manifest.job_id,
            trace_id=manifest.trace_id,
            runtime_units=runtime_units,
            sandboxes=sandboxes,
            revision=sequence,
            observed_at=(
                latest_event.occurred_at if latest_event.HasField("occurred_at") else None
            ),
        )
        projected_units = list(projection.runtime_units)
        status = control_pb2.ComponentStatus()
        status.CopyFrom(projection.component_status)
        status.data_kind = latest_event.data_kind or manifest.data_kind
        if latest_event.HasField("semantic_context"):
            status.semantic_context.CopyFrom(latest_event.semantic_context)
        elif manifest.HasField("semantic_context"):
            status.semantic_context.CopyFrom(manifest.semantic_context)
        return projected_units, status

    def _apply_runtime_observation_commit(
        self,
        *,
        commit: RuntimeObservationCommit,
        projected_units: list[runtime_pb2.RuntimeUnit],
    ) -> runtime_pb2.SandboxEvent:
        self.sandboxes.update_one(commit.sandbox.run_id, commit.sandbox)
        self.runtime_units.put_many(commit.runtime_unit.run_id, projected_units)
        self.sandbox_events.append(commit.event, sequence=commit.runtime_event_sequence)
        status_key = (commit.component_status.run_id, commit.component_status.component)
        self.component_statuses[status_key] = commit.component_status
        stored = runtime_pb2.SandboxEvent()
        stored.CopyFrom(commit.event)
        return stored

    def _configured_capabilities(self) -> resource_pb2.CapabilitySet | None:
        if self.config_bundle is None:
            return None
        capabilities = self.config_bundle.capabilities
        return resource_pb2.CapabilitySet(
            names=list(capabilities.names),
            attributes=dict(capabilities.attributes),
            algorithms=list(capabilities.algorithms),
            rollout_modes=list(capabilities.rollout_modes),
            source=capabilities.source,
            revision=capabilities.revision,
            supported_actions=list(capabilities.supported_actions),
        )

    def _rollout_mode_from_projection(self, rollout_mode: str) -> int:
        normalized = rollout_mode.strip().lower()
        if normalized == "sync":
            return trace_pb2.ROLLOUT_MODE_SYNC
        if normalized == "partially_async":
            return trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC
        if normalized == "fully_async":
            return trace_pb2.ROLLOUT_MODE_FULLY_ASYNC
        raise RuntimeLifecycleError(f"unsupported config rollout_mode: {rollout_mode}")

    def _runtime_registry_component(self, field: str, value: str) -> str:
        normalized = value.strip().lower().replace("-", "").replace("_", "")
        aliases = {
            "framework": {
                "mock": "mock",
                "fake": "mock",
                "verl": "verl",
                "openrlhf": "openrlhf",
            },
            "execution_backend": {
                "mock": "mock",
                "fake": "mock",
                "inprocessmock": "mock",
                "ray": "ray",
            },
            "trainer": {
                "mock": "fake",
                "fake": "fake",
                "pytorch": "pytorch",
            },
            "rollout_engine": {
                "mock": "fake",
                "fake": "fake",
                "vllm": "vllm",
                "sglang": "sglang",
            },
        }[field]
        alias = aliases.get(normalized)
        if alias is None:
            raise RuntimeLifecycleError(
                f"runtime adapter registry does not support {field}={value!r}"
            )
        return alias

    def _matches_component(self, current: str, field: str) -> bool:
        projection = self.config_projection
        if projection is None:
            return True
        target = getattr(projection, field)
        return self._runtime_registry_component(field, current) == self._runtime_registry_component(
            field, target
        )

    def _apply_config_projection(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        projected = runtime_pb2.RuntimeManifest()
        projected.CopyFrom(manifest)
        if self.config_projection is None:
            return projected

        mapping = {
            "framework": self.config_projection.framework,
            "execution_backend": self.config_projection.execution_backend,
            "trainer": self.config_projection.trainer,
            "rollout_engine": self.config_projection.rollout_engine,
        }
        for field_name, expected in mapping.items():
            current = getattr(projected, field_name)
            if current and not self._matches_component(current, field_name):
                raise RuntimeLifecycleError(
                    f"manifest {field_name}={current!r} conflicts with config projection "
                    f"{expected!r}"
                )
            setattr(projected, field_name, self._runtime_registry_component(field_name, expected))

        if (
            projected.compatibility_profile
            and projected.compatibility_profile != self.config_projection.compatibility_profile
        ):
            raise RuntimeLifecycleError(
                "manifest compatibility_profile conflicts with config projection"
            )
        projected.compatibility_profile = self.config_projection.compatibility_profile

        projected_data_kind = {
            "synthetic": trace_pb2.DATA_KIND_SYNTHETIC,
            "replay": trace_pb2.DATA_KIND_REPLAY,
            "live": trace_pb2.DATA_KIND_LIVE,
        }[self.config_projection.data_kind]
        if projected.data_kind not in (trace_pb2.DATA_KIND_UNKNOWN, projected_data_kind):
            raise RuntimeLifecycleError("manifest data_kind conflicts with config projection")
        projected.data_kind = projected_data_kind

        projected_rollout_mode = self._rollout_mode_from_projection(
            self.config_projection.rollout_mode
        )
        if projected.rollout_mode not in (
            trace_pb2.ROLLOUT_MODE_UNKNOWN,
            projected_rollout_mode,
        ):
            raise RuntimeLifecycleError("manifest rollout_mode conflicts with config projection")
        projected.rollout_mode = projected_rollout_mode

        if (
            projected.policy_version
            and projected.policy_version != self.config_projection.policy_version
        ):
            raise RuntimeLifecycleError("manifest policy_version conflicts with config projection")
        projected.policy_version = self.config_projection.policy_version

        if projected.desired_units not in (0, self.config_projection.desired_units):
            raise RuntimeLifecycleError("manifest desired_units conflicts with config projection")
        projected.desired_units = self.config_projection.desired_units

        algorithm = projected.annotations.get("algorithm", "")
        if algorithm and algorithm != self.config_projection.algorithm:
            raise RuntimeLifecycleError("manifest algorithm conflicts with config projection")
        projected.annotations["algorithm"] = self.config_projection.algorithm
        projected.annotations["policy_version"] = self.config_projection.policy_version
        projected.annotations["selection_strategy"] = self.config_projection.selection_strategy
        projected.annotations["provider_source"] = self.config_projection.provider_source
        projected.annotations["provider_kind"] = self.config_projection.provider_kind
        projected.annotations["rollout_mode"] = self.config_projection.rollout_mode

        capabilities = self._configured_capabilities()
        if capabilities is not None:
            current_capabilities = projected.required_capabilities
            if current_capabilities.names and set(current_capabilities.names) != set(
                capabilities.names
            ):
                raise RuntimeLifecycleError(
                    "manifest required_capabilities.names conflicts with config projection"
                )
            capabilities.component_versions.extend(current_capabilities.component_versions)
            projected.required_capabilities.CopyFrom(capabilities)
        return projected

    def _compile_with_projection(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> tuple[runtime_pb2.RuntimeManifest, list[runtime_pb2.RuntimeUnit]]:
        projected_manifest = self._apply_config_projection(manifest)
        normalized, runtime_units, _diagnostics = self.registry.compile(projected_manifest)
        for unit in runtime_units:
            unit.generation = 0
            unit.observed_at.CopyFrom(to_timestamp(datetime.now(tz=UTC)))
            unit.data_kind = normalized.data_kind
            unit.annotations["provider_source"] = normalized.annotations.get("provider_source", "")
            unit.annotations["provider_kind"] = normalized.annotations.get("provider_kind", "")
            unit.annotations["selection_strategy"] = normalized.annotations.get(
                "selection_strategy", ""
            )
            unit.annotations["algorithm"] = normalized.annotations.get("algorithm", "")
            unit.annotations["rollout_mode"] = normalized.annotations.get("rollout_mode", "")
            unit.annotations["policy_version"] = normalized.policy_version
        return normalized, runtime_units

    def validate_runtime(
        self, request: runtime_pb2.ValidateRuntimeRequest
    ) -> runtime_pb2.ValidateRuntimeResponse:
        normalized_input = self._apply_config_projection(request.manifest)
        normalized, diagnostics = self.registry.validate(normalized_input)
        valid = not any(":UNSUPPORTED:" in item for item in diagnostics)
        if not valid:
            return runtime_pb2.ValidateRuntimeResponse(
                valid=False,
                normalized_manifest=normalized,
                diagnostics=diagnostics,
                cursor=_cursor("validate", normalized.run_id, request.idempotency_key),
            )
        self.manifests.put(normalized)
        self.persistence.save_manifest(normalized)
        return runtime_pb2.ValidateRuntimeResponse(
            valid=valid,
            normalized_manifest=normalized,
            diagnostics=diagnostics,
            cursor=_cursor("validate", normalized.run_id, request.idempotency_key),
        )

    def compile_runtime(
        self, request: runtime_pb2.CompileRuntimeRequest
    ) -> runtime_pb2.CompileRuntimeResponse:
        normalized, runtime_units = self._compile_with_projection(request.manifest)
        self.manifests.put(normalized)
        self.persistence.save_manifest(normalized)
        stored_units = self.runtime_units.replace(normalized.run_id, runtime_units)
        self.persistence.save_runtime_units(stored_units)
        return runtime_pb2.CompileRuntimeResponse(
            manifest=normalized,
            runtime_units=stored_units,
            cursor=_cursor("compile", normalized.run_id, request.idempotency_key),
        )

    def prepare_runtime(
        self, request: runtime_pb2.PrepareRuntimeRequest
    ) -> runtime_pb2.PrepareRuntimeResponse:
        return self.lifecycle.prepare(request)

    async def start_runtime(
        self, request: runtime_pb2.StartRuntimeRequest
    ) -> runtime_pb2.StartRuntimeResponse:
        return await self.lifecycle.start(request)

    def pause_runtime(
        self, request: runtime_pb2.PauseRuntimeRequest
    ) -> runtime_pb2.PauseRuntimeResponse:
        return self.lifecycle.pause(request)

    def resume_runtime(
        self, request: runtime_pb2.ResumeRuntimeRequest
    ) -> runtime_pb2.ResumeRuntimeResponse:
        return self.lifecycle.resume(request)

    def checkpoint_runtime(
        self, request: runtime_pb2.CheckpointRuntimeRequest
    ) -> runtime_pb2.CheckpointRuntimeResponse:
        return self.lifecycle.checkpoint(request)

    def stop_runtime(
        self, request: runtime_pb2.StopRuntimeRequest
    ) -> runtime_pb2.StopRuntimeResponse:
        return self.lifecycle.stop(request)

    def terminate_runtime(
        self, request: runtime_pb2.TerminateRuntimeRequest
    ) -> runtime_pb2.TerminateRuntimeResponse:
        return self.lifecycle.terminate(request)

    def get_runtime_manifest(
        self, request: runtime_pb2.GetRuntimeManifestRequest
    ) -> runtime_pb2.GetRuntimeManifestResponse:
        return runtime_pb2.GetRuntimeManifestResponse(
            manifest=self._ensure_run(request.run_id),
            cursor=_cursor("manifest", request.run_id),
        )

    def list_runtime_units(
        self, request: runtime_pb2.ListRuntimeUnitsRequest
    ) -> runtime_pb2.ListRuntimeUnitsResponse:
        items = self.runtime_units.list(request.run_id, stage_id=request.stage_id)
        scope = "runtime-units"
        filters = (request.run_id, request.stage_id)
        start = decode_page_token(request.page_token, scope=scope, filters=filters)
        limit = int(request.limit or len(items) or 1)
        page = items[start : start + limit]
        next_offset = start + len(page)
        next_page_token = (
            encode_page_token(scope=scope, filters=filters, offset=next_offset)
            if next_offset < len(items)
            else ""
        )
        return runtime_pb2.ListRuntimeUnitsResponse(
            runtime_units=page,
            next_page_token=next_page_token,
            cursor=_cursor("units", request.run_id, request.stage_id, next_page_token),
        )

    def list_sandboxes(
        self, request: runtime_pb2.ListSandboxesRequest
    ) -> runtime_pb2.ListSandboxesResponse:
        items = self.sandboxes.list(request.run_id, job_id=request.job_id)
        scope = "sandboxes"
        filters = (request.run_id, request.job_id)
        start = decode_page_token(request.page_token, scope=scope, filters=filters)
        limit = int(request.limit or len(items) or 1)
        page = items[start : start + limit]
        next_offset = start + len(page)
        next_page_token = (
            encode_page_token(scope=scope, filters=filters, offset=next_offset)
            if next_offset < len(items)
            else ""
        )
        return runtime_pb2.ListSandboxesResponse(
            sandboxes=page,
            next_page_token=next_page_token,
            cursor=_cursor("sandboxes", request.run_id, request.job_id, next_page_token),
        )

    def publish_sandbox_event(
        self, request: runtime_pb2.PublishSandboxEventRequest
    ) -> runtime_pb2.PublishSandboxEventResponse:
        event = runtime_pb2.SandboxEvent()
        event.CopyFrom(request.event)
        manifest = self._ensure_run(event.run_id)
        existing_event = self.sandbox_events.find_by_event_id(event.event_id)
        if existing_event is not None:
            if not _same_message_payload(existing_event, event):
                raise RuntimeLifecycleError(
                    f"sandbox event {event.event_id} already exists with different payload"
                )
            if existing_event.run_id != event.run_id:
                raise RuntimeLifecycleError(
                    f"sandbox event {event.event_id} already exists for run {existing_event.run_id}"
                )
            return runtime_pb2.PublishSandboxEventResponse(event=existing_event)
        try:
            existing = self.sandboxes.get(event.run_id, event.sandbox_id)
        except KeyError:
            existing = None
        binding = scheduling_pb2.Binding()
        if event.HasField("binding"):
            binding.CopyFrom(event.binding)
        if binding.runtime_unit_id:
            runtime_unit = self.runtime_units.get_by_runtime_unit_id(
                event.run_id, binding.runtime_unit_id
            )
        elif binding.pending_unit_id:
            runtime_unit = self.runtime_units.get_by_runtime_unit_id(
                event.run_id, binding.pending_unit_id
            )
        else:
            runtime_unit = self.runtime_units.get_by_sandbox_id(event.run_id, event.sandbox_id)
        if not event.sandbox_id:
            event.sandbox_id = runtime_unit.sandbox_id
        if existing is not None:
            if event.generation < existing.generation:
                raise RuntimeLifecycleError("sandbox event generation must not go backwards")
            if event.generation == existing.generation and not (
                _is_allowed_same_generation_transition(existing.state, event.state)
            ):
                raise RuntimeLifecycleError("sandbox event transition is not allowed")
        confirmed_at = to_timestamp(self.clock().astimezone(UTC))
        sandbox = runtime_pb2.Sandbox(
            sandbox_id=event.sandbox_id,
            run_id=runtime_unit.run_id,
            job_id=runtime_unit.job_id,
            trace_id=runtime_unit.trace_id,
            state=event.state,
            generation=event.generation,
            binding=binding,
            share=_event_or_default_share(
                event,
                binding=binding,
                runtime_unit=runtime_unit,
                manifest=manifest,
                existing=existing,
            ),
            priority=_event_or_default_priority(
                event,
                runtime_unit=runtime_unit,
                manifest=manifest,
                existing=existing,
            ),
            safe_point=event.safe_point,
            offloaded=_event_or_default_offloaded(
                event,
                existing=existing,
            ),
            observed_at=event.occurred_at,
            data_kind=event.data_kind or manifest.data_kind,
            semantic_context=event.semantic_context or manifest.semantic_context,
        )
        sandbox.last_confirmed_at.CopyFrom(confirmed_at)
        if (
            existing is None
            or existing.generation != event.generation
            or existing.state != event.state
        ):
            sandbox.state_changed_at.CopyFrom(confirmed_at)
        elif existing.HasField("state_changed_at"):
            sandbox.state_changed_at.CopyFrom(existing.state_changed_at)
        elif existing.HasField("observed_at"):
            # A legacy payload has only observed_at. Materialize that effective
            # transition time before replacing observed_at with the new event.
            sandbox.state_changed_at.CopyFrom(existing.observed_at)
        candidate_unit = runtime_pb2.RuntimeUnit()
        candidate_unit.CopyFrom(runtime_unit)
        candidate_unit.generation = max(candidate_unit.generation, event.generation)
        current_units = self.runtime_units.list(event.run_id)
        candidate_units: list[runtime_pb2.RuntimeUnit] = []
        for item in current_units:
            if item.runtime_unit_id == candidate_unit.runtime_unit_id:
                updated = runtime_pb2.RuntimeUnit()
                updated.CopyFrom(candidate_unit)
                candidate_units.append(updated)
            else:
                candidate_units.append(item)
        current_sandboxes = self.sandboxes.list(event.run_id)
        candidate_sandboxes: list[runtime_pb2.Sandbox] = []
        replaced = False
        for item in current_sandboxes:
            if item.sandbox_id == sandbox.sandbox_id:
                updated_sandbox = runtime_pb2.Sandbox()
                updated_sandbox.CopyFrom(sandbox)
                candidate_sandboxes.append(updated_sandbox)
                replaced = True
            else:
                candidate_sandboxes.append(item)
        if not replaced:
            candidate_sandboxes.append(sandbox)
        next_sequence = self.versions.peek("runtime_event_sequence") + 1
        projected_units, status = self._project_runtime_status_candidate(
            run_id=event.run_id,
            runtime_units=candidate_units,
            sandboxes=candidate_sandboxes,
            sequence=next_sequence,
            latest_event=event,
        )
        persisted_unit = next(
            (
                unit
                for unit in projected_units
                if unit.runtime_unit_id == candidate_unit.runtime_unit_id
            ),
            candidate_unit,
        )
        commit = self.persistence.persist_runtime_observation(
            RuntimeObservationBatch(
                sandbox=sandbox,
                runtime_unit=persisted_unit,
                event=event,
                component_status=status,
            )
        )
        self.versions.restore({"runtime_event_sequence": commit.runtime_event_sequence})
        stored = self._apply_runtime_observation_commit(
            commit=commit,
            projected_units=projected_units,
        )
        return runtime_pb2.PublishSandboxEventResponse(event=stored)

    def get_runtime_status(
        self, request: runtime_pb2.GetRuntimeStatusRequest
    ) -> runtime_pb2.GetRuntimeStatusResponse:
        return runtime_pb2.GetRuntimeStatusResponse(
            manifest=self._ensure_run(request.run_id),
            runtime_units=self.runtime_units.list(request.run_id),
            sandboxes=self.sandboxes.list(request.run_id),
            cursor=_cursor("status", request.run_id),
        )

    def watch_runtime_events(
        self, request: runtime_pb2.WatchRuntimeEventsRequest
    ) -> list[runtime_pb2.WatchRuntimeEventsResponse]:
        after_sequence = 0
        if request.after_cursor:
            after_sequence, after_event_id = _decode_sequence_cursor(request.after_cursor)
            matched_sequence = self.sandbox_events.sequence_for_event_id(after_event_id)
            if matched_sequence is not None:
                after_sequence = max(after_sequence, matched_sequence)
        elif request.after_event_id:
            matched_sequence = self.sandbox_events.sequence_for_event_id(request.after_event_id)
            if matched_sequence is not None:
                after_sequence = matched_sequence
            elif request.after_generation:
                for sequence, event in self.sandbox_events.list_with_sequences(request.run_id):
                    if (
                        event.generation == request.after_generation
                        and event.event_id == request.after_event_id
                    ):
                        after_sequence = sequence
                        break
        elif request.after_generation:
            for sequence, event in self.sandbox_events.list_with_sequences(request.run_id):
                if event.generation <= request.after_generation:
                    after_sequence = sequence
                else:
                    break
        return [
            runtime_pb2.WatchRuntimeEventsResponse(
                event=event,
                cursor=_sequence_cursor(sequence, event.event_id),
                sequence=sequence,
            )
            for sequence, event in self.sandbox_events.list_with_sequences(
                request.run_id, after_sequence=after_sequence
            )
        ]

    def create_replay(
        self, request: experiment_pb2.CreateReplayRequest
    ) -> experiment_pb2.CreateReplayResponse:
        replay = self.experiments.create_replay(request.replay, created_at=datetime.now(tz=UTC))
        replay.cursor = _cursor("replay", replay.replay_id)
        self.experiments.store.put_replay(replay)
        self.persistence.record_replay(replay)
        return experiment_pb2.CreateReplayResponse(replay=replay, cursor=replay.cursor)

    async def apply_replay_command(
        self, request: experiment_pb2.ApplyReplayCommandRequest
    ) -> experiment_pb2.ApplyReplayCommandResponse:
        if not request.idempotency_key:
            raise RuntimeLifecycleError("replay command idempotency_key is required")
        async with self.experiments.lock_for_replay(request.replay_id):
            return await self._apply_replay_command_locked(request)

    async def _apply_replay_command_locked(
        self, request: experiment_pb2.ApplyReplayCommandRequest
    ) -> experiment_pb2.ApplyReplayCommandResponse:
        existing = self.experiments.store.get_replay(request.replay_id)
        if self.experiments.command_is_idempotent(
            existing, request.command, request.idempotency_key
        ):
            return experiment_pb2.ApplyReplayCommandResponse(
                replay=existing, cursor=existing.cursor
            )
        if request.command == experiment_pb2.REPLAY_COMMAND_TYPE_START:
            if self.scheduler_client is None or not hasattr(self.scheduler_client, "schedule"):
                raise RuntimeLifecycleError(
                    "replay START requires a scheduler client with preview support"
                )
            experiment_id = existing.annotations.get("experiment_id") or (
                f"experiment:{existing.replay_id}"
            )
            try:
                replay = await self._start_replay_durably(
                    existing,
                    idempotency_key=request.idempotency_key,
                    scheduler=cast(ReplayScheduler, self.scheduler_client),
                )
            except Exception:
                failed_replay = self.experiments.store.get_replay(request.replay_id)
                self.persistence.record_replay(failed_replay)
                try:
                    failed_experiment = self.experiments.store.get_experiment(experiment_id)
                except KeyError:
                    pass
                else:
                    self.persistence.record_experiment(failed_experiment)
                raise
            self.persistence.record_experiment(self.experiments.store.get_experiment(experiment_id))
        else:
            replay = self.experiments.apply_replay_command(
                request.replay_id, request.command, now=datetime.now(tz=UTC)
            )
        replay.cursor = _cursor("replay", replay.replay_id, replay.state, replay.applied_events)
        replay = self.experiments.commit_command(replay, request.command, request.idempotency_key)
        self.persistence.record_replay(replay)
        return experiment_pb2.ApplyReplayCommandResponse(replay=replay, cursor=replay.cursor)

    async def _start_replay_durably(
        self,
        replay: experiment_pb2.Replay,
        *,
        idempotency_key: str,
        scheduler: ReplayScheduler,
    ) -> experiment_pb2.Replay:
        steps = decode_replay_steps(replay)
        step_artifacts = sorted(
            (
                artifact
                for artifact in replay.artifacts
                if artifact.kind == REPLAY_STEP_ARTIFACT_KIND
            ),
            key=lambda artifact: artifact.replay_step.ordinal,
        )
        key_digest = replay_start_key_digest(idempotency_key)
        completed = self.persistence.list_replay_schedule_steps(
            replay_id=replay.replay_id, start_key_digest=key_digest
        )
        if len(completed) > len(steps):
            raise RuntimeLifecycleError("durable replay progress exceeds replay inputs")
        runner = SchedulerReplayRunner(steps, scheduler=scheduler, seed=replay.seed)
        for ordinal, item in enumerate(completed, start=1):
            step = steps[ordinal - 1]
            if item.ordinal != ordinal or item.step_digest not in replay_step_progress_digests(
                step_artifacts[ordinal - 1], step
            ):
                raise RuntimeLifecycleError("durable replay progress does not match inputs")
            runner.restore_decision(item.decision)
        while runner.remaining:
            result = await runner.step()
            if result is None:  # pragma: no cover - guarded by remaining
                break
            step = steps[result.ordinal - 1]
            self.persistence.record_replay_schedule_step(
                ReplayScheduleStep(
                    replay_id=replay.replay_id,
                    start_key_digest=key_digest,
                    ordinal=result.ordinal,
                    step_digest=step_artifacts[result.ordinal - 1].digest,
                    decision=result.decision,
                    completed_at=datetime.now(tz=UTC),
                )
            )
        return await self.experiments.complete_replay(
            replay_id=replay.replay_id, steps=steps, results=runner.results
        )

    def create_experiment(
        self, request: experiment_pb2.CreateExperimentRequest
    ) -> experiment_pb2.CreateExperimentResponse:
        experiment = self.experiments.create_experiment(
            request.experiment, created_at=datetime.now(tz=UTC)
        )
        experiment.cursor = _cursor("experiment", experiment.experiment_id)
        self.experiments.store.put_experiment(experiment)
        self.persistence.record_experiment(experiment)
        return experiment_pb2.CreateExperimentResponse(
            experiment=experiment, cursor=experiment.cursor
        )
