"""Runtime supervisor, grpc servicers, and experiment lifecycle tests."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import grpc
import pytest
from tgsrl.v1 import (
    control_pb2,
    control_pb2_grpc,
    execution_pb2,
    experiment_pb2,
    experiment_pb2_grpc,
    operator_pb2,
    operator_pb2_grpc,
    resource_pb2,
    runtime_pb2,
    runtime_pb2_grpc,
    scheduling_pb2,
    trace_pb2,
)
from tgsrl_runtime import config as runtime_config
from tgsrl_runtime.duration import to_timestamp
from tgsrl_runtime.executor import FakeProcessDriver
from tgsrl_runtime.job_control_client import JobControlClient
from tgsrl_runtime.operator_client import OperatorClient
from tgsrl_runtime.persistence_adapter import NullPersistenceHook, SQLitePersistenceHook
from tgsrl_runtime.replay import ReplayArtifactStep, encode_replay_step_artifact
from tgsrl_runtime.runtime_app import _parser, build_runtime_supervisor
from tgsrl_runtime.runtime_errors import RuntimeLifecycleError
from tgsrl_runtime.runtime_transport import ExperimentServicer, RuntimeControlServicer
from tgsrl_runtime.supervisor import RuntimeSupervisor

from adapters import (
    GRPOAdapter,
    LifecycleAction,
    PartialAsyncRolloutAdapter,
    build_execution_contract,
)


class _SchedulerStub:
    def __init__(self) -> None:
        self.published: list[scheduling_pb2.SchedulingIntent] = []

    async def publish_intent(self, intent: scheduling_pb2.SchedulingIntent) -> None:
        self.published.append(intent)

    async def schedule(
        self,
        intent: scheduling_pb2.SchedulingIntent,
        snapshot: resource_pb2.ClusterSnapshot,
    ) -> scheduling_pb2.DecisionRecord:
        return scheduling_pb2.DecisionRecord(
            decision_id=f"replay:{intent.stage_id}",
            execution_id=intent.execution_id,
            stage_id=intent.stage_id,
            intent_version=intent.version,
            snapshot_revision=snapshot.revision,
            policy_version=intent.policy_version,
            deterministic_seed=intent.deterministic_seed,
            data_kind=trace_pb2.DATA_KIND_REPLAY,
        )


class _PartialFailScheduler(_SchedulerStub):
    def __init__(self, *, fail_call: int) -> None:
        super().__init__()
        self.fail_call = fail_call
        self.attempted: list[scheduling_pb2.SchedulingIntent] = []
        self._failed = False

    async def publish_intent(
        self, intent: scheduling_pb2.SchedulingIntent
    ) -> scheduling_pb2.PublishIntentResponse:
        clone = scheduling_pb2.SchedulingIntent()
        clone.CopyFrom(intent)
        self.attempted.append(clone)
        if len(self.attempted) == self.fail_call and not self._failed:
            self._failed = True
            raise RuntimeError("scheduler partial failure")
        self.published.append(clone)
        return scheduling_pb2.PublishIntentResponse(
            status=scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED,
            execution_id=intent.execution_id,
            stage_id=intent.stage_id,
            version=intent.version,
        )


class _DeduplicatingScheduler(_SchedulerStub):
    def __init__(self) -> None:
        super().__init__()
        self.responses: list[tuple[scheduling_pb2.SchedulingIntent, int]] = []
        self._seen_keys: set[str] = set()

    async def publish_intent(
        self, intent: scheduling_pb2.SchedulingIntent
    ) -> scheduling_pb2.PublishIntentResponse:
        clone = scheduling_pb2.SchedulingIntent()
        clone.CopyFrom(intent)
        status = scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED
        if intent.idempotency_key in self._seen_keys:
            status = scheduling_pb2.INTENT_PUBLISH_STATUS_DEDUPLICATED
        else:
            self._seen_keys.add(intent.idempotency_key)
        self.published.append(clone)
        self.responses.append((clone, status))
        return scheduling_pb2.PublishIntentResponse(
            status=status,
            execution_id=intent.execution_id,
            stage_id=intent.stage_id,
            version=intent.version,
        )


class _JobControlStub:
    def __init__(self) -> None:
        self.reported: list[control_pb2.ComponentStatus] = []

    async def report_component_status(
        self, component_status: control_pb2.ComponentStatus
    ) -> control_pb2.ComponentStatus:
        self.reported.append(component_status)
        return component_status


class _JobControlGRPCServicer(control_pb2_grpc.JobControlServiceServicer):
    def __init__(self) -> None:
        self.reported: list[control_pb2.ComponentStatus] = []

    async def ReportComponentStatus(
        self,
        request: control_pb2.ReportComponentStatusRequest,
        context: grpc.aio.ServicerContext[
            control_pb2.ReportComponentStatusRequest,
            control_pb2.ReportComponentStatusResponse,
        ],
    ) -> control_pb2.ReportComponentStatusResponse:
        del context
        status = control_pb2.ComponentStatus()
        status.CopyFrom(request.component_status)
        self.reported.append(status)
        return control_pb2.ReportComponentStatusResponse(component_status=status)


class _OperatorStub:
    def __init__(
        self,
        *,
        accepted: bool = True,
        detail: str = "accepted",
        error: grpc.aio.AioRpcError | None = None,
    ) -> None:
        self.accepted = accepted
        self.detail = detail
        self.error = error
        self.requests: list[operator_pb2.ApplyRuntimeControlRequest] = []

    async def apply_runtime_control(
        self, request: operator_pb2.ApplyRuntimeControlRequest
    ) -> operator_pb2.ApplyRuntimeControlResponse:
        clone = operator_pb2.ApplyRuntimeControlRequest()
        clone.CopyFrom(request)
        self.requests.append(clone)
        if self.error is not None:
            raise self.error
        return operator_pb2.ApplyRuntimeControlResponse(
            accepted=self.accepted,
            backend_revision=9,
            detail=self.detail,
        )


class _FailingObservationPersistence(SQLitePersistenceHook):
    def persist_runtime_observation(
        self,
        batch: Any,
    ) -> Any:
        raise sqlite3.OperationalError("inject runtime observation failure")


class _FailingStartAckPersistence(SQLitePersistenceHook):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self._failed = False

    def save_runtime_units(
        self, runtime_units: Sequence[runtime_pb2.RuntimeUnit]
    ) -> list[runtime_pb2.RuntimeUnit]:
        if not self._failed and any(
            unit.annotations.get("tgsrl.start_intent_ack", "") for unit in runtime_units
        ):
            self._failed = True
            raise sqlite3.OperationalError("inject start ack persistence failure")
        return super().save_runtime_units(runtime_units)


class _OperatorGRPCServicer(operator_pb2_grpc.RuntimeBackendControlServiceServicer):
    def __init__(self) -> None:
        self.requests: list[operator_pb2.ApplyRuntimeControlRequest] = []
        self.before_response: (
            Callable[[operator_pb2.ApplyRuntimeControlRequest], Awaitable[None]] | None
        ) = None

    async def ApplyRuntimeControl(
        self,
        request: operator_pb2.ApplyRuntimeControlRequest,
        context: grpc.aio.ServicerContext[
            operator_pb2.ApplyRuntimeControlRequest,
            operator_pb2.ApplyRuntimeControlResponse,
        ],
    ) -> operator_pb2.ApplyRuntimeControlResponse:
        del context
        clone = operator_pb2.ApplyRuntimeControlRequest()
        clone.CopyFrom(request)
        self.requests.append(clone)
        if self.before_response is not None:
            await self.before_response(clone)
        return operator_pb2.ApplyRuntimeControlResponse(
            accepted=True, backend_revision=12, detail="accepted"
        )


class _Context:
    pass


def _context() -> grpc.aio.ServicerContext[Any, Any]:
    return cast(grpc.aio.ServicerContext[Any, Any], _Context())


class _AbortContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details = ""

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        self.code = code
        self.details = details
        raise RuntimeError(f"{code.name}: {details}")


def _manifest() -> runtime_pb2.RuntimeManifest:
    return runtime_pb2.RuntimeManifest(
        manifest_id="manifest-1",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        framework="fake",
        execution_backend="fake",
        trainer="fake",
        rollout_engine="fake",
        compatibility_profile="cpu-mock",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
        execution_contract=build_execution_contract(GRPOAdapter(), PartialAsyncRolloutAdapter()),
        annotations={"algorithm": "grpo"},
        resources_per_unit=resource_pb2.ResourceVector(
            cpu_millis=2000, memory_bytes=2 << 30, accelerator_units=0.5
        ),
        desired_units=3,
        priority=7,
        queue="gold",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version="policy-1",
        deterministic_seed=11,
        command=["python", "-c", "print('runtime started')"],
        environment={"MODE": "test"},
        working_directory="/tmp",
    )


def _make_sandbox_event(
    *,
    runtime_unit: runtime_pb2.RuntimeUnit,
    event_id: str,
    event_type: int,
    state: int,
    generation: int = 1,
    binding_id: str = "",
    sandbox_id: str = "",
) -> runtime_pb2.SandboxEvent:
    selected_sandbox_id = (
        sandbox_id or runtime_unit.sandbox_id or f"sandbox:{runtime_unit.run_id}:unit"
    )
    return runtime_pb2.SandboxEvent(
        event_id=event_id,
        event_type=event_type,
        sandbox_id=selected_sandbox_id,
        run_id=runtime_unit.run_id,
        job_id=runtime_unit.job_id,
        trace_id=runtime_unit.trace_id,
        generation=generation,
        state=state,
        binding=scheduling_pb2.Binding(
            binding_id=binding_id or f"binding:{event_id}",
            pending_unit_id=runtime_unit.runtime_unit_id,
            sandbox_id=selected_sandbox_id,
            generation=generation,
        ),
        occurred_at=runtime_unit.observed_at,
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )


@pytest.mark.asyncio
async def test_supervisor_full_fake_lifecycle_publishes_intents() -> None:
    scheduler = _SchedulerStub()
    supervisor = RuntimeSupervisor(scheduler_client=scheduler)

    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=_manifest(), request_id="req-1", idempotency_key="idem-1"
        )
    )
    assert validate.valid
    assert validate.cursor

    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate.normalized_manifest, request_id="req-2", idempotency_key="idem-2"
        )
    )
    assert compiled.runtime_units
    assert compiled.cursor

    prepared = supervisor.prepare_runtime(
        runtime_pb2.PrepareRuntimeRequest(
            run_id="run-1", request_id="req-3", idempotency_key="idem-3"
        )
    )
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in prepared.runtime_units)
    assert all(unit.generation == 0 for unit in prepared.runtime_units)
    assert supervisor.sandboxes.list("run-1") == []
    assert prepared.cursor

    started = await supervisor.start_runtime(
        runtime_pb2.StartRuntimeRequest(
            run_id="run-1", request_id="req-4", idempotency_key="idem-4"
        )
    )
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in started.runtime_units)
    assert all(unit.generation == 1 for unit in started.runtime_units)
    assert all(unit.status_reason == "starting" for unit in started.runtime_units)
    assert scheduler.published
    assert len(scheduler.published) == len(started.runtime_units)
    assert all(intent.run_id == "run-1" for intent in scheduler.published)
    assert all(intent.cursor for intent in scheduler.published)
    assert all(intent.generation == 1 for intent in scheduler.published)
    assert all("sandbox_id" not in intent.labels for intent in scheduler.published)
    assert all(intent.unit_count == 3 for intent in scheduler.published)
    assert all(intent.priority == 7 for intent in scheduler.published)
    assert all(intent.queue == "gold" for intent in scheduler.published)
    assert all(intent.deterministic_seed == 11 for intent in scheduler.published)
    assert all(intent.resources_per_unit.cpu_millis == 2000 for intent in scheduler.published)
    assert all(intent.resources_per_unit.accelerator_units == 0.5 for intent in scheduler.published)
    assert started.cursor
    assert supervisor.sandboxes.list("run-1") == []
    fake_driver = supervisor.fake_process_driver
    assert isinstance(fake_driver, FakeProcessDriver)
    assert len(fake_driver.start_calls) == 0

    applied = supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=runtime_pb2.SandboxEvent(
                event_id="sandbox-event-1",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                sandbox_id="sandbox:run-1:decode",
                run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
                generation=1,
                state=runtime_pb2.RUNTIME_STATE_BOUND,
                binding=scheduling_pb2.Binding(
                    binding_id="binding-1",
                    pending_unit_id="run-1:decode",
                    device_ids=["gpu-0"],
                    resources=started.runtime_units[1].requested_resources,
                    sandbox_id="sandbox:run-1:decode",
                    generation=1,
                ),
                detail="bound by provider",
                occurred_at=started.runtime_units[1].observed_at,
                data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
            )
        )
    )
    assert applied.event.state == runtime_pb2.RUNTIME_STATE_BOUND
    converged_unit = supervisor.runtime_units.get_by_runtime_unit_id("run-1", "run-1:decode")
    assert converged_unit.state == runtime_pb2.RUNTIME_STATE_UNKNOWN

    paused = supervisor.pause_runtime(
        runtime_pb2.PauseRuntimeRequest(
            run_id="run-1", request_id="req-5", idempotency_key="idem-5", reason="operator"
        )
    )
    paused_decode = next(
        unit for unit in paused.runtime_units if unit.runtime_unit_id == "run-1:decode"
    )
    paused_other = [unit for unit in paused.runtime_units if unit.runtime_unit_id != "run-1:decode"]
    assert paused_decode.state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in paused_other)
    assert all(unit.status_reason == "pausing" for unit in paused.runtime_units)
    assert len(supervisor.sandbox_events.list("run-1")) == 1
    assert paused.cursor

    resumed = supervisor.resume_runtime(
        runtime_pb2.ResumeRuntimeRequest(
            run_id="run-1", request_id="req-6", idempotency_key="idem-6"
        )
    )
    resumed_decode = next(
        unit for unit in resumed.runtime_units if unit.runtime_unit_id == "run-1:decode"
    )
    resumed_other = [
        unit for unit in resumed.runtime_units if unit.runtime_unit_id != "run-1:decode"
    ]
    assert resumed_decode.state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in resumed_other)
    assert all(unit.status_reason == "resuming" for unit in resumed.runtime_units)
    assert resumed.cursor

    checkpoint = supervisor.checkpoint_runtime(
        runtime_pb2.CheckpointRuntimeRequest(
            run_id="run-1", request_id="req-7", idempotency_key="idem-7"
        )
    )
    assert checkpoint.checkpoint_ref.startswith("checkpoint:run-1:")
    assert supervisor.checkpoints.latest("run-1") is not None
    assert checkpoint.cursor

    stopped = supervisor.stop_runtime(
        runtime_pb2.StopRuntimeRequest(
            run_id="run-1", request_id="req-8", idempotency_key="idem-8", reason="done"
        )
    )
    stopped_decode = next(
        unit for unit in stopped.runtime_units if unit.runtime_unit_id == "run-1:decode"
    )
    stopped_other = [
        unit for unit in stopped.runtime_units if unit.runtime_unit_id != "run-1:decode"
    ]
    assert stopped_decode.state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in stopped_other)
    assert all(unit.status_reason == "stopping" for unit in stopped.runtime_units)
    assert stopped.cursor

    terminated = supervisor.terminate_runtime(
        runtime_pb2.TerminateRuntimeRequest(
            run_id="run-1", request_id="req-9", idempotency_key="idem-9", reason="cleanup"
        )
    )
    terminated_decode = next(
        unit for unit in terminated.runtime_units if unit.runtime_unit_id == "run-1:decode"
    )
    terminated_other = [
        unit for unit in terminated.runtime_units if unit.runtime_unit_id != "run-1:decode"
    ]
    assert terminated_decode.state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in terminated_other)
    assert all(unit.status_reason == "terminating" for unit in terminated.runtime_units)
    status = supervisor.get_runtime_status(runtime_pb2.GetRuntimeStatusRequest(run_id="run-1"))
    assert status.manifest.run_id == "run-1"
    assert status.sandboxes
    assert supervisor.sandbox_events.list("run-1")
    assert status.cursor


def test_publish_sandbox_event_is_idempotent_for_same_event_id_and_payload() -> None:
    supervisor = RuntimeSupervisor()
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    event = runtime_pb2.SandboxEvent(
        event_id="sandbox-event-dup",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        sandbox_id="sandbox:run-1:decode",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        generation=1,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding=scheduling_pb2.Binding(
            binding_id="binding-dup",
            pending_unit_id=compiled.runtime_units[1].runtime_unit_id,
            sandbox_id="sandbox:run-1:decode",
            generation=1,
        ),
        occurred_at=compiled.runtime_units[1].observed_at,
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    first = supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=event))
    second = supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=event))
    assert first.event.event_id == second.event.event_id
    assert len(supervisor.sandbox_events.list("run-1")) == 1


def test_publish_sandbox_event_rejects_same_event_id_with_different_payload() -> None:
    supervisor = RuntimeSupervisor()
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    event = runtime_pb2.SandboxEvent(
        event_id="sandbox-event-conflict",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        sandbox_id="sandbox:run-1:decode",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        generation=1,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding=scheduling_pb2.Binding(
            binding_id="binding-conflict",
            pending_unit_id=compiled.runtime_units[1].runtime_unit_id,
            sandbox_id="sandbox:run-1:decode",
            generation=1,
        ),
        occurred_at=compiled.runtime_units[1].observed_at,
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=event))
    conflicting = runtime_pb2.SandboxEvent()
    conflicting.CopyFrom(event)
    conflicting.detail = "different"
    with pytest.raises(RuntimeLifecycleError, match="already exists with different payload"):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=conflicting))


def test_publish_sandbox_event_rejects_same_event_id_reused_by_different_run() -> None:
    supervisor = RuntimeSupervisor()
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    first_event = _make_sandbox_event(
        runtime_unit=compiled.runtime_units[1],
        event_id="sandbox-event-global",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding_id="binding-global",
        sandbox_id="sandbox:run-1:decode",
    )
    supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=first_event))

    manifest_two = _manifest()
    manifest_two.run_id = "run-2"
    manifest_two.manifest_id = "manifest-2"
    manifest_two.job_id = "job-2"
    manifest_two.trace_id = "trace-2"
    validate_two = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=manifest_two, request_id="req-3")
    )
    compiled_two = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate_two.normalized_manifest,
            request_id="req-4",
        )
    )
    conflicting = _make_sandbox_event(
        runtime_unit=compiled_two.runtime_units[1],
        event_id="sandbox-event-global",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding_id="binding-global-two",
        sandbox_id="sandbox:run-2:decode",
    )

    with pytest.raises(RuntimeLifecycleError, match="already exists with different payload"):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=conflicting))


def test_publish_sandbox_event_persist_failure_keeps_memory_and_db_unchanged(
    tmp_path: Path,
) -> None:
    state_db = tmp_path / "runtime-observation-fail.sqlite3"
    persistence = _FailingObservationPersistence(str(state_db))
    supervisor = RuntimeSupervisor(persistence=persistence)
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    runtime_unit = compiled.runtime_units[1]
    event = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-fail",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding_id="binding-fail",
        sandbox_id="sandbox:run-1:decode",
    )

    before_sandboxes = supervisor.sandboxes.list("run-1")
    before_events = supervisor.sandbox_events.list("run-1")
    before_components = dict(supervisor.component_statuses)
    with pytest.raises(sqlite3.OperationalError, match="inject runtime observation failure"):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=event))

    assert supervisor.sandboxes.list("run-1") == before_sandboxes
    assert supervisor.sandbox_events.list("run-1") == before_events
    assert supervisor.component_statuses == before_components

    persisted = SQLitePersistenceHook(str(state_db))
    try:
        assert persisted._runtime.list_sandboxes(run_id="run-1", limit=10).items == []
        assert persisted._runtime.watch_events(run_id="run-1", limit=10).items == []
        assert persisted._runtime.list_component_statuses(run_id="run-1", limit=10).items == []
    finally:
        persisted.close()
        persistence.close()


def test_publish_sandbox_event_allows_explicit_same_generation_transitions() -> None:
    supervisor = RuntimeSupervisor()
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    runtime_unit = compiled.runtime_units[1]
    bound = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-bound",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    running = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-running",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    paused = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-paused",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED,
        state=runtime_pb2.RUNTIME_STATE_PAUSED,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    resumed = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-resumed",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    sleeping = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-sleeping",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_SLEEPING,
        state=runtime_pb2.RUNTIME_STATE_SLEEPING,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    woke = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-woke",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    failed = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-failed",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_FAILED,
        state=runtime_pb2.RUNTIME_STATE_FAILED,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    for event in (bound, running, paused, resumed, sleeping, woke, failed):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=event))
    unit = supervisor.runtime_units.get_by_runtime_unit_id("run-1", runtime_unit.runtime_unit_id)
    assert unit.state == runtime_pb2.RUNTIME_STATE_FAILED

    rollback = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-rollback",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding_id="binding-forward",
        sandbox_id="sandbox:run-1:decode",
    )
    with pytest.raises(RuntimeLifecycleError, match="transition is not allowed"):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=rollback))


def test_publish_sandbox_event_rejects_older_generation() -> None:
    supervisor = RuntimeSupervisor()
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    newer = runtime_pb2.SandboxEvent(
        event_id="sandbox-event-newer",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        sandbox_id="sandbox:run-1:decode",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        generation=2,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding=scheduling_pb2.Binding(
            binding_id="binding-newer",
            pending_unit_id=compiled.runtime_units[1].runtime_unit_id,
            sandbox_id="sandbox:run-1:decode",
            generation=2,
        ),
        occurred_at=compiled.runtime_units[1].observed_at,
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=newer))
    older = runtime_pb2.SandboxEvent()
    older.CopyFrom(newer)
    older.event_id = "sandbox-event-older"
    older.generation = 1
    older.binding.generation = 1
    with pytest.raises(RuntimeLifecycleError, match="must not go backwards"):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=older))


def test_publish_sandbox_event_rejects_same_generation_terminated_rollback() -> None:
    supervisor = RuntimeSupervisor()
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    runtime_unit = compiled.runtime_units[1]
    terminated = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-terminated",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_TERMINATED,
        state=runtime_pb2.RUNTIME_STATE_TERMINATED,
        binding_id="binding-terminated",
        sandbox_id="sandbox:run-1:decode",
    )
    supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=terminated))
    rollback = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sandbox-event-terminated-rollback",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        binding_id="binding-terminated",
        sandbox_id="sandbox:run-1:decode",
    )
    with pytest.raises(RuntimeLifecycleError, match="transition is not allowed"):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=rollback))


def test_watch_runtime_events_uses_stable_append_sequence_cursor() -> None:
    supervisor = RuntimeSupervisor()
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    runtime_unit = compiled.runtime_units[1]
    first = supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=runtime_unit,
                event_id="sandbox-event-a",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                state=runtime_pb2.RUNTIME_STATE_BOUND,
                binding_id="binding-seq",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    second = supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=runtime_unit,
                event_id="sandbox-event-c",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
                state=runtime_pb2.RUNTIME_STATE_RUNNING,
                binding_id="binding-seq",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    third = supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=runtime_unit,
                event_id="sandbox-event-b",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED,
                state=runtime_pb2.RUNTIME_STATE_PAUSED,
                binding_id="binding-seq",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    del first, second, third

    events = supervisor.watch_runtime_events(runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1"))
    assert [response.sequence for response in events] == [1, 2, 3]
    assert [response.event.event_id for response in events] == [
        "sandbox-event-a",
        "sandbox-event-c",
        "sandbox-event-b",
    ]

    resumed = supervisor.watch_runtime_events(
        runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1", after_cursor=events[1].cursor)
    )
    assert [response.sequence for response in resumed] == [3]
    assert [response.event.event_id for response in resumed] == ["sandbox-event-b"]

    legacy = supervisor.watch_runtime_events(
        runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1", after_event_id="sandbox-event-c")
    )
    assert [response.sequence for response in legacy] == [3]
    assert [response.event.event_id for response in legacy] == ["sandbox-event-b"]

    after_generation = supervisor.watch_runtime_events(
        runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1", after_generation=1)
    )
    assert after_generation == []


def test_sqlite_publish_sandbox_event_uses_global_sequence_across_runs(tmp_path: Path) -> None:
    state_db = tmp_path / "runtime-events-global.sqlite3"
    supervisor = RuntimeSupervisor(persistence=SQLitePersistenceHook(str(state_db)))
    manifest_one = _manifest()
    validate_one = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=manifest_one, request_id="req-1")
    )
    compiled_one = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate_one.normalized_manifest,
            request_id="req-2",
        )
    )
    manifest_two = _manifest()
    manifest_two.run_id = "run-2"
    manifest_two.manifest_id = "manifest-2"
    manifest_two.job_id = "job-2"
    manifest_two.trace_id = "trace-2"
    validate_two = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=manifest_two, request_id="req-3")
    )
    compiled_two = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate_two.normalized_manifest,
            request_id="req-4",
        )
    )

    supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=compiled_one.runtime_units[1],
                event_id="sqlite-run1-event",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                state=runtime_pb2.RUNTIME_STATE_BOUND,
                binding_id="sqlite-run1",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=compiled_two.runtime_units[1],
                event_id="sqlite-run2-event",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                state=runtime_pb2.RUNTIME_STATE_BOUND,
                binding_id="sqlite-run2",
                sandbox_id="sandbox:run-2:decode",
            )
        )
    )

    run1_events = supervisor.watch_runtime_events(
        runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1")
    )
    run2_events = supervisor.watch_runtime_events(
        runtime_pb2.WatchRuntimeEventsRequest(run_id="run-2")
    )
    assert [response.sequence for response in run1_events] == [1]
    assert [response.sequence for response in run2_events] == [2]
    cast(SQLitePersistenceHook, supervisor.persistence).close()


def test_sqlite_publish_sandbox_event_conflict_keeps_memory_and_db_unchanged(
    tmp_path: Path,
) -> None:
    state_db = tmp_path / "runtime-events-conflict.sqlite3"
    persistence = SQLitePersistenceHook(str(state_db))
    supervisor = RuntimeSupervisor(persistence=persistence)
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )
    runtime_unit = compiled.runtime_units[1]
    event = _make_sandbox_event(
        runtime_unit=runtime_unit,
        event_id="sqlite-conflict-event",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
        state=runtime_pb2.RUNTIME_STATE_BOUND,
        binding_id="sqlite-conflict",
        sandbox_id="sandbox:run-1:decode",
    )
    supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=event))
    before_units = supervisor.runtime_units.list("run-1")
    before_sandboxes = supervisor.sandboxes.list("run-1")
    before_events = supervisor.sandbox_events.list("run-1")
    before_components = dict(supervisor.component_statuses)

    conflicting = runtime_pb2.SandboxEvent()
    conflicting.CopyFrom(event)
    conflicting.detail = "different"
    with pytest.raises(RuntimeLifecycleError, match="already exists with different payload"):
        supervisor.publish_sandbox_event(runtime_pb2.PublishSandboxEventRequest(event=conflicting))

    assert supervisor.runtime_units.list("run-1") == before_units
    assert supervisor.sandboxes.list("run-1") == before_sandboxes
    assert supervisor.sandbox_events.list("run-1") == before_events
    assert supervisor.component_statuses == before_components
    restored = RuntimeSupervisor(persistence=SQLitePersistenceHook(str(state_db)))
    try:
        responses = restored.watch_runtime_events(
            runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1")
        )
        assert [response.event.event_id for response in responses] == ["sqlite-conflict-event"]
        assert [response.sequence for response in responses] == [1]
    finally:
        cast(SQLitePersistenceHook, restored.persistence).close()
        persistence.close()


@pytest.mark.asyncio
async def test_runtime_and_experiment_servicers_share_supervisor_state() -> None:
    scheduler = _SchedulerStub()
    supervisor = RuntimeSupervisor(scheduler_client=scheduler)
    runtime_servicer = RuntimeControlServicer(supervisor)
    experiment_servicer = ExperimentServicer(supervisor)
    manifest = _manifest()

    await runtime_servicer.ValidateRuntime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=manifest, request_id="validate", idempotency_key="v"
        ),
        _context(),
    )
    await runtime_servicer.CompileRuntime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=manifest, request_id="compile", idempotency_key="c"
        ),
        _context(),
    )
    await runtime_servicer.PrepareRuntime(
        runtime_pb2.PrepareRuntimeRequest(
            run_id="run-1", request_id="prepare", idempotency_key="p"
        ),
        _context(),
    )
    await runtime_servicer.StartRuntime(
        runtime_pb2.StartRuntimeRequest(run_id="run-1", request_id="start", idempotency_key="s"),
        _context(),
    )

    replay = (
        await experiment_servicer.CreateReplay(
            experiment_pb2.CreateReplayRequest(
                replay=experiment_pb2.Replay(
                    replay_id="replay-1",
                    run_id="run-1",
                    trace_id="trace-1",
                    source_trace_ref="memory://trace-1",
                    speed=1.0,
                    seed=7,
                    data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
                    annotations={"experiment_id": "exp-1"},
                ),
                request_id="replay-create",
                idempotency_key="er1",
            ),
            _context(),
        )
    ).replay
    assert replay.state == experiment_pb2.REPLAY_STATE_PENDING
    assert replay.cursor

    await experiment_servicer.CreateExperiment(
        experiment_pb2.CreateExperimentRequest(
            experiment=experiment_pb2.Experiment(
                experiment_id="exp-1", display_name="runtime compare"
            ),
            request_id="exp-create",
            idempotency_key="ee1",
        ),
        _context(),
    )
    event = supervisor.trace_ingestor.list_causal("run-1")[0]
    intent = scheduling_pb2.SchedulingIntent()
    intent.CopyFrom(scheduler.published[0])
    intent.deterministic_seed = replay.seed
    step = ReplayArtifactStep(
        ordinal=1,
        event=event,
        intent=intent,
        snapshot=resource_pb2.ClusterSnapshot(
            snapshot_id="snapshot-1", revision=1, observed_at=event.occurred_at
        ),
    )
    replay = supervisor.experiments.store.get_replay("replay-1")
    replay.artifacts.append(encode_replay_step_artifact("replay-1", step))
    supervisor.experiments.store.put_replay(replay)

    replay = (
        await experiment_servicer.ApplyReplayCommand(
            experiment_pb2.ApplyReplayCommandRequest(
                replay_id="replay-1",
                command=experiment_pb2.REPLAY_COMMAND_TYPE_START,
                request_id="replay-start",
                idempotency_key="er2",
            ),
            _context(),
        )
    ).replay
    assert replay.state == experiment_pb2.REPLAY_STATE_COMPLETED
    assert replay.applied_events == 1
    assert replay.emitted_decisions == 1
    assert replay.artifacts
    assert replay.cursor
    fetched_replay = await experiment_servicer.GetReplay(
        experiment_pb2.GetReplayRequest(replay_id="replay-1"), _context()
    )
    payload_kinds = [
        artifact.WhichOneof("typed_payload") for artifact in fetched_replay.replay.artifacts
    ]
    assert payload_kinds == ["replay_step", "replay_decision"]
    assert fetched_replay.replay.artifacts[0].attributes == {}
    assert fetched_replay.replay.artifacts[1].attributes == {}

    experiment = supervisor.experiments.store.get_experiment("exp-1")
    assert experiment.state == experiment_pb2.EXPERIMENT_STATE_COMPLETED
    fetched = await experiment_servicer.GetExperiment(
        experiment_pb2.GetExperimentRequest(experiment_id="exp-1"), _context()
    )
    assert fetched.experiment.summary
    assert fetched.cursor

    reconciled = supervisor.experiments.reconcile_run(
        experiment_id="exp-1",
        run_id="run-1",
        trace_id="trace-1",
        kind=experiment_pb2.EXPERIMENT_RUN_KIND_SIMULATION,
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
        policy_version="policy-1",
        config_hash="cfg-sha256",
        code_revision="local",
    )
    assert reconciled.state == experiment_pb2.EXPERIMENT_STATE_COMPLETED
    assert reconciled.runs and reconciled.results


@pytest.mark.asyncio
async def test_replay_start_requires_persisted_artifacts_and_scheduler_preview() -> None:
    coordinator = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    servicer = ExperimentServicer(coordinator)
    await servicer.CreateReplay(
        experiment_pb2.CreateReplayRequest(
            replay=experiment_pb2.Replay(
                replay_id="missing-artifact",
                run_id="run-1",
                trace_id="trace-1",
                source_trace_ref="memory://trace-1",
                speed=1.0,
                seed=7,
                data_kind=trace_pb2.DATA_KIND_REPLAY,
            )
        ),
        _context(),
    )
    context = _AbortContext()
    with pytest.raises(RuntimeError, match=r"FAILED_PRECONDITION.*no scheduler input"):
        await servicer.ApplyReplayCommand(
            experiment_pb2.ApplyReplayCommandRequest(
                replay_id="missing-artifact",
                command=experiment_pb2.REPLAY_COMMAND_TYPE_START,
                idempotency_key="missing-artifact-start",
            ),
            cast(grpc.aio.ServicerContext[Any, Any], context),
        )
    assert context.code is grpc.StatusCode.FAILED_PRECONDITION
    replay = coordinator.experiments.store.get_replay("missing-artifact")
    assert replay.state == experiment_pb2.REPLAY_STATE_PENDING


@pytest.mark.asyncio
async def test_replay_start_requires_scheduler_preview_capability() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=None)
    servicer = ExperimentServicer(supervisor)
    await servicer.CreateReplay(
        experiment_pb2.CreateReplayRequest(
            replay=experiment_pb2.Replay(
                replay_id="missing-scheduler",
                run_id="run-1",
                trace_id="trace-1",
                source_trace_ref="memory://trace-1",
                speed=1.0,
                seed=7,
                data_kind=trace_pb2.DATA_KIND_REPLAY,
            )
        ),
        _context(),
    )
    context = _AbortContext()
    with pytest.raises(RuntimeError, match=r"FAILED_PRECONDITION.*scheduler client"):
        await servicer.ApplyReplayCommand(
            experiment_pb2.ApplyReplayCommandRequest(
                replay_id="missing-scheduler",
                command=experiment_pb2.REPLAY_COMMAND_TYPE_START,
                idempotency_key="missing-scheduler-start",
            ),
            cast(grpc.aio.ServicerContext[Any, Any], context),
        )
    assert context.code is grpc.StatusCode.FAILED_PRECONDITION


@pytest.mark.asyncio
async def test_replay_artifacts_and_decisions_survive_sqlite_restart(tmp_path: Path) -> None:
    state_db = tmp_path / "replay-state.sqlite3"
    persistence = SQLitePersistenceHook(str(state_db))
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub(), persistence=persistence)
    supervisor.create_replay(
        experiment_pb2.CreateReplayRequest(
            replay=experiment_pb2.Replay(
                replay_id="durable-replay",
                run_id="run-1",
                trace_id="trace-1",
                source_trace_ref="sqlite://trace-1",
                speed=1.0,
                seed=7,
                data_kind=trace_pb2.DATA_KIND_REPLAY,
                annotations={"experiment_id": "experiment:durable-replay"},
            )
        )
    )
    event = trace_pb2.TraceEvent(
        event_id="event-1",
        job_id="job-1",
        execution_id="execution-1",
        phase_id="decode",
        occurred_at=to_timestamp(datetime(2025, 1, 1, tzinfo=UTC)),
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
        algorithm="grpo",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version="policy-1",
        decision_id="recorded-1",
        sequence=1,
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        stage_id="decode",
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
    )
    step = ReplayArtifactStep(
        ordinal=1,
        event=event,
        intent=scheduling_pb2.SchedulingIntent(
            execution_id="execution-1",
            stage_id="decode",
            version=1,
            deterministic_seed=7,
            policy_version="policy-1",
        ),
        snapshot=resource_pb2.ClusterSnapshot(
            snapshot_id="snapshot-1", revision=1, observed_at=event.occurred_at
        ),
    )
    replay = supervisor.experiments.attach_replay_steps("durable-replay", [step])
    persistence.record_replay(replay)
    await supervisor.apply_replay_command(
        experiment_pb2.ApplyReplayCommandRequest(
            replay_id="durable-replay",
            command=experiment_pb2.REPLAY_COMMAND_TYPE_START,
            idempotency_key="durable-replay-start",
        )
    )
    persistence.close()

    restarted_persistence = SQLitePersistenceHook(str(state_db))
    restarted = RuntimeSupervisor(
        scheduler_client=_SchedulerStub(), persistence=restarted_persistence
    )
    hydrated = restarted.experiments.store.get_replay("durable-replay")
    assert hydrated.state == experiment_pb2.REPLAY_STATE_COMPLETED
    assert restarted.experiments.replay_steps("durable-replay")[0].event.event_id == "event-1"
    assert restarted.experiments.store.get_decisions("durable-replay")[0].decision_id
    restarted_persistence.close()


@pytest.mark.asyncio
async def test_replay_start_resumes_only_unfinished_steps_after_sqlite_restart(
    tmp_path: Path,
) -> None:
    class RecordingPreviewScheduler(_SchedulerStub):
        def __init__(self, *, fail_call: int | None = None) -> None:
            super().__init__()
            self.fail_call = fail_call
            self.calls: list[str] = []

        async def schedule(
            self,
            intent: scheduling_pb2.SchedulingIntent,
            snapshot: resource_pb2.ClusterSnapshot,
        ) -> scheduling_pb2.DecisionRecord:
            self.calls.append(intent.stage_id)
            if self.fail_call == len(self.calls):
                raise RuntimeError("replay preview interrupted")
            return await super().schedule(intent, snapshot)

    def replay_step(ordinal: int) -> ReplayArtifactStep:
        occurred_at = to_timestamp(datetime(2025, 1, 1, 0, 0, ordinal, tzinfo=UTC))
        return ReplayArtifactStep(
            ordinal=ordinal,
            event=trace_pb2.TraceEvent(
                event_id=f"event-{ordinal}",
                job_id="job-1",
                execution_id=f"execution-{ordinal}",
                phase_id=f"stage-{ordinal}",
                occurred_at=occurred_at,
                event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
                algorithm="grpo",
                rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
                policy_version="policy-1",
                decision_id=f"recorded-{ordinal}",
                phase_kind=execution_pb2.PHASE_KIND_DECODE,
                run_id="run-1",
                trace_id="trace-1",
                stage_id=f"stage-{ordinal}",
                data_kind=trace_pb2.DATA_KIND_REPLAY,
            ),
            intent=scheduling_pb2.SchedulingIntent(
                execution_id=f"execution-{ordinal}",
                stage_id=f"stage-{ordinal}",
                version=ordinal,
                deterministic_seed=7,
                policy_version="policy-1",
            ),
            snapshot=resource_pb2.ClusterSnapshot(
                snapshot_id=f"snapshot-{ordinal}",
                revision=ordinal,
                observed_at=occurred_at,
            ),
        )

    state_db = tmp_path / "replay-partial.sqlite3"
    failing_scheduler = RecordingPreviewScheduler(fail_call=2)
    persistence = SQLitePersistenceHook(str(state_db))
    supervisor = RuntimeSupervisor(scheduler_client=failing_scheduler, persistence=persistence)
    supervisor.create_replay(
        experiment_pb2.CreateReplayRequest(
            replay=experiment_pb2.Replay(
                replay_id="partial-replay",
                run_id="run-1",
                trace_id="trace-1",
                source_trace_ref="sqlite://trace-1",
                speed=1.0,
                seed=7,
                data_kind=trace_pb2.DATA_KIND_REPLAY,
            )
        )
    )
    replay = supervisor.experiments.attach_replay_steps(
        "partial-replay", [replay_step(1), replay_step(2)]
    )
    persistence.record_replay(replay)
    request = experiment_pb2.ApplyReplayCommandRequest(
        replay_id="partial-replay",
        command=experiment_pb2.REPLAY_COMMAND_TYPE_START,
        idempotency_key="partial-replay-start",
    )

    with pytest.raises(RuntimeError, match="replay preview interrupted"):
        await supervisor.apply_replay_command(request)
    assert failing_scheduler.calls == ["stage-1", "stage-2"]
    persistence.close()

    resumed_scheduler = RecordingPreviewScheduler()
    resumed_persistence = SQLitePersistenceHook(str(state_db))
    resumed = RuntimeSupervisor(scheduler_client=resumed_scheduler, persistence=resumed_persistence)
    completed = await resumed.apply_replay_command(request)

    assert resumed_scheduler.calls == ["stage-2"]
    assert completed.replay.state == experiment_pb2.REPLAY_STATE_COMPLETED
    assert completed.replay.applied_events == 2
    repeated = await resumed.apply_replay_command(request)
    assert repeated.replay == completed.replay
    assert resumed_scheduler.calls == ["stage-2"]
    resumed_persistence.close()


@pytest.mark.asyncio
async def test_experiment_grpc_start_executes_typed_replay_artifact() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    server = grpc.aio.server()
    experiment_pb2_grpc.add_ExperimentServiceServicer_to_server(
        ExperimentServicer(supervisor), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = experiment_pb2_grpc.ExperimentServiceStub(channel)
    event = trace_pb2.TraceEvent(
        event_id="grpc-event",
        job_id="job-1",
        execution_id="grpc-execution",
        phase_id="decode",
        occurred_at=to_timestamp(datetime(2025, 1, 1, tzinfo=UTC)),
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
        algorithm="grpo",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version="policy-1",
        decision_id="recorded-grpc",
        sequence=1,
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        stage_id="decode",
        run_id="grpc-run",
        trace_id="grpc-trace",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
    )
    step = ReplayArtifactStep(
        ordinal=1,
        event=event,
        intent=scheduling_pb2.SchedulingIntent(
            execution_id="grpc-execution",
            stage_id="decode",
            version=1,
            deterministic_seed=7,
            policy_version="policy-1",
        ),
        snapshot=resource_pb2.ClusterSnapshot(
            snapshot_id="grpc-snapshot", revision=1, observed_at=event.occurred_at
        ),
    )
    try:
        created = await stub.CreateReplay(
            experiment_pb2.CreateReplayRequest(
                replay=experiment_pb2.Replay(
                    replay_id="grpc-replay",
                    run_id="grpc-run",
                    trace_id="grpc-trace",
                    source_trace_ref="inline://grpc",
                    speed=1.0,
                    seed=7,
                    data_kind=trace_pb2.DATA_KIND_REPLAY,
                    artifacts=[encode_replay_step_artifact("grpc-replay", step)],
                )
            )
        )
        assert created.replay.artifacts[0].WhichOneof("typed_payload") == "replay_step"
        started = await stub.ApplyReplayCommand(
            experiment_pb2.ApplyReplayCommandRequest(
                replay_id="grpc-replay",
                command=experiment_pb2.REPLAY_COMMAND_TYPE_START,
                idempotency_key="grpc-replay-start",
            )
        )
        assert started.replay.state == experiment_pb2.REPLAY_STATE_COMPLETED
        assert started.replay.emitted_decisions == 1
        fetched = await stub.GetReplay(experiment_pb2.GetReplayRequest(replay_id="grpc-replay"))
        assert [artifact.WhichOneof("typed_payload") for artifact in fetched.replay.artifacts] == [
            "replay_step",
            "replay_decision",
        ]

        await stub.CreateReplay(
            experiment_pb2.CreateReplayRequest(
                replay=experiment_pb2.Replay(
                    replay_id="grpc-missing",
                    run_id="grpc-run",
                    trace_id="grpc-trace",
                    source_trace_ref="inline://missing",
                    speed=1.0,
                    seed=7,
                    data_kind=trace_pb2.DATA_KIND_REPLAY,
                )
            )
        )
        with pytest.raises(grpc.aio.AioRpcError) as failure:
            await stub.ApplyReplayCommand(
                experiment_pb2.ApplyReplayCommandRequest(
                    replay_id="grpc-missing",
                    command=experiment_pb2.REPLAY_COMMAND_TYPE_START,
                    idempotency_key="grpc-missing-start",
                )
            )
        assert failure.value.code() is grpc.StatusCode.FAILED_PRECONDITION
    finally:
        await channel.close()
        await server.stop(None)


@pytest.mark.asyncio
async def test_watch_runtime_events_streams_responses() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    servicer = RuntimeControlServicer(supervisor)
    manifest = _manifest()

    await servicer.ValidateRuntime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=manifest, request_id="validate", idempotency_key="v"
        ),
        _context(),
    )
    await servicer.CompileRuntime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=manifest, request_id="compile", idempotency_key="c"
        ),
        _context(),
    )
    await servicer.PrepareRuntime(
        runtime_pb2.PrepareRuntimeRequest(
            run_id="run-1", request_id="prepare", idempotency_key="p"
        ),
        _context(),
    )
    supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=runtime_pb2.SandboxEvent(
                event_id="sandbox-event-1",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_REQUESTED,
                sandbox_id="sandbox:run-1:prefill",
                run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
                generation=1,
                state=runtime_pb2.RUNTIME_STATE_REQUESTED,
                binding=scheduling_pb2.Binding(
                    binding_id="binding-prefill",
                    pending_unit_id="run-1:prefill",
                    sandbox_id="sandbox:run-1:prefill",
                    generation=1,
                ),
                detail="requested",
                occurred_at=runtime_pb2.RuntimeUnit().observed_at,
                data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
            )
        )
    )

    events = [
        response.event
        async for response in servicer.WatchRuntimeEvents(
            runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1"), _context()
        )
    ]
    assert events
    assert all(event.run_id == "run-1" for event in events)


def test_runtime_list_page_tokens_are_opaque_and_bound_to_query() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    compiled = supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    first = supervisor.list_runtime_units(
        runtime_pb2.ListRuntimeUnitsRequest(run_id="run-1", limit=2)
    )

    assert len(first.runtime_units) == 2
    assert first.next_page_token
    assert first.next_page_token != "2"
    second = supervisor.list_runtime_units(
        runtime_pb2.ListRuntimeUnitsRequest(
            run_id="run-1", limit=2, page_token=first.next_page_token
        )
    )
    assert [unit.runtime_unit_id for unit in second.runtime_units] == [
        unit.runtime_unit_id for unit in compiled.runtime_units[2:4]
    ]

    with pytest.raises(ValueError, match="does not match"):
        supervisor.list_runtime_units(
            runtime_pb2.ListRuntimeUnitsRequest(
                run_id="run-1",
                stage_id=compiled.runtime_units[0].stage_id,
                page_token=first.next_page_token,
            )
        )
    with pytest.raises(ValueError, match="does not match"):
        supervisor.list_runtime_units(
            runtime_pb2.ListRuntimeUnitsRequest(
                run_id="different-run", page_token=first.next_page_token
            )
        )


def test_sandbox_list_page_tokens_reject_cross_scope_and_filter_reuse() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    first = supervisor.list_sandboxes(
        runtime_pb2.ListSandboxesRequest(run_id="run-1", job_id="job-1", limit=2)
    )

    assert len(first.sandboxes) == 2
    assert first.next_page_token
    assert first.next_page_token != "2"
    second = supervisor.list_sandboxes(
        runtime_pb2.ListSandboxesRequest(
            run_id="run-1",
            job_id="job-1",
            limit=2,
            page_token=first.next_page_token,
        )
    )
    assert len(second.sandboxes) == 2

    with pytest.raises(ValueError, match="does not match"):
        supervisor.list_sandboxes(
            runtime_pb2.ListSandboxesRequest(
                run_id="run-1", job_id="other-job", page_token=first.next_page_token
            )
        )
    with pytest.raises(ValueError, match="does not match"):
        supervisor.list_runtime_units(
            runtime_pb2.ListRuntimeUnitsRequest(run_id="run-1", page_token=first.next_page_token)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "list_request"),
    [
        (
            "ListRuntimeUnits",
            runtime_pb2.ListRuntimeUnitsRequest(run_id="run-1", page_token="not-base64!"),
        ),
        (
            "ListSandboxes",
            runtime_pb2.ListSandboxesRequest(run_id="run-1", page_token="not-base64!"),
        ),
    ],
)
async def test_runtime_list_servicers_map_invalid_tokens_to_invalid_argument(
    method_name: str, list_request: object
) -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    context = _AbortContext()
    method = getattr(RuntimeControlServicer(supervisor), method_name)

    with pytest.raises(RuntimeError, match=r"INVALID_ARGUMENT.*page_token"):
        await method(list_request, cast(grpc.aio.ServicerContext[Any, Any], context))

    assert context.code is grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.asyncio
@pytest.mark.parametrize("method_name", ["ListReplays", "ListExperiments"])
async def test_experiment_lists_use_scoped_opaque_tokens_and_map_invalid_tokens(
    method_name: str,
) -> None:
    supervisor = RuntimeSupervisor()
    servicer = ExperimentServicer(supervisor)
    for index in range(3):
        supervisor.experiments.create_replay(
            experiment_pb2.Replay(
                replay_id=f"replay-{index}",
                run_id="run-1",
                trace_id="trace-1",
                source_trace_ref="memory://trace",
                speed=1.0,
            ),
            created_at=datetime.now(tz=UTC),
        )
        supervisor.experiments.create_experiment(
            experiment_pb2.Experiment(experiment_id=f"experiment-{index}"),
            created_at=datetime.now(tz=UTC),
        )

    method = getattr(servicer, method_name)
    request_type = (
        experiment_pb2.ListReplaysRequest
        if method_name == "ListReplays"
        else experiment_pb2.ListExperimentsRequest
    )
    first = await method(request_type(limit=2), _context())
    assert first.next_page_token and first.next_page_token != "2"
    second = await method(request_type(limit=2, page_token=first.next_page_token), _context())
    assert len(second.replays if method_name == "ListReplays" else second.experiments) == 1

    context = _AbortContext()
    with pytest.raises(RuntimeError, match=r"INVALID_ARGUMENT.*page_token"):
        await method(
            request_type(page_token="not-base64!"),
            cast(grpc.aio.ServicerContext[Any, Any], context),
        )
    assert context.code is grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("servicer_kind", "method_name", "rpc_request", "expected_code"),
    [
        (
            "runtime",
            "GetRuntimeManifest",
            runtime_pb2.GetRuntimeManifestRequest(run_id="missing"),
            grpc.StatusCode.NOT_FOUND,
        ),
        (
            "runtime",
            "GetRuntimeStatus",
            runtime_pb2.GetRuntimeStatusRequest(run_id="missing"),
            grpc.StatusCode.NOT_FOUND,
        ),
        (
            "experiment",
            "GetReplay",
            experiment_pb2.GetReplayRequest(replay_id="missing"),
            grpc.StatusCode.NOT_FOUND,
        ),
        (
            "experiment",
            "GetReplayStatus",
            experiment_pb2.GetReplayStatusRequest(replay_id="missing"),
            grpc.StatusCode.NOT_FOUND,
        ),
        (
            "experiment",
            "GetExperiment",
            experiment_pb2.GetExperimentRequest(experiment_id="missing"),
            grpc.StatusCode.NOT_FOUND,
        ),
        (
            "experiment",
            "CreateReplay",
            experiment_pb2.CreateReplayRequest(
                replay=experiment_pb2.Replay(replay_id="invalid", speed=1.0)
            ),
            grpc.StatusCode.INVALID_ARGUMENT,
        ),
        (
            "experiment",
            "CreateExperiment",
            experiment_pb2.CreateExperimentRequest(),
            grpc.StatusCode.INVALID_ARGUMENT,
        ),
    ],
)
async def test_public_read_and_create_rpcs_map_domain_errors(
    servicer_kind: str, method_name: str, rpc_request: object, expected_code: grpc.StatusCode
) -> None:
    supervisor = RuntimeSupervisor()
    servicer = (
        RuntimeControlServicer(supervisor)
        if servicer_kind == "runtime"
        else ExperimentServicer(supervisor)
    )
    context = _AbortContext()
    method = getattr(servicer, method_name)

    with pytest.raises(RuntimeError, match=expected_code.name):
        await method(rpc_request, cast(grpc.aio.ServicerContext[Any, Any], context))
    assert context.code is expected_code


@pytest.mark.asyncio
async def test_watch_runtime_events_maps_invalid_cursor_to_invalid_argument() -> None:
    supervisor = RuntimeSupervisor()
    context = _AbortContext()
    stream = RuntimeControlServicer(supervisor).WatchRuntimeEvents(
        runtime_pb2.WatchRuntimeEventsRequest(run_id="missing", after_cursor="not-a-sequence"),
        cast(grpc.aio.ServicerContext[Any, Any], context),
    )
    with pytest.raises(RuntimeError, match="INVALID_ARGUMENT"):
        await anext(stream)
    assert context.code is grpc.StatusCode.INVALID_ARGUMENT


def _materialize_runtime_units(supervisor: RuntimeSupervisor) -> None:
    """Give every compiled unit one concrete backend target for control tests."""
    updated: list[runtime_pb2.RuntimeUnit] = []
    sandboxes: list[runtime_pb2.Sandbox] = []
    for current in supervisor.runtime_units.list("run-1"):
        unit = runtime_pb2.RuntimeUnit()
        unit.CopyFrom(current)
        unit.sandbox_id = f"sandbox:{unit.runtime_unit_id}"
        unit.generation = 4
        unit.state = runtime_pb2.RUNTIME_STATE_RUNNING
        unit.status_reason = "starting"
        updated.append(unit)
        sandboxes.append(
            runtime_pb2.Sandbox(
                sandbox_id=unit.sandbox_id,
                run_id=unit.run_id,
                job_id=unit.job_id,
                trace_id=unit.trace_id,
                state=unit.state,
                generation=unit.generation,
                binding=scheduling_pb2.Binding(
                    binding_id=f"binding:{unit.runtime_unit_id}",
                    runtime_unit_id=unit.runtime_unit_id,
                    sandbox_id=unit.sandbox_id,
                    generation=unit.generation,
                ),
                observed_at=unit.observed_at,
                data_kind=unit.data_kind,
            )
        )
    supervisor.runtime_units.put_many("run-1", updated)
    supervisor.sandboxes.replace("run-1", sandboxes)


def test_operator_control_targets_all_sandboxes_with_stable_deduplication() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    sandboxes = supervisor.sandboxes.list("run-1")
    shared_unit_id = sandboxes[0].binding.runtime_unit_id
    extra = runtime_pb2.Sandbox()
    extra.CopyFrom(sandboxes[0])
    extra.sandbox_id = "sandbox:shared-unit:replica-2"
    extra.binding.runtime_unit_id = ""
    extra.binding.pending_unit_id = shared_unit_id
    extra.binding.sandbox_id = extra.sandbox_id
    duplicate = runtime_pb2.Sandbox()
    duplicate.CopyFrom(extra)
    supervisor.sandboxes.replace("run-1", [extra, *reversed(sandboxes), duplicate])

    request = supervisor.lifecycle.build_operator_control_request(
        runtime_pb2.PauseRuntimeRequest(
            run_id="run-1", request_id="pause", idempotency_key="pause-key"
        ),
        action=LifecycleAction.PAUSE,
    )

    targets = [
        (target.runtime_unit_id, target.sandbox_id, target.expected_generation)
        for target in request.targets
    ]
    assert targets == sorted(set(targets))
    assert sum(target[0] == shared_unit_id for target in targets) == 2
    assert (shared_unit_id, extra.sandbox_id, 4) in targets


@pytest.mark.asyncio
async def test_runtime_control_servicer_dispatches_typed_operator_actions() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    operator = _OperatorStub()
    servicer = RuntimeControlServicer(supervisor, operator_client=operator)

    cases = [
        (
            servicer.PauseRuntime,
            runtime_pb2.PauseRuntimeRequest(
                run_id="run-1",
                request_id="pause-request",
                idempotency_key="pause-key",
                reason="maintenance",
            ),
            control_pb2.JOB_COMMAND_TYPE_PAUSE,
            "pausing",
        ),
        (
            servicer.ResumeRuntime,
            runtime_pb2.ResumeRuntimeRequest(
                run_id="run-1", request_id="resume-request", idempotency_key="resume-key"
            ),
            control_pb2.JOB_COMMAND_TYPE_RESUME,
            "resuming",
        ),
        (
            servicer.StopRuntime,
            runtime_pb2.StopRuntimeRequest(
                run_id="run-1", request_id="stop-request", idempotency_key="stop-key"
            ),
            control_pb2.JOB_COMMAND_TYPE_STOP,
            "stopping",
        ),
        (
            servicer.TerminateRuntime,
            runtime_pb2.TerminateRuntimeRequest(
                run_id="run-1",
                request_id="terminate-request",
                idempotency_key="terminate-key",
                reason="cancelled",
            ),
            control_pb2.JOB_COMMAND_TYPE_TERMINATE,
            "terminating",
        ),
    ]
    for method, request, expected_action, expected_reason in cases:
        before_states = [unit.state for unit in supervisor.runtime_units.list("run-1")]
        response = await method(request, _context())
        sent = operator.requests[-1]
        assert sent.action == expected_action
        assert sent.job_id == "job-1"
        assert sent.run_id == "run-1"
        assert sent.trace_id == "trace-1"
        assert sent.request_id == request.request_id
        assert sent.idempotency_key == request.idempotency_key
        assert len(sent.targets) == len(response.runtime_units)
        assert all(target.runtime_unit_id for target in sent.targets)
        assert all(target.sandbox_id for target in sent.targets)
        assert all(target.expected_generation == 4 for target in sent.targets)
        assert [unit.state for unit in response.runtime_units] == before_states
        assert all(unit.status_reason == expected_reason for unit in response.runtime_units)
    assert supervisor.fake_command_driver.calls == []


@pytest.mark.asyncio
async def test_runtime_control_servicer_rejection_does_not_persist_desired_state() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    before = [(unit.state, unit.status_reason) for unit in supervisor.runtime_units.list("run-1")]
    servicer = RuntimeControlServicer(
        supervisor, operator_client=_OperatorStub(accepted=False, detail="generation mismatch")
    )
    context = _AbortContext()

    with pytest.raises(RuntimeError, match=r"FAILED_PRECONDITION.*generation mismatch"):
        await servicer.PauseRuntime(
            runtime_pb2.PauseRuntimeRequest(run_id="run-1", idempotency_key="pause-key"),
            cast(grpc.aio.ServicerContext[Any, Any], context),
        )

    assert context.code is grpc.StatusCode.FAILED_PRECONDITION
    assert [
        (unit.state, unit.status_reason) for unit in supervisor.runtime_units.list("run-1")
    ] == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.CANCELLED,
    ],
)
async def test_unknown_operator_result_keeps_staged_desired_state(
    code: grpc.StatusCode,
) -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    operator = _OperatorStub(error=grpc.aio.AioRpcError(code, details="result unknown"))
    servicer = RuntimeControlServicer(supervisor, operator_client=operator)
    context = _AbortContext()

    with pytest.raises(RuntimeError, match=rf"{code.name}.*result unknown"):
        await servicer.PauseRuntime(
            runtime_pb2.PauseRuntimeRequest(
                run_id="run-1", idempotency_key="retryable-control-key"
            ),
            cast(grpc.aio.ServicerContext[Any, Any], context),
        )

    assert context.code is code
    assert all(unit.status_reason == "pausing" for unit in supervisor.runtime_units.list("run-1"))

    operator.error = None
    retried = await servicer.PauseRuntime(
        runtime_pb2.PauseRuntimeRequest(run_id="run-1", idempotency_key="retryable-control-key"),
        _context(),
    )
    assert all(unit.status_reason == "pausing" for unit in retried.runtime_units)
    assert [item.idempotency_key for item in operator.requests] == [
        "retryable-control-key",
        "retryable-control-key",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.UNIMPLEMENTED,
    ],
)
async def test_definite_operator_rejection_rolls_back_desired_state(
    code: grpc.StatusCode,
) -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    before = [(unit.state, unit.status_reason) for unit in supervisor.runtime_units.list("run-1")]
    servicer = RuntimeControlServicer(
        supervisor,
        operator_client=_OperatorStub(
            error=grpc.aio.AioRpcError(code, details="definite rejection")
        ),
    )

    with pytest.raises(RuntimeError):
        await servicer.PauseRuntime(
            runtime_pb2.PauseRuntimeRequest(run_id="run-1", idempotency_key="reject"),
            cast(grpc.aio.ServicerContext[Any, Any], _AbortContext()),
        )

    assert [
        (unit.state, unit.status_reason) for unit in supervisor.runtime_units.list("run-1")
    ] == before


@pytest.mark.asyncio
async def test_runtime_control_servicer_requires_operator_dependency() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    before = [(unit.state, unit.status_reason) for unit in supervisor.runtime_units.list("run-1")]
    context = _AbortContext()

    with pytest.raises(RuntimeError, match=r"FAILED_PRECONDITION.*operator lifecycle"):
        await RuntimeControlServicer(supervisor).StopRuntime(
            runtime_pb2.StopRuntimeRequest(run_id="run-1"),
            cast(grpc.aio.ServicerContext[Any, Any], context),
        )

    assert context.code is grpc.StatusCode.FAILED_PRECONDITION
    assert [
        (unit.state, unit.status_reason) for unit in supervisor.runtime_units.list("run-1")
    ] == before


@pytest.mark.asyncio
async def test_runtime_control_dispatches_over_generated_grpc_contract() -> None:
    operator_server = grpc.aio.server()
    operator_servicer = _OperatorGRPCServicer()
    operator_pb2_grpc.add_RuntimeBackendControlServiceServicer_to_server(
        operator_servicer, operator_server
    )
    operator_port = operator_server.add_insecure_port("127.0.0.1:0")
    await operator_server.start()
    operator_client = OperatorClient(target=f"127.0.0.1:{operator_port}")

    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    runtime_server = grpc.aio.server()
    runtime_pb2_grpc.add_RuntimeControlServiceServicer_to_server(
        RuntimeControlServicer(supervisor, operator_client=operator_client), runtime_server
    )
    runtime_port = runtime_server.add_insecure_port("127.0.0.1:0")
    await runtime_server.start()
    runtime_channel = grpc.aio.insecure_channel(f"127.0.0.1:{runtime_port}")
    runtime_stub = runtime_pb2_grpc.RuntimeControlServiceStub(runtime_channel)

    try:
        response = await runtime_stub.PauseRuntime(
            runtime_pb2.PauseRuntimeRequest(
                run_id="run-1",
                request_id="grpc-pause",
                idempotency_key="grpc-pause-key",
                reason="maintenance",
            )
        )
        assert response.runtime_units
        assert all(unit.status_reason == "pausing" for unit in response.runtime_units)
        assert len(operator_servicer.requests) == 1
        assert operator_servicer.requests[0].action == control_pb2.JOB_COMMAND_TYPE_PAUSE
        assert all(
            target.expected_generation == 4 for target in operator_servicer.requests[0].targets
        )
    finally:
        await runtime_channel.close()
        await runtime_server.stop(None)
        await operator_client.close()
        await operator_server.stop(None)


@pytest.mark.asyncio
async def test_grpc_control_acceptance_precedes_observed_status_report() -> None:
    operator_server = grpc.aio.server()
    operator_servicer = _OperatorGRPCServicer()
    operator_pb2_grpc.add_RuntimeBackendControlServiceServicer_to_server(
        operator_servicer, operator_server
    )
    operator_port = operator_server.add_insecure_port("127.0.0.1:0")
    await operator_server.start()
    operator_client = OperatorClient(target=f"127.0.0.1:{operator_port}")
    reporter = _JobControlStub()

    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    compiled = supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    runtime_server = grpc.aio.server()
    runtime_pb2_grpc.add_RuntimeControlServiceServicer_to_server(
        RuntimeControlServicer(
            supervisor,
            operator_client=operator_client,
            job_control_reporter=reporter,
        ),
        runtime_server,
    )
    runtime_port = runtime_server.add_insecure_port("127.0.0.1:0")
    await runtime_server.start()
    runtime_channel = grpc.aio.insecure_channel(f"127.0.0.1:{runtime_port}")
    runtime_stub = runtime_pb2_grpc.RuntimeControlServiceStub(runtime_channel)

    async def publish_before_operator_response(
        request: operator_pb2.ApplyRuntimeControlRequest,
    ) -> None:
        staged_units = supervisor.runtime_units.list("run-1")
        assert all(unit.status_reason == "pausing" for unit in staged_units)
        by_id = {unit.runtime_unit_id: unit for unit in staged_units}
        for index, target in enumerate(request.targets, start=1):
            await runtime_stub.PublishSandboxEvent(
                runtime_pb2.PublishSandboxEventRequest(
                    event=_make_sandbox_event(
                        runtime_unit=by_id[target.runtime_unit_id],
                        event_id=f"pause-observation-{index}",
                        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED,
                        state=runtime_pb2.RUNTIME_STATE_PAUSED,
                        generation=target.expected_generation,
                        sandbox_id=target.sandbox_id,
                    )
                )
            )

    operator_servicer.before_response = publish_before_operator_response

    try:
        controlled = await runtime_stub.PauseRuntime(
            runtime_pb2.PauseRuntimeRequest(
                run_id="run-1",
                request_id="grpc-pause",
                idempotency_key="grpc-pause-order",
            )
        )
        assert all(unit.status_reason == "pausing" for unit in controlled.runtime_units)
        assert len(reporter.reported) == len(compiled.runtime_units)
        assert all(not item.converged for item in reporter.reported[:-1])
        final = reporter.reported[-1]
        assert not final.converged
        assert final.observed_runtime_state == runtime_pb2.RUNTIME_STATE_UNKNOWN
        assert final.health == control_pb2.COMPONENT_HEALTH_PROGRESSING
        assert final.revision == len(compiled.runtime_units)
    finally:
        await runtime_channel.close()
        await runtime_server.stop(None)
        await operator_client.close()
        await operator_server.stop(None)


@pytest.mark.asyncio
async def test_publish_sandbox_event_servicer_reports_component_status_to_job_control() -> None:
    supervisor = RuntimeSupervisor(
        scheduler_client=_SchedulerStub(),
    )
    reporter = _JobControlStub()
    servicer = RuntimeControlServicer(supervisor, job_control_reporter=reporter)
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(manifest=_manifest(), request_id="req-1")
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest, request_id="req-2")
    )

    await servicer.PublishSandboxEvent(
        runtime_pb2.PublishSandboxEventRequest(
            event=runtime_pb2.SandboxEvent(
                event_id="sandbox-event-report",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                sandbox_id="sandbox:run-1:decode",
                run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
                generation=1,
                state=runtime_pb2.RUNTIME_STATE_BOUND,
                binding=scheduling_pb2.Binding(
                    binding_id="binding-report",
                    pending_unit_id=compiled.runtime_units[1].runtime_unit_id,
                    sandbox_id="sandbox:run-1:decode",
                    generation=1,
                ),
                occurred_at=compiled.runtime_units[1].observed_at,
                data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
            )
        ),
        _context(),
    )

    assert len(reporter.reported) == 1
    assert reporter.reported[0].run_id == "run-1"
    assert reporter.reported[0].component == "runtime"
    assert reporter.reported[0].annotations["runtime.converged"] == "false"
    assert reporter.reported[0].annotations["runtime.state"] == "RUNTIME_STATE_UNKNOWN"
    assert not reporter.reported[0].converged
    assert reporter.reported[0].observed_runtime_state == runtime_pb2.RUNTIME_STATE_UNKNOWN


@pytest.mark.asyncio
async def test_publish_sandbox_event_reports_over_generated_job_control_grpc() -> None:
    job_server = grpc.aio.server()
    job_servicer = _JobControlGRPCServicer()
    control_pb2_grpc.add_JobControlServiceServicer_to_server(job_servicer, job_server)
    job_port = job_server.add_insecure_port("127.0.0.1:0")
    await job_server.start()
    reporter = JobControlClient(target=f"127.0.0.1:{job_port}")

    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    compiled = supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    servicer = RuntimeControlServicer(supervisor, job_control_reporter=reporter)
    try:
        await servicer.PublishSandboxEvent(
            runtime_pb2.PublishSandboxEventRequest(
                event=_make_sandbox_event(
                    runtime_unit=compiled.runtime_units[0],
                    event_id="grpc-observed-status",
                    event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
                    state=runtime_pb2.RUNTIME_STATE_RUNNING,
                )
            ),
            _context(),
        )
        assert len(job_servicer.reported) == 1
        assert job_servicer.reported[0].source == "runtime-observation"
        assert job_servicer.reported[0].revision == 1
    finally:
        await reporter.close()
        await job_server.stop(None)


def _multi_sandbox_runtime() -> tuple[
    RuntimeSupervisor, runtime_pb2.RuntimeUnit, runtime_pb2.RuntimeUnit
]:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    compiled = supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    primary = runtime_pb2.RuntimeUnit()
    primary.CopyFrom(compiled.runtime_units[0])
    primary.generation = 1
    primary.status_reason = "pausing"
    primary.annotations["unit_count"] = "2"
    secondary = runtime_pb2.RuntimeUnit()
    secondary.CopyFrom(compiled.runtime_units[1])
    secondary.generation = 1
    secondary.status_reason = "pausing"
    secondary.annotations["unit_count"] = "1"
    supervisor.runtime_units.replace("run-1", [primary, secondary])
    return supervisor, primary, secondary


def _publish_observation(
    supervisor: RuntimeSupervisor,
    *,
    unit: runtime_pb2.RuntimeUnit,
    sandbox_id: str,
    state: int,
    ordinal: int,
) -> None:
    supervisor.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=unit,
                event_id=f"multi-sandbox-{ordinal}",
                event_type={
                    runtime_pb2.RUNTIME_STATE_PAUSED: runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED,
                    runtime_pb2.RUNTIME_STATE_RUNNING: runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
                    runtime_pb2.RUNTIME_STATE_FAILED: runtime_pb2.SANDBOX_EVENT_TYPE_FAILED,
                }[state],
                state=state,
                sandbox_id=sandbox_id,
            )
        )
    )


def test_multi_sandbox_first_replica_does_not_converge_unit_or_runtime() -> None:
    supervisor, primary, _secondary = _multi_sandbox_runtime()

    _publish_observation(
        supervisor,
        unit=primary,
        sandbox_id="sandbox-primary-1",
        state=runtime_pb2.RUNTIME_STATE_PAUSED,
        ordinal=1,
    )
    status = supervisor.get_component_status("run-1", "runtime")
    projected = supervisor.runtime_units.get_by_runtime_unit_id("run-1", primary.runtime_unit_id)

    assert projected.state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert status.health == control_pb2.COMPONENT_HEALTH_PROGRESSING
    assert status.observed_runtime_state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert not status.converged
    assert status.annotations["runtime.observed_count"] == "1"
    assert status.annotations["runtime.target_count"] == "3"


def test_multi_sandbox_without_observations_never_converges() -> None:
    supervisor, _primary, _secondary = _multi_sandbox_runtime()

    status = supervisor.project_runtime_status("run-1")

    assert status.health == control_pb2.COMPONENT_HEALTH_PROGRESSING
    assert status.observed_runtime_state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert not status.converged
    assert status.annotations["runtime.observed_count"] == "0"


def test_multi_sandbox_converges_only_after_all_expected_targets_arrive() -> None:
    supervisor, primary, secondary = _multi_sandbox_runtime()
    _publish_observation(
        supervisor,
        unit=primary,
        sandbox_id="sandbox-primary-1",
        state=runtime_pb2.RUNTIME_STATE_PAUSED,
        ordinal=1,
    )
    _publish_observation(
        supervisor,
        unit=primary,
        sandbox_id="sandbox-primary-2",
        state=runtime_pb2.RUNTIME_STATE_PAUSED,
        ordinal=2,
    )
    assert not supervisor.get_component_status("run-1", "runtime").converged

    _publish_observation(
        supervisor,
        unit=secondary,
        sandbox_id="sandbox-secondary-1",
        state=runtime_pb2.RUNTIME_STATE_PAUSED,
        ordinal=3,
    )
    status = supervisor.get_component_status("run-1", "runtime")

    assert status.health == control_pb2.COMPONENT_HEALTH_HEALTHY
    assert status.observed_runtime_state == runtime_pb2.RUNTIME_STATE_PAUSED
    assert status.converged


def test_multi_sandbox_failure_is_fail_fast() -> None:
    supervisor, primary, _secondary = _multi_sandbox_runtime()
    _publish_observation(
        supervisor,
        unit=primary,
        sandbox_id="sandbox-primary-1",
        state=runtime_pb2.RUNTIME_STATE_FAILED,
        ordinal=1,
    )
    status = supervisor.get_component_status("run-1", "runtime")
    projected = supervisor.runtime_units.get_by_runtime_unit_id("run-1", primary.runtime_unit_id)

    assert projected.state == runtime_pb2.RUNTIME_STATE_FAILED
    assert status.health == control_pb2.COMPONENT_HEALTH_FAILED
    assert status.observed_runtime_state == runtime_pb2.RUNTIME_STATE_FAILED
    assert not status.converged


@pytest.mark.parametrize(
    ("reason", "state"),
    [
        ("starting", runtime_pb2.RUNTIME_STATE_RUNNING),
        ("pausing", runtime_pb2.RUNTIME_STATE_PAUSED),
        ("resuming", runtime_pb2.RUNTIME_STATE_RUNNING),
        ("stopping", runtime_pb2.RUNTIME_STATE_TERMINATED),
        ("terminating", runtime_pb2.RUNTIME_STATE_TERMINATED),
    ],
)
def test_observed_status_converges_only_at_requested_target(reason: str, state: int) -> None:
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    manifest = _manifest()
    supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=manifest))
    supervisor.compile_runtime(runtime_pb2.CompileRuntimeRequest(manifest=manifest))
    _materialize_runtime_units(supervisor)
    units = supervisor.runtime_units.list("run-1")
    for unit in units:
        unit.status_reason = reason
        unit.annotations["unit_count"] = "1"
    supervisor.runtime_units.put_many("run-1", units)
    sandboxes = supervisor.sandboxes.list("run-1")
    for sandbox in sandboxes:
        sandbox.state = state
    supervisor.sandboxes.replace("run-1", sandboxes)
    status = supervisor.project_runtime_status("run-1")

    assert status.observed_runtime_state == state
    assert status.converged
    assert status.annotations["runtime.converged"] == "true"


def test_build_runtime_supervisor_with_null_defaults() -> None:
    supervisor = build_runtime_supervisor()
    assert supervisor.scheduler_client is None
    assert supervisor.persistence.__class__.__name__ == "NullPersistenceHook"


def test_mock_alias_builds_fake_supervisor_units() -> None:
    manifest = _manifest()
    manifest.framework = "mock"
    manifest.execution_backend = "mock"
    manifest.trainer = "mock"
    manifest.rollout_engine = "mock"
    supervisor = RuntimeSupervisor(scheduler_client=_SchedulerStub())
    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=manifest, request_id="req-1", idempotency_key="idem-1"
        )
    )
    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate.normalized_manifest, request_id="req-2", idempotency_key="idem-2"
        )
    )
    assert compiled.runtime_units


def test_build_runtime_supervisor_loads_config_bundle_from_options() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    supervisor = build_runtime_supervisor(
        config_load_options=runtime_config.LoadOptions(root=repo_root)
    )
    assert supervisor.config_bundle is not None
    assert supervisor.config_projection is not None
    assert supervisor.config_bundle.manifest.manifest_id == "cpu-mock"
    assert supervisor.config_bundle.policy.selection.top_k == 1
    assert supervisor.config_projection.execution_backend == "inprocessmock"
    assert supervisor.config_projection.algorithm == "grpo"


def test_parser_accepts_config_flags() -> None:
    args = _parser().parse_args(
        [
            "--config-root",
            "/tmp/repo",
            "--manifest",
            "compatibility/manifests/cpu-mock.yaml",
            "--operator-target",
            "127.0.0.1:50081",
            "--job-control-target",
            "127.0.0.1:50061",
        ]
    )
    assert args.config_root == "/tmp/repo"
    assert args.manifest == "compatibility/manifests/cpu-mock.yaml"
    assert args.operator_target == "127.0.0.1:50081"
    assert args.job_control_target == "127.0.0.1:50061"


@pytest.mark.asyncio
async def test_config_bundle_projects_runtime_manifest_and_intents() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    scheduler = _SchedulerStub()
    supervisor = build_runtime_supervisor(
        scheduler_client=scheduler,
        config_load_options=runtime_config.LoadOptions(root=repo_root),
    )
    manifest = _manifest()
    manifest.framework = ""
    manifest.execution_backend = ""
    manifest.queue = ""
    manifest.compatibility_profile = ""
    manifest.policy_version = ""
    manifest.rollout_mode = trace_pb2.ROLLOUT_MODE_UNKNOWN
    manifest.desired_units = 0
    manifest.ClearField("required_capabilities")

    validate = supervisor.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=manifest, request_id="validate", idempotency_key="cfg-v"
        )
    )
    assert validate.valid
    assert validate.normalized_manifest.framework == "mock"
    assert validate.normalized_manifest.execution_backend == "mock"
    assert validate.normalized_manifest.trainer == "fake"
    assert validate.normalized_manifest.rollout_engine == "fake"
    assert validate.normalized_manifest.compatibility_profile == "cpu-mock-v1"
    assert validate.normalized_manifest.policy_version == "1"
    assert validate.normalized_manifest.rollout_mode == trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC
    assert validate.normalized_manifest.desired_units == 2
    assert validate.normalized_manifest.annotations["algorithm"] == "grpo"
    assert validate.normalized_manifest.annotations["provider_source"] == "mock"
    assert validate.normalized_manifest.annotations["provider_kind"] == "MockResourceProvider"
    assert validate.normalized_manifest.annotations["selection_strategy"] == "stablefirstfit"
    assert list(validate.normalized_manifest.required_capabilities.algorithms) == ["ppo", "grpo"]

    compiled = supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate.normalized_manifest, request_id="compile", idempotency_key="cfg-c"
        )
    )
    assert compiled.manifest.desired_units == 2
    assert compiled.runtime_units
    assert all(unit.annotations["provider_source"] == "mock" for unit in compiled.runtime_units)
    assert all(
        unit.annotations["provider_kind"] == "MockResourceProvider"
        for unit in compiled.runtime_units
    )
    assert all(
        unit.annotations["selection_strategy"] == "stablefirstfit"
        for unit in compiled.runtime_units
    )
    assert all(unit.annotations["algorithm"] == "grpo" for unit in compiled.runtime_units)
    assert all(unit.annotations["policy_version"] == "1" for unit in compiled.runtime_units)
    assert all(unit.annotations["unit_count"] == "2" for unit in compiled.runtime_units)
    assert all(unit.required_capabilities.source == "mock" for unit in compiled.runtime_units)

    started = await supervisor.start_runtime(
        runtime_pb2.StartRuntimeRequest(run_id="run-1", request_id="start", idempotency_key="cfg-s")
    )
    assert started.runtime_units
    assert scheduler.published
    assert all(
        intent.rollout_mode == trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC
        for intent in scheduler.published
    )
    assert all(intent.policy_version == "1" for intent in scheduler.published)
    assert all(intent.unit_count == 2 for intent in scheduler.published)
    assert all(intent.required_capabilities.source == "mock" for intent in scheduler.published)
    assert all(
        intent.labels["selection_strategy"] == "stablefirstfit" for intent in scheduler.published
    )
    assert all(intent.labels["provider_source"] == "mock" for intent in scheduler.published)
    assert all(
        intent.labels["provider_kind"] == "MockResourceProvider" for intent in scheduler.published
    )
    assert all(intent.labels["algorithm"] == "grpo" for intent in scheduler.published)


def test_config_bundle_rejects_conflicting_manifest_inputs() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    supervisor = build_runtime_supervisor(
        config_load_options=runtime_config.LoadOptions(root=repo_root)
    )
    manifest = _manifest()
    manifest.compatibility_profile = ""
    manifest.policy_version = ""
    manifest.desired_units = 0
    manifest.annotations["algorithm"] = "ppo"

    with pytest.raises(ValueError, match="algorithm conflicts with config projection"):
        supervisor.validate_runtime(
            runtime_pb2.ValidateRuntimeRequest(
                manifest=manifest, request_id="validate", idempotency_key="cfg-conflict"
            )
        )


@pytest.mark.asyncio
async def test_sqlite_persistence_hydrates_supervisor_memory_on_startup(tmp_path: Path) -> None:
    state_db = tmp_path / "runtime-state.sqlite3"
    scheduler = _SchedulerStub()
    persisted = RuntimeSupervisor(
        scheduler_client=scheduler,
        persistence=SQLitePersistenceHook(str(state_db)),
    )

    validate = persisted.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=_manifest(),
            request_id="validate",
            idempotency_key="v",
        )
    )
    persisted.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate.normalized_manifest,
            request_id="compile",
            idempotency_key="c",
        )
    )
    persisted.prepare_runtime(
        runtime_pb2.PrepareRuntimeRequest(
            run_id="run-1",
            request_id="prepare",
            idempotency_key="p",
        )
    )
    await persisted.start_runtime(
        runtime_pb2.StartRuntimeRequest(
            run_id="run-1",
            request_id="start",
            idempotency_key="s",
        )
    )
    persisted.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=runtime_pb2.SandboxEvent(
                event_id="sandbox-event-1",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                sandbox_id="sandbox:run-1:decode",
                run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
                generation=1,
                state=runtime_pb2.RUNTIME_STATE_BOUND,
                binding=scheduling_pb2.Binding(
                    binding_id="binding-1",
                    pending_unit_id="run-1:decode",
                    sandbox_id="sandbox:run-1:decode",
                    generation=1,
                ),
                detail="bound",
                occurred_at=to_timestamp(datetime.now(tz=UTC)),
                data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
            )
        )
    )
    persisted.checkpoint_runtime(
        runtime_pb2.CheckpointRuntimeRequest(
            run_id="run-1",
            request_id="checkpoint",
            idempotency_key="cp",
            checkpoint_ref="checkpoint:run-1:restored",
        )
    )
    persisted.create_replay(
        experiment_pb2.CreateReplayRequest(
            replay=experiment_pb2.Replay(
                replay_id="replay-1",
                run_id="run-1",
                trace_id="trace-1",
                source_trace_ref="memory://trace-1",
                speed=1.0,
                seed=7,
                data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
            ),
            request_id="replay-create",
            idempotency_key="er1",
        )
    )
    persisted.create_experiment(
        experiment_pb2.CreateExperimentRequest(
            experiment=experiment_pb2.Experiment(
                experiment_id="exp-1",
                display_name="runtime compare",
            ),
            request_id="exp-create",
            idempotency_key="ee1",
        )
    )
    persisted.persistence.record_component_status(
        control_pb2.ComponentStatus(
            component="scheduler",
            health=control_pb2.COMPONENT_HEALTH_HEALTHY,
            source="scheduler",
            revision=2,
            job_id="job-1",
            run_id="run-1",
            trace_id="trace-1",
        )
    )
    cast(SQLitePersistenceHook, persisted.persistence).close()

    restored = RuntimeSupervisor(
        scheduler_client=_SchedulerStub(),
        persistence=SQLitePersistenceHook(str(state_db)),
    )

    assert restored.manifests.has("run-1")
    assert restored.runtime_units.list("run-1")
    assert restored.sandboxes.list("run-1")
    assert restored.sandbox_events.list("run-1")
    assert restored.trace_ingestor.list("run-1")
    assert restored.published_intents
    assert restored.component_statuses[("run-1", "runtime")].component == "runtime"
    assert restored.component_statuses[("run-1", "scheduler")].component == "scheduler"
    assert len([key for key in restored.component_statuses if key[0] == "run-1"]) == 2
    assert restored.checkpoints.latest("run-1") is not None
    assert restored.experiments.store.get_replay("replay-1").replay_id == "replay-1"
    assert restored.experiments.store.get_experiment("exp-1").experiment_id == "exp-1"
    assert restored.versions.peek("checkpoint") >= 1
    assert restored.versions.peek("run:run-1:generation") >= 1
    cast(SQLitePersistenceHook, restored.persistence).close()


@pytest.mark.asyncio
async def test_sqlite_restart_preserves_next_start_generation(tmp_path: Path) -> None:
    state_db = tmp_path / "runtime-generation.sqlite3"
    scheduler = _SchedulerStub()
    persisted = RuntimeSupervisor(
        scheduler_client=scheduler,
        persistence=SQLitePersistenceHook(str(state_db)),
    )

    validate = persisted.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=_manifest(),
            request_id="validate",
            idempotency_key="v",
        )
    )
    persisted.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate.normalized_manifest,
            request_id="compile",
            idempotency_key="c",
        )
    )
    first_start = await persisted.start_runtime(
        runtime_pb2.StartRuntimeRequest(
            run_id="run-1",
            request_id="start-1",
            idempotency_key="s1",
        )
    )
    assert all(unit.generation == 1 for unit in first_start.runtime_units)
    cast(SQLitePersistenceHook, persisted.persistence).close()

    restored_scheduler = _SchedulerStub()
    restored = RuntimeSupervisor(
        scheduler_client=restored_scheduler,
        persistence=SQLitePersistenceHook(str(state_db)),
    )

    restored_sqlite = cast(SQLitePersistenceHook, restored.persistence)
    restored.persistence = NullPersistenceHook()
    second_start = await restored.start_runtime(
        runtime_pb2.StartRuntimeRequest(
            run_id="run-1",
            request_id="start-2",
            idempotency_key="s2",
        )
    )
    assert all(unit.generation == 2 for unit in second_start.runtime_units)
    assert restored_scheduler.published
    assert all(intent.generation == 2 for intent in restored_scheduler.published)
    restored_sqlite.close()


@pytest.mark.asyncio
async def test_start_runtime_same_idempotency_key_is_durable_across_restart(tmp_path: Path) -> None:
    state_db = tmp_path / "runtime-start-idempotency.sqlite3"
    scheduler = _SchedulerStub()
    persisted = RuntimeSupervisor(
        scheduler_client=scheduler,
        persistence=SQLitePersistenceHook(str(state_db)),
    )

    validate = persisted.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=_manifest(),
            request_id="validate",
            idempotency_key="v",
        )
    )
    persisted.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate.normalized_manifest,
            request_id="compile",
            idempotency_key="c",
        )
    )
    first_start = await persisted.start_runtime(
        runtime_pb2.StartRuntimeRequest(
            run_id="run-1",
            request_id="start-1",
            idempotency_key="same-start",
        )
    )
    assert all(unit.generation == 1 for unit in first_start.runtime_units)
    assert all(
        unit.annotations["tgsrl.start_idempotency_key"] == "same-start"
        for unit in persisted.runtime_units.list("run-1")
    )
    assert len(scheduler.published) == len(first_start.runtime_units)
    first_intent_versions = {
        (intent.execution_id, intent.stage_id): intent.version for intent in scheduler.published
    }
    first_intent_keys = {
        (intent.execution_id, intent.stage_id): intent.idempotency_key
        for intent in scheduler.published
    }
    cast(SQLitePersistenceHook, persisted.persistence).close()

    restored_scheduler = _SchedulerStub()
    restored = RuntimeSupervisor(
        scheduler_client=restored_scheduler,
        persistence=SQLitePersistenceHook(str(state_db)),
    )

    restored_sqlite = cast(SQLitePersistenceHook, restored.persistence)
    restored.persistence = NullPersistenceHook()
    second_start = await restored.start_runtime(
        runtime_pb2.StartRuntimeRequest(
            run_id="run-1",
            request_id="start-2",
            idempotency_key="same-start",
        )
    )
    assert all(unit.generation == 1 for unit in second_start.runtime_units)
    assert all(
        unit.annotations["tgsrl.start_idempotency_key"] == "same-start"
        for unit in second_start.runtime_units
    )
    assert restored_scheduler.published == []

    third_start = await restored.start_runtime(
        runtime_pb2.StartRuntimeRequest(
            run_id="run-1",
            request_id="start-3",
            idempotency_key="new-start",
        )
    )
    assert all(unit.generation == 2 for unit in third_start.runtime_units)
    assert restored_scheduler.published
    assert all(intent.generation == 2 for intent in restored_scheduler.published)
    assert {
        (intent.execution_id, intent.stage_id): intent.version
        for intent in restored_scheduler.published
    } == {key: version + 1 for key, version in first_intent_versions.items()}
    assert {
        (intent.execution_id, intent.stage_id): intent.idempotency_key
        for intent in restored_scheduler.published
    } != first_intent_keys
    restored_sqlite.close()


@pytest.mark.asyncio
async def test_start_partial_scheduler_failure_retries_only_unacknowledged_intents() -> None:
    scheduler = _PartialFailScheduler(fail_call=2)
    supervisor = RuntimeSupervisor(scheduler_client=scheduler)
    validate = supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )
    request = runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="partial-start")

    with pytest.raises(RuntimeError, match="scheduler partial failure"):
        await supervisor.start_runtime(request)
    pending = supervisor.runtime_units.list("run-1")
    assert {unit.annotations["tgsrl.start_publication_state"] for unit in pending} == {"pending"}
    assert sum("tgsrl.start_intent_ack" in unit.annotations for unit in pending) == 1
    first_attempts = list(scheduler.attempted)

    completed = await supervisor.start_runtime(request)
    assert len(scheduler.attempted) == len(completed.runtime_units) + 1
    assert scheduler.attempted[0].stage_id not in {
        intent.stage_id for intent in scheduler.attempted[len(first_attempts) :]
    }
    assert {
        unit.annotations["tgsrl.start_publication_state"] for unit in completed.runtime_units
    } == {"complete"}
    repeated = await supervisor.start_runtime(request)
    assert repeated.runtime_units == completed.runtime_units
    assert len(scheduler.attempted) == len(completed.runtime_units) + 1
    with pytest.raises(ValueError, match="pending Start with a different"):
        pending_scheduler = _PartialFailScheduler(fail_call=1)
        pending_supervisor = RuntimeSupervisor(scheduler_client=pending_scheduler)
        pending_validate = pending_supervisor.validate_runtime(
            runtime_pb2.ValidateRuntimeRequest(manifest=_manifest())
        )
        pending_supervisor.compile_runtime(
            runtime_pb2.CompileRuntimeRequest(manifest=pending_validate.normalized_manifest)
        )
        with pytest.raises(RuntimeError):
            await pending_supervisor.start_runtime(
                runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="first-key")
            )
        await pending_supervisor.start_runtime(
            runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="different-key")
        )


@pytest.mark.asyncio
async def test_start_without_scheduler_fails_closed_without_ack_or_completion() -> None:
    supervisor = RuntimeSupervisor(scheduler_client=None)
    validate = supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )

    with pytest.raises(RuntimeError, match="requires a scheduler client"):
        await supervisor.start_runtime(
            runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="no-scheduler-start")
        )

    units = supervisor.runtime_units.list("run-1")
    assert {unit.annotations["tgsrl.start_publication_state"] for unit in units} == {"pending"}
    assert all("tgsrl.start_intent_ack" not in unit.annotations for unit in units)


@pytest.mark.asyncio
async def test_start_rejects_empty_idempotency_key_without_side_effects() -> None:
    scheduler = _SchedulerStub()
    supervisor = RuntimeSupervisor(scheduler_client=scheduler)
    validate = supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )
    before_units = [
        unit.SerializeToString(deterministic=True)
        for unit in supervisor.runtime_units.list("run-1")
    ]
    before_intents = list(supervisor.published_intents)

    with pytest.raises(ValueError, match="Start idempotency_key is required"):
        await supervisor.start_runtime(runtime_pb2.StartRuntimeRequest(run_id="run-1"))

    assert [
        unit.SerializeToString(deterministic=True)
        for unit in supervisor.runtime_units.list("run-1")
    ] == before_units
    assert supervisor.published_intents == before_intents
    assert scheduler.published == []


@pytest.mark.asyncio
async def test_scheduler_rejected_intent_maps_to_failed_precondition() -> None:
    class RejectingScheduler(_SchedulerStub):
        async def publish_intent(
            self, intent: scheduling_pb2.SchedulingIntent
        ) -> scheduling_pb2.PublishIntentResponse:
            return scheduling_pb2.PublishIntentResponse(
                status=scheduling_pb2.INTENT_PUBLISH_STATUS_REJECTED,
                detail="policy rejected",
            )

    supervisor = RuntimeSupervisor(scheduler_client=RejectingScheduler())
    validate = supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )
    context = _AbortContext()

    with pytest.raises(RuntimeError, match=r"FAILED_PRECONDITION.*policy rejected"):
        await RuntimeControlServicer(supervisor).StartRuntime(
            runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="rejected-start"),
            cast(grpc.aio.ServicerContext[Any, Any], context),
        )
    assert context.code is grpc.StatusCode.FAILED_PRECONDITION
    units = supervisor.runtime_units.list("run-1")
    assert {unit.annotations["tgsrl.start_publication_state"] for unit in units} == {"pending"}
    assert all("tgsrl.start_intent_ack" not in unit.annotations for unit in units)


@pytest.mark.asyncio
async def test_concurrent_same_key_start_waits_for_single_outbox_drain() -> None:
    class BlockingScheduler(_SchedulerStub):
        def __init__(self) -> None:
            super().__init__()
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def publish_intent(
            self, intent: scheduling_pb2.SchedulingIntent
        ) -> scheduling_pb2.PublishIntentResponse:
            self.entered.set()
            await self.release.wait()
            self.published.append(intent)
            return scheduling_pb2.PublishIntentResponse(
                status=scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED
            )

    scheduler = BlockingScheduler()
    supervisor = RuntimeSupervisor(scheduler_client=scheduler)
    validate = supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )
    request = runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="concurrent-start")
    first = asyncio.create_task(supervisor.start_runtime(request))
    await scheduler.entered.wait()
    second = asyncio.create_task(supervisor.start_runtime(request))
    await asyncio.sleep(0)
    assert not second.done()
    scheduler.release.set()
    first_result, second_result = await asyncio.gather(first, second)
    assert first_result.runtime_units == second_result.runtime_units
    assert len(scheduler.published) == len(first_result.runtime_units)
    assert {unit.generation for unit in first_result.runtime_units} == {1}


@pytest.mark.asyncio
async def test_start_and_same_run_checkpoint_share_coordinator_serialization() -> None:
    class BlockingScheduler(_SchedulerStub):
        def __init__(self) -> None:
            super().__init__()
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def publish_intent(
            self, intent: scheduling_pb2.SchedulingIntent
        ) -> scheduling_pb2.PublishIntentResponse:
            self.entered.set()
            await self.release.wait()
            await super().publish_intent(intent)
            return scheduling_pb2.PublishIntentResponse(
                status=scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED
            )

    scheduler = BlockingScheduler()
    supervisor = RuntimeSupervisor(scheduler_client=scheduler)
    servicer = RuntimeControlServicer(supervisor)
    validate = supervisor.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    supervisor.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )
    started = asyncio.create_task(
        servicer.StartRuntime(
            runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="serialized-start"),
            _context(),
        )
    )
    await scheduler.entered.wait()
    checkpoint = asyncio.create_task(
        servicer.CheckpointRuntime(
            runtime_pb2.CheckpointRuntimeRequest(
                run_id="run-1", idempotency_key="serialized-checkpoint"
            ),
            _context(),
        )
    )
    await asyncio.sleep(0)
    assert not checkpoint.done()
    scheduler.release.set()
    await started
    assert (await checkpoint).checkpoint_ref


@pytest.mark.asyncio
async def test_pending_start_outbox_recovers_after_sqlite_restart(tmp_path: Path) -> None:
    state_db = tmp_path / "pending-start.sqlite3"
    failed_scheduler = _PartialFailScheduler(fail_call=2)
    persisted = RuntimeSupervisor(
        scheduler_client=failed_scheduler,
        persistence=SQLitePersistenceHook(str(state_db)),
    )
    validate = persisted.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    persisted.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )
    request = runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="recover-start")
    with pytest.raises(RuntimeError, match="scheduler partial failure"):
        await persisted.start_runtime(request)
    unit_count = len(persisted.runtime_units.list("run-1"))
    cast(SQLitePersistenceHook, persisted.persistence).close()

    scheduler = _SchedulerStub()
    restored = RuntimeSupervisor(
        scheduler_client=scheduler, persistence=SQLitePersistenceHook(str(state_db))
    )
    completed = await restored.start_runtime(request)
    assert len(scheduler.published) == unit_count - 1
    assert {unit.generation for unit in completed.runtime_units} == {1}
    assert {
        unit.annotations["tgsrl.start_publication_state"] for unit in completed.runtime_units
    } == {"complete"}
    cast(SQLitePersistenceHook, restored.persistence).close()


@pytest.mark.asyncio
async def test_start_ack_persist_failure_retries_via_scheduler_dedup_after_sqlite_restart(
    tmp_path: Path,
) -> None:
    state_db = tmp_path / "start-ack-persist-retry.sqlite3"
    first_scheduler = _DeduplicatingScheduler()
    persisted = RuntimeSupervisor(
        scheduler_client=first_scheduler,
        persistence=_FailingStartAckPersistence(str(state_db)),
    )
    validate = persisted.validate_runtime(runtime_pb2.ValidateRuntimeRequest(manifest=_manifest()))
    persisted.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(manifest=validate.normalized_manifest)
    )
    request = runtime_pb2.StartRuntimeRequest(run_id="run-1", idempotency_key="ack-retry")

    with pytest.raises(sqlite3.OperationalError, match="inject start ack persistence failure"):
        await persisted.start_runtime(request)

    first_units = persisted.runtime_units.list("run-1")
    assert {unit.generation for unit in first_units} == {1}
    assert {unit.annotations["tgsrl.start_publication_state"] for unit in first_units} == {
        "pending"
    }
    assert all("tgsrl.start_intent_ack" not in unit.annotations for unit in first_units)
    first_identities = {
        (intent.execution_id, intent.stage_id): (intent.version, intent.idempotency_key)
        for intent in first_scheduler.published
    }
    assert first_scheduler.responses
    assert all(
        status == scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED
        for _intent, status in first_scheduler.responses
    )
    cast(_FailingStartAckPersistence, persisted.persistence).close()

    restored_scheduler = _DeduplicatingScheduler()
    for intent in first_scheduler.published:
        restored_scheduler._seen_keys.add(intent.idempotency_key)
    restored = RuntimeSupervisor(
        scheduler_client=restored_scheduler,
        persistence=SQLitePersistenceHook(str(state_db)),
    )

    completed = await restored.start_runtime(request)
    assert {unit.generation for unit in completed.runtime_units} == {1}
    assert {
        unit.annotations["tgsrl.start_publication_state"] for unit in completed.runtime_units
    } == {"complete"}
    assert all(unit.annotations["tgsrl.start_intent_ack"] for unit in completed.runtime_units)
    restored_identities = {
        (intent.execution_id, intent.stage_id): (intent.version, intent.idempotency_key)
        for intent in restored_scheduler.published
    }
    assert set(first_identities).issubset(restored_identities)
    assert {key: restored_identities[key] for key in first_identities} == first_identities
    assert restored_scheduler.responses
    assert any(
        status == scheduling_pb2.INTENT_PUBLISH_STATUS_DEDUPLICATED
        for _intent, status in restored_scheduler.responses
    )
    assert all(
        status
        in {
            scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED,
            scheduling_pb2.INTENT_PUBLISH_STATUS_DEDUPLICATED,
        }
        for _intent, status in restored_scheduler.responses
    )

    repeated = await restored.start_runtime(request)
    assert repeated.runtime_units == completed.runtime_units
    assert len(restored_scheduler.published) == len(completed.runtime_units)
    cast(SQLitePersistenceHook, restored.persistence).close()


def test_sqlite_restore_preserves_watch_runtime_event_sequence(tmp_path: Path) -> None:
    state_db = tmp_path / "runtime-events-sequence.sqlite3"
    persisted = RuntimeSupervisor(persistence=SQLitePersistenceHook(str(state_db)))
    validate = persisted.validate_runtime(
        runtime_pb2.ValidateRuntimeRequest(
            manifest=_manifest(),
            request_id="validate",
            idempotency_key="v",
        )
    )
    compiled = persisted.compile_runtime(
        runtime_pb2.CompileRuntimeRequest(
            manifest=validate.normalized_manifest,
            request_id="compile",
            idempotency_key="c",
        )
    )
    runtime_unit = compiled.runtime_units[1]
    persisted.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=runtime_unit,
                event_id="sandbox-event-a",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                state=runtime_pb2.RUNTIME_STATE_BOUND,
                binding_id="binding-seq",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    persisted.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=runtime_unit,
                event_id="sandbox-event-c",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
                state=runtime_pb2.RUNTIME_STATE_RUNNING,
                binding_id="binding-seq",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    persisted.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=runtime_unit,
                event_id="sandbox-event-b",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED,
                state=runtime_pb2.RUNTIME_STATE_PAUSED,
                binding_id="binding-seq",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    persisted_sqlite = cast(SQLitePersistenceHook, persisted.persistence)
    persisted_sqlite._store._connection.execute(
        "DELETE FROM runtime_events WHERE event_id = ?",
        ("sandbox-event-c",),
    )
    persisted.publish_sandbox_event(
        runtime_pb2.PublishSandboxEventRequest(
            event=_make_sandbox_event(
                runtime_unit=runtime_unit,
                event_id="sandbox-event-d",
                event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
                state=runtime_pb2.RUNTIME_STATE_RUNNING,
                binding_id="binding-seq",
                sandbox_id="sandbox:run-1:decode",
            )
        )
    )
    persisted_sqlite.close()

    restored = RuntimeSupervisor(persistence=SQLitePersistenceHook(str(state_db)))
    responses = restored.watch_runtime_events(runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1"))
    assert [response.sequence for response in responses] == [1, 3, 4]
    assert [response.event.event_id for response in responses] == [
        "sandbox-event-a",
        "sandbox-event-b",
        "sandbox-event-d",
    ]

    resumed = restored.watch_runtime_events(
        runtime_pb2.WatchRuntimeEventsRequest(run_id="run-1", after_cursor=responses[1].cursor)
    )
    assert [response.sequence for response in resumed] == [4]
    assert [response.event.event_id for response in resumed] == ["sandbox-event-d"]
    cast(SQLitePersistenceHook, restored.persistence).close()
