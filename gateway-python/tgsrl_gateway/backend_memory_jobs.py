"""Job/run/operation fixture state and service for the in-memory gateway backend."""

from __future__ import annotations

import copy
import itertools
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, TypeVar, cast

from google.protobuf.message import Message
from tgsrl.v1 import control_pb2, experiment_pb2, job_pb2, runtime_pb2, trace_pb2

from tgsrl_gateway.errors import BadRequestError, ConflictError, NotFoundError
from tgsrl_gateway.pagination import paginate
from tgsrl_gateway.protojson import clone_message, timestamp_from_datetime

T = TypeVar("T")


def clone_value(value: object) -> object:
    if isinstance(value, Message):
        return clone_message(value)
    if isinstance(value, dict):
        return {key: clone_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clone_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(clone_value(item) for item in value)
    return copy.deepcopy(value)


@dataclass(slots=True)
class MemoryJobState:
    job_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    run_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    operation_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    event_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    jobs: dict[str, job_pb2.RLTrainingJob] = field(default_factory=dict)
    runs: dict[str, control_pb2.JobRun] = field(default_factory=dict)
    runs_by_job: dict[str, list[str]] = field(default_factory=dict)
    operations: dict[str, control_pb2.Operation] = field(default_factory=dict)
    events_by_job: dict[str, list[control_pb2.JobEvent]] = field(default_factory=dict)
    idempotency: dict[tuple[str, str], object] = field(default_factory=dict)

    def next_job_id(self) -> str:
        return f"job-{next(self.job_counter):04d}"

    def next_run_id(self) -> str:
        return f"run-{next(self.run_counter):04d}"

    def next_operation_id(self) -> str:
        return f"op-{next(self.operation_counter):04d}"

    def next_event_id(self) -> str:
        return f"event-{next(self.event_counter):04d}"

    def with_idempotency(self, scope: str, idempotency_key: str, factory: Callable[[], T]) -> T:
        key = (scope, idempotency_key)
        if idempotency_key and key in self.idempotency:
            return cast(T, copy.deepcopy(self.idempotency[key]))
        result = factory()
        if idempotency_key:
            self.idempotency[key] = clone_value(result)
        return result

    def list_jobs(self) -> list[job_pb2.RLTrainingJob]:
        return [clone_message(self.jobs[job_id]) for job_id in sorted(self.jobs)]

    def list_jobs_paginated(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        data_kind: str | None,
    ) -> dict[str, object]:
        jobs = self.list_jobs()
        if data_kind:
            jobs = [job for job in jobs if trace_pb2.DataKind.Name(job.data_kind) == data_kind]
        items, next_token = paginate(
            jobs,
            page_token=page_token,
            limit=limit,
            scope="jobs",
            filters={"data_kind": data_kind},
        )
        return {"jobs": items, "next_page_token": next_token}

    def get_job(self, job_id: str) -> job_pb2.RLTrainingJob:
        if job_id not in self.jobs:
            raise NotFoundError("job", job_id)
        return clone_message(self.jobs[job_id])

    def create_job(
        self,
        job: job_pb2.RLTrainingJob,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        def do_create() -> dict[str, object]:
            if job.job_id in self.jobs:
                raise ConflictError("job already exists", details={"job_id": job.job_id})
            self.jobs[job.job_id] = clone_message(job)
            self.runs_by_job.setdefault(job.job_id, [])
            operation = control_pb2.Operation(
                operation_id=self.next_operation_id(),
                type=control_pb2.OPERATION_TYPE_VALIDATE,
                state=control_pb2.OPERATION_STATE_SUCCEEDED,
                actor="gateway",
                reason="job created",
                created_at=timestamp_from_datetime(),
                completed_at=timestamp_from_datetime(),
                job_id=job.job_id,
                data_kind=job.data_kind,
                request_id=request_id,
                idempotency_key=idempotency_key,
            )
            self.operations[operation.operation_id] = operation
            return {"job": clone_message(self.jobs[job.job_id]), "operation": operation}

        return self.with_idempotency("create-job", idempotency_key, do_create)

    def list_runs(
        self, job_id: str, *, limit: int | None, page_token: str | None
    ) -> dict[str, object]:
        self.get_job(job_id)
        runs = [clone_message(self.runs[run_id]) for run_id in self.runs_by_job.get(job_id, [])]
        items, next_token = paginate(
            runs,
            page_token=page_token,
            limit=limit,
            scope="job_runs",
            filters={"job_id": job_id},
        )
        return {"runs": items, "next_page_token": next_token}

    def get_run(self, job_id: str, run_id: str) -> control_pb2.JobRun:
        self.get_job(job_id)
        run = self.runs.get(run_id)
        if run is None or run.job_id != job_id:
            raise NotFoundError("run", run_id)
        return clone_message(run)

    def list_operations(
        self,
        *,
        job_id: str | None = None,
        run_id: str | None = None,
        operation_type: int | None = None,
        state: int | None = None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        operations = [
            clone_message(self.operations[operation_id]) for operation_id in sorted(self.operations)
        ]
        if job_id:
            operations = [operation for operation in operations if operation.job_id == job_id]
        if run_id:
            operations = [operation for operation in operations if operation.run_id == run_id]
        if operation_type:
            operations = [operation for operation in operations if operation.type == operation_type]
        if state:
            operations = [operation for operation in operations if operation.state == state]
        items, next_token = paginate(
            operations,
            page_token=page_token,
            limit=limit,
            scope="operations",
            filters={
                "job_id": job_id,
                "run_id": run_id,
                "operation_type": operation_type,
                "state": state,
            },
        )
        return {"operations": items, "next_page_token": next_token}

    def get_operation(self, operation_id: str) -> control_pb2.Operation:
        operation = self.operations.get(operation_id)
        if operation is None:
            raise NotFoundError("operation", operation_id)
        return clone_message(operation)

    def record_event(
        self,
        *,
        job_id: str,
        run_id: str,
        event_type: int,
        state: int,
        run_state: int,
        detail: str,
        operation: control_pb2.Operation | None = None,
    ) -> control_pb2.JobEvent:
        run = self.runs[run_id]
        event = control_pb2.JobEvent(
            event_id=self.next_event_id(),
            event_type=event_type,
            job_id=job_id,
            run_id=run_id,
            trace_id=run.trace_id,
            occurred_at=timestamp_from_datetime(),
            state=state,
            detail=detail,
            data_kind=run.data_kind,
            run_state=run_state,
        )
        if operation is not None:
            event.operation.CopyFrom(operation)
        self.events_by_job.setdefault(job_id, []).append(event)
        return event

    def record_operation(
        self,
        *,
        job_id: str,
        run_id: str,
        operation_type: int,
        actor: str,
        reason: str,
        request_id: str,
        idempotency_key: str,
    ) -> control_pb2.Operation:
        run = self.runs[run_id]
        operation = control_pb2.Operation(
            operation_id=self.next_operation_id(),
            type=operation_type,
            state=control_pb2.OPERATION_STATE_SUCCEEDED,
            actor=actor,
            reason=reason,
            created_at=timestamp_from_datetime(),
            completed_at=timestamp_from_datetime(),
            job_id=job_id,
            run_id=run_id,
            trace_id=run.trace_id,
            data_kind=run.data_kind,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )
        self.operations[operation.operation_id] = operation
        run.operations.append(operation)
        return operation


@dataclass(slots=True)
class MemoryFixtureCounts:
    jobs: int
    runs: int
    operations: int
    decisions: int
    replays: int
    experiments: int


class RuntimeLifecycleService(Protocol):
    def register_run(self, job: job_pb2.RLTrainingJob, run: control_pb2.JobRun) -> None: ...

    def sync_runtime_state(self, run_id: str, state: int) -> None: ...


@dataclass(slots=True)
class MemoryJobService:
    state: MemoryJobState
    runtime_service: RuntimeLifecycleService
    capability_factory: Callable[[], object]
    default_job_factory: Callable[[str, str], job_pb2.RLTrainingJob]
    execution_contract_factory: Callable[[], object]
    resources_factory: Callable[[], object]
    runtime_spec_factory: Callable[[], object]

    def seed_job(self, job_id: str, display_name: str) -> job_pb2.RLTrainingJob:
        job = self.default_job_factory(job_id, display_name)
        self.state.jobs[job.job_id] = job
        self.state.runs_by_job[job.job_id] = []
        return job

    def normalize_job(self, job: job_pb2.RLTrainingJob) -> job_pb2.RLTrainingJob:
        normalized = clone_message(job)
        if not normalized.job_id:
            normalized.job_id = self.state.next_job_id()
        if not normalized.display_name:
            normalized.display_name = normalized.job_id
        if not normalized.protocol_version:
            normalized.protocol_version = "v0.3"
        if not normalized.algorithm:
            normalized.algorithm = "ppo"
        if not normalized.HasField("runtime"):
            normalized.runtime.CopyFrom(self.runtime_spec_factory())
        if not normalized.HasField("execution_contract"):
            normalized.execution_contract.CopyFrom(self.execution_contract_factory())
        if not normalized.HasField("resources_per_unit"):
            normalized.resources_per_unit.CopyFrom(self.resources_factory())
        if not normalized.HasField("required_capabilities"):
            normalized.required_capabilities.CopyFrom(self.capability_factory())
        if normalized.desired_units == 0:
            normalized.desired_units = 1
        if normalized.state == job_pb2.JOB_STATE_UNKNOWN:
            normalized.state = job_pb2.JOB_STATE_PENDING
        if not normalized.HasField("created_at"):
            normalized.created_at.CopyFrom(timestamp_from_datetime())
        if normalized.rollout_mode == trace_pb2.ROLLOUT_MODE_UNKNOWN:
            normalized.rollout_mode = trace_pb2.ROLLOUT_MODE_SYNC
        if normalized.data_kind == trace_pb2.DATA_KIND_UNKNOWN:
            normalized.data_kind = trace_pb2.DATA_KIND_SYNTHETIC
        return normalized

    def validate_job(
        self,
        job: job_pb2.RLTrainingJob,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        def do_validate() -> dict[str, object]:
            normalized = self.normalize_job(job)
            diagnostics: list[str] = []
            if normalized.protocol_version != "v0.3":
                diagnostics.append("protocol_version normalized to v0.3")
                normalized.protocol_version = "v0.3"
            operation = control_pb2.Operation(
                operation_id=self.state.next_operation_id(),
                type=control_pb2.OPERATION_TYPE_VALIDATE,
                state=control_pb2.OPERATION_STATE_SUCCEEDED,
                actor="gateway",
                reason="job validation",
                created_at=timestamp_from_datetime(),
                completed_at=timestamp_from_datetime(),
                job_id=normalized.job_id,
                request_id=request_id,
                idempotency_key=idempotency_key,
                data_kind=normalized.data_kind,
            )
            self.state.operations[operation.operation_id] = operation
            return {
                "valid": True,
                "normalized_job": normalized,
                "diagnostics": diagnostics,
                "operation": operation,
            }

        return self.state.with_idempotency("validate-job", idempotency_key, do_validate)

    def admit_job(
        self,
        job_id: str,
        *,
        reason: str,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        def do_admit() -> dict[str, object]:
            self.state.get_job(job_id)
            self.state.jobs[job_id].state = job_pb2.JOB_STATE_PENDING
            run_ids = self.state.runs_by_job.get(job_id, [])
            if run_ids:
                run = self.state.runs[run_ids[-1]]
                run.run_state = control_pb2.JOB_RUN_STATE_ADMITTED
                operation = self.state.record_operation(
                    job_id=job_id,
                    run_id=run.run_id,
                    operation_type=control_pb2.OPERATION_TYPE_ADMIT,
                    actor="gateway",
                    reason=reason or "job admitted",
                    request_id=request_id,
                    idempotency_key=idempotency_key,
                )
                self.state.record_event(
                    job_id=job_id,
                    run_id=run.run_id,
                    event_type=control_pb2.JOB_EVENT_TYPE_JOB_ADMITTED,
                    state=self.state.jobs[job_id].state,
                    run_state=run.run_state,
                    detail=reason or "job admitted",
                    operation=operation,
                )
            else:
                operation = control_pb2.Operation(
                    operation_id=self.state.next_operation_id(),
                    type=control_pb2.OPERATION_TYPE_ADMIT,
                    state=control_pb2.OPERATION_STATE_SUCCEEDED,
                    actor="gateway",
                    reason=reason or "job admitted",
                    created_at=timestamp_from_datetime(),
                    completed_at=timestamp_from_datetime(),
                    job_id=job_id,
                    request_id=request_id,
                    idempotency_key=idempotency_key,
                    data_kind=self.state.jobs[job_id].data_kind,
                )
                self.state.operations[operation.operation_id] = operation
            return {"job": self.state.get_job(job_id), "operation": operation}

        return self.state.with_idempotency(f"admit-job:{job_id}", idempotency_key, do_admit)

    def create_run(
        self,
        job: job_pb2.RLTrainingJob,
        *,
        idempotency_key: str,
    ) -> control_pb2.JobRun:
        run_id = self.state.next_run_id()
        run = control_pb2.JobRun(
            run_id=run_id,
            job_id=job.job_id,
            trace_id=f"trace-{run_id}",
            display_name=f"{job.display_name} Run",
            state=job.state,
            created_at=timestamp_from_datetime(),
            attempt=1,
            runtime=job.runtime,
            execution_contract=job.execution_contract,
            rollout_mode=job.rollout_mode,
            data_kind=job.data_kind,
            policy_version=job.policy_ref or "policy-ref-default",
            random_seed=1,
            labels=dict(job.labels),
            run_state=control_pb2.JOB_RUN_STATE_VALIDATING,
        )
        self.state.runs[run_id] = run
        self.state.runs_by_job.setdefault(job.job_id, []).append(run_id)
        self.runtime_service.register_run(job, run)
        operation = self.state.record_operation(
            job_id=job.job_id,
            run_id=run_id,
            operation_type=control_pb2.OPERATION_TYPE_VALIDATE,
            actor="gateway",
            reason="run created",
            request_id=f"create-run-{run_id}",
            idempotency_key=idempotency_key,
        )
        self.state.record_event(
            job_id=job.job_id,
            run_id=run_id,
            event_type=control_pb2.JOB_EVENT_TYPE_JOB_CREATED,
            state=run.state,
            run_state=run.run_state,
            detail="run created",
            operation=operation,
        )
        return run

    def create_job_run(
        self,
        job_id: str,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        del request_id

        def do_create_run() -> dict[str, object]:
            job = self.state.get_job(job_id)
            run = self.create_run(job, idempotency_key=idempotency_key or f"run:{job_id}")
            return {"run": run}

        return self.state.with_idempotency(f"create-run:{job_id}", idempotency_key, do_create_run)

    def apply_job_command(
        self,
        job_id: str,
        run_id: str,
        command: int,
        *,
        actor: str,
        reason: str,
        request_id: str,
        idempotency_key: str,
        event_type_for_command: Callable[[int], int],
        operation_type_for_command: Callable[[int], int],
    ) -> dict[str, object]:
        def do_apply() -> dict[str, object]:
            run = self.state.runs.get(run_id)
            if run is None or run.job_id != job_id:
                raise NotFoundError("run", run_id)
            state_mapping = {
                control_pb2.JOB_COMMAND_TYPE_START: (
                    job_pb2.JOB_STATE_RUNNING,
                    control_pb2.JOB_RUN_STATE_RUNNING,
                    runtime_pb2.RUNTIME_STATE_RUNNING,
                ),
                control_pb2.JOB_COMMAND_TYPE_PAUSE: (
                    job_pb2.JOB_STATE_PAUSED,
                    control_pb2.JOB_RUN_STATE_PAUSED,
                    runtime_pb2.RUNTIME_STATE_PAUSED,
                ),
                control_pb2.JOB_COMMAND_TYPE_RESUME: (
                    job_pb2.JOB_STATE_RUNNING,
                    control_pb2.JOB_RUN_STATE_RUNNING,
                    runtime_pb2.RUNTIME_STATE_RUNNING,
                ),
                control_pb2.JOB_COMMAND_TYPE_STOP: (
                    job_pb2.JOB_STATE_CANCELLED,
                    control_pb2.JOB_RUN_STATE_STOPPED,
                    runtime_pb2.RUNTIME_STATE_TERMINATED,
                ),
                control_pb2.JOB_COMMAND_TYPE_RETRY: (
                    job_pb2.JOB_STATE_PENDING,
                    control_pb2.JOB_RUN_STATE_PREPARING,
                    runtime_pb2.RUNTIME_STATE_REQUESTED,
                ),
                control_pb2.JOB_COMMAND_TYPE_TERMINATE: (
                    job_pb2.JOB_STATE_CANCELLED,
                    control_pb2.JOB_RUN_STATE_TERMINATED,
                    runtime_pb2.RUNTIME_STATE_TERMINATED,
                ),
            }
            job_state, run_state, runtime_state = state_mapping[command]
            self.state.jobs[job_id].state = job_state
            run.state = job_state
            run.run_state = run_state
            if command == control_pb2.JOB_COMMAND_TYPE_START and not run.HasField("started_at"):
                run.started_at.CopyFrom(timestamp_from_datetime())
            if command in {
                control_pb2.JOB_COMMAND_TYPE_STOP,
                control_pb2.JOB_COMMAND_TYPE_TERMINATE,
            }:
                run.completed_at.CopyFrom(timestamp_from_datetime())
            self.runtime_service.sync_runtime_state(run_id, runtime_state)
            operation = self.state.record_operation(
                job_id=job_id,
                run_id=run_id,
                operation_type=operation_type_for_command(command),
                actor=actor,
                reason=reason,
                request_id=request_id,
                idempotency_key=idempotency_key,
            )
            self.state.record_event(
                job_id=job_id,
                run_id=run_id,
                event_type=event_type_for_command(command),
                state=job_state,
                run_state=run_state,
                detail=reason,
                operation=operation,
            )
            return {"operation": operation, "run": clone_message(run)}

        return self.state.with_idempotency(
            f"job-command:{job_id}:{run_id}:{command}",
            idempotency_key,
            do_apply,
        )

    def list_timeline(
        self,
        job_id: str,
        *,
        run_id: str | None,
        after_event_id: str | None,
        page_token: str | None,
        limit: int | None,
    ) -> dict[str, object]:
        self.state.get_job(job_id)
        if after_event_id and page_token:
            raise BadRequestError(
                "after_event_id and page_token are mutually exclusive",
            )
        events = [clone_message(event) for event in self.state.events_by_job.get(job_id, [])]
        if run_id:
            events = [event for event in events if event.run_id == run_id]
        if after_event_id:
            seen = False
            filtered: list[control_pb2.JobEvent] = []
            for event in events:
                if seen:
                    filtered.append(event)
                if event.event_id == after_event_id:
                    seen = True
            events = filtered
        items, next_token = paginate(
            events,
            page_token=page_token,
            limit=limit,
            scope="timeline",
            filters={"job_id": job_id, "run_id": run_id},
        )
        return {"events": items, "next_page_token": next_token}

    def get_dag(self, job_id: str, *, run_id: str | None) -> dict[str, object]:
        job = self.state.get_job(job_id)
        selected_run = self.state.get_run(job_id, run_id) if run_id else None
        graph = clone_message(job.execution_contract.phase_graph)
        dependencies = {
            phase.phase_id: sorted(
                edge.from_phase_id for edge in graph.edges if edge.to_phase_id == phase.phase_id
            )
            for phase in graph.phases
        }
        return {"job": job, "run": selected_run, "phase_graph": graph, "dependencies": dependencies}

    def capabilities(self) -> dict[str, object]:
        return {
            "protocol_version": "v0.3",
            "openapi_version": "3.1.0",
            "backend_mode": "memory",
            "data_kinds": [
                trace_pb2.DataKind.Name(trace_pb2.DATA_KIND_SYNTHETIC),
                trace_pb2.DataKind.Name(trace_pb2.DATA_KIND_REPLAY),
                trace_pb2.DataKind.Name(trace_pb2.DATA_KIND_LIVE),
            ],
            "job_commands": ["start", "pause", "resume", "stop", "retry", "terminate"],
            "replay_commands": ["start", "pause", "resume", "stop", "terminate"],
            "pagination": {"kind": "opaque_stable_token"},
            "routes": [
                "/health",
                "/openapi.json",
                "/v1/capabilities",
                "/v1/jobs",
                "/v1/operations",
                "/v1/replays",
                "/v1/experiments",
            ],
        }

    def health(self, counts: MemoryFixtureCounts) -> dict[str, object]:
        return {
            "status": "ok",
            "mode": "memory",
            "observed_at": timestamp_from_datetime().ToJsonString(),
            "backend": "in_memory",
            "counts": {
                "jobs": counts.jobs,
                "runs": counts.runs,
                "operations": counts.operations,
                "decisions": counts.decisions,
                "replays": counts.replays,
                "experiments": counts.experiments,
            },
        }

    def seed_replay(
        self,
        *,
        run: control_pb2.JobRun,
    ) -> experiment_pb2.Replay:
        return experiment_pb2.Replay(
            replay_id="replay-seeded",
            run_id=run.run_id,
            trace_id=run.trace_id,
            state=experiment_pb2.REPLAY_STATE_COMPLETED,
            source_trace_ref="trace-ref-seeded",
            speed=1.0,
            seed=7,
            created_at=timestamp_from_datetime(),
            started_at=timestamp_from_datetime(),
            completed_at=timestamp_from_datetime(),
            applied_events=4,
            emitted_decisions=1,
            data_kind=trace_pb2.DATA_KIND_REPLAY,
        )

    def seed_experiment(
        self,
        *,
        run: control_pb2.JobRun,
    ) -> experiment_pb2.Experiment:
        return experiment_pb2.Experiment(
            experiment_id="experiment-seeded",
            display_name="Seeded Experiment",
            state=experiment_pb2.EXPERIMENT_STATE_COMPLETED,
            created_at=timestamp_from_datetime(),
            completed_at=timestamp_from_datetime(),
            runs=[
                experiment_pb2.ExperimentRun(
                    experiment_run_id="experiment-run-seeded",
                    experiment_id="experiment-seeded",
                    run_id=run.run_id,
                    trace_id=run.trace_id,
                    kind=experiment_pb2.EXPERIMENT_RUN_KIND_SIMULATION,
                    data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
                    config_hash="cfg-1",
                    code_revision="rev-1",
                    policy_version=run.policy_version,
                )
            ],
            summary="Seeded synthetic comparison",
            results=[
                experiment_pb2.ExperimentResult(
                    result_id="result-seeded",
                    experiment_id="experiment-seeded",
                    observed_at=timestamp_from_datetime(),
                    summary="Synthetic baseline succeeded",
                    metrics=[experiment_pb2.MetricValue(name="reward", value=1.0, unit="score")],
                )
            ],
        )
