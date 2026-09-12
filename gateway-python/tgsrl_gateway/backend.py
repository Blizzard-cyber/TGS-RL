"""Gateway backend protocol and thin in-memory composition layer."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, cast

from tgsrl.v1 import control_pb2, experiment_pb2, job_pb2, resource_pb2, scheduling_pb2, trace_pb2

from tgsrl_gateway.backend_memory_experiments import (
    MemoryExperimentService,
    MemoryExperimentState,
)
from tgsrl_gateway.backend_memory_fixtures import (
    build_capability_set,
    build_default_job,
    build_execution_contract,
    build_resources,
    build_runtime_spec,
)
from tgsrl_gateway.backend_memory_jobs import (
    MemoryFixtureCounts,
    MemoryJobService,
    MemoryJobState,
)
from tgsrl_gateway.backend_memory_runtime import (
    MemoryRuntimeService,
    MemorySchedulerRuntimeState,
)
from tgsrl_gateway.errors import BadRequestError
from tgsrl_gateway.pagination import paginate
from tgsrl_gateway.protojson import timestamp_from_datetime


def _event_type_for_command(command: int) -> int:
    mapping = {
        control_pb2.JOB_COMMAND_TYPE_START: control_pb2.JOB_EVENT_TYPE_JOB_STARTED,
        control_pb2.JOB_COMMAND_TYPE_PAUSE: control_pb2.JOB_EVENT_TYPE_JOB_PAUSED,
        control_pb2.JOB_COMMAND_TYPE_RESUME: control_pb2.JOB_EVENT_TYPE_JOB_RESUMED,
        control_pb2.JOB_COMMAND_TYPE_STOP: control_pb2.JOB_EVENT_TYPE_JOB_STOPPED,
        control_pb2.JOB_COMMAND_TYPE_RETRY: control_pb2.JOB_EVENT_TYPE_JOB_RETRIED,
        control_pb2.JOB_COMMAND_TYPE_TERMINATE: control_pb2.JOB_EVENT_TYPE_JOB_TERMINATED,
    }
    return mapping[command]


def _operation_type_for_command(command: int) -> int:
    mapping = {
        control_pb2.JOB_COMMAND_TYPE_START: control_pb2.OPERATION_TYPE_START,
        control_pb2.JOB_COMMAND_TYPE_PAUSE: control_pb2.OPERATION_TYPE_PAUSE,
        control_pb2.JOB_COMMAND_TYPE_RESUME: control_pb2.OPERATION_TYPE_RESUME,
        control_pb2.JOB_COMMAND_TYPE_STOP: control_pb2.OPERATION_TYPE_STOP,
        control_pb2.JOB_COMMAND_TYPE_RETRY: control_pb2.OPERATION_TYPE_RETRY,
        control_pb2.JOB_COMMAND_TYPE_TERMINATE: control_pb2.OPERATION_TYPE_TERMINATE,
    }
    return mapping[command]


def _after_id_page(
    items: list[Any],
    *,
    after_id: str | None,
    id_attr: str,
    limit: int | None,
    scope: str,
    filters: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if not after_id:
        paged_items, next_page_token = paginate(
            items,
            page_token=None,
            limit=limit,
            scope=scope,
            filters=filters,
        )
        return {"items": paged_items, "next_page_token": next_page_token}
    offset = len(items)
    for index, item in enumerate(items):
        if getattr(item, id_attr, "") == after_id:
            offset = index + 1
            break
    paged_items, next_page_token = paginate(
        items,
        page_token=None,
        limit=limit,
        scope=scope,
        filters=filters,
        offset=offset,
    )
    return {"items": paged_items, "next_page_token": next_page_token}


def _normalize_data_kind_value(data_kind: int | None) -> int | None:
    if data_kind is None:
        return None
    descriptor = trace_pb2.DESCRIPTOR.pool.FindEnumTypeByName("tgsrl.v1.DataKind")
    if descriptor.values_by_number.get(data_kind) is None:
        raise BadRequestError("unsupported data_kind", details={"data_kind": data_kind})
    return data_kind


def _sorted_runs_desc_for_latest_contract(
    runs: list[control_pb2.JobRun],
) -> list[control_pb2.JobRun]:
    return sorted(
        runs,
        key=lambda run: (
            run.attempt,
            run.run_id,
        ),
        reverse=True,
    )


class GatewayBackend(Protocol):
    def list_jobs(self) -> list[job_pb2.RLTrainingJob]: ...

    def list_jobs_paginated(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_job_id: str | None,
        data_kind: int | None,
    ) -> dict[str, object]: ...

    def validate_job(
        self,
        job: job_pb2.RLTrainingJob,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]: ...

    def create_job(
        self,
        job: job_pb2.RLTrainingJob,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]: ...

    def get_job(self, job_id: str) -> job_pb2.RLTrainingJob: ...

    def admit_job(
        self,
        job_id: str,
        *,
        reason: str,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]: ...

    def create_job_run(
        self,
        job_id: str,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]: ...

    def list_runs(
        self,
        job_id: str,
        *,
        limit: int | None,
        page_token: str | None,
        after_run_id: str | None,
    ) -> dict[str, object]: ...

    def get_run(self, job_id: str, run_id: str) -> control_pb2.JobRun: ...

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
    ) -> dict[str, object]: ...

    def list_operations(
        self,
        *,
        job_id: str | None = None,
        run_id: str | None = None,
        operation_type: int | None = None,
        state: int | None = None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]: ...

    def get_operation(self, operation_id: str) -> control_pb2.Operation: ...

    def list_timeline(
        self,
        job_id: str,
        *,
        run_id: str | None,
        after_event_id: str | None,
        page_token: str | None,
        limit: int | None,
    ) -> dict[str, object]: ...

    def list_traces(
        self,
        job_id: str,
        *,
        run_id: str | None,
        trace_id: str | None,
        data_kind: int | None,
        page_token: str | None,
        limit: int | None,
    ) -> dict[str, object]: ...

    def get_dag(self, job_id: str, *, run_id: str | None) -> dict[str, object]: ...

    def get_topology(self, job_id: str, *, run_id: str | None) -> dict[str, object]: ...

    def get_resources(self) -> dict[str, object]: ...

    def list_sandboxes(
        self,
        job_id: str,
        *,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]: ...

    def list_decisions(
        self,
        job_id: str,
        *,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]: ...

    def get_decision(self, job_id: str, decision_id: str) -> scheduling_pb2.DecisionRecord: ...

    def list_replays(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_replay_id: str | None,
    ) -> dict[str, object]: ...

    def create_replay(
        self,
        replay: experiment_pb2.Replay,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]: ...

    def get_replay(self, replay_id: str) -> experiment_pb2.Replay: ...

    def apply_replay_command(
        self,
        replay_id: str,
        command: int,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]: ...

    def list_experiments(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_experiment_id: str | None,
    ) -> dict[str, object]: ...

    def create_experiment(
        self,
        experiment: experiment_pb2.Experiment,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]: ...

    def get_experiment(self, experiment_id: str) -> experiment_pb2.Experiment: ...

    def capabilities(self) -> dict[str, object]: ...

    def health(self) -> dict[str, object]: ...


class InMemoryGatewayBackend:
    """Explicit dev/test fixture backend composed from focused memory stores."""

    def __init__(self) -> None:
        self.job_state = MemoryJobState()
        self.runtime_state = MemorySchedulerRuntimeState()
        self.experiment_state = MemoryExperimentState()
        self.runtime_service = MemoryRuntimeService(self.runtime_state)
        self.experiment_service = MemoryExperimentService(self.experiment_state)
        self.job_service = MemoryJobService(
            state=self.job_state,
            runtime_service=self.runtime_service,
            capability_factory=build_capability_set,
            default_job_factory=build_default_job,
            execution_contract_factory=build_execution_contract,
            resources_factory=build_resources,
            runtime_spec_factory=build_runtime_spec,
        )
        self._seed()

    def _seed(self) -> None:
        job = self.job_service.seed_job("job-seeded", "Seeded Job")
        run = self.job_service.create_run(job, idempotency_key="seed-run")
        self.job_service.apply_job_command(
            job.job_id,
            run.run_id,
            control_pb2.JOB_COMMAND_TYPE_START,
            actor="seed",
            reason="seed startup",
            idempotency_key="seed-start",
            request_id="seed-start",
            event_type_for_command=_event_type_for_command,
            operation_type_for_command=_operation_type_for_command,
        )
        self.experiment_service.seed(
            replay=self.job_service.seed_replay(run=run),
            experiment=self.job_service.seed_experiment(run=run),
        )

    def list_jobs(self) -> list[job_pb2.RLTrainingJob]:
        return self.job_state.list_jobs()

    def get_resources(self) -> dict[str, object]:
        return {
            "snapshot": resource_pb2.ClusterSnapshot(
                snapshot_id="snapshot-demo-resources",
                revision=12,
                observed_at=timestamp_from_datetime(),
                devices=[
                    resource_pb2.Device(
                        device_id="GPU-A10-DEMO",
                        kind=resource_pb2.DEVICE_KIND_GPU,
                        health=resource_pb2.DEVICE_HEALTH_READY,
                        capacity=resource_pb2.ResourceVector(
                            accelerator_units=1,
                            memory_bytes=24 * 1024 * 1024 * 1024,
                        ),
                        allocatable=resource_pb2.ResourceVector(
                            accelerator_units=0.3,
                            memory_bytes=8 * 1024 * 1024 * 1024,
                        ),
                        capabilities=resource_pb2.CapabilitySet(
                            names=["nvidia-gpu", "hami-vgpu", "nvidia-mps"],
                            supported_actions=["bind", "set_share"],
                            attributes={
                                "partition_mode": "hami-core",
                                "driver_version": "580.178.04",
                            },
                        ),
                        labels={
                            "provider": "nvidia",
                            "node": "gpu-a10-01",
                            "name": "NVIDIA A10",
                            "uuid": "GPU-A10-DEMO",
                            "partition_mode": "hami-core",
                            "supports_full": "true",
                            "supports_hami": "true",
                            "supports_mps": "true",
                            "supports_mig": "false",
                        },
                    ),
                    resource_pb2.Device(
                        device_id="MIG-A100-DEMO",
                        kind=resource_pb2.DEVICE_KIND_GPU,
                        health=resource_pb2.DEVICE_HEALTH_READY,
                        capacity=resource_pb2.ResourceVector(
                            accelerator_units=1,
                            memory_bytes=10 * 1024 * 1024 * 1024,
                        ),
                        allocatable=resource_pb2.ResourceVector(
                            accelerator_units=1,
                            memory_bytes=10 * 1024 * 1024 * 1024,
                        ),
                        capabilities=resource_pb2.CapabilitySet(
                            names=["nvidia-gpu", "nvidia-mig"],
                            supported_actions=["bind", "rebind", "recreate"],
                            attributes={"partition_mode": "mig"},
                        ),
                        labels={
                            "provider": "nvidia",
                            "node": "gpu-a100-03",
                            "name": "NVIDIA A100",
                            "uuid": "MIG-A100-DEMO",
                            "parent_uuid": "GPU-A100-MIG-PARENT-DEMO",
                            "profile": "1g.10gb",
                            "partition_mode": "mig",
                            "supports_full": "false",
                            "supports_hami": "false",
                            "supports_mps": "false",
                            "supports_mig": "true",
                        },
                    ),
                ],
                allocations=[
                    resource_pb2.Allocation(
                        allocation_id="allocation-a10-rollout",
                        execution_id="exec-live-017",
                        stage_id="actor-rollout",
                        job_id="job-live-017",
                        pending_unit_id="unit-rollout-1",
                        device_ids=["GPU-A10-DEMO"],
                        resources=resource_pb2.ResourceVector(
                            accelerator_units=0.7,
                            memory_bytes=16 * 1024 * 1024 * 1024,
                        ),
                        state=resource_pb2.ALLOCATION_STATE_ACTIVE,
                        run_id="run-live-017-a",
                        sandbox_id="sbx-live-a14",
                        binding_id="binding-a10",
                        generation=7,
                    )
                ],
            )
        }

    def list_jobs_paginated(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_job_id: str | None,
        data_kind: int | None,
    ) -> dict[str, object]:
        normalized_data_kind = _normalize_data_kind_value(data_kind)
        jobs = self.job_state.list_jobs()
        if normalized_data_kind is not None:
            jobs = [job for job in jobs if job.data_kind == normalized_data_kind]
        filters = {"data_kind": normalized_data_kind}
        if page_token:
            items, next_page_token = paginate(
                jobs,
                page_token=page_token,
                limit=limit,
                scope="jobs",
                filters=filters,
            )
            return {"jobs": items, "next_page_token": next_page_token}
        page = _after_id_page(
            jobs,
            after_id=after_job_id,
            id_attr="job_id",
            limit=limit,
            scope="jobs",
            filters=filters,
        )
        return {"jobs": page["items"], "next_page_token": page["next_page_token"]}

    def validate_job(
        self,
        job: job_pb2.RLTrainingJob,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        return self.job_service.validate_job(
            job,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )

    def create_job(
        self,
        job: job_pb2.RLTrainingJob,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        normalized = self.job_service.normalize_job(job)
        return self.job_state.create_job(
            normalized,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )

    def get_job(self, job_id: str) -> job_pb2.RLTrainingJob:
        return self.job_state.get_job(job_id)

    def admit_job(
        self,
        job_id: str,
        *,
        reason: str,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        return self.job_service.admit_job(
            job_id,
            reason=reason,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )

    def create_job_run(
        self,
        job_id: str,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        return self.job_service.create_job_run(
            job_id,
            request_id=request_id,
            idempotency_key=idempotency_key,
        )

    def list_runs(
        self,
        job_id: str,
        *,
        limit: int | None,
        page_token: str | None,
        after_run_id: str | None,
    ) -> dict[str, object]:
        self.get_job(job_id)
        runs = _sorted_runs_desc_for_latest_contract(
            [
                self.job_state.get_run(job_id, run_id)
                for run_id in self.job_state.runs_by_job.get(job_id, [])
            ]
        )
        filters = {"job_id": job_id}
        if page_token:
            items, next_page_token = paginate(
                runs,
                page_token=page_token,
                limit=limit,
                scope="job_runs",
                filters=filters,
            )
            return {"runs": items, "next_page_token": next_page_token}
        page = _after_id_page(
            runs,
            after_id=after_run_id,
            id_attr="run_id",
            limit=limit,
            scope="job_runs",
            filters=filters,
        )
        return {"runs": page["items"], "next_page_token": page["next_page_token"]}

    def get_run(self, job_id: str, run_id: str) -> control_pb2.JobRun:
        return self.job_state.get_run(job_id, run_id)

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
    ) -> dict[str, object]:
        return self.job_service.apply_job_command(
            job_id,
            run_id,
            command,
            actor=actor,
            reason=reason,
            request_id=request_id,
            idempotency_key=idempotency_key,
            event_type_for_command=_event_type_for_command,
            operation_type_for_command=_operation_type_for_command,
        )

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
        return self.job_state.list_operations(
            job_id=job_id,
            run_id=run_id,
            operation_type=operation_type,
            state=state,
            limit=limit,
            page_token=page_token,
        )

    def get_operation(self, operation_id: str) -> control_pb2.Operation:
        return self.job_state.get_operation(operation_id)

    def list_timeline(
        self,
        job_id: str,
        *,
        run_id: str | None,
        after_event_id: str | None,
        page_token: str | None,
        limit: int | None,
    ) -> dict[str, object]:
        return self.job_service.list_timeline(
            job_id,
            run_id=run_id,
            after_event_id=after_event_id,
            page_token=page_token,
            limit=limit,
        )

    def list_traces(
        self,
        job_id: str,
        *,
        run_id: str | None,
        trace_id: str | None,
        data_kind: int | None,
        page_token: str | None,
        limit: int | None,
    ) -> dict[str, object]:
        self.get_job(job_id)
        selected_run_id = run_id
        if selected_run_id is None:
            latest = self.list_runs(job_id, limit=1, page_token=None, after_run_id=None)["runs"]
            latest_runs = cast(list[control_pb2.JobRun], latest)
            if not latest_runs:
                from tgsrl_gateway.errors import NotFoundError

                raise NotFoundError("run", "latest")
            selected_run_id = latest_runs[0].run_id
        self.get_run(job_id, selected_run_id)
        expected_trace_id = trace_id or ""
        return self.runtime_service.list_traces(
            job_id=job_id,
            run_id=selected_run_id,
            trace_id=expected_trace_id,
            data_kind=data_kind,
            limit=limit,
            page_token=page_token,
        )

    def get_dag(self, job_id: str, *, run_id: str | None) -> dict[str, object]:
        return self.job_service.get_dag(job_id, run_id=run_id)

    def get_topology(self, job_id: str, *, run_id: str | None) -> dict[str, object]:
        job = self.get_job(job_id)
        selected_run_id = run_id
        if selected_run_id is None:
            latest_runs = self.list_runs(
                job_id,
                limit=1,
                page_token=None,
                after_run_id=None,
            )["runs"]
            latest_runs_value = cast(list[control_pb2.JobRun], latest_runs)
            if not latest_runs_value:
                from tgsrl_gateway.errors import NotFoundError

                raise NotFoundError("run", "latest")
            selected_run_id = latest_runs_value[0].run_id
        run = self.get_run(job_id, selected_run_id)
        return self.runtime_service.get_topology(job=job, run=run)

    def list_sandboxes(
        self,
        job_id: str,
        *,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        self.get_job(job_id)
        return self.runtime_service.list_sandboxes(
            job_id=job_id,
            run_ids=self.job_state.runs_by_job.get(job_id, []),
            run_id=run_id,
            limit=limit,
            page_token=page_token,
        )

    def list_decisions(
        self,
        job_id: str,
        *,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        self.get_job(job_id)
        return self.runtime_service.list_decisions(
            job_id=job_id,
            run_id=run_id,
            limit=limit,
            page_token=page_token,
        )

    def get_decision(self, job_id: str, decision_id: str) -> scheduling_pb2.DecisionRecord:
        self.get_job(job_id)
        return self.runtime_service.get_decision(job_id=job_id, decision_id=decision_id)

    def list_replays(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_replay_id: str | None,
    ) -> dict[str, object]:
        experiment_service = cast(Any, self.experiment_service)
        replays = [
            experiment_service.get_replay(replay_id)
            for replay_id in sorted(experiment_service.state.replays)
        ]
        if page_token:
            items, next_page_token = paginate(
                replays,
                page_token=page_token,
                limit=limit,
                scope="replays",
            )
            return {"replays": items, "next_page_token": next_page_token}
        page = _after_id_page(
            replays,
            after_id=after_replay_id,
            id_attr="replay_id",
            limit=limit,
            scope="replays",
        )
        return {"replays": page["items"], "next_page_token": page["next_page_token"]}

    def create_replay(
        self,
        replay: experiment_pb2.Replay,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        return self.job_state.with_idempotency(
            "create-replay",
            idempotency_key,
            lambda: self.experiment_service.create_replay(replay, request_id=request_id),
        )

    def get_replay(self, replay_id: str) -> experiment_pb2.Replay:
        return self.experiment_service.get_replay(replay_id)

    def apply_replay_command(
        self,
        replay_id: str,
        command: int,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        return self.job_state.with_idempotency(
            f"replay-command:{replay_id}:{command}",
            idempotency_key,
            lambda: self.experiment_service.apply_replay_command(
                replay_id,
                command,
                request_id=request_id,
            ),
        )

    def list_experiments(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_experiment_id: str | None,
    ) -> dict[str, object]:
        experiment_service = cast(Any, self.experiment_service)
        experiments = [
            experiment_service.get_experiment(experiment_id)
            for experiment_id in sorted(experiment_service.state.experiments)
        ]
        if page_token:
            items, next_page_token = paginate(
                experiments,
                page_token=page_token,
                limit=limit,
                scope="experiments",
            )
            return {"experiments": items, "next_page_token": next_page_token}
        page = _after_id_page(
            experiments,
            after_id=after_experiment_id,
            id_attr="experiment_id",
            limit=limit,
            scope="experiments",
        )
        return {"experiments": page["items"], "next_page_token": page["next_page_token"]}

    def create_experiment(
        self,
        experiment: experiment_pb2.Experiment,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        return self.job_state.with_idempotency(
            "create-experiment",
            idempotency_key,
            lambda: self.experiment_service.create_experiment(
                experiment,
                request_id=request_id,
            ),
        )

    def get_experiment(self, experiment_id: str) -> experiment_pb2.Experiment:
        return self.experiment_service.get_experiment(experiment_id)

    def capabilities(self) -> dict[str, object]:
        return self.job_service.capabilities()

    def health(self) -> dict[str, object]:
        return self.job_service.health(
            MemoryFixtureCounts(
                jobs=len(self.job_state.jobs),
                runs=len(self.job_state.runs),
                operations=len(self.job_state.operations),
                decisions=self.runtime_service.decision_count(),
                replays=self.experiment_service.replay_count(),
                experiments=self.experiment_service.experiment_count(),
            )
        )


def command_name_to_enum(name: str) -> int:
    normalized = name.strip().lower()
    mapping = {
        "start": control_pb2.JOB_COMMAND_TYPE_START,
        "pause": control_pb2.JOB_COMMAND_TYPE_PAUSE,
        "resume": control_pb2.JOB_COMMAND_TYPE_RESUME,
        "stop": control_pb2.JOB_COMMAND_TYPE_STOP,
        "retry": control_pb2.JOB_COMMAND_TYPE_RETRY,
        "terminate": control_pb2.JOB_COMMAND_TYPE_TERMINATE,
    }
    if normalized not in mapping:
        raise BadRequestError("unsupported job command", details={"command": name})
    return mapping[normalized]


def replay_command_name_to_enum(name: str) -> int:
    normalized = name.strip().lower()
    mapping = {
        "start": experiment_pb2.REPLAY_COMMAND_TYPE_START,
        "pause": experiment_pb2.REPLAY_COMMAND_TYPE_PAUSE,
        "resume": experiment_pb2.REPLAY_COMMAND_TYPE_RESUME,
        "stop": experiment_pb2.REPLAY_COMMAND_TYPE_STOP,
        "terminate": experiment_pb2.REPLAY_COMMAND_TYPE_TERMINATE,
    }
    if normalized not in mapping:
        raise BadRequestError("unsupported replay command", details={"command": name})
    return mapping[normalized]


def optional_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise BadRequestError(
            "query parameter must be an integer", details={"value": value}
        ) from error
    if parsed <= 0:
        raise BadRequestError("query parameter must be positive", details={"value": value})
    return parsed
