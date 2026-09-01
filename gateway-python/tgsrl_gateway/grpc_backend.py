"""gRPC-backed repository implementation for the northbound gateway."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast

import grpc
from tgsrl.v1 import (
    control_pb2,
    control_pb2_grpc,
    experiment_pb2,
    experiment_pb2_grpc,
    runtime_pb2,
    runtime_pb2_grpc,
    scheduling_pb2,
    scheduling_pb2_grpc,
    trace_pb2,
)

from tgsrl_gateway.config import GatewayConfig
from tgsrl_gateway.errors import ConflictError, GatewayError, NotFoundError
from tgsrl_gateway.pagination import decode_backend_page_token, encode_backend_page_token
from tgsrl_gateway.protojson import clone_message

health_pb2 = None
health_pb2_grpc = None
try:
    import importlib

    health_pb2 = importlib.import_module("grpc_health.v1.health_pb2")
    health_pb2_grpc = importlib.import_module("grpc_health.v1.health_pb2_grpc")
except ImportError:  # pragma: no cover - optional dependency
    pass


def _grpc_status_to_gateway_error(error: grpc.RpcError) -> GatewayError:
    code = error.code()
    detail = error.details() or "backend request failed"
    if code == grpc.StatusCode.NOT_FOUND:
        return NotFoundError("backend_resource", error.details() or "unknown")
    if code == grpc.StatusCode.ALREADY_EXISTS:
        return ConflictError(error.details() or "resource already exists")
    if code in {grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.FAILED_PRECONDITION}:
        return GatewayError(
            code="backend_invalid_argument",
            message=error.details() or "backend rejected request",
            status=400,
            details={"grpc_code": code.name},
        )
    mapped = {
        grpc.StatusCode.UNAUTHENTICATED: ("backend_unauthenticated", 401),
        grpc.StatusCode.PERMISSION_DENIED: ("backend_forbidden", 403),
        grpc.StatusCode.RESOURCE_EXHAUSTED: ("backend_resource_exhausted", 429),
        grpc.StatusCode.UNIMPLEMENTED: ("backend_feature_unavailable", 501),
        grpc.StatusCode.DEADLINE_EXCEEDED: ("backend_timeout", 504),
    }.get(code)
    if mapped is not None:
        error_code, http_status = mapped
        return GatewayError(
            code=error_code,
            message=detail,
            status=http_status,
            details={"grpc_code": code.name},
        )
    return GatewayError(
        code="backend_unavailable",
        message=detail,
        status=503,
        details={"grpc_code": code.name},
    )


@dataclass(slots=True)
class DependencyStatus:
    name: str
    target: str
    serving: bool
    detail: str


class GrpcGatewayBackend:
    """Gateway backend that proxies to gRPC control/runtime/scheduler/experiment services."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        job_control_channel: grpc.Channel | None = None,
        scheduler_channel: grpc.Channel | None = None,
        runtime_channel: grpc.Channel | None = None,
        experiment_channel: grpc.Channel | None = None,
    ) -> None:
        self.config = config
        self._job_control_channel = job_control_channel or grpc.insecure_channel(
            self.config.job_control_target
        )
        self._scheduler_channel = scheduler_channel or grpc.insecure_channel(
            self.config.scheduler_target
        )
        self._runtime_channel = runtime_channel or grpc.insecure_channel(self.config.runtime_target)
        self._experiment_channel = experiment_channel or grpc.insecure_channel(
            self.config.experiment_target
        )
        self._job_control = control_pb2_grpc.JobControlServiceStub(self._job_control_channel)
        self._scheduler = scheduling_pb2_grpc.SchedulerServiceStub(self._scheduler_channel)
        self._runtime = runtime_pb2_grpc.RuntimeControlServiceStub(self._runtime_channel)
        self._experiment = experiment_pb2_grpc.ExperimentServiceStub(self._experiment_channel)

    @staticmethod
    def _message_has_field(message_cls: type[Any], field_name: str) -> bool:
        descriptor = getattr(message_cls, "DESCRIPTOR", None)
        return descriptor is not None and field_name in descriptor.fields_by_name

    @staticmethod
    def _build_request(message_cls: type[Any], **kwargs: object) -> Any:
        descriptor = getattr(message_cls, "DESCRIPTOR", None)
        if descriptor is None:
            filtered = {key: value for key, value in kwargs.items() if value is not None}
        else:
            filtered = {
                key: value
                for key, value in kwargs.items()
                if value is not None and key in descriptor.fields_by_name
            }
        return message_cls(**filtered)

    @staticmethod
    def _require_rpc(
        stub: Any, method_name: str, request_cls: type[Any] | None
    ) -> Callable[..., Any]:
        method = getattr(stub, method_name, None)
        if callable(method) and request_cls is not None:
            return cast(Callable[..., Any], method)
        raise GatewayError(
            code="backend_feature_unavailable",
            message=f"backend does not expose unary {method_name} RPC",
            status=501,
            details={"rpc": method_name},
        )

    def _call(self, func: Callable[..., Any], request: Any, *, timeout: float | None = None) -> Any:
        try:
            return func(request, timeout=timeout or self.config.grpc_timeout_seconds)
        except grpc.RpcError as error:
            raise _grpc_status_to_gateway_error(error) from error

    @staticmethod
    def _decode_northbound_page_token(
        page_token: str | None,
        *,
        scope: str,
        filters: Mapping[str, object] | None = None,
    ) -> str:
        return (
            decode_backend_page_token(page_token, scope=scope, filters=filters)
            if page_token
            else ""
        )

    @staticmethod
    def _wrap_next_page_token(
        backend_token: str,
        *,
        scope: str,
        filters: Mapping[str, object] | None = None,
    ) -> str:
        return (
            encode_backend_page_token(
                backend_cursor=backend_token,
                scope=scope,
                filters=filters,
            )
            if backend_token
            else ""
        )

    def _probe_dependency_with_health(
        self,
        channel: grpc.Channel,
    ) -> DependencyStatus | None:
        if health_pb2 is None or health_pb2_grpc is None:
            return None
        stub = health_pb2_grpc.HealthStub(channel)
        try:
            response = stub.Check(
                health_pb2.HealthCheckRequest(service=""),
                timeout=self.config.health_timeout_seconds,
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNIMPLEMENTED:
                return None
            return DependencyStatus(
                name="",
                target="",
                serving=False,
                detail=f"grpc_health:{error.code().name.lower()}",
            )
        serving_status = getattr(health_pb2.HealthCheckResponse, "SERVING", None)
        is_serving = response.status == serving_status
        return DependencyStatus(
            name="",
            target="",
            serving=is_serving,
            detail=f"grpc_health:{health_pb2.HealthCheckResponse.ServingStatus.Name(response.status).lower()}",
        )

    def _probe_dependency_with_rpc(
        self,
        *,
        name: str,
        target: str,
        probe: Callable[[], Any],
    ) -> DependencyStatus:
        try:
            probe()
            return DependencyStatus(
                name=name,
                target=target,
                serving=True,
                detail="grpc_probe:ok",
            )
        except grpc.RpcError as error:
            ready_codes = {
                grpc.StatusCode.NOT_FOUND,
                grpc.StatusCode.INVALID_ARGUMENT,
                grpc.StatusCode.FAILED_PRECONDITION,
                grpc.StatusCode.ALREADY_EXISTS,
            }
            return DependencyStatus(
                name=name,
                target=target,
                serving=error.code() in ready_codes,
                detail=f"grpc_probe:{error.code().name.lower()}",
            )

    def _probe_dependency(
        self,
        *,
        name: str,
        target: str,
        channel: grpc.Channel,
        probe: Callable[[], Any],
    ) -> DependencyStatus:
        health_status = self._probe_dependency_with_health(channel)
        if health_status is not None:
            return DependencyStatus(
                name=name,
                target=target,
                serving=health_status.serving,
                detail=health_status.detail,
            )
        return self._probe_dependency_with_rpc(name=name, target=target, probe=probe)

    def list_jobs_paginated(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_job_id: str | None,
        data_kind: int | None,
    ) -> dict[str, object]:
        filters = {"data_kind": data_kind}
        request = control_pb2.ListJobsRequest(
            limit=50 if limit is None else limit,
            page_token=self._decode_northbound_page_token(
                page_token,
                scope="jobs",
                filters=filters,
            ),
            after_job_id=after_job_id or "",
        )
        if data_kind is not None:
            request.data_kind = data_kind
        response = self._call(self._job_control.ListJobs, request)
        return {
            "jobs": [clone_message(job) for job in response.jobs],
            "next_page_token": self._wrap_next_page_token(
                response.next_page_token,
                scope="jobs",
                filters=filters,
            ),
        }

    def validate_job(
        self,
        job: Any,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        response = self._call(
            self._job_control.ValidateJob,
            control_pb2.ValidateJobRequest(
                job=job,
                request_id=request_id,
                idempotency_key=idempotency_key,
            ),
        )
        return {
            "valid": response.valid,
            "normalized_job": clone_message(response.normalized_job),
            "diagnostics": list(response.diagnostics),
            "operation": clone_message(response.operation),
        }

    def create_job(self, job: Any, *, request_id: str, idempotency_key: str) -> dict[str, object]:
        response = self._call(
            self._job_control.CreateJob,
            control_pb2.CreateJobRequest(
                job=job,
                request_id=request_id,
                idempotency_key=idempotency_key,
            ),
        )
        return {"job": clone_message(response.job), "operation": clone_message(response.operation)}

    def get_job(self, job_id: str) -> Any:
        response = self._call(self._job_control.GetJob, control_pb2.GetJobRequest(job_id=job_id))
        return clone_message(response.job)

    def admit_job(
        self,
        job_id: str,
        *,
        reason: str,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        response = self._call(
            self._job_control.AdmitJob,
            control_pb2.AdmitJobRequest(
                job_id=job_id,
                reason=reason,
                request_id=request_id,
                idempotency_key=idempotency_key,
            ),
        )
        return {"job": clone_message(response.job), "operation": clone_message(response.operation)}

    def create_job_run(
        self,
        job_id: str,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        request_cls = control_pb2.CreateJobRunRequest
        if self._message_has_field(request_cls, "job_id"):
            request = request_cls(
                job_id=job_id,
                request_id=request_id,
                idempotency_key=idempotency_key,
            )
        else:
            request = request_cls(
                job=self.get_job(job_id),
                request_id=request_id,
                idempotency_key=idempotency_key,
            )
        response = self._call(
            self._job_control.CreateJobRun,
            request,
        )
        return {"run": clone_message(response.run)}

    def list_runs(
        self,
        job_id: str,
        *,
        limit: int | None,
        page_token: str | None,
        after_run_id: str | None,
    ) -> dict[str, object]:
        filters = {"job_id": job_id}
        response = self._call(
            self._job_control.ListJobRuns,
            control_pb2.ListJobRunsRequest(
                job_id=job_id,
                limit=50 if limit is None else limit,
                page_token=self._decode_northbound_page_token(
                    page_token,
                    scope="job_runs",
                    filters=filters,
                ),
                after_run_id=after_run_id or "",
            ),
        )
        return {
            "runs": [clone_message(run) for run in response.runs],
            "next_page_token": self._wrap_next_page_token(
                response.next_page_token,
                scope="job_runs",
                filters=filters,
            ),
        }

    def get_run(self, job_id: str, run_id: str) -> Any:
        response = self._call(
            self._job_control.GetJobRun,
            control_pb2.GetJobRunRequest(job_id=job_id, run_id=run_id),
        )
        return clone_message(response.run)

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
        response = self._call(
            self._job_control.ApplyJobCommand,
            control_pb2.ApplyJobCommandRequest(
                job_id=job_id,
                run_id=run_id,
                command=command,
                actor=actor,
                reason=reason,
                request_id=request_id,
                idempotency_key=idempotency_key,
            ),
            timeout=self.config.command_timeout_seconds,
        )
        return {"operation": clone_message(response.operation), "run": clone_message(response.run)}

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
        filters = {
            "job_id": job_id,
            "run_id": run_id,
            "operation_type": operation_type,
            "state": state,
        }
        request_cls = cast(type[Any] | None, getattr(control_pb2, "ListOperationsRequest", None))
        assert request_cls is not None
        rpc = self._require_rpc(self._job_control, "ListOperations", request_cls)
        response = self._call(
            rpc,
            self._build_request(
                request_cls,
                job_id=job_id or "",
                run_id=run_id or "",
                type=operation_type,
                state=state,
                limit=50 if limit is None else limit,
                page_token=self._decode_northbound_page_token(
                    page_token,
                    scope="operations",
                    filters=filters,
                ),
            ),
        )
        return {
            "operations": [clone_message(operation) for operation in response.operations],
            "next_page_token": self._wrap_next_page_token(
                getattr(response, "next_page_token", ""),
                scope="operations",
                filters=filters,
            ),
        }

    def get_operation(self, operation_id: str) -> Any:
        response = self._call(
            self._job_control.GetOperation,
            control_pb2.GetOperationRequest(operation_id=operation_id),
        )
        return clone_message(response.operation)

    def list_timeline(
        self,
        job_id: str,
        *,
        run_id: str | None,
        after_event_id: str | None,
        page_token: str | None,
        limit: int | None,
    ) -> dict[str, object]:
        filters = {"job_id": job_id, "run_id": run_id}
        response = self._call(
            self._job_control.ListJobEvents,
            control_pb2.ListJobEventsRequest(
                job_id=job_id,
                run_id=run_id or "",
                after_event_id=after_event_id or "",
                page_token=self._decode_northbound_page_token(
                    page_token,
                    scope="timeline",
                    filters=filters,
                ),
                limit=50 if limit is None else limit,
            ),
        )
        return {
            "events": [clone_message(event) for event in response.events],
            "next_page_token": self._wrap_next_page_token(
                response.next_page_token,
                scope="timeline",
                filters=filters,
            ),
        }

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
        selected_run_id = run_id
        if selected_run_id is None:
            runs_value = self.list_runs(job_id, limit=1, page_token=None, after_run_id=None)["runs"]
            runs = cast(list[Any], runs_value)
            if not runs:
                raise NotFoundError("run", "latest")
            selected_run_id = cast(Any, runs[0]).run_id
        self.get_run(job_id, selected_run_id)
        selected_trace_id = trace_id or ""
        filters = {
            "job_id": job_id,
            "run_id": selected_run_id,
            "trace_id": selected_trace_id,
            "data_kind": data_kind,
        }
        response = self._call(
            self._runtime.ListTraceEvents,
            runtime_pb2.ListTraceEventsRequest(
                run_id=selected_run_id,
                job_id=job_id,
                trace_id=selected_trace_id,
                data_kind=data_kind or trace_pb2.DATA_KIND_UNKNOWN,
                limit=100 if limit is None else limit,
                page_token=self._decode_northbound_page_token(
                    page_token, scope="traces", filters=filters
                ),
            ),
        )
        return {
            "events": [clone_message(event) for event in response.events],
            "next_page_token": self._wrap_next_page_token(
                response.next_page_token, scope="traces", filters=filters
            ),
        }

    def get_dag(self, job_id: str, *, run_id: str | None) -> dict[str, object]:
        job = self.get_job(job_id)
        selected_run = self.get_run(job_id, run_id) if run_id else None
        graph = clone_message(job.execution_contract.phase_graph)
        dependencies = {
            phase.phase_id: sorted(
                edge.from_phase_id for edge in graph.edges if edge.to_phase_id == phase.phase_id
            )
            for phase in graph.phases
        }
        return {"job": job, "run": selected_run, "phase_graph": graph, "dependencies": dependencies}

    def get_topology(self, job_id: str, *, run_id: str | None) -> dict[str, object]:
        selected_run_id = run_id
        if selected_run_id is None:
            runs_value = self.list_runs(
                job_id,
                limit=1,
                page_token=None,
                after_run_id=None,
            )["runs"]
            runs = cast(list[Any], runs_value)
            if not runs:
                raise NotFoundError("run", "latest")
            selected_run_id = cast(Any, runs[0]).run_id
        run = self.get_run(job_id, selected_run_id)
        manifest_response = self._call(
            self._runtime.GetRuntimeManifest,
            runtime_pb2.GetRuntimeManifestRequest(run_id=selected_run_id),
        )
        status_response = self._call(
            self._runtime.GetRuntimeStatus,
            runtime_pb2.GetRuntimeStatusRequest(run_id=selected_run_id),
        )
        decision = None
        request_cls = cast(type[Any] | None, getattr(scheduling_pb2, "ListDecisionsRequest", None))
        list_rpc = getattr(self._scheduler, "ListDecisions", None)
        if request_cls is not None and callable(list_rpc):
            response = self._call(
                cast(Callable[..., Any], list_rpc),
                self._build_request(
                    request_cls,
                    job_id=job_id,
                    job_ids=[job_id],
                    run_id=selected_run_id,
                    limit=1,
                ),
            )
            if response.decisions:
                decision = clone_message(response.decisions[0])
        return {
            "run": run,
            "manifest": clone_message(manifest_response.manifest),
            "runtime_units": [clone_message(unit) for unit in status_response.runtime_units],
            "sandboxes": [clone_message(sandbox) for sandbox in status_response.sandboxes],
            "decision": decision,
        }

    def list_sandboxes(
        self,
        job_id: str,
        *,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        filters = {"job_id": job_id, "run_id": run_id}
        response = self._call(
            self._runtime.ListSandboxes,
            runtime_pb2.ListSandboxesRequest(
                run_id=run_id or "",
                job_id=job_id,
                limit=50 if limit is None else limit,
                page_token=self._decode_northbound_page_token(
                    page_token,
                    scope="sandboxes",
                    filters=filters,
                ),
            ),
        )
        return {
            "sandboxes": [clone_message(sandbox) for sandbox in response.sandboxes],
            "next_page_token": self._wrap_next_page_token(
                getattr(response, "next_page_token", ""),
                scope="sandboxes",
                filters=filters,
            ),
        }

    def list_decisions(
        self,
        job_id: str,
        *,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        filters = {"job_id": job_id, "run_id": run_id}
        request_cls = cast(type[Any] | None, getattr(scheduling_pb2, "ListDecisionsRequest", None))
        assert request_cls is not None
        rpc = self._require_rpc(self._scheduler, "ListDecisions", request_cls)
        response = self._call(
            rpc,
            self._build_request(
                request_cls,
                job_id=job_id,
                job_ids=[job_id],
                run_id=run_id or "",
                limit=50 if limit is None else limit,
                page_token=self._decode_northbound_page_token(
                    page_token,
                    scope="decisions",
                    filters=filters,
                ),
            ),
        )
        return {
            "decisions": [clone_message(decision) for decision in response.decisions],
            "next_page_token": self._wrap_next_page_token(
                getattr(response, "next_page_token", ""),
                scope="decisions",
                filters=filters,
            ),
        }

    def get_decision(self, job_id: str, decision_id: str) -> Any:
        request_cls = cast(type[Any] | None, getattr(scheduling_pb2, "GetDecisionRequest", None))
        assert request_cls is not None
        rpc = self._require_rpc(self._scheduler, "GetDecision", request_cls)
        response = self._call(
            rpc,
            self._build_request(
                request_cls,
                job_id=job_id,
                decision_id=decision_id,
            ),
        )
        decision = clone_message(response.decision)
        if getattr(decision, "job_id", "") == job_id:
            return decision
        page_token: str | None = None
        while True:
            scoped = self.list_decisions(
                job_id,
                run_id=None,
                limit=100,
                page_token=page_token,
            )
            decisions = cast(list[Any], scoped["decisions"])
            if any(getattr(item, "decision_id", "") == decision_id for item in decisions):
                return decision
            next_page_token = cast(str, scoped["next_page_token"])
            if not next_page_token:
                break
            page_token = next_page_token
        raise NotFoundError("decision", decision_id)

    def list_replays(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_replay_id: str | None,
    ) -> dict[str, object]:
        response = self._call(
            self._experiment.ListReplays,
            experiment_pb2.ListReplaysRequest(
                limit=50 if limit is None else limit,
                page_token=self._decode_northbound_page_token(
                    page_token,
                    scope="replays",
                ),
                after_replay_id=after_replay_id or "",
            ),
        )
        return {
            "replays": [clone_message(replay) for replay in response.replays],
            "next_page_token": self._wrap_next_page_token(
                response.next_page_token,
                scope="replays",
            ),
        }

    def create_replay(
        self,
        replay: Any,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        response = self._call(
            self._experiment.CreateReplay,
            experiment_pb2.CreateReplayRequest(
                replay=replay,
                request_id=request_id,
                idempotency_key=idempotency_key,
            ),
        )
        return {"replay": clone_message(response.replay), "request_id": request_id}

    def get_replay(self, replay_id: str) -> Any:
        response = self._call(
            self._experiment.GetReplay,
            experiment_pb2.GetReplayRequest(replay_id=replay_id),
        )
        return clone_message(response.replay)

    def apply_replay_command(
        self,
        replay_id: str,
        command: int,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        response = self._call(
            self._experiment.ApplyReplayCommand,
            experiment_pb2.ApplyReplayCommandRequest(
                replay_id=replay_id,
                command=command,
                request_id=request_id,
                idempotency_key=idempotency_key,
            ),
        )
        return {"replay": clone_message(response.replay), "request_id": request_id}

    def list_experiments(
        self,
        *,
        limit: int | None,
        page_token: str | None,
        after_experiment_id: str | None,
    ) -> dict[str, object]:
        response = self._call(
            self._experiment.ListExperiments,
            experiment_pb2.ListExperimentsRequest(
                limit=50 if limit is None else limit,
                page_token=self._decode_northbound_page_token(
                    page_token,
                    scope="experiments",
                ),
                after_experiment_id=after_experiment_id or "",
            ),
        )
        return {
            "experiments": [clone_message(experiment) for experiment in response.experiments],
            "next_page_token": self._wrap_next_page_token(
                response.next_page_token,
                scope="experiments",
            ),
        }

    def create_experiment(
        self,
        experiment: Any,
        *,
        request_id: str,
        idempotency_key: str,
    ) -> dict[str, object]:
        response = self._call(
            self._experiment.CreateExperiment,
            experiment_pb2.CreateExperimentRequest(
                experiment=experiment,
                request_id=request_id,
                idempotency_key=idempotency_key,
            ),
        )
        return {"experiment": clone_message(response.experiment), "request_id": request_id}

    def get_experiment(self, experiment_id: str) -> Any:
        response = self._call(
            self._experiment.GetExperiment,
            experiment_pb2.GetExperimentRequest(experiment_id=experiment_id),
        )
        return clone_message(response.experiment)

    def capabilities(self) -> dict[str, object]:
        list_operations_unary = callable(
            getattr(self._job_control, "ListOperations", None)
        ) and hasattr(control_pb2, "ListOperationsRequest")
        list_decisions_unary = callable(
            getattr(self._scheduler, "ListDecisions", None)
        ) and hasattr(scheduling_pb2, "ListDecisionsRequest")
        get_decision_unary = callable(getattr(self._scheduler, "GetDecision", None)) and hasattr(
            scheduling_pb2, "GetDecisionRequest"
        )
        return {
            "protocol_version": "v0.3",
            "openapi_version": "3.1.0",
            "backend_mode": "grpc",
            "contract_features": {
                "list_operations_unary": list_operations_unary,
                "list_decisions_unary": list_decisions_unary,
                "get_decision_unary": get_decision_unary,
                "create_job_run_job_id": self._message_has_field(
                    control_pb2.CreateJobRunRequest,
                    "job_id",
                ),
            },
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
                "/v1/jobs/{job_id}/traces",
                "/v1/operations",
                "/v1/replays",
                "/v1/experiments",
            ],
        }

    def dependency_status(self) -> list[DependencyStatus]:
        invalid_statuses: list[DependencyStatus] = []
        for name, target in (
            ("job_control", self.config.job_control_target),
            ("scheduler", self.config.scheduler_target),
            ("runtime", self.config.runtime_target),
            ("experiment", self.config.experiment_target),
        ):
            host, _, port_text = target.rpartition(":")
            if not host or not port_text:
                invalid_statuses.append(
                    DependencyStatus(
                        name=name,
                        target=target,
                        serving=False,
                        detail="invalid target",
                    )
                )
                continue
            try:
                int(port_text)
            except ValueError:
                invalid_statuses.append(
                    DependencyStatus(
                        name=name,
                        target=target,
                        serving=False,
                        detail="invalid target",
                    )
                )
        if invalid_statuses:
            return invalid_statuses

        return [
            self._probe_dependency(
                name="job_control",
                target=self.config.job_control_target,
                channel=self._job_control_channel,
                probe=lambda: self._job_control.ListJobs(
                    control_pb2.ListJobsRequest(limit=1, page_token=""),
                    timeout=self.config.health_timeout_seconds,
                ),
            ),
            self._probe_dependency(
                name="scheduler",
                target=self.config.scheduler_target,
                channel=self._scheduler_channel,
                probe=lambda: self._scheduler.ListDecisions(
                    scheduling_pb2.ListDecisionsRequest(job_id="__healthcheck__", limit=1),
                    timeout=self.config.health_timeout_seconds,
                ),
            ),
            self._probe_dependency(
                name="runtime",
                target=self.config.runtime_target,
                channel=self._runtime_channel,
                probe=lambda: self._runtime.GetRuntimeStatus(
                    runtime_pb2.GetRuntimeStatusRequest(run_id="__healthcheck__"),
                    timeout=self.config.health_timeout_seconds,
                ),
            ),
            self._probe_dependency(
                name="experiment",
                target=self.config.experiment_target,
                channel=self._experiment_channel,
                probe=lambda: self._experiment.ListExperiments(
                    experiment_pb2.ListExperimentsRequest(limit=1, page_token=""),
                    timeout=self.config.health_timeout_seconds,
                ),
            ),
        ]

    def health(self) -> dict[str, object]:
        dependencies = self.dependency_status()
        return {
            "status": "ok" if all(item.serving for item in dependencies) else "degraded",
            "backend": "grpc",
            "dependencies": [
                {
                    "name": item.name,
                    "target": item.target,
                    "serving": item.serving,
                    "detail": item.detail,
                }
                for item in dependencies
            ],
        }
