"""End-to-end contract tests for the northbound gateway API, SDK, and CLI."""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import subprocess
import time
from collections.abc import Callable, Iterator, Mapping
from concurrent import futures
from contextlib import contextmanager
from email.message import Message
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError
from wsgiref.types import WSGIApplication
from wsgiref.util import setup_testing_defaults

import grpc
import pytest
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
from tgsrl_gateway.app import create_app
from tgsrl_gateway.backend import InMemoryGatewayBackend
from tgsrl_gateway.config import GatewayConfig
from tgsrl_gateway.contracts import (
    ROUTE_CONTRACTS,
    all_route_paths,
    cli_command_names,
    sdk_method_names,
)
from tgsrl_gateway.errors import GatewayError
from tgsrl_gateway.sdk import GatewayClient

ROOT = Path(__file__).resolve().parents[2]
VENV_PYTHON = ROOT / ".venv/bin/python"
type JsonObject = dict[str, Any]


class WsgiHarness:
    def __init__(self, app: WSGIApplication | None = None) -> None:
        self.app: WSGIApplication = app or create_app(InMemoryGatewayBackend()).__call__
        self.job_control_servicer: _JobControlServicer | None = None
        self.last_response_headers: list[tuple[str, str]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        body: JsonObject | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, JsonObject]:
        from io import BytesIO

        payload = json.dumps(body or {}).encode("utf-8")
        environ: dict[str, object] = {}
        setup_testing_defaults(environ)
        environ["REQUEST_METHOD"] = method.upper()
        route, _, query = path.partition("?")
        environ["PATH_INFO"] = route
        environ["QUERY_STRING"] = query
        environ["CONTENT_TYPE"] = "application/json"
        environ["CONTENT_LENGTH"] = str(len(payload))
        environ["wsgi.input"] = BytesIO(payload)
        for key, value in (headers or {}).items():
            environ[f"HTTP_{key.upper().replace('-', '_')}"] = value
        captured: dict[str, object] = {}

        def start_response(
            status: str,
            response_headers: list[tuple[str, str]],
            exc_info: object = None,
        ) -> Callable[[bytes], object]:
            captured["status"] = status
            captured["headers"] = response_headers
            self.last_response_headers = response_headers
            return lambda _data: None

        chunks = self.app(environ, start_response)
        body_bytes = b"".join(chunks)
        code = int(str(captured["status"]).split()[0])
        return code, cast(JsonObject, json.loads(body_bytes.decode("utf-8")))


class _JobControlServicer(control_pb2_grpc.JobControlServiceServicer):
    def __init__(self, backend: InMemoryGatewayBackend) -> None:
        self.backend = backend
        self.last_create_run_request: control_pb2.CreateJobRunRequest | None = None
        self.last_list_jobs_request: control_pb2.ListJobsRequest | None = None
        self.last_list_runs_request: control_pb2.ListJobRunsRequest | None = None
        self.last_list_events_request: control_pb2.ListJobEventsRequest | None = None
        self.last_list_operations_request: control_pb2.ListOperationsRequest | None = None

    def ListJobs(
        self, request: control_pb2.ListJobsRequest, context: grpc.ServicerContext
    ) -> control_pb2.ListJobsResponse:
        self.last_list_jobs_request = control_pb2.ListJobsRequest()
        self.last_list_jobs_request.CopyFrom(request)
        payload = self.backend.list_jobs_paginated(
            limit=request.limit or None,
            page_token=None,
            after_job_id=request.after_job_id or None,
            data_kind=request.data_kind or None,
        )
        return control_pb2.ListJobsResponse(
            jobs=payload["jobs"],
            next_page_token=(
                f"downstream-jobs:{request.data_kind}:{request.page_token or 'start'}"
            ),
        )

    def ValidateJob(
        self, request: control_pb2.ValidateJobRequest, context: grpc.ServicerContext
    ) -> control_pb2.ValidateJobResponse:
        payload = self.backend.validate_job(
            request.job,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return control_pb2.ValidateJobResponse(
            valid=bool(payload["valid"]),
            normalized_job=payload["normalized_job"],
            diagnostics=payload["diagnostics"],
            operation=payload["operation"],
        )

    def CreateJob(
        self, request: control_pb2.CreateJobRequest, context: grpc.ServicerContext
    ) -> control_pb2.CreateJobResponse:
        payload = self.backend.create_job(
            request.job,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return control_pb2.CreateJobResponse(job=payload["job"], operation=payload["operation"])

    def GetJob(
        self, request: control_pb2.GetJobRequest, context: grpc.ServicerContext
    ) -> control_pb2.GetJobResponse:
        try:
            job = self.backend.get_job(request.job_id)
        except GatewayError as error:
            context.abort(grpc.StatusCode.NOT_FOUND, error.message)
        return control_pb2.GetJobResponse(job=job)

    def AdmitJob(
        self, request: control_pb2.AdmitJobRequest, context: grpc.ServicerContext
    ) -> control_pb2.AdmitJobResponse:
        payload = self.backend.admit_job(
            request.job_id,
            reason=request.reason,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return control_pb2.AdmitJobResponse(job=payload["job"], operation=payload["operation"])

    def CreateJobRun(
        self, request: control_pb2.CreateJobRunRequest, context: grpc.ServicerContext
    ) -> control_pb2.CreateJobRunResponse:
        self.last_create_run_request = control_pb2.CreateJobRunRequest()
        self.last_create_run_request.CopyFrom(request)
        if request.DESCRIPTOR.fields_by_name.get("job_id") is not None and request.job_id:
            job_id = request.job_id
        else:
            job_id = request.job.job_id
        payload = self.backend.create_job_run(
            job_id,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return control_pb2.CreateJobRunResponse(run=payload["run"])

    def ListJobRuns(
        self, request: control_pb2.ListJobRunsRequest, context: grpc.ServicerContext
    ) -> control_pb2.ListJobRunsResponse:
        self.last_list_runs_request = control_pb2.ListJobRunsRequest()
        self.last_list_runs_request.CopyFrom(request)
        payload = self.backend.list_runs(
            request.job_id,
            limit=request.limit or None,
            page_token=None,
            after_run_id=request.after_run_id or None,
        )
        return control_pb2.ListJobRunsResponse(
            runs=payload["runs"],
            next_page_token=f"downstream-runs:{request.job_id}:{request.page_token or 'start'}",
        )

    def GetJobRun(
        self, request: control_pb2.GetJobRunRequest, context: grpc.ServicerContext
    ) -> control_pb2.GetJobRunResponse:
        return control_pb2.GetJobRunResponse(
            run=self.backend.get_run(request.job_id, request.run_id)
        )

    def ListJobEvents(
        self, request: control_pb2.ListJobEventsRequest, context: grpc.ServicerContext
    ) -> control_pb2.ListJobEventsResponse:
        self.last_list_events_request = control_pb2.ListJobEventsRequest()
        self.last_list_events_request.CopyFrom(request)
        payload = self.backend.list_timeline(
            request.job_id,
            run_id=request.run_id or None,
            after_event_id=request.after_event_id or None,
            page_token=None,
            limit=request.limit or None,
        )
        return control_pb2.ListJobEventsResponse(
            events=payload["events"],
            next_page_token=f"downstream-events:{request.job_id}:{request.run_id or 'all'}",
        )

    def ApplyJobCommand(
        self, request: control_pb2.ApplyJobCommandRequest, context: grpc.ServicerContext
    ) -> control_pb2.ApplyJobCommandResponse:
        payload = self.backend.apply_job_command(
            request.job_id,
            request.run_id,
            request.command,
            actor=request.actor,
            reason=request.reason,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return control_pb2.ApplyJobCommandResponse(
            operation=payload["operation"],
            run=payload["run"],
        )

    def GetOperation(
        self, request: control_pb2.GetOperationRequest, context: grpc.ServicerContext
    ) -> control_pb2.GetOperationResponse:
        return control_pb2.GetOperationResponse(
            operation=self.backend.get_operation(request.operation_id)
        )

    def ListOperations(
        self, request: control_pb2.ListOperationsRequest, context: grpc.ServicerContext
    ) -> control_pb2.ListOperationsResponse:
        self.last_list_operations_request = control_pb2.ListOperationsRequest()
        self.last_list_operations_request.CopyFrom(request)
        payload = self.backend.list_operations(
            limit=request.limit or None,
            page_token=None,
        )
        operations = cast(list[control_pb2.Operation], payload["operations"])
        if request.job_id:
            operations = [
                operation for operation in operations if operation.job_id == request.job_id
            ]
        if request.run_id:
            operations = [
                operation for operation in operations if operation.run_id == request.run_id
            ]
        if request.type:
            operations = [operation for operation in operations if operation.type == request.type]
        if request.state:
            operations = [operation for operation in operations if operation.state == request.state]
        return control_pb2.ListOperationsResponse(
            operations=operations,
            next_page_token=(
                "downstream-operations:"
                f"{request.job_id or 'all'}:{request.run_id or 'all'}:"
                f"{request.type}:{request.state}"
            ),
        )


class _SchedulerServicer(scheduling_pb2_grpc.SchedulerServiceServicer):
    def __init__(self, backend: InMemoryGatewayBackend) -> None:
        self.backend = backend
        self.last_list_decisions_request: scheduling_pb2.ListDecisionsRequest | None = None

    def ListDecisions(
        self, request: scheduling_pb2.ListDecisionsRequest, context: grpc.ServicerContext
    ) -> scheduling_pb2.ListDecisionsResponse:
        self.last_list_decisions_request = scheduling_pb2.ListDecisionsRequest()
        self.last_list_decisions_request.CopyFrom(request)
        try:
            payload = self.backend.list_decisions(
                request.job_id,
                run_id=request.run_id or None,
                limit=request.limit or None,
                page_token=None,
            )
        except GatewayError as error:
            context.abort(grpc.StatusCode.NOT_FOUND, error.message)
        return scheduling_pb2.ListDecisionsResponse(
            decisions=payload["decisions"],
            next_page_token=f"downstream-decisions:{request.job_id}:{request.run_id or 'all'}",
        )

    def GetDecision(
        self, request: scheduling_pb2.GetDecisionRequest, context: grpc.ServicerContext
    ) -> scheduling_pb2.GetDecisionResponse:
        for job in self.backend.list_jobs():
            payload = self.backend.list_decisions(
                job.job_id,
                run_id=None,
                limit=None,
                page_token=None,
            )
            for decision in cast(list[scheduling_pb2.DecisionRecord], payload["decisions"]):
                if decision.decision_id == request.decision_id:
                    return scheduling_pb2.GetDecisionResponse(decision=decision)
        context.abort(grpc.StatusCode.NOT_FOUND, "decision not found")


class _RuntimeServicer(runtime_pb2_grpc.RuntimeControlServiceServicer):
    def __init__(self, backend: InMemoryGatewayBackend) -> None:
        self.backend = backend
        self.last_list_sandboxes_request: runtime_pb2.ListSandboxesRequest | None = None

    def GetRuntimeManifest(
        self, request: runtime_pb2.GetRuntimeManifestRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.GetRuntimeManifestResponse:
        for job in self.backend.list_jobs():
            try:
                topology = self.backend.get_topology(job.job_id, run_id=request.run_id)
                return runtime_pb2.GetRuntimeManifestResponse(manifest=topology["manifest"])
            except GatewayError:
                continue
        context.abort(grpc.StatusCode.NOT_FOUND, "run not found")

    def ListSandboxes(
        self, request: runtime_pb2.ListSandboxesRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.ListSandboxesResponse:
        self.last_list_sandboxes_request = runtime_pb2.ListSandboxesRequest()
        self.last_list_sandboxes_request.CopyFrom(request)
        payload = self.backend.list_sandboxes(
            request.job_id,
            run_id=request.run_id or None,
            limit=request.limit or None,
            page_token=None,
        )
        return runtime_pb2.ListSandboxesResponse(
            sandboxes=payload["sandboxes"],
            next_page_token=f"downstream-sandboxes:{request.job_id}:{request.run_id or 'all'}",
        )

    def GetRuntimeStatus(
        self, request: runtime_pb2.GetRuntimeStatusRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.GetRuntimeStatusResponse:
        for job in self.backend.list_jobs():
            try:
                topology = self.backend.get_topology(job.job_id, run_id=request.run_id)
                return runtime_pb2.GetRuntimeStatusResponse(
                    manifest=topology["manifest"],
                    runtime_units=topology["runtime_units"],
                    sandboxes=topology["sandboxes"],
                )
            except GatewayError:
                continue
        context.abort(grpc.StatusCode.NOT_FOUND, "run not found")


class _ExperimentServicer(experiment_pb2_grpc.ExperimentServiceServicer):
    def __init__(self, backend: InMemoryGatewayBackend) -> None:
        self.backend = backend
        self.last_list_replays_request: experiment_pb2.ListReplaysRequest | None = None
        self.last_list_experiments_request: experiment_pb2.ListExperimentsRequest | None = None

    def ListReplays(
        self, request: experiment_pb2.ListReplaysRequest, context: grpc.ServicerContext
    ) -> experiment_pb2.ListReplaysResponse:
        self.last_list_replays_request = experiment_pb2.ListReplaysRequest()
        self.last_list_replays_request.CopyFrom(request)
        payload = self.backend.list_replays(
            limit=request.limit or None,
            page_token=None,
            after_replay_id=request.after_replay_id or None,
        )
        return experiment_pb2.ListReplaysResponse(
            replays=payload["replays"],
            next_page_token=f"downstream-replays:{request.page_token or 'start'}",
        )

    def CreateReplay(
        self, request: experiment_pb2.CreateReplayRequest, context: grpc.ServicerContext
    ) -> experiment_pb2.CreateReplayResponse:
        payload = self.backend.create_replay(
            request.replay,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return experiment_pb2.CreateReplayResponse(replay=payload["replay"])

    def GetReplay(
        self, request: experiment_pb2.GetReplayRequest, context: grpc.ServicerContext
    ) -> experiment_pb2.GetReplayResponse:
        return experiment_pb2.GetReplayResponse(replay=self.backend.get_replay(request.replay_id))

    def ApplyReplayCommand(
        self, request: experiment_pb2.ApplyReplayCommandRequest, context: grpc.ServicerContext
    ) -> experiment_pb2.ApplyReplayCommandResponse:
        payload = self.backend.apply_replay_command(
            request.replay_id,
            request.command,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return experiment_pb2.ApplyReplayCommandResponse(replay=payload["replay"])

    def ListExperiments(
        self, request: experiment_pb2.ListExperimentsRequest, context: grpc.ServicerContext
    ) -> experiment_pb2.ListExperimentsResponse:
        self.last_list_experiments_request = experiment_pb2.ListExperimentsRequest()
        self.last_list_experiments_request.CopyFrom(request)
        payload = self.backend.list_experiments(
            limit=request.limit or None,
            page_token=None,
            after_experiment_id=request.after_experiment_id or None,
        )
        return experiment_pb2.ListExperimentsResponse(
            experiments=payload["experiments"],
            next_page_token=f"downstream-experiments:{request.page_token or 'start'}",
        )

    def CreateExperiment(
        self, request: experiment_pb2.CreateExperimentRequest, context: grpc.ServicerContext
    ) -> experiment_pb2.CreateExperimentResponse:
        payload = self.backend.create_experiment(
            request.experiment,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
        )
        return experiment_pb2.CreateExperimentResponse(experiment=payload["experiment"])

    def GetExperiment(
        self, request: experiment_pb2.GetExperimentRequest, context: grpc.ServicerContext
    ) -> experiment_pb2.GetExperimentResponse:
        return experiment_pb2.GetExperimentResponse(
            experiment=self.backend.get_experiment(request.experiment_id)
        )


@contextmanager
def grpc_gateway_harness() -> Iterator[WsgiHarness]:
    backend = InMemoryGatewayBackend()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    job_control = _JobControlServicer(backend)
    control_pb2_grpc.add_JobControlServiceServicer_to_server(job_control, server)
    scheduling_pb2_grpc.add_SchedulerServiceServicer_to_server(_SchedulerServicer(backend), server)
    runtime_pb2_grpc.add_RuntimeControlServiceServicer_to_server(_RuntimeServicer(backend), server)
    experiment_pb2_grpc.add_ExperimentServiceServicer_to_server(
        _ExperimentServicer(backend), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        config = GatewayConfig(
            backend_mode="grpc",
            job_control_target=f"127.0.0.1:{port}",
            scheduler_target=f"127.0.0.1:{port}",
            runtime_target=f"127.0.0.1:{port}",
            experiment_target=f"127.0.0.1:{port}",
        )
        harness = WsgiHarness(create_app(config=config).__call__)
        harness.job_control_servicer = job_control
        yield harness
    finally:
        server.stop(None).wait()


def _real_controller_job_payload(*, suffix: str) -> JsonObject:
    return {
        "displayName": "trainer",
        "protocolVersion": "v0.3",
        "algorithm": "ppo",
        "runtime": {
            "framework": "fake",
            "frameworkVersion": "1.0.0",
            "executionBackend": "fake",
            "executionBackendVersion": "1.0.0",
            "trainer": "fake",
            "trainerVersion": "1.0.0",
            "rolloutEngine": "fake",
            "rolloutEngineVersion": "1.0.0",
            "imageDigest": (
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            ),
            "patchSet": ["patch-b", "patch-a"],
        },
        "executionContract": {
            "contractId": "contract-1",
            "version": "1.0.0",
            "phaseGraph": {
                "phases": [
                    {
                        "phaseId": "phase-train",
                        "displayName": "Train",
                        "kind": "PHASE_KIND_ACTOR",
                        "parallelism": 1,
                        "maxAttempts": 1,
                        "labels": {"stage": "train"},
                    }
                ],
                "entryPhaseIds": ["phase-train"],
            },
            "commitPolicy": {
                "mode": "COMMIT_MODE_ALL_OR_NOTHING",
                "minimumSuccessfulUnits": 1,
                "maxRetries": 1,
                "commitTimeout": "60s",
                "requireSafePoint": True,
            },
            "backpressurePolicy": {
                "mode": "BACKPRESSURE_MODE_BLOCK_PRODUCER",
                "lowWatermark": 1,
                "highWatermark": 2,
                "maximumBufferLevel": 4,
                "stallTimeout": "60s",
            },
            "safePointPolicy": {
                "enabled": True,
                "trigger": "SAFE_POINT_TRIGGER_EXPLICIT",
            },
        },
        "resourcesPerUnit": {
            "cpuMillis": 1000,
            "memoryBytes": 4294967296,
            "acceleratorUnits": 1,
        },
        "requiredCapabilities": {
            "names": ["gpu"],
            "algorithms": ["ppo"],
            "rolloutModes": ["ROLLOUT_MODE_SYNC"],
            "supportedActions": ["start", "pause", "resume", "stop", "retry", "terminate"],
        },
        "desiredUnits": 1,
        "priority": 1,
        "queue": "default",
        "labels": {"team": "rl", "testSuffix": suffix},
        "rolloutMode": "ROLLOUT_MODE_SYNC",
        "modelRef": f"model-{suffix}",
        "datasetRef": f"dataset-{suffix}",
        "policyRef": f"policy-{suffix}",
        "dataKind": "DATA_KIND_SYNTHETIC",
    }


class _MinimalRuntimeServicer(runtime_pb2_grpc.RuntimeControlServiceServicer):
    def __init__(self) -> None:
        self.manifests: dict[str, runtime_pb2.RuntimeManifest] = {}

    def _manifest_for_run(self, run_id: str) -> runtime_pb2.RuntimeManifest:
        manifest = self.manifests.get(run_id)
        if manifest is not None:
            return manifest
        manifest = runtime_pb2.RuntimeManifest(
            manifest_id=f"manifest-{run_id}",
            run_id=run_id,
            job_id="",
            trace_id=f"trace-{run_id}",
            data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
        )
        self.manifests[run_id] = manifest
        return manifest

    def ValidateRuntime(
        self, request: runtime_pb2.ValidateRuntimeRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.ValidateRuntimeResponse:
        manifest = runtime_pb2.RuntimeManifest()
        manifest.CopyFrom(request.manifest)
        return runtime_pb2.ValidateRuntimeResponse(
            valid=True,
            normalized_manifest=manifest,
            cursor="validate",
        )

    def CompileRuntime(
        self, request: runtime_pb2.CompileRuntimeRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.CompileRuntimeResponse:
        manifest = runtime_pb2.RuntimeManifest()
        manifest.CopyFrom(request.manifest)
        self.manifests[manifest.run_id] = manifest
        unit = runtime_pb2.RuntimeUnit(
            runtime_unit_id=f"unit-{manifest.run_id}",
            run_id=manifest.run_id,
            job_id=manifest.job_id,
            trace_id=manifest.trace_id,
            state=runtime_pb2.RUNTIME_STATE_BOUND,
            generation=1,
            data_kind=manifest.data_kind,
        )
        return runtime_pb2.CompileRuntimeResponse(
            manifest=manifest,
            runtime_units=[unit],
            cursor="compile",
        )

    def PrepareRuntime(
        self, request: runtime_pb2.PrepareRuntimeRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.PrepareRuntimeResponse:
        manifest = self._manifest_for_run(request.run_id)
        unit = runtime_pb2.RuntimeUnit(
            runtime_unit_id=f"unit-{request.run_id}",
            run_id=request.run_id,
            job_id=manifest.job_id,
            trace_id=manifest.trace_id,
            state=runtime_pb2.RUNTIME_STATE_BOUND,
            generation=1,
            data_kind=manifest.data_kind,
        )
        return runtime_pb2.PrepareRuntimeResponse(
            manifest=manifest,
            runtime_units=[unit],
            cursor="prepare",
        )

    def GetRuntimeManifest(
        self, request: runtime_pb2.GetRuntimeManifestRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.GetRuntimeManifestResponse:
        manifest = self._manifest_for_run(request.run_id)
        return runtime_pb2.GetRuntimeManifestResponse(manifest=manifest, cursor="manifest")

    def GetRuntimeStatus(
        self, request: runtime_pb2.GetRuntimeStatusRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.GetRuntimeStatusResponse:
        manifest = self._manifest_for_run(request.run_id)
        unit = runtime_pb2.RuntimeUnit(
            runtime_unit_id=f"unit-{request.run_id}",
            run_id=request.run_id,
            job_id=manifest.job_id,
            trace_id=manifest.trace_id,
            state=runtime_pb2.RUNTIME_STATE_BOUND,
            generation=1,
            data_kind=manifest.data_kind,
        )
        sandbox = runtime_pb2.Sandbox(
            sandbox_id=f"sbx-{request.run_id}",
            run_id=request.run_id,
            job_id=manifest.job_id,
            trace_id=manifest.trace_id,
            state=runtime_pb2.RUNTIME_STATE_BOUND,
            generation=1,
            data_kind=manifest.data_kind,
        )
        return runtime_pb2.GetRuntimeStatusResponse(
            manifest=manifest,
            runtime_units=[unit],
            sandboxes=[sandbox],
            cursor="status",
        )

    def ListSandboxes(
        self, request: runtime_pb2.ListSandboxesRequest, context: grpc.ServicerContext
    ) -> runtime_pb2.ListSandboxesResponse:
        manifest = self._manifest_for_run(request.run_id)
        sandbox = runtime_pb2.Sandbox(
            sandbox_id=f"sbx-{request.run_id}",
            run_id=request.run_id,
            job_id=manifest.job_id,
            trace_id=manifest.trace_id,
            state=runtime_pb2.RUNTIME_STATE_BOUND,
            generation=1,
            data_kind=manifest.data_kind,
        )
        return runtime_pb2.ListSandboxesResponse(
            sandboxes=[sandbox],
            next_page_token="",
            cursor="sandboxes",
        )


class _MinimalSchedulerServicer(scheduling_pb2_grpc.SchedulerServiceServicer):
    def ListDecisions(
        self, request: scheduling_pb2.ListDecisionsRequest, context: grpc.ServicerContext
    ) -> scheduling_pb2.ListDecisionsResponse:
        if not request.run_id:
            return scheduling_pb2.ListDecisionsResponse(
                decisions=[],
                next_page_token="",
                cursor="decisions",
            )
        decision = scheduling_pb2.DecisionRecord(
            decision_id=f"decision-{request.run_id}",
            run_id=request.run_id,
            job_id=request.job_id,
            trace_id=f"trace-{request.run_id}",
            sequence=1,
            cursor=f"decision-{request.run_id}",
        )
        return scheduling_pb2.ListDecisionsResponse(
            decisions=[decision],
            next_page_token="",
            cursor="decisions",
        )

    def GetDecision(
        self, request: scheduling_pb2.GetDecisionRequest, context: grpc.ServicerContext
    ) -> scheduling_pb2.GetDecisionResponse:
        context.abort(grpc.StatusCode.NOT_FOUND, "decision not found")


class _MinimalExperimentServicer(experiment_pb2_grpc.ExperimentServiceServicer):
    pass


@pytest.fixture(scope="session")
def job_controller_binary(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the real Job Controller once, outside the service readiness window."""
    binary = tmp_path_factory.mktemp("job-controller-bin") / "tgsrl-job-controller"
    completed = subprocess.run(
        [
            "go",
            "build",
            "-trimpath",
            "-o",
            str(binary),
            "./job-controller-go/cmd/job-controller",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        timeout=180,
    )
    if completed.returncode != 0:
        pytest.fail(
            f"failed to build the Job Controller test binary:\n{completed.stdout}{completed.stderr}"
        )
    return binary


@contextmanager
def real_job_controller_gateway_harness(
    tmp_path: Path, job_controller_binary: Path
) -> Iterator[WsgiHarness]:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{ROOT / 'gateway-python'}:{ROOT / 'gen/python'}"

    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    runtime_pb2_grpc.add_RuntimeControlServiceServicer_to_server(
        _MinimalRuntimeServicer(), grpc_server
    )
    scheduling_pb2_grpc.add_SchedulerServiceServicer_to_server(
        _MinimalSchedulerServicer(), grpc_server
    )
    experiment_pb2_grpc.add_ExperimentServiceServicer_to_server(
        _MinimalExperimentServicer(), grpc_server
    )
    grpc_port = grpc_server.add_insecure_port("127.0.0.1:0")
    grpc_server.start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listen_socket:
        listen_socket.bind(("127.0.0.1", 0))
        job_controller_port = listen_socket.getsockname()[1]

    state_dir = tmp_path / "job-controller-state"
    state_dir.mkdir()
    stdout_path = tmp_path / "job-controller.stdout.log"
    stderr_path = tmp_path / "job-controller.stderr.log"
    stdout_file = stdout_path.open("w", encoding="utf-8")
    stderr_file = stderr_path.open("w", encoding="utf-8")
    job_controller: subprocess.Popen[str] | None = None
    channel: grpc.Channel | None = None
    try:
        job_controller = subprocess.Popen(
            [
                str(job_controller_binary),
                "--listen",
                f"127.0.0.1:{job_controller_port}",
                "--runtime-target",
                f"127.0.0.1:{grpc_port}",
                "--state-dir",
                str(state_dir),
            ],
            cwd=ROOT,
            env=env,
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
        )
        channel = grpc.insecure_channel(f"127.0.0.1:{job_controller_port}")
        client = control_pb2_grpc.JobControlServiceStub(channel)
        deadline = time.monotonic() + 30.0
        last_error: grpc.RpcError | None = None
        while True:
            return_code = job_controller.poll()
            if return_code is not None:
                stdout_file.flush()
                stderr_file.flush()
                raise RuntimeError(
                    f"Job Controller exited before readiness with code {return_code}:\n"
                    f"{stdout_path.read_text(encoding='utf-8')}"
                    f"{stderr_path.read_text(encoding='utf-8')}"
                )
            try:
                client.ListJobs(control_pb2.ListJobsRequest(limit=1), timeout=0.5)
                break
            except grpc.RpcError as error:
                last_error = error
                if time.monotonic() >= deadline:
                    stdout_file.flush()
                    stderr_file.flush()
                    raise TimeoutError(
                        "Job Controller did not become ready within 30 seconds:\n"
                        f"last gRPC status: {last_error.code().name}: "
                        f"{last_error.details()}\n"
                        f"{stdout_path.read_text(encoding='utf-8')}"
                        f"{stderr_path.read_text(encoding='utf-8')}"
                    ) from error
                time.sleep(0.1)

        config = GatewayConfig(
            backend_mode="grpc",
            job_control_target=f"127.0.0.1:{job_controller_port}",
            scheduler_target=f"127.0.0.1:{grpc_port}",
            runtime_target=f"127.0.0.1:{grpc_port}",
            experiment_target=f"127.0.0.1:{grpc_port}",
        )
        yield WsgiHarness(create_app(config=config).__call__)
    finally:
        if channel is not None:
            channel.close()
        if job_controller is not None:
            job_controller.terminate()
            try:
                job_controller.wait(timeout=10)
            except subprocess.TimeoutExpired:
                job_controller.kill()
                job_controller.wait(timeout=10)
        stdout_file.close()
        stderr_file.close()
        grpc_server.stop(None).wait()


def test_openapi_artifact_is_present_and_declares_required_routes() -> None:
    spec = json.loads((ROOT / "api/openapi.json").read_text(encoding="utf-8"))
    assert spec["openapi"] == "3.1.0"
    for route in (
        "/health",
        "/v1/capabilities",
        "/v1/jobs",
        "/v1/operations",
        "/v1/replays",
        "/v1/experiments",
        "/v1/jobs/{job_id}/timeline",
        "/v1/jobs/{job_id}/topology",
        "/v1/jobs/{job_id}/decisions/{decision_id}",
    ):
        assert route in spec["paths"]


def test_gateway_config_defaults_runtime_and_experiment_to_same_local_target() -> None:
    config = GatewayConfig()
    assert config.runtime_target == "127.0.0.1:50071"
    assert config.experiment_target == "127.0.0.1:50071"


def test_contract_registry_stays_aligned_with_openapi_sdk_and_cli() -> None:
    from tgsrl_gateway.cli import build_parser

    spec = json.loads((ROOT / "api/openapi.json").read_text(encoding="utf-8"))
    parser = build_parser()

    registry_paths = set(all_route_paths())
    assert registry_paths.issubset(set(spec["paths"]))

    sdk_public_methods = {
        name
        for name, value in GatewayClient.__dict__.items()
        if callable(value) and not name.startswith("_")
    }
    assert set(sdk_method_names()).issubset(sdk_public_methods)

    subparsers = next(
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    )
    cli_commands = set(subparsers.choices)
    assert set(cli_command_names()).issubset(cli_commands)

    for route in ROUTE_CONTRACTS:
        operation = spec["paths"][route.path_template][route.method.lower()]
        assert operation["operationId"] == route.operation_id
        assert operation["summary"] == route.summary
        assert operation["responses"]["default"]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/ErrorEnvelope"
        }
        if route.parameters:
            assert {item["name"] for item in operation["parameters"]} == {
                parameter.name for parameter in route.parameters
            }
        else:
            assert "parameters" not in operation
        explicit_error_statuses = {status for status in operation["responses"] if status.isdigit()}
        assert explicit_error_statuses == {
            str(route.success_status),
            *(str(status) for status in route.error_statuses),
        }


def test_gateway_routes_cover_jobs_runtime_decisions_and_operations() -> None:
    harness = WsgiHarness()

    status, health = harness.request("GET", "/health")
    assert status == 200
    assert health["status"] == "ok"
    assert health["mode"] == "memory"

    validate_status, validation = harness.request(
        "POST",
        "/v1/jobs/validate",
        body={"displayName": "Validated Job"},
        headers={"Idempotency-Key": "validate-1", "X-Request-Id": "req-validate"},
    )
    assert validate_status == 200
    assert validation["valid"] is True
    assert validation["normalized_job"]["protocolVersion"] == "v0.3"

    create_status, created = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Demo Job"},
        headers={"Idempotency-Key": "job-create-1", "X-Request-Id": "req-create"},
    )
    assert create_status == 201
    job_id = created["job"]["jobId"]

    duplicate_status, duplicate = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Demo Job"},
        headers={"Idempotency-Key": "job-create-1", "X-Request-Id": "req-create-dup"},
    )
    assert duplicate_status == 201
    assert duplicate["job"]["jobId"] == job_id

    get_job_status, fetched = harness.request("GET", f"/v1/jobs/{job_id}")
    assert get_job_status == 200
    assert fetched["job"]["jobId"] == job_id

    admit_status, admitted = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/admit",
        body={"reason": "approved"},
        headers={"Idempotency-Key": "admit-1", "X-Request-Id": "req-admit"},
    )
    assert admit_status == 200
    assert admitted["job"]["jobId"] == job_id

    create_run_status, created_run = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs",
        headers={"Idempotency-Key": "run-create-1", "X-Request-Id": "req-run-create"},
    )
    assert create_run_status == 201
    run_id = created_run["run"]["runId"]

    list_runs_status, runs = harness.request("GET", f"/v1/jobs/{job_id}/runs?limit=10")
    assert list_runs_status == 200
    assert any(run["runId"] == run_id for run in runs["runs"])

    start_status, started = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs/{run_id}/commands/start",
        body={"actor": "tester", "reason": "kickoff"},
        headers={"Idempotency-Key": "cmd-start-1", "X-Request-Id": "req-start"},
    )
    assert start_status == 200
    assert started["run"]["runState"] == "JOB_RUN_STATE_RUNNING"

    pause_status, paused = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs/{run_id}/commands/pause",
        body={"actor": "tester", "reason": "checkpoint"},
        headers={"Idempotency-Key": "cmd-pause-1", "X-Request-Id": "req-pause"},
    )
    assert pause_status == 200
    assert paused["run"]["runState"] == "JOB_RUN_STATE_PAUSED"

    resume_status, resumed = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs/{run_id}/commands/resume",
        body={"actor": "tester", "reason": "continue"},
        headers={"Idempotency-Key": "cmd-resume-1", "X-Request-Id": "req-resume"},
    )
    assert resume_status == 200
    assert resumed["run"]["runState"] == "JOB_RUN_STATE_RUNNING"

    timeline_status, timeline = harness.request(
        "GET", f"/v1/jobs/{job_id}/timeline?run_id={run_id}"
    )
    assert timeline_status == 200
    assert len(timeline["events"]) >= 3

    dag_status, dag = harness.request("GET", f"/v1/jobs/{job_id}/dag?run_id={run_id}")
    assert dag_status == 200
    assert dag["phase_graph"]["entryPhaseIds"] == ["prefill"]
    assert dag["dependencies"]["decode"] == ["prefill"]

    topology_status, topology = harness.request(
        "GET", f"/v1/jobs/{job_id}/topology?run_id={run_id}"
    )
    assert topology_status == 200
    assert topology["manifest"]["runId"] == run_id
    assert len(topology["runtime_units"]) == 2
    assert len(topology["sandboxes"]) == 2
    assert topology["decision"]["runId"] == run_id

    sandboxes_status, sandboxes = harness.request(
        "GET", f"/v1/jobs/{job_id}/sandboxes?run_id={run_id}"
    )
    assert sandboxes_status == 200
    assert len(sandboxes["sandboxes"]) == 2
    assert sandboxes["next_page_token"] == ""

    decisions_status, decisions = harness.request(
        "GET", f"/v1/jobs/{job_id}/decisions?run_id={run_id}"
    )
    assert decisions_status == 200
    assert len(decisions["decisions"]) == 1
    decision_id = decisions["decisions"][0]["decisionId"]

    decision_status, decision = harness.request("GET", f"/v1/jobs/{job_id}/decisions/{decision_id}")
    assert decision_status == 200
    assert decision["decision"]["decisionId"] == decision_id

    operations_status, operations = harness.request("GET", "/v1/operations?limit=20")
    assert operations_status == 200
    assert len(operations["operations"]) >= 4
    operation_id = operations["operations"][0]["operationId"]

    operation_status, operation = harness.request("GET", f"/v1/operations/{operation_id}")
    assert operation_status == 200
    assert operation["operation"]["operationId"] == operation_id


def test_data_kind_http_aliases_and_invalid_values_in_memory_gateway() -> None:
    harness = WsgiHarness()

    synthetic_status, synthetic_job = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Synthetic Job"},
        headers={"Idempotency-Key": "data-kind-job-1", "X-Request-Id": "data-kind-job-1"},
    )
    assert synthetic_status == 201
    synthetic_job_id = synthetic_job["job"]["jobId"]

    live_status, live_job = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Live Job", "dataKind": "DATA_KIND_LIVE"},
        headers={"Idempotency-Key": "data-kind-job-2", "X-Request-Id": "data-kind-job-2"},
    )
    assert live_status == 201
    live_job_id = live_job["job"]["jobId"]

    short_name_status, short_name_jobs = harness.request("GET", "/v1/jobs?data_kind=live")
    assert short_name_status == 200
    assert [job["jobId"] for job in short_name_jobs["jobs"]] == [live_job_id]

    full_name_status, full_name_jobs = harness.request(
        "GET", "/v1/jobs?data_kind=DATA_KIND_SYNTHETIC"
    )
    assert full_name_status == 200
    assert synthetic_job_id in [job["jobId"] for job in full_name_jobs["jobs"]]
    assert live_job_id not in [job["jobId"] for job in full_name_jobs["jobs"]]

    invalid_status, invalid_payload = harness.request("GET", "/v1/jobs?data_kind=invalid-kind")
    assert invalid_status == 400
    assert invalid_payload["error"]["code"] == "bad_request"
    assert invalid_payload["error"]["message"] == "unsupported data_kind"


def test_latest_run_semantics_match_list_runs_and_topology_in_memory_gateway() -> None:
    harness = WsgiHarness()

    create_status, created = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Latest Run Job"},
        headers={"Idempotency-Key": "latest-job-1", "X-Request-Id": "latest-job-1"},
    )
    assert create_status == 201
    job_id = created["job"]["jobId"]

    first_run_status, first_run = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs",
        headers={"Idempotency-Key": "latest-run-1", "X-Request-Id": "latest-run-1"},
    )
    assert first_run_status == 201
    first_run_id = first_run["run"]["runId"]

    second_run_status, second_run = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs",
        headers={"Idempotency-Key": "latest-run-2", "X-Request-Id": "latest-run-2"},
    )
    assert second_run_status == 201
    second_run_id = second_run["run"]["runId"]

    list_runs_status, list_runs = harness.request("GET", f"/v1/jobs/{job_id}/runs?limit=1")
    assert list_runs_status == 200
    assert len(list_runs["runs"]) == 1
    assert list_runs["runs"][0]["runId"] == second_run_id
    assert list_runs["runs"][0]["runId"] != first_run_id

    topology_status, topology = harness.request("GET", f"/v1/jobs/{job_id}/topology")
    assert topology_status == 200
    assert topology["run"]["runId"] == second_run_id
    assert topology["manifest"]["runId"] == second_run_id


def test_unknown_path_is_404_and_method_mismatch_is_405_with_allow_in_memory_gateway() -> None:
    harness = WsgiHarness()

    missing_status, missing_payload = harness.request("GET", "/v1/does-not-exist")
    assert missing_status == 404
    assert missing_payload["error"]["code"] == "not_found"
    headers = dict(harness.last_response_headers)
    assert "Allow" not in headers

    method_status, method_payload = harness.request("POST", "/v1/capabilities")
    assert method_status == 405
    assert method_payload["error"]["code"] == "method_not_allowed"
    headers = dict(harness.last_response_headers)
    assert headers["Allow"] == "GET"


def test_timeline_page_token_and_after_event_id_are_mutually_exclusive() -> None:
    harness = WsgiHarness()

    create_status, created = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Timeline Contract Job"},
        headers={"Idempotency-Key": "timeline-job-1", "X-Request-Id": "timeline-create"},
    )
    assert create_status == 201
    job_id = created["job"]["jobId"]

    run_status, run_payload = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs",
        headers={"Idempotency-Key": "timeline-run-1", "X-Request-Id": "timeline-run-create"},
    )
    assert run_status == 201
    run_id = run_payload["run"]["runId"]

    for index, command in enumerate(("start", "pause", "resume"), start=1):
        status, _ = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs/{run_id}/commands/{command}",
            body={"actor": "tester", "reason": command},
            headers={
                "Idempotency-Key": f"timeline-cmd-{index}",
                "X-Request-Id": f"timeline-cmd-{index}",
            },
        )
        assert status == 200

    timeline_status, timeline = harness.request(
        "GET",
        f"/v1/jobs/{job_id}/timeline?run_id={run_id}&limit=1",
    )
    assert timeline_status == 200
    assert len(timeline["events"]) == 1
    assert timeline["next_page_token"]
    event_id = timeline["events"][0]["eventId"]

    conflict_status, conflict = harness.request(
        "GET",
        f"/v1/jobs/{job_id}/timeline?run_id={run_id}&after_event_id={event_id}"
        f"&page_token={timeline['next_page_token']}",
    )
    assert conflict_status == 400
    assert conflict["error"]["message"] == "after_event_id and page_token are mutually exclusive"


def test_list_routes_after_cursor_and_page_token_are_mutually_exclusive() -> None:
    harness = WsgiHarness()

    create_job_status, created_job = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Cursor Job"},
        headers={"Idempotency-Key": "cursor-job-1", "X-Request-Id": "cursor-job-1"},
    )
    assert create_job_status == 201
    job_id = created_job["job"]["jobId"]

    create_run_status, created_run = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs",
        headers={"Idempotency-Key": "cursor-run-1", "X-Request-Id": "cursor-run-1"},
    )
    assert create_run_status == 201
    run_id = created_run["run"]["runId"]
    second_run_status, _ = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs",
        headers={"Idempotency-Key": "cursor-run-2", "X-Request-Id": "cursor-run-2"},
    )
    assert second_run_status == 201

    create_replay_status, created_replay = harness.request(
        "POST",
        "/v1/replays",
        body={"runId": run_id, "traceId": "trace-cursor", "sourceTraceRef": "trace-ref-cursor"},
        headers={"Idempotency-Key": "cursor-replay-1", "X-Request-Id": "cursor-replay-1"},
    )
    assert create_replay_status == 201
    replay_id = created_replay["replay"]["replayId"]

    create_experiment_status, created_experiment = harness.request(
        "POST",
        "/v1/experiments",
        body={"displayName": "Cursor Experiment", "summary": "cursor"},
        headers={"Idempotency-Key": "cursor-experiment-1", "X-Request-Id": "cursor-experiment-1"},
    )
    assert create_experiment_status == 201
    experiment_id = created_experiment["experiment"]["experimentId"]

    jobs_status, jobs = harness.request("GET", "/v1/jobs?limit=1")
    assert jobs_status == 200
    assert jobs["next_page_token"]
    conflict_status, conflict = harness.request(
        "GET",
        f"/v1/jobs?page_token={jobs['next_page_token']}&after_job_id={job_id}",
    )
    assert conflict_status == 400
    assert conflict["error"]["message"] == "after_job_id and page_token are mutually exclusive"

    runs_status, runs = harness.request("GET", f"/v1/jobs/{job_id}/runs?limit=1")
    assert runs_status == 200
    assert runs["next_page_token"]
    conflict_status, conflict = harness.request(
        "GET",
        f"/v1/jobs/{job_id}/runs?page_token={runs['next_page_token']}&after_run_id={run_id}",
    )
    assert conflict_status == 400
    assert conflict["error"]["message"] == "after_run_id and page_token are mutually exclusive"

    replays_status, replays = harness.request("GET", "/v1/replays?limit=1")
    assert replays_status == 200
    conflict_status, conflict = harness.request(
        "GET",
        f"/v1/replays?page_token={replays['next_page_token']}&after_replay_id={replay_id}",
    )
    assert conflict_status == 400
    assert conflict["error"]["message"] == "after_replay_id and page_token are mutually exclusive"

    experiments_status, experiments = harness.request("GET", "/v1/experiments?limit=1")
    assert experiments_status == 200
    conflict_status, conflict = harness.request(
        "GET",
        f"/v1/experiments?page_token={experiments['next_page_token']}"
        f"&after_experiment_id={experiment_id}",
    )
    assert conflict_status == 400
    assert conflict["error"]["message"] == (
        "after_experiment_id and page_token are mutually exclusive"
    )


def test_timeline_and_sandbox_pagination_routes() -> None:
    harness = WsgiHarness()

    create_status, created = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Pagination Job"},
        headers={"Idempotency-Key": "pagination-job-1", "X-Request-Id": "pagination-create"},
    )
    assert create_status == 201
    job_id = created["job"]["jobId"]

    run_status, run_payload = harness.request(
        "POST",
        f"/v1/jobs/{job_id}/runs",
        headers={"Idempotency-Key": "pagination-run-1", "X-Request-Id": "pagination-run-create"},
    )
    assert run_status == 201
    run_id = run_payload["run"]["runId"]

    for index, command in enumerate(("start", "pause", "resume"), start=1):
        status, _ = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs/{run_id}/commands/{command}",
            body={"actor": "tester", "reason": command},
            headers={
                "Idempotency-Key": f"pagination-cmd-{index}",
                "X-Request-Id": f"pagination-cmd-{index}",
            },
        )
        assert status == 200

    first_timeline_status, first_timeline = harness.request(
        "GET",
        f"/v1/jobs/{job_id}/timeline?run_id={run_id}&limit=1",
    )
    assert first_timeline_status == 200
    assert len(first_timeline["events"]) == 1
    assert first_timeline["next_page_token"]

    second_timeline_status, second_timeline = harness.request(
        "GET",
        f"/v1/jobs/{job_id}/timeline?run_id={run_id}&limit=1"
        f"&page_token={first_timeline['next_page_token']}",
    )
    assert second_timeline_status == 200
    assert len(second_timeline["events"]) == 1
    assert second_timeline["events"][0]["eventId"] != first_timeline["events"][0]["eventId"]

    first_sandboxes_status, first_sandboxes = harness.request(
        "GET",
        f"/v1/jobs/{job_id}/sandboxes?run_id={run_id}&limit=1",
    )
    assert first_sandboxes_status == 200
    assert len(first_sandboxes["sandboxes"]) == 1
    assert first_sandboxes["next_page_token"]

    second_sandboxes_status, second_sandboxes = harness.request(
        "GET",
        f"/v1/jobs/{job_id}/sandboxes?run_id={run_id}&limit=1"
        f"&page_token={first_sandboxes['next_page_token']}",
    )
    assert second_sandboxes_status == 200
    assert len(second_sandboxes["sandboxes"]) == 1
    assert (
        second_sandboxes["sandboxes"][0]["sandboxId"]
        != first_sandboxes["sandboxes"][0]["sandboxId"]
    )


def test_after_cursor_routes_for_jobs_runs_replays_and_experiments() -> None:
    harness = WsgiHarness()

    first_job_status, first_job = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "After Cursor Job One"},
        headers={"Idempotency-Key": "after-job-1", "X-Request-Id": "after-job-1"},
    )
    assert first_job_status == 201
    first_job_id = first_job["job"]["jobId"]

    second_job_status, second_job = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "After Cursor Job Two"},
        headers={"Idempotency-Key": "after-job-2", "X-Request-Id": "after-job-2"},
    )
    assert second_job_status == 201
    second_job_id = second_job["job"]["jobId"]

    first_run_status, first_run = harness.request(
        "POST",
        f"/v1/jobs/{first_job_id}/runs",
        headers={"Idempotency-Key": "after-run-1", "X-Request-Id": "after-run-1"},
    )
    assert first_run_status == 201
    first_run_id = first_run["run"]["runId"]

    second_run_status, second_run = harness.request(
        "POST",
        f"/v1/jobs/{first_job_id}/runs",
        headers={"Idempotency-Key": "after-run-2", "X-Request-Id": "after-run-2"},
    )
    assert second_run_status == 201
    second_run_id = second_run["run"]["runId"]

    first_replay_status, first_replay = harness.request(
        "POST",
        "/v1/replays",
        body={"runId": first_run_id, "traceId": "trace-after-1", "sourceTraceRef": "trace-ref-1"},
        headers={"Idempotency-Key": "after-replay-1", "X-Request-Id": "after-replay-1"},
    )
    assert first_replay_status == 201
    first_replay_id = first_replay["replay"]["replayId"]

    second_replay_status, second_replay = harness.request(
        "POST",
        "/v1/replays",
        body={"runId": second_run_id, "traceId": "trace-after-2", "sourceTraceRef": "trace-ref-2"},
        headers={"Idempotency-Key": "after-replay-2", "X-Request-Id": "after-replay-2"},
    )
    assert second_replay_status == 201
    second_replay_id = second_replay["replay"]["replayId"]

    first_experiment_status, first_experiment = harness.request(
        "POST",
        "/v1/experiments",
        body={"displayName": "After Experiment One", "summary": "first"},
        headers={"Idempotency-Key": "after-experiment-1", "X-Request-Id": "after-experiment-1"},
    )
    assert first_experiment_status == 201
    first_experiment_id = first_experiment["experiment"]["experimentId"]

    second_experiment_status, second_experiment = harness.request(
        "POST",
        "/v1/experiments",
        body={"displayName": "After Experiment Two", "summary": "second"},
        headers={"Idempotency-Key": "after-experiment-2", "X-Request-Id": "after-experiment-2"},
    )
    assert second_experiment_status == 201
    second_experiment_id = second_experiment["experiment"]["experimentId"]

    jobs_status, jobs = harness.request("GET", f"/v1/jobs?after_job_id={first_job_id}&limit=1")
    assert jobs_status == 200
    assert len(jobs["jobs"]) == 1
    assert jobs["jobs"][0]["jobId"] == second_job_id

    runs_status, runs = harness.request(
        "GET",
        f"/v1/jobs/{first_job_id}/runs?after_run_id={second_run_id}&limit=1",
    )
    assert runs_status == 200
    assert len(runs["runs"]) == 1
    assert runs["runs"][0]["runId"] == first_run_id

    replays_status, replays = harness.request(
        "GET",
        f"/v1/replays?after_replay_id={first_replay_id}&limit=1",
    )
    assert replays_status == 200
    assert len(replays["replays"]) == 1
    assert replays["replays"][0]["replayId"] == second_replay_id

    experiments_status, experiments = harness.request(
        "GET",
        f"/v1/experiments?after_experiment_id={first_experiment_id}&limit=1",
    )
    assert experiments_status == 200
    assert len(experiments["experiments"]) == 1
    assert experiments["experiments"][0]["experimentId"] == second_experiment_id


def test_pagination_rejects_invalid_cross_route_and_cross_filter_tokens() -> None:
    harness = WsgiHarness()

    job_status, job = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Token Job"},
        headers={"Idempotency-Key": "token-job-1", "X-Request-Id": "token-job-1"},
    )
    assert job_status == 201
    job_id = job["job"]["jobId"]

    for index in range(2):
        run_status, created_run = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={
                "Idempotency-Key": f"token-run-{index}",
                "X-Request-Id": f"token-run-{index}",
            },
        )
        assert run_status == 201
        if index == 0:
            run_id = created_run["run"]["runId"]

    replay_status, _ = harness.request(
        "POST",
        "/v1/replays",
        body={"runId": run_id, "traceId": "trace-token", "sourceTraceRef": "trace-ref-token"},
        headers={"Idempotency-Key": "token-replay-1", "X-Request-Id": "token-replay-1"},
    )
    assert replay_status == 201

    malformed_status, malformed = harness.request("GET", "/v1/jobs?page_token=not-a-token")
    assert malformed_status == 400
    assert malformed["error"]["message"] == "invalid page_token"

    jobs_status, jobs = harness.request("GET", "/v1/jobs?limit=1")
    assert jobs_status == 200
    assert jobs["next_page_token"]

    cross_route_status, cross_route = harness.request(
        "GET",
        f"/v1/replays?limit=1&page_token={jobs['next_page_token']}",
    )
    assert cross_route_status == 400
    assert cross_route["error"]["message"] == "invalid page_token"

    synthetic_job_status, _ = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Synthetic Token Job", "dataKind": "DATA_KIND_SYNTHETIC"},
        headers={"Idempotency-Key": "token-filter-job-1", "X-Request-Id": "token-filter-job-1"},
    )
    assert synthetic_job_status == 201

    second_synthetic_job_status, _ = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Synthetic Token Job Two", "dataKind": "DATA_KIND_SYNTHETIC"},
        headers={"Idempotency-Key": "token-filter-job-2", "X-Request-Id": "token-filter-job-2"},
    )
    assert second_synthetic_job_status == 201

    live_job_status, _ = harness.request(
        "POST",
        "/v1/jobs",
        body={"displayName": "Live Token Job", "dataKind": "DATA_KIND_LIVE"},
        headers={"Idempotency-Key": "token-filter-job-3", "X-Request-Id": "token-filter-job-3"},
    )
    assert live_job_status == 201

    filtered_status, filtered = harness.request("GET", "/v1/jobs?data_kind=synthetic&limit=1")
    assert filtered_status == 200
    assert filtered["next_page_token"]

    cross_filter_status, cross_filter = harness.request(
        "GET",
        f"/v1/jobs?data_kind=live&page_token={filtered['next_page_token']}",
    )
    assert cross_filter_status == 400
    assert cross_filter["error"]["message"] == "invalid page_token"


def test_pagination_rejects_non_object_tokens_and_non_positive_limits() -> None:
    harness = WsgiHarness()
    non_object_token = base64.urlsafe_b64encode(b"[]").decode("ascii")

    status, payload = harness.request("GET", f"/v1/jobs?page_token={non_object_token}")
    assert status == 400
    assert payload["error"]["code"] == "bad_request"
    assert payload["error"]["message"] == "invalid page_token"

    boolean_offset_token = base64.urlsafe_b64encode(
        json.dumps({"version": 1, "offset": True, "scope": "jobs", "filters": {}}).encode("utf-8")
    ).decode("ascii")
    status, payload = harness.request("GET", f"/v1/jobs?page_token={boolean_offset_token}")
    assert status == 400
    assert payload["error"]["message"] == "invalid page_token"

    list_paths = [
        "/v1/jobs",
        "/v1/jobs/missing/runs",
        "/v1/jobs/missing/timeline",
        "/v1/jobs/missing/sandboxes",
        "/v1/jobs/missing/decisions",
        "/v1/operations",
        "/v1/replays",
        "/v1/experiments",
    ]
    for path in list_paths:
        for invalid_limit in (0, -1):
            status, payload = harness.request("GET", f"{path}?limit={invalid_limit}")
            assert status == 400, (path, invalid_limit, payload)
            assert payload["error"]["code"] == "bad_request"


def test_after_cursor_then_page_token_continues_within_filtered_job_scope() -> None:
    harness = WsgiHarness()

    job_specs = (
        ("After Scope Synthetic One", "DATA_KIND_SYNTHETIC"),
        ("After Scope Live One", "DATA_KIND_LIVE"),
        ("After Scope Synthetic Two", "DATA_KIND_SYNTHETIC"),
        ("After Scope Synthetic Three", "DATA_KIND_SYNTHETIC"),
        ("After Scope Live Two", "DATA_KIND_LIVE"),
        ("After Scope Synthetic Four", "DATA_KIND_SYNTHETIC"),
    )

    for index, (display_name, data_kind) in enumerate(job_specs, start=1):
        create_status, _created = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": display_name, "dataKind": data_kind},
            headers={
                "Idempotency-Key": f"after-scope-job-{index}",
                "X-Request-Id": f"after-scope-job-{index}",
            },
        )
        assert create_status == 201

    filtered_status, filtered = harness.request("GET", "/v1/jobs?data_kind=synthetic&limit=10")
    assert filtered_status == 200
    filtered_job_ids = [job["jobId"] for job in filtered["jobs"]]
    assert len(filtered_job_ids) >= 5

    anchor_job_id = filtered_job_ids[2]
    expected_tail = filtered_job_ids[3:]

    first_page_status, first_page = harness.request(
        "GET",
        f"/v1/jobs?data_kind=synthetic&after_job_id={anchor_job_id}&limit=1",
    )
    assert first_page_status == 200
    assert [job["jobId"] for job in first_page["jobs"]] == expected_tail[:1]
    assert first_page["next_page_token"]

    second_page_status, second_page = harness.request(
        "GET",
        f"/v1/jobs?data_kind=synthetic&limit=1&page_token={first_page['next_page_token']}",
    )
    assert second_page_status == 200
    assert [job["jobId"] for job in second_page["jobs"]] == expected_tail[1:2]

    combined_job_ids = [job["jobId"] for job in first_page["jobs"]] + [
        job["jobId"] for job in second_page["jobs"]
    ]
    assert combined_job_ids == expected_tail[:2]


def test_memory_latest_run_order_uses_attempt_then_run_id_not_created_at() -> None:
    backend = InMemoryGatewayBackend()
    job = backend.job_state.get_job("job-seeded")

    first = backend.job_service.create_run(job, idempotency_key="attempt-sort-1")
    second = backend.job_service.create_run(job, idempotency_key="attempt-sort-2")
    third = backend.job_service.create_run(job, idempotency_key="attempt-sort-3")

    first.attempt = 3
    second.attempt = 3
    third.attempt = 2

    first.created_at.seconds = 300
    second.created_at.seconds = 100
    third.created_at.seconds = 500

    listed = backend.list_runs(job.job_id, limit=10, page_token=None, after_run_id=None)
    run_ids = [run.run_id for run in cast(list[control_pb2.JobRun], listed["runs"])]

    assert run_ids[:3] == [*sorted([first.run_id, second.run_id], reverse=True), third.run_id]


def test_replay_and_experiment_lifecycle_routes() -> None:
    harness = WsgiHarness()

    create_replay_status, created_replay = harness.request(
        "POST",
        "/v1/replays",
        body={"runId": "run-demo", "traceId": "trace-demo", "sourceTraceRef": "trace-ref-demo"},
        headers={"Idempotency-Key": "replay-create-1", "X-Request-Id": "req-replay-create"},
    )
    assert create_replay_status == 201
    replay_id = created_replay["replay"]["replayId"]
    assert created_replay["replay"]["state"] == "REPLAY_STATE_PENDING"

    start_status, replay_started = harness.request(
        "POST",
        f"/v1/replays/{replay_id}/commands/start",
        headers={"Idempotency-Key": "replay-start-1", "X-Request-Id": "req-replay-start"},
    )
    assert start_status == 200
    assert replay_started["replay"]["state"] == "REPLAY_STATE_RUNNING"

    replay_status, replay_fetched = harness.request("GET", f"/v1/replays/{replay_id}")
    assert replay_status == 200
    assert replay_fetched["replay"]["replayId"] == replay_id

    list_replays_status, replays = harness.request("GET", "/v1/replays?limit=10")
    assert list_replays_status == 200
    assert any(item["replayId"] == replay_id for item in replays["replays"])

    create_experiment_status, created_experiment = harness.request(
        "POST",
        "/v1/experiments",
        body={"displayName": "Experiment Demo", "summary": "comparison"},
        headers={"Idempotency-Key": "experiment-create-1", "X-Request-Id": "req-experiment-create"},
    )
    assert create_experiment_status == 201
    experiment_id = created_experiment["experiment"]["experimentId"]
    assert created_experiment["experiment"]["state"] == "EXPERIMENT_STATE_PENDING"

    experiment_status, experiment = harness.request("GET", f"/v1/experiments/{experiment_id}")
    assert experiment_status == 200
    assert experiment["experiment"]["experimentId"] == experiment_id

    list_experiments_status, experiments = harness.request("GET", "/v1/experiments?limit=10")
    assert list_experiments_status == 200
    assert any(item["experimentId"] == experiment_id for item in experiments["experiments"])


def test_sdk_against_wsgi_harness_backend() -> None:
    harness = WsgiHarness()

    class LocalClient(GatewayClient):
        def __init__(self) -> None:
            super().__init__("http://local.invalid")

        def _request(self, method: str, path: str, **kwargs: object) -> dict[str, object]:
            query = cast(Mapping[str, object] | None, kwargs.get("query"))
            full_path = path
            if query:
                parts = [
                    f"{key}={value}" for key, value in query.items() if value not in (None, "")
                ]
                if parts:
                    full_path = f"{path}?{'&'.join(parts)}"
            headers: dict[str, str] = {}
            if kwargs.get("idempotency_key"):
                headers["Idempotency-Key"] = str(kwargs["idempotency_key"])
            if kwargs.get("request_id"):
                headers["X-Request-Id"] = str(kwargs["request_id"])
            status, payload = harness.request(
                method,
                full_path,
                body=cast(JsonObject | None, kwargs.get("json_body")),
                headers=headers,
            )
            if status >= 400:
                raise HTTPError(full_path, status, "error", Message(), None)
            return payload

    client = LocalClient()
    created = client.create_job(
        {"displayName": "SDK Job"},
        idempotency_key="sdk-job-1",
        request_id="sdk-req-1",
    )
    created_job = cast(JsonObject, created["job"])
    job_id = cast(str, created_job["jobId"])
    run = cast(
        JsonObject,
        client.create_run(job_id, idempotency_key="sdk-run-1", request_id="sdk-run-req")["run"],
    )
    run_id = cast(str, run["runId"])
    client.apply_job_command(
        job_id,
        run_id,
        "start",
        actor="sdk",
        idempotency_key="sdk-cmd-1",
        request_id="sdk-cmd-1",
    )
    client.apply_job_command(
        job_id,
        run_id,
        "pause",
        actor="sdk",
        idempotency_key="sdk-cmd-2",
        request_id="sdk-cmd-2",
    )
    topology = client.get_topology(job_id, run_id=run_id)
    decisions = client.list_decisions(job_id, run_id=run_id)
    timeline = client.list_timeline(job_id, run_id=run_id, limit=1)
    sandboxes = client.list_sandboxes(job_id, run_id=run_id, limit=1)
    assert cast(JsonObject, topology["manifest"])["runId"] == run_id
    assert len(cast(list[object], decisions["decisions"])) == 1
    assert len(cast(list[object], timeline["events"])) >= 1
    assert len(cast(list[object], sandboxes["sandboxes"])) == 1
    assert cast(str, timeline["next_page_token"])
    assert cast(str, sandboxes["next_page_token"])


def test_grpc_default_backend_routes_reach_in_process_servicers() -> None:
    with grpc_gateway_harness() as harness:
        health_status, health = harness.request("GET", "/health")
        assert health_status == 200
        assert health["backend"] == "grpc"
        assert health["status"] == "ok"
        assert all(
            dependency["detail"].startswith("grpc_") for dependency in health["dependencies"]
        )

        capabilities_status, capabilities = harness.request("GET", "/v1/capabilities")
        assert capabilities_status == 200
        assert capabilities["backend_mode"] == "grpc"
        assert capabilities["contract_features"]["create_job_run_job_id"] is True
        assert capabilities["contract_features"]["list_decisions_unary"] is True
        assert capabilities["contract_features"]["list_operations_unary"] is True

        create_status, created = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": "gRPC Job"},
            headers={"Idempotency-Key": "grpc-job-1", "X-Request-Id": "grpc-create"},
        )
        assert create_status == 201
        job_id = created["job"]["jobId"]

        run_status, created_run = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={"Idempotency-Key": "grpc-run-1", "X-Request-Id": "grpc-run-create"},
        )
        assert run_status == 201
        run_id = created_run["run"]["runId"]

        start_status, started = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs/{run_id}/commands/start",
            body={"actor": "grpc-tester", "reason": "grpc kickoff"},
            headers={"Idempotency-Key": "grpc-start-1", "X-Request-Id": "grpc-start"},
        )
        assert start_status == 200
        assert started["run"]["runState"] == "JOB_RUN_STATE_RUNNING"

        topology_status, topology = harness.request(
            "GET", f"/v1/jobs/{job_id}/topology?run_id={run_id}"
        )
        assert topology_status == 200
        assert topology["manifest"]["runId"] == run_id
        assert topology["decision"]["runId"] == run_id

        timeline_status, timeline = harness.request(
            "GET", f"/v1/jobs/{job_id}/timeline?run_id={run_id}&limit=1"
        )
        assert timeline_status == 200
        assert len(timeline["events"]) >= 1
        assert timeline["next_page_token"]

        sandboxes_status, sandboxes = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/sandboxes?run_id={run_id}&limit=1",
        )
        assert sandboxes_status == 200
        assert len(sandboxes["sandboxes"]) == 1
        assert sandboxes["next_page_token"]

        list_decisions_status, list_decisions = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/decisions?run_id={run_id}",
        )
        assert list_decisions_status == 200
        assert len(list_decisions["decisions"]) == 1
        decision_id = list_decisions["decisions"][0]["decisionId"]

        list_operations_status, list_operations = harness.request("GET", "/v1/operations")
        assert list_operations_status == 200
        assert any(operation["jobId"] == job_id for operation in list_operations["operations"])
        filtered_operations_status, filtered_operations = harness.request(
            "GET",
            f"/v1/operations?job_id={job_id}&run_id={run_id}&type=start&state=succeeded",
        )
        assert filtered_operations_status == 200
        assert len(filtered_operations["operations"]) == 1
        assert filtered_operations["operations"][0]["jobId"] == job_id
        assert filtered_operations["operations"][0]["runId"] == run_id

        get_decision_status, get_decision = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/decisions/{decision_id}",
        )
        assert get_decision_status == 200
        assert get_decision["decision"]["decisionId"] == decision_id

        missing_scoped_decision_status, missing_scoped_decision = harness.request(
            "GET",
            f"/v1/jobs/job-other/decisions/{decision_id}",
        )
        assert missing_scoped_decision_status == 404
        assert missing_scoped_decision["error"]["code"] == "not_found"


def test_grpc_gateway_data_kind_aliases_latest_run_and_404_405_contracts() -> None:
    with grpc_gateway_harness() as harness:
        synthetic_status, synthetic_job = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": "gRPC Synthetic Job"},
            headers={"Idempotency-Key": "grpc-data-kind-1", "X-Request-Id": "grpc-data-kind-1"},
        )
        assert synthetic_status == 201
        synthetic_job_id = synthetic_job["job"]["jobId"]

        live_status, live_job = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": "gRPC Live Job", "dataKind": "DATA_KIND_LIVE"},
            headers={"Idempotency-Key": "grpc-data-kind-2", "X-Request-Id": "grpc-data-kind-2"},
        )
        assert live_status == 201
        live_job_id = live_job["job"]["jobId"]

        live_query_status, live_query = harness.request("GET", "/v1/jobs?data_kind=live")
        assert live_query_status == 200
        assert [job["jobId"] for job in live_query["jobs"]] == [live_job_id]

        enum_query_status, enum_query = harness.request(
            "GET", "/v1/jobs?data_kind=DATA_KIND_SYNTHETIC"
        )
        assert enum_query_status == 200
        assert synthetic_job_id in [job["jobId"] for job in enum_query["jobs"]]
        assert live_job_id not in [job["jobId"] for job in enum_query["jobs"]]

        invalid_status, invalid_payload = harness.request("GET", "/v1/jobs?data_kind=unknown")
        assert invalid_status == 400
        assert invalid_payload["error"]["code"] == "bad_request"

        run_job_status, run_job = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": "gRPC Latest Run Job"},
            headers={"Idempotency-Key": "grpc-latest-job", "X-Request-Id": "grpc-latest-job"},
        )
        assert run_job_status == 201
        run_job_id = run_job["job"]["jobId"]

        first_run_status, first_run = harness.request(
            "POST",
            f"/v1/jobs/{run_job_id}/runs",
            headers={"Idempotency-Key": "grpc-latest-run-1", "X-Request-Id": "grpc-latest-run-1"},
        )
        assert first_run_status == 201
        first_run_id = first_run["run"]["runId"]

        second_run_status, second_run = harness.request(
            "POST",
            f"/v1/jobs/{run_job_id}/runs",
            headers={"Idempotency-Key": "grpc-latest-run-2", "X-Request-Id": "grpc-latest-run-2"},
        )
        assert second_run_status == 201
        second_run_id = second_run["run"]["runId"]

        list_runs_status, list_runs = harness.request("GET", f"/v1/jobs/{run_job_id}/runs?limit=1")
        assert list_runs_status == 200
        assert list_runs["runs"][0]["runId"] == second_run_id
        assert list_runs["runs"][0]["runId"] != first_run_id

        topology_status, topology = harness.request("GET", f"/v1/jobs/{run_job_id}/topology")
        assert topology_status == 200
        assert topology["run"]["runId"] == second_run_id
        assert topology["manifest"]["runId"] == second_run_id

        missing_status, missing_payload = harness.request("GET", "/v1/nope")
        assert missing_status == 404
        assert missing_payload["error"]["code"] == "not_found"
        headers = dict(harness.last_response_headers)
        assert "Allow" not in headers

        method_status, method_payload = harness.request("POST", "/v1/capabilities")
        assert method_status == 405
        assert method_payload["error"]["code"] == "method_not_allowed"
        headers = dict(harness.last_response_headers)
        assert headers["Allow"] == "GET"


def test_grpc_gateway_wraps_northbound_page_tokens_for_all_list_routes() -> None:
    with grpc_gateway_harness() as harness:
        create_status, created = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": "gRPC Token Job"},
            headers={"Idempotency-Key": "grpc-token-job", "X-Request-Id": "grpc-token-job"},
        )
        assert create_status == 201
        job_id = created["job"]["jobId"]

        other_job_status, other_job = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": "gRPC Token Other Job"},
            headers={"Idempotency-Key": "grpc-token-job-2", "X-Request-Id": "grpc-token-job-2"},
        )
        assert other_job_status == 201
        other_job_id = other_job["job"]["jobId"]

        run_status, created_run = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={"Idempotency-Key": "grpc-token-run", "X-Request-Id": "grpc-token-run"},
        )
        assert run_status == 201
        run_id = created_run["run"]["runId"]

        second_run_status, _ = harness.request(
            "POST",
            f"/v1/jobs/{other_job_id}/runs",
            headers={"Idempotency-Key": "grpc-token-run-2", "X-Request-Id": "grpc-token-run-2"},
        )
        assert second_run_status == 201

        for index, command in enumerate(("start", "pause", "resume"), start=1):
            status, _ = harness.request(
                "POST",
                f"/v1/jobs/{job_id}/runs/{run_id}/commands/{command}",
                body={"actor": "grpc", "reason": command},
                headers={
                    "Idempotency-Key": f"grpc-token-cmd-{index}",
                    "X-Request-Id": f"grpc-token-cmd-{index}",
                },
            )
            assert status == 200

        replay_status, _ = harness.request(
            "POST",
            "/v1/replays",
            body={"runId": run_id, "traceId": "grpc-token-trace", "sourceTraceRef": "grpc-ref"},
            headers={"Idempotency-Key": "grpc-token-replay", "X-Request-Id": "grpc-token-replay"},
        )
        assert replay_status == 201

        experiment_status, _ = harness.request(
            "POST",
            "/v1/experiments",
            body={"displayName": "gRPC Token Experiment", "summary": "token"},
            headers={
                "Idempotency-Key": "grpc-token-experiment",
                "X-Request-Id": "grpc-token-experiment",
            },
        )
        assert experiment_status == 201

        jobs_status, jobs = harness.request("GET", "/v1/jobs?limit=1")
        assert jobs_status == 200
        assert jobs["next_page_token"]
        assert "downstream-jobs" not in jobs["next_page_token"]
        assert harness.job_control_servicer is not None
        assert harness.job_control_servicer.last_list_jobs_request is not None
        assert harness.job_control_servicer.last_list_jobs_request.page_token == ""

        next_jobs_status, _next_jobs = harness.request(
            "GET",
            f"/v1/jobs?limit=1&page_token={jobs['next_page_token']}",
        )
        assert next_jobs_status == 200
        assert harness.job_control_servicer.last_list_jobs_request is not None
        assert harness.job_control_servicer.last_list_jobs_request.page_token.startswith(
            "downstream-jobs:"
        )

        filtered_jobs_status, filtered_jobs = harness.request(
            "GET", "/v1/jobs?data_kind=synthetic&limit=1"
        )
        assert filtered_jobs_status == 200
        cross_filter_status, cross_filter = harness.request(
            "GET",
            f"/v1/jobs?data_kind=live&page_token={filtered_jobs['next_page_token']}",
        )
        assert cross_filter_status == 400
        assert cross_filter["error"]["message"] == "invalid page_token"

        runs_status, runs = harness.request("GET", f"/v1/jobs/{job_id}/runs?limit=1")
        assert runs_status == 200
        assert runs["next_page_token"]
        assert "downstream-runs" not in runs["next_page_token"]
        runs_next_status, _ = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/runs?limit=1&page_token={runs['next_page_token']}",
        )
        assert runs_next_status == 200
        assert harness.job_control_servicer.last_list_runs_request is not None
        assert harness.job_control_servicer.last_list_runs_request.page_token.startswith(
            "downstream-runs:"
        )
        cross_job_runs_status, cross_job_runs = harness.request(
            "GET",
            f"/v1/jobs/{other_job_id}/runs?limit=1&page_token={runs['next_page_token']}",
        )
        assert cross_job_runs_status == 400
        assert cross_job_runs["error"]["message"] == "invalid page_token"

        operations_status, operations = harness.request(
            "GET", f"/v1/operations?job_id={job_id}&limit=1"
        )
        assert operations_status == 200
        assert operations["next_page_token"]
        assert "downstream-operations" not in operations["next_page_token"]
        operations_next_status, _ = harness.request(
            "GET",
            f"/v1/operations?job_id={job_id}&limit=1&page_token={operations['next_page_token']}",
        )
        assert operations_next_status == 200
        assert harness.job_control_servicer.last_list_operations_request is not None
        assert harness.job_control_servicer.last_list_operations_request.page_token.startswith(
            "downstream-operations:"
        )
        cross_operations_status, cross_operations = harness.request(
            "GET",
            f"/v1/operations?job_id={other_job_id}&limit=1&page_token={operations['next_page_token']}",
        )
        assert cross_operations_status == 400
        assert cross_operations["error"]["message"] == "invalid page_token"

        timeline_status, timeline = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/timeline?run_id={run_id}&limit=1",
        )
        assert timeline_status == 200
        assert timeline["next_page_token"]
        assert "downstream-events" not in timeline["next_page_token"]
        timeline_next_status, _ = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/timeline?run_id={run_id}&limit=1&page_token={timeline['next_page_token']}",
        )
        assert timeline_next_status == 200
        assert harness.job_control_servicer.last_list_events_request is not None
        assert harness.job_control_servicer.last_list_events_request.page_token.startswith(
            "downstream-events:"
        )
        cross_timeline_status, cross_timeline = harness.request(
            "GET",
            f"/v1/jobs/{other_job_id}/timeline?run_id={run_id}&limit=1&page_token={timeline['next_page_token']}",
        )
        assert cross_timeline_status == 400
        assert cross_timeline["error"]["message"] == "invalid page_token"

        sandboxes_status, sandboxes = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/sandboxes?run_id={run_id}&limit=1",
        )
        assert sandboxes_status == 200
        assert sandboxes["next_page_token"]
        assert "downstream-sandboxes" not in sandboxes["next_page_token"]
        sandboxes_next_status, _ = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/sandboxes?run_id={run_id}&limit=1&page_token={sandboxes['next_page_token']}",
        )
        assert sandboxes_next_status == 200

        decisions_status, decisions = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/decisions?run_id={run_id}&limit=1",
        )
        assert decisions_status == 200
        assert decisions["next_page_token"]
        assert "downstream-decisions" not in decisions["next_page_token"]
        decisions_next_status, _ = harness.request(
            "GET",
            f"/v1/jobs/{job_id}/decisions?run_id={run_id}&limit=1&page_token={decisions['next_page_token']}",
        )
        assert decisions_next_status == 200
        cross_decisions_status, cross_decisions = harness.request(
            "GET",
            f"/v1/jobs/{other_job_id}/decisions?run_id={run_id}&limit=1&page_token={decisions['next_page_token']}",
        )
        assert cross_decisions_status == 400
        assert cross_decisions["error"]["message"] == "invalid page_token"

        replays_status, replays = harness.request("GET", "/v1/replays?limit=1")
        assert replays_status == 200
        assert replays["next_page_token"]
        assert "downstream-replays" not in replays["next_page_token"]
        replays_next_status, _ = harness.request(
            "GET",
            f"/v1/replays?limit=1&page_token={replays['next_page_token']}",
        )
        assert replays_next_status == 200

        experiments_status, experiments = harness.request("GET", "/v1/experiments?limit=1")
        assert experiments_status == 200
        assert experiments["next_page_token"]
        assert "downstream-experiments" not in experiments["next_page_token"]
        experiments_next_status, _ = harness.request(
            "GET",
            f"/v1/experiments?limit=1&page_token={experiments['next_page_token']}",
        )
        assert experiments_next_status == 200

        cross_route_status, cross_route = harness.request(
            "GET",
            f"/v1/replays?limit=1&page_token={jobs['next_page_token']}",
        )
        assert cross_route_status == 400
        assert cross_route["error"]["message"] == "invalid page_token"


def test_real_job_controller_grpc_gateway_topology_defaults_to_latest_run(
    tmp_path: Path, job_controller_binary: Path
) -> None:
    suffix = f"{tmp_path.name}-{time.time_ns()}"
    with real_job_controller_gateway_harness(tmp_path, job_controller_binary) as harness:
        create_status, created = harness.request(
            "POST",
            "/v1/jobs",
            body=_real_controller_job_payload(suffix=suffix),
            headers={
                "Idempotency-Key": "real-controller-job",
                "X-Request-Id": "real-controller-job",
            },
        )
        assert create_status == 201
        job_id = created["job"]["jobId"]

        first_run_status, first_run = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={
                "Idempotency-Key": "real-controller-run-1",
                "X-Request-Id": "real-controller-run-1",
            },
        )
        assert first_run_status == 201
        first_run_id = first_run["run"]["runId"]

        second_run_status, second_run = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={
                "Idempotency-Key": "real-controller-run-2",
                "X-Request-Id": "real-controller-run-2",
            },
        )
        assert second_run_status == 201
        second_run_id = second_run["run"]["runId"]

        runs_status, runs = harness.request("GET", f"/v1/jobs/{job_id}/runs?limit=10")
        assert runs_status == 200
        assert [run["runId"] for run in runs["runs"][:2]] == [second_run_id, first_run_id]

        topology_status, topology = harness.request("GET", f"/v1/jobs/{job_id}/topology")
        assert topology_status == 200
        assert topology["run"]["runId"] == second_run_id
        assert topology["manifest"]["runId"] == second_run_id
        assert topology["decision"]["runId"] == second_run_id


def test_real_job_controller_gateway_latest_run_prefers_attempt_over_created_at(
    tmp_path: Path,
    job_controller_binary: Path,
) -> None:
    suffix = f"{tmp_path.name}-{time.time_ns()}-attempt"
    with real_job_controller_gateway_harness(tmp_path, job_controller_binary) as harness:
        create_status, created = harness.request(
            "POST",
            "/v1/jobs",
            body=_real_controller_job_payload(suffix=suffix),
            headers={
                "Idempotency-Key": "real-controller-attempt-job",
                "X-Request-Id": "real-controller-attempt-job",
            },
        )
        assert create_status == 201
        job_id = created["job"]["jobId"]

        first_run_status, first_run = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={
                "Idempotency-Key": "real-controller-attempt-run-1",
                "X-Request-Id": "real-controller-attempt-run-1",
            },
        )
        assert first_run_status == 201
        first_run_id = first_run["run"]["runId"]

        second_run_status, second_run = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={
                "Idempotency-Key": "real-controller-attempt-run-2",
                "X-Request-Id": "real-controller-attempt-run-2",
            },
        )
        assert second_run_status == 201
        second_run_id = second_run["run"]["runId"]

        runs_status, runs = harness.request("GET", f"/v1/jobs/{job_id}/runs?limit=10")
        assert runs_status == 200
        assert int(first_run["run"]["attempt"]) == 1
        assert int(second_run["run"]["attempt"]) == 2
        assert runs["runs"][0]["runId"] == second_run_id
        assert runs["runs"][1]["runId"] == first_run_id

        topology_status, topology = harness.request("GET", f"/v1/jobs/{job_id}/topology")
        assert topology_status == 200
        assert topology["run"]["runId"] == second_run_id


def test_grpc_gateway_create_run_uses_authoritative_job_id_field() -> None:
    with grpc_gateway_harness() as harness:
        create_status, created = harness.request(
            "POST",
            "/v1/jobs",
            body={"displayName": "gRPC Job"},
            headers={"Idempotency-Key": "grpc-job-contract-1", "X-Request-Id": "grpc-create-1"},
        )
        assert create_status == 201
        job_id = created["job"]["jobId"]

        run_status, _ = harness.request(
            "POST",
            f"/v1/jobs/{job_id}/runs",
            headers={"Idempotency-Key": "grpc-run-contract-1", "X-Request-Id": "grpc-run-1"},
        )
        assert run_status == 201

        assert harness.job_control_servicer is not None
        request = harness.job_control_servicer.last_create_run_request
        assert request is not None
        assert request.job_id == job_id
        assert not request.HasField("job")


def test_grpc_errors_map_to_http_statuses() -> None:
    with grpc_gateway_harness() as harness:
        missing_job_status, missing_job = harness.request("GET", "/v1/jobs/job-missing")
        assert missing_job_status == 404
        assert missing_job["error"]["code"] == "not_found"

        missing_decision_status, missing_decision = harness.request(
            "GET",
            "/v1/jobs/job-seeded/decisions/decision-missing",
        )
        assert missing_decision_status == 404
        assert missing_decision["error"]["code"] == "not_found"


def test_grpc_health_degrades_when_dependency_rpc_is_unavailable() -> None:
    config = GatewayConfig(
        backend_mode="grpc",
        job_control_target="127.0.0.1:1",
        scheduler_target="127.0.0.1:1",
        runtime_target="127.0.0.1:1",
        experiment_target="127.0.0.1:1",
        health_timeout_seconds=0.1,
    )
    harness = WsgiHarness(create_app(config=config).__call__)
    status, health = harness.request("GET", "/health")
    assert status == 200
    assert health["backend"] == "grpc"
    assert health["status"] == "degraded"
    assert all(dependency["detail"].startswith("grpc_") for dependency in health["dependencies"])


def test_cli_commands_are_runnable_against_live_gateway(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{ROOT / 'gateway-python'}:{ROOT / 'gen/python'}"
    job_payload = tmp_path / "job.json"
    job_payload.write_text(json.dumps({"displayName": "CLI Job"}), encoding="utf-8")

    openapi_output = tmp_path / "openapi.json"
    subprocess.run(
        [str(VENV_PYTHON), "-m", "tgsrl_gateway", "openapi", "--output", str(openapi_output)],
        check=True,
        cwd=ROOT,
        env=env,
    )
    exported = json.loads(openapi_output.read_text(encoding="utf-8"))
    assert exported["openapi"] == "3.1.0"

    port = 18081
    server = subprocess.Popen(
        [
            str(VENV_PYTHON),
            "-m",
            "tgsrl_gateway",
            "serve",
            "--port",
            str(port),
            "--backend-mode",
            "memory",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        client = GatewayClient(f"http://127.0.0.1:{port}", timeout=0.5)
        deadline = time.time() + 10.0
        while True:
            try:
                health = client.health()
                assert health["status"] == "ok"
                assert health["mode"] == "memory"
                break
            except Exception:
                if time.time() >= deadline:
                    raise
                time.sleep(0.1)

        create_job = subprocess.run(
            [
                str(VENV_PYTHON),
                "-m",
                "tgsrl_gateway",
                "create-job",
                "--base-url",
                f"http://127.0.0.1:{port}",
                "--job",
                str(job_payload),
                "--idempotency-key",
                "cli-job-1",
            ],
            check=True,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
        created = json.loads(create_job.stdout)
        job_id = created["job"]["jobId"]

        create_run = subprocess.run(
            [
                str(VENV_PYTHON),
                "-m",
                "tgsrl_gateway",
                "create-run",
                "--base-url",
                f"http://127.0.0.1:{port}",
                job_id,
                "--idempotency-key",
                "cli-run-1",
            ],
            check=True,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
        run = json.loads(create_run.stdout)["run"]
        run_id = run["runId"]

        start = subprocess.run(
            [
                str(VENV_PYTHON),
                "-m",
                "tgsrl_gateway",
                "job-command",
                "--base-url",
                f"http://127.0.0.1:{port}",
                job_id,
                run_id,
                "start",
                "--actor",
                "cli-test",
                "--idempotency-key",
                "cli-start-1",
            ],
            check=True,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
        started = json.loads(start.stdout)
        assert started["run"]["runState"] == "JOB_RUN_STATE_RUNNING"

        topology = subprocess.run(
            [
                str(VENV_PYTHON),
                "-m",
                "tgsrl_gateway",
                "topology",
                "--base-url",
                f"http://127.0.0.1:{port}",
                job_id,
                "--run-id",
                run_id,
            ],
            check=True,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
        topology_payload = json.loads(topology.stdout)
        assert topology_payload["manifest"]["runId"] == run_id
        assert len(topology_payload["runtime_units"]) == 2

        decisions = subprocess.run(
            [
                str(VENV_PYTHON),
                "-m",
                "tgsrl_gateway",
                "list-decisions",
                "--base-url",
                f"http://127.0.0.1:{port}",
                job_id,
                "--run-id",
                run_id,
            ],
            check=True,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
        decision_payload = json.loads(decisions.stdout)
        assert len(decision_payload["decisions"]) == 1
    finally:
        server.terminate()
        server.wait(timeout=10)
