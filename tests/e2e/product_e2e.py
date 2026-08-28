#!/usr/bin/env python3
"""Drive and verify the real local product stack across process restarts."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError

import grpc
from google.protobuf import json_format
from tgsrl.v1 import (
    control_pb2,
    control_pb2_grpc,
    execution_pb2,
    experiment_pb2,
    resource_pb2,
    runtime_pb2,
    runtime_pb2_grpc,
    scheduling_pb2,
    scheduling_pb2_grpc,
    trace_pb2,
)
from tgsrl_gateway.sdk import GatewayClient
from tgsrl_runtime.duration import to_timestamp
from tgsrl_runtime.replay import ReplayArtifactStep, encode_replay_step_artifact

from adapters.contracts import canonical_contract_id

type JsonObject = dict[str, Any]


def _complete_job() -> JsonObject:
    job: JsonObject = {
        "displayName": "product-e2e-fake-job",
        "protocolVersion": "v0.3",
        "createdAt": "2026-01-01T00:00:00Z",
        "algorithm": "grpo",
        "runtime": {
            "framework": "fake",
            "frameworkVersion": "1.0.0",
            "executionBackend": "fake",
            "executionBackendVersion": "1.0.0",
            "trainer": "fake",
            "trainerVersion": "1.0.0",
            "rolloutEngine": "fake",
            "rolloutEngineVersion": "1.0.0",
            "imageDigest": "sha256:" + "1" * 64,
            "compatibilityProfile": "cpu-mock-v1",
            "command": ["python", "-c", "print('local fake runtime')"],
        },
        "executionContract": {
            "contractId": "computed-below",
            "version": "1.0.0",
            "phaseGraph": {
                "phases": [
                    {
                        "phaseId": "decode",
                        "displayName": "Decode",
                        "kind": "PHASE_KIND_DECODE",
                        "parallelism": 1,
                        "maxAttempts": 1,
                    }
                ],
                "entryPhaseIds": ["decode"],
            },
            "validityRules": [
                {
                    "ruleId": "synthetic-only",
                    "description": "Only generated local data is used",
                    "expression": "data_kind == synthetic",
                    "failureMode": "VALIDITY_FAILURE_MODE_REJECT",
                }
            ],
            "versionConstraints": [
                {
                    "component": "protocol",
                    "operator": "VERSION_OPERATOR_COMPATIBLE",
                    "version": "0.3.0",
                    "source": "product-e2e",
                    "revision": "1",
                }
            ],
            "commitPolicy": {
                "mode": "COMMIT_MODE_ALL_OR_NOTHING",
                "minimumSuccessfulUnits": 1,
                "maxRetries": 1,
                "commitTimeout": "30s",
            },
            "backpressurePolicy": {
                "mode": "BACKPRESSURE_MODE_BLOCK_PRODUCER",
                "lowWatermark": "1",
                "highWatermark": "2",
                "maximumBufferLevel": "4",
                "stallTimeout": "30s",
            },
            "safePointPolicy": {"enabled": False},
            "capabilities": {"deterministicReplay": True},
        },
        "resourcesPerUnit": {"cpuMillis": "1000", "memoryBytes": str(512 << 20)},
        "requiredCapabilities": {
            "names": ["logical-cpu"],
            "algorithms": ["grpo"],
            "rolloutModes": ["partially_async"],
            "source": "mock",
            "revision": "1",
            "supportedActions": ["bind"],
        },
        "desiredUnits": 2,
        "priority": 1,
        "queue": "default",
        "labels": {"quota_group": "default", "allow_preemption": "false"},
        "rolloutMode": "ROLLOUT_MODE_PARTIALLY_ASYNC",
        "policyRef": "1",
        "dataKind": "DATA_KIND_SYNTHETIC",
    }
    contract = execution_pb2.ExecutionContract()
    json_format.ParseDict(cast(JsonObject, job["executionContract"]), contract)
    cast(JsonObject, job["executionContract"])["contractId"] = canonical_contract_id(contract)
    return job


def _wait_until(
    description: str,
    predicate: Callable[[], Any],
    *,
    timeout: float,
    interval: float = 0.1,
) -> Any:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as error:
            last_error = error
        time.sleep(interval)
    suffix = f"; last error: {last_error}" if last_error is not None else ""
    raise AssertionError(f"timed out waiting for {description}{suffix}")


def _health(client: GatewayClient, timeout: float) -> JsonObject:
    def dependency_ready(item: JsonObject) -> bool:
        # Runtime has no health RPC and its unknown-run probe currently reaches
        # the service as UNKNOWN; the direct runtime RPC readiness check below
        # proves that boundary before any lifecycle mutation.
        return bool(item.get("serving")) or (
            item.get("name") == "runtime" and item.get("detail") == "grpc_probe:unknown"
        )

    def ready() -> JsonObject:
        payload = client.health()
        dependencies = cast(list[JsonObject], payload.get("dependencies", []))
        if not dependencies or not all(dependency_ready(item) for item in dependencies):
            raise AssertionError(f"gateway dependencies are not ready: {payload}")
        return payload

    health = cast(
        JsonObject,
        _wait_until(
            "gateway and all gRPC dependencies",
            ready,
            timeout=timeout,
        ),
    )
    dependencies = cast(list[JsonObject], health["dependencies"])
    expected_names = {
        "job_control",
        "scheduler",
        "runtime",
        "experiment",
    }
    if {item["name"] for item in dependencies} != expected_names or not all(
        dependency_ready(item) for item in dependencies
    ):
        raise AssertionError(f"unexpected gateway health: {health}")
    capabilities = client.capabilities()
    if capabilities.get("backend_mode") != "grpc":
        raise AssertionError(f"gateway did not use its gRPC backend: {capabilities}")
    return health


def _runtime_stub(target: str, timeout: float) -> tuple[grpc.Channel, Any]:
    channel = grpc.insecure_channel(target)
    grpc.channel_ready_future(channel).result(timeout=timeout)
    return channel, runtime_pb2_grpc.RuntimeControlServiceStub(channel)


def _control_stub(target: str, timeout: float) -> tuple[grpc.Channel, Any]:
    channel = grpc.insecure_channel(target)
    grpc.channel_ready_future(channel).result(timeout=timeout)
    return channel, control_pb2_grpc.JobControlServiceStub(channel)


def _scheduler_stub(target: str, timeout: float) -> tuple[grpc.Channel, Any]:
    channel = grpc.insecure_channel(target)
    grpc.channel_ready_future(channel).result(timeout=timeout)
    return channel, scheduling_pb2_grpc.SchedulerServiceStub(channel)


def _runtime_status(runtime: Any, run_id: str) -> runtime_pb2.GetRuntimeStatusResponse:
    return cast(
        runtime_pb2.GetRuntimeStatusResponse,
        runtime.GetRuntimeStatus(runtime_pb2.GetRuntimeStatusRequest(run_id=run_id), timeout=2.0),
    )


def _persisted_intent(state_db: str, run_id: str) -> scheduling_pb2.SchedulingIntent:
    with sqlite3.connect(Path(state_db)) as connection:
        row = connection.execute(
            "SELECT payload FROM intents WHERE run_id = ? ORDER BY version DESC LIMIT 1",
            (run_id,),
        ).fetchone()
    if row is None:
        raise AssertionError(f"SQLite contains no scheduling intent for {run_id}")
    return scheduling_pb2.SchedulingIntent.FromString(row[0])


def _job_run(control: Any, job_id: str, run_id: str) -> control_pb2.JobRun:
    response = control.GetJobRun(
        control_pb2.GetJobRunRequest(job_id=job_id, run_id=run_id), timeout=2.0
    )
    return cast(control_pb2.JobRun, response.run)


def _require_states(
    status: runtime_pb2.GetRuntimeStatusResponse, expected: int, *, sandboxes: bool = True
) -> runtime_pb2.GetRuntimeStatusResponse | None:
    if not status.runtime_units or not all(unit.state == expected for unit in status.runtime_units):
        return None
    if sandboxes and (
        not status.sandboxes or not all(sandbox.state == expected for sandbox in status.sandboxes)
    ):
        return None
    return status


def _require_run_state(run: control_pb2.JobRun, expected: int) -> control_pb2.JobRun | None:
    return run if run.run_state == expected else None


def _operation(client: GatewayClient, operation_id: str) -> JsonObject:
    return cast(JsonObject, client.get_operation(operation_id)["operation"])


def _runtime_events(runtime: Any, job_id: str, run_id: str) -> list[Any]:
    return list(
        runtime.WatchRuntimeEvents(
            runtime_pb2.WatchRuntimeEventsRequest(run_id=run_id, job_id=job_id), timeout=2.0
        )
    )


def _assert_event_stream_order(responses: list[Any]) -> None:
    if not responses:
        raise AssertionError("runtime event stream is empty")
    sequences = [int(response.sequence) for response in responses]
    if any(sequence <= 0 for sequence in sequences) or any(
        current >= following for current, following in pairwise(sequences)
    ):
        raise AssertionError(f"runtime event sequences are not strictly increasing: {sequences}")
    event_ids = [response.event.event_id for response in responses]
    if len(set(event_ids)) != len(event_ids):
        raise AssertionError(f"runtime event IDs are not unique: {event_ids}")
    for response in responses:
        if response.sequence <= 0 or not response.cursor:
            raise AssertionError(f"runtime event lacks sequence/cursor: {response}")
        expected = f"{response.sequence}:{response.event.event_id}"
        if response.cursor != expected:
            raise AssertionError(
                f"runtime event cursor {response.cursor!r} does not match {expected!r}"
            )


def _assert_event_causality(
    event: runtime_pb2.SandboxEvent,
    *,
    job_id: str,
    run_id: str,
    trace_id: str,
    idempotency_key: str | None = None,
    decision_id: str | None = None,
    plan_id: str | None = None,
    action_id: str | None = None,
) -> None:
    if (event.job_id, event.run_id, event.trace_id) != (job_id, run_id, trace_id):
        raise AssertionError(f"runtime event identity mismatch: {event}")
    if not event.runtime_unit_id or not event.sandbox_id:
        raise AssertionError(f"runtime event lacks target identity: {event}")
    if event.binding.runtime_unit_id != event.runtime_unit_id:
        raise AssertionError(f"runtime event binding identity mismatch: {event}")
    if not event.decision_id or not event.plan_id or not event.action_id:
        raise AssertionError(f"runtime event lacks scheduling causality: {event}")
    if event.provider_revision <= 0 or event.generation <= 0:
        raise AssertionError(f"runtime event lacks provider/generation fence: {event}")
    if idempotency_key is not None and event.idempotency_key != idempotency_key:
        raise AssertionError(f"runtime event idempotency mismatch: {event}")
    if decision_id is not None and event.decision_id != decision_id:
        raise AssertionError(f"runtime event decision mismatch: {event}")
    if plan_id is not None and event.plan_id != plan_id:
        raise AssertionError(f"runtime event plan mismatch: {event}")
    if action_id is not None and (not action_id or event.action_id != action_id):
        raise AssertionError(f"runtime event action mismatch: {event}")


def _latest_matching_event(
    responses: list[Any], *, event_type: int, idempotency_key: str
) -> runtime_pb2.SandboxEvent | None:
    matches = [
        response.event
        for response in responses
        if response.event.event_type == event_type
        and response.event.idempotency_key == idempotency_key
    ]
    return matches[-1] if matches else None


def _wait_for_lifecycle(
    client: GatewayClient,
    runtime: Any,
    control: Any,
    *,
    job_id: str,
    run_id: str,
    trace_id: str,
    operation_id: str,
    idempotency_key: str,
    event_type: int,
    runtime_state: int,
    run_state: int,
    timeout: float,
) -> tuple[runtime_pb2.GetRuntimeStatusResponse, control_pb2.JobRun, list[Any]]:
    def converged() -> (
        tuple[runtime_pb2.GetRuntimeStatusResponse, control_pb2.JobRun, list[Any]] | None
    ):
        events = _runtime_events(runtime, job_id, run_id)
        matching = _latest_matching_event(
            events, event_type=event_type, idempotency_key=idempotency_key
        )
        status = _runtime_status(runtime, run_id)
        job_run = _job_run(control, job_id, run_id)
        operation = _operation(client, operation_id)
        if matching is None:
            return None
        if _require_states(status, runtime_state) is None:
            return None
        if job_run.run_state != run_state:
            return None
        if operation.get("state") != "OPERATION_STATE_SUCCEEDED":
            return None
        runtime_components = [
            item for item in job_run.component_status if item.component == "runtime"
        ]
        if not any(
            item.converged and item.observed_runtime_state == runtime_state
            for item in runtime_components
        ):
            return None
        _assert_event_stream_order(events)
        _assert_event_causality(
            matching,
            job_id=job_id,
            run_id=run_id,
            trace_id=trace_id,
            idempotency_key=idempotency_key,
        )
        resumed = _runtime_events_after(runtime, job_id, run_id, events[-1].cursor)
        if resumed:
            raise AssertionError(f"runtime cursor replayed old events: {resumed}")
        return status, job_run, events

    return cast(
        tuple[runtime_pb2.GetRuntimeStatusResponse, control_pb2.JobRun, list[Any]],
        _wait_until(f"observed lifecycle event {idempotency_key}", converged, timeout=timeout),
    )


def _wait_for_start(
    client: GatewayClient,
    runtime: Any,
    control: Any,
    *,
    job_id: str,
    run_id: str,
    operation_id: str,
    timeout: float,
) -> tuple[runtime_pb2.GetRuntimeStatusResponse, control_pb2.JobRun, list[Any]]:
    def converged() -> (
        tuple[runtime_pb2.GetRuntimeStatusResponse, control_pb2.JobRun, list[Any]] | None
    ):
        events = _runtime_events(runtime, job_id, run_id)
        event_types = {response.event.event_type for response in events}
        status = _runtime_status(runtime, run_id)
        job_run = _job_run(control, job_id, run_id)
        operation = _operation(client, operation_id)
        if not {
            runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
            runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        }.issubset(event_types):
            return None
        if _require_states(status, runtime_pb2.RUNTIME_STATE_RUNNING) is None:
            return None
        if job_run.run_state != control_pb2.JOB_RUN_STATE_RUNNING:
            return None
        if operation.get("state") != "OPERATION_STATE_SUCCEEDED":
            return None
        runtime_components = [
            item for item in job_run.component_status if item.component == "runtime"
        ]
        if not any(
            item.converged and item.observed_runtime_state == runtime_pb2.RUNTIME_STATE_RUNNING
            for item in runtime_components
        ):
            return None
        _assert_event_stream_order(events)
        if _runtime_events_after(runtime, job_id, run_id, events[-1].cursor):
            raise AssertionError("runtime cursor replayed old start events")
        return status, job_run, events

    return cast(
        tuple[runtime_pb2.GetRuntimeStatusResponse, control_pb2.JobRun, list[Any]],
        _wait_until("observed start convergence", converged, timeout=timeout),
    )


def _command(
    client: GatewayClient,
    job_id: str,
    run_id: str,
    command: str,
    transitional_run_state: str,
    *,
    idempotency_key: str,
) -> JsonObject:
    result = client.apply_job_command(
        job_id,
        run_id,
        command,
        actor="product-e2e",
        reason=f"product E2E {command}",
        idempotency_key=idempotency_key,
        request_id=f"{idempotency_key}-request",
    )
    operation = cast(JsonObject, result["operation"])
    run = cast(JsonObject, result["run"])
    if operation["state"] != "OPERATION_STATE_RUNNING":
        raise AssertionError(f"{command} operation was not accepted as RUNNING: {result}")
    if run["runState"] != transitional_run_state:
        raise AssertionError(f"{command} did not converge: {result}")
    return result


def _runtime_events_after(runtime: Any, job_id: str, run_id: str, cursor: str) -> list[Any]:
    return list(
        runtime.WatchRuntimeEvents(
            runtime_pb2.WatchRuntimeEventsRequest(
                run_id=run_id, job_id=job_id, after_cursor=cursor
            ),
            timeout=2.0,
        )
    )


def run_before_restart(args: argparse.Namespace) -> JsonObject:
    client = GatewayClient(args.gateway_url, timeout=2.0)
    _health(client, args.timeout)
    job_payload = _complete_job()
    created = client.create_job(
        job_payload,
        idempotency_key="product-e2e-create",
        request_id="product-e2e-create-request",
    )
    repeated = client.create_job(
        job_payload,
        idempotency_key="product-e2e-create",
        request_id="product-e2e-create-request",
    )
    created_job = cast(JsonObject, created["job"])
    created_operation = cast(JsonObject, created["operation"])
    repeated_operation = cast(JsonObject, repeated["operation"])
    job_id = str(created_job["jobId"])
    if cast(JsonObject, repeated["job"])["jobId"] != job_id:
        raise AssertionError("same create idempotency key produced a different job")
    if repeated_operation["operationId"] != created_operation["operationId"]:
        raise AssertionError("same create idempotency key produced a different operation")
    jobs = cast(list[JsonObject], client.list_jobs()["jobs"])
    if [item["jobId"] for item in jobs].count(job_id) != 1:
        raise AssertionError(f"idempotent create duplicated job {job_id}: {jobs}")

    admitted = client.admit_job(
        job_id,
        reason="product E2E admission",
        idempotency_key="product-e2e-admit",
        request_id="product-e2e-admit-request",
    )
    repeated_admit = client.admit_job(
        job_id,
        reason="product E2E admission",
        idempotency_key="product-e2e-admit",
        request_id="product-e2e-admit-request",
    )
    admit_operation = cast(JsonObject, admitted["operation"])
    if admit_operation["state"] != "OPERATION_STATE_SUCCEEDED":
        raise AssertionError(f"admission did not succeed: {admitted}")
    if (
        admit_operation["operationId"]
        != cast(JsonObject, repeated_admit["operation"])["operationId"]
    ):
        raise AssertionError("same admit idempotency key produced a different operation")
    runs = cast(list[JsonObject], client.list_runs(job_id)["runs"])
    if len(runs) != 1:
        raise AssertionError(f"admission should produce exactly one run, got {runs}")
    run_id, trace_id = str(runs[0]["runId"]), str(runs[0]["traceId"])

    runtime_channel, runtime = _runtime_stub(args.runtime_target, args.timeout)
    control_channel, control = _control_stub(args.control_target, args.timeout)
    try:
        before_start = _runtime_status(runtime, run_id)
        if (
            _require_states(before_start, runtime_pb2.RUNTIME_STATE_REQUESTED, sandboxes=False)
            is None
        ):
            raise AssertionError(f"runtime did not start REQUESTED: {before_start}")
        if before_start.sandboxes or any(unit.sandbox_id for unit in before_start.runtime_units):
            raise AssertionError(f"runtime was pre-bound before scheduling: {before_start}")

        started = _command(
            client,
            job_id,
            run_id,
            "start",
            "JOB_RUN_STATE_STARTING",
            idempotency_key="product-e2e-start",
        )
        start_operation = cast(JsonObject, started["operation"])
        start_operation_id = str(start_operation["operationId"])

        decisions_payload = cast(
            JsonObject,
            _wait_until(
                "a non-fallback scheduler decision",
                lambda: (
                    payload
                    if (payload := client.list_decisions(job_id, run_id=run_id)).get("decisions")
                    else None
                ),
                timeout=args.timeout,
            ),
        )
        decisions = cast(list[JsonObject], decisions_payload["decisions"])
        decision = next((item for item in decisions if not item.get("fallback", False)), None)
        if decision is None:
            raise AssertionError(f"scheduler produced only fallback decisions: {decisions}")
        if (decision.get("jobId"), decision.get("runId"), decision.get("traceId")) != (
            job_id,
            run_id,
            trace_id,
        ):
            raise AssertionError(f"decision identity mismatch: {decision}")
        if not cast(JsonObject, decision.get("selectedPlan", {})).get("bindings"):
            raise AssertionError(f"decision contains no binding: {decision}")
        if {item["status"] for item in cast(list[JsonObject], decision["actionResults"])} != {
            "ACTION_RESULT_STATUS_SUCCEEDED"
        }:
            raise AssertionError(f"scheduler actions failed: {decision}")
        selected_plan = cast(JsonObject, decision["selectedPlan"])
        bindings = cast(list[JsonObject], selected_plan["bindings"])
        actions = cast(list[JsonObject], selected_plan["actions"])
        if len(bindings) != 2 or len(actions) != 2:
            raise AssertionError(f"expected two concrete runtime targets: {decision}")
        action_by_id = {
            str(cast(JsonObject, action["binding"])["bindingId"]): str(action["actionId"])
            for action in actions
        }
        if any(not value for value in action_by_id.values()):
            raise AssertionError(f"scheduler actions lack binding causality: {actions}")

        _running, running_job, runtime_events = _wait_for_start(
            client,
            runtime,
            control,
            job_id=job_id,
            run_id=run_id,
            operation_id=start_operation_id,
            timeout=args.timeout,
        )
        event_types = {item.event.event_type for item in runtime_events}
        if not {
            runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
            runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        }.issubset(event_types):
            raise AssertionError(f"operator BOUND/RUNNING events missing: {runtime_events}")
        before_lifecycle_event_count = len(runtime_events)
        _assert_event_stream_order(runtime_events)
        for response in runtime_events:
            if response.event.event_type in {
                runtime_pb2.SANDBOX_EVENT_TYPE_BOUND,
                runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
            }:
                event = response.event
                if (event.job_id, event.run_id, event.trace_id) != (
                    job_id,
                    run_id,
                    trace_id,
                ):
                    raise AssertionError(f"initial runtime event identity mismatch: {event}")
                if not event.sandbox_id or not event.binding.runtime_unit_id:
                    raise AssertionError(f"initial runtime event lacks target identity: {event}")
                if (event.decision_id, event.plan_id, event.action_id) != (
                    str(decision["decisionId"]),
                    str(selected_plan["planId"]),
                    action_by_id.get(event.binding.binding_id, ""),
                ):
                    raise AssertionError(f"initial runtime event causality mismatch: {event}")
        repeated_start = client.apply_job_command(
            job_id,
            run_id,
            "start",
            actor="product-e2e",
            reason="product E2E start",
            idempotency_key="product-e2e-start",
            request_id="product-e2e-start-request",
        )
        if cast(JsonObject, repeated_start["operation"])["operationId"] != start_operation_id:
            raise AssertionError("same start idempotency key produced a different operation")

        paused = _command(
            client,
            job_id,
            run_id,
            "pause",
            "JOB_RUN_STATE_PAUSING",
            idempotency_key="product-e2e-pause",
        )
        _paused_status, _, _ = _wait_for_lifecycle(
            client,
            runtime,
            control,
            job_id=job_id,
            run_id=run_id,
            trace_id=trace_id,
            operation_id=str(cast(JsonObject, paused["operation"])["operationId"]),
            idempotency_key="product-e2e-pause",
            event_type=runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED,
            runtime_state=runtime_pb2.RUNTIME_STATE_PAUSED,
            run_state=control_pb2.JOB_RUN_STATE_PAUSED,
            timeout=args.timeout,
        )

        resumed = _command(
            client,
            job_id,
            run_id,
            "resume",
            "JOB_RUN_STATE_RESUMING",
            idempotency_key="product-e2e-resume",
        )
        _resumed_status, _, _ = _wait_for_lifecycle(
            client,
            runtime,
            control,
            job_id=job_id,
            run_id=run_id,
            trace_id=trace_id,
            operation_id=str(cast(JsonObject, resumed["operation"])["operationId"]),
            idempotency_key="product-e2e-resume",
            event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
            runtime_state=runtime_pb2.RUNTIME_STATE_RUNNING,
            run_state=control_pb2.JOB_RUN_STATE_RUNNING,
            timeout=args.timeout,
        )

        stopped = _command(
            client,
            job_id,
            run_id,
            "stop",
            "JOB_RUN_STATE_STOPPING",
            idempotency_key="product-e2e-stop",
        )
        _stopped_status, _, _ = _wait_for_lifecycle(
            client,
            runtime,
            control,
            job_id=job_id,
            run_id=run_id,
            trace_id=trace_id,
            operation_id=str(cast(JsonObject, stopped["operation"])["operationId"]),
            idempotency_key="product-e2e-stop",
            event_type=runtime_pb2.SANDBOX_EVENT_TYPE_TERMINATED,
            runtime_state=runtime_pb2.RUNTIME_STATE_TERMINATED,
            run_state=control_pb2.JOB_RUN_STATE_STOPPED,
            timeout=args.timeout,
        )
        stopped_events = _runtime_events(runtime, job_id, run_id)
        _assert_event_stream_order(stopped_events)
        if len(stopped_events) <= len(runtime_events):
            raise AssertionError("lifecycle controls did not append observed runtime events")
        expected_lifecycle = [
            ("product-e2e-pause", runtime_pb2.SANDBOX_EVENT_TYPE_PAUSED),
            ("product-e2e-resume", runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING),
            ("product-e2e-stop", runtime_pb2.SANDBOX_EVENT_TYPE_TERMINATED),
        ]
        for key, event_type in expected_lifecycle:
            observed = _latest_matching_event(
                stopped_events, event_type=event_type, idempotency_key=key
            )
            if observed is None:
                raise AssertionError(f"expected one observed event for {key}: {stopped_events}")
            _assert_event_causality(
                observed,
                job_id=job_id,
                run_id=run_id,
                trace_id=trace_id,
                idempotency_key=key,
                decision_id=str(decision["decisionId"]),
                plan_id=str(selected_plan["planId"]),
                action_id=action_by_id.get(observed.binding.binding_id),
            )
        try:
            client.apply_job_command(
                job_id,
                run_id,
                "pause",
                actor="product-e2e",
                reason="invalid pause after stop",
                idempotency_key="product-e2e-invalid-pause",
                request_id="product-e2e-invalid-pause-request",
            )
        except HTTPError as error:
            failure_payload = json.loads(error.read().decode("utf-8"))
            if (
                error.code != 400
                or cast(JsonObject, failure_payload["error"])["code"] != "backend_invalid_argument"
            ):
                raise AssertionError(
                    f"unexpected terminal-transition error: {failure_payload}"
                ) from error
        else:
            raise AssertionError("pause after stop unexpectedly succeeded")
        if _job_run(control, job_id, run_id).run_state != control_pb2.JOB_RUN_STATE_STOPPED:
            raise AssertionError("failed pause mutated the stopped job state")
        if (
            _require_states(_runtime_status(runtime, run_id), runtime_pb2.RUNTIME_STATE_TERMINATED)
            is None
        ):
            raise AssertionError("failed pause mutated the stopped runtime state")

        retried = client.apply_job_command(
            job_id,
            run_id,
            "retry",
            actor="product-e2e",
            reason="product E2E retry for terminate coverage",
            idempotency_key="product-e2e-retry",
            request_id="product-e2e-retry-request",
        )
        retry_operation = cast(JsonObject, retried["operation"])
        retried_run = cast(JsonObject, retried["run"])
        if retry_operation["state"] != "OPERATION_STATE_SUCCEEDED":
            raise AssertionError(f"retry preparation failed: {retried}")
        if retried_run["runState"] != "JOB_RUN_STATE_WAITING":
            raise AssertionError(f"retry did not produce a waiting run: {retried}")
        terminal_run_id = str(retried_run["runId"])
        terminal_trace_id = str(retried_run["traceId"])
        before_terminal_start = _runtime_status(runtime, terminal_run_id)
        if (
            _require_states(
                before_terminal_start, runtime_pb2.RUNTIME_STATE_REQUESTED, sandboxes=False
            )
            is None
            or before_terminal_start.sandboxes
        ):
            raise AssertionError(f"retry runtime was pre-bound: {before_terminal_start}")

        terminal_started = _command(
            client,
            job_id,
            terminal_run_id,
            "start",
            "JOB_RUN_STATE_STARTING",
            idempotency_key="product-e2e-terminal-start",
        )
        terminal_start_operation_id = str(
            cast(JsonObject, terminal_started["operation"])["operationId"]
        )
        terminal_decisions_payload = cast(
            JsonObject,
            _wait_until(
                "a non-fallback scheduler decision for the retry run",
                lambda: (
                    payload
                    if (payload := client.list_decisions(job_id, run_id=terminal_run_id)).get(
                        "decisions"
                    )
                    else None
                ),
                timeout=args.timeout,
            ),
        )
        terminal_decisions = cast(list[JsonObject], terminal_decisions_payload["decisions"])
        terminal_decision = next(
            (item for item in terminal_decisions if not item.get("fallback", False)), None
        )
        if terminal_decision is None:
            raise AssertionError(f"retry run has no actionable decision: {terminal_decisions}")
        terminal_plan = cast(JsonObject, terminal_decision["selectedPlan"])
        terminal_actions = cast(list[JsonObject], terminal_plan["actions"])
        if len(terminal_actions) != 2:
            raise AssertionError(f"retry run expected two actions: {terminal_decision}")
        terminal_action_by_id = {
            str(cast(JsonObject, action["binding"])["bindingId"]): str(action["actionId"])
            for action in terminal_actions
        }
        if any(not value for value in terminal_action_by_id.values()):
            raise AssertionError(f"retry scheduler actions lack causality: {terminal_actions}")
        terminal_running, _terminal_running_job, _terminal_start_events = _wait_for_start(
            client,
            runtime,
            control,
            job_id=job_id,
            run_id=terminal_run_id,
            operation_id=terminal_start_operation_id,
            timeout=args.timeout,
        )
        terminated = _command(
            client,
            job_id,
            terminal_run_id,
            "terminate",
            "JOB_RUN_STATE_TERMINATING",
            idempotency_key="product-e2e-terminate",
        )
        _terminal_status, _, terminal_events = _wait_for_lifecycle(
            client,
            runtime,
            control,
            job_id=job_id,
            run_id=terminal_run_id,
            trace_id=terminal_trace_id,
            operation_id=str(cast(JsonObject, terminated["operation"])["operationId"]),
            idempotency_key="product-e2e-terminate",
            event_type=runtime_pb2.SANDBOX_EVENT_TYPE_TERMINATED,
            runtime_state=runtime_pb2.RUNTIME_STATE_TERMINATED,
            run_state=control_pb2.JOB_RUN_STATE_TERMINATED,
            timeout=args.timeout,
        )
        terminal_observations = [
            response.event
            for response in terminal_events
            if response.event.event_type == runtime_pb2.SANDBOX_EVENT_TYPE_TERMINATED
            and response.event.idempotency_key == "product-e2e-terminate"
        ]
        if len(terminal_observations) != 2:
            raise AssertionError(f"terminate observations are incomplete: {terminal_events}")
        for terminal_event in terminal_observations:
            _assert_event_causality(
                terminal_event,
                job_id=job_id,
                run_id=terminal_run_id,
                trace_id=terminal_trace_id,
                idempotency_key="product-e2e-terminate",
                decision_id=str(terminal_decision["decisionId"]),
                plan_id=str(terminal_plan["planId"]),
                action_id=terminal_action_by_id.get(terminal_event.binding.binding_id),
            )

        topology = client.get_topology(job_id, run_id=terminal_run_id)
        sandboxes = client.list_sandboxes(job_id, run_id=terminal_run_id)
        timeline = client.list_timeline(job_id, run_id=terminal_run_id, limit=100)
        fetched_decision = client.get_decision(job_id, str(terminal_decision["decisionId"]))
        topology_units = cast(list[JsonObject], topology["runtime_units"])
        topology_sandboxes = cast(list[JsonObject], topology["sandboxes"])
        if {item["state"] for item in topology_units} != {"RUNTIME_STATE_TERMINATED"}:
            raise AssertionError(f"terminal topology mismatch: {topology}")
        if cast(list[JsonObject], sandboxes["sandboxes"]) != topology_sandboxes:
            raise AssertionError("gateway sandbox and topology views disagree")
        if (
            cast(JsonObject, fetched_decision["decision"])["decisionId"]
            != terminal_decision["decisionId"]
        ):
            raise AssertionError("gateway GetDecision returned another decision")
        timeline_events = cast(list[JsonObject], timeline["events"])
        required_job_events = {
            "JOB_EVENT_TYPE_JOB_STARTED",
            "JOB_EVENT_TYPE_JOB_TERMINATED",
            "JOB_EVENT_TYPE_COMPONENT_CHANGED",
        }
        if not required_job_events.issubset({item["eventType"] for item in timeline_events}):
            raise AssertionError(f"gateway timeline lacks lifecycle evidence: {timeline}")
    finally:
        runtime_channel.close()
        control_channel.close()

    with sqlite3.connect(Path(args.state_db)) as connection:
        stopped_persisted_events = [
            runtime_pb2.SandboxEvent.FromString(row[0])
            for row in connection.execute(
                "SELECT payload FROM runtime_events WHERE run_id = ?", (run_id,)
            )
        ]
        terminal_persisted_events = [
            runtime_pb2.SandboxEvent.FromString(row[0])
            for row in connection.execute(
                "SELECT payload FROM runtime_events WHERE run_id = ?", (terminal_run_id,)
            )
        ]
    required_runtime_states = {
        runtime_pb2.RUNTIME_STATE_BOUND,
        runtime_pb2.RUNTIME_STATE_RUNNING,
        runtime_pb2.RUNTIME_STATE_PAUSED,
        runtime_pb2.RUNTIME_STATE_TERMINATED,
    }
    if not required_runtime_states.issubset({event.state for event in stopped_persisted_events}):
        raise AssertionError(f"SQLite lacks lifecycle events: {stopped_persisted_events}")
    if runtime_pb2.RUNTIME_STATE_TERMINATED not in {
        event.state for event in terminal_persisted_events
    }:
        raise AssertionError(f"SQLite lacks terminate event: {terminal_persisted_events}")

    replay_now = datetime.now(tz=UTC)
    replay_event = trace_pb2.TraceEvent(
        event_id="product-e2e-replay-event",
        job_id=job_id,
        execution_id=terminal_run_id,
        phase_id="decode",
        occurred_at=to_timestamp(replay_now),
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
        algorithm="grpo",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version="1",
        decision_id=str(terminal_decision["decisionId"]),
        sequence=1,
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        stage_id="decode",
        run_id=terminal_run_id,
        trace_id=terminal_trace_id,
        data_kind=trace_pb2.DATA_KIND_REPLAY,
    )
    replay_intent = _persisted_intent(args.state_db, terminal_run_id)
    replay_intent.submitted_at.CopyFrom(to_timestamp(replay_now))
    replay_intent.valid_until.CopyFrom(to_timestamp(replay_now + timedelta(minutes=1)))
    replay_intent.unit_count = 1
    replay_intent.resources_per_unit.cpu_millis = 1000
    replay_intent.resources_per_unit.memory_bytes = 1 << 30
    replay_capabilities = resource_pb2.CapabilitySet()
    replay_capabilities.CopyFrom(replay_intent.required_capabilities)
    replay_capabilities.measured_at.CopyFrom(replay_event.occurred_at)
    replay_snapshot = resource_pb2.ClusterSnapshot(
        snapshot_id="product-e2e-replay-snapshot",
        revision=1,
        observed_at=replay_event.occurred_at,
        devices=[
            resource_pb2.Device(
                device_id="product-e2e-replay-device",
                kind=resource_pb2.DEVICE_KIND_CPU,
                health=resource_pb2.DEVICE_HEALTH_READY,
                capacity=resource_pb2.ResourceVector(
                    cpu_millis=8000, memory_bytes=16 << 30, accelerator_units=1
                ),
                allocatable=resource_pb2.ResourceVector(
                    cpu_millis=8000, memory_bytes=16 << 30, accelerator_units=1
                ),
                capabilities=replay_capabilities,
            )
        ],
    )
    replay_step = ReplayArtifactStep(
        ordinal=1,
        event=replay_event,
        intent=replay_intent,
        snapshot=replay_snapshot,
    )
    replay_message = experiment_pb2.Replay(
        replay_id="product-e2e-replay",
        run_id=terminal_run_id,
        trace_id=terminal_trace_id,
        source_trace_ref="local://product-e2e-trace",
        speed=1.0,
        seed=replay_intent.deterministic_seed,
        data_kind=trace_pb2.DATA_KIND_REPLAY,
        artifacts=[encode_replay_step_artifact("product-e2e-replay", replay_step)],
    )
    replay = client.create_replay(
        json_format.MessageToDict(replay_message, preserving_proto_field_name=False),
        idempotency_key="product-e2e-replay-create",
        request_id="product-e2e-replay-create-request",
    )
    replay_id = str(cast(JsonObject, replay["replay"])["replayId"])
    replay_started = client.apply_replay_command(
        replay_id,
        "start",
        idempotency_key="product-e2e-replay-start",
        request_id="product-e2e-replay-start-request",
    )
    if cast(JsonObject, replay_started["replay"])["state"] != "REPLAY_STATE_COMPLETED":
        raise AssertionError(f"replay did not start: {replay_started}")
    experiment = client.create_experiment(
        {
            "experimentId": "product-e2e-experiment",
            "displayName": "Product E2E comparison",
            "runs": [
                {
                    "experimentRunId": "product-e2e-experiment-run",
                    "runId": run_id,
                    "traceId": trace_id,
                    "kind": "EXPERIMENT_RUN_KIND_REPLAY",
                    "dataKind": "DATA_KIND_REPLAY",
                    "configHash": "product-e2e-config",
                    "codeRevision": "local",
                    "policyVersion": "1",
                    "replayId": replay_id,
                }
            ],
        },
        idempotency_key="product-e2e-experiment-create",
        request_id="product-e2e-experiment-create-request",
    )
    experiment_id = str(cast(JsonObject, experiment["experiment"])["experimentId"])
    if cast(JsonObject, client.get_replay(replay_id)["replay"])["replayId"] != replay_id:
        raise AssertionError("gateway could not read the created replay")
    if (
        cast(JsonObject, client.get_experiment(experiment_id)["experiment"])["experimentId"]
        != experiment_id
    ):
        raise AssertionError("gateway could not read the created experiment")

    state = {
        "job_id": job_id,
        "run_id": terminal_run_id,
        "trace_id": terminal_trace_id,
        "stopped_run_id": run_id,
        "stopped_trace_id": trace_id,
        "decision_id": terminal_decision["decisionId"],
        "create_operation_id": created_operation["operationId"],
        "admit_operation_id": admit_operation["operationId"],
        "start_operation_id": start_operation["operationId"],
        "pause_operation_id": cast(JsonObject, paused["operation"])["operationId"],
        "resume_operation_id": cast(JsonObject, resumed["operation"])["operationId"],
        "stop_operation_id": cast(JsonObject, stopped["operation"])["operationId"],
        "retry_operation_id": retry_operation["operationId"],
        "terminal_start_operation_id": terminal_start_operation_id,
        "terminate_operation_id": cast(JsonObject, terminated["operation"])["operationId"],
        "timeline_event_count": len(timeline_events),
        "runtime_event_count": len(terminal_persisted_events),
        "stopped_runtime_event_count": len(stopped_persisted_events),
        "initial_runtime_event_count": before_lifecycle_event_count,
        "sandbox_count": len(terminal_running.sandboxes),
        "replay_id": replay_id,
        "experiment_id": experiment_id,
        "invalid_terminal_transition_status": 400,
        "observed_running_state": control_pb2.JobRunState.Name(running_job.run_state),
    }
    Path(args.state_file).write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    return {"phase": "before_restart", "status": "ok"} | state


def run_after_restart(args: argparse.Namespace) -> JsonObject:
    state = cast(JsonObject, json.loads(Path(args.state_file).read_text()))
    job_id, run_id = str(state["job_id"]), str(state["run_id"])
    stopped_run_id = str(state["stopped_run_id"])
    client = GatewayClient(args.gateway_url, timeout=2.0)
    _health(client, args.timeout)
    jobs = cast(list[JsonObject], client.list_jobs()["jobs"])
    runs = cast(list[JsonObject], client.list_runs(job_id)["runs"])
    if [item["jobId"] for item in jobs].count(job_id) != 1 or len(runs) != 2:
        raise AssertionError(f"controller recovery duplicated or lost state: {jobs}, {runs}")
    runs_by_id = {str(item["runId"]): item for item in runs}
    if runs_by_id.get(stopped_run_id, {}).get("runState") != "JOB_RUN_STATE_STOPPED":
        raise AssertionError(f"controller did not recover terminal state: {runs}")
    if runs_by_id.get(run_id, {}).get("runState") != "JOB_RUN_STATE_TERMINATED":
        raise AssertionError(f"controller did not recover terminated retry state: {runs}")

    decisions = cast(list[JsonObject], client.list_decisions(job_id, run_id=run_id)["decisions"])
    recovered_original = next(
        (item for item in decisions if item["decisionId"] == state["decision_id"]), None
    )
    if recovered_original is None:
        raise AssertionError(f"scheduler did not recover its decision: {decisions}")
    actionable = [item for item in decisions if item.get("actionResults")]
    if len(actionable) != 1 or actionable[0]["decisionId"] != state["decision_id"]:
        raise AssertionError(f"scheduler replayed a provider mutation after restart: {decisions}")
    if any(
        item.get("fallback", False)
        or item.get("jobId") != job_id
        or item.get("runId") != run_id
        or item.get("traceId") != state["trace_id"]
        for item in decisions
    ):
        raise AssertionError(f"scheduler recovery produced an invalid decision: {decisions}")
    topology = client.get_topology(job_id, run_id=run_id)
    topology_units = cast(list[JsonObject], topology["runtime_units"])
    topology_sandboxes = cast(list[JsonObject], topology["sandboxes"])
    if {item["state"] for item in topology_units} != {"RUNTIME_STATE_TERMINATED"}:
        raise AssertionError(f"runtime did not recover units: {topology}")
    if {item["state"] for item in topology_sandboxes} != {"RUNTIME_STATE_TERMINATED"}:
        raise AssertionError(f"runtime did not recover sandboxes: {topology}")
    stopped_topology = client.get_topology(job_id, run_id=stopped_run_id)
    if {item["state"] for item in cast(list[JsonObject], stopped_topology["runtime_units"])} != {
        "RUNTIME_STATE_TERMINATED"
    }:
        raise AssertionError(f"stopped runtime did not recover: {stopped_topology}")
    timeline = cast(
        list[JsonObject], client.list_timeline(job_id, run_id=run_id, limit=100)["events"]
    )
    if len(timeline) != state["timeline_event_count"]:
        raise AssertionError(f"controller timeline changed across restart: {timeline}")
    stopped_timeline = cast(
        list[JsonObject],
        client.list_timeline(job_id, run_id=stopped_run_id, limit=100)["events"],
    )
    if "JOB_EVENT_TYPE_JOB_STOPPED" not in {item["eventType"] for item in stopped_timeline}:
        raise AssertionError(f"stopped-run timeline did not recover: {stopped_timeline}")
    recovered_replay = cast(JsonObject, client.get_replay(str(state["replay_id"]))["replay"])
    recovered_experiment = cast(
        JsonObject, client.get_experiment(str(state["experiment_id"]))["experiment"]
    )
    if recovered_replay["state"] != "REPLAY_STATE_COMPLETED":
        raise AssertionError(f"replay did not recover: {recovered_replay}")
    if recovered_experiment["experimentId"] != state["experiment_id"]:
        raise AssertionError(f"experiment did not recover: {recovered_experiment}")

    replayed_create = client.create_job(
        _complete_job(),
        idempotency_key="product-e2e-create",
        request_id="product-e2e-create-request",
    )
    if (
        cast(JsonObject, replayed_create["operation"])["operationId"]
        != state["create_operation_id"]
    ):
        raise AssertionError("create idempotency record was not recovered")
    repeated_stop = client.apply_job_command(
        job_id,
        stopped_run_id,
        "stop",
        actor="product-e2e",
        reason="product E2E stop",
        idempotency_key="product-e2e-stop",
        request_id="product-e2e-stop-request",
    )
    if cast(JsonObject, repeated_stop["operation"])["operationId"] != state["stop_operation_id"]:
        raise AssertionError("stop idempotency record was not recovered")
    repeated_terminate = client.apply_job_command(
        job_id,
        run_id,
        "terminate",
        actor="product-e2e",
        reason="product E2E terminate",
        idempotency_key="product-e2e-terminate",
        request_id="product-e2e-terminate-request",
    )
    if (
        cast(JsonObject, repeated_terminate["operation"])["operationId"]
        != state["terminate_operation_id"]
    ):
        raise AssertionError("terminate idempotency record was not recovered")
    if len(cast(list[JsonObject], client.list_runs(job_id)["runs"])) != 2:
        raise AssertionError("idempotent retry after restart duplicated the run")

    cursor = cast(
        JsonObject,
        _wait_until(
            "operator cursor recovery",
            lambda: json.loads(Path(args.operator_cursor).read_text()),
            timeout=args.timeout,
        ),
    )
    if int(str(cursor.get("sequence", 0))) < 1:
        raise AssertionError(f"operator cursor did not recover: {cursor}")

    runtime_channel, runtime = _runtime_stub(args.runtime_target, args.timeout)
    control_channel, control = _control_stub(args.control_target, args.timeout)
    try:
        recovered_runtime = _runtime_status(runtime, run_id)
        recovered_run = _job_run(control, job_id, run_id)
        if recovered_run.run_state != control_pb2.JOB_RUN_STATE_TERMINATED:
            raise AssertionError(f"gRPC job state was not recovered: {recovered_run}")
        if _require_states(recovered_runtime, runtime_pb2.RUNTIME_STATE_TERMINATED) is None:
            raise AssertionError(f"gRPC runtime state was not recovered: {recovered_runtime}")
        recovered_events = list(
            runtime.WatchRuntimeEvents(
                runtime_pb2.WatchRuntimeEventsRequest(run_id=run_id, job_id=job_id), timeout=2.0
            )
        )
        if len(recovered_events) != state["runtime_event_count"]:
            raise AssertionError(f"runtime event log changed across restart: {recovered_events}")
        stopped_recovered_events = _runtime_events(runtime, job_id, stopped_run_id)
        if len(stopped_recovered_events) != state["stopped_runtime_event_count"]:
            raise AssertionError(
                f"stopped runtime event log changed across restart: {stopped_recovered_events}"
            )
    finally:
        runtime_channel.close()
        control_channel.close()

    return {
        "phase": "after_restart",
        "status": "ok",
        "job_id": job_id,
        "run_id": run_id,
        "trace_id": state["trace_id"],
        "decision_id": state["decision_id"],
        "job_run_state": "JOB_RUN_STATE_TERMINATED",
        "runtime_state": "RUNTIME_STATE_TERMINATED",
        "job_count": len(jobs),
        "run_count": len(runs),
        "decision_count": len(decisions),
        "timeline_event_count": len(timeline),
        "runtime_event_count": len(recovered_events),
        "operator_cursor_sequence": cursor.get("sequence"),
        "replay_id": state["replay_id"],
        "experiment_id": state["experiment_id"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["before-restart", "after-restart"])
    parser.add_argument("--gateway-url", required=True)
    parser.add_argument("--runtime-target", required=True)
    parser.add_argument("--control-target", required=True)
    parser.add_argument("--state-db", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--operator-cursor", required=True)
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run_before_restart(args) if args.phase == "before-restart" else run_after_restart(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
