#!/usr/bin/env python3
"""Drive one Gate iteration through the live local TGS-RL service stack."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError

import grpc
from google.protobuf import json_format
from tgsrl.v1 import execution_pb2, runtime_pb2, runtime_pb2_grpc
from tgsrl_gateway.sdk import GatewayClient

from adapters.contracts import canonical_contract_id

type JsonObject = dict[str, Any]


def _wait(description: str, predicate: Callable[[], Any], timeout: float) -> Any:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as error:
            last_error = error
        time.sleep(0.05)
    suffix = f"; last error: {last_error}" if last_error else ""
    raise RuntimeError(f"timed out waiting for {description}{suffix}")


def _job(args: argparse.Namespace, trace_path: Path, checkpoint_root: Path) -> JsonObject:
    contract = {
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
                "description": "Gate CPU input is synthetic",
                "expression": "data_kind == synthetic",
                "failureMode": "VALIDITY_FAILURE_MODE_REJECT",
            }
        ],
        "versionConstraints": [
            {
                "component": "protocol",
                "operator": "VERSION_OPERATOR_COMPATIBLE",
                "version": "0.3.0",
                "source": "gate-full-stack",
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
        "safePointPolicy": {
            "enabled": True,
            "trigger": "SAFE_POINT_TRIGGER_PHASE_BOUNDARY",
            "maximumWait": "10s",
            "requiredPhaseIds": ["decode"],
        },
        "capabilities": {"deterministicReplay": True},
    }
    typed = execution_pb2.ExecutionContract()
    json_format.ParseDict(contract, typed)
    contract["contractId"] = canonical_contract_id(typed)
    python = os.environ["TGSRL_GATE_PYTHON"]
    root = os.environ["TGSRL_GATE_REPO_ROOT"]
    return {
        "displayName": f"gate-{args.label}-{args.phase}-{args.iteration}",
        "protocolVersion": "v0.3",
        "createdAt": "2026-01-01T00:00:00Z",
        "algorithm": "grpo",
        "runtime": {
            "framework": "verl",
            "frameworkVersion": "0.9.0",
            "executionBackend": "fake",
            "executionBackendVersion": "1.0.0",
            "trainer": "fake",
            "trainerVersion": "1.0.0",
            "rolloutEngine": "fake",
            "rolloutEngineVersion": "1.0.0",
            "imageDigest": "sha256:" + "1" * 64,
            "compatibilityProfile": "cpu-process-verl-v1",
            "command": [python, "scripts/gate-managed-workload.py"],
            "args": [
                "--seed",
                str(args.seed),
                "--items",
                str(args.items),
                "--iteration",
                str(args.iteration),
                "--checkpoint-root",
                str(checkpoint_root),
            ],
            "workingDirectory": root,
            "environment": {
                "TGSRL_VERL_CONTROL_SOCKET": str(trace_path.with_suffix(".sock")),
                "TGSRL_VERL_TRACE_PATH": str(trace_path),
                "TGSRL_VERL_STATE_PATH": str(trace_path.with_suffix(".state.json")),
            },
        },
        "executionContract": contract,
        "resourcesPerUnit": {"cpuMillis": "1000", "memoryBytes": str(512 << 20)},
        "requiredCapabilities": {
            "names": ["logical-cpu"],
            "algorithms": ["grpo"],
            "rolloutModes": ["partially_async"],
            "source": "mock",
            "revision": "1",
            "supportedActions": ["bind"],
        },
        "desiredUnits": 1,
        "priority": 1,
        "queue": "default",
        "labels": {"quota_group": "default", "allow_preemption": "false"},
        "rolloutMode": "ROLLOUT_MODE_PARTIALLY_ASYNC",
        "policyRef": "1",
        "dataKind": "DATA_KIND_SYNTHETIC",
    }


def _operation_succeeded(client: GatewayClient, operation_id: str) -> JsonObject | None:
    operation = cast(JsonObject, client.get_operation(operation_id)["operation"])
    if operation.get("state") == "OPERATION_STATE_FAILED":
        raise RuntimeError(f"operation failed: {operation}")
    return operation if operation.get("state") == "OPERATION_STATE_SUCCEEDED" else None


def _runtime_state(runtime: Any, run_id: str, state: int) -> bool:
    status = runtime.GetRuntimeStatus(
        runtime_pb2.GetRuntimeStatusRequest(run_id=run_id), timeout=2.0
    )
    return bool(status.sandboxes) and all(item.state == state for item in status.sandboxes)


def _command(
    client: GatewayClient,
    runtime: Any,
    *,
    job_id: str,
    run_id: str,
    command: str,
    state: int,
    key: str,
    timeout: float,
) -> tuple[float, JsonObject]:
    started = time.perf_counter_ns()
    result = client.apply_job_command(
        job_id,
        run_id,
        command,
        actor="gate-full-stack",
        reason=f"full-stack Gate {command}",
        idempotency_key=key,
        request_id=key + "-request",
    )
    operation_id = str(cast(JsonObject, result["operation"])["operationId"])
    _wait(
        f"{command} operation and runtime state",
        lambda: (
            _operation_succeeded(client, operation_id) and _runtime_state(runtime, run_id, state)
        ),
        timeout,
    )
    runtime_events = list(
        runtime.WatchRuntimeEvents(
            runtime_pb2.WatchRuntimeEventsRequest(run_id=run_id, job_id=job_id),
            timeout=2.0,
        )
    )
    observed = next(
        (item.event for item in reversed(runtime_events) if item.event.idempotency_key == key),
        None,
    )
    if observed is None:
        raise RuntimeError(f"{command} operation has no correlated Runtime observation")
    return (time.perf_counter_ns() - started) / 1_000_000, {
        "runtime_event_id": observed.event_id,
        "decision_id": observed.decision_id,
        "plan_id": observed.plan_id,
        "action_id": observed.action_id,
        "provider_revision": int(observed.provider_revision),
        "operation_id": operation_id,
    }


def execute(args: argparse.Namespace) -> list[JsonObject]:
    work_root = Path(os.environ["TGSRL_GATE_WORK_DIR"]) / (
        f"{args.label}-{args.phase}-{args.iteration}"
    )
    work_root.mkdir(parents=True, exist_ok=True)
    trace_path = work_root / "worker.ndjson"
    checkpoint_root = work_root / "checkpoints"
    client = GatewayClient(os.environ["TGSRL_GATE_GATEWAY_URL"], timeout=3.0)
    timeout = float(os.environ.get("TGSRL_GATE_TIMEOUT", "30"))
    created = client.create_job(
        _job(args, trace_path, checkpoint_root),
        idempotency_key=f"gate-create-{args.label}-{args.phase}-{args.iteration}",
        request_id=f"gate-create-{args.label}-{args.phase}-{args.iteration}-request",
    )
    job_id = str(cast(JsonObject, created["job"])["jobId"])
    admitted = client.admit_job(
        job_id,
        reason="full-stack Gate admission",
        idempotency_key=f"gate-admit-{args.label}-{args.phase}-{args.iteration}",
        request_id=f"gate-admit-{args.label}-{args.phase}-{args.iteration}-request",
    )
    admit_operation = str(cast(JsonObject, admitted["operation"])["operationId"])

    def admitted_run() -> JsonObject | None:
        operation = _operation_succeeded(client, admit_operation)
        if not operation:
            return None
        runs = cast(list[JsonObject], client.list_runs(job_id)["runs"])
        admitted_run = next(
            (
                run
                for run in runs
                if run.get("runState") in {"JOB_RUN_STATE_ADMITTED", "JOB_RUN_STATE_WAITING"}
            ),
            None,
        )
        if admitted_run is None:
            raise RuntimeError(
                "admission operation succeeded without an ADMITTED or WAITING run: "
                + json.dumps(runs, sort_keys=True)
            )
        return admitted_run

    run = cast(JsonObject, _wait("admission convergence", admitted_run, timeout))
    run_id = str(run["runId"])
    channel = grpc.insecure_channel(os.environ["TGSRL_GATE_RUNTIME_TARGET"])
    grpc.channel_ready_future(channel).result(timeout=timeout)
    runtime = runtime_pb2_grpc.RuntimeControlServiceStub(channel)
    started_at = datetime.now(tz=UTC)
    try:
        start_result = client.apply_job_command(
            job_id,
            run_id,
            "start",
            actor="gate-full-stack",
            reason="full-stack Gate start",
            idempotency_key=f"gate-start-{args.label}-{args.phase}-{args.iteration}",
            request_id=f"gate-start-{args.label}-{args.phase}-{args.iteration}-request",
        )
    except HTTPError as error:
        raise RuntimeError(
            f"start request failed with HTTP {error.code}: "
            + error.read().decode("utf-8", errors="replace")
        ) from error
    start_operation = str(cast(JsonObject, start_result["operation"])["operationId"])
    _wait(
        "start convergence",
        lambda: (
            _operation_succeeded(client, start_operation)
            and _runtime_state(runtime, run_id, runtime_pb2.RUNTIME_STATE_RUNNING)
        ),
        timeout,
    )
    decisions = cast(list[JsonObject], client.list_decisions(job_id, run_id=run_id)["decisions"])
    selected = next((item for item in decisions if not item.get("fallback")), None)
    if selected is None:
        raise RuntimeError(f"scheduler produced no non-fallback decision: {decisions}")
    service_events: list[JsonObject] = [
        {
            "event_type": "decision_applied",
            "source": "scheduler",
            "action": "bind",
            "decision_id": selected["decisionId"],
            "plan_id": cast(JsonObject, selected["selectedPlan"])["planId"],
            "duration_ms": max(
                0.0,
                (
                    datetime.fromisoformat(str(selected["decidedAt"]).replace("Z", "+00:00"))
                    - started_at
                ).total_seconds()
                * 1000.0,
            ),
            "succeeded": all(
                result.get("status") == "ACTION_RESULT_STATUS_SUCCEEDED"
                for result in cast(list[JsonObject], selected.get("actionResults", []))
            ),
            "rolled_back": False,
            "recovery_time_ms": 0.0,
        }
    ]
    if args.control_mode == "tgsrl":
        _wait(
            "managed workload progress",
            lambda: (
                trace_path.is_file()
                and '"event_type":"sample_consumed"' in trace_path.read_text(encoding="utf-8")
            ),
            timeout,
        )
        pause_ms, pause_evidence = _command(
            client,
            runtime,
            job_id=job_id,
            run_id=run_id,
            command="pause",
            state=runtime_pb2.RUNTIME_STATE_PAUSED,
            key=f"gate-pause-{args.label}-{args.phase}-{args.iteration}",
            timeout=timeout,
        )
        service_events.append(
            {
                "event_type": "decision_applied",
                "source": "operator",
                "action": "pause",
                "duration_ms": pause_ms,
                "succeeded": True,
                "rolled_back": False,
                "recovery_time_ms": 0.0,
                **pause_evidence,
            }
        )
        resume_ms, resume_evidence = _command(
            client,
            runtime,
            job_id=job_id,
            run_id=run_id,
            command="resume",
            state=runtime_pb2.RUNTIME_STATE_RUNNING,
            key=f"gate-resume-{args.label}-{args.phase}-{args.iteration}",
            timeout=timeout,
        )
        service_events.append(
            {
                "event_type": "decision_applied",
                "source": "operator",
                "action": "resume",
                "duration_ms": resume_ms,
                "succeeded": True,
                "rolled_back": False,
                "recovery_time_ms": 0.0,
                **resume_evidence,
            }
        )
    _wait(
        "managed workload completion trace",
        lambda: (
            trace_path.is_file()
            and '"event_type":"workload_completed"' in trace_path.read_text(encoding="utf-8")
        ),
        timeout,
    )
    stop_ms, stop_evidence = _command(
        client,
        runtime,
        job_id=job_id,
        run_id=run_id,
        command="stop",
        state=runtime_pb2.RUNTIME_STATE_TERMINATED,
        key=f"gate-stop-{args.label}-{args.phase}-{args.iteration}",
        timeout=timeout,
    )
    channel.close()
    events = [
        cast(JsonObject, json.loads(line))
        for line in trace_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if args.control_mode == "tgsrl":
        worker_actions = {str(event.get("event_type")) for event in events}
        required_actions = {"prepare_pause", "pause", "resume"}
        if not required_actions.issubset(worker_actions):
            raise RuntimeError(
                "managed worker trace is missing lifecycle callbacks: "
                + ", ".join(sorted(required_actions - worker_actions))
            )
    service_events.append(
        {
            "event_type": "control_completed",
            "source": "operator",
            "action": "stop",
            "duration_ms": stop_ms,
            "succeeded": True,
            **stop_evidence,
        }
    )
    events.extend(service_events)
    node_id = hashlib.sha256(socket.gethostname().encode()).hexdigest()[:16]
    for event in events:
        event.setdefault("source", "worker")
        event.update(
            {
                "label": args.label,
                "phase": args.phase,
                "iteration": args.iteration,
                "device": args.device,
                "node_id": node_id,
                "service_run_id": run_id,
                "service_job_id": job_id,
            }
        )
    return events


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", choices=("baseline", "variant"), required=True)
    parser.add_argument("--phase", choices=("warmup", "measurement"), required=True)
    parser.add_argument("--iteration", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--items", type=int, required=True)
    parser.add_argument("--device", choices=("cpu",), required=True)
    parser.add_argument("--control-mode", choices=("static", "tgsrl"), required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    for event in execute(args):
        print(json.dumps(event, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
