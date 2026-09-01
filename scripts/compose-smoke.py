#!/usr/bin/env python3
"""Verify the running Compose stack through the Console HTTP origin."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from typing import Any, cast

import grpc
from google.protobuf import json_format
from tgsrl.v1 import execution_pb2, scheduling_pb2, scheduling_pb2_grpc
from tgsrl_gateway.sdk import GatewayClient

from adapters.contracts import canonical_contract_id

type JsonObject = dict[str, Any]


def _job_payload(name: str) -> JsonObject:
    contract: JsonObject = {
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
                "source": "compose-smoke",
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
    }
    message = execution_pb2.ExecutionContract()
    json_format.ParseDict(contract, message)
    contract["contractId"] = canonical_contract_id(message)
    return {
        "displayName": name,
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
        "desiredUnits": 2,
        "priority": 1,
        "queue": "default",
        "labels": {
            "quota_group": "default",
            "allow_preemption": "false",
            "local_smoke": name,
        },
        "rolloutMode": "ROLLOUT_MODE_PARTIALLY_ASYNC",
        "policyRef": "1",
        "dataKind": "DATA_KIND_SYNTHETIC",
    }


def _wait_for_run(
    client: GatewayClient,
    job_id: str,
    run_id: str,
    operation_id: str,
    expected_state: str,
    timeout: float,
) -> JsonObject:
    deadline = time.monotonic() + timeout
    last: JsonObject = {}
    while time.monotonic() < deadline:
        run = cast(JsonObject, client.get_run(job_id, run_id)["run"])
        operation = cast(JsonObject, client.get_operation(operation_id)["operation"])
        last = {"run": run, "operation": operation}
        if (
            run.get("runState") == expected_state
            and operation.get("state") == "OPERATION_STATE_SUCCEEDED"
        ):
            return last
        time.sleep(0.2)
    raise RuntimeError(
        f"timed out waiting for {expected_state}: {json.dumps(last, sort_keys=True)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    parser.add_argument("--scheduler-target", default="127.0.0.1:50051")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    client = GatewayClient(args.base_url, timeout=3.0)
    health = cast(JsonObject, client.health())
    dependencies = cast(list[JsonObject], health.get("dependencies", []))
    if (
        health.get("status") != "ok"
        or not dependencies
        or not all(item.get("serving") for item in dependencies)
    ):
        raise RuntimeError(f"Compose dependencies are not ready: {health}")

    token = "compose-smoke-" + uuid.uuid4().hex[:12]
    payload = _job_payload(token)
    created = client.create_job(
        payload, idempotency_key=f"{token}-create", request_id=f"{token}-create-request"
    )
    job_id = str(cast(JsonObject, created["job"])["jobId"])
    admitted = client.admit_job(
        job_id,
        reason="Compose full-stack smoke",
        idempotency_key=f"{token}-admit",
        request_id=f"{token}-admit-request",
    )
    if cast(JsonObject, admitted["operation"])["state"] != "OPERATION_STATE_SUCCEEDED":
        raise RuntimeError(f"admission failed: {admitted}")
    runs = cast(list[JsonObject], client.list_runs(job_id)["runs"])
    if len(runs) != 1:
        raise RuntimeError(f"admission produced {len(runs)} runs")
    run_id = str(runs[0]["runId"])

    outcomes: list[JsonObject] = []
    for command, expected in (
        ("start", "JOB_RUN_STATE_RUNNING"),
        ("pause", "JOB_RUN_STATE_PAUSED"),
        ("resume", "JOB_RUN_STATE_RUNNING"),
        ("stop", "JOB_RUN_STATE_STOPPED"),
    ):
        key = f"{token}-{command}"
        response = client.apply_job_command(
            job_id,
            run_id,
            command,
            actor="compose-smoke",
            reason=f"Compose smoke {command}",
            idempotency_key=key,
            request_id=f"{key}-request",
        )
        operation_id = str(cast(JsonObject, response["operation"])["operationId"])
        converged = _wait_for_run(client, job_id, run_id, operation_id, expected, args.timeout)
        outcomes.append(
            {
                "command": command,
                "run_state": cast(JsonObject, converged["run"])["runState"],
                "operation_state": cast(JsonObject, converged["operation"])["state"],
            }
        )

    topology = cast(JsonObject, client.get_topology(job_id, run_id=run_id))
    sandboxes = cast(list[JsonObject], topology.get("sandboxes", []))
    decisions = cast(list[JsonObject], client.list_decisions(job_id, run_id=run_id)["decisions"])
    if len(sandboxes) != 2 or {item.get("state") for item in sandboxes} != {
        "RUNTIME_STATE_TERMINATED"
    }:
        raise RuntimeError(f"unexpected terminal topology: {topology}")
    if not any(not item.get("fallback", False) for item in decisions):
        raise RuntimeError(f"no successful scheduling decision: {decisions}")

    channel = grpc.insecure_channel(args.scheduler_target)
    try:
        grpc.channel_ready_future(channel).result(timeout=args.timeout)
        scheduler = scheduling_pb2_grpc.SchedulerServiceStub(channel)
        snapshot = scheduler.GetSnapshot(
            scheduling_pb2.GetSnapshotRequest(include_pending_units=True), timeout=3.0
        ).snapshot
    finally:
        channel.close()
    retained = [
        allocation.allocation_id
        for allocation in snapshot.allocations
        if allocation.run_id == run_id
    ]
    if retained:
        raise RuntimeError(f"terminal run retained scheduler allocations: {retained}")

    print(
        json.dumps(
            {
                "status": "ok",
                "job_id": job_id,
                "run_id": run_id,
                "outcomes": outcomes,
                "sandbox_count": len(sandboxes),
                "decision_count": len(decisions),
                "retained_allocation_count": len(retained),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
