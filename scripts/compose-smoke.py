#!/usr/bin/env python3
"""Verify the local stack through the Console origin and managed-worker Trace RPC."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import grpc
from tgsrl.v1 import (
    runtime_pb2,
    runtime_pb2_grpc,
    scheduling_pb2,
    scheduling_pb2_grpc,
    trace_pb2,
)
from tgsrl_gateway.sdk import GatewayClient
from tgsrl_runtime.duration import to_timestamp
from tgsrl_runtime.trace_auth import sign_trace_request

type JsonObject = dict[str, Any]

_LOCAL_TRACE_SIGNING_KEY = b"tgsrl-local-worker-trace-signing-key-v1"


def _job_payload(name: str) -> JsonObject:
    path = Path(__file__).resolve().parents[1] / "configs/cpu-job.example.json"
    job = cast(JsonObject, json.loads(path.read_text(encoding="utf-8")))
    job["displayName"] = name
    job["labels"]["local_smoke"] = name
    return job


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


def _publish_synthetic_trace(
    *,
    runtime_target: str,
    job_id: str,
    run_id: str,
    trace_id: str,
    decision_id: str,
    timeout: float,
) -> int:
    """Publish one clearly-labelled local span graph through the production RPC path."""
    channel = grpc.insecure_channel(runtime_target)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout)
        runtime = runtime_pb2_grpc.RuntimeControlServiceStub(channel)
        status = runtime.GetRuntimeStatus(
            runtime_pb2.GetRuntimeStatusRequest(run_id=run_id), timeout=3.0
        )
        selected = next(
            (
                (item, sandbox)
                for item in status.runtime_units
                for sandbox in status.sandboxes
                if item.sandbox_id
                and sandbox.sandbox_id == item.sandbox_id
                and sandbox.binding.runtime_unit_id == item.runtime_unit_id
            ),
            None,
        )
        if selected is None:
            raise RuntimeError("local synthetic trace requires one materialized runtime unit")
        unit, sandbox = selected
        binding = sandbox.binding
        device_ids = list(binding.device_ids)
        required_attributes = {
            "runtime_unit_id": unit.runtime_unit_id,
            "binding_id": binding.binding_id,
            "device_ids": ",".join(device_ids),
            "source": "compose-smoke",
        }
        base = datetime.now(tz=UTC)
        spans = (
            (
                "trainer-step",
                0,
                3200,
                "trainer",
                "训练步骤 #1",
                "采样与训练迭代",
                "",
                "",
                "span-trainer",
                "",
                "32",
            ),
            (
                "request-1",
                120,
                1380,
                "request",
                "请求 req-local-1",
                "智能体请求",
                "req-local-1",
                "",
                "span-request-1",
                "span-trainer",
                "",
            ),
            (
                "agent-loop-1",
                170,
                260,
                "request",
                "智能体循环 / req-local-1",
                "提示词与工具编排",
                "req-local-1",
                "",
                "span-agent-1",
                "span-request-1",
                "",
            ),
            (
                "executor-1",
                460,
                690,
                "executor",
                "推理执行器 #0",
                "连续批处理调度",
                "req-local-1",
                "executor-0",
                "span-executor-1",
                "span-request-1",
                "16",
            ),
            (
                "worker-0-1",
                510,
                310,
                "worker",
                "工作进程 #0",
                "模型前向计算",
                "req-local-1",
                "executor-0",
                "span-worker-0-1",
                "span-executor-1",
                "8",
            ),
            (
                "worker-1-1",
                510,
                470,
                "worker",
                "工作进程 #1",
                "模型前向计算",
                "req-local-1",
                "executor-0",
                "span-worker-1-1",
                "span-executor-1",
                "8",
            ),
            (
                "request-2",
                480,
                2320,
                "request",
                "请求 req-local-2",
                "智能体请求 - 长尾",
                "req-local-2",
                "",
                "span-request-2",
                "span-trainer",
                "",
            ),
            (
                "tool-2",
                520,
                280,
                "request",
                "工具调用 / req-local-2",
                "工具调用等待",
                "req-local-2",
                "",
                "span-tool-2",
                "span-request-2",
                "",
            ),
            (
                "executor-2",
                860,
                1420,
                "executor",
                "推理执行器 #0",
                "长尾批次调度",
                "req-local-2",
                "executor-0",
                "span-executor-2",
                "span-request-2",
                "16",
            ),
            (
                "worker-0-2",
                930,
                840,
                "worker",
                "工作进程 #0",
                "模型前向计算",
                "req-local-2",
                "executor-0",
                "span-worker-0-2",
                "span-executor-2",
                "8",
            ),
            (
                "worker-1-2",
                930,
                1120,
                "worker",
                "工作进程 #1",
                "模型前向计算 - 长尾",
                "req-local-2",
                "executor-0",
                "span-worker-1-2",
                "span-executor-2",
                "8",
            ),
        )
        events: list[trace_pb2.TraceEvent] = []
        for sequence, span in enumerate(spans, start=101):
            (
                event_suffix,
                offset_ms,
                duration_ms,
                component,
                track,
                display_name,
                request_id,
                executor_id,
                span_id,
                parent_span_id,
                batch_size,
            ) = span
            attributes = {
                **required_attributes,
                "component": component,
                "track": track,
                "display_name": display_name,
                "duration_ms": str(duration_ms),
                "span_id": span_id,
            }
            for key, value in (
                ("request_id", request_id),
                ("executor_id", executor_id),
                ("parent_span_id", parent_span_id),
                ("batch_size", batch_size),
            ):
                if value:
                    attributes[key] = value
            if component == "worker":
                attributes["worker_id"] = track.replace("工作进程 #", "worker-")
            event_type = {
                "trainer": trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
                "request": trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
                "executor": trace_pb2.TRACE_EVENT_TYPE_DECISION_APPLIED,
                "worker": trace_pb2.TRACE_EVENT_TYPE_SAMPLE_CONSUMED,
            }[component]
            event = trace_pb2.TraceEvent(
                event_id=f"compose-smoke:{run_id}:{event_suffix}",
                job_id=job_id,
                execution_id=unit.execution_id,
                phase_id=unit.phase_id,
                event_type=event_type,
                algorithm="grpo",
                rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
                policy_version=status.manifest.policy_version,
                buffer_level=42,
                decision_id=decision_id,
                sequence=sequence,
                attributes=attributes,
                phase_kind=unit.phase_kind,
                raw_phase_label=unit.phase_id,
                sandbox_id=sandbox.sandbox_id,
                stage_id=unit.stage_id,
                run_id=run_id,
                data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
                trace_id=trace_id,
                generation=sandbox.generation,
            )
            event.occurred_at.CopyFrom(to_timestamp(base + timedelta(milliseconds=offset_ms)))
            events.append(event)
        safe_point = trace_pb2.TraceEvent(
            event_id=f"compose-smoke:{run_id}:safe-point",
            job_id=job_id,
            execution_id=unit.execution_id,
            phase_id=unit.phase_id,
            event_type=trace_pb2.TRACE_EVENT_TYPE_SAFE_POINT_REACHED,
            algorithm="grpo",
            rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
            policy_version=status.manifest.policy_version,
            buffer_level=35,
            safe_point=True,
            decision_id=decision_id,
            sequence=112,
            attributes={
                **required_attributes,
                "component": "trainer",
                "track": "训练步骤 #1",
                "display_name": "到达安全点",
                "span_id": "span-safe-point",
                "parent_span_id": "span-trainer",
            },
            phase_kind=unit.phase_kind,
            raw_phase_label=unit.phase_id,
            sandbox_id=sandbox.sandbox_id,
            stage_id=unit.stage_id,
            run_id=run_id,
            data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
            trace_id=trace_id,
            generation=sandbox.generation,
        )
        safe_point.occurred_at.CopyFrom(to_timestamp(base + timedelta(milliseconds=3200)))
        events.append(safe_point)
        batch = trace_pb2.TraceEventBatch(
            execution_id=unit.execution_id,
            first_sequence=101,
            last_sequence=112,
            events=events,
            run_id=run_id,
            trace_id=trace_id,
            data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
        )
        payload = batch.SerializeToString(deterministic=True)
        request = runtime_pb2.PublishTraceBatchRequest(
            batch_payload=payload,
            runtime_unit_id=unit.runtime_unit_id,
            sandbox_id=sandbox.sandbox_id,
            binding_id=binding.binding_id,
            generation=sandbox.generation,
            device_ids=device_ids,
            idempotency_key="trace-sha256-" + hashlib.sha256(payload).hexdigest(),
        )
        signing_key = os.environ.get(
            "TGSRL_WORKER_REGISTRY_SIGNING_KEY", _LOCAL_TRACE_SIGNING_KEY.decode()
        ).encode()
        request.authentication_tag = sign_trace_request(signing_key, request)
        response = runtime.PublishTraceBatch(request, timeout=5.0)
        if response.accepted_event_count != len(events):
            raise RuntimeError("Runtime did not accept the complete local synthetic trace")
        return len(events)
    finally:
        channel.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    parser.add_argument("--scheduler-target", default="127.0.0.1:50051")
    parser.add_argument("--runtime-target", default="127.0.0.1:50071")
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
    trace_id = str(runs[0]["traceId"])

    outcomes: list[JsonObject] = []
    synthetic_trace_event_count = 0
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
        if command == "start":
            decisions = cast(
                list[JsonObject], client.list_decisions(job_id, run_id=run_id)["decisions"]
            )
            synthetic_trace_event_count = _publish_synthetic_trace(
                runtime_target=args.runtime_target,
                job_id=job_id,
                run_id=run_id,
                trace_id=trace_id,
                decision_id=str(decisions[0].get("decisionId", "")) if decisions else "",
                timeout=args.timeout,
            )

    topology = cast(JsonObject, client.get_topology(job_id, run_id=run_id))
    sandboxes = cast(list[JsonObject], topology.get("sandboxes", []))
    decisions = cast(list[JsonObject], client.list_decisions(job_id, run_id=run_id)["decisions"])
    traces = cast(list[JsonObject], client.list_traces(job_id, run_id=run_id)["events"])
    if len(sandboxes) != 2 or {item.get("state") for item in sandboxes} != {
        "RUNTIME_STATE_TERMINATED"
    }:
        raise RuntimeError(f"unexpected terminal topology: {topology}")
    if not any(not item.get("fallback", False) for item in decisions):
        raise RuntimeError(f"no successful scheduling decision: {decisions}")
    if not traces or not all(
        item.get("runId") == run_id and item.get("traceId") == trace_id for item in traces
    ):
        raise RuntimeError(f"runtime trace events are missing or out of scope: {traces}")
    local_spans = [
        item
        for item in traces
        if cast(JsonObject, item.get("attributes", {})).get("source") == "compose-smoke"
    ]
    if len(local_spans) != synthetic_trace_event_count:
        raise RuntimeError(
            "managed-worker Trace readback did not retain every synthetic span: "
            f"accepted={synthetic_trace_event_count}, read_back={len(local_spans)}"
        )
    components = {
        str(cast(JsonObject, item.get("attributes", {})).get("component", ""))
        for item in local_spans
    }
    if not {"trainer", "request", "executor", "worker"}.issubset(components):
        raise RuntimeError(f"managed-worker Trace is missing required tracks: {components}")
    request_links = {
        str(cast(JsonObject, item.get("attributes", {})).get("request_id", ""))
        for item in local_spans
        if cast(JsonObject, item.get("attributes", {})).get("request_id")
    }
    if request_links != {"req-local-1", "req-local-2"}:
        raise RuntimeError(f"managed-worker Trace request linkage is incomplete: {request_links}")

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
                "trace_id": trace_id,
                "outcomes": outcomes,
                "sandbox_count": len(sandboxes),
                "decision_count": len(decisions),
                "trace_event_count": len(traces),
                "synthetic_trace_event_count": synthetic_trace_event_count,
                "retained_allocation_count": len(retained),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
