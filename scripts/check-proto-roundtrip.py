"""Verify Python -> Go -> Python binary compatibility for the core intent."""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tgsrl.v1 import (
    control_pb2,
    execution_pb2,
    experiment_pb2,
    operator_pb2,
    resource_pb2,
    runtime_pb2,
    scheduling_pb2,
    semantic_pb2,
    trace_pb2,
)
from tgsrl_runtime.intent import IntentBuilder

from adapters import GRPOAdapter, PartialAsyncRolloutAdapter, build_execution_contract


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    contract = build_execution_contract(GRPOAdapter(), PartialAsyncRolloutAdapter())
    intent = IntentBuilder(clock=lambda: datetime(2026, 8, 27, tzinfo=UTC)).build(
        execution_id="roundtrip-execution",
        stage_id="decode",
        job_id="roundtrip-job",
        contract=contract,
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        policy_version="roundtrip-policy",
        ttl=timedelta(minutes=5),
        resources_per_unit=resource_pb2.ResourceVector(cpu_millis=500, memory_bytes=1 << 30),
        required_capabilities=resource_pb2.CapabilitySet(
            names=["logical-cpu"],
            algorithms=["grpo"],
            rollout_modes=["partially_async"],
            source="mock",
            revision=1,
            supported_actions=["bind"],
        ),
        deterministic_seed=42,
        labels={"data_kind": "synthetic"},
    )
    intent.run_id = "roundtrip-run"
    intent.trace_id = "roundtrip-trace"
    intent.data_kind = trace_pb2.DATA_KIND_SYNTHETIC
    intent.semantic_context.CopyFrom(
        trace_pb2.SemanticEnvelope(
            envelope_id="roundtrip-envelope",
            schema_version="v1",
            type_name="tgsrl.v1.SchedulingIntent",
            payload=semantic_pb2.SemanticObject(
                fields=[
                    semantic_pb2.SemanticField(
                        key="surface",
                        value=semantic_pb2.SemanticValue(string_value="proto-roundtrip"),
                    )
                ]
            ),
            attributes={"surface": "proto-roundtrip"},
            digest="sha256:roundtrip",
        )
    )
    original = intent.SerializeToString(deterministic=True)
    completed = subprocess.run(
        ["go", "run", "./scripts/proto-roundtrip-go.go"],
        cwd=root,
        input=original,
        stdout=subprocess.PIPE,
        check=True,
    )
    decoded = scheduling_pb2.SchedulingIntent.FromString(completed.stdout)
    if decoded != intent:
        raise SystemExit("Go round-trip changed SchedulingIntent semantics")
    if completed.stdout != original:
        raise SystemExit("Go round-trip changed deterministic wire bytes")
    # Touch the new generated surfaces so import regressions fail fast even when
    # the core wire fixture only round-trips SchedulingIntent.
    assert control_pb2.JobRun().DESCRIPTOR.full_name == "tgsrl.v1.JobRun"
    assert runtime_pb2.RuntimeManifest().DESCRIPTOR.full_name == "tgsrl.v1.RuntimeManifest"
    assert experiment_pb2.Experiment().DESCRIPTOR.full_name == "tgsrl.v1.Experiment"
    assert operator_pb2.ApplyRuntimeControlRequest().DESCRIPTOR.full_name == (
        "tgsrl.v1.ApplyRuntimeControlRequest"
    )
    assert runtime_pb2.WatchRuntimeEventsResponse(sequence=1).sequence == 1
    print("Go <-> Python SchedulingIntent round-trip passed")


if __name__ == "__main__":
    main()
