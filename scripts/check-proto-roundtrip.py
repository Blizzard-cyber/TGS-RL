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
    observation = execution_pb2.ContractObservation(
        source="roundtrip",
        event_id="roundtrip-event",
        phase_id="decode",
        policy_version="roundtrip-policy",
        buffer_level=16,
        policy_lag=4,
        sample_stale=False,
        effective_sample_size=12.0,
        accepted_samples=11,
        expected_samples=12,
        sample_count=12,
        effective_sample_size_ratio=1.0,
        safe_point=True,
        typed_facts=[
            semantic_pb2.SemanticField(
                key="sample.policy_lag",
                value=semantic_pb2.SemanticValue(uint64_value=4),
            )
        ],
    )
    observation.observed_at.FromDatetime(datetime(2026, 8, 27, tzinfo=UTC))
    observation.oldest_sample_at.FromDatetime(datetime(2026, 8, 27, 0, 0, 1, tzinfo=UTC))
    observation.backpressure_started_at.FromDatetime(datetime(2026, 8, 27, 0, 0, 2, tzinfo=UTC))
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
    intent.contract_observation.CopyFrom(observation)
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
    evaluation = execution_pb2.ContractEvaluation(
        evaluation_id="eval-1",
        contract_id=contract.contract_id,
        clause_kind=execution_pb2.CONTRACT_CLAUSE_KIND_POLICY_LAG,
        clause_id="lag-bound",
        status=execution_pb2.CONTRACT_EVALUATION_STATUS_SATISFIED,
        predicate=execution_pb2.Condition(
            condition_id="predicate-1",
            operator=execution_pb2.CONDITION_OPERATOR_LE,
            fact_path="sample.policy_lag",
            comparison_fact_path="contract.max_policy_lag",
            operands=[semantic_pb2.SemanticValue(uint64_value=5)],
        ),
        observations=[
            semantic_pb2.SemanticField(
                key="sample.policy_lag",
                value=semantic_pb2.SemanticValue(uint64_value=4),
            )
        ],
        evidence=[
            semantic_pb2.SemanticField(
                key="accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=11),
            )
        ],
        recommended_action=execution_pb2.CONTRACT_DECISION_ACTION_ALLOW,
        detail="roundtrip coverage",
    )
    decision = scheduling_pb2.DecisionRecord(
        decision_id="decision-1",
        execution_id=intent.execution_id,
        stage_id=intent.stage_id,
        tick_kind=scheduling_pb2.TICK_KIND_FAST,
        evaluation_context=scheduling_pb2.EvaluationContext(
            tick_kind=scheduling_pb2.TICK_KIND_FAST,
            evaluation_time=observation.observed_at,
            decision_sequence=7,
            cause="roundtrip",
            observed_revision=21,
        ),
        contract_evaluations=[evaluation],
    )
    trace_event = trace_pb2.TraceEvent(
        event_id=observation.event_id,
        execution_id=intent.execution_id,
        phase_id=observation.phase_id,
        event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
        contract_observation=observation,
    )
    replay_step = experiment_pb2.ReplayStepArtifact(
        ordinal=1,
        event=trace_event,
        intent=intent,
        recorded_decision=decision,
        evaluation_context=scheduling_pb2.EvaluationContext(
            tick_kind=scheduling_pb2.TICK_KIND_MEDIUM,
            evaluation_time=observation.oldest_sample_at,
            decision_sequence=8,
            cause="replay-step",
            observed_revision=22,
        ),
    )
    assert control_pb2.JobRun().DESCRIPTOR.full_name == "tgsrl.v1.JobRun"
    assert runtime_pb2.RuntimeManifest().DESCRIPTOR.full_name == "tgsrl.v1.RuntimeManifest"
    assert experiment_pb2.Experiment().DESCRIPTOR.full_name == "tgsrl.v1.Experiment"
    assert operator_pb2.ApplyRuntimeControlRequest().DESCRIPTOR.full_name == (
        "tgsrl.v1.ApplyRuntimeControlRequest"
    )
    assert runtime_pb2.WatchRuntimeEventsResponse(sequence=1).sequence == 1
    assert intent.contract_observation.buffer_level == 16
    assert decision.contract_evaluations[0].predicate.fact_path == "sample.policy_lag"
    assert (
        decision.contract_evaluations[0].predicate.comparison_fact_path == "contract.max_policy_lag"
    )
    assert trace_event.contract_observation.policy_version == "roundtrip-policy"
    assert replay_step.evaluation_context.tick_kind == scheduling_pb2.TICK_KIND_MEDIUM
    print("Go <-> Python SchedulingIntent round-trip passed")


if __name__ == "__main__":
    main()
