"""Verify Python -> Go -> Python binary compatibility for scheduling DTOs."""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

from google.protobuf import duration_pb2
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


def _encode_varint(value: int) -> bytes:
    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _frame(payload: bytes) -> bytes:
    return _encode_varint(len(payload)) + payload


def _read_frame(stream: BytesIO) -> bytes:
    length = 0
    shift = 0
    while True:
        byte = stream.read(1)
        if not byte:
            raise SystemExit("truncated round-trip frame")
        length |= (byte[0] & 0x7F) << shift
        if byte[0] < 0x80:
            break
        shift += 7
    payload = stream.read(length)
    if len(payload) != length:
        raise SystemExit("truncated round-trip payload")
    return payload


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
        fact_observations=[
            execution_pb2.ObservedFact(
                fact=semantic_pb2.SemanticField(
                    key="sample.policy_lag",
                    value=semantic_pb2.SemanticValue(uint64_value=4),
                ),
                source="roundtrip-runtime",
                revision=41,
            )
        ],
        component_versions=[
            execution_pb2.ComponentVersion(
                kind=execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND,
                name="execution-backend-primary",
                version="1.2.3-rc.1",
                source="roundtrip-registry",
                revision=42,
                attributes={"region": "test"},
            )
        ],
    )
    observation.observed_at.FromDatetime(datetime(2026, 8, 27, tzinfo=UTC))
    observation.oldest_sample_at.FromDatetime(datetime(2026, 8, 27, 0, 0, 1, tzinfo=UTC))
    observation.backpressure_started_at.FromDatetime(datetime(2026, 8, 27, 0, 0, 2, tzinfo=UTC))
    observation.fact_observations[0].observed_at.CopyFrom(observation.observed_at)
    observation.component_versions[0].observed_at.CopyFrom(observation.observed_at)
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
            component_versions=[observation.component_versions[0]],
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
    version_constraint = intent.execution_contract.version_constraints[0]
    version_constraint.allow_prerelease = True
    version_constraint.source = "roundtrip-registry"
    version_constraint.revision = 42
    version_constraint.component_kind = execution_pb2.COMPONENT_KIND_PROTOCOL
    version_constraint.observation_policy.CopyFrom(
        execution_pb2.ObservationPolicy(
            maximum_age=duration_pb2.Duration(seconds=30),
            missing=execution_pb2.OBSERVATION_DISPOSITION_BLOCK,
            stale=execution_pb2.OBSERVATION_DISPOSITION_HOLD,
        )
    )
    intent.execution_contract.critical_fact_policies.add(
        fact_path="sample.policy_lag",
        observation_policy=execution_pb2.ObservationPolicy(
            maximum_age=duration_pb2.Duration(seconds=10),
            missing=execution_pb2.OBSERVATION_DISPOSITION_DEGRADE,
            stale=execution_pb2.OBSERVATION_DISPOSITION_NOT_APPLICABLE,
        ),
    )
    plan = scheduling_pb2.PlacementPlan(
        plan_id="roundtrip-plan",
        execution_id=intent.execution_id,
        stage_id=intent.stage_id,
        intent_version=intent.version,
        snapshot_revision=21,
        purpose=scheduling_pb2.PLAN_PURPOSE_RECONCILIATION,
        rollback_policy=scheduling_pb2.ROLLBACK_POLICY_NOT_REQUIRED,
        capability_requirements=[
            scheduling_pb2.CapabilityRequirement(
                kind=scheduling_pb2.CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY,
                name="checkpoint",
                min_version="1.0.0",
                required=False,
            ),
        ],
        affected_allocation_ids=["allocation-a"],
        actions=[
            scheduling_pb2.Action(
                action_id="roundtrip-action",
                action_type=scheduling_pb2.ACTION_TYPE_BIND,
                level=scheduling_pb2.ACTION_LEVEL_L1,
                plan_id="roundtrip-plan",
                expected_snapshot_revision=21,
                tick_kind=scheduling_pb2.TICK_KIND_FAST,
                target_id="roundtrip-unit",
                preconditions=[scheduling_pb2.ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH],
                expected_impacts=[
                    scheduling_pb2.EXPECTED_IMPACT_ALLOCATION_CREATED,
                    scheduling_pb2.EXPECTED_IMPACT_CAPACITY_RESERVED,
                ],
                rollback=scheduling_pb2.Rollback(
                    action_type=scheduling_pb2.ACTION_TYPE_RELEASE,
                    target_id="roundtrip-binding",
                ),
            )
        ],
    )
    evaluation = execution_pb2.ContractEvaluation(
        evaluation_id="eval-1",
        contract_id=contract.contract_id,
        clause_kind=execution_pb2.CONTRACT_CLAUSE_KIND_POLICY_LAG,
        clause_id="lag-bound",
        status=execution_pb2.CONTRACT_EVALUATION_STATUS_INDETERMINATE,
        observation_disposition=execution_pb2.OBSERVATION_DISPOSITION_HOLD,
        recommended_action=execution_pb2.CONTRACT_DECISION_ACTION_PAUSE_REQUIRED,
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
    manifest = runtime_pb2.RuntimeManifest(
        manifest_id="roundtrip-manifest",
        run_id=intent.run_id,
        job_id=intent.job_id,
        trace_id=intent.trace_id,
        framework="torch",
        execution_backend="execution-backend-primary",
        trainer="trainer-primary",
        rollout_engine="rollout-primary",
        component_versions=[observation.component_versions[0]],
    )
    original_intent = intent.SerializeToString(deterministic=True)
    original_plan = plan.SerializeToString(deterministic=True)
    original_decision = decision.SerializeToString(deterministic=True)
    original_manifest = manifest.SerializeToString(deterministic=True)
    completed = subprocess.run(
        ["go", "run", "./scripts/proto-roundtrip-go.go"],
        cwd=root,
        input=(
            _frame(original_intent)
            + _frame(original_plan)
            + _frame(original_decision)
            + _frame(original_manifest)
        ),
        stdout=subprocess.PIPE,
        check=True,
    )
    output = BytesIO(completed.stdout)
    intent_output = _read_frame(output)
    plan_output = _read_frame(output)
    decision_output = _read_frame(output)
    manifest_output = _read_frame(output)
    if output.read():
        raise SystemExit("unexpected trailing round-trip bytes")
    decoded = scheduling_pb2.SchedulingIntent.FromString(intent_output)
    decoded_plan = scheduling_pb2.PlacementPlan.FromString(plan_output)
    decoded_decision = scheduling_pb2.DecisionRecord.FromString(decision_output)
    decoded_manifest = runtime_pb2.RuntimeManifest.FromString(manifest_output)
    if decoded != intent:
        raise SystemExit("Go round-trip changed SchedulingIntent semantics")
    if intent_output != original_intent:
        raise SystemExit("Go round-trip changed deterministic wire bytes")
    if decoded_plan != plan or plan_output != original_plan:
        raise SystemExit("Go round-trip changed PlacementPlan semantics or wire bytes")
    if decoded_decision != decision or decision_output != original_decision:
        raise SystemExit("Go round-trip changed DecisionRecord semantics or wire bytes")
    if decoded_manifest != manifest or manifest_output != original_manifest:
        raise SystemExit("Go round-trip changed RuntimeManifest semantics or wire bytes")
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
        observation_disposition=execution_pb2.OBSERVATION_DISPOSITION_DEGRADE,
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
    assert intent.contract_observation.fact_observations[0].revision == 41
    assert intent.contract_observation.component_versions[0].attributes["region"] == "test"
    assert intent.required_capabilities.component_versions[0].revision == 42
    assert manifest.component_versions[0].name == "execution-backend-primary"
    assert intent.execution_contract.version_constraints[0].observation_policy.stale == (
        execution_pb2.OBSERVATION_DISPOSITION_HOLD
    )
    assert intent.execution_contract.critical_fact_policies[0].observation_policy.missing == (
        execution_pb2.OBSERVATION_DISPOSITION_DEGRADE
    )
    assert decision.contract_evaluations[0].predicate.fact_path == "sample.policy_lag"
    assert (
        decision.contract_evaluations[0].predicate.comparison_fact_path == "contract.max_policy_lag"
    )
    assert trace_event.contract_observation.policy_version == "roundtrip-policy"
    assert replay_step.evaluation_context.tick_kind == scheduling_pb2.TICK_KIND_MEDIUM
    print("Go <-> Python scheduling DTO round-trip passed")


if __name__ == "__main__":
    main()
