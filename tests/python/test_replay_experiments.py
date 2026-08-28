"""Exact scheduler replay and experiment execution contract tests."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import pytest
from google.protobuf import timestamp_pb2
from google.protobuf.message import Message
from tgsrl.v1 import (
    execution_pb2,
    experiment_pb2,
    resource_pb2,
    scheduling_pb2,
    semantic_pb2,
    trace_pb2,
)
from tgsrl_runtime.experiments import ExperimentCoordinator
from tgsrl_runtime.replay import (
    DECISION_CANONICALIZER_V1,
    REPLAY_ARTIFACT_SCHEMA_V1,
    DecisionCanonicalizer,
    DecisionStatus,
    ReplayArtifactStep,
    ReplayError,
    SchedulerReplayRunner,
    SeedMode,
    decode_decision_artifacts,
    decode_replay_steps,
    encode_replay_step_artifact,
    replay_step_digest,
)
from tgsrl_runtime.trace_ingest import TraceIngestor

EventFactory = Callable[..., trace_pb2.TraceEvent]


def _timestamp(seconds: int) -> timestamp_pb2.Timestamp:
    value = timestamp_pb2.Timestamp()
    value.FromDatetime(datetime(2025, 1, 1, 0, 0, seconds, tzinfo=UTC))
    return value


def _clone_intent(
    intent: scheduling_pb2.SchedulingIntent,
) -> scheduling_pb2.SchedulingIntent:
    clone = scheduling_pb2.SchedulingIntent()
    clone.CopyFrom(intent)
    return clone


def _clone_snapshot(
    snapshot: resource_pb2.ClusterSnapshot,
) -> resource_pb2.ClusterSnapshot:
    clone = resource_pb2.ClusterSnapshot()
    clone.CopyFrom(snapshot)
    return clone


def _clone_decision(
    decision: scheduling_pb2.DecisionRecord,
) -> scheduling_pb2.DecisionRecord:
    clone = scheduling_pb2.DecisionRecord()
    clone.CopyFrom(decision)
    return clone


def _wire(message: Message) -> bytes:
    return message.SerializeToString(deterministic=True)


def _clone_message[MessageT: Message](message: MessageT) -> MessageT:
    clone = type(message)()
    clone.CopyFrom(message)
    return clone


def _artifact_digest(parts: Iterable[bytes]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return digest.hexdigest()


def _decision(
    ordinal: int,
    *,
    volatile: str = "recorded",
    score: float | None = None,
    fallback: bool = False,
    policy_version: str | None = None,
) -> scheduling_pb2.DecisionRecord:
    semantic_score = float(ordinal) if score is None else score
    plan = scheduling_pb2.PlacementPlan(
        plan_id=f"plan-{volatile}",
        execution_id=f"execution-{ordinal}",
        stage_id=f"stage-{ordinal}",
        intent_version=ordinal,
        snapshot_revision=100 + ordinal,
        created_at=_timestamp(1 + ordinal),
        expires_at=_timestamp(10 + ordinal),
        decision_id=f"nested-decision-{volatile}",
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
        generation=3,
        cursor=f"plan-cursor-{volatile}",
        job_id="job-1",
        bindings=[
            scheduling_pb2.Binding(
                binding_id=f"binding-{volatile}",
                pending_unit_id=f"unit-{ordinal}",
                device_ids=[f"cpu-{ordinal}"],
                resources=resource_pb2.ResourceVector(cpu_millis=1000),
                sandbox_id=f"sandbox-{volatile}",
                generation=3,
                runtime_unit_id=f"runtime-unit-{ordinal}",
            )
        ],
        actions=[
            scheduling_pb2.Action(
                action_id=f"action-{volatile}",
                action_type=scheduling_pb2.ACTION_TYPE_BIND,
                level=scheduling_pb2.ACTION_LEVEL_L1,
                target_id=f"unit-{ordinal}",
                order=1,
                plan_id=f"plan-{volatile}",
                sandbox_id=f"sandbox-{volatile}",
                expected_generation=3,
                expected_snapshot_revision=100 + ordinal,
                deadline=_timestamp(20 + ordinal),
                idempotency_key=f"idempotency-{volatile}",
            )
        ],
    )
    candidate_plan = scheduling_pb2.PlacementPlan()
    candidate_plan.CopyFrom(plan)
    observation = execution_pb2.ContractObservation(
        observed_at=_timestamp(ordinal),
        oldest_sample_at=_timestamp(max(0, ordinal - 1)),
        source="replay-test",
        event_id=f"event-{ordinal}",
        phase_id=f"stage-{ordinal}",
        policy_version=policy_version or f"policy-{ordinal}",
        accepted_samples=10 + ordinal,
        expected_samples=20 + ordinal,
    )
    evaluation = execution_pb2.ContractEvaluation(
        evaluation_id=f"evaluation-{ordinal}",
        contract_id="contract-1",
        clause_kind=execution_pb2.CONTRACT_CLAUSE_KIND_POLICY_LAG,
        clause_id=f"clause-{ordinal}",
        status=execution_pb2.CONTRACT_EVALUATION_STATUS_SATISFIED,
        predicate=execution_pb2.Condition(
            fact_path="sample.policy_lag",
            comparison_fact_path="contract.max_policy_lag",
        ),
        observations=[
            semantic_pb2.SemanticField(
                key="policy_lag",
                value=semantic_pb2.SemanticValue(uint64_value=ordinal),
            ),
            semantic_pb2.SemanticField(
                key="buffer_level",
                value=semantic_pb2.SemanticValue(uint64_value=10 + ordinal),
            ),
        ],
        evidence=[
            semantic_pb2.SemanticField(
                key="accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=10 + ordinal),
            ),
            semantic_pb2.SemanticField(
                key="expected_samples",
                value=semantic_pb2.SemanticValue(uint64_value=20 + ordinal),
            ),
        ],
        missing_keys=["contract.safe_point", "contract.latency_budget"],
        recommended_action=execution_pb2.CONTRACT_DECISION_ACTION_ALLOW,
        detail="stable deterministic evidence",
    )
    return scheduling_pb2.DecisionRecord(
        decision_id=f"decision-{volatile}",
        sequence=1000 + ordinal,
        execution_id=f"execution-{ordinal}",
        stage_id=f"stage-{ordinal}",
        intent_version=ordinal,
        snapshot_revision=100 + ordinal,
        candidates=[
            scheduling_pb2.PlacementCandidate(
                candidate_id=f"candidate-{ordinal}",
                plan=candidate_plan,
                score=semantic_score,
                component_scores={"balance": semantic_score},
            )
        ],
        selected_plan=plan,
        score=semantic_score,
        fallback=fallback,
        fallback_reason="capacity" if fallback else "",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
        code_revision="scheduler-code-v1",
        decided_at=_timestamp(30 + ordinal),
        policy_version=policy_version or f"policy-{ordinal}",
        deterministic_seed=7,
        config_revision="scheduler-config-v1",
        run_id="run-1",
        trace_id="trace-1",
        generation=3,
        cursor=f"decision-cursor-{volatile}",
        job_id="job-1",
        tick_kind=scheduling_pb2.TICK_KIND_FAST,
        evaluation_context=scheduling_pb2.EvaluationContext(
            tick_kind=scheduling_pb2.TICK_KIND_FAST,
            evaluation_time=_timestamp(30 + ordinal),
            decision_sequence=1000 + ordinal,
            cause="recorded-decision",
            observed_revision=100 + ordinal,
            contract_observation=observation,
        ),
        contract_evaluations=[evaluation],
    )


def _steps(
    event_factory: EventFactory,
    *,
    count: int = 3,
    seed: int = 7,
    recorded_decisions: Iterable[scheduling_pb2.DecisionRecord | None] | None = None,
) -> list[ReplayArtifactStep]:
    expected = (
        list(recorded_decisions)
        if recorded_decisions is not None
        else [_decision(ordinal) for ordinal in range(1, count + 1)]
    )
    steps: list[ReplayArtifactStep] = []
    for ordinal in range(1, count + 1):
        event = event_factory(
            f"event-{ordinal}", seconds=ordinal, sequence=ordinal, phase_id=f"stage-{ordinal}"
        )
        event.run_id = "run-1"
        event.trace_id = "trace-1"
        event.data_kind = trace_pb2.DATA_KIND_REPLAY
        event.generation = 3
        steps.append(
            ReplayArtifactStep(
                ordinal=ordinal,
                event=event,
                intent=scheduling_pb2.SchedulingIntent(
                    execution_id=f"execution-{ordinal}",
                    stage_id=f"stage-{ordinal}",
                    version=ordinal,
                    deterministic_seed=seed,
                    run_id="run-1",
                    trace_id="trace-1",
                    data_kind=trace_pb2.DATA_KIND_REPLAY,
                    policy_version=f"policy-{ordinal}",
                    job_id="job-1",
                ),
                snapshot=resource_pb2.ClusterSnapshot(
                    snapshot_id=f"snapshot-{ordinal}",
                    revision=100 + ordinal,
                    observed_at=_timestamp(ordinal),
                    run_id="run-1",
                    trace_id="trace-1",
                    data_kind=trace_pb2.DATA_KIND_REPLAY,
                ),
                evaluation_context=scheduling_pb2.EvaluationContext(
                    tick_kind=scheduling_pb2.TICK_KIND_FAST,
                    evaluation_time=_timestamp(ordinal),
                    decision_sequence=ordinal,
                    cause="recorded-step",
                    observed_revision=100 + ordinal,
                    contract_observation=execution_pb2.ContractObservation(
                        observed_at=_timestamp(ordinal),
                        source="event",
                        event_id=f"event-{ordinal}",
                        phase_id=f"stage-{ordinal}",
                        policy_version=f"policy-{ordinal}",
                    ),
                ),
                recorded_decision=expected[ordinal - 1],
            )
        )
    return steps


class RecordingScheduler:
    def __init__(
        self,
        decisions: Iterable[scheduling_pb2.DecisionRecord],
        *,
        fail_on_call: int | None = None,
        mutate_inputs: bool = False,
    ) -> None:
        self._decisions = list(decisions)
        self.fail_on_call = fail_on_call
        self.mutate_inputs = mutate_inputs
        self.calls: list[
            tuple[
                scheduling_pb2.SchedulingIntent,
                resource_pb2.ClusterSnapshot,
                scheduling_pb2.EvaluationContext | None,
            ]
        ] = []
        self.returned: list[scheduling_pb2.DecisionRecord] = []

    async def schedule(
        self,
        intent: scheduling_pb2.SchedulingIntent,
        snapshot: resource_pb2.ClusterSnapshot,
        evaluation_context: scheduling_pb2.EvaluationContext | None = None,
    ) -> scheduling_pb2.DecisionRecord:
        cloned_context = None
        if evaluation_context is not None:
            cloned_context = scheduling_pb2.EvaluationContext()
            cloned_context.CopyFrom(evaluation_context)
        self.calls.append((_clone_intent(intent), _clone_snapshot(snapshot), cloned_context))
        call_number = len(self.calls)
        if self.mutate_inputs:
            intent.labels["scheduler_mutation"] = "true"
            snapshot.annotations["scheduler_mutation"] = "true"
            if evaluation_context is not None:
                evaluation_context.cause = "mutated-by-scheduler"
        if self.fail_on_call == call_number:
            raise RuntimeError(f"scheduler failed on call {call_number}")
        returned = _clone_decision(self._decisions[call_number - 1])
        self.returned.append(returned)
        return returned


def _artifact_wire(steps: Iterable[ReplayArtifactStep]) -> list[tuple[bytes, ...]]:
    return [
        (
            _wire(step.event),
            _wire(step.intent),
            _wire(step.snapshot),
            _wire(step.recorded_decision) if step.recorded_decision is not None else b"",
        )
        for step in steps
    ]


def test_replay_step_artifacts_round_trip_and_reject_tampering(
    event_factory: EventFactory,
) -> None:
    steps = _steps(event_factory, count=2)
    replay = experiment_pb2.Replay(
        replay_id="persisted-replay",
        artifacts=[encode_replay_step_artifact("persisted-replay", step) for step in steps],
    )
    assert _artifact_wire(decode_replay_steps(replay)) == _artifact_wire(steps)

    replay.artifacts[0].replay_step.intent.stage_id = "tampered"
    with pytest.raises(ReplayError, match="digest"):
        decode_replay_steps(replay)


def test_replay_step_digest_changes_with_context_and_is_stable_for_permuted_evidence() -> None:
    step = _steps(
        lambda event_id, **_: trace_pb2.TraceEvent(event_id=event_id, occurred_at=_timestamp(1)),
        count=1,
    )[0]
    baseline = replay_step_digest(step)

    reordered = _clone_message(step)
    evaluation = reordered.recorded_decision.contract_evaluations[0]
    observations = list(evaluation.observations)
    evidence = list(evaluation.evidence)
    missing = list(evaluation.missing_keys)
    del evaluation.observations[:]
    del evaluation.evidence[:]
    del evaluation.missing_keys[:]
    evaluation.observations.extend(reversed(observations))
    evaluation.evidence.extend(reversed(evidence))
    evaluation.missing_keys.extend(reversed(missing))
    assert DecisionCanonicalizer().digest(step.recorded_decision) == DecisionCanonicalizer().digest(
        reordered.recorded_decision
    )

    changed = _clone_message(step)
    changed.evaluation_context.cause = "different-cause"

    assert replay_step_digest(reordered) != baseline
    assert replay_step_digest(changed) != baseline


def test_decision_canonicalizer_ignores_recursive_volatile_identity_and_time() -> None:
    recorded = _decision(1, volatile="recorded")
    replayed = _decision(1, volatile="replayed")
    replayed.sequence = 9999
    replayed.decided_at.CopyFrom(_timestamp(59))
    replayed.selected_plan.created_at.CopyFrom(_timestamp(40))
    replayed.selected_plan.expires_at.CopyFrom(_timestamp(50))
    replayed.candidates[0].plan.created_at.CopyFrom(_timestamp(41))
    replayed.candidates[0].plan.actions[0].deadline.CopyFrom(_timestamp(42))

    canonicalizer = DecisionCanonicalizer()
    comparison = canonicalizer.compare(recorded, replayed)

    assert canonicalizer.semantic_bytes(recorded) == canonicalizer.semantic_bytes(replayed)
    assert canonicalizer.digest(recorded) == canonicalizer.digest(replayed)
    assert comparison.status is DecisionStatus.EQUIVALENT
    assert comparison.equivalent
    assert comparison.expected_digest == comparison.actual_digest


def test_decision_canonicalizer_v2_ignores_contract_evaluation_ordering() -> None:
    recorded = _decision(1, volatile="recorded")
    replayed = _clone_decision(recorded)
    evaluation = replayed.contract_evaluations[0]
    observations = list(evaluation.observations)
    evidence = list(evaluation.evidence)
    missing = list(evaluation.missing_keys)
    del evaluation.observations[:]
    del evaluation.evidence[:]
    del evaluation.missing_keys[:]
    evaluation.observations.extend(reversed(observations))
    evaluation.evidence.extend(reversed(evidence))
    evaluation.missing_keys.extend(reversed(missing))

    comparison = DecisionCanonicalizer().compare(recorded, replayed)

    assert comparison.status is DecisionStatus.EQUIVALENT
    assert comparison.expected_digest == comparison.actual_digest


@pytest.mark.parametrize("semantic_change", ["score", "device", "action", "policy"])
def test_decision_canonicalizer_detects_semantic_differences(
    semantic_change: str,
) -> None:
    expected = _decision(1)
    actual = _clone_decision(expected)
    if semantic_change == "score":
        actual.score += 1.0
    elif semantic_change == "device":
        actual.selected_plan.bindings[0].device_ids[0] = "other-cpu"
    elif semantic_change == "action":
        actual.selected_plan.actions[0].action_type = scheduling_pb2.ACTION_TYPE_RELEASE
    else:
        actual.policy_version = "other-policy"

    comparison = DecisionCanonicalizer().compare(expected, actual)

    assert comparison.status is DecisionStatus.DIFFERENT
    assert not comparison.equivalent
    assert comparison.expected_digest != comparison.actual_digest


def test_decision_canonicalizer_reports_an_unrecorded_decision() -> None:
    actual = _decision(1)
    canonicalizer = DecisionCanonicalizer()

    comparison = canonicalizer.compare(None, actual)

    assert comparison.status is DecisionStatus.NOT_RECORDED
    assert not comparison.equivalent
    assert comparison.expected_digest is None
    assert comparison.actual_digest == canonicalizer.digest(actual)


@pytest.mark.asyncio
async def test_runner_schedules_every_exact_artifact_in_ordinal_order_and_compares(
    event_factory: EventFactory,
) -> None:
    actual = [_decision(1, volatile="actual-1"), _decision(2), _decision(3)]
    recorded = [
        _decision(1, volatile="recorded-1"),
        _decision(2, score=200.0),
        None,
    ]
    steps = _steps(event_factory, recorded_decisions=recorded)
    before = _artifact_wire(steps)
    scheduler = RecordingScheduler(actual, mutate_inputs=True)
    runner = SchedulerReplayRunner([steps[2], steps[0], steps[1]], scheduler=scheduler, seed=7)

    results = await runner.run()

    assert [call[0].stage_id for call in scheduler.calls] == [
        "stage-1",
        "stage-2",
        "stage-3",
    ]
    assert [call[1].snapshot_id for call in scheduler.calls] == [
        "snapshot-1",
        "snapshot-2",
        "snapshot-3",
    ]
    contexts = [cast(scheduling_pb2.EvaluationContext, call[2]) for call in scheduler.calls]
    assert all(context is not None for context in contexts)
    assert [context.cause for context in contexts] == [
        "recorded-step",
        "recorded-step",
        "recorded-step",
    ]
    assert [result.ordinal for result in results] == [1, 2, 3]
    assert [result.event_id for result in results] == [
        "event-1",
        "event-2",
        "event-3",
    ]
    assert [result.comparison.status for result in results] == [
        DecisionStatus.EQUIVALENT,
        DecisionStatus.DIFFERENT,
        DecisionStatus.NOT_RECORDED,
    ]
    assert [_wire(result.decision) for result in results] == [_wire(item) for item in actual]
    assert _artifact_wire(steps) == before
    scheduler.returned[0].score = 999.0
    assert results[0].decision.score == 1.0
    assert runner.remaining == 0
    assert await runner.step() is None


@pytest.mark.asyncio
async def test_recorded_seed_mode_preserves_seed_and_rejects_conflicts(
    event_factory: EventFactory,
) -> None:
    steps = _steps(event_factory, count=2, seed=11)
    scheduler = RecordingScheduler([_decision(1), _decision(2)])

    await SchedulerReplayRunner(
        steps, scheduler=scheduler, seed=11, seed_mode=SeedMode.RECORDED
    ).run()

    assert [call[0].deterministic_seed for call in scheduler.calls] == [11, 11]
    contexts = [cast(scheduling_pb2.EvaluationContext, call[2]) for call in scheduler.calls]
    assert all(context is not None for context in contexts)
    assert [context.decision_sequence for context in contexts] == [1, 2]
    conflicting = RecordingScheduler([_decision(1)])
    with pytest.raises(ReplayError, match="recorded intent seed conflicts"):
        await SchedulerReplayRunner(
            _steps(event_factory, count=1, seed=10),
            scheduler=conflicting,
            seed=11,
            seed_mode=SeedMode.RECORDED,
        ).run()
    assert conflicting.calls == []


@pytest.mark.asyncio
async def test_override_seed_mode_changes_only_the_scheduler_clone(
    event_factory: EventFactory,
) -> None:
    steps = _steps(event_factory, count=2, seed=3)
    before = _artifact_wire(steps)
    scheduler = RecordingScheduler([_decision(1), _decision(2)])

    await SchedulerReplayRunner(
        steps, scheduler=scheduler, seed=29, seed_mode=SeedMode.OVERRIDE
    ).run()

    assert [call[0].deterministic_seed for call in scheduler.calls] == [29, 29]
    assert _artifact_wire(steps) == before


@pytest.mark.asyncio
async def test_derived_seed_mode_is_stable_per_artifact_and_ordinal(
    event_factory: EventFactory,
) -> None:
    steps = _steps(event_factory, count=2, seed=3)

    async def derived_seeds(base_seed: int) -> list[int]:
        scheduler = RecordingScheduler([_decision(1), _decision(2)])
        await SchedulerReplayRunner(
            steps, scheduler=scheduler, seed=base_seed, seed_mode=SeedMode.DERIVE
        ).run()
        return [call[0].deterministic_seed for call in scheduler.calls]

    first = await derived_seeds(17)
    second = await derived_seeds(17)
    changed_base = await derived_seeds(18)

    assert first == second
    assert first[0] != first[1]
    assert changed_base != first
    assert [step.intent.deterministic_seed for step in steps] == [3, 3]


@pytest.mark.asyncio
async def test_checkpoint_resume_executes_only_remaining_steps_without_duplicates(
    event_factory: EventFactory,
) -> None:
    steps = _steps(event_factory)
    first_scheduler = RecordingScheduler([_decision(1), _decision(2), _decision(3)])
    first_runner = SchedulerReplayRunner(steps, scheduler=first_scheduler, seed=7)
    first_result = await first_runner.step()
    assert first_result is not None
    checkpoint = first_runner.checkpoint()

    resumed_scheduler = RecordingScheduler([_decision(2), _decision(3)])
    resumed = SchedulerReplayRunner(steps, scheduler=resumed_scheduler, seed=7)
    resumed.restore(checkpoint, [first_result])
    results = await resumed.run()

    assert [result.ordinal for result in results] == [1, 2, 3]
    assert [call[0].stage_id for call in first_scheduler.calls] == ["stage-1"]
    assert [call[0].stage_id for call in resumed_scheduler.calls] == [
        "stage-2",
        "stage-3",
    ]
    assert resumed.remaining == 0
    assert resumed.checkpoint().decision_count == 3


@pytest.mark.asyncio
async def test_checkpoint_rejects_tampered_decision_history(
    event_factory: EventFactory,
) -> None:
    steps = _steps(event_factory, count=2)
    source = SchedulerReplayRunner(
        steps, scheduler=RecordingScheduler([_decision(1), _decision(2)]), seed=7
    )
    result = await source.step()
    assert result is not None
    checkpoint = source.checkpoint()
    tampered_decision = _clone_decision(result.decision)
    tampered_decision.score += 1.0
    tampered = replace(result, decision=tampered_decision)
    restored = SchedulerReplayRunner(steps, scheduler=RecordingScheduler([_decision(2)]), seed=7)

    with pytest.raises(ReplayError, match="decision digest"):
        restored.restore(checkpoint, [tampered])

    assert restored.results == ()
    assert restored.remaining == 2


@pytest.mark.asyncio
async def test_checkpoint_rejects_changed_artifact_or_seed_configuration(
    event_factory: EventFactory,
) -> None:
    steps = _steps(event_factory, count=2)
    source = SchedulerReplayRunner(
        steps, scheduler=RecordingScheduler([_decision(1), _decision(2)]), seed=7
    )
    result = await source.step()
    assert result is not None
    checkpoint = source.checkpoint()
    changed_steps = _steps(event_factory, count=2)
    changed_steps[1].snapshot.revision += 1

    changed_artifact = SchedulerReplayRunner(
        changed_steps, scheduler=RecordingScheduler([_decision(2)]), seed=7
    )
    with pytest.raises(ReplayError, match="another replay artifact"):
        changed_artifact.restore(checkpoint, [result])

    changed_seed = SchedulerReplayRunner(
        steps,
        scheduler=RecordingScheduler([_decision(2)]),
        seed=8,
        seed_mode=SeedMode.OVERRIDE,
    )
    with pytest.raises(ReplayError, match="seed configuration"):
        changed_seed.restore(checkpoint, [result])


def test_decode_replay_steps_synthesizes_legacy_context_and_accepts_v1_digest(
    event_factory: EventFactory,
) -> None:
    step = _steps(event_factory, count=1)[0]
    step.event.contract_observation.CopyFrom(
        execution_pb2.ContractObservation(
            observed_at=_timestamp(1),
            source="event",
            event_id=step.event.event_id,
            phase_id=step.event.phase_id,
            policy_version="policy-1",
        )
    )
    legacy = experiment_pb2.ReplayArtifact(
        artifact_id="replay-1:step:1:legacy",
        replay_id="replay-1",
        kind="scheduler-replay-step",
        uri="inline://replays/replay-1/steps/1",
        digest=replay_step_digest(step, schema_version=REPLAY_ARTIFACT_SCHEMA_V1),
        observed_at=step.event.occurred_at,
        schema_version=REPLAY_ARTIFACT_SCHEMA_V1,
        replay_step=experiment_pb2.ReplayStepArtifact(
            ordinal=step.ordinal,
            event=step.event,
            intent=step.intent,
            snapshot=step.snapshot,
            recorded_decision=step.recorded_decision,
        ),
    )
    replay = experiment_pb2.Replay(replay_id="replay-1", artifacts=[legacy])

    decoded = decode_replay_steps(replay)[0]

    assert decoded.evaluation_context.tick_kind == scheduling_pb2.TICK_KIND_FAST
    assert decoded.evaluation_context.cause == "replay-compat"
    assert decoded.evaluation_context.decision_sequence == step.recorded_decision.sequence
    assert decoded.evaluation_context.observed_revision == step.snapshot.revision
    assert decoded.evaluation_context.evaluation_time == step.recorded_decision.decided_at
    assert decoded.evaluation_context.compatibility_defaults_applied
    assert decoded.evaluation_context.contract_observation.event_id == step.event.event_id


def test_decode_decision_artifacts_accepts_v1_canonicalizer() -> None:
    decision = _decision(1)
    artifact = experiment_pb2.ReplayArtifact(
        artifact_id="replay-1:decision:1:legacy",
        replay_id="replay-1",
        kind="scheduler-decision",
        uri="inline://replays/replay-1/decisions/1",
        observed_at=_timestamp(1),
        schema_version=REPLAY_ARTIFACT_SCHEMA_V1,
        canonicalizer=DECISION_CANONICALIZER_V1,
        replay_decision=experiment_pb2.ReplayDecisionArtifact(
            ordinal=1,
            event_id="event-1",
            decision=decision,
            semantic_digest=DecisionCanonicalizer().digest(decision),
            expected_digest=DecisionCanonicalizer().digest(decision),
            comparison_status=experiment_pb2.REPLAY_DECISION_COMPARISON_STATUS_EQUIVALENT,
        ),
    )
    artifact.digest = _artifact_digest(
        [
            REPLAY_ARTIFACT_SCHEMA_V1.encode(),
            b"1",
            _wire(decision),
        ]
    )
    replay = experiment_pb2.Replay(replay_id="replay-1", artifacts=[artifact])

    decoded = decode_decision_artifacts(replay)

    assert [_wire(item) for item in decoded] == [_wire(decision)]


def _coordinator(
    event_factory: EventFactory, *, event_count: int = 2
) -> tuple[ExperimentCoordinator, list[ReplayArtifactStep]]:
    steps = _steps(event_factory, count=event_count)
    ingestor = TraceIngestor()
    ingestor.ingest("run-1", [step.event for step in steps])
    coordinator = ExperimentCoordinator(trace_ingestor=ingestor)
    coordinator.create_replay(
        experiment_pb2.Replay(
            replay_id="replay-1",
            run_id="run-1",
            trace_id="trace-1",
            source_trace_ref="memory://run-1",
            speed=1.0,
            seed=7,
            data_kind=trace_pb2.DATA_KIND_REPLAY,
        ),
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    coordinator.create_experiment(
        experiment_pb2.Experiment(experiment_id="experiment-1", display_name="replay comparison"),
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    return coordinator, steps


@pytest.mark.asyncio
async def test_execute_replay_runs_artifacts_and_persists_decisions_metrics_and_provenance(
    event_factory: EventFactory,
) -> None:
    recorded = [
        _decision(1, volatile="recorded"),
        _decision(2, score=200.0),
    ]
    coordinator, steps = _coordinator(event_factory)
    steps = _steps(event_factory, count=2, recorded_decisions=recorded)
    before = _artifact_wire(steps)
    actual = [
        _decision(1, volatile="actual"),
        _decision(2, fallback=True, policy_version="policy-final"),
    ]
    scheduler = RecordingScheduler(actual)
    times = iter(
        [
            datetime(2025, 1, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 1, 2, tzinfo=UTC),
        ]
    )

    experiment = await coordinator.execute_replay(
        experiment_id="experiment-1",
        replay_id="replay-1",
        steps=[steps[1], steps[0]],
        scheduler=scheduler,
        now=lambda: next(times),
        config_hash="config-sha256",
        code_revision="runtime-revision",
    )

    assert [call[0].stage_id for call in scheduler.calls] == ["stage-1", "stage-2"]
    assert _artifact_wire(steps) == before
    replay = coordinator.store.get_replay("replay-1")
    assert replay.state == experiment_pb2.REPLAY_STATE_COMPLETED
    assert replay.started_at.ToDatetime(tzinfo=UTC) == datetime(2025, 1, 1, 1, tzinfo=UTC)
    assert replay.completed_at.ToDatetime(tzinfo=UTC) == datetime(2025, 1, 1, 2, tzinfo=UTC)
    assert replay.applied_events == 2
    assert replay.emitted_decisions == 2
    assert replay.cursor == "replay:replay-1:2"
    metrics = {metric.name: metric.value for metric in replay.metrics}
    assert metrics["event_count"] == 2.0
    assert metrics["decision_count"] == 2.0
    assert metrics["fallback_count"] == 1.0
    assert metrics["equivalent_decision_count"] == 1.0

    stored_decisions = coordinator.store.get_decisions("replay-1")
    assert [decision.execution_id for decision in stored_decisions] == [
        "execution-1",
        "execution-2",
    ]
    assert [decision.decision_id for decision in stored_decisions] == [
        "decision-actual",
        "decision-recorded",
    ]
    assert [_wire(decision) for decision in stored_decisions] == [_wire(item) for item in actual]
    stored_decisions[0].score = 999.0
    assert coordinator.store.get_decisions("replay-1")[0].score == 1.0
    persisted_replay = coordinator.store.get_replay("replay-1")
    assert [_wire(item) for item in decode_decision_artifacts(persisted_replay)] == [
        _wire(item) for item in actual
    ]

    assert experiment.state == experiment_pb2.EXPERIMENT_STATE_COMPLETED
    assert experiment.completed_at.ToDatetime(tzinfo=UTC) == datetime(2025, 1, 1, 2, tzinfo=UTC)
    assert experiment.summary == "events=2 decisions=2 fallbacks=1"
    assert experiment.cursor == "experiment:experiment-1:1:1"
    assert len(experiment.runs) == len(experiment.results) == 1
    run = experiment.runs[0]
    result = experiment.results[0]
    assert run.experiment_run_id == "experiment-1:replay-1"
    assert run.experiment_id == experiment.experiment_id
    assert run.run_id == replay.run_id
    assert run.trace_id == replay.trace_id
    assert run.kind == experiment_pb2.EXPERIMENT_RUN_KIND_REPLAY
    assert run.data_kind == trace_pb2.DATA_KIND_REPLAY
    assert run.replay_id == replay.replay_id
    assert run.config_hash == "config-sha256"
    assert run.code_revision == "runtime-revision"
    assert run.policy_version == "policy-final"
    assert run.result_id == result.result_id
    assert run.replay_state == experiment_pb2.REPLAY_STATE_COMPLETED
    assert run.result_cursor == experiment.cursor
    assert result.result_id == "result:experiment-1:replay-1"
    assert result.experiment_id == experiment.experiment_id
    assert result.trace_id == replay.trace_id
    assert result.data_kind == trace_pb2.DATA_KIND_REPLAY
    assert result.summary == experiment.summary
    assert result.annotations["cursor"] == experiment.cursor
    assert {metric.name: metric.value for metric in result.metrics} == metrics


@pytest.mark.asyncio
async def test_execute_replay_persists_failure_and_does_not_publish_partial_result(
    event_factory: EventFactory,
) -> None:
    coordinator, steps = _coordinator(event_factory, event_count=3)
    scheduler = RecordingScheduler([_decision(1), _decision(2), _decision(3)], fail_on_call=2)
    times = iter(
        [
            datetime(2025, 1, 2, 1, tzinfo=UTC),
            datetime(2025, 1, 2, 2, tzinfo=UTC),
        ]
    )

    with pytest.raises(RuntimeError, match="scheduler failed on call 2"):
        await coordinator.execute_replay(
            experiment_id="experiment-1",
            replay_id="replay-1",
            steps=steps,
            scheduler=scheduler,
            now=lambda: next(times),
        )

    assert [call[0].stage_id for call in scheduler.calls] == ["stage-1", "stage-2"]
    replay = coordinator.store.get_replay("replay-1")
    experiment = coordinator.store.get_experiment("experiment-1")
    assert replay.state == experiment_pb2.REPLAY_STATE_FAILED
    assert experiment.state == experiment_pb2.EXPERIMENT_STATE_FAILED
    assert replay.applied_events == 1
    assert replay.emitted_decisions == 1
    assert replay.completed_at.ToDatetime(tzinfo=UTC) == datetime(2025, 1, 2, 2, tzinfo=UTC)
    assert experiment.completed_at == replay.completed_at
    assert replay.annotations["error"] == "RuntimeError: scheduler failed on call 2"
    assert experiment.annotations["error"] == replay.annotations["error"]
    assert coordinator.store.get_decisions("replay-1") == ()
    assert experiment.runs == []
    assert experiment.results == []


@pytest.mark.asyncio
async def test_execute_replay_rejects_empty_steps_and_terminal_replays(
    event_factory: EventFactory,
) -> None:
    coordinator, steps = _coordinator(event_factory, event_count=1)
    scheduler = RecordingScheduler([_decision(1)])

    with pytest.raises(ValueError, match="requires recorded artifact steps"):
        await coordinator.execute_replay(
            experiment_id="experiment-1",
            replay_id="replay-1",
            steps=[],
            scheduler=scheduler,
        )

    replay = coordinator.store.get_replay("replay-1")
    replay.state = experiment_pb2.REPLAY_STATE_COMPLETED
    coordinator.store.put_replay(replay)
    with pytest.raises(ValueError, match="terminal replay"):
        await coordinator.execute_replay(
            experiment_id="experiment-1",
            replay_id="replay-1",
            steps=steps,
            scheduler=scheduler,
        )
    assert scheduler.calls == []


@pytest.mark.asyncio
async def test_concurrent_replay_start_with_same_key_executes_once(
    event_factory: EventFactory,
) -> None:
    coordinator, steps = _coordinator(event_factory, event_count=1)
    coordinator.attach_replay_steps("replay-1", steps)
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingScheduler(RecordingScheduler):
        async def schedule(
            self,
            intent: scheduling_pb2.SchedulingIntent,
            snapshot: resource_pb2.ClusterSnapshot,
            evaluation_context: scheduling_pb2.EvaluationContext | None = None,
        ) -> scheduling_pb2.DecisionRecord:
            entered.set()
            await release.wait()
            return await super().schedule(intent, snapshot, evaluation_context=evaluation_context)

    scheduler = BlockingScheduler([_decision(1)])

    async def start() -> experiment_pb2.Replay:
        async with coordinator.lock_for_replay("replay-1"):
            replay = coordinator.store.get_replay("replay-1")
            if coordinator.command_is_idempotent(
                replay, experiment_pb2.REPLAY_COMMAND_TYPE_START, "same-start"
            ):
                return replay
            replay = await coordinator.start_replay("replay-1", scheduler=scheduler)
            return coordinator.commit_command(
                replay, experiment_pb2.REPLAY_COMMAND_TYPE_START, "same-start"
            )

    first = asyncio.create_task(start())
    await entered.wait()
    second = asyncio.create_task(start())
    await asyncio.sleep(0)
    assert not second.done()
    release.set()
    first_result, second_result = await asyncio.gather(first, second)

    assert first_result == second_result
    assert len(scheduler.calls) == 1
    assert first_result.state == experiment_pb2.REPLAY_STATE_COMPLETED
