"""Smoke tests for newly generated proto surfaces."""

from google.protobuf import timestamp_pb2
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


def test_generated_proto_surfaces_are_importable() -> None:
    observation = execution_pb2.ContractObservation(
        source="runtime",
        event_id="event-1",
        phase_id="decode",
        policy_version="policy-7",
        policy_lag=4,
        sample_stale=False,
        buffer_level=12,
        effective_sample_size=8.0,
        accepted_samples=7,
        expected_samples=8,
        sample_count=8,
        effective_sample_size_ratio=1.0,
        safe_point=True,
        typed_facts=[
            semantic_pb2.SemanticField(
                key="sample.policy_lag",
                value=semantic_pb2.SemanticValue(uint64_value=4),
            ),
            semantic_pb2.SemanticField(
                key="sample.stale",
                value=semantic_pb2.SemanticValue(bool_value=False),
            ),
            semantic_pb2.SemanticField(
                key="buffer.level.current",
                value=semantic_pb2.SemanticValue(uint64_value=12),
            ),
            semantic_pb2.SemanticField(
                key="group.accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=7),
            ),
            semantic_pb2.SemanticField(
                key="group.expected_samples",
                value=semantic_pb2.SemanticValue(uint64_value=8),
            ),
            semantic_pb2.SemanticField(
                key="sample.sample_count",
                value=semantic_pb2.SemanticValue(uint64_value=8),
            ),
            semantic_pb2.SemanticField(
                key="batch.effective_sample_size",
                value=semantic_pb2.SemanticValue(double_value=8.0),
            ),
            semantic_pb2.SemanticField(
                key="batch.effective_sample_size_ratio",
                value=semantic_pb2.SemanticValue(double_value=1.0),
            ),
            semantic_pb2.SemanticField(
                key="runtime.safe_point",
                value=semantic_pb2.SemanticValue(bool_value=True),
            ),
        ],
    )
    observation.observed_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1))
    observation.oldest_sample_at.CopyFrom(timestamp_pb2.Timestamp(seconds=2))
    observation.backpressure_started_at.CopyFrom(timestamp_pb2.Timestamp(seconds=3))

    predicate = execution_pb2.Condition(
        condition_id="predicate-1",
        operator=execution_pb2.CONDITION_OPERATOR_LE,
        fact_path="sample.policy_lag",
        operands=[semantic_pb2.SemanticValue(uint64_value=5)],
    )
    evaluation = execution_pb2.ContractEvaluation(
        evaluation_id="eval-1",
        contract_id="contract-1",
        clause_kind=execution_pb2.CONTRACT_CLAUSE_KIND_POLICY_LAG,
        clause_id="lag-bound",
        status=execution_pb2.CONTRACT_EVALUATION_STATUS_SATISFIED,
        predicate=predicate,
        observations=[
            semantic_pb2.SemanticField(
                key="sample.policy_lag",
                value=semantic_pb2.SemanticValue(uint64_value=4),
            )
        ],
        evidence=[
            semantic_pb2.SemanticField(
                key="buffer.level",
                value=semantic_pb2.SemanticValue(uint64_value=12),
            )
        ],
        recommended_action=execution_pb2.CONTRACT_DECISION_ACTION_ALLOW,
        detail="within policy lag threshold",
    )

    intent = scheduling_pb2.SchedulingIntent(
        execution_id="execution-1",
        stage_id="decode",
        version=1,
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
        contract_observation=observation,
    )
    intent.semantic_context.CopyFrom(
        trace_pb2.SemanticEnvelope(
            envelope_id="env-1",
            schema_version="v1",
            type_name="tgsrl.v1.SchedulingIntent",
            payload=semantic_pb2.SemanticObject(
                fields=[
                    semantic_pb2.SemanticField(
                        key="kind",
                        value=semantic_pb2.SemanticValue(string_value="smoke"),
                    )
                ]
            ),
        )
    )
    assert intent.run_id == "run-1"
    assert intent.trace_id == "trace-1"
    assert intent.contract_observation.phase_id == "decode"
    assert intent.contract_observation.policy_lag == 4

    evidence = resource_pb2.CapabilityEvidence(
        evidence_id="cap-1",
        source="mock",
        revision=1,
        collector="unit-test",
    )
    capabilities = resource_pb2.CapabilitySet(evidence=[evidence])
    assert capabilities.evidence[0].collector == "unit-test"

    run = control_pb2.JobRun(
        run_id="run-1",
        job_id="job-1",
        random_seed=7,
        component_status=[
            control_pb2.ComponentStatus(
                component="runtime",
                observed_runtime_state=runtime_pb2.RUNTIME_STATE_RUNNING,
                converged=True,
            )
        ],
    )
    manifest = runtime_pb2.RuntimeManifest(
        manifest_id="manifest-1",
        run_id="run-1",
        command=["python", "-m", "trainer"],
        args=["--epochs", "1"],
        environment={"MODE": "test"},
        working_directory="/workspace/run",
    )
    experiment = experiment_pb2.Experiment(experiment_id="exp-1", display_name="contract smoke")
    record = scheduling_pb2.DecisionRecord(
        decision_id="decision-1",
        execution_id="execution-1",
        stage_id="decode",
        tick_kind=scheduling_pb2.TICK_KIND_FAST,
        evaluation_context=scheduling_pb2.EvaluationContext(
            tick_kind=scheduling_pb2.TICK_KIND_FAST,
            decision_sequence=9,
            cause="sample-arrival",
            observed_revision=17,
        ),
        contract_evaluations=[evaluation],
    )
    trace_event = trace_pb2.TraceEvent(
        event_id="event-1",
        execution_id="execution-1",
        phase_id="decode",
        event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
        contract_observation=observation,
    )
    replay_step = experiment_pb2.ReplayStepArtifact(
        ordinal=1,
        event=trace_event,
        intent=intent,
        recorded_decision=record,
        evaluation_context=scheduling_pb2.EvaluationContext(
            tick_kind=scheduling_pb2.TICK_KIND_MEDIUM,
            decision_sequence=10,
            cause="replay-step",
            observed_revision=18,
        ),
    )
    control = operator_pb2.ApplyRuntimeControlRequest(
        action=control_pb2.JOB_COMMAND_TYPE_PAUSE,
        job_id="job-1",
        run_id="run-1",
        trace_id="trace-1",
        targets=[
            operator_pb2.RuntimeControlTarget(
                runtime_unit_id="unit-1", sandbox_id="sandbox-1", expected_generation=3
            )
        ],
        request_id="req-1",
        idempotency_key="idem-1",
    )
    runtime_watch = runtime_pb2.WatchRuntimeEventsResponse(sequence=9)
    assert run.run_id == manifest.run_id
    assert run.random_seed == 7
    assert run.component_status[0].observed_runtime_state == runtime_pb2.RUNTIME_STATE_RUNNING
    assert run.component_status[0].converged
    assert manifest.command[0] == "python"
    assert manifest.environment["MODE"] == "test"
    assert experiment.display_name == "contract smoke"
    assert control.action == control_pb2.JOB_COMMAND_TYPE_PAUSE
    assert control.targets[0].expected_generation == 3
    assert runtime_watch.sequence == 9
    assert evaluation.predicate.fact_path == "sample.policy_lag"
    assert evaluation.predicate.operands[0].uint64_value == 5
    assert record.contract_evaluations[0].recommended_action == (
        execution_pb2.CONTRACT_DECISION_ACTION_ALLOW
    )
    assert trace_event.contract_observation.event_id == "event-1"
    assert replay_step.evaluation_context.tick_kind == scheduling_pb2.TICK_KIND_MEDIUM
