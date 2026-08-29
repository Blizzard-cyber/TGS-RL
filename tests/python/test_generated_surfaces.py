"""Smoke tests for newly generated proto surfaces."""

from typing import Any

from google.protobuf import descriptor as descriptor_mod
from google.protobuf import duration_pb2, timestamp_pb2
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
    observation_policy = execution_pb2.ObservationPolicy(
        maximum_age=duration_pb2.Duration(seconds=30),
        missing=execution_pb2.OBSERVATION_DISPOSITION_BLOCK,
        stale=execution_pb2.OBSERVATION_DISPOSITION_HOLD,
    )
    observed_fact = execution_pb2.ObservedFact(
        fact=semantic_pb2.SemanticField(
            key="sample.policy_lag",
            value=semantic_pb2.SemanticValue(uint64_value=4),
        ),
        observed_at=timestamp_pb2.Timestamp(seconds=1),
        source="runtime",
        revision=7,
    )
    component_version = execution_pb2.ComponentVersion(
        kind=execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND,
        name="execution-backend-primary",
        version="1.2.3-rc.1",
        observed_at=timestamp_pb2.Timestamp(seconds=1),
        source="runtime-registry",
        revision=9,
        attributes={"region": "test"},
    )
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
        fact_observations=[observed_fact],
        component_versions=[component_version],
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
        observation_disposition=execution_pb2.OBSERVATION_DISPOSITION_DEGRADE,
        detail="within policy lag threshold",
    )
    contract = execution_pb2.ExecutionContract(
        version_constraints=[
            execution_pb2.VersionConstraint(
                component="execution-backend-primary",
                operator=execution_pb2.VERSION_OPERATOR_COMPATIBLE,
                version="1.2.0",
                allow_prerelease=True,
                source="runtime-registry",
                revision=9,
                component_kind=execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND,
                observation_policy=observation_policy,
            )
        ],
        critical_fact_policies=[
            execution_pb2.CriticalFactPolicy(
                fact_path="sample.policy_lag", observation_policy=observation_policy
            )
        ],
    )

    intent = scheduling_pb2.SchedulingIntent(
        execution_id="execution-1",
        stage_id="decode",
        version=1,
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
        execution_contract=contract,
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
    assert intent.contract_observation.fact_observations[0].revision == 7
    assert intent.contract_observation.component_versions[0].kind == (
        execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND
    )
    assert intent.execution_contract.version_constraints[0].observation_policy == (
        observation_policy
    )
    assert intent.execution_contract.critical_fact_policies[0].fact_path == ("sample.policy_lag")

    evidence = resource_pb2.CapabilityEvidence(
        evidence_id="cap-1",
        source="mock",
        revision=1,
        collector="unit-test",
    )
    capabilities = resource_pb2.CapabilitySet(
        evidence=[evidence], component_versions=[component_version]
    )
    intent.required_capabilities.CopyFrom(capabilities)
    assert capabilities.evidence[0].collector == "unit-test"
    assert intent.required_capabilities.component_versions[0].name == ("execution-backend-primary")

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
        component_versions=[component_version],
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
    action = scheduling_pb2.Action(
        action_id="action-1",
        action_type=scheduling_pb2.ACTION_TYPE_BIND,
        level=scheduling_pb2.ACTION_LEVEL_L1,
        plan_id="plan-1",
        expected_snapshot_revision=17,
        preconditions=[scheduling_pb2.ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH],
        expected_impacts=[
            scheduling_pb2.EXPECTED_IMPACT_ALLOCATION_CREATED,
            scheduling_pb2.EXPECTED_IMPACT_CAPACITY_RESERVED,
        ],
    )
    plan = scheduling_pb2.PlacementPlan(
        plan_id="plan-1",
        purpose=scheduling_pb2.PLAN_PURPOSE_ADMISSION,
        rollback_policy=scheduling_pb2.ROLLBACK_POLICY_REQUIRED_COMPENSATION,
        capability_requirements=[
            scheduling_pb2.CapabilityRequirement(
                kind=scheduling_pb2.CAPABILITY_REQUIREMENT_KIND_PROVIDER_CAPABILITY,
                name="checkpoint",
                min_version="1.0.0",
                required=False,
            )
        ],
        affected_allocation_ids=["allocation-1"],
        actions=[action],
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
    assert manifest.component_versions[0].kind == (execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND)
    assert experiment.display_name == "contract smoke"
    assert control.action == control_pb2.JOB_COMMAND_TYPE_PAUSE
    assert control.targets[0].expected_generation == 3
    assert runtime_watch.sequence == 9
    assert evaluation.predicate.fact_path == "sample.policy_lag"
    assert evaluation.predicate.operands[0].uint64_value == 5
    assert scheduling_pb2.PlacementPlan.DESCRIPTOR.full_name == "tgsrl.v1.PlacementPlan"
    assert plan.purpose == scheduling_pb2.PLAN_PURPOSE_ADMISSION
    assert plan.rollback_policy == scheduling_pb2.ROLLBACK_POLICY_REQUIRED_COMPENSATION
    assert not plan.capability_requirements[0].required
    assert plan.capability_requirements[0].name == "checkpoint"
    assert plan.capability_requirements[0].min_version == "1.0.0"
    assert plan.affected_allocation_ids == ["allocation-1"]
    assert plan.actions[0].preconditions == [
        scheduling_pb2.ACTION_PRECONDITION_SNAPSHOT_REVISION_MATCH
    ]
    assert plan.actions[0].expected_impacts == [
        scheduling_pb2.EXPECTED_IMPACT_ALLOCATION_CREATED,
        scheduling_pb2.EXPECTED_IMPACT_CAPACITY_RESERVED,
    ]
    assert record.contract_evaluations[0].recommended_action == (
        execution_pb2.CONTRACT_DECISION_ACTION_ALLOW
    )
    assert record.contract_evaluations[0].observation_disposition == (
        execution_pb2.OBSERVATION_DISPOSITION_DEGRADE
    )
    assert trace_event.contract_observation.event_id == "event-1"
    assert replay_step.evaluation_context.tick_kind == scheduling_pb2.TICK_KIND_MEDIUM


def test_pr2_execution_descriptors_match_wire_contract() -> None:
    repeated = descriptor_mod.FieldDescriptor.LABEL_REPEATED
    optional = descriptor_mod.FieldDescriptor.LABEL_OPTIONAL
    message = descriptor_mod.FieldDescriptor.TYPE_MESSAGE
    enum = descriptor_mod.FieldDescriptor.TYPE_ENUM

    def assert_field(
        message_type: Any,
        name: str,
        number: int,
        label: int,
        field_type: int,
        type_name: str | None = None,
    ) -> None:
        field = message_type.DESCRIPTOR.fields_by_name[name]
        assert (field.number, field.label, field.type) == (number, label, field_type)
        if type_name is not None:
            target = field.message_type or field.enum_type
            assert target.full_name == type_name

    assert_field(
        execution_pb2.ObservationPolicy,
        "maximum_age",
        1,
        optional,
        message,
        "google.protobuf.Duration",
    )
    assert_field(
        execution_pb2.ObservationPolicy,
        "missing",
        2,
        optional,
        enum,
        "tgsrl.v1.ObservationDisposition",
    )
    assert_field(
        execution_pb2.ObservationPolicy,
        "stale",
        3,
        optional,
        enum,
        "tgsrl.v1.ObservationDisposition",
    )
    assert_field(execution_pb2.ObservedFact, "fact", 1, optional, message, "tgsrl.v1.SemanticField")
    assert_field(
        execution_pb2.ObservedFact, "observed_at", 2, optional, message, "google.protobuf.Timestamp"
    )
    assert_field(
        execution_pb2.ObservedFact,
        "source",
        3,
        optional,
        descriptor_mod.FieldDescriptor.TYPE_STRING,
    )
    assert_field(
        execution_pb2.ObservedFact,
        "revision",
        4,
        optional,
        descriptor_mod.FieldDescriptor.TYPE_UINT64,
    )
    assert_field(
        execution_pb2.ComponentVersion, "kind", 1, optional, enum, "tgsrl.v1.ComponentKind"
    )
    for name, number, field_type in (
        ("name", 2, descriptor_mod.FieldDescriptor.TYPE_STRING),
        ("version", 3, descriptor_mod.FieldDescriptor.TYPE_STRING),
        ("source", 5, descriptor_mod.FieldDescriptor.TYPE_STRING),
        ("revision", 6, descriptor_mod.FieldDescriptor.TYPE_UINT64),
    ):
        assert_field(execution_pb2.ComponentVersion, name, number, optional, field_type)
    assert_field(
        execution_pb2.ComponentVersion,
        "observed_at",
        4,
        optional,
        message,
        "google.protobuf.Timestamp",
    )
    assert_field(
        execution_pb2.ComponentVersion,
        "attributes",
        7,
        repeated,
        message,
        "tgsrl.v1.ComponentVersion.AttributesEntry",
    )
    assert_field(
        execution_pb2.VersionConstraint,
        "component_kind",
        7,
        optional,
        enum,
        "tgsrl.v1.ComponentKind",
    )
    assert_field(
        execution_pb2.VersionConstraint,
        "observation_policy",
        8,
        optional,
        message,
        "tgsrl.v1.ObservationPolicy",
    )
    assert_field(
        execution_pb2.ContractObservation,
        "fact_observations",
        18,
        repeated,
        message,
        "tgsrl.v1.ObservedFact",
    )
    assert_field(
        execution_pb2.ContractObservation,
        "component_versions",
        19,
        repeated,
        message,
        "tgsrl.v1.ComponentVersion",
    )
    assert_field(
        execution_pb2.ContractEvaluation,
        "observation_disposition",
        13,
        optional,
        enum,
        "tgsrl.v1.ObservationDisposition",
    )
    assert_field(
        execution_pb2.ExecutionContract,
        "critical_fact_policies",
        11,
        repeated,
        message,
        "tgsrl.v1.CriticalFactPolicy",
    )
    assert_field(
        execution_pb2.CriticalFactPolicy,
        "fact_path",
        1,
        optional,
        descriptor_mod.FieldDescriptor.TYPE_STRING,
    )
    assert_field(
        execution_pb2.CriticalFactPolicy,
        "observation_policy",
        2,
        optional,
        message,
        "tgsrl.v1.ObservationPolicy",
    )
    assert_field(
        resource_pb2.CapabilitySet,
        "component_versions",
        11,
        repeated,
        message,
        "tgsrl.v1.ComponentVersion",
    )
    assert_field(
        runtime_pb2.RuntimeManifest,
        "component_versions",
        29,
        repeated,
        message,
        "tgsrl.v1.ComponentVersion",
    )

    assert execution_pb2.ObservationDisposition.items() == [
        ("OBSERVATION_DISPOSITION_UNKNOWN", 0),
        ("OBSERVATION_DISPOSITION_BLOCK", 1),
        ("OBSERVATION_DISPOSITION_HOLD", 2),
        ("OBSERVATION_DISPOSITION_DEGRADE", 3),
        ("OBSERVATION_DISPOSITION_NOT_APPLICABLE", 4),
    ]
    assert execution_pb2.ComponentKind.items() == [
        ("COMPONENT_KIND_UNKNOWN", 0),
        ("COMPONENT_KIND_PROTOCOL", 1),
        ("COMPONENT_KIND_SCHEDULER", 2),
        ("COMPONENT_KIND_RUNTIME", 3),
        ("COMPONENT_KIND_OPERATOR", 4),
        ("COMPONENT_KIND_PROVIDER", 5),
        ("COMPONENT_KIND_FRAMEWORK_ADAPTER", 6),
        ("COMPONENT_KIND_ROLLOUT_ENGINE", 7),
        ("COMPONENT_KIND_TRAINER", 8),
        ("COMPONENT_KIND_CUDA_DRIVER", 9),
        ("COMPONENT_KIND_EXECUTION_BACKEND", 10),
    ]
