"""Smoke tests for newly generated proto surfaces."""

from tgsrl.v1 import (
    control_pb2,
    experiment_pb2,
    operator_pb2,
    resource_pb2,
    runtime_pb2,
    scheduling_pb2,
    semantic_pb2,
    trace_pb2,
)


def test_generated_proto_surfaces_are_importable() -> None:
    intent = scheduling_pb2.SchedulingIntent(
        execution_id="execution-1",
        stage_id="decode",
        version=1,
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
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
