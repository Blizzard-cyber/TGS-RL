"""Shared fixture builders for the in-memory gateway backend."""

from __future__ import annotations

from datetime import timedelta

from google.protobuf import duration_pb2
from tgsrl.v1 import execution_pb2, job_pb2, resource_pb2, trace_pb2

from tgsrl_gateway.protojson import timestamp_from_datetime


def build_phase_graph() -> execution_pb2.PhaseGraph:
    return execution_pb2.PhaseGraph(
        phases=[
            execution_pb2.Phase(
                phase_id="prefill",
                display_name="Prefill",
                kind=execution_pb2.PHASE_KIND_PREFILL,
                parallelism=1,
                max_attempts=1,
            ),
            execution_pb2.Phase(
                phase_id="decode",
                display_name="Decode",
                kind=execution_pb2.PHASE_KIND_DECODE,
                parallelism=1,
                max_attempts=1,
            ),
            execution_pb2.Phase(
                phase_id="optimizer",
                display_name="Optimizer",
                kind=execution_pb2.PHASE_KIND_OPTIMIZER,
                parallelism=1,
                max_attempts=1,
            ),
        ],
        edges=[
            execution_pb2.PhaseEdge(from_phase_id="prefill", to_phase_id="decode"),
            execution_pb2.PhaseEdge(from_phase_id="decode", to_phase_id="optimizer"),
        ],
        entry_phase_ids=["prefill"],
    )


def build_execution_contract() -> execution_pb2.ExecutionContract:
    commit_timeout = duration_pb2.Duration()
    commit_timeout.FromTimedelta(timedelta(seconds=30))
    stall_timeout = duration_pb2.Duration()
    stall_timeout.FromTimedelta(timedelta(seconds=20))
    return execution_pb2.ExecutionContract(
        contract_id="contract-sha256-demo",
        version="0.3.0",
        phase_graph=build_phase_graph(),
        validity_rules=[
            execution_pb2.ValidityRule(
                rule_id="rule-policy-lag",
                description="Policy lag stays bounded",
                expression="sample.policy_lag <= 32",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_REJECT,
            )
        ],
        version_constraints=[
            execution_pb2.VersionConstraint(
                component="protocol",
                operator=execution_pb2.VERSION_OPERATOR_COMPATIBLE,
                version="0.3.0",
            )
        ],
        commit_policy=execution_pb2.CommitPolicy(
            mode=execution_pb2.COMMIT_MODE_ALL_OR_NOTHING,
            minimum_successful_units=1,
            max_retries=1,
            commit_timeout=commit_timeout,
            require_safe_point=True,
        ),
        backpressure_policy=execution_pb2.BackpressurePolicy(
            mode=execution_pb2.BACKPRESSURE_MODE_BLOCK_PRODUCER,
            low_watermark=10,
            high_watermark=100,
            maximum_buffer_level=120,
            stall_timeout=stall_timeout,
        ),
        safe_point_policy=execution_pb2.SafePointPolicy(
            enabled=True,
            trigger=execution_pb2.SAFE_POINT_TRIGGER_PHASE_BOUNDARY,
            required_phase_ids=["decode"],
        ),
        capabilities=execution_pb2.Capabilities(
            elastic_parallelism=True,
            deterministic_replay=True,
            transactional_commits=True,
            checkpoint_restore=True,
            policy_hot_swap=True,
            extensions=["timeline", "topology"],
        ),
    )


def build_runtime_spec() -> job_pb2.FrameworkRuntimeSpec:
    return job_pb2.FrameworkRuntimeSpec(
        framework="pytorch",
        framework_version="2.5.1",
        execution_backend="local-fake",
        execution_backend_version="0.1.0",
        trainer="demo-trainer",
        trainer_version="0.1.0",
        rollout_engine="demo-rollout",
        rollout_engine_version="0.1.0",
        image_digest="sha256:1111111111111111111111111111111111111111111111111111111111111111",
        compatibility_profile="cpu-demo",
        command=["python", "train.py"],
    )


def build_resources() -> resource_pb2.ResourceVector:
    return resource_pb2.ResourceVector(
        cpu_millis=2000,
        memory_bytes=8 * 1024 * 1024 * 1024,
        accelerator_units=1.0,
        ephemeral_storage_bytes=10 * 1024 * 1024 * 1024,
        network_bandwidth_bps=1_000_000_000,
    )


def build_capability_set() -> resource_pb2.CapabilitySet:
    return resource_pb2.CapabilitySet(
        names=["checkpoint_restore", "deterministic_replay", "timeline", "topology"],
        algorithms=["ppo", "grpo"],
        rollout_modes=["sync", "partially_async", "fully_async"],
        source="gateway-python",
        revision=1,
        supported_actions=["start", "pause", "resume", "stop", "terminate"],
        limits={"max_runs_per_job": 8.0},
    )


def build_default_job(job_id: str, display_name: str) -> job_pb2.RLTrainingJob:
    return job_pb2.RLTrainingJob(
        job_id=job_id,
        display_name=display_name,
        protocol_version="v0.3",
        algorithm="ppo",
        runtime=build_runtime_spec(),
        execution_contract=build_execution_contract(),
        resources_per_unit=build_resources(),
        required_capabilities=build_capability_set(),
        desired_units=1,
        priority=10,
        queue="default",
        state=job_pb2.JOB_STATE_PENDING,
        created_at=timestamp_from_datetime(),
        labels={"owner": "gateway"},
        rollout_mode=trace_pb2.ROLLOUT_MODE_SYNC,
        model_ref="model-ref-demo",
        dataset_ref="dataset-ref-demo",
        policy_ref="policy-ref-demo",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
