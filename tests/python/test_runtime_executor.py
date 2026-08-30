"""Tests for runtime execution supervision over adapter lifecycle contracts."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tgsrl.v1 import resource_pb2, runtime_pb2, trace_pb2
from tgsrl_runtime.execution_drivers import FakeCommandDriver
from tgsrl_runtime.executor import RuntimeExecutor

from adapters import (
    AdapterErrorKind,
    AdapterUnavailableError,
    CommandResult,
    GRPOAdapter,
    LifecycleAction,
    PartialAsyncRolloutAdapter,
    RuntimeAdapterRegistry,
    build_execution_contract,
)


def _manifest(
    *,
    framework: str = "fake",
    execution_backend: str = "fake",
    trainer: str = "fake",
    rollout_engine: str = "fake",
) -> runtime_pb2.RuntimeManifest:
    return runtime_pb2.RuntimeManifest(
        manifest_id="manifest-1",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        framework=framework,
        execution_backend=execution_backend,
        trainer=trainer,
        rollout_engine=rollout_engine,
        compatibility_profile="cpu-mock",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
        execution_contract=build_execution_contract(GRPOAdapter(), PartialAsyncRolloutAdapter()),
        annotations={"algorithm": "grpo", "policy_version": "policy-1"},
        resources_per_unit=resource_pb2.ResourceVector(cpu_millis=1000, memory_bytes=1 << 30),
        desired_units=2,
        priority=3,
        queue="default",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version="policy-1",
        command=["python"],
        args=["-m", "trainer"],
        environment={"MODE": "test"},
        working_directory="/workspace/run",
    )


def test_executor_fake_lifecycle_and_idempotency() -> None:
    command_driver = FakeCommandDriver()
    executor = RuntimeExecutor(
        registry=RuntimeAdapterRegistry(),
        command_driver=command_driver,
        default_timeout_seconds=5.0,
    )
    manifest, runtime_units = executor.register_runtime(_manifest())

    assert manifest.run_id == "run-1"
    assert runtime_units

    prepared = executor.prepare("run-1", idempotency_key="prepare-1")
    assert prepared.ok
    assert prepared.runtime_units
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in prepared.runtime_units)
    prepared_env = dict(command_driver.calls[0][1])
    assert prepared_env["TGSRL_GENERATION"] == "0"
    assert prepared_env["TGSRL_IDEMPOTENCY_KEY"] == "prepare-1"

    started = executor.start("run-1", idempotency_key="start-1")
    assert started.ok
    assert started.action is LifecycleAction.LAUNCH
    assert started.generation == 1
    assert all(unit.generation == 1 for unit in started.runtime_units)
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in started.runtime_units)
    assert all(unit.status_reason == "starting" for unit in started.runtime_units)

    started_again = executor.start("run-1", idempotency_key="start-1")
    assert started_again.idempotent
    assert started_again.cursor == started.cursor

    checkpointed = executor.checkpoint(
        "run-1",
        checkpoint_ref="ckpt://run-1",
        idempotency_key="cp-1",
    )
    assert checkpointed.ok
    assert checkpointed.checkpoint_ref == "ckpt://run-1"
    assert all(
        unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in checkpointed.runtime_units
    )
    assert all(unit.status_reason == "checkpointing" for unit in checkpointed.runtime_units)


def test_executor_normalizes_unavailable_dependencies_without_real_processes() -> None:
    executor = RuntimeExecutor()
    executor.register_runtime(
        _manifest(
            framework="verl",
            execution_backend="ray",
            trainer="pytorch",
            rollout_engine="vllm",
        )
    )

    prepared = executor.prepare("run-1", idempotency_key="prepare-real")

    assert not prepared.ok
    assert {item.error.kind for item in prepared.component_results if item.error is not None} == {
        AdapterErrorKind.UNAVAILABLE
    }
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in prepared.runtime_units)
    assert all(unit.error_code == AdapterErrorKind.UNAVAILABLE for unit in prepared.runtime_units)
    assert all(unit.status_reason == "preparing" for unit in prepared.runtime_units)


def test_executor_normalizes_prepare_timeout_failures() -> None:
    command_driver = FakeCommandDriver()
    executor = RuntimeExecutor(command_driver=command_driver)
    executor.register_runtime(_manifest())

    command_driver.set_behavior(
        action=LifecycleAction.PREPARE,
        component="framework",
        run_id="run-1",
        exception=TimeoutError("prepare timed out"),
    )
    prepared = executor.prepare("run-1", idempotency_key="prepare-timeout")
    framework_result = next(
        item for item in prepared.component_results if item.component == "framework"
    )
    assert framework_result.error is not None
    assert framework_result.error.kind is AdapterErrorKind.TRANSIENT
    assert framework_result.error.retryable
    assert framework_result.observed_state == runtime_pb2.RUNTIME_STATE_PREPARING
    assert all(unit.status_reason == "preparing" for unit in prepared.runtime_units)


def test_executor_normalizes_checkpoint_exit_code_failures() -> None:
    command_driver = FakeCommandDriver()
    executor = RuntimeExecutor(command_driver=command_driver)
    executor.register_runtime(_manifest())

    command_driver.set_behavior(
        action=LifecycleAction.CHECKPOINT,
        component="execution",
        run_id="run-1",
        result=CommandResult(exit_code=7, stderr="checkpoint failed"),
    )
    checkpointed = executor.checkpoint("run-1", idempotency_key="checkpoint-exit")
    execution_result = next(
        item for item in checkpointed.component_results if item.component == "execution"
    )
    assert execution_result.error is not None
    assert execution_result.error.kind is AdapterErrorKind.BACKEND_FAILURE
    assert execution_result.error.exit_code == 7
    assert execution_result.command_result is not None
    assert execution_result.command_result.exit_code == 7
    assert all(
        unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in checkpointed.runtime_units
    )
    assert all(
        unit.error_code == AdapterErrorKind.BACKEND_FAILURE for unit in checkpointed.runtime_units
    )
    assert all(unit.status_reason == "checkpointing" for unit in checkpointed.runtime_units)


def test_executor_exposes_only_internal_offload_reload_mutators() -> None:
    executor = RuntimeExecutor()

    for name in ("pause", "resume", "stop", "terminate", "recreate"):
        assert not hasattr(executor, name)
    for name in ("offload", "reload"):
        assert hasattr(executor, name)


def test_executor_rejects_action_without_an_implementing_adapter() -> None:
    executor = RuntimeExecutor(command_driver=FakeCommandDriver())
    executor.register_runtime(_manifest())
    bundle = executor.registry.bundle_for(executor._manifests["run-1"])
    for adapter in (
        bundle.framework,
        bundle.execution,
        bundle.trainer,
        bundle.rollout_engine,
    ):
        cast(Any, adapter)._supported_actions = tuple(
            candidate
            for candidate in adapter.supported_actions
            if candidate is not LifecycleAction.SLEEP
        )

    with pytest.raises(
        AdapterUnavailableError,
        match="no selected runtime adapter supports lifecycle action sleep",
    ):
        executor.offload("run-1", idempotency_key="offload-unsupported")


def test_executor_routes_offload_to_first_party_verl_bridge_and_passes_checkpoint() -> None:
    command_driver = FakeCommandDriver()
    executor = RuntimeExecutor(command_driver=command_driver)
    manifest = _manifest(framework="verl")
    manifest.environment["TGSRL_VERL_CONTROL_SOCKET"] = "/tmp/verl.sock"
    executor.register_runtime(manifest)

    checkpointed = executor.checkpoint(
        "run-1", checkpoint_ref="/tmp/checkpoint-1", idempotency_key="checkpoint-1"
    )
    offloaded = executor.offload("run-1", idempotency_key="offload-1")

    assert checkpointed.ok and offloaded.ok
    checkpoint_calls = [
        call for call in command_driver.calls if dict(call[1]).get("TGSRL_ACTION") == "checkpoint"
    ]
    assert checkpoint_calls
    assert dict(checkpoint_calls[0][1])["TGSRL_CHECKPOINT_REF"] == "/tmp/checkpoint-1"
    offload_calls = [
        call for call in command_driver.calls if dict(call[1]).get("TGSRL_ACTION") == "sleep"
    ]
    assert len(offload_calls) == 1
    assert dict(offload_calls[0][1])["TGSRL_COMPONENT"] == "framework"


def test_executor_generation_advances_and_can_restore_durable_watermark() -> None:
    command_driver = FakeCommandDriver()
    executor = RuntimeExecutor(command_driver=command_driver)
    executor.register_runtime(_manifest())

    first = executor.start("run-1", idempotency_key="start-1")
    assert first.generation == 1
    assert all(unit.generation == 1 for unit in first.runtime_units)

    second = executor.start("run-1", idempotency_key="start-2")
    assert second.generation == 2
    assert all(unit.generation == 2 for unit in second.runtime_units)

    restarted = RuntimeExecutor(command_driver=command_driver)
    manifest, units = restarted.register_runtime(_manifest())
    assert manifest.run_id == "run-1"
    restarted.restore_runtime_generation("run-1", 2)
    restarted.register_runtime(manifest, units)
    next_start = restarted.start("run-1", idempotency_key="start-3")
    assert next_start.generation == 3


def test_executor_restores_durable_start_idempotency_from_runtime_units() -> None:
    command_driver = FakeCommandDriver()
    executor = RuntimeExecutor(command_driver=command_driver)
    manifest, _runtime_units = executor.register_runtime(_manifest())

    started = executor.start("run-1", idempotency_key="start-1")
    persisted_units = []
    for unit in started.runtime_units:
        persisted = runtime_pb2.RuntimeUnit()
        persisted.CopyFrom(unit)
        persisted.annotations["tgsrl.start_idempotency_key"] = "start-1"
        persisted_units.append(persisted)

    restarted = RuntimeExecutor(command_driver=command_driver)
    restarted.register_runtime(manifest, persisted_units)

    started_again = restarted.start("run-1", idempotency_key="start-1")
    assert started_again.idempotent
    assert started_again.generation == 1
    assert all(unit.generation == 1 for unit in started_again.runtime_units)

    next_start = restarted.start("run-1", idempotency_key="start-2")
    assert not next_start.idempotent
    assert next_start.generation == 2
    assert all(unit.generation == 2 for unit in next_start.runtime_units)
