"""Tests for runtime execution supervision over adapter lifecycle contracts."""

from __future__ import annotations

from tgsrl.v1 import resource_pb2, runtime_pb2, trace_pb2
from tgsrl_runtime.executor import (
    FakeCommandDriver,
    FakeProcessDriver,
    RuntimeExecutor,
)

from adapters import (
    AdapterErrorKind,
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
    process_driver = FakeProcessDriver()
    command_driver = FakeCommandDriver(process_driver=process_driver)
    executor = RuntimeExecutor(
        registry=RuntimeAdapterRegistry(),
        process_driver=process_driver,
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

    started = executor.start("run-1", idempotency_key="start-1")
    assert started.ok
    assert not started.reconciled
    assert started.action is LifecycleAction.LAUNCH
    assert started.generation == 1
    assert all(unit.generation == 1 for unit in started.runtime_units)
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in started.runtime_units)
    assert all(unit.status_reason == "starting" for unit in started.runtime_units)

    started_again = executor.start("run-1", idempotency_key="start-1")
    assert started_again.idempotent
    assert started_again.cursor == started.cursor
    assert len(process_driver.start_calls) == 0

    status = executor.status("run-1", idempotency_key="status-1")
    assert status.ok
    assert not status.reconciled
    assert status.action is LifecycleAction.STATUS
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in status.runtime_units)
    assert all(unit.status_reason == "unknown" for unit in status.runtime_units)

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


def test_executor_normalizes_status_timeout_failures() -> None:
    process_driver = FakeProcessDriver()
    command_driver = FakeCommandDriver(process_driver=process_driver)
    executor = RuntimeExecutor(process_driver=process_driver, command_driver=command_driver)
    executor.register_runtime(_manifest())
    executor.start("run-1", idempotency_key="start-1")

    command_driver.set_behavior(
        action=LifecycleAction.STATUS,
        component="framework",
        run_id="run-1",
        exception=TimeoutError("pause timed out"),
    )
    status = executor.status("run-1", idempotency_key="status-timeout")
    framework_result = next(
        item for item in status.component_results if item.component == "framework"
    )
    assert framework_result.error is not None
    assert framework_result.error.kind is AdapterErrorKind.TRANSIENT
    assert framework_result.error.retryable
    assert framework_result.observed_state == runtime_pb2.RUNTIME_STATE_UNKNOWN
    assert all(unit.status_reason == "unknown" for unit in status.runtime_units)


def test_executor_normalizes_status_exit_code_failures() -> None:
    process_driver = FakeProcessDriver()
    command_driver = FakeCommandDriver(process_driver=process_driver)
    executor = RuntimeExecutor(process_driver=process_driver, command_driver=command_driver)
    executor.register_runtime(_manifest())
    executor.start("run-1", idempotency_key="start-1")

    command_driver.set_behavior(
        action=LifecycleAction.STATUS,
        component="execution",
        run_id="run-1",
        result=CommandResult(exit_code=7, stderr="resume failed"),
    )
    resumed = executor.status("run-1", idempotency_key="status-exit")
    execution_result = next(
        item for item in resumed.component_results if item.component == "execution"
    )
    assert execution_result.error is not None
    assert execution_result.error.kind is AdapterErrorKind.BACKEND_FAILURE
    assert execution_result.error.exit_code == 7
    assert execution_result.command_result is not None
    assert execution_result.command_result.exit_code == 7
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in resumed.runtime_units)
    assert all(
        unit.error_code == AdapterErrorKind.BACKEND_FAILURE for unit in resumed.runtime_units
    )
    assert all(unit.status_reason == "unknown" for unit in resumed.runtime_units)


def test_executor_does_not_expose_backend_lifecycle_mutators() -> None:
    executor = RuntimeExecutor()

    for name in ("pause", "resume", "stop", "terminate"):
        assert not hasattr(executor, name)


def test_executor_generation_advances_and_survives_restore() -> None:
    process_driver = FakeProcessDriver()
    command_driver = FakeCommandDriver(process_driver=process_driver)
    executor = RuntimeExecutor(process_driver=process_driver, command_driver=command_driver)
    executor.register_runtime(_manifest())

    first = executor.start("run-1", idempotency_key="start-1")
    assert first.generation == 1
    assert all(unit.generation == 1 for unit in first.runtime_units)

    second = executor.start("run-1", idempotency_key="start-2")
    assert second.generation == 2
    assert all(unit.generation == 2 for unit in second.runtime_units)

    restarted = RuntimeExecutor(process_driver=process_driver, command_driver=command_driver)
    restarted.restore(executor.snapshot())
    reconciled = restarted.reconcile("run-1")
    assert reconciled.ok
    assert reconciled.reconciled
    assert reconciled.generation == 2
    assert all(unit.generation == 2 for unit in reconciled.runtime_units)
    assert all(
        unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in reconciled.runtime_units
    )


def test_executor_restores_durable_start_idempotency_from_runtime_units() -> None:
    process_driver = FakeProcessDriver()
    command_driver = FakeCommandDriver(process_driver=process_driver)
    executor = RuntimeExecutor(process_driver=process_driver, command_driver=command_driver)
    manifest, _runtime_units = executor.register_runtime(_manifest())

    started = executor.start("run-1", idempotency_key="start-1")
    persisted_units = []
    for unit in started.runtime_units:
        persisted = runtime_pb2.RuntimeUnit()
        persisted.CopyFrom(unit)
        persisted.annotations["tgsrl.start_idempotency_key"] = "start-1"
        persisted_units.append(persisted)

    restarted = RuntimeExecutor(process_driver=process_driver, command_driver=command_driver)
    restarted.register_runtime(manifest, persisted_units)

    started_again = restarted.start("run-1", idempotency_key="start-1")
    assert started_again.idempotent
    assert started_again.generation == 1
    assert all(unit.generation == 1 for unit in started_again.runtime_units)

    next_start = restarted.start("run-1", idempotency_key="start-2")
    assert not next_start.idempotent
    assert next_start.generation == 2
    assert all(unit.generation == 2 for unit in next_start.runtime_units)
