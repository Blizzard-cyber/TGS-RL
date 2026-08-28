"""Runtime adapter registry and executable control-contract tests."""

from __future__ import annotations

from collections.abc import Callable
from types import ModuleType
from typing import cast

import pytest
from tgsrl.v1 import execution_pb2, runtime_pb2, trace_pb2

from adapters import (
    AdapterErrorKind,
    AdapterExecutor,
    AdapterSupport,
    FakeFrameworkAdapter,
    GRPOAdapter,
    LifecycleAction,
    PartialAsyncRolloutAdapter,
    RecordingCommandRunner,
    RecordingProcessRunner,
    RunnerKind,
    RuntimeAdapterRegistry,
    build_execution_contract,
    execute_control_argv,
)
from adapters.compliance.runtime import ComponentAdapter
from adapters.control import CommandResult, ControlRequest
from adapters.execution import RayExecutionBackendAdapter
from adapters.frameworks import OpenRLHFFrameworkAdapter, VerlFrameworkAdapter
from adapters.rollout_engines import SGLangRolloutEngineAdapter, VLLMRolloutEngineAdapter
from adapters.trainers import PyTorchTrainerAdapter

type AdapterUnderTest = (
    VerlFrameworkAdapter
    | OpenRLHFFrameworkAdapter
    | RayExecutionBackendAdapter
    | PyTorchTrainerAdapter
    | VLLMRolloutEngineAdapter
    | SGLangRolloutEngineAdapter
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
        policy_version="policy-1",
        deterministic_seed=11,
        desired_units=2,
        priority=7,
        queue="gold",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        annotations={"algorithm": "grpo"},
    )


def _module_with_hook(name: str, hook_name: str = "handle_lifecycle") -> ModuleType:
    module = ModuleType(name)

    def _hook(request: ControlRequest) -> CommandResult:
        payload = (
            f"{request.component}:{request.adapter}:{request.action.value}:"
            f"{request.bridge_target.kind.value}:{request.bridge_target.endpoint or '-'}"
        )
        return CommandResult(exit_code=0, stdout=payload)

    setattr(module, hook_name, _hook)
    return module


def _module_loader(module_map: dict[str, ModuleType]) -> Callable[[str], ModuleType]:
    def _load(name: str) -> ModuleType:
        if name not in module_map:
            raise ModuleNotFoundError(name)
        return module_map[name]

    return _load


def _provider_manifest(
    *,
    framework: str = "fake",
    execution_backend: str = "fake",
    trainer: str = "fake",
    rollout_engine: str = "fake",
    module_name: str = "",
    endpoint: str = "",
) -> runtime_pb2.RuntimeManifest:
    manifest = _manifest(
        framework=framework,
        execution_backend=execution_backend,
        trainer=trainer,
        rollout_engine=rollout_engine,
    )
    if module_name:
        manifest.artifacts.append(
            runtime_pb2.RuntimeArtifact(
                artifact_id=f"artifact:{module_name}",
                kind="python_module",
                uri=module_name,
                attributes={"module": module_name},
            )
        )
    if endpoint:
        manifest.artifacts.append(
            runtime_pb2.RuntimeArtifact(
                artifact_id=f"endpoint:{endpoint}",
                kind="endpoint",
                uri=endpoint,
                attributes={"endpoint": endpoint},
            )
        )
    return manifest


def _force_dependency_available(adapter: ComponentAdapter) -> None:
    adapter.dependency_name = None


def test_fake_manifest_compiles_to_runtime_units() -> None:
    manifest = _manifest()
    normalized, runtime_units, diagnostics = RuntimeAdapterRegistry().compile(manifest)

    assert normalized.run_id == "run-1"
    assert runtime_units
    assert all(unit.state == runtime_pb2.RUNTIME_STATE_REQUESTED for unit in runtime_units)
    assert {unit.phase_kind for unit in runtime_units} >= {
        execution_pb2.PHASE_KIND_PREFILL,
        execution_pb2.PHASE_KIND_DECODE,
        execution_pb2.PHASE_KIND_ACTOR,
    }
    assert any("framework:SUPPORT" in diagnostic for diagnostic in diagnostics)
    assert any(
        unit.annotations["component_support"] == AdapterSupport.SUPPORT for unit in runtime_units
    )


def test_real_boundary_skeletons_report_unavailable_without_dependencies() -> None:
    manifest = _manifest(
        framework="verl",
        execution_backend="ray",
        trainer="pytorch",
        rollout_engine="vllm",
    )

    _, diagnostics = RuntimeAdapterRegistry().validate(manifest)

    assert any("framework:UNAVAILABLE" in diagnostic for diagnostic in diagnostics)
    assert any("execution:UNAVAILABLE" in diagnostic for diagnostic in diagnostics)
    assert any("trainer:UNAVAILABLE" in diagnostic for diagnostic in diagnostics)
    assert any("rollout_engine:UNAVAILABLE" in diagnostic for diagnostic in diagnostics)


def test_fake_framework_exposes_executable_lifecycle_contract() -> None:
    adapter = FakeFrameworkAdapter()
    manifest = _manifest()

    launch_call = adapter.lifecycle_call(manifest, action=adapter._supported_actions[3])
    status_call = adapter.lifecycle_call(manifest, action=adapter._supported_actions[4])

    assert launch_call.action.value == "launch"
    assert launch_call.runner_kind is RunnerKind.PROCESS
    assert launch_call.state_contract.requested_state == runtime_pb2.RUNTIME_STATE_STARTING
    assert (
        launch_call.state_contract.immediate_observed_state == runtime_pb2.RUNTIME_STATE_REQUESTED
    )
    assert "--component" in launch_call.launch_spec.argv
    assert "--adapter" in launch_call.launch_spec.argv
    assert "--action" in launch_call.launch_spec.argv

    assert status_call.action.value == "status"
    assert status_call.runner_kind is RunnerKind.COMMAND
    assert status_call.state_contract.requires_external_event is False


def test_rollout_engine_adds_rollout_specific_actions_and_runner_execution() -> None:
    manifest = _manifest(rollout_engine="fake")
    fake_rollout = RuntimeAdapterRegistry().bundle_for(manifest).rollout_engine

    lifecycle = {call.action.value: call for call in fake_rollout.lifecycle_calls(manifest)}

    assert {"launch", "status", "weight_update", "sleep", "wake", "recreate"} <= set(lifecycle)
    assert lifecycle["launch"].runner_kind is RunnerKind.PROCESS
    assert lifecycle["weight_update"].runner_kind is RunnerKind.COMMAND
    assert lifecycle["sleep"].state_contract.eventual_observed_states == (
        runtime_pb2.RUNTIME_STATE_SLEEPING,
        runtime_pb2.RUNTIME_STATE_FAILED,
    )

    command_runner = RecordingCommandRunner(next_result=CommandResult(exit_code=0, stdout="ok"))
    process_runner = RecordingProcessRunner(next_pid=4321)
    executor = AdapterExecutor(command_runner=command_runner, process_runner=process_runner)

    launched = executor.execute(lifecycle["launch"])
    updated = executor.execute(lifecycle["weight_update"])

    assert launched.process_handle is not None
    assert launched.process_handle.pid == 4321
    assert updated.command_result is not None
    assert updated.command_result.stdout == "ok"
    assert len(process_runner.calls) == 1
    assert len(command_runner.calls) == 1


def test_adapter_error_mapping_reports_unavailable_runner() -> None:
    adapter = FakeFrameworkAdapter()
    manifest = _manifest()
    status_call = adapter.lifecycle_call(manifest, action=adapter._supported_actions[4])

    try:
        AdapterExecutor().execute(status_call)
    except Exception as error:  # pragma: no cover - exercised by assertion below
        mapped = adapter.map_error(status_call.action, error)
    else:  # pragma: no cover
        raise AssertionError("expected missing runner to raise")

    assert mapped.kind is AdapterErrorKind.UNAVAILABLE
    assert mapped.retryable is False
    assert mapped.resulting_state == runtime_pb2.RUNTIME_STATE_UNKNOWN


@pytest.mark.parametrize(
    ("adapter", "manifest", "action", "module_name", "expected_kind"),
    [
        (
            VerlFrameworkAdapter(),
            _provider_manifest(framework="verl", module_name="provider.verl"),
            "launch",
            "provider.verl",
            "python_module",
        ),
        (
            OpenRLHFFrameworkAdapter(),
            _provider_manifest(framework="openrlhf", module_name="provider.openrlhf"),
            "recreate",
            "provider.openrlhf",
            "python_module",
        ),
        (
            RayExecutionBackendAdapter(),
            _provider_manifest(execution_backend="ray", module_name="provider.ray"),
            "status",
            "provider.ray",
            "python_module",
        ),
        (
            PyTorchTrainerAdapter(),
            _provider_manifest(trainer="pytorch", module_name="provider.pytorch"),
            "checkpoint",
            "provider.pytorch",
            "python_module",
        ),
        (
            VLLMRolloutEngineAdapter(),
            _provider_manifest(
                rollout_engine="vllm",
                module_name="provider.vllm",
                endpoint="https://vllm.example.test/lifecycle",
            ),
            "weight_update",
            "provider.vllm",
            "api_hook",
        ),
        (
            SGLangRolloutEngineAdapter(),
            _provider_manifest(
                rollout_engine="sglang",
                module_name="provider.sglang",
                endpoint="https://sglang.example.test/lifecycle",
            ),
            "sleep",
            "provider.sglang",
            "api_hook",
        ),
    ],
)
def test_real_adapters_execute_control_bridge_via_injected_modules(
    adapter: AdapterUnderTest,
    manifest: runtime_pb2.RuntimeManifest,
    action: str,
    module_name: str,
    expected_kind: str,
) -> None:
    _force_dependency_available(cast(ComponentAdapter, adapter))
    call = adapter.lifecycle_call(manifest, action=LifecycleAction(action))
    result = execute_control_argv(
        call.launch_spec.argv[3:],
        module_loader=_module_loader({module_name: _module_with_hook(module_name)}),
    )

    assert result.exit_code == 0
    assert f":{action}:" in result.stdout
    assert f":{expected_kind}:" in result.stdout


def test_control_bridge_requires_endpoint_for_api_hook() -> None:
    adapter = VLLMRolloutEngineAdapter()
    _force_dependency_available(cast(ComponentAdapter, adapter))
    manifest = _provider_manifest(rollout_engine="vllm", module_name="provider.vllm")
    call = adapter.lifecycle_call(manifest, action=LifecycleAction.WEIGHT_UPDATE)

    with pytest.raises(Exception) as error_info:
        execute_control_argv(
            call.launch_spec.argv[3:],
            module_loader=_module_loader({"provider.vllm": _module_with_hook("provider.vllm")}),
        )

    assert "missing endpoint" in str(error_info.value)


def test_control_bridge_reports_unavailable_for_missing_module() -> None:
    adapter = RayExecutionBackendAdapter()
    _force_dependency_available(cast(ComponentAdapter, adapter))
    manifest = _provider_manifest(execution_backend="ray", module_name="provider.ray")
    call = adapter.lifecycle_call(manifest, action=LifecycleAction.STATUS)

    with pytest.raises(Exception) as error_info:
        execute_control_argv(call.launch_spec.argv[3:], module_loader=_module_loader({}))

    mapped = adapter.map_error(call.action, error_info.value)
    assert mapped.kind is AdapterErrorKind.UNAVAILABLE
