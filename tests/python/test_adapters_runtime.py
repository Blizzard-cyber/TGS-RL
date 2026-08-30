"""Runtime adapter registry and executable control-contract tests."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import cast

import pytest
from tgsrl.v1 import execution_pb2, job_pb2, runtime_pb2, trace_pb2
from tgsrl_runtime.trace_ingest import TraceIngestor

from adapters import (
    AdapterErrorKind,
    AdapterSupport,
    AdapterUnavailableError,
    FakeFrameworkAdapter,
    GRPOAdapter,
    LifecycleAction,
    ManifestValidationError,
    PartialAsyncRolloutAdapter,
    RunnerKind,
    RuntimeAdapterRegistry,
    build_execution_contract,
    execute_control_argv,
)
from adapters.compliance.runtime import ComponentAdapter
from adapters.control import CommandResult, ControlRequest
from adapters.execution import RayExecutionBackendAdapter
from adapters.frameworks import OpenRLHFFrameworkAdapter, VerlFrameworkAdapter
from adapters.frameworks.verl_bridge import (
    ReferenceCallbacks,
    VerlWorkerBridge,
    WorkerIdentity,
)
from adapters.rollout_engines import SGLangRolloutEngineAdapter, VLLMRolloutEngineAdapter
from adapters.runtime_registry import runtime_manifest_from_job
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
    selected = next(
        (
            (component, adapter)
            for component, adapter in (
                ("framework", framework),
                ("execution", execution_backend),
                ("trainer", trainer),
                ("rollout_engine", rollout_engine),
            )
            if adapter not in {"fake", "mock"}
        ),
        ("framework", framework),
    )
    component, adapter = selected
    if module_name:
        manifest.artifacts.append(
            runtime_pb2.RuntimeArtifact(
                artifact_id=f"artifact:{module_name}",
                kind="python_module",
                uri=module_name,
                attributes={
                    "module": module_name,
                    "component": component,
                    "adapter": adapter,
                },
            )
        )
    if endpoint:
        manifest.artifacts.append(
            runtime_pb2.RuntimeArtifact(
                artifact_id=f"endpoint:{endpoint}",
                kind="endpoint",
                uri=endpoint,
                attributes={
                    "endpoint": endpoint,
                    "component": component,
                    "adapter": adapter,
                },
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


def test_runtime_manifest_preserves_all_declared_component_version_pins() -> None:
    job = job_pb2.RLTrainingJob(
        job_id="job-1",
        runtime=job_pb2.FrameworkRuntimeSpec(
            framework="verl",
            framework_version="0.5.0",
            execution_backend="ray",
            execution_backend_version="2.9.0",
            trainer="pytorch",
            trainer_version="2.5.1",
            rollout_engine="vllm",
            rollout_engine_version="0.8.0",
        ),
    )

    manifest = runtime_manifest_from_job(job, observed_at=datetime(2025, 1, 1, tzinfo=UTC))

    pins = {(item.kind, item.name): item for item in manifest.component_versions}
    assert {key: value.version for key, value in pins.items()} == {
        (execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER, "verl"): "0.5.0",
        (execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND, "ray"): "2.9.0",
        (execution_pb2.COMPONENT_KIND_TRAINER, "pytorch"): "2.5.1",
        (execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE, "vllm"): "0.8.0",
    }
    assert all(item.source == "job.runtime" for item in pins.values())
    assert all(item.attributes["provenance"] == "declared" for item in pins.values())
    assert runtime_manifest_from_job(job, observed_at=datetime(2025, 1, 1, tzinfo=UTC)) == manifest


def test_runtime_manifest_requires_authoritative_declaration_time() -> None:
    job = job_pb2.RLTrainingJob(
        job_id="job-1",
        runtime=job_pb2.FrameworkRuntimeSpec(
            framework="fake",
            framework_version="0.1.0",
        ),
    )

    with pytest.raises(ValueError, match=r"job\.created_at or an explicit observed_at"):
        runtime_manifest_from_job(job)


def test_legacy_runtime_manifest_without_version_pins_is_deterministic_without_time() -> None:
    job = job_pb2.RLTrainingJob(
        job_id="job-legacy",
        runtime=job_pb2.FrameworkRuntimeSpec(framework="fake"),
    )

    first = runtime_manifest_from_job(job)
    second = runtime_manifest_from_job(job)

    assert not first.component_versions
    assert first.SerializeToString(deterministic=True) == second.SerializeToString(
        deterministic=True
    )


def test_runtime_registry_resolves_only_explicit_distribution_versions() -> None:
    observed_at = datetime(2025, 1, 1, tzinfo=UTC)
    registry = RuntimeAdapterRegistry(
        version_resolver=lambda distribution: {
            "tgsrl-runtime": "0.1.0",
            "ray": "2.9.0",
        }.get(distribution),
        clock=lambda: observed_at,
    )
    manifest = _manifest(execution_backend="ray")

    resolved = registry.resolved_component_versions(manifest)

    assert {(item.kind, item.name, item.version) for item in resolved} == {
        (execution_pb2.COMPONENT_KIND_RUNTIME, "tgsrl-runtime", "0.1.0"),
        (execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER, "fake", "0.1.0"),
        (execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND, "ray", "2.9.0"),
        (execution_pb2.COMPONENT_KIND_TRAINER, "fake", "0.1.0"),
        (execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE, "fake", "0.1.0"),
    }
    assert all(item.source == "runtime-adapter-registry" for item in resolved)
    assert all(item.attributes["provenance"] == "observed/resolved" for item in resolved)


def _manifest_component_version(
    *,
    kind: int = execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER,
    name: str = "fake",
    version: str = "0.1.0",
    source: str = "job.runtime",
    provenance: str = "declared",
) -> execution_pb2.ComponentVersion:
    component = execution_pb2.ComponentVersion(
        kind=kind,
        name=name,
        version=version,
        source=source,
        revision=1,
        attributes={"provenance": provenance},
    )
    component.observed_at.FromDatetime(datetime(2025, 1, 1, tzinfo=UTC))
    return component


def _forge_manifest_component_version(
    manifest: runtime_pb2.RuntimeManifest,
) -> None:
    manifest.component_versions[0].source = "external"
    manifest.component_versions[0].attributes["provenance"] = "forged"


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (
            lambda manifest: manifest.component_versions[0].ClearField("observed_at"),
            "observed_at",
        ),
        (
            lambda manifest: manifest.component_versions.append(
                _manifest_component_version(version="0.2.0")
            ),
            "kind/name identity",
        ),
        (
            lambda manifest: setattr(manifest.component_versions[0], "name", "verl"),
            "does not match selected framework",
        ),
        (
            lambda manifest: setattr(
                manifest.component_versions[0], "source", "runtime-adapter-registry"
            ),
            "resolved provenance",
        ),
        (
            lambda manifest: manifest.component_versions[0].attributes.__setitem__(
                "provenance", "observed/resolved"
            ),
            "resolved provenance",
        ),
        (
            _forge_manifest_component_version,
            "accepts only declared versions",
        ),
        (
            lambda manifest: manifest.component_versions[0].attributes.pop("provenance"),
            "accepts only declared versions",
        ),
    ],
)
def test_manifest_component_version_trust_boundary(
    mutate: Callable[[runtime_pb2.RuntimeManifest], None], error: str
) -> None:
    manifest = _manifest()
    manifest.component_versions.append(_manifest_component_version())
    mutate(manifest)

    with pytest.raises(ManifestValidationError, match=error):
        RuntimeAdapterRegistry().validate(manifest)


def test_first_party_verl_bridge_is_degraded_without_runtime_socket() -> None:
    manifest = _manifest(
        framework="verl",
        execution_backend="ray",
        trainer="pytorch",
        rollout_engine="vllm",
    )

    _, diagnostics = RuntimeAdapterRegistry().validate(manifest)

    assert any("framework:DEGRADED" in diagnostic for diagnostic in diagnostics)
    assert any("execution:UNAVAILABLE" in diagnostic for diagnostic in diagnostics)
    assert any("trainer:UNAVAILABLE" in diagnostic for diagnostic in diagnostics)
    assert any("rollout_engine:UNAVAILABLE" in diagnostic for diagnostic in diagnostics)


def test_global_command_is_owned_by_execution_backend() -> None:
    adapter = RayExecutionBackendAdapter()
    _force_dependency_available(cast(ComponentAdapter, adapter))
    manifest = _manifest(execution_backend="ray")
    manifest.command[:] = ("verlctl", "reference-bridge", "--role", "reference")
    manifest.args[:] = (
        "--checkpoint-dir",
        "/tmp/checkpoints/run-1",
    )
    manifest.environment["MODEL"] = "policy-1"
    manifest.working_directory = "/tmp/verl-runtime"

    call = adapter.lifecycle_call(manifest, action=LifecycleAction.LAUNCH)

    assert call.runner_kind is RunnerKind.PROCESS
    assert call.launch_spec.argv == (
        "verlctl",
        "reference-bridge",
        "--role",
        "reference",
        "--checkpoint-dir",
        "/tmp/checkpoints/run-1",
    )
    assert call.launch_spec.working_directory == "/tmp/verl-runtime"
    assert dict(call.launch_spec.env)["TGSRL_ACTION"] == "launch"
    assert dict(call.launch_spec.env)["MODEL"] == "policy-1"

    with pytest.raises(AdapterUnavailableError, match="no executable execution bridge"):
        adapter.lifecycle_call(manifest, action=LifecycleAction.CHECKPOINT)


def test_verl_support_diagnostics_surface_explicit_lifecycle_matrix() -> None:
    adapter = VerlFrameworkAdapter()
    _force_dependency_available(cast(ComponentAdapter, adapter))
    manifest = _provider_manifest(framework="verl", module_name="provider.verl")

    report = adapter.describe_support(manifest)
    assert report.status is AdapterSupport.SUPPORT

    _, diagnostics = RuntimeAdapterRegistry().validate(manifest)
    expected_actions = (
        "framework:executable_actions="
        "validate,compile,prepare,launch,status,prepare_pause,pause,resume,checkpoint,"
        "stop,terminate,weight_update,sleep,wake"
    )
    assert expected_actions in diagnostics
    assert "framework:unavailable_actions=recreate" in diagnostics


def test_verl_uses_repository_bridge_without_manifest_override() -> None:
    adapter = VerlFrameworkAdapter()
    _force_dependency_available(cast(ComponentAdapter, adapter))
    manifest = _manifest(framework="verl")
    report = adapter.describe_support(manifest)

    call = adapter.lifecycle_call(manifest, action=LifecycleAction.CHECKPOINT)

    assert report.status is AdapterSupport.DEGRADED
    assert "adapters.frameworks.verl_bridge" in call.launch_spec.argv
    assert call.runner_kind is RunnerKind.COMMAND


def test_verl_worker_bridge_controls_lifecycle_and_emits_quality_observations(
    tmp_path: Path,
) -> None:
    typed_events: list[trace_pb2.TraceEvent] = []
    bridge = VerlWorkerBridge(
        socket_path=tmp_path / "worker.sock",
        trace_path=tmp_path / "trace.ndjson",
        identity=WorkerIdentity(
            run_id="run-1",
            job_id="job-1",
            trace_id="trace-1",
            sandbox_id="sandbox-1",
            role="rollout",
            generation=1,
            policy_version="policy-1",
            binding_id="binding-1",
            device_id="MIG-a",
            share=1.0,
        ),
        callbacks=ReferenceCallbacks(tmp_path / "checkpoints"),
        trace_sink=typed_events.append,
    )
    requests = [
        {"action": "prepare_pause", "idempotency_key": "prepare"},
        {"action": "checkpoint", "idempotency_key": "checkpoint"},
        {"action": "offload", "idempotency_key": "offload"},
        {"action": "reload", "idempotency_key": "reload"},
        {"action": "resume", "idempotency_key": "resume"},
        {
            "action": "weight_update",
            "idempotency_key": "weight-update",
            "policy_version": "policy-2",
        },
    ]
    for request in requests:
        response = bridge.handle({"sandbox_id": "sandbox-1", "generation": 1, **request})
        assert response["accepted"], response
    replay = bridge.handle(
        {
            "action": "weight_update",
            "sandbox_id": "sandbox-1",
            "generation": 1,
            "idempotency_key": "weight-update",
            "policy_version": "policy-2",
        }
    )
    assert replay["accepted"]
    bridge.observe(
        event_type="sample_consumed",
        buffer_level=3,
        policy_lag=1,
        sample_stale=False,
        effective_sample_size=7.5,
        accepted_samples=8,
        expected_samples=8,
        duration_ms=4.0,
    )

    events = [json.loads(line) for line in bridge.trace_path.read_text().splitlines()]
    assert bridge.state == "running"
    assert bridge.identity.policy_version == "policy-2"
    assert events[-1]["contract_observation"] == {
        "accepted_samples": 8,
        "buffer_level": 3,
        "effective_sample_size": 7.5,
        "expected_samples": 8,
        "policy_lag": 1,
        "policy_version": "policy-2",
        "safe_point": False,
        "sample_stale": False,
    }
    assert typed_events[-1].policy_version == "policy-2"
    assert typed_events[-1].contract_observation.policy_lag == 1
    assert typed_events[-1].contract_observation.effective_sample_size == 7.5
    assert typed_events[-1].contract_observation.sample_count == 8
    assert typed_events[-1].contract_observation.effective_sample_size_ratio == 0.9375
    assert typed_events[-1].contract_observation.HasField("observed_at")
    ingested = TraceIngestor().ingest("run-1", typed_events)
    assert ingested[-1].event_id == typed_events[-1].event_id
    assert ingested[-1].phase_kind == execution_pb2.PHASE_KIND_DECODE


def test_verl_control_module_reaches_worker_over_unix_socket(tmp_path: Path) -> None:
    with TemporaryDirectory(prefix="verl-bridge-", dir="/tmp") as directory:
        root = Path(directory)
        bridge = VerlWorkerBridge(
            socket_path=root / "worker.sock",
            trace_path=root / "trace.ndjson",
            identity=WorkerIdentity(
                run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
                sandbox_id="sandbox-1",
                role="rollout",
                generation=1,
                policy_version="policy-1",
            ),
            callbacks=ReferenceCallbacks(root / "checkpoints"),
        )
        server = threading.Thread(target=bridge.serve_forever)
        server.start()
        try:
            deadline = time.monotonic() + 2
            while not bridge.socket_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            result = execute_control_argv(
                [
                    "--component",
                    "framework",
                    "--adapter",
                    "verl",
                    "--action",
                    "status",
                    "--run-id",
                    "run-1",
                    "--job-id",
                    "job-1",
                    "--trace-id",
                    "trace-1",
                    "--bridge-kind",
                    "python_module",
                    "--bridge-module",
                    "adapters.frameworks.verl_bridge",
                    "--target-env",
                    f"TGSRL_VERL_CONTROL_SOCKET={bridge.socket_path}",
                    "--target-env",
                    "TGSRL_SANDBOX_ID=sandbox-1",
                    "--target-env",
                    "TGSRL_GENERATION=1",
                ]
            )
            assert result.exit_code == 0
            assert json.loads(result.stdout)["ready"] is True
        finally:
            bridge.shutdown()
            server.join(timeout=2)
        assert not server.is_alive()


def test_verl_control_module_composes_pause_safe_point_sequence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with TemporaryDirectory(prefix="verl-bridge-", dir="/tmp") as directory:
        root = Path(directory)
        bridge = VerlWorkerBridge(
            socket_path=root / "worker.sock",
            trace_path=root / "trace.ndjson",
            identity=WorkerIdentity(
                run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
                sandbox_id="sandbox-1",
                role="rollout",
                generation=1,
                policy_version="policy-1",
            ),
            callbacks=ReferenceCallbacks(root / "checkpoints"),
        )
        server = threading.Thread(target=bridge.serve_forever)
        server.start()
        try:
            deadline = time.monotonic() + 2
            while not bridge.socket_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            monkeypatch.setenv("TGSRL_VERL_CONTROL_SOCKET", str(bridge.socket_path))
            monkeypatch.setenv("TGSRL_SANDBOX_ID", "sandbox-1")
            monkeypatch.setenv("TGSRL_GENERATION", "1")
            monkeypatch.setenv("TGSRL_IDEMPOTENCY_KEY", "pause-command")
            result = execute_control_argv(
                [
                    "--component",
                    "framework",
                    "--adapter",
                    "verl",
                    "--action",
                    "pause",
                    "--run-id",
                    "run-1",
                    "--job-id",
                    "job-1",
                    "--trace-id",
                    "trace-1",
                    "--bridge-kind",
                    "python_module",
                    "--bridge-module",
                    "adapters.frameworks.verl_bridge",
                ]
            )
            assert result.exit_code == 0
            assert bridge.state == "paused"
            assert bridge.safe_point
            assert [
                json.loads(line)["event_type"]
                for line in bridge.trace_path.read_text().splitlines()
            ] == ["prepare_pause", "pause"]
        finally:
            bridge.shutdown()
            server.join(timeout=2)


def test_verl_worker_bridge_recovers_completed_idempotency_state(tmp_path: Path) -> None:
    identity = WorkerIdentity(
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        sandbox_id="sandbox-1",
        role="rollout",
        generation=1,
        policy_version="policy-1",
    )
    state_path = tmp_path / "worker-state.json"
    first = VerlWorkerBridge(
        socket_path=tmp_path / "worker.sock",
        trace_path=tmp_path / "trace.ndjson",
        state_path=state_path,
        identity=identity,
        callbacks=ReferenceCallbacks(tmp_path / "checkpoints"),
    )
    request = {
        "action": "prepare_pause",
        "sandbox_id": "sandbox-1",
        "generation": 1,
        "idempotency_key": "prepare-key",
    }
    assert first.handle(request)["accepted"]
    event_count = len(first.trace_path.read_text().splitlines())

    restarted = VerlWorkerBridge(
        socket_path=tmp_path / "worker.sock",
        trace_path=tmp_path / "trace.ndjson",
        state_path=state_path,
        identity=WorkerIdentity(
            run_id="run-1",
            job_id="job-1",
            trace_id="trace-1",
            sandbox_id="sandbox-1",
            role="rollout",
            generation=1,
            policy_version="policy-1",
        ),
        callbacks=ReferenceCallbacks(tmp_path / "checkpoints"),
    )
    replay = restarted.handle(request)

    assert replay["accepted"]
    assert restarted.safe_point
    assert len(restarted.trace_path.read_text().splitlines()) == event_count
    assert state_path.stat().st_mode & 0o777 == 0o600


def test_fake_framework_exposes_executable_lifecycle_contract() -> None:
    adapter = FakeFrameworkAdapter()
    manifest = _manifest()

    launch_call = adapter.lifecycle_call(manifest, action=adapter.supported_actions[3])
    status_call = adapter.lifecycle_call(manifest, action=adapter.supported_actions[4])

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


def test_rollout_engine_adds_rollout_specific_actions() -> None:
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


def test_adapter_error_mapping_reports_unavailable_runner() -> None:
    adapter = FakeFrameworkAdapter()
    manifest = _manifest()
    status_call = adapter.lifecycle_call(manifest, action=adapter.supported_actions[4])

    mapped = adapter.map_error(
        status_call.action, AdapterUnavailableError("command runner is unavailable")
    )

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
            "checkpoint",
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
    with pytest.raises(AdapterUnavailableError) as error_info:
        adapter.lifecycle_call(manifest, action=LifecycleAction.WEIGHT_UPDATE)

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
