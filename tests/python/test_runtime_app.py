"""Product-server startup boundary tests."""

from dataclasses import replace
from pathlib import Path

import pytest
from tgsrl.v1 import runtime_pb2
from tgsrl_runtime import config as runtime_config
from tgsrl_runtime.persistence_adapter import NullPersistenceHook, SQLitePersistenceHook
from tgsrl_runtime.runtime_app import (
    DEFAULT_CONFIG_ROOT,
    DEFAULT_JOB_CONTROL_TARGET,
    DEFAULT_OPERATOR_TARGET,
    DEFAULT_RUNTIME_STATE_DB,
    DEFAULT_SCHEDULER_TARGET,
    _allows_mock_component_projection,
    _parser,
    _require_product_server_dependencies,
    _run_server,
    _server_state_db,
    _validate_server_manifest_components,
    build_runtime_supervisor,
    serve_runtime,
)


def test_server_cli_defaults_to_explicit_local_state_database() -> None:
    args = _parser().parse_args([])
    assert args.state_db == DEFAULT_RUNTIME_STATE_DB == ".cache/tgsrl/runtime.db"
    assert args.scheduler_target == DEFAULT_SCHEDULER_TARGET == "127.0.0.1:50051"
    assert args.operator_target == DEFAULT_OPERATOR_TARGET == "127.0.0.1:50081"
    assert args.job_control_target == DEFAULT_JOB_CONTROL_TARGET == "127.0.0.1:50061"
    assert args.config_root == DEFAULT_CONFIG_ROOT == "."


def test_server_state_database_resolves_empty_value_to_persistent_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert _server_state_db("") == DEFAULT_RUNTIME_STATE_DB
    assert (tmp_path / ".cache" / "tgsrl").is_dir()


def test_programmatic_supervisor_keeps_explicit_null_persistence_compatibility() -> None:
    supervisor = build_runtime_supervisor()
    assert isinstance(supervisor.persistence, NullPersistenceHook)


def test_explicit_state_database_creates_parent_and_sqlite_hook(tmp_path: Path) -> None:
    state_db = tmp_path / "nested" / "runtime.db"
    supervisor = build_runtime_supervisor(state_db=str(state_db))
    persistence = supervisor.persistence
    assert isinstance(persistence, SQLitePersistenceHook)
    try:
        assert state_db.exists()
    finally:
        persistence.close()


def test_explicit_mock_profile_may_project_missing_component_names() -> None:
    bundle = runtime_config.load_bundle()
    assert _allows_mock_component_projection(bundle)
    _validate_server_manifest_components(runtime_pb2.RuntimeManifest(), bundle)


def test_real_profile_fails_closed_when_component_names_are_missing() -> None:
    bundle = runtime_config.load_bundle()
    provider = replace(
        bundle.profile.provider,
        kind="NvidiaProvider",
        source="nvidia",
        live_hardware=True,
    )
    profile = replace(bundle.profile, provider=provider)
    claims = replace(bundle.capabilities.claims, mock_only=False, live_gpu=True)
    capabilities = replace(bundle.capabilities, source="nvidia", claims=claims)
    real_bundle = replace(bundle, profile=profile, capabilities=capabilities)
    manifest = runtime_pb2.RuntimeManifest(framework="verl")

    with pytest.raises(
        ValueError,
        match="execution_backend, trainer, rollout_engine",
    ):
        _validate_server_manifest_components(manifest, real_bundle)


def test_server_without_config_fails_closed_but_complete_manifest_is_valid() -> None:
    with pytest.raises(ValueError, match="explicit component names"):
        _validate_server_manifest_components(runtime_pb2.RuntimeManifest(), None)

    _validate_server_manifest_components(
        runtime_pb2.RuntimeManifest(
            framework="verl",
            execution_backend="ray",
            trainer="pytorch",
            rollout_engine="vllm",
        ),
        None,
    )


@pytest.mark.parametrize(
    "missing",
    ["scheduler_target", "operator_target", "job_control_target", "config_root"],
)
def test_product_server_dependencies_fail_fast_when_empty(missing: str) -> None:
    values = {
        "scheduler_target": DEFAULT_SCHEDULER_TARGET,
        "operator_target": DEFAULT_OPERATOR_TARGET,
        "job_control_target": DEFAULT_JOB_CONTROL_TARGET,
        "config_root": DEFAULT_CONFIG_ROOT,
    }
    values[missing] = " "
    with pytest.raises(ValueError, match=missing):
        _require_product_server_dependencies(**values)


@pytest.mark.asyncio
async def test_run_server_rejects_empty_dependency_before_building() -> None:
    with pytest.raises(ValueError, match="scheduler_target"):
        await _run_server(
            bind="127.0.0.1:0",
            state_db=":memory:",
            scheduler_target="",
            operator_target=DEFAULT_OPERATOR_TARGET,
            job_control_target=DEFAULT_JOB_CONTROL_TARGET,
            config_root=DEFAULT_CONFIG_ROOT,
            manifest="",
        )


@pytest.mark.asyncio
async def test_programmatic_serve_runtime_accepts_custom_supervisor_without_targets() -> None:
    supervisor = build_runtime_supervisor()
    server = await serve_runtime(bind="127.0.0.1:0", supervisor=supervisor)
    await server.stop(None)


@pytest.mark.asyncio
async def test_programmatic_product_server_rejects_missing_dependencies() -> None:
    with pytest.raises(ValueError, match="scheduler_target"):
        await serve_runtime(bind="127.0.0.1:0", state_db=":memory:")
