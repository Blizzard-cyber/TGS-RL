"""Bootstrap and process entrypoint helpers for the runtime supervisor service."""

from __future__ import annotations

import argparse
import asyncio
import math
import os
from pathlib import Path
from typing import TYPE_CHECKING

import grpc
from tgsrl.v1 import experiment_pb2_grpc, runtime_pb2, runtime_pb2_grpc

import tgsrl_runtime.config as runtime_config
from tgsrl_runtime.job_control_client import JobControlClient
from tgsrl_runtime.operator_client import OperatorClient
from tgsrl_runtime.persistence_adapter import (
    NullPersistenceHook,
    PersistenceHook,
    SQLitePersistenceHook,
)
from tgsrl_runtime.runtime_transport import ExperimentServicer, RuntimeControlServicer
from tgsrl_runtime.scheduler_client import SchedulerClient

if TYPE_CHECKING:
    from tgsrl_runtime.supervisor import RuntimeSupervisor, SchedulerClientProtocol

DEFAULT_RUNTIME_STATE_DB = ".cache/tgsrl/runtime.db"
DEFAULT_SCHEDULER_TARGET = "127.0.0.1:50051"
DEFAULT_OPERATOR_TARGET = "127.0.0.1:50081"
DEFAULT_JOB_CONTROL_TARGET = "127.0.0.1:50061"
DEFAULT_CONFIG_ROOT = "."
DEFAULT_OPERATOR_TIMEOUT_SECONDS = 30.0
_RUNTIME_COMPONENT_FIELDS = (
    "framework",
    "execution_backend",
    "trainer",
    "rollout_engine",
)


def _trace_signing_key(path: str) -> bytes:
    configured = os.environ.get("TGSRL_WORKER_REGISTRY_SIGNING_KEY", "").strip()
    if path.strip():
        configured = Path(path).read_text(encoding="utf-8").strip()
    if configured and len(configured.encode()) < 32:
        raise ValueError("worker trace signing key must contain at least 32 bytes")
    return configured.encode()


def _operator_timeout_seconds() -> float:
    raw = os.environ.get(
        "TGSRL_RUNTIME_OPERATOR_TIMEOUT_SECONDS", str(DEFAULT_OPERATOR_TIMEOUT_SECONDS)
    ).strip()
    value = float(raw or DEFAULT_OPERATOR_TIMEOUT_SECONDS)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("Runtime Operator timeout must be positive and finite")
    return value


def _server_state_db(value: str | None) -> str:
    """Resolve server persistence, making an ephemeral store an explicit choice."""
    resolved = value or DEFAULT_RUNTIME_STATE_DB
    if resolved != ":memory:":
        Path(resolved).expanduser().parent.mkdir(parents=True, exist_ok=True)
    return resolved


def _require_product_server_dependencies(
    *,
    scheduler_target: str,
    operator_target: str,
    job_control_target: str,
    config_root: str,
) -> None:
    missing = [
        name
        for name, value in (
            ("scheduler_target", scheduler_target),
            ("operator_target", operator_target),
            ("job_control_target", job_control_target),
            ("config_root", config_root),
        )
        if not value.strip()
    ]
    if missing:
        raise ValueError(
            "product runtime server requires nonempty dependencies: " + ", ".join(missing)
        )


def _allows_mock_component_projection(
    bundle: runtime_config.ConfigBundle | None,
) -> bool:
    if bundle is None:
        return False
    provider = bundle.profile.provider
    return (
        bundle.capabilities.claims.mock_only
        and provider.kind.casefold() == "mockresourceprovider"
        and provider.source.casefold() == "mock"
        and not provider.live_hardware
    )


def _validate_server_manifest_components(
    manifest: runtime_pb2.RuntimeManifest,
    bundle: runtime_config.ConfigBundle | None,
) -> None:
    missing = [
        field_name
        for field_name in _RUNTIME_COMPONENT_FIELDS
        if not getattr(manifest, field_name).strip()
    ]
    if missing and not _allows_mock_component_projection(bundle):
        raise ValueError(
            "server runtime manifest requires explicit component names: " + ", ".join(missing)
        )


class ServerRuntimeControlServicer(RuntimeControlServicer):
    """Transport guard that keeps production requests from defaulting to fake adapters."""

    async def ValidateRuntime(
        self,
        request: runtime_pb2.ValidateRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.ValidateRuntimeRequest, runtime_pb2.ValidateRuntimeResponse
        ],
    ) -> runtime_pb2.ValidateRuntimeResponse:
        try:
            _validate_server_manifest_components(request.manifest, self._supervisor.config_bundle)
        except ValueError as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error
        return await super().ValidateRuntime(request, context)

    async def CompileRuntime(
        self,
        request: runtime_pb2.CompileRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.CompileRuntimeRequest, runtime_pb2.CompileRuntimeResponse
        ],
    ) -> runtime_pb2.CompileRuntimeResponse:
        try:
            _validate_server_manifest_components(request.manifest, self._supervisor.config_bundle)
        except ValueError as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error
        return await super().CompileRuntime(request, context)


def build_runtime_supervisor(
    *,
    state_db: str | None = None,
    scheduler_target: str | None = None,
    scheduler_client: SchedulerClientProtocol | None = None,
    job_control_target: str | None = None,
    persistence: PersistenceHook | None = None,
    config_load_options: runtime_config.LoadOptions | None = None,
    trace_signing_key: bytes = b"",
) -> RuntimeSupervisor:
    """Construct a supervisor with optional sqlite persistence and real scheduler client."""
    from tgsrl_runtime.supervisor import RuntimeSupervisor

    if persistence is not None:
        hook = persistence
    elif state_db:
        hook = SQLitePersistenceHook(_server_state_db(state_db))
    else:
        hook = NullPersistenceHook()
    client = scheduler_client or (
        SchedulerClient(target=scheduler_target) if scheduler_target else None
    )
    # Compatibility-only keyword. Product servers inject the reporter at the
    # transport boundary instead of putting network clients in RuntimeSupervisor.
    del job_control_target
    bundle = (
        runtime_config.load_bundle_with_options(config_load_options)
        if config_load_options is not None
        else None
    )
    return RuntimeSupervisor(
        config_bundle=bundle,
        scheduler_client=client,
        persistence=hook,
        trace_signing_key=trace_signing_key,
    )


async def serve_runtime(
    *,
    bind: str,
    supervisor: RuntimeSupervisor | None = None,
    state_db: str | None = None,
    scheduler_target: str | None = None,
    job_control_target: str | None = None,
    operator_target: str | None = None,
    operator_client: OperatorClient | None = None,
    job_control_reporter: JobControlClient | None = None,
    config_load_options: runtime_config.LoadOptions | None = None,
    trace_signing_key: bytes = b"",
) -> grpc.aio.Server:
    """Create and start the Python runtime gRPC server."""
    if supervisor is None:
        _require_product_server_dependencies(
            scheduler_target=scheduler_target or "",
            operator_target=operator_target or "",
            job_control_target=job_control_target or "",
            config_root=str(config_load_options.root) if config_load_options is not None else "",
        )
    server = grpc.aio.server()
    runtime_supervisor = supervisor or build_runtime_supervisor(
        state_db=_server_state_db(state_db),
        scheduler_target=scheduler_target,
        config_load_options=config_load_options,
        trace_signing_key=trace_signing_key,
    )
    resolved_operator = operator_client or (
        OperatorClient(target=operator_target) if operator_target else None
    )
    resolved_reporter = job_control_reporter or (
        JobControlClient(target=job_control_target) if job_control_target else None
    )
    runtime_pb2_grpc.add_RuntimeControlServiceServicer_to_server(
        ServerRuntimeControlServicer(
            runtime_supervisor,
            operator_client=resolved_operator,
            job_control_reporter=resolved_reporter,
        ),
        server,
    )
    experiment_pb2_grpc.add_ExperimentServiceServicer_to_server(
        ExperimentServicer(runtime_supervisor), server
    )
    server.add_insecure_port(bind)
    await server.start()
    return server


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Runtime supervisor, gRPC services, and process entrypoint."
    )
    parser.add_argument("--bind", default="[::]:50071")
    parser.add_argument("--state-db", default=DEFAULT_RUNTIME_STATE_DB)
    parser.add_argument("--scheduler-target", default=DEFAULT_SCHEDULER_TARGET)
    parser.add_argument("--job-control-target", default=DEFAULT_JOB_CONTROL_TARGET)
    parser.add_argument("--operator-target", default=DEFAULT_OPERATOR_TARGET)
    parser.add_argument("--config-root", default=DEFAULT_CONFIG_ROOT)
    parser.add_argument("--manifest", default="")
    parser.add_argument(
        "--worker-registry-signing-key-file",
        default="",
        help="shared HMAC key used to authenticate Scheduler-forwarded worker traces",
    )
    parser.add_argument("role", nargs="?")
    parser.add_argument("component", nargs="?")
    parser.add_argument("run_id", nargs="?")
    return parser


async def _run_server(
    *,
    bind: str,
    state_db: str,
    scheduler_target: str,
    job_control_target: str,
    operator_target: str,
    config_root: str,
    manifest: str,
    worker_registry_signing_key_file: str = "",
) -> None:
    _require_product_server_dependencies(
        scheduler_target=scheduler_target,
        operator_target=operator_target,
        job_control_target=job_control_target,
        config_root=config_root,
    )
    config_load_options = runtime_config.LoadOptions(
        root=runtime_config.Path(config_root),
        manifest_path=manifest or runtime_config.DEFAULT_MANIFEST,
    )
    supervisor = build_runtime_supervisor(
        state_db=_server_state_db(state_db),
        scheduler_target=scheduler_target,
        config_load_options=config_load_options,
        trace_signing_key=_trace_signing_key(worker_registry_signing_key_file),
    )
    operator_client = OperatorClient(target=operator_target, timeout=_operator_timeout_seconds())
    job_control_reporter = JobControlClient(target=job_control_target)
    server = await serve_runtime(
        bind=bind,
        supervisor=supervisor,
        operator_client=operator_client,
        job_control_reporter=job_control_reporter,
    )
    try:
        await server.wait_for_termination()
    finally:
        if isinstance(supervisor.scheduler_client, SchedulerClient):
            await supervisor.scheduler_client.close()
        if operator_client is not None:
            await operator_client.close()
        if job_control_reporter is not None:
            await job_control_reporter.close()
        if isinstance(supervisor.persistence, SQLitePersistenceHook):
            supervisor.persistence.close()


def main() -> None:
    args = _parser().parse_args()
    if args.role:
        print(
            f"runtime-supervisor role={args.role} component={args.component or ''} "
            f"run_id={args.run_id or ''}"
        )
        return
    asyncio.run(
        _run_server(
            bind=args.bind,
            state_db=args.state_db,
            scheduler_target=args.scheduler_target,
            job_control_target=args.job_control_target,
            operator_target=args.operator_target,
            config_root=args.config_root,
            manifest=args.manifest,
            worker_registry_signing_key_file=args.worker_registry_signing_key_file,
        )
    )


if __name__ == "__main__":
    main()
