"""Executable adapter control contracts with injectable runners."""

from __future__ import annotations

import argparse
import importlib
import os
import subprocess
import sys
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import (
    AdapterUnavailableError,
    LifecycleAction,
)


@dataclass(frozen=True)
class CommandResult:
    """Result from a command-style adapter action."""

    exit_code: int
    stdout: str = ""
    stderr: str = ""


class BridgeKind(StrEnum):
    """How the control shim reaches the provider."""

    COMMAND = "command"
    PYTHON_MODULE = "python_module"
    API_HOOK = "api_hook"


@dataclass(frozen=True)
class BridgeTarget:
    """Resolved provider-specific target for one lifecycle action."""

    kind: BridgeKind
    module_name: str = ""
    endpoint: str = ""
    command_argv: tuple[str, ...] = ()
    env: tuple[tuple[str, str], ...] = ()
    working_directory: str = ""


@dataclass(frozen=True)
class ControlRequest:
    """Parsed control-shim request."""

    component: str
    adapter: str
    action: LifecycleAction
    run_id: str
    job_id: str
    trace_id: str
    policy_version: str
    desired_units: int
    queue: str
    deterministic_seed: int
    bridge_target: BridgeTarget


@runtime_checkable
class CommandRunner(Protocol):
    """Runner for one-shot command invocations."""

    def run(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
    ) -> CommandResult:
        """Run one command and return its result."""


@runtime_checkable
class LifecycleHook(Protocol):
    """Provider bridge hook contract."""

    def handle_lifecycle(self, request: ControlRequest) -> int | bool | CommandResult | None:
        """Handle one lifecycle request."""


def _artifact_matches(
    artifact: runtime_pb2.RuntimeArtifact,
    *,
    component: str,
    adapter: str,
    action: LifecycleAction,
) -> bool:
    artifact_component = artifact.attributes.get("component", "").strip().casefold()
    artifact_adapter = artifact.attributes.get("adapter", "").strip().casefold()
    artifact_action = artifact.attributes.get("action", "").strip().casefold()
    return (
        (not artifact_component or artifact_component == component.casefold())
        and (not artifact_adapter or artifact_adapter == adapter.casefold())
        and (not artifact_action or artifact_action == action.value.casefold())
    )


def _artifact_value(
    manifest: runtime_pb2.RuntimeManifest,
    *,
    component: str,
    adapter: str,
    action: LifecycleAction,
    kinds: set[str],
    attribute_keys: tuple[str, ...],
) -> str:
    for artifact in manifest.artifacts:
        if artifact.kind.casefold() not in kinds:
            continue
        if not _artifact_matches(artifact, component=component, adapter=adapter, action=action):
            continue
        for key in attribute_keys:
            value = artifact.attributes.get(key, "").strip()
            if value:
                return value
        if artifact.uri.strip():
            return artifact.uri.strip()
    return ""


def manifest_typed_environment(
    manifest: runtime_pb2.RuntimeManifest,
) -> tuple[tuple[str, str], ...]:
    """Return only typed manifest environment entries."""
    return tuple(sorted((key, value) for key, value in manifest.environment.items()))


def resolve_bridge_target(
    manifest: runtime_pb2.RuntimeManifest,
    *,
    component: str,
    adapter: str,
    action: LifecycleAction,
    default_module_name: str,
    preferred_kind: BridgeKind,
) -> BridgeTarget:
    """Resolve command/API/module target from typed manifest fields."""
    command_argv = (
        tuple(token for token in (*manifest.command, *manifest.args) if token.strip())
        if component.casefold() == "execution" and action is LifecycleAction.LAUNCH
        else ()
    )
    target_env = manifest_typed_environment(manifest)
    working_directory = manifest.working_directory.strip()
    module_name = _artifact_value(
        manifest,
        component=component,
        adapter=adapter,
        action=action,
        kinds={"python_module", "module", "lifecycle_hook"},
        attribute_keys=("module", "hook_module", "python_module"),
    )
    endpoint = _artifact_value(
        manifest,
        component=component,
        adapter=adapter,
        action=action,
        kinds={"endpoint", "api_endpoint", "service_endpoint", "control_endpoint"},
        attribute_keys=("endpoint", "url", "base_url"),
    )
    if command_argv:
        return BridgeTarget(
            kind=BridgeKind.COMMAND,
            command_argv=command_argv,
            env=target_env,
            working_directory=working_directory,
        )
    if endpoint or preferred_kind is BridgeKind.API_HOOK:
        return BridgeTarget(
            kind=BridgeKind.API_HOOK,
            module_name=module_name or default_module_name,
            endpoint=endpoint,
            env=target_env,
            working_directory=working_directory,
        )
    if module_name or preferred_kind is BridgeKind.PYTHON_MODULE:
        return BridgeTarget(
            kind=BridgeKind.PYTHON_MODULE,
            module_name=module_name or default_module_name,
            env=target_env,
            working_directory=working_directory,
        )
    return BridgeTarget(
        kind=preferred_kind,
        module_name=module_name or default_module_name,
        env=target_env,
        working_directory=working_directory,
    )


def bridge_target_cli_args(target: BridgeTarget) -> tuple[str, ...]:
    """Encode one bridge target into control-shim CLI args."""
    args: list[str] = ["--bridge-kind", target.kind.value]
    if target.module_name:
        args.extend(["--bridge-module", target.module_name])
    if target.endpoint:
        args.extend(["--bridge-endpoint", target.endpoint])
    if target.working_directory:
        args.extend(["--target-working-directory", target.working_directory])
    for token in target.command_argv:
        args.extend(["--target-argv", token])
    for key, value in target.env:
        args.extend(["--target-env", f"{key}={value}"])
    return tuple(args)


def _parse_key_value(items: list[str]) -> tuple[tuple[str, str], ...]:
    env: list[tuple[str, str]] = []
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key:
            raise AdapterUnavailableError(f"invalid target env entry: {item}")
        env.append((key, value))
    return tuple(env)


def _parse_request(namespace: argparse.Namespace) -> ControlRequest:
    return ControlRequest(
        component=namespace.component,
        adapter=namespace.adapter,
        action=LifecycleAction(namespace.action),
        run_id=namespace.run_id,
        job_id=namespace.job_id,
        trace_id=namespace.trace_id,
        policy_version=namespace.policy_version,
        desired_units=int(namespace.desired_units),
        queue=namespace.queue,
        deterministic_seed=int(namespace.deterministic_seed),
        bridge_target=BridgeTarget(
            kind=BridgeKind(namespace.bridge_kind),
            module_name=namespace.bridge_module,
            endpoint=namespace.bridge_endpoint,
            command_argv=tuple(namespace.target_argv),
            env=_parse_key_value(namespace.target_env),
            working_directory=namespace.target_working_directory,
        ),
    )


def _default_command_runner(
    *,
    argv: tuple[str, ...],
    env: tuple[tuple[str, str], ...],
    working_directory: str,
) -> CommandResult:
    completed = subprocess.run(
        argv,
        env={**dict(os_env()), **dict(env)},
        cwd=working_directory or None,
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandResult(
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _resolve_hook(module: Any, request: ControlRequest) -> Any:
    action_hook = getattr(module, request.action.value, None)
    if callable(action_hook):
        return action_hook
    generic_hook = getattr(module, "handle_lifecycle", None)
    if callable(generic_hook):
        return generic_hook
    raise AdapterUnavailableError(
        f"bridge module {request.bridge_target.module_name} does not expose lifecycle hooks"
    )


def execute_control_request(
    request: ControlRequest,
    *,
    module_loader: Any = importlib.import_module,
    command_runner: CommandRunner | None = None,
) -> CommandResult:
    """Execute one parsed control request."""
    target = request.bridge_target
    if target.kind is BridgeKind.COMMAND:
        if not target.command_argv:
            raise AdapterUnavailableError("command bridge target is missing command argv")
        runner = command_runner.run if command_runner is not None else _default_command_runner
        merged_env = tuple(
            sorted(
                {
                    **{
                        "TGSRL_COMPONENT": request.component,
                        "TGSRL_ADAPTER": request.adapter,
                        "TGSRL_ACTION": request.action.value,
                        "TGSRL_RUN_ID": request.run_id,
                        "TGSRL_JOB_ID": request.job_id,
                        "TGSRL_TRACE_ID": request.trace_id,
                        "TGSRL_POLICY_VERSION": request.policy_version,
                        "TGSRL_DESIRED_UNITS": str(request.desired_units),
                        "TGSRL_QUEUE": request.queue,
                        "TGSRL_DETERMINISTIC_SEED": str(request.deterministic_seed),
                    },
                    **dict(target.env),
                }.items()
            )
        )
        return runner(
            argv=target.command_argv,
            env=merged_env,
            working_directory=target.working_directory,
        )
    if not target.module_name:
        raise AdapterUnavailableError("bridge module target is missing module name")
    if target.kind is BridgeKind.API_HOOK and not target.endpoint:
        raise AdapterUnavailableError("api bridge target is missing endpoint")
    try:
        module = module_loader(target.module_name)
    except ModuleNotFoundError as error:
        raise AdapterUnavailableError(
            f"bridge module {target.module_name} is unavailable"
        ) from error
    hook = _resolve_hook(module, request)
    result = hook(request)
    if isinstance(result, CommandResult):
        return result
    if isinstance(result, bool):
        return CommandResult(exit_code=0 if result else 1)
    if isinstance(result, int):
        return CommandResult(exit_code=result)
    if result is None:
        return CommandResult(exit_code=0)
    raise AdapterUnavailableError(
        f"bridge hook {target.module_name} returned unsupported result type {type(result)!r}"
    )


def execute_control_argv(
    argv: tuple[str, ...] | list[str],
    *,
    module_loader: Any = importlib.import_module,
    command_runner: CommandRunner | None = None,
) -> CommandResult:
    """Parse control-shim argv and execute the requested bridge."""
    namespace = _parser().parse_args(list(argv))
    request = _parse_request(namespace)
    return execute_control_request(
        request,
        module_loader=module_loader,
        command_runner=command_runner,
    )


def os_env() -> tuple[tuple[str, str], ...]:
    """Return current process environment as stable pairs."""
    return tuple(sorted((key, value) for key, value in os.environ.items()))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", required=True)
    parser.add_argument("--adapter", required=True)
    parser.add_argument("--action", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--trace-id", required=True)
    parser.add_argument("--policy-version", default="")
    parser.add_argument("--desired-units", default="1")
    parser.add_argument("--queue", default="")
    parser.add_argument("--deterministic-seed", default="0")
    parser.add_argument("--bridge-kind", required=True)
    parser.add_argument("--bridge-module", default="")
    parser.add_argument("--bridge-endpoint", default="")
    parser.add_argument("--target-argv", action="append", default=[])
    parser.add_argument("--target-env", action="append", default=[])
    parser.add_argument("--target-working-directory", default="")
    return parser


def main() -> None:
    try:
        result = execute_control_argv(sys.argv[1:])
    except AdapterUnavailableError as error:
        print(f"UNAVAILABLE: {error}", file=sys.stderr)
        raise SystemExit(69) from error
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
    raise SystemExit(result.exit_code)


if __name__ == "__main__":
    main()
