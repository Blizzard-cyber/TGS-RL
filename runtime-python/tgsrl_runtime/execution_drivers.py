"""Runtime execution driver implementations."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field

from adapters import CommandResult, LifecycleAction
from tgsrl_runtime.execution_types import DriverBehavior


@dataclass
class FakeCommandDriver:
    """Deterministic command driver used by local and unit-test runtime profiles."""

    behaviors: dict[tuple[str, str, str], DriverBehavior] = field(default_factory=dict)
    calls: list[tuple[tuple[str, ...], tuple[tuple[str, str], ...], str, float | None]] = field(
        default_factory=list
    )

    def set_behavior(
        self,
        *,
        action: LifecycleAction,
        component: str,
        run_id: str,
        result: CommandResult | None = None,
        exception: Exception | None = None,
    ) -> None:
        self.behaviors[(action.value, component, run_id)] = DriverBehavior(
            result=result,
            exception=exception,
        )

    def run(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        self.calls.append((argv, env, working_directory, timeout_seconds))
        environment = dict(env)
        action = environment.get("TGSRL_ACTION", "")
        component = environment.get("TGSRL_COMPONENT", "")
        run_id = environment.get("TGSRL_RUN_ID", "")
        behavior = self.behaviors.get((action, component, run_id))
        if behavior is not None:
            if behavior.exception is not None:
                raise behavior.exception
            result = behavior.result if behavior.result is not None else CommandResult(exit_code=0)
        else:
            result = CommandResult(exit_code=0)
        return result


@dataclass
class SubprocessCommandDriver:
    """OS-backed runner for explicit worker lifecycle bridge commands."""

    def run(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        completed = subprocess.run(
            argv,
            check=False,
            capture_output=True,
            cwd=working_directory or None,
            env=self._merged_env(env),
            text=True,
            timeout=timeout_seconds,
        )
        return CommandResult(
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    def _merged_env(self, env: tuple[tuple[str, str], ...]) -> dict[str, str]:
        import os

        merged = dict(os.environ)
        merged.update(dict(env))
        return merged
