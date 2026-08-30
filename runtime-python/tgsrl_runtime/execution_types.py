"""Shared runtime execution types and protocols."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from tgsrl.v1 import runtime_pb2

from adapters import (
    AdapterErrorKind,
    CommandResult,
    LaunchSpec,
    LifecycleAction,
    RunnerKind,
)


class CommandDriver(Protocol):
    """One-shot command runner used for adapter command actions."""

    def run(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        """Run one command and return its result."""


@dataclass(frozen=True)
class NormalizedActionError:
    """Stable action failure returned by the executor."""

    kind: AdapterErrorKind
    retryable: bool
    summary: str
    resulting_state: int
    exit_code: int | None = None


@dataclass(frozen=True)
class ComponentActionResult:
    """Observed result for one component action."""

    component: str
    action: LifecycleAction
    runner_kind: RunnerKind
    generation: int
    requested_state: int
    observed_state: int
    launch_spec: LaunchSpec
    command_result: CommandResult | None = None
    error: NormalizedActionError | None = None


@dataclass(frozen=True)
class RuntimeExecutionResult:
    """Result for one supervised runtime action."""

    manifest: runtime_pb2.RuntimeManifest
    runtime_units: tuple[runtime_pb2.RuntimeUnit, ...]
    action: LifecycleAction
    generation: int
    cursor: str
    component_results: tuple[ComponentActionResult, ...]
    idempotent: bool = False
    checkpoint_ref: str = ""

    @property
    def ok(self) -> bool:
        return all(item.error is None for item in self.component_results)


@dataclass(frozen=True)
class DriverBehavior:
    """Configurable fake-driver outcome for one action."""

    result: CommandResult | None = None
    exception: Exception | None = None
