"""Shared runtime execution types and protocols."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from tgsrl.v1 import runtime_pb2

from adapters import (
    AdapterErrorKind,
    CommandResult,
    LaunchSpec,
    LifecycleAction,
    ProcessHandle,
    RunnerKind,
)


@dataclass(frozen=True)
class ProcessStatus:
    """Observed process state returned by an injected process driver."""

    state: int
    exit_code: int | None = None
    detail: str = ""


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


class ProcessDriver(Protocol):
    """Long-lived process runner used for adapter launch actions and reconciliation."""

    def start(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> ProcessHandle:
        """Start one process and return its handle."""

    def inspect(
        self,
        *,
        handle: ProcessHandle,
        timeout_seconds: float | None = None,
    ) -> ProcessStatus:
        """Inspect one previously started process handle."""


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
    process_handle: ProcessHandle | None = None
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
    reconciled: bool = False

    @property
    def ok(self) -> bool:
        return all(item.error is None for item in self.component_results)


@dataclass(frozen=True)
class ComponentProcessRecord:
    """Persistable process tracking record used for restart reconciliation."""

    component: str
    generation: int
    handle: ProcessHandle
    requested_state: int
    observed_state: int


@dataclass(frozen=True)
class ExecutorSnapshot:
    """Serializable executor state needed for restart reconciliation."""

    manifests: tuple[runtime_pb2.RuntimeManifest, ...]
    runtime_units: tuple[tuple[str, tuple[runtime_pb2.RuntimeUnit, ...]], ...]
    generations: tuple[tuple[str, int], ...]
    desired_states: tuple[tuple[str, int], ...]
    processes: tuple[tuple[str, tuple[ComponentProcessRecord, ...]], ...]


@dataclass(frozen=True)
class DriverBehavior:
    """Configurable fake-driver outcome for one action."""

    result: CommandResult | None = None
    exception: Exception | None = None


type RecoveredProcesses = Sequence[ComponentProcessRecord]
