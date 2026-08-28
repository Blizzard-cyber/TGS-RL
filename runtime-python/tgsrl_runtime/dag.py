"""Incremental phase DAG and derived RL scheduling-gap classification."""

from collections.abc import Iterable
from enum import StrEnum
from typing import ClassVar

from tgsrl.v1 import execution_pb2, trace_pb2


class DAGError(ValueError):
    """Base class for malformed phase-graph updates."""


class CycleError(DAGError):
    """Raised when an update would introduce a dependency cycle."""


class GapKind(StrEnum):
    """Stable identifiers for the five research gap classes."""

    LONG_TAIL_WAIT = "long_tail_wait"
    TOOL_ENVIRONMENT_WAIT = "tool_environment_wait"
    ROLE_PHASE_MISALIGNMENT = "role_phase_misalignment"
    SERIAL_OR_WEIGHT_SYNC = "serial_or_weight_sync"
    INVALID_STALE_OUTPUT = "invalid_stale_output"
    UNKNOWN = "unknown"


def _clone_phase(phase: execution_pb2.Phase) -> execution_pb2.Phase:
    clone = execution_pb2.Phase()
    clone.CopyFrom(phase)
    return clone


class IncrementalDAG:
    """Mutable derived state whose public graph remains generated Proto."""

    def __init__(self, graph: execution_pb2.PhaseGraph | None = None) -> None:
        self._phases: dict[str, execution_pb2.Phase] = {}
        self._dependencies: dict[str, set[str]] = {}
        self._outgoing: dict[str, set[str]] = {}
        self._conditions: dict[tuple[str, str], str] = {}
        self._completed: set[str] = set()
        self._remaining_seconds: dict[str, float] = {}
        self._declared_entries: set[str] = set()
        if graph is not None:
            self.update_graph(graph)

    def update_graph(self, graph: execution_pb2.PhaseGraph) -> None:
        """Apply a graph atomically; any invalid member leaves this DAG unchanged."""
        candidate = self._copy()
        candidate._update_graph_in_place(graph)
        self._phases = candidate._phases
        self._dependencies = candidate._dependencies
        self._outgoing = candidate._outgoing
        self._conditions = candidate._conditions
        self._completed = candidate._completed
        self._remaining_seconds = candidate._remaining_seconds
        self._declared_entries = candidate._declared_entries

    def _copy(self) -> "IncrementalDAG":
        candidate = IncrementalDAG()
        candidate._phases = {
            phase_id: _clone_phase(phase) for phase_id, phase in self._phases.items()
        }
        candidate._dependencies = {
            phase_id: set(dependencies) for phase_id, dependencies in self._dependencies.items()
        }
        candidate._outgoing = {
            phase_id: set(targets) for phase_id, targets in self._outgoing.items()
        }
        candidate._conditions = dict(self._conditions)
        candidate._completed = set(self._completed)
        candidate._remaining_seconds = dict(self._remaining_seconds)
        candidate._declared_entries = set(self._declared_entries)
        return candidate

    def _update_graph_in_place(self, graph: execution_pb2.PhaseGraph) -> None:
        for phase in graph.phases:
            self.add_phase(phase)
        for edge in graph.edges:
            self.add_edge(edge.from_phase_id, edge.to_phase_id, condition=edge.condition)
        for entry in graph.entry_phase_ids:
            if entry not in self._phases:
                raise DAGError(f"entry phase does not exist: {entry}")
            self._declared_entries.add(entry)
        for entry in self._declared_entries:
            if self._dependencies.get(entry):
                raise DAGError(f"entry phase must be a graph root: {entry}")

    def add_phase(
        self, phase: execution_pb2.Phase, *, remaining_seconds: float | None = None
    ) -> None:
        if not phase.phase_id:
            raise DAGError("phase_id is required")
        if phase.phase_id in self._phases:
            existing = self._phases[phase.phase_id].SerializeToString(deterministic=True)
            incoming = phase.SerializeToString(deterministic=True)
            if existing != incoming:
                raise DAGError(f"phase already exists with different content: {phase.phase_id}")
        else:
            self._phases[phase.phase_id] = _clone_phase(phase)
        self._dependencies.setdefault(phase.phase_id, set())
        self._outgoing.setdefault(phase.phase_id, set())
        if remaining_seconds is not None:
            self.set_remaining_time(phase.phase_id, remaining_seconds)

    def add_edge(self, source: str, target: str, *, condition: str = "") -> None:
        if not source or not target:
            raise DAGError("edge endpoints are required")
        if target in self._outgoing.setdefault(source, set()):
            if self._conditions.get((source, target), "") != condition:
                raise DAGError(f"edge already exists with different condition: {source}->{target}")
            return
        self._dependencies.setdefault(source, set())
        self._dependencies.setdefault(target, set())
        self._outgoing.setdefault(target, set())
        if source == target or self._path_exists(target, source):
            raise CycleError(f"edge would create a cycle: {source}->{target}")
        self._outgoing[source].add(target)
        self._dependencies[target].add(source)
        self._conditions[(source, target)] = condition

    def _path_exists(self, source: str, target: str) -> bool:
        pending = [source]
        visited: set[str] = set()
        while pending:
            current = pending.pop()
            if current == target:
                return True
            if current in visited:
                continue
            visited.add(current)
            pending.extend(sorted(self._outgoing.get(current, ()), reverse=True))
        return False

    @property
    def uncertain_edges(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted(
                (source, target)
                for source, targets in self._outgoing.items()
                for target in targets
                if source not in self._phases or target not in self._phases
            )
        )

    @property
    def ready_set(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                phase_id
                for phase_id in self._phases
                if phase_id not in self._completed
                and (
                    not self._declared_entries
                    or phase_id in self._declared_entries
                    or bool(self._dependencies.get(phase_id))
                )
                and all(
                    dependency in self._completed
                    for dependency in self._dependencies.get(phase_id, ())
                )
            )
        )

    @property
    def completed(self) -> tuple[str, ...]:
        return tuple(sorted(self._completed))

    def mark_completed(self, phase_id: str) -> None:
        if phase_id not in self._phases:
            raise DAGError(f"cannot complete unknown phase: {phase_id}")
        self._completed.add(phase_id)
        self._remaining_seconds[phase_id] = 0.0

    def apply_event(self, event: trace_pb2.TraceEvent) -> None:
        if event.phase_id not in self._phases:
            self.add_phase(
                execution_pb2.Phase(
                    phase_id=event.phase_id,
                    display_name=event.raw_phase_label or event.phase_id,
                    kind=event.phase_kind,
                    parallelism=1,
                    max_attempts=1,
                    labels={"inferred": "true"},
                )
            )
        if event.event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED:
            self.mark_completed(event.phase_id)

    def set_remaining_time(self, phase_id: str, seconds: float) -> None:
        if phase_id not in self._phases:
            raise DAGError(f"cannot set remaining time for unknown phase: {phase_id}")
        if seconds < 0:
            raise DAGError("remaining time must be non-negative")
        self._remaining_seconds[phase_id] = seconds

    def remaining_time(self, phase_id: str) -> float | None:
        return self._remaining_seconds.get(phase_id)

    def graph(self) -> execution_pb2.PhaseGraph:
        entries = self._declared_entries or {
            phase_id for phase_id in self._phases if not self._dependencies.get(phase_id)
        }
        return execution_pb2.PhaseGraph(
            phases=[self._phases[phase_id] for phase_id in sorted(self._phases)],
            edges=[
                execution_pb2.PhaseEdge(
                    from_phase_id=source,
                    to_phase_id=target,
                    condition=self._conditions[(source, target)],
                )
                for source in sorted(self._outgoing)
                for target in sorted(self._outgoing[source])
            ],
            entry_phase_ids=sorted(entries),
        )

    def dependencies(self, phase_id: str) -> tuple[str, ...]:
        if phase_id not in self._phases:
            raise DAGError(f"unknown phase: {phase_id}")
        return tuple(sorted(self._dependencies.get(phase_id, ())))

    def complete_many(self, phase_ids: Iterable[str]) -> None:
        for phase_id in phase_ids:
            self.mark_completed(phase_id)


class GapClassifier:
    """Classify derived gaps without changing the source trace message."""

    _true_values: ClassVar[frozenset[str]] = frozenset({"1", "true", "yes", "stale"})

    def classify(self, event: trace_pb2.TraceEvent) -> GapKind:
        attributes = {key.casefold(): value.casefold() for key, value in event.attributes.items()}
        reason = attributes.get("gap_reason", "")
        stale = attributes.get("stale", "") in self._true_values
        if stale or reason in {"version_drift", "invalid_stale_output"}:
            return GapKind.INVALID_STALE_OUTPUT
        if event.phase_kind == execution_pb2.PHASE_KIND_TOOL_WAIT or reason in {
            "tool_wait",
            "environment_wait",
        }:
            return GapKind.TOOL_ENVIRONMENT_WAIT
        if event.phase_kind in {
            execution_pb2.PHASE_KIND_OPTIMIZER,
            execution_pb2.PHASE_KIND_WEIGHT_SYNC,
        } or reason in {"serial_section", "weight_sync"}:
            return GapKind.SERIAL_OR_WEIGHT_SYNC
        if reason in {"long_tail", "barrier_wait"} or attributes.get("waiting_for") in {
            "barrier",
            "slow_rank",
        }:
            return GapKind.LONG_TAIL_WAIT
        if event.phase_kind == execution_pb2.PHASE_KIND_IDLE or reason in {
            "role_phase_misalignment",
            "trainer_starvation",
            "buffer_backlog",
        }:
            return GapKind.ROLE_PHASE_MISALIGNMENT
        return GapKind.UNKNOWN
