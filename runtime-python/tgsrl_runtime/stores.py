"""In-memory stores for runtime manifests, units, sandboxes, and events."""

from __future__ import annotations

from dataclasses import dataclass, field

from tgsrl.v1 import runtime_pb2

from tgsrl_runtime.proto_utils import clone_message

type RuntimeUnitList = list[runtime_pb2.RuntimeUnit]
type SandboxList = list[runtime_pb2.Sandbox]
type SandboxEventList = list[runtime_pb2.SandboxEvent]
type RuntimeUnitEntries = list[tuple[str, RuntimeUnitList]]
type SandboxEntries = list[tuple[str, SandboxList]]
type SandboxEventEntries = list[tuple[str, SandboxEventList]]
type SequencedSandboxEvent = tuple[int, runtime_pb2.SandboxEvent]
type SequencedSandboxEventList = list[SequencedSandboxEvent]
type SequencedSandboxEventEntries = list[tuple[str, SequencedSandboxEventList]]


@dataclass
class RuntimeManifestStore:
    """Store manifests by run identifier."""

    _items: dict[str, runtime_pb2.RuntimeManifest] = field(default_factory=dict)

    def put(self, manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
        stored = clone_message(manifest)
        self._items[stored.run_id] = stored
        return clone_message(stored)

    def get(self, run_id: str) -> runtime_pb2.RuntimeManifest:
        return clone_message(self._items[run_id])

    def has(self, run_id: str) -> bool:
        return run_id in self._items

    def is_empty(self) -> bool:
        """Report whether the store contains any manifests."""
        return not self._items

    def run_ids(self) -> tuple[str, ...]:
        """Return stored run identifiers in stable order."""
        return tuple(sorted(self._items))

    def items(self) -> tuple[runtime_pb2.RuntimeManifest, ...]:
        """Return all stored manifests in stable order."""
        return tuple(self.get(run_id) for run_id in self.run_ids())

    def restore(
        self,
        manifests: list[runtime_pb2.RuntimeManifest] | tuple[runtime_pb2.RuntimeManifest, ...],
        *,
        replace: bool = False,
    ) -> None:
        """Restore manifests from public serialized state."""
        if replace:
            self._items = {}
        for manifest in manifests:
            stored = clone_message(manifest)
            self._items[stored.run_id] = stored


@dataclass
class RuntimeUnitStore:
    """Store runtime units grouped by run identifier."""

    _items: dict[str, RuntimeUnitList] = field(default_factory=dict)

    def replace(self, run_id: str, runtime_units: RuntimeUnitList) -> RuntimeUnitList:
        stored = [clone_message(unit) for unit in runtime_units]
        self._items[run_id] = stored
        return [clone_message(unit) for unit in stored]

    def list(self, run_id: str, *, stage_id: str = "") -> RuntimeUnitList:
        items = self._items.get(run_id, [])
        if not stage_id:
            return [clone_message(unit) for unit in items]
        return [clone_message(unit) for unit in items if unit.stage_id == stage_id]

    def update_states(
        self,
        run_id: str,
        *,
        state: int,
        sandbox_id_to_generation: dict[str, int] | None = None,
    ) -> RuntimeUnitList:
        updated: RuntimeUnitList = []
        for unit in self._items[run_id]:
            unit.state = state
            if sandbox_id_to_generation is not None:
                unit.generation = sandbox_id_to_generation.get(unit.sandbox_id, unit.generation)
            updated.append(clone_message(unit))
        return updated

    def put_many(self, run_id: str, runtime_units: RuntimeUnitList) -> None:
        self._items[run_id] = [clone_message(unit) for unit in runtime_units]

    def get_by_runtime_unit_id(self, run_id: str, runtime_unit_id: str) -> runtime_pb2.RuntimeUnit:
        for unit in self._items.get(run_id, []):
            if unit.runtime_unit_id == runtime_unit_id:
                return clone_message(unit)
        raise KeyError(runtime_unit_id)

    def get_by_sandbox_id(self, run_id: str, sandbox_id: str) -> runtime_pb2.RuntimeUnit:
        for unit in self._items.get(run_id, []):
            if unit.sandbox_id == sandbox_id:
                return clone_message(unit)
        raise KeyError(sandbox_id)

    def update_one(
        self, run_id: str, runtime_unit: runtime_pb2.RuntimeUnit
    ) -> runtime_pb2.RuntimeUnit:
        updated = clone_message(runtime_unit)
        items = self._items.get(run_id, [])
        for index, existing in enumerate(items):
            if existing.runtime_unit_id == updated.runtime_unit_id:
                items[index] = updated
                return clone_message(updated)
        items.append(updated)
        self._items[run_id] = items
        return clone_message(updated)

    def run_ids(self) -> tuple[str, ...]:
        """Return run identifiers with runtime units in stable order."""
        return tuple(sorted(self._items))

    def items(self) -> tuple[tuple[str, tuple[runtime_pb2.RuntimeUnit, ...]], ...]:
        """Return all stored runtime units grouped by run identifier."""
        return tuple((run_id, tuple(self.list(run_id))) for run_id in self.run_ids())

    def restore(self, entries: RuntimeUnitEntries, *, replace: bool = False) -> None:
        """Restore grouped runtime units from public serialized state."""
        if replace:
            self._items = {}
        for run_id, runtime_units in entries:
            self._items[run_id] = [clone_message(unit) for unit in runtime_units]


@dataclass
class SandboxStore:
    """Store sandboxes grouped by run identifier."""

    _items: dict[str, SandboxList] = field(default_factory=dict)

    def replace(self, run_id: str, sandboxes: SandboxList) -> SandboxList:
        stored = [clone_message(item) for item in sandboxes]
        self._items[run_id] = stored
        return [clone_message(item) for item in stored]

    def list(self, run_id: str, *, job_id: str = "") -> SandboxList:
        sandboxes = self._items.get(run_id, [])
        if not job_id:
            return [clone_message(item) for item in sandboxes]
        return [clone_message(item) for item in sandboxes if item.job_id == job_id]

    def update_states(self, run_id: str, *, state: int) -> SandboxList:
        updated: SandboxList = []
        for sandbox in self._items[run_id]:
            sandbox.state = state
            sandbox.generation += 1
            updated.append(clone_message(sandbox))
        return updated

    def generation_map(self, run_id: str) -> dict[str, int]:
        return {sandbox.sandbox_id: sandbox.generation for sandbox in self._items.get(run_id, [])}

    def get(self, run_id: str, sandbox_id: str) -> runtime_pb2.Sandbox:
        for sandbox in self._items.get(run_id, []):
            if sandbox.sandbox_id == sandbox_id:
                return clone_message(sandbox)
        raise KeyError(sandbox_id)

    def has(self, run_id: str, sandbox_id: str) -> bool:
        return any(sandbox.sandbox_id == sandbox_id for sandbox in self._items.get(run_id, []))

    def update_one(self, run_id: str, sandbox: runtime_pb2.Sandbox) -> runtime_pb2.Sandbox:
        updated = clone_message(sandbox)
        items = self._items.get(run_id, [])
        for index, existing in enumerate(items):
            if existing.sandbox_id == updated.sandbox_id:
                items[index] = updated
                return clone_message(updated)
        items.append(updated)
        self._items[run_id] = items
        return clone_message(updated)

    def run_ids(self) -> tuple[str, ...]:
        """Return run identifiers with sandboxes in stable order."""
        return tuple(sorted(self._items))

    def items(self) -> tuple[tuple[str, tuple[runtime_pb2.Sandbox, ...]], ...]:
        """Return all stored sandboxes grouped by run identifier."""
        return tuple((run_id, tuple(self.list(run_id))) for run_id in self.run_ids())

    def restore(self, entries: SandboxEntries, *, replace: bool = False) -> None:
        """Restore grouped sandboxes from public serialized state."""
        if replace:
            self._items = {}
        for run_id, sandboxes in entries:
            self._items[run_id] = [clone_message(item) for item in sandboxes]


@dataclass
class SandboxEventStore:
    """Append-only sandbox event log indexed by run identifier."""

    _items: dict[str, SequencedSandboxEventList] = field(default_factory=dict)
    _index: dict[str, SequencedSandboxEvent] = field(default_factory=dict)
    _next_sequence: int = 1

    def append(
        self,
        event: runtime_pb2.SandboxEvent,
        *,
        sequence: int | None = None,
    ) -> tuple[int, runtime_pb2.SandboxEvent]:
        stored = clone_message(event)
        existing = self._index.get(stored.event_id)
        if existing is not None:
            return existing[0], clone_message(existing[1])
        items = self._items.setdefault(stored.run_id, [])
        assigned = sequence if sequence is not None else self._next_sequence
        self._next_sequence = max(self._next_sequence, assigned + 1)
        items.append((assigned, stored))
        self._index[stored.event_id] = (assigned, stored)
        items.sort(key=lambda item: item[0])
        return assigned, clone_message(stored)

    def find_by_event_id(self, event_id: str) -> runtime_pb2.SandboxEvent | None:
        """Return one event by id across all runs, if present."""
        stored = self._index.get(event_id)
        if stored is not None:
            return clone_message(stored[1])
        return None

    def find_with_sequence_by_event_id(
        self, event_id: str
    ) -> tuple[int, runtime_pb2.SandboxEvent] | None:
        """Return one globally indexed event plus its append sequence, if present."""
        stored = self._index.get(event_id)
        if stored is not None:
            return stored[0], clone_message(stored[1])
        return None

    def sequence_for_event_id(self, event_id: str) -> int | None:
        """Return global append sequence for one event id, if present."""
        stored = self._index.get(event_id)
        return stored[0] if stored is not None else None

    def list(
        self, run_id: str, *, after_sequence: int = 0, after_event_id: str = ""
    ) -> SandboxEventList:
        events = self._items.get(run_id, [])
        if after_event_id:
            matched_sequence = self.sequence_for_event_id(after_event_id)
            if matched_sequence is not None:
                after_sequence = max(after_sequence, matched_sequence)
        return [clone_message(item) for sequence, item in events if sequence > after_sequence]

    def list_with_sequences(
        self, run_id: str, *, after_sequence: int = 0, after_event_id: str = ""
    ) -> SequencedSandboxEventList:
        events = self._items.get(run_id, [])
        if after_event_id:
            matched_sequence = self.sequence_for_event_id(after_event_id)
            if matched_sequence is not None:
                after_sequence = max(after_sequence, matched_sequence)
        return [
            (sequence, clone_message(item))
            for sequence, item in events
            if sequence > after_sequence
        ]

    def run_ids(self) -> tuple[str, ...]:
        """Return run identifiers with sandbox events in stable order."""
        return tuple(sorted(self._items))

    def items(self) -> tuple[tuple[str, tuple[runtime_pb2.SandboxEvent, ...]], ...]:
        """Return all stored sandbox events grouped by run identifier."""
        return tuple((run_id, tuple(self.list(run_id))) for run_id in self.run_ids())

    def restore(
        self,
        entries: SandboxEventEntries | SequencedSandboxEventEntries,
        *,
        replace: bool = False,
    ) -> None:
        """Restore grouped sandbox events from public serialized state."""
        if replace:
            self._items = {}
            self._index = {}
            self._next_sequence = 1
        for run_id, events in entries:
            restored: SequencedSandboxEventList = []
            for event in events:
                if (
                    isinstance(event, tuple)
                    and len(event) == 2
                    and isinstance(event[0], int)
                    and isinstance(event[1], runtime_pb2.SandboxEvent)
                ):
                    sequence = event[0]
                    cloned = clone_message(event[1])
                else:
                    sequence = self._next_sequence
                    cloned = clone_message(event)
                restored.append((sequence, cloned))
                self._index[cloned.event_id] = (sequence, cloned)
                self._next_sequence = max(self._next_sequence, sequence + 1)
            restored.sort(key=lambda item: item[0])
            self._items[run_id] = restored
