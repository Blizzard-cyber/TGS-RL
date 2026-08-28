"""In-memory checkpoint registry for fake/local runtime lifecycles."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class CheckpointRecord:
    """One immutable checkpoint completion record."""

    checkpoint_ref: str
    run_id: str
    completed_at: datetime
    state_digest: str


type CheckpointEntries = Sequence[tuple[str, Sequence[CheckpointRecord]]]


@dataclass
class CheckpointStore:
    """Append-only checkpoint store by run."""

    _items: dict[str, list[CheckpointRecord]] = field(default_factory=dict)

    def add(self, record: CheckpointRecord) -> CheckpointRecord:
        self._items.setdefault(record.run_id, []).append(record)
        return record

    def latest(self, run_id: str) -> CheckpointRecord | None:
        items = self._items.get(run_id, [])
        return items[-1] if items else None

    def list(self, run_id: str) -> tuple[CheckpointRecord, ...]:
        return tuple(self._items.get(run_id, ()))

    def run_ids(self) -> tuple[str, ...]:
        """Return run identifiers with checkpoints in stable order."""
        return tuple(sorted(self._items))

    def items(self) -> tuple[tuple[str, tuple[CheckpointRecord, ...]], ...]:
        """Return all checkpoints grouped by run identifier."""
        return tuple((run_id, self.list(run_id)) for run_id in self.run_ids())

    def restore(
        self,
        entries: CheckpointEntries,
        *,
        replace: bool = False,
    ) -> None:
        """Restore grouped checkpoints from public serialized state."""
        if replace:
            self._items = {}
        for run_id, records in entries:
            self._items[run_id] = list(records)
