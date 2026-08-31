"""Atomic trace ingestion with stream-boundary and causal-order validation."""

from __future__ import annotations

import builtins
import hashlib
from dataclasses import dataclass, field

from tgsrl.v1 import trace_pb2

from tgsrl_runtime.proto_utils import clone_message
from tgsrl_runtime.trace import TraceNormalizer


class TraceIngestError(ValueError):
    """Raised when an event batch violates its recorded stream boundary."""


def _causal_key(event: trace_pb2.TraceEvent) -> tuple[str, str, int, int, str]:
    sequence = event.sequence if event.sequence else (1 << 64) - 1
    logical_time = event.occurred_at.seconds * 1_000_000_000 + event.occurred_at.nanos
    return event.execution_id, event.sandbox_id, sequence, logical_time, event.event_id


@dataclass
class TraceIngestor:
    """Normalize and atomically retain trace events grouped by run identifier."""

    normalizer: TraceNormalizer = field(default_factory=TraceNormalizer)
    _by_run: dict[str, builtins.list[trace_pb2.TraceEvent]] = field(default_factory=dict)

    def ingest(
        self, run_id: str, events: builtins.list[trace_pb2.TraceEvent]
    ) -> builtins.list[trace_pb2.TraceEvent]:
        if not run_id.strip() or run_id != run_id.strip():
            raise TraceIngestError("run_id must be nonblank and canonical")
        normalized = self.normalizer.normalize(events)
        self._validate_stream(run_id, normalized)
        current = [clone_message(event) for event in self._by_run.get(run_id, ())]
        try:
            candidate = self.normalizer.normalize([*current, *normalized])
            self._validate_sequence_ownership(candidate)
        except ValueError as error:
            raise TraceIngestError(str(error)) from error
        self._by_run[run_id] = [clone_message(event) for event in candidate]
        return self.list_causal(run_id)

    def ingest_batch(
        self, run_id: str, batch: trace_pb2.TraceEventBatch
    ) -> builtins.list[trace_pb2.TraceEvent]:
        events = list(batch.events)
        if not events:
            raise TraceIngestError("trace batch must contain events")
        if batch.run_id and batch.run_id != run_id:
            raise TraceIngestError("trace batch run_id does not match ingestion run")
        if any(event.execution_id != batch.execution_id for event in events):
            raise TraceIngestError("trace batch execution_id does not match its events")
        sequences = [event.sequence for event in events]
        if any(sequence <= 0 for sequence in sequences):
            raise TraceIngestError("trace batch events require positive sequences")
        if batch.first_sequence != min(sequences):
            raise TraceIngestError("first_sequence does not match batch events")
        if batch.last_sequence and batch.last_sequence != max(sequences):
            raise TraceIngestError("last_sequence does not match batch events")
        if batch.trace_id and any(
            event.trace_id and event.trace_id != batch.trace_id for event in events
        ):
            raise TraceIngestError("trace batch trace_id does not match its events")
        if batch.data_kind and any(
            event.data_kind and event.data_kind != batch.data_kind for event in events
        ):
            raise TraceIngestError("trace batch data_kind does not match its events")
        return self.ingest(run_id, events)

    @staticmethod
    def _validate_stream(run_id: str, events: builtins.list[trace_pb2.TraceEvent]) -> None:
        if any(event.run_id and event.run_id != run_id for event in events):
            raise TraceIngestError("event run_id does not match ingestion run")
        execution_ids = {event.execution_id for event in events}
        trace_ids = {event.trace_id for event in events if event.trace_id}
        data_kinds = {event.data_kind for event in events if event.data_kind}
        if len(execution_ids) > 1:
            raise TraceIngestError("one ingestion batch may contain only one execution_id")
        if len(trace_ids) > 1:
            raise TraceIngestError("one ingestion batch may contain only one trace_id")
        if len(data_kinds) > 1:
            raise TraceIngestError("one ingestion batch may contain only one data_kind")

    @staticmethod
    def _validate_sequence_ownership(
        events: builtins.list[trace_pb2.TraceEvent],
    ) -> None:
        owners: dict[tuple[str, str, str, int, int], str] = {}
        for event in events:
            if not event.sequence:
                continue
            key = (
                event.execution_id,
                event.stage_id,
                event.sandbox_id,
                event.generation,
                event.sequence,
            )
            previous = owners.setdefault(key, event.event_id)
            if previous != event.event_id:
                raise TraceIngestError(f"sequence {event.sequence} is owned by conflicting events")

    def list(self, run_id: str) -> builtins.list[trace_pb2.TraceEvent]:
        return [clone_message(event) for event in self._by_run.get(run_id, ())]

    def list_causal(self, run_id: str) -> builtins.list[trace_pb2.TraceEvent]:
        return sorted(self.list(run_id), key=_causal_key)

    def source_digest(self, run_id: str) -> str:
        digest = hashlib.sha256()
        for event in self.list_causal(run_id):
            wire = event.SerializeToString(deterministic=True)
            digest.update(len(wire).to_bytes(8, "big"))
            digest.update(wire)
        return digest.hexdigest()
