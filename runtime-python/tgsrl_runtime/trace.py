"""Validation and deterministic normalization for canonical trace messages."""

from collections.abc import Iterable

from google.protobuf.message import DecodeError
from tgsrl.v1 import execution_pb2, trace_pb2


class TraceValidationError(ValueError):
    """Raised when a trace event lacks required semantic context."""


_PHASE_LABELS = {
    "prefill": execution_pb2.PHASE_KIND_PREFILL,
    "decode": execution_pb2.PHASE_KIND_DECODE,
    "tool_wait": execution_pb2.PHASE_KIND_TOOL_WAIT,
    "tool-wait": execution_pb2.PHASE_KIND_TOOL_WAIT,
    "kv_restore": execution_pb2.PHASE_KIND_KV_RESTORE,
    "kv-restore": execution_pb2.PHASE_KIND_KV_RESTORE,
    "reference": execution_pb2.PHASE_KIND_REFERENCE,
    "reward": execution_pb2.PHASE_KIND_REWARD,
    "actor": execution_pb2.PHASE_KIND_ACTOR,
    "optimizer": execution_pb2.PHASE_KIND_OPTIMIZER,
    "weight_sync": execution_pb2.PHASE_KIND_WEIGHT_SYNC,
    "weight-sync": execution_pb2.PHASE_KIND_WEIGHT_SYNC,
    "idle": execution_pb2.PHASE_KIND_IDLE,
}


def clone_event(event: trace_pb2.TraceEvent) -> trace_pb2.TraceEvent:
    """Return a detached generated message, preserving unknown wire fields."""
    clone = trace_pb2.TraceEvent()
    try:
        clone.ParseFromString(event.SerializeToString(deterministic=True))
    except DecodeError as error:  # pragma: no cover - protobuf objects serialize validly
        raise TraceValidationError("trace event cannot be cloned") from error
    return clone


class TraceNormalizer:
    """Validate, clone, deduplicate, and stably order trace events."""

    required_string_fields = (
        "event_id",
        "job_id",
        "execution_id",
        "phase_id",
        "stage_id",
        "algorithm",
        "policy_version",
        "decision_id",
    )

    def validate(self, event: trace_pb2.TraceEvent) -> None:
        missing = [name for name in self.required_string_fields if not getattr(event, name)]
        if missing:
            raise TraceValidationError(f"missing required trace fields: {', '.join(missing)}")
        if not event.HasField("occurred_at"):
            raise TraceValidationError("missing required trace field: occurred_at")
        try:
            event.occurred_at.ToDatetime()
        except (OverflowError, ValueError) as error:
            raise TraceValidationError(
                "occurred_at is outside the valid timestamp range"
            ) from error
        if event.event_type == trace_pb2.TRACE_EVENT_TYPE_UNKNOWN:
            raise TraceValidationError("event_type must not be UNKNOWN")
        if event.rollout_mode == trace_pb2.ROLLOUT_MODE_UNKNOWN:
            raise TraceValidationError("rollout_mode must not be UNKNOWN")

    def normalize_event(self, event: trace_pb2.TraceEvent) -> trace_pb2.TraceEvent:
        self.validate(event)
        normalized = clone_event(event)
        if normalized.phase_kind == execution_pb2.PHASE_KIND_UNKNOWN:
            raw_label = normalized.raw_phase_label or normalized.phase_id
            normalized.raw_phase_label = raw_label
            normalized.phase_kind = _PHASE_LABELS.get(
                raw_label.strip().casefold(), execution_pb2.PHASE_KIND_UNKNOWN
            )
        return normalized

    def normalize(self, events: Iterable[trace_pb2.TraceEvent]) -> list[trace_pb2.TraceEvent]:
        """Return unique events ordered by timestamp, source revision, and ID."""
        by_id: dict[str, trace_pb2.TraceEvent] = {}
        wire_by_id: dict[str, bytes] = {}
        for source in events:
            normalized = self.normalize_event(source)
            wire = normalized.SerializeToString(deterministic=True)
            previous = wire_by_id.get(normalized.event_id)
            if previous is not None and previous != wire:
                raise TraceValidationError(f"conflicting duplicate event_id: {normalized.event_id}")
            by_id[normalized.event_id] = normalized
            wire_by_id[normalized.event_id] = wire
        return sorted(by_id.values(), key=self.sort_key)

    @staticmethod
    def sort_key(event: trace_pb2.TraceEvent) -> tuple[int, int, str]:
        logical_time = event.occurred_at.seconds * 1_000_000_000 + event.occurred_at.nanos
        raw_revision = event.attributes.get("source_revision", str(event.sequence))
        try:
            source_revision = int(raw_revision)
        except ValueError as error:
            raise TraceValidationError(
                f"source_revision must be an integer for event {event.event_id}"
            ) from error
        return logical_time, source_revision, event.event_id
