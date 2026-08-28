"""Deterministic trace and concrete micro-stage aggregation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from tgsrl.v1 import experiment_pb2, trace_pb2

from tgsrl_runtime.trace import TraceNormalizer


@dataclass(frozen=True, order=True, slots=True)
class MicroStageKey:
    """Identity of one concrete stage attempt in a recorded execution."""

    run_id: str
    trace_id: str
    execution_id: str
    stage_id: str
    generation: int
    attempt: int


@dataclass(frozen=True, slots=True)
class MicroStageAggregate:
    """Lifecycle and observability data derived from one micro-stage attempt."""

    key: MicroStageKey
    phase_id: str
    phase_kind: int
    first_sequence: int
    last_sequence: int
    started_at: datetime | None
    completed_at: datetime | None
    duration: timedelta | None
    event_count: int
    event_ids: tuple[str, ...]
    decision_ids: tuple[str, ...]
    policy_versions: tuple[str, ...]
    max_buffer_level: int
    safe_point_count: int
    complete: bool
    clock_skew: bool


@dataclass(frozen=True, slots=True)
class TraceSummary:
    """Derived trace metrics used by checkpoints, intents, and experiments."""

    event_count: int
    safe_point_count: int
    policy_publish_count: int
    phase_counts: dict[str, int]
    max_buffer_level: int
    micro_stage_count: int
    completed_micro_stage_count: int
    incomplete_micro_stage_count: int
    total_micro_stage_seconds: float
    micro_stages: tuple[MicroStageAggregate, ...]


def _event_time(event: trace_pb2.TraceEvent) -> datetime:
    return event.occurred_at.ToDatetime(tzinfo=UTC)


def _causal_key(event: trace_pb2.TraceEvent) -> tuple[int, int, str]:
    timestamp = event.occurred_at.seconds * 1_000_000_000 + event.occurred_at.nanos
    sequence = event.sequence if event.sequence else (1 << 64) - 1
    return sequence, timestamp, event.event_id


def _stable_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


class TraceAggregator:
    """Pair lifecycle events and summarize stable micro-stage metrics."""

    def __init__(self, normalizer: TraceNormalizer | None = None) -> None:
        self._normalizer = normalizer or TraceNormalizer()

    def aggregate_micro_stages(
        self, events: Iterable[trace_pb2.TraceEvent]
    ) -> tuple[MicroStageAggregate, ...]:
        normalized = self._normalizer.normalize(events)
        groups: dict[tuple[str, str, str, str, int], list[trace_pb2.TraceEvent]] = {}
        for event in normalized:
            base_key = (
                event.run_id,
                event.trace_id,
                event.execution_id,
                event.stage_id,
                event.generation,
            )
            groups.setdefault(base_key, []).append(event)

        aggregates: list[MicroStageAggregate] = []
        for base_key, group_events in sorted(groups.items()):
            attempt = 0
            current: list[trace_pb2.TraceEvent] = []
            for event in sorted(group_events, key=_causal_key):
                if event.event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED:
                    if current:
                        attempt += 1
                        aggregates.append(self._finalize(base_key, attempt, current))
                    current = [event]
                    continue
                if not current:
                    current = [event]
                else:
                    current.append(event)
                if event.event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED:
                    attempt += 1
                    aggregates.append(self._finalize(base_key, attempt, current))
                    current = []
            if current:
                attempt += 1
                aggregates.append(self._finalize(base_key, attempt, current))
        return tuple(sorted(aggregates, key=lambda item: item.key))

    @staticmethod
    def _finalize(
        base_key: tuple[str, str, str, str, int],
        attempt: int,
        events: list[trace_pb2.TraceEvent],
    ) -> MicroStageAggregate:
        started = next(
            (
                event
                for event in events
                if event.event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED
            ),
            None,
        )
        completed = next(
            (
                event
                for event in reversed(events)
                if event.event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED
            ),
            None,
        )
        started_at = _event_time(started) if started is not None else None
        completed_at = _event_time(completed) if completed is not None else None
        clock_skew = bool(
            started_at is not None and completed_at is not None and completed_at < started_at
        )
        duration = (
            completed_at - started_at
            if started_at is not None and completed_at is not None and not clock_skew
            else None
        )
        first = events[0]
        return MicroStageAggregate(
            key=MicroStageKey(*base_key, attempt),
            phase_id=first.phase_id,
            phase_kind=first.phase_kind,
            first_sequence=min(event.sequence for event in events),
            last_sequence=max(event.sequence for event in events),
            started_at=started_at,
            completed_at=completed_at,
            duration=duration,
            event_count=len(events),
            event_ids=tuple(event.event_id for event in events),
            decision_ids=_stable_unique(event.decision_id for event in events),
            policy_versions=_stable_unique(event.policy_version for event in events),
            max_buffer_level=max(event.buffer_level for event in events),
            safe_point_count=sum(event.safe_point for event in events),
            complete=started is not None and completed is not None and not clock_skew,
            clock_skew=clock_skew,
        )

    def summarize(self, events: Iterable[trace_pb2.TraceEvent]) -> TraceSummary:
        normalized = self._normalizer.normalize(events)
        phase_counts: dict[str, int] = {}
        safe_point_count = 0
        policy_publish_count = 0
        max_buffer_level = 0
        for event in normalized:
            phase_counts[event.phase_id] = phase_counts.get(event.phase_id, 0) + 1
            safe_point_count += int(event.safe_point)
            policy_publish_count += int(
                event.event_type == trace_pb2.TRACE_EVENT_TYPE_POLICY_PUBLISHED
            )
            max_buffer_level = max(max_buffer_level, event.buffer_level)
        micro_stages = self.aggregate_micro_stages(normalized)
        completed = tuple(stage for stage in micro_stages if stage.complete)
        return TraceSummary(
            event_count=len(normalized),
            safe_point_count=safe_point_count,
            policy_publish_count=policy_publish_count,
            phase_counts=phase_counts,
            max_buffer_level=max_buffer_level,
            micro_stage_count=len(micro_stages),
            completed_micro_stage_count=len(completed),
            incomplete_micro_stage_count=len(micro_stages) - len(completed),
            total_micro_stage_seconds=sum(
                stage.duration.total_seconds() for stage in completed if stage.duration is not None
            ),
            micro_stages=micro_stages,
        )

    def to_metrics(self, summary: TraceSummary) -> list[experiment_pb2.MetricValue]:
        metrics = [
            experiment_pb2.MetricValue(
                name="event_count", value=float(summary.event_count), unit="count"
            ),
            experiment_pb2.MetricValue(
                name="safe_point_count",
                value=float(summary.safe_point_count),
                unit="count",
            ),
            experiment_pb2.MetricValue(
                name="policy_publish_count",
                value=float(summary.policy_publish_count),
                unit="count",
            ),
            experiment_pb2.MetricValue(
                name="max_buffer_level",
                value=float(summary.max_buffer_level),
                unit="count",
            ),
            experiment_pb2.MetricValue(
                name="micro_stage_count",
                value=float(summary.micro_stage_count),
                unit="count",
            ),
            experiment_pb2.MetricValue(
                name="completed_micro_stage_count",
                value=float(summary.completed_micro_stage_count),
                unit="count",
            ),
            experiment_pb2.MetricValue(
                name="incomplete_micro_stage_count",
                value=float(summary.incomplete_micro_stage_count),
                unit="count",
            ),
            experiment_pb2.MetricValue(
                name="micro_stage_duration",
                value=summary.total_micro_stage_seconds,
                unit="seconds",
            ),
        ]
        metrics.extend(
            experiment_pb2.MetricValue(
                name=f"phase_events:{phase_id}",
                value=float(count),
                unit="count",
                attributes={"phase_id": phase_id},
            )
            for phase_id, count in sorted(summary.phase_counts.items())
        )
        return metrics
