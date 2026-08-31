"""Deterministic trace and concrete micro-stage aggregation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from google.protobuf import timestamp_pb2
from tgsrl.v1 import execution_pb2, experiment_pb2, semantic_pb2, trace_pb2

from adapters.contracts import ContractValidationError, validate_contract_observation
from tgsrl_runtime.trace import TraceNormalizer


@dataclass(frozen=True, order=True, slots=True)
class MicroStageKey:
    """Identity of one concrete stage attempt in a recorded execution."""

    run_id: str
    trace_id: str
    execution_id: str
    stage_id: str
    sandbox_id: str
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
    latest_buffer_level: int
    latest_safe_point: bool
    latest_policy_version: str
    latest_phase_id: str
    latest_event_id: str
    latest_observation: execution_pb2.ContractObservation | None
    micro_stage_count: int
    completed_micro_stage_count: int
    incomplete_micro_stage_count: int
    total_micro_stage_seconds: float
    micro_stages: tuple[MicroStageAggregate, ...]
    latest_observations_by_stage: dict[str, execution_pb2.ContractObservation] = field(
        default_factory=dict
    )

    def latest_observation_for_stage(
        self, stage_id: str
    ) -> execution_pb2.ContractObservation | None:
        return self.latest_observations_by_stage.get(stage_id)


def _event_time(event: trace_pb2.TraceEvent) -> datetime:
    return event.occurred_at.ToDatetime(tzinfo=UTC)


def _causal_key(event: trace_pb2.TraceEvent) -> tuple[int, int, str]:
    timestamp = event.occurred_at.seconds * 1_000_000_000 + event.occurred_at.nanos
    sequence = event.sequence if event.sequence else (1 << 64) - 1
    return sequence, timestamp, event.event_id


def _stable_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _typed_fact_map(
    observation: execution_pb2.ContractObservation,
) -> dict[str, semantic_pb2.SemanticValue]:
    return {field.key: field.value for field in observation.typed_facts}


def _clone_observed_fact(
    fact: execution_pb2.ObservedFact,
) -> execution_pb2.ObservedFact:
    clone = execution_pb2.ObservedFact()
    clone.CopyFrom(fact)
    return clone


def _clone_component_version(
    version: execution_pb2.ComponentVersion,
) -> execution_pb2.ComponentVersion:
    clone = execution_pb2.ComponentVersion()
    clone.CopyFrom(version)
    return clone


def _source_revision(event: trace_pb2.TraceEvent) -> int:
    raw_revision = event.attributes.get("source_revision", str(event.sequence))
    try:
        revision = int(raw_revision)
    except ValueError as error:
        raise ContractValidationError(
            f"source_revision must be an integer for event {event.event_id}"
        ) from error
    if revision < 0:
        raise ContractValidationError(
            f"source_revision must be non-negative for event {event.event_id}"
        )
    return max(revision, 1)


def _uint64_fact(
    facts: dict[str, semantic_pb2.SemanticValue],
    key: str,
) -> int | None:
    value = facts.get(key)
    if value is None or value.WhichOneof("kind") != "uint64_value":
        return None
    return int(value.uint64_value)


def _bool_fact(
    facts: dict[str, semantic_pb2.SemanticValue],
    key: str,
) -> bool | None:
    value = facts.get(key)
    if value is None or value.WhichOneof("kind") != "bool_value":
        return None
    return bool(value.bool_value)


def _double_fact(
    facts: dict[str, semantic_pb2.SemanticValue],
    key: str,
) -> float | None:
    value = facts.get(key)
    if value is None or value.WhichOneof("kind") != "double_value":
        return None
    return float(value.double_value)


def _semantic_field(
    key: str,
    *,
    uint64: int | None = None,
    boolean: bool | None = None,
    double: float | None = None,
) -> semantic_pb2.SemanticField:
    if uint64 is not None:
        return semantic_pb2.SemanticField(
            key=key,
            value=semantic_pb2.SemanticValue(uint64_value=uint64),
        )
    if boolean is not None:
        return semantic_pb2.SemanticField(
            key=key,
            value=semantic_pb2.SemanticValue(bool_value=boolean),
        )
    if double is not None:
        return semantic_pb2.SemanticField(
            key=key,
            value=semantic_pb2.SemanticValue(double_value=double),
        )
    raise ValueError("one typed fact value is required")


def _latest_observation_from_event(
    event: trace_pb2.TraceEvent,
) -> execution_pb2.ContractObservation:
    if event.HasField("contract_observation"):
        observation = execution_pb2.ContractObservation()
        observation.CopyFrom(event.contract_observation)
    else:
        observation = execution_pb2.ContractObservation()
    typed_facts = _typed_fact_map(observation)
    explicit_facts: dict[str, execution_pb2.ObservedFact] = {}
    for item in observation.fact_observations:
        key = item.fact.key
        if key in explicit_facts:
            raise ContractValidationError(
                f"contract observation fact_observations contains duplicate key {key!r}"
            )
        explicit_facts[key] = _clone_observed_fact(item)
    for key, observed in explicit_facts.items():
        legacy = typed_facts.get(key)
        if legacy is not None and legacy != observed.fact.value:
            raise ContractValidationError(
                f"contract observation fact_observations {key} conflicts with typed_facts"
            )
        typed_facts[key] = observed.fact.value
    scalar_bindings = (
        ("policy_lag", "sample.policy_lag", "uint64_value"),
        ("sample_stale", "sample.stale", "bool_value"),
        ("buffer_level", "buffer.level.current", "uint64_value"),
        ("effective_sample_size", "batch.effective_sample_size", "double_value"),
        ("safe_point", "runtime.safe_point", "bool_value"),
        ("sample_count", "sample.sample_count", "uint64_value"),
        (
            "effective_sample_size_ratio",
            "batch.effective_sample_size_ratio",
            "double_value",
        ),
    )
    for field_name, fact_path, value_kind in scalar_bindings:
        observed = explicit_facts.get(fact_path)
        if observed is None or not observation.HasField(field_name):
            continue
        if observed.fact.value.WhichOneof("kind") != value_kind or getattr(
            observed.fact.value, value_kind
        ) != getattr(observation, field_name):
            raise ContractValidationError(
                f"contract observation fact_observations {fact_path} conflicts with {field_name}"
            )
    if observation.HasField("accepted_samples"):
        accepted = [
            explicit_facts[path]
            for path in ("batch.accepted_samples", "group.accepted_samples")
            if path in explicit_facts
        ]
        if any(
            item.fact.value.WhichOneof("kind") != "uint64_value"
            or item.fact.value.uint64_value != observation.accepted_samples
            for item in accepted
        ):
            raise ContractValidationError(
                "contract observation fact_observations conflicts with accepted_samples"
            )
    expected = explicit_facts.get("group.expected_samples")
    if (
        expected is not None
        and observation.HasField("expected_samples")
        and (
            expected.fact.value.WhichOneof("kind") != "uint64_value"
            or expected.fact.value.uint64_value != observation.expected_samples
        )
    ):
        raise ContractValidationError(
            "contract observation fact_observations group.expected_samples conflicts with "
            "expected_samples"
        )

    latest_policy_lag = _uint64_fact(typed_facts, "sample.policy_lag")
    latest_sample_stale = _bool_fact(typed_facts, "sample.stale")
    latest_buffer_level = _uint64_fact(typed_facts, "buffer.level.current")
    if latest_buffer_level is None:
        latest_buffer_level = int(event.buffer_level)
    latest_safe_point = _bool_fact(typed_facts, "runtime.safe_point")
    if latest_safe_point is None:
        latest_safe_point = bool(event.safe_point)
    latest_accepted_samples = _uint64_fact(typed_facts, "batch.accepted_samples")
    if latest_accepted_samples is None:
        latest_accepted_samples = _uint64_fact(typed_facts, "group.accepted_samples")
    latest_expected_samples = _uint64_fact(typed_facts, "group.expected_samples")
    latest_sample_count = _uint64_fact(typed_facts, "sample.sample_count")
    latest_ess = _double_fact(typed_facts, "batch.effective_sample_size")
    latest_ess_ratio = _double_fact(typed_facts, "batch.effective_sample_size_ratio")

    source = observation.source or "trace-summary"
    observed_at = timestamp_pb2.Timestamp()
    observed_at.CopyFrom(
        observation.observed_at if observation.HasField("observed_at") else event.occurred_at
    )
    observation.source = source
    observation.event_id = event.event_id
    observation.phase_id = event.phase_id
    observation.policy_version = event.policy_version
    observation.observed_at.CopyFrom(event.occurred_at)
    observation.policy_lag = latest_policy_lag if latest_policy_lag is not None else 0
    if latest_policy_lag is None:
        observation.ClearField("policy_lag")
    observation.sample_stale = latest_sample_stale if latest_sample_stale is not None else False
    if latest_sample_stale is None:
        observation.ClearField("sample_stale")
    observation.buffer_level = latest_buffer_level
    observation.safe_point = latest_safe_point
    if latest_accepted_samples is not None:
        observation.accepted_samples = latest_accepted_samples
    else:
        observation.ClearField("accepted_samples")
    if latest_expected_samples is not None:
        observation.expected_samples = latest_expected_samples
    else:
        observation.ClearField("expected_samples")
    if latest_sample_count is not None:
        observation.sample_count = latest_sample_count
    else:
        observation.ClearField("sample_count")
    if latest_ess is not None:
        observation.effective_sample_size = latest_ess
    else:
        observation.ClearField("effective_sample_size")
    if latest_ess_ratio is not None:
        observation.effective_sample_size_ratio = latest_ess_ratio
    else:
        observation.ClearField("effective_sample_size_ratio")

    rebuilt_facts: list[semantic_pb2.SemanticField] = []
    if observation.HasField("policy_lag"):
        rebuilt_facts.append(_semantic_field("sample.policy_lag", uint64=observation.policy_lag))
    if observation.HasField("sample_stale"):
        rebuilt_facts.append(_semantic_field("sample.stale", boolean=observation.sample_stale))
    rebuilt_facts.append(_semantic_field("buffer.level.current", uint64=observation.buffer_level))
    if observation.HasField("effective_sample_size"):
        rebuilt_facts.append(
            _semantic_field(
                "batch.effective_sample_size",
                double=observation.effective_sample_size,
            )
        )
    if observation.HasField("accepted_samples"):
        accepted_fact_key = (
            "batch.accepted_samples"
            if "batch.accepted_samples" in typed_facts
            else "group.accepted_samples"
        )
        rebuilt_facts.append(
            _semantic_field(accepted_fact_key, uint64=observation.accepted_samples)
        )
    if observation.HasField("expected_samples"):
        rebuilt_facts.append(
            _semantic_field("group.expected_samples", uint64=observation.expected_samples)
        )
    if observation.HasField("safe_point"):
        rebuilt_facts.append(_semantic_field("runtime.safe_point", boolean=observation.safe_point))
    if observation.HasField("sample_count"):
        rebuilt_facts.append(
            _semantic_field("sample.sample_count", uint64=observation.sample_count)
        )
    if observation.HasField("effective_sample_size_ratio"):
        rebuilt_facts.append(
            _semantic_field(
                "batch.effective_sample_size_ratio",
                double=observation.effective_sample_size_ratio,
            )
        )
    rebuilt_facts.sort(key=lambda item: item.key)
    del observation.typed_facts[:]
    observation.typed_facts.extend(rebuilt_facts)
    del observation.fact_observations[:]
    for fact in rebuilt_facts:
        explicit = explicit_facts.get(fact.key)
        if explicit is not None:
            observation.fact_observations.append(explicit)
            continue
        observation.fact_observations.append(
            execution_pb2.ObservedFact(
                fact=fact,
                observed_at=observed_at,
                source=source,
                revision=_source_revision(event),
            )
        )
    validate_contract_observation(observation)
    return observation


def _merged_observation_from_events(
    events: Iterable[trace_pb2.TraceEvent],
) -> execution_pb2.ContractObservation:
    ordered = sorted(events, key=_causal_key)
    if not ordered:
        raise ValueError("at least one trace event is required")
    facts: dict[str, execution_pb2.ObservedFact] = {}
    versions: dict[tuple[int, str], execution_pb2.ComponentVersion] = {}
    latest: execution_pb2.ContractObservation | None = None
    for event in ordered:
        current = _latest_observation_from_event(event)
        latest = current
        for fact in current.fact_observations:
            facts[fact.fact.key] = _clone_observed_fact(fact)
        for version in current.component_versions:
            versions[(version.kind, version.name.casefold())] = _clone_component_version(version)
    assert latest is not None
    merged = execution_pb2.ContractObservation()
    merged.CopyFrom(latest)
    del merged.fact_observations[:]
    merged.fact_observations.extend(facts[key] for key in sorted(facts))
    del merged.typed_facts[:]
    merged.typed_facts.extend(facts[key].fact for key in sorted(facts))
    del merged.component_versions[:]
    merged.component_versions.extend(
        versions[key] for key in sorted(versions, key=lambda item: (item[0], item[1]))
    )

    fact_values = _typed_fact_map(merged)
    scalar_bindings = (
        ("policy_lag", "sample.policy_lag", "uint64_value"),
        ("sample_stale", "sample.stale", "bool_value"),
        ("buffer_level", "buffer.level.current", "uint64_value"),
        ("effective_sample_size", "batch.effective_sample_size", "double_value"),
        ("safe_point", "runtime.safe_point", "bool_value"),
        ("sample_count", "sample.sample_count", "uint64_value"),
        (
            "effective_sample_size_ratio",
            "batch.effective_sample_size_ratio",
            "double_value",
        ),
    )
    for field_name, fact_path, value_kind in scalar_bindings:
        value = fact_values.get(fact_path)
        if value is None:
            merged.ClearField(field_name)
        else:
            setattr(merged, field_name, getattr(value, value_kind))
    accepted_paths = [
        path for path in ("batch.accepted_samples", "group.accepted_samples") if path in fact_values
    ]
    if len(accepted_paths) == 1:
        merged.accepted_samples = fact_values[accepted_paths[0]].uint64_value
    else:
        merged.ClearField("accepted_samples")
    expected = fact_values.get("group.expected_samples")
    if expected is None:
        merged.ClearField("expected_samples")
    else:
        merged.expected_samples = expected.uint64_value
    validate_contract_observation(merged)
    return merged


class TraceAggregator:
    """Pair lifecycle events and summarize stable micro-stage metrics."""

    def __init__(self, normalizer: TraceNormalizer | None = None) -> None:
        self._normalizer = normalizer or TraceNormalizer()

    def aggregate_micro_stages(
        self, events: Iterable[trace_pb2.TraceEvent]
    ) -> tuple[MicroStageAggregate, ...]:
        normalized = self._normalizer.normalize(events)
        groups: dict[tuple[str, str, str, str, str, int], list[trace_pb2.TraceEvent]] = {}
        for event in normalized:
            base_key = (
                event.run_id,
                event.trace_id,
                event.execution_id,
                event.stage_id,
                event.sandbox_id,
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
        base_key: tuple[str, str, str, str, str, int],
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
        latest_observation: execution_pb2.ContractObservation | None = None
        latest_event: trace_pb2.TraceEvent | None = None
        events_by_stage: dict[str, list[trace_pb2.TraceEvent]] = {}
        for event in normalized:
            phase_counts[event.phase_id] = phase_counts.get(event.phase_id, 0) + 1
            safe_point_count += int(event.safe_point)
            policy_publish_count += int(
                event.event_type == trace_pb2.TRACE_EVENT_TYPE_POLICY_PUBLISHED
            )
            max_buffer_level = max(max_buffer_level, event.buffer_level)
            if latest_event is None or _causal_key(event) >= _causal_key(latest_event):
                latest_event = event
            events_by_stage.setdefault(event.stage_id, []).append(event)
        latest_observations_by_stage = {
            stage_id: _merged_observation_from_events(stage_events)
            for stage_id, stage_events in sorted(events_by_stage.items())
        }
        if latest_event is not None:
            latest_observation = latest_observations_by_stage[latest_event.stage_id]
        micro_stages = self.aggregate_micro_stages(normalized)
        completed = tuple(stage for stage in micro_stages if stage.complete)
        return TraceSummary(
            event_count=len(normalized),
            safe_point_count=safe_point_count,
            policy_publish_count=policy_publish_count,
            phase_counts=phase_counts,
            max_buffer_level=max_buffer_level,
            latest_buffer_level=latest_observation.buffer_level if latest_observation else 0,
            latest_safe_point=latest_observation.safe_point if latest_observation else False,
            latest_policy_version=latest_observation.policy_version if latest_observation else "",
            latest_phase_id=latest_observation.phase_id if latest_observation else "",
            latest_event_id=latest_observation.event_id if latest_observation else "",
            latest_observation=latest_observation,
            micro_stage_count=len(micro_stages),
            completed_micro_stage_count=len(completed),
            incomplete_micro_stage_count=len(micro_stages) - len(completed),
            total_micro_stage_seconds=sum(
                stage.duration.total_seconds() for stage in completed if stage.duration is not None
            ),
            micro_stages=micro_stages,
            latest_observations_by_stage=latest_observations_by_stage,
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
