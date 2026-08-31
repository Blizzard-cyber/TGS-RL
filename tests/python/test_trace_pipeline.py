"""Trace aggregation and atomic ingestion contract tests."""

from collections.abc import Callable
from datetime import timedelta

import pytest
from tgsrl.v1 import execution_pb2, semantic_pb2, trace_pb2
from tgsrl_runtime.aggregation import TraceAggregator
from tgsrl_runtime.trace_ingest import TraceIngestError, TraceIngestor

EventFactory = Callable[..., trace_pb2.TraceEvent]


def _event(
    factory: EventFactory,
    event_id: str,
    *,
    stage_id: str = "decode",
    seconds: int = 1,
    sequence: int = 1,
    event_type: int = trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
    execution_id: str = "execution-1",
    trace_id: str = "trace-1",
    run_id: str = "run-1",
    data_kind: int = trace_pb2.DATA_KIND_REPLAY,
    generation: int = 1,
) -> trace_pb2.TraceEvent:
    event = factory(
        event_id,
        seconds=seconds,
        sequence=sequence,
        phase_id=stage_id,
        event_type=event_type,
    )
    event.execution_id = execution_id
    event.stage_id = stage_id
    event.run_id = run_id
    event.trace_id = trace_id
    event.data_kind = data_kind
    event.generation = generation
    return event


def _wire(events: list[trace_pb2.TraceEvent]) -> list[bytes]:
    return [event.SerializeToString(deterministic=True) for event in events]


def test_aggregator_pairs_interleaved_micro_stages_by_concrete_identity(
    event_factory: EventFactory,
) -> None:
    decode_start = _event(event_factory, "decode-start", sequence=1, seconds=1)
    reward_start = _event(event_factory, "reward-start", stage_id="reward", sequence=2, seconds=2)
    decode_sample = _event(
        event_factory,
        "decode-sample",
        sequence=3,
        seconds=3,
        event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
    )
    reward_complete = _event(
        event_factory,
        "reward-complete",
        stage_id="reward",
        sequence=4,
        seconds=5,
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
    )
    decode_complete = _event(
        event_factory,
        "decode-complete",
        sequence=5,
        seconds=7,
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
    )
    decode_start.buffer_level = 2
    decode_sample.buffer_level = 9
    decode_sample.safe_point = True
    decode_sample.decision_id = "decision-2"
    decode_complete.policy_version = "policy-2"

    aggregates = TraceAggregator().aggregate_micro_stages(
        [decode_complete, reward_start, decode_sample, decode_start, reward_complete]
    )

    assert [(item.key.stage_id, item.key.attempt) for item in aggregates] == [
        ("decode", 1),
        ("reward", 1),
    ]
    decode, reward = aggregates
    assert decode.event_ids == ("decode-start", "decode-sample", "decode-complete")
    assert decode.first_sequence == 1
    assert decode.last_sequence == 5
    assert decode.duration == timedelta(seconds=6)
    assert decode.complete and not decode.clock_skew
    assert decode.max_buffer_level == 9
    assert decode.safe_point_count == 1
    assert decode.decision_ids == ("decision-1", "decision-2")
    assert decode.policy_versions == ("policy-1", "policy-2")
    assert reward.event_ids == ("reward-start", "reward-complete")
    assert reward.duration == timedelta(seconds=3)


def test_aggregator_retains_orphans_and_restarted_attempts_as_incomplete(
    event_factory: EventFactory,
) -> None:
    events = [
        _event(
            event_factory,
            "orphan-complete",
            sequence=1,
            seconds=1,
            event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
        ),
        _event(event_factory, "first-start", sequence=2, seconds=2),
        _event(
            event_factory,
            "first-sample",
            sequence=3,
            seconds=3,
            event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
        ),
        _event(event_factory, "restart", sequence=4, seconds=4),
        _event(
            event_factory,
            "final-complete",
            sequence=5,
            seconds=6,
            event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
        ),
    ]

    aggregates = TraceAggregator().aggregate_micro_stages(reversed(events))

    assert [item.key.attempt for item in aggregates] == [1, 2, 3]
    assert [item.event_ids for item in aggregates] == [
        ("orphan-complete",),
        ("first-start", "first-sample"),
        ("restart", "final-complete"),
    ]
    assert [item.complete for item in aggregates] == [False, False, True]
    assert aggregates[0].started_at is None
    assert aggregates[1].completed_at is None
    assert aggregates[2].duration == timedelta(seconds=2)


def test_aggregator_separates_generations_and_excludes_clock_skew_from_duration(
    event_factory: EventFactory,
) -> None:
    generation_one = [
        _event(event_factory, "g1-start", sequence=1, seconds=1, generation=1),
        _event(
            event_factory,
            "g1-complete",
            sequence=2,
            seconds=5,
            generation=1,
            event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
        ),
    ]
    generation_two = [
        _event(event_factory, "g2-start", sequence=3, seconds=10, generation=2),
        _event(
            event_factory,
            "g2-complete",
            sequence=4,
            seconds=8,
            generation=2,
            event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
        ),
    ]

    summary = TraceAggregator().summarize([*generation_two, *generation_one])

    assert [stage.key.generation for stage in summary.micro_stages] == [1, 2]
    assert summary.micro_stages[0].duration == timedelta(seconds=4)
    assert summary.micro_stages[1].duration is None
    assert summary.micro_stages[1].clock_skew
    assert summary.completed_micro_stage_count == 1
    assert summary.incomplete_micro_stage_count == 1
    assert summary.total_micro_stage_seconds == 4.0
    metrics = {metric.name: metric.value for metric in TraceAggregator().to_metrics(summary)}
    assert metrics["micro_stage_duration"] == 4.0
    assert metrics["completed_micro_stage_count"] == 1.0


def test_summary_uses_causally_latest_observation_not_historical_max(
    event_factory: EventFactory,
) -> None:
    early = _event(
        event_factory,
        "early",
        sequence=1,
        seconds=1,
        event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
    )
    early.buffer_level = 11
    early.contract_observation.source = "runtime"
    early.contract_observation.event_id = "early"
    early.contract_observation.phase_id = "decode"
    early.contract_observation.policy_version = "policy-1"
    early.contract_observation.observed_at.CopyFrom(early.occurred_at)
    early.contract_observation.buffer_level = 11
    early.contract_observation.safe_point = False
    early.contract_observation.typed_facts.extend(
        [
            semantic_pb2.SemanticField(
                key="buffer.level.current",
                value=semantic_pb2.SemanticValue(uint64_value=11),
            ),
            semantic_pb2.SemanticField(
                key="batch.accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=6),
            ),
        ]
    )
    early.contract_observation.accepted_samples = 6

    late = _event(
        event_factory,
        "late",
        sequence=2,
        seconds=2,
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
    )
    late.buffer_level = 3
    late.safe_point = True
    late.policy_version = "policy-2"
    late.contract_observation.source = "runtime"
    late.contract_observation.event_id = "late"
    late.contract_observation.phase_id = "decode"
    late.contract_observation.policy_version = "policy-2"
    late.contract_observation.observed_at.CopyFrom(late.occurred_at)
    late.contract_observation.buffer_level = 3
    late.contract_observation.safe_point = True
    late.contract_observation.typed_facts.extend(
        [
            semantic_pb2.SemanticField(
                key="buffer.level.current",
                value=semantic_pb2.SemanticValue(uint64_value=3),
            ),
            semantic_pb2.SemanticField(
                key="runtime.safe_point",
                value=semantic_pb2.SemanticValue(bool_value=True),
            ),
            semantic_pb2.SemanticField(
                key="batch.accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=4),
            ),
        ]
    )
    late.contract_observation.accepted_samples = 4

    summary = TraceAggregator().summarize([late, early])

    assert summary.max_buffer_level == 11
    assert summary.latest_buffer_level == 3
    assert summary.latest_safe_point
    assert summary.latest_policy_version == "policy-2"
    assert summary.latest_event_id == "late"
    assert summary.latest_observation is not None
    assert summary.latest_observation.buffer_level == 3
    assert summary.latest_observation.accepted_samples == 4
    decode_observation = summary.latest_observation_for_stage("decode")
    assert decode_observation is not None
    assert decode_observation.event_id == "late"


def test_summary_tracks_latest_observation_per_stage_without_cross_stage_leakage(
    event_factory: EventFactory,
) -> None:
    decode = _event(
        event_factory,
        "decode-late",
        stage_id="decode",
        sequence=2,
        seconds=2,
        event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
    )
    decode.policy_version = "policy-decode"
    decode.contract_observation.source = "runtime"
    decode.contract_observation.event_id = "decode-late"
    decode.contract_observation.phase_id = "decode"
    decode.contract_observation.policy_version = "policy-decode"
    decode.contract_observation.observed_at.CopyFrom(decode.occurred_at)
    decode.contract_observation.buffer_level = 5
    decode.contract_observation.safe_point = False
    decode.contract_observation.policy_lag = 1
    decode.contract_observation.typed_facts.extend(
        [
            semantic_pb2.SemanticField(
                key="sample.policy_lag",
                value=semantic_pb2.SemanticValue(uint64_value=1),
            ),
            semantic_pb2.SemanticField(
                key="buffer.level.current",
                value=semantic_pb2.SemanticValue(uint64_value=5),
            ),
            semantic_pb2.SemanticField(
                key="group.accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=3),
            ),
            semantic_pb2.SemanticField(
                key="group.expected_samples",
                value=semantic_pb2.SemanticValue(uint64_value=4),
            ),
        ]
    )
    decode.contract_observation.accepted_samples = 3
    decode.contract_observation.expected_samples = 4

    reward = _event(
        event_factory,
        "reward-late",
        stage_id="reward",
        sequence=3,
        seconds=3,
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
    )
    reward.policy_version = "policy-reward"
    reward.buffer_level = 9
    reward.safe_point = True
    reward.contract_observation.source = "runtime"
    reward.contract_observation.event_id = "reward-late"
    reward.contract_observation.phase_id = "reward"
    reward.contract_observation.policy_version = "policy-reward"
    reward.contract_observation.observed_at.CopyFrom(reward.occurred_at)
    reward.contract_observation.buffer_level = 9
    reward.contract_observation.safe_point = True
    reward.contract_observation.sample_stale = True
    reward.contract_observation.typed_facts.extend(
        [
            semantic_pb2.SemanticField(
                key="sample.stale",
                value=semantic_pb2.SemanticValue(bool_value=True),
            ),
            semantic_pb2.SemanticField(
                key="buffer.level.current",
                value=semantic_pb2.SemanticValue(uint64_value=9),
            ),
            semantic_pb2.SemanticField(
                key="runtime.safe_point",
                value=semantic_pb2.SemanticValue(bool_value=True),
            ),
            semantic_pb2.SemanticField(
                key="batch.accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=7),
            ),
        ]
    )
    reward.contract_observation.accepted_samples = 7

    summary = TraceAggregator().summarize([reward, decode])

    assert summary.latest_observation is not None
    assert summary.latest_observation.event_id == "reward-late"
    assert set(summary.latest_observations_by_stage) == {"decode", "reward"}
    decode_observation = summary.latest_observation_for_stage("decode")
    reward_observation = summary.latest_observation_for_stage("reward")
    assert decode_observation is not None
    assert reward_observation is not None
    assert decode_observation.event_id == "decode-late"
    assert decode_observation.phase_id == "decode"
    assert decode_observation.policy_version == "policy-decode"
    assert decode_observation.policy_lag == 1
    assert not decode_observation.sample_stale
    assert reward_observation.event_id == "reward-late"
    assert reward_observation.phase_id == "reward"
    assert reward_observation.policy_version == "policy-reward"
    assert reward_observation.sample_stale
    assert summary.latest_observation_for_stage("missing") is None


def test_summary_preserves_missing_ess_presence(event_factory: EventFactory) -> None:
    event = _event(
        event_factory,
        "sample",
        sequence=1,
        seconds=1,
        event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
    )
    event.contract_observation.source = "runtime"
    event.contract_observation.event_id = "sample"
    event.contract_observation.phase_id = "decode"
    event.contract_observation.policy_version = "policy-1"
    event.contract_observation.observed_at.CopyFrom(event.occurred_at)
    event.contract_observation.buffer_level = 2
    event.contract_observation.typed_facts.extend(
        [
            semantic_pb2.SemanticField(
                key="buffer.level.current",
                value=semantic_pb2.SemanticValue(uint64_value=2),
            ),
            semantic_pb2.SemanticField(
                key="batch.accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=1),
            ),
        ]
    )
    event.contract_observation.accepted_samples = 1

    summary = TraceAggregator().summarize([event])

    assert summary.latest_observation is not None
    assert not summary.latest_observation.HasField("effective_sample_size")
    assert not summary.latest_observation.HasField("effective_sample_size_ratio")


def test_summary_merges_provenance_per_stage_by_event_causal_order(
    event_factory: EventFactory,
) -> None:
    early = _event(
        event_factory,
        "early",
        stage_id="decode",
        sequence=1,
        seconds=1,
        event_type=trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
    )
    early.contract_observation.CopyFrom(
        execution_pb2.ContractObservation(
            observed_at=early.occurred_at,
            source="runtime",
            event_id="early",
            phase_id="decode",
            policy_version="policy-1",
            accepted_samples=4,
            typed_facts=[
                semantic_pb2.SemanticField(
                    key="batch.accepted_samples",
                    value=semantic_pb2.SemanticValue(uint64_value=4),
                )
            ],
            fact_observations=[
                execution_pb2.ObservedFact(
                    fact=semantic_pb2.SemanticField(
                        key="batch.accepted_samples",
                        value=semantic_pb2.SemanticValue(uint64_value=4),
                    ),
                    observed_at=early.occurred_at,
                    source="sampler",
                    revision=0,
                )
            ],
        )
    )
    late = _event(
        event_factory,
        "late",
        stage_id="decode",
        sequence=2,
        seconds=5,
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
    )
    late.safe_point = True
    late.contract_observation.CopyFrom(
        execution_pb2.ContractObservation(
            observed_at=late.occurred_at,
            source="runtime",
            event_id="late",
            phase_id="decode",
            policy_version="policy-1",
            safe_point=True,
            typed_facts=[
                semantic_pb2.SemanticField(
                    key="runtime.safe_point",
                    value=semantic_pb2.SemanticValue(bool_value=True),
                )
            ],
            fact_observations=[
                execution_pb2.ObservedFact(
                    fact=semantic_pb2.SemanticField(
                        key="runtime.safe_point",
                        value=semantic_pb2.SemanticValue(bool_value=True),
                    ),
                    observed_at=late.occurred_at,
                    source="runtime",
                    revision=2,
                )
            ],
        )
    )
    other = _event(
        event_factory,
        "other",
        stage_id="reward",
        sequence=3,
        seconds=6,
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
    )

    summary = TraceAggregator().summarize([other, late, early])
    merged = summary.latest_observation_for_stage("decode")

    assert merged is not None
    facts = {item.fact.key: item for item in merged.fact_observations}
    assert facts["batch.accepted_samples"].fact.value.uint64_value == 4
    assert facts["batch.accepted_samples"].observed_at == early.occurred_at
    assert facts["batch.accepted_samples"].revision == 0
    assert facts["runtime.safe_point"].observed_at == late.occurred_at
    reward_observation = summary.latest_observation_for_stage("reward")
    assert reward_observation is not None
    assert "batch.accepted_samples" not in {
        item.fact.key for item in reward_observation.fact_observations
    }


def test_ingestor_orders_each_execution_causally_across_batches_and_clones(
    event_factory: EventFactory,
) -> None:
    ingestor = TraceIngestor()
    execution_b = _event(event_factory, "b-1", execution_id="execution-b", sequence=1, seconds=1)
    execution_a_late = _event(
        event_factory, "a-2", execution_id="execution-a", sequence=2, seconds=2
    )
    execution_a_early = _event(
        event_factory, "a-1", execution_id="execution-a", sequence=1, seconds=9
    )
    ingestor.ingest("run-1", [execution_b])
    ingestor.ingest("run-1", [execution_a_late, execution_a_early])

    causal = ingestor.list_causal("run-1")

    assert [event.event_id for event in causal] == ["a-1", "a-2", "b-1"]
    causal[0].event_id = "caller-mutation"
    assert [event.event_id for event in ingestor.list_causal("run-1")] == [
        "a-1",
        "a-2",
        "b-1",
    ]


def test_ingestor_rejects_cross_stream_input_without_partial_commit(
    event_factory: EventFactory,
) -> None:
    ingestor = TraceIngestor()
    valid = _event(event_factory, "valid")
    ingestor.ingest("run-1", [valid])
    before = _wire(ingestor.list("run-1"))
    before_digest = ingestor.source_digest("run-1")

    invalid_batches = [
        [
            _event(event_factory, "new-valid", sequence=2),
            _event(event_factory, "other-run", run_id="run-2", sequence=3),
        ],
        [
            _event(event_factory, "execution-a", execution_id="execution-a"),
            _event(event_factory, "execution-b", execution_id="execution-b", sequence=2),
        ],
        [
            _event(event_factory, "trace-a", trace_id="trace-a"),
            _event(event_factory, "trace-b", trace_id="trace-b", sequence=2),
        ],
        [
            _event(event_factory, "replay", data_kind=trace_pb2.DATA_KIND_REPLAY),
            _event(
                event_factory,
                "live",
                data_kind=trace_pb2.DATA_KIND_LIVE,
                sequence=2,
            ),
        ],
    ]

    for events in invalid_batches:
        with pytest.raises(TraceIngestError):
            ingestor.ingest("run-1", events)
        assert _wire(ingestor.list("run-1")) == before
        assert ingestor.source_digest("run-1") == before_digest

    for run_id in ("", " run-1", "run-1 "):
        with pytest.raises(TraceIngestError, match="canonical"):
            ingestor.ingest(run_id, [valid])


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("empty", "contain events"),
        ("run", "run_id"),
        ("execution", "execution_id"),
        ("zero_sequence", "positive sequences"),
        ("first_sequence", "first_sequence"),
        ("last_sequence", "last_sequence"),
        ("trace", "trace_id"),
        ("data_kind", "data_kind"),
    ],
)
def test_ingest_batch_validates_its_envelope(
    event_factory: EventFactory, fault: str, message: str
) -> None:
    events = [
        _event(event_factory, "one", sequence=1),
        _event(event_factory, "two", sequence=2, seconds=2),
    ]
    batch = trace_pb2.TraceEventBatch(
        execution_id="execution-1",
        first_sequence=1,
        last_sequence=2,
        events=events,
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
    )
    if fault == "empty":
        del batch.events[:]
    elif fault == "run":
        batch.run_id = "run-2"
    elif fault == "execution":
        batch.execution_id = "execution-2"
    elif fault == "zero_sequence":
        batch.events[0].sequence = 0
    elif fault == "first_sequence":
        batch.first_sequence = 2
    elif fault == "last_sequence":
        batch.last_sequence = 1
    elif fault == "trace":
        batch.trace_id = "trace-2"
    elif fault == "data_kind":
        batch.data_kind = trace_pb2.DATA_KIND_LIVE

    ingestor = TraceIngestor()
    with pytest.raises(TraceIngestError, match=message):
        ingestor.ingest_batch("run-1", batch)
    assert ingestor.list("run-1") == []


def test_ingest_batch_accepts_a_valid_envelope(event_factory: EventFactory) -> None:
    events = [
        _event(event_factory, "late", sequence=2, seconds=1),
        _event(event_factory, "early", sequence=1, seconds=8),
    ]
    batch = trace_pb2.TraceEventBatch(
        execution_id="execution-1",
        first_sequence=1,
        last_sequence=2,
        events=events,
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_REPLAY,
    )

    ingested = TraceIngestor().ingest_batch("run-1", batch)

    assert [event.event_id for event in ingested] == ["early", "late"]


def test_ingestion_is_idempotent_and_conflicts_are_atomic(event_factory: EventFactory) -> None:
    original = _event(event_factory, "original", sequence=1)
    ingestor = TraceIngestor()
    first = ingestor.ingest("run-1", [original])
    first_digest = ingestor.source_digest("run-1")

    retry = ingestor.ingest("run-1", [original])

    assert [event.event_id for event in first] == ["original"]
    assert [event.event_id for event in retry] == ["original"]
    assert ingestor.source_digest("run-1") == first_digest

    sequence_conflict = _event(event_factory, "other-owner", sequence=1, seconds=2)
    with pytest.raises(TraceIngestError, match=r"sequence 1.*conflicting"):
        ingestor.ingest("run-1", [sequence_conflict])
    assert [event.event_id for event in ingestor.list("run-1")] == ["original"]
    assert ingestor.source_digest("run-1") == first_digest

    duplicate_conflict = _event(event_factory, "original", sequence=2, seconds=3)
    with pytest.raises(TraceIngestError, match="conflicting duplicate event_id"):
        ingestor.ingest("run-1", [duplicate_conflict])
    assert [event.event_id for event in ingestor.list("run-1")] == ["original"]
    assert ingestor.source_digest("run-1") == first_digest


def test_ingestion_allows_worker_local_sequences_across_sandboxes(
    event_factory: EventFactory,
) -> None:
    first = _event(event_factory, "worker-a", sequence=1, seconds=1)
    first.sandbox_id = "sandbox-a"
    second = _event(event_factory, "worker-b", sequence=1, seconds=2)
    second.sandbox_id = "sandbox-b"

    ingested = TraceIngestor().ingest("run-1", [second, first])
    aggregates = TraceAggregator().aggregate_micro_stages(ingested)

    assert {event.event_id for event in ingested} == {"worker-a", "worker-b"}
    assert {stage.key.sandbox_id for stage in aggregates} == {"sandbox-a", "sandbox-b"}


def test_observation_merge_preserves_worker_sequence_despite_clock_skew(
    event_factory: EventFactory,
) -> None:
    first = _event(event_factory, "worker-first", sequence=1, seconds=10)
    first.sandbox_id = "sandbox-a"
    first.contract_observation.policy_version = "policy-1"
    second = _event(event_factory, "worker-second", sequence=2, seconds=1)
    second.sandbox_id = "sandbox-a"
    second.policy_version = "policy-2"
    second.contract_observation.policy_version = "policy-2"

    summary = TraceAggregator().summarize([second, first])

    assert summary.latest_event_id == "worker-second"
    assert summary.latest_policy_version == "policy-2"


def test_source_digest_is_independent_of_batch_arrival_order(
    event_factory: EventFactory,
) -> None:
    first = _event(event_factory, "first", sequence=1, seconds=7)
    second = _event(event_factory, "second", sequence=2, seconds=2)
    left = TraceIngestor()
    right = TraceIngestor()
    left.ingest("run-1", [first])
    left.ingest("run-1", [second])
    right.ingest("run-1", [second])
    right.ingest("run-1", [first])

    assert left.source_digest("run-1") == right.source_digest("run-1")
