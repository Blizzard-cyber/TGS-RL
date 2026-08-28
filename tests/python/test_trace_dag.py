"""Trace normalization, DAG, and gap-classification behavior."""

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from google.protobuf import timestamp_pb2
from hypothesis import given
from hypothesis import strategies as st
from tgsrl.v1 import execution_pb2, trace_pb2
from tgsrl_runtime.dag import CycleError, DAGError, GapClassifier, GapKind, IncrementalDAG
from tgsrl_runtime.trace import TraceNormalizer, TraceValidationError

EventFactory = Callable[..., trace_pb2.TraceEvent]


def test_normalize_deduplicates_sorts_and_does_not_mutate(event_factory: EventFactory) -> None:
    late = event_factory("z", seconds=2, sequence=2)
    early = event_factory("a", seconds=1, sequence=1)
    before = early.SerializeToString(deterministic=True)
    output = TraceNormalizer().normalize([late, early, early])
    assert [event.event_id for event in output] == ["a", "z"]
    assert early.SerializeToString(deterministic=True) == before
    assert output[0] is not early


def test_unknown_phase_preserves_raw_label(event_factory: EventFactory) -> None:
    event = event_factory(phase_id="vendor-fused-stage", phase_kind=0)
    event.raw_phase_label = "VendorFusedStage"
    normalized = TraceNormalizer().normalize([event])[0]
    assert normalized.phase_kind == execution_pb2.PHASE_KIND_UNKNOWN
    assert normalized.raw_phase_label == "VendorFusedStage"


def test_missing_required_field_and_conflicting_duplicate_fail(event_factory: EventFactory) -> None:
    missing = event_factory()
    missing.job_id = ""
    with pytest.raises(TraceValidationError, match="job_id"):
        TraceNormalizer().normalize([missing])
    first = event_factory("same")
    conflict = event_factory("same", seconds=2)
    with pytest.raises(TraceValidationError, match="conflicting"):
        TraceNormalizer().normalize([first, conflict])


@given(order=st.permutations(("a", "b", "c")))
def test_order_is_input_permutation_independent(order: list[str]) -> None:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(datetime(2025, 1, 1, tzinfo=UTC))
    events = {
        name: trace_pb2.TraceEvent(
            event_id=name,
            job_id="job",
            execution_id="execution",
            phase_id="decode",
            occurred_at=timestamp,
            event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
            algorithm="grpo",
            rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
            policy_version="policy",
            decision_id="decision",
            sequence=1,
            attributes={"source_revision": "1"},
            phase_kind=execution_pb2.PHASE_KIND_DECODE,
            stage_id="decode",
        )
        for name in order
    }
    normalized = TraceNormalizer().normalize(events[name] for name in order)
    assert [event.event_id for event in normalized] == ["a", "b", "c"]


def test_incremental_dag_ready_set_uncertainty_and_cycle() -> None:
    dag = IncrementalDAG()
    dag.add_edge("a", "b")
    assert dag.uncertain_edges == (("a", "b"),)
    dag.add_phase(execution_pb2.Phase(phase_id="a"))
    dag.add_phase(execution_pb2.Phase(phase_id="b"))
    assert dag.ready_set == ("a",)
    dag.mark_completed("a")
    assert dag.ready_set == ("b",)
    with pytest.raises(CycleError):
        dag.add_edge("b", "a")


def test_declared_entries_are_the_only_initial_ready_phases() -> None:
    graph = execution_pb2.PhaseGraph(
        phases=[
            execution_pb2.Phase(phase_id="entry"),
            execution_pb2.Phase(phase_id="disconnected"),
        ],
        entry_phase_ids=["entry"],
    )
    dag = IncrementalDAG(graph)
    assert dag.ready_set == ("entry",)


def test_declared_entry_with_incoming_edge_is_rejected() -> None:
    graph = execution_pb2.PhaseGraph(
        phases=[execution_pb2.Phase(phase_id="a"), execution_pb2.Phase(phase_id="b")],
        edges=[execution_pb2.PhaseEdge(from_phase_id="a", to_phase_id="b")],
        entry_phase_ids=["b"],
    )
    with pytest.raises(DAGError, match=r"entry.*(?:incoming|root)"):
        IncrementalDAG(graph)


@pytest.mark.parametrize("failure", ["cycle", "missing-entry"])
def test_failed_graph_update_is_transactional(failure: str) -> None:
    dag = IncrementalDAG(
        execution_pb2.PhaseGraph(phases=[execution_pb2.Phase(phase_id="a")], entry_phase_ids=["a"])
    )
    before = dag.graph().SerializeToString(deterministic=True)
    if failure == "cycle":
        update = execution_pb2.PhaseGraph(
            phases=[execution_pb2.Phase(phase_id="b")],
            edges=[
                execution_pb2.PhaseEdge(from_phase_id="a", to_phase_id="b"),
                execution_pb2.PhaseEdge(from_phase_id="b", to_phase_id="a"),
            ],
        )
        error: type[DAGError] = CycleError
    else:
        update = execution_pb2.PhaseGraph(
            phases=[execution_pb2.Phase(phase_id="b")],
            edges=[execution_pb2.PhaseEdge(from_phase_id="a", to_phase_id="b")],
            entry_phase_ids=["missing"],
        )
        error = DAGError

    with pytest.raises(error):
        dag.update_graph(update)
    assert dag.graph().SerializeToString(deterministic=True) == before
    assert dag.ready_set == ("a",)


@pytest.mark.parametrize(
    ("phase_kind", "reason", "expected"),
    [
        (execution_pb2.PHASE_KIND_DECODE, "long_tail", GapKind.LONG_TAIL_WAIT),
        (execution_pb2.PHASE_KIND_TOOL_WAIT, "tool_wait", GapKind.TOOL_ENVIRONMENT_WAIT),
        (execution_pb2.PHASE_KIND_IDLE, "trainer_starvation", GapKind.ROLE_PHASE_MISALIGNMENT),
        (execution_pb2.PHASE_KIND_WEIGHT_SYNC, "weight_sync", GapKind.SERIAL_OR_WEIGHT_SYNC),
        (execution_pb2.PHASE_KIND_DECODE, "version_drift", GapKind.INVALID_STALE_OUTPUT),
    ],
)
def test_five_gap_classes(
    event_factory: EventFactory, phase_kind: int, reason: str, expected: GapKind
) -> None:
    event = event_factory(phase_kind=phase_kind)
    event.attributes["gap_reason"] = reason
    assert GapClassifier().classify(event) is expected
