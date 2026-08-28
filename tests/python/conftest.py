"""Shared generated-Proto fixtures for Python semantic-plane tests."""

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from google.protobuf import timestamp_pb2
from tgsrl.v1 import execution_pb2, trace_pb2


@pytest.fixture
def event_factory() -> Callable[..., trace_pb2.TraceEvent]:
    def make_event(
        event_id: str = "event-1",
        *,
        seconds: int = 1,
        sequence: int = 1,
        phase_id: str = "decode",
        phase_kind: int = execution_pb2.PHASE_KIND_DECODE,
        event_type: int = trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
    ) -> trace_pb2.TraceEvent:
        occurred_at = timestamp_pb2.Timestamp()
        occurred_at.FromDatetime(datetime(2025, 1, 1, tzinfo=UTC).replace(second=seconds))
        return trace_pb2.TraceEvent(
            event_id=event_id,
            job_id="job-1",
            execution_id="execution-1",
            phase_id=phase_id,
            occurred_at=occurred_at,
            event_type=event_type,
            algorithm="grpo",
            rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
            policy_version="policy-1",
            decision_id="decision-1",
            sequence=sequence,
            attributes={"source_revision": str(sequence)},
            phase_kind=phase_kind,
            raw_phase_label=phase_id,
            stage_id=phase_id,
        )

    return make_event
