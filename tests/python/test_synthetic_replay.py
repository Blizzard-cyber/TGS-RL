"""Synthetic scenario and virtual-clock replay tests."""

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from tgsrl.v1 import trace_pb2
from tgsrl_runtime.replay import ReplayController, ReplayError, VirtualClock
from tgsrl_runtime.synthetic import SyntheticScenario, SyntheticWorkload

EventFactory = Callable[..., trace_pb2.TraceEvent]


def test_all_required_scenarios_are_deterministic() -> None:
    assert {scenario.value for scenario in SyntheticScenario} == {
        "tool_wait",
        "long_tail",
        "trainer_starvation",
        "buffer_backlog",
        "version_drift",
    }
    left = SyntheticWorkload(7).generate_all()
    right = SyntheticWorkload(7).generate_all()
    assert [event.SerializeToString(deterministic=True) for event in left] == [
        event.SerializeToString(deterministic=True) for event in right
    ]
    assert all(event.attributes["seed"] == "7" for event in left)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (SyntheticWorkload(7, algorithm="ppo"), SyntheticWorkload(7, algorithm="grpo")),
        (
            SyntheticWorkload(7, rollout_mode=trace_pb2.ROLLOUT_MODE_SYNC),
            SyntheticWorkload(7, rollout_mode=trace_pb2.ROLLOUT_MODE_FULLY_ASYNC),
        ),
        (
            SyntheticWorkload(7, base_time=datetime(2025, 1, 1, tzinfo=UTC)),
            SyntheticWorkload(7, base_time=datetime(2025, 1, 2, tzinfo=UTC)),
        ),
    ],
)
def test_semantically_distinct_workloads_have_distinct_ids(
    left: SyntheticWorkload, right: SyntheticWorkload
) -> None:
    left_event = left.generate(SyntheticScenario.TOOL_WAIT)[0]
    right_event = right.generate(SyntheticScenario.TOOL_WAIT)[0]
    assert left_event.event_id != right_event.event_id
    assert left_event.execution_id != right_event.execution_id
    assert left_event.decision_id != right_event.decision_id


def test_replay_pause_step_checkpoint_and_immutable_input(event_factory: EventFactory) -> None:
    source = [event_factory("b", seconds=2, sequence=2), event_factory("a", seconds=1)]
    source_wire = [event.SerializeToString(deterministic=True) for event in source]
    replay = ReplayController(source, seed=11, clock=VirtualClock(datetime(2024, 1, 1, tzinfo=UTC)))
    replay.pause()
    assert replay.next_event() is None
    first = replay.step()
    assert first is not None and first.event_id == "a"
    checkpoint = replay.checkpoint()
    replay.resume()
    assert replay.next_event().event_id == "b"  # type: ignore[union-attr]
    replay.restore(checkpoint)
    assert replay.paused and replay.step().event_id == "b"  # type: ignore[union-attr]
    assert [event.SerializeToString(deterministic=True) for event in source] == source_wire


def test_checkpoint_rejects_another_stream(event_factory: EventFactory) -> None:
    first = ReplayController([event_factory("a")], seed=1)
    second = ReplayController([event_factory("b")], seed=1)
    with pytest.raises(ReplayError, match="different event stream"):
        second.restore(first.checkpoint())


@pytest.mark.asyncio
async def test_pause_during_sleep_prevents_next_emission(event_factory: EventFactory) -> None:
    events = [event_factory("a", seconds=1), event_factory("b", seconds=2, sequence=2)]
    replay: ReplayController

    async def pause_during_sleep(_delay: float) -> None:
        replay.pause()

    replay = ReplayController(events, seed=7, sleeper=pause_during_sleep)
    emitted = [event.event_id async for event in replay.events()]

    assert emitted == ["a"]
    assert replay.paused
    assert replay.position == 1
    assert replay.remaining == 1
    assert replay.clock.now() == datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC)
