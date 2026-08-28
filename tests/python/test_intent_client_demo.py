"""Intent, async client, and demo fixture integration tests."""

import hashlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from math import inf, nan
from typing import Any

import grpc
import pytest
from tgsrl.v1 import execution_pb2, resource_pb2, scheduling_pb2, trace_pb2
from tgsrl_runtime.demo import (
    build_demo_fixture,
    build_live_fixture,
    render_demo_fixture,
    run_live_demo,
)
from tgsrl_runtime.intent import IntentBuilder, IntentValidationError
from tgsrl_runtime.scheduler_client import SchedulerClient

from adapters import GRPOAdapter, PartialAsyncRolloutAdapter, build_execution_contract


def _builder() -> IntentBuilder:
    return IntentBuilder(clock=lambda: datetime(2025, 1, 1, tzinfo=UTC))


def _build_intent(builder: IntentBuilder, **changes: Any) -> scheduling_pb2.SchedulingIntent:
    arguments: dict[str, Any] = {
        "execution_id": "execution",
        "stage_id": "decode",
        "job_id": "job",
        "contract": build_execution_contract(GRPOAdapter(), PartialAsyncRolloutAdapter()),
        "rollout_mode": trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        "phase_kind": execution_pb2.PHASE_KIND_DECODE,
        "policy_version": "policy-1",
        "ttl": timedelta(seconds=30),
        "resources_per_unit": resource_pb2.ResourceVector(
            cpu_millis=500, memory_bytes=1 << 30, accelerator_units=0.25
        ),
    }
    arguments.update(changes)
    return builder.build(**arguments)


def test_intent_versions_ttl_idempotency_and_device_independence() -> None:
    builder = _builder()
    first = _build_intent(builder)
    second = _build_intent(builder)
    assert (first.version, second.version) == (1, 2)
    actual_ttl = first.valid_until.ToNanoseconds() - first.submitted_at.ToNanoseconds()
    assert actual_ttl == first.ttl.ToNanoseconds()
    assert first.idempotency_key != second.idempotency_key
    assert "device" not in first.DESCRIPTOR.fields_by_name
    with pytest.raises(IntentValidationError, match="monotonically"):
        _build_intent(builder, version=2)
    with pytest.raises(IntentValidationError, match="device"):
        _build_intent(_builder(), labels={"device_id": "gpu-0"})


def _clear_required_string(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.queue = " "


def _zero_version(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.version = 0


def _zero_unit_count(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.unit_count = 0


def _unknown_rollout_mode(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.rollout_mode = trace_pb2.ROLLOUT_MODE_UNKNOWN


def _unknown_phase_kind(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.phase_kind = execution_pb2.PHASE_KIND_UNKNOWN


def _missing_contract(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.ClearField("execution_contract")


def _stage_not_in_contract(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.stage_id = "not-a-contract-phase"
    material = f"{intent.execution_id}\0{intent.stage_id}\0{intent.version}".encode()
    intent.idempotency_key = "intent-sha256-" + hashlib.sha256(material).hexdigest()


def _mismatched_phase_kind(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.phase_kind = execution_pb2.PHASE_KIND_REWARD


def _missing_resources(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.ClearField("resources_per_unit")


def _zero_resources(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.resources_per_unit.Clear()


def _invalid_accelerator(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.resources_per_unit.accelerator_units = nan


def _wrong_idempotency_key(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.idempotency_key = "caller-controlled"


def _expiry_mismatch(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.valid_until.seconds += 1


def _device_selector_with_padding(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.labels[" Device_ID "] = "gpu-0"


def _invalid_preference(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.preferences["score"] = inf


def _duplicate_capability(intent: scheduling_pb2.SchedulingIntent) -> None:
    intent.required_capabilities.names[:] = ["logical-cpu", "logical-cpu"]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (_clear_required_string, "queue"),
        (_zero_version, "version"),
        (_zero_unit_count, "unit_count"),
        (_unknown_rollout_mode, "rollout_mode"),
        (_unknown_phase_kind, "phase_kind"),
        (_missing_contract, "contract"),
        (_stage_not_in_contract, "stage"),
        (_mismatched_phase_kind, "phase_kind"),
        (_missing_resources, "resources_per_unit"),
        (_zero_resources, "resources_per_unit"),
        (_invalid_accelerator, "accelerator_units"),
        (_wrong_idempotency_key, "idempotency"),
        (_expiry_mismatch, "valid_until"),
        (_device_selector_with_padding, "device"),
        (_invalid_preference, "preference"),
        (_duplicate_capability, "capabilit"),
    ],
)
def test_validate_rejects_invalid_mutated_intents(
    mutation: Callable[[scheduling_pb2.SchedulingIntent], None], message: str
) -> None:
    intent = _build_intent(_builder())
    mutation(intent)
    with pytest.raises(IntentValidationError, match=message):
        IntentBuilder.validate(intent)


def test_intent_idempotency_key_is_canonical_and_failed_build_does_not_advance() -> None:
    builder = _builder()
    intent = _build_intent(builder)
    material = b"execution\x00decode\x001"
    assert intent.idempotency_key == "intent-sha256-" + hashlib.sha256(material).hexdigest()
    with pytest.raises(IntentValidationError):
        _build_intent(builder, resources_per_unit=resource_pb2.ResourceVector())
    assert builder.last_version("execution", "decode") == 1


class _Stream:
    def __init__(self, responses: list[scheduling_pb2.WatchDecisionsResponse]) -> None:
        self._responses = responses

    def __aiter__(self) -> AsyncIterator[scheduling_pb2.WatchDecisionsResponse]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[scheduling_pb2.WatchDecisionsResponse]:
        for response in self._responses:
            yield response


class _FailingStream(_Stream):
    async def _iterate(self) -> AsyncIterator[scheduling_pb2.WatchDecisionsResponse]:
        for response in self._responses:
            yield response
        raise grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, details="disconnect")


class _Stub:
    async def PublishIntent(
        self, request: scheduling_pb2.PublishIntentRequest, *, timeout: float
    ) -> scheduling_pb2.PublishIntentResponse:
        return scheduling_pb2.PublishIntentResponse(
            status=scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED,
            execution_id=request.intent.execution_id,
            stage_id=request.intent.stage_id,
            version=request.intent.version,
        )

    def WatchDecisions(
        self, request: scheduling_pb2.WatchDecisionsRequest, *, timeout: None
    ) -> _Stream:
        del request, timeout
        one = scheduling_pb2.WatchDecisionsResponse(
            decision=scheduling_pb2.DecisionRecord(decision_id="d1", sequence=1)
        )
        return _Stream([one, one])


@pytest.mark.asyncio
async def test_async_client_publishes_and_deduplicates_stream() -> None:
    intent = _build_intent(_builder())
    client = SchedulerClient(stub=_Stub())
    response = await client.publish_intent(intent)
    assert response.status == scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED
    decisions = [decision async for decision in client.watch_decisions()]
    assert [decision.decision_id for decision in decisions] == ["d1"]


class _ReconnectStub:
    def __init__(self) -> None:
        self.requests: list[scheduling_pb2.WatchDecisionsRequest] = []

    def WatchDecisions(
        self, request: scheduling_pb2.WatchDecisionsRequest, *, timeout: None
    ) -> _Stream:
        del timeout
        clone = scheduling_pb2.WatchDecisionsRequest()
        clone.CopyFrom(request)
        self.requests.append(clone)
        if len(self.requests) == 1:
            return _FailingStream(
                [
                    scheduling_pb2.WatchDecisionsResponse(),
                    scheduling_pb2.WatchDecisionsResponse(
                        decision=scheduling_pb2.DecisionRecord(decision_id="d1", sequence=6)
                    ),
                ]
            )
        return _Stream(
            [
                scheduling_pb2.WatchDecisionsResponse(),
                scheduling_pb2.WatchDecisionsResponse(
                    decision=scheduling_pb2.DecisionRecord(decision_id="d1", sequence=6)
                ),
                scheduling_pb2.WatchDecisionsResponse(
                    decision=scheduling_pb2.DecisionRecord(decision_id="d2", sequence=7)
                ),
            ]
        )


@pytest.mark.asyncio
async def test_watch_reconnect_uses_one_cursor_and_ignores_empty_heartbeats() -> None:
    stub = _ReconnectStub()
    sleeps: list[float] = []

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)

    client = SchedulerClient(stub=stub, max_retries=1, sleeper=sleeper)
    decisions = [decision async for decision in client.watch_decisions(after_sequence=5)]

    assert [decision.decision_id for decision in decisions] == ["d1", "d2"]
    assert sleeps == [0.1]
    assert len(stub.requests) == 2
    assert stub.requests[0].after_sequence == 5
    assert stub.requests[0].after_decision_id == ""
    resumed = stub.requests[1]
    assert bool(resumed.after_sequence) ^ bool(resumed.after_decision_id)
    assert resumed.after_decision_id == "d1"
    assert resumed.after_sequence == 0


@pytest.mark.asyncio
async def test_watch_rejects_two_initial_cursor_types() -> None:
    client = SchedulerClient(stub=_Stub())
    stream = client.watch_decisions(after_sequence=1, after_decision_id="d1")
    with pytest.raises(ValueError, match="alternative cursors"):
        await anext(stream)


def test_demo_fixture_is_generated_proto_and_byte_stable() -> None:
    fixture = build_demo_fixture(23)
    assert set(fixture) == {"execution_contract", "scheduling_intent", "trace"}
    rendered = render_demo_fixture(23)
    assert hashlib.sha256(rendered.encode()).hexdigest() == (
        "0dc277875279816c0717e193576e4064cbd7c14a540d889e32cf098eb826b396"
    )
    assert '"data_kind": "synthetic"' in rendered


def test_live_fixture_uses_captured_current_time_and_mock_capability_evidence() -> None:
    now = datetime(2026, 8, 27, 10, 30, tzinfo=UTC)
    fixture = build_live_fixture(23, now=now)
    intent = fixture["scheduling_intent"]
    assert isinstance(intent, scheduling_pb2.SchedulingIntent)
    assert intent.submitted_at.ToDatetime(tzinfo=UTC) == now
    assert intent.valid_until.ToDatetime(tzinfo=UTC) == now + timedelta(seconds=60)
    trace = fixture["trace"]
    assert isinstance(trace, trace_pb2.TraceEventBatch)
    assert trace.events[0].occurred_at.ToDatetime(tzinfo=UTC) >= now
    required = intent.required_capabilities
    assert required.source == "mock"
    assert list(required.names) == ["logical-cpu"]
    assert list(required.supported_actions) == ["bind"]
    assert list(required.algorithms) == ["grpo"]
    assert list(required.rollout_modes) == ["partially_async"]
    assert "gpu" not in " ".join(required.names).casefold()


class _LiveStub(_Stub):
    def __init__(self) -> None:
        self.published: list[scheduling_pb2.SchedulingIntent] = []

    async def PublishIntent(
        self, request: scheduling_pb2.PublishIntentRequest, *, timeout: float
    ) -> scheduling_pb2.PublishIntentResponse:
        del timeout
        intent = scheduling_pb2.SchedulingIntent()
        intent.CopyFrom(request.intent)
        self.published.append(intent)
        return scheduling_pb2.PublishIntentResponse(
            status=scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED,
            execution_id=intent.execution_id,
            stage_id=intent.stage_id,
            version=intent.version,
        )

    def WatchDecisions(
        self, request: scheduling_pb2.WatchDecisionsRequest, *, timeout: None
    ) -> _Stream:
        del request, timeout
        return _Stream(
            [
                scheduling_pb2.WatchDecisionsResponse(
                    decision=scheduling_pb2.DecisionRecord(
                        decision_id="live-decision",
                        sequence=1,
                        fallback=False,
                        action_results=[
                            scheduling_pb2.ActionResult(
                                action_id="bind-1",
                                status=scheduling_pb2.ACTION_RESULT_STATUS_SUCCEEDED,
                            )
                        ],
                    )
                )
            ]
        )


@pytest.mark.asyncio
async def test_run_live_demo_publishes_current_compatible_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _LiveStub()

    def client_factory(target: str, *, timeout: float) -> SchedulerClient:
        assert target == "scheduler:50051"
        assert timeout == 10.0
        return SchedulerClient(stub=stub, timeout=timeout)

    monkeypatch.setattr("tgsrl_runtime.demo.SchedulerClient", client_factory)
    result = await run_live_demo("scheduler:50051", seed=23)

    assert len(stub.published) == 1
    assert stub.published[0].valid_until.ToDatetime(tzinfo=UTC) > datetime.now(tz=UTC)
    publish_response = result["publish_response"]
    decision = result["decision"]
    assert isinstance(publish_response, scheduling_pb2.PublishIntentResponse)
    assert isinstance(decision, scheduling_pb2.DecisionRecord)
    assert publish_response.status == scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED
    assert decision.decision_id == "live-decision"
