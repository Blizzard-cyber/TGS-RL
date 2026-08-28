"""Intent, async client, and demo fixture integration tests."""

import hashlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from math import inf, nan
from typing import Any

import grpc
import pytest
from google.protobuf import timestamp_pb2
from tgsrl.v1 import (
    execution_pb2,
    resource_pb2,
    runtime_pb2,
    scheduling_pb2,
    semantic_pb2,
    trace_pb2,
)
from tgsrl_runtime.aggregation import TraceSummary
from tgsrl_runtime.demo import (
    build_demo_fixture,
    build_live_fixture,
    render_demo_fixture,
    run_live_demo,
)
from tgsrl_runtime.intent import IntentBuilder, IntentValidationError
from tgsrl_runtime.intent_coordinator import IntentCoordinator
from tgsrl_runtime.scheduler_client import SchedulerClient

from adapters import GRPOAdapter, PartialAsyncRolloutAdapter, build_execution_contract


def _timestamp(seconds: int) -> timestamp_pb2.Timestamp:
    value = timestamp_pb2.Timestamp()
    value.FromDatetime(datetime(2025, 1, 1, 0, 0, seconds, tzinfo=UTC))
    return value


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


def _observation(
    *,
    phase_id: str = "decode",
    policy_version: str = "policy-1",
) -> execution_pb2.ContractObservation:
    observation = execution_pb2.ContractObservation(
        source="runtime",
        event_id="evt-1",
        phase_id=phase_id,
        policy_version=policy_version,
        policy_lag=1,
        sample_stale=False,
        buffer_level=4,
        accepted_samples=8,
        expected_samples=8,
        sample_count=8,
        safe_point=True,
        typed_facts=[
            semantic_pb2.SemanticField(
                key="sample.policy_lag",
                value=semantic_pb2.SemanticValue(uint64_value=1),
            ),
            semantic_pb2.SemanticField(
                key="sample.stale",
                value=semantic_pb2.SemanticValue(bool_value=False),
            ),
            semantic_pb2.SemanticField(
                key="buffer.level.current",
                value=semantic_pb2.SemanticValue(uint64_value=4),
            ),
            semantic_pb2.SemanticField(
                key="group.accepted_samples",
                value=semantic_pb2.SemanticValue(uint64_value=8),
            ),
            semantic_pb2.SemanticField(
                key="group.expected_samples",
                value=semantic_pb2.SemanticValue(uint64_value=8),
            ),
            semantic_pb2.SemanticField(
                key="sample.sample_count",
                value=semantic_pb2.SemanticValue(uint64_value=8),
            ),
            semantic_pb2.SemanticField(
                key="runtime.safe_point",
                value=semantic_pb2.SemanticValue(bool_value=True),
            ),
        ],
    )
    observation.observed_at.FromDatetime(datetime(2025, 1, 1, tzinfo=UTC))
    return observation


def test_builder_clones_contract_observation_and_validates_it() -> None:
    builder = _builder()
    observation = _observation()

    intent = _build_intent(builder, contract_observation=observation)
    observation.event_id = "mutated-after-build"

    assert intent.contract_observation.event_id == "evt-1"
    assert intent.contract_observation.phase_id == "decode"
    assert intent.contract_observation.accepted_samples == 8


def test_validate_rejects_contract_observation_mismatches() -> None:
    intent = _build_intent(_builder(), contract_observation=_observation())
    intent.contract_observation.policy_version = "policy-2"

    with pytest.raises(IntentValidationError, match="policy_version"):
        IntentBuilder.validate(intent)

    intent = _build_intent(_builder(), contract_observation=_observation())
    intent.contract_observation.phase_id = "reward"

    with pytest.raises(IntentValidationError, match="phase_id"):
        IntentBuilder.validate(intent)


def test_intent_coordinator_writes_stage_matched_observation_as_authority() -> None:
    coordinator = IntentCoordinator(builder=_builder())
    manifest = runtime_pb2.RuntimeManifest(
        manifest_id="manifest-1",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        framework="fake",
        execution_backend="fake",
        trainer="fake",
        rollout_engine="fake",
        compatibility_profile="cpu-mock",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
        execution_contract=build_execution_contract(GRPOAdapter(), PartialAsyncRolloutAdapter()),
        resources_per_unit=resource_pb2.ResourceVector(cpu_millis=1000, memory_bytes=1 << 30),
        desired_units=1,
        priority=7,
        queue="gold",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version="policy-1",
        deterministic_seed=11,
    )
    runtime_unit = runtime_pb2.RuntimeUnit(
        runtime_unit_id="unit-1",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        kind=runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        phase_id="decode",
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        state=runtime_pb2.RUNTIME_STATE_REQUESTED,
        requested_resources=resource_pb2.ResourceVector(cpu_millis=1000, memory_bytes=1 << 30),
        required_capabilities=resource_pb2.CapabilitySet(names=["runtime-unit"]),
        execution_id="execution-1",
        stage_id="decode",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    reward_runtime_unit = runtime_pb2.RuntimeUnit(
        runtime_unit_id="unit-2",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        kind=runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        phase_id="reward",
        phase_kind=execution_pb2.PHASE_KIND_REWARD,
        state=runtime_pb2.RUNTIME_STATE_REQUESTED,
        requested_resources=resource_pb2.ResourceVector(cpu_millis=1000, memory_bytes=1 << 30),
        required_capabilities=resource_pb2.CapabilitySet(names=["runtime-unit"]),
        execution_id="execution-1",
        stage_id="reward",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    decode_observation = _observation()
    summary = TraceSummary(
        event_count=2,
        safe_point_count=1,
        policy_publish_count=0,
        phase_counts={"decode": 2},
        max_buffer_level=11,
        latest_buffer_level=4,
        latest_safe_point=True,
        latest_policy_version="policy-1",
        latest_phase_id="decode",
        latest_event_id="evt-1",
        latest_observation=decode_observation,
        micro_stage_count=1,
        completed_micro_stage_count=1,
        incomplete_micro_stage_count=0,
        total_micro_stage_seconds=1.0,
        micro_stages=(),
        latest_observations_by_stage={"decode": decode_observation},
    )

    intents = coordinator.build_for_units(manifest, [runtime_unit, reward_runtime_unit], summary)

    assert len(intents) == 2
    decode_intent, reward_intent = intents
    assert decode_intent.stage_id == "decode"
    assert decode_intent.contract_observation.event_id == "evt-1"
    assert decode_intent.contract_observation.buffer_level == 4
    assert decode_intent.preferences["max_buffer_level"] == 11.0
    assert decode_intent.preferences["latest_buffer_level"] == 4.0
    assert reward_intent.stage_id == "reward"
    assert not reward_intent.HasField("contract_observation")


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
    def __init__(self) -> None:
        self.schedule_requests: list[scheduling_pb2.ScheduleRequest] = []

    async def PublishIntent(
        self, request: scheduling_pb2.PublishIntentRequest, *, timeout: float
    ) -> scheduling_pb2.PublishIntentResponse:
        return scheduling_pb2.PublishIntentResponse(
            status=scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED,
            execution_id=request.intent.execution_id,
            stage_id=request.intent.stage_id,
            version=request.intent.version,
        )

    async def Schedule(
        self, request: scheduling_pb2.ScheduleRequest, *, timeout: float
    ) -> scheduling_pb2.ScheduleResponse:
        del timeout
        clone = scheduling_pb2.ScheduleRequest()
        clone.CopyFrom(request)
        self.schedule_requests.append(clone)
        return scheduling_pb2.ScheduleResponse(
            decision=scheduling_pb2.DecisionRecord(
                decision_id="scheduled-1",
                execution_id=request.intent.execution_id,
                stage_id=request.intent.stage_id,
                snapshot_revision=request.snapshot.revision,
                evaluation_context=request.evaluation_context,
            )
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


@pytest.mark.asyncio
async def test_schedule_forwards_optional_evaluation_context() -> None:
    stub = _Stub()
    client = SchedulerClient(stub=stub)
    intent = _build_intent(_builder())
    snapshot = resource_pb2.ClusterSnapshot(snapshot_id="snapshot-1", revision=17)
    context = scheduling_pb2.EvaluationContext(
        tick_kind=scheduling_pb2.TICK_KIND_FAST,
        evaluation_time=_timestamp(5),
        decision_sequence=23,
        cause="replay-step",
        observed_revision=17,
        contract_observation=execution_pb2.ContractObservation(
            observed_at=_timestamp(4),
            policy_lag=2,
            safe_point=True,
            source="recorded-event",
            event_id="event-1",
            typed_facts=[
                semantic_pb2.SemanticField(
                    key="sample.policy_lag",
                    value=semantic_pb2.SemanticValue(uint64_value=2),
                )
            ],
        ),
    )
    expected_context = context.SerializeToString(deterministic=True)

    decision = await client.schedule(intent, snapshot, evaluation_context=context)

    assert len(stub.schedule_requests) == 1
    request = stub.schedule_requests[0]
    assert request.intent.execution_id == intent.execution_id
    assert request.snapshot.snapshot_id == "snapshot-1"
    assert request.evaluation_context.SerializeToString(deterministic=True) == expected_context
    assert decision.evaluation_context.SerializeToString(deterministic=True) == expected_context
    assert context.SerializeToString(deterministic=True) == expected_context


@pytest.mark.asyncio
async def test_schedule_omits_absent_evaluation_context() -> None:
    stub = _Stub()
    client = SchedulerClient(stub=stub)

    await client.schedule(
        _build_intent(_builder()),
        resource_pb2.ClusterSnapshot(snapshot_id="snapshot-1", revision=17),
    )

    assert len(stub.schedule_requests) == 1
    assert not stub.schedule_requests[0].HasField("evaluation_context")


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
        "456265b178b5e48049793ddb963179709e58eeff61c895595fd34b502e0fe3e3"
    )
    assert '"data_kind": "synthetic"' in rendered


def test_live_fixture_uses_captured_current_time_and_mock_capability_evidence() -> None:
    now = datetime(2026, 8, 27, 10, 30, tzinfo=UTC)
    fixture = build_live_fixture(23, now=now)
    intent = fixture["scheduling_intent"]
    assert isinstance(intent, scheduling_pb2.SchedulingIntent)
    assert intent.submitted_at.ToDatetime(tzinfo=UTC) == now
    assert intent.valid_until.ToDatetime(tzinfo=UTC) == now + timedelta(seconds=60)
    observation = intent.contract_observation
    assert observation.observed_at.ToDatetime(tzinfo=UTC) == now
    assert observation.source == "synthetic-demo"
    assert observation.event_id.endswith("-intent-decode")
    assert observation.phase_id == intent.stage_id == "decode"
    assert observation.policy_version == intent.policy_version == "policy-1"
    assert observation.policy_lag == 1
    assert observation.accepted_samples == observation.expected_samples == 8
    assert observation.buffer_level == 0
    assert observation.safe_point is True
    typed_facts = {field.key: field.value for field in observation.typed_facts}
    assert typed_facts["sample.policy_lag"].uint64_value == observation.policy_lag
    assert typed_facts["group.accepted_samples"].uint64_value == observation.accepted_samples
    assert typed_facts["group.expected_samples"].uint64_value == observation.expected_samples
    assert typed_facts["buffer.level.current"].uint64_value == observation.buffer_level
    assert typed_facts["runtime.safe_point"].bool_value is observation.safe_point
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
    published_observation = stub.published[0].contract_observation
    assert published_observation.phase_id == stub.published[0].stage_id == "decode"
    assert published_observation.policy_version == stub.published[0].policy_version == "policy-1"
    assert published_observation.accepted_samples == 8
    assert published_observation.expected_samples == 8
    assert published_observation.safe_point is True
    publish_response = result["publish_response"]
    decision = result["decision"]
    assert isinstance(publish_response, scheduling_pb2.PublishIntentResponse)
    assert isinstance(decision, scheduling_pb2.DecisionRecord)
    assert publish_response.status == scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED
    assert decision.decision_id == "live-decision"
