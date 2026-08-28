"""Deterministic trace replay driven by an injected virtual clock."""

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import ClassVar, Protocol, TypeAlias

from google.protobuf import json_format, timestamp_pb2
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from tgsrl.v1 import execution_pb2, experiment_pb2, resource_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.trace import TraceNormalizer, clone_event


class ReplayError(ValueError):
    """Raised when replay state or a checkpoint is invalid."""


class ReplayArtifactError(ReplayError):
    """Raised when a persisted replay artifact is absent or corrupt."""


REPLAY_STEP_ARTIFACT_KIND = "scheduler-replay-step"
REPLAY_DECISION_ARTIFACT_KIND = "scheduler-decision"
REPLAY_ARTIFACT_SCHEMA_V1 = "tgsrl.replay-artifact.v1"
REPLAY_ARTIFACT_SCHEMA_V2 = "tgsrl.replay-artifact.v2"
REPLAY_ARTIFACT_SCHEMA = REPLAY_ARTIFACT_SCHEMA_V2
DECISION_CANONICALIZER_V1 = "decision-semantic-v1"
DECISION_CANONICALIZER_V2 = "decision-semantic-v2"
REPLAY_STEP_CANONICALIZER_V2 = "replay-step-input-v2-presence"


class SeedMode(StrEnum):
    """How a replay maps its seed onto recorded scheduler inputs."""

    RECORDED = "recorded"
    OVERRIDE = "override"
    DERIVE = "derive"


class DecisionStatus(StrEnum):
    NOT_RECORDED = "not_recorded"
    EQUIVALENT = "equivalent"
    DIFFERENT = "different"


_COMPARISON_STATUS = {
    DecisionStatus.NOT_RECORDED: (experiment_pb2.REPLAY_DECISION_COMPARISON_STATUS_NOT_RECORDED),
    DecisionStatus.EQUIVALENT: (experiment_pb2.REPLAY_DECISION_COMPARISON_STATUS_EQUIVALENT),
    DecisionStatus.DIFFERENT: (experiment_pb2.REPLAY_DECISION_COMPARISON_STATUS_DIFFERENT),
}


class VirtualClock:
    """A monotonic clock that advances only when replay directs it."""

    def __init__(self, start: datetime | None = None) -> None:
        value = start or datetime(1970, 1, 1, tzinfo=UTC)
        if value.tzinfo is None:
            raise ValueError("virtual clock start must be timezone-aware")
        self._now = value.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        if delta < timedelta(0):
            raise ReplayError("virtual clock cannot move backwards")
        self._now += delta
        return self._now

    def advance_to(self, target: datetime) -> datetime:
        if target.tzinfo is None:
            raise ReplayError("virtual clock target must be timezone-aware")
        target = target.astimezone(UTC)
        if target < self._now:
            raise ReplayError("virtual clock cannot move backwards")
        self._now = target
        return self._now


@dataclass(frozen=True, slots=True)
class ReplayCheckpoint:
    """Serializable control-plane cursor; it contains no event mutation."""

    source_digest: str
    index: int
    clock: datetime
    paused: bool
    rate: float
    seed: int
    decision_count: int = 0
    decision_digest: str = hashlib.sha256(b"").hexdigest()
    canonicalizer_version: str = DECISION_CANONICALIZER_V2
    seed_mode: str = SeedMode.RECORDED


ReplayArtifactStep: TypeAlias = experiment_pb2.ReplayStepArtifact  # noqa: UP040
"""Compatibility alias for the generated typed replay-step domain object."""


@dataclass(frozen=True, slots=True)
class DecisionComparison:
    status: DecisionStatus
    equivalent: bool
    expected_digest: str | None
    actual_digest: str


@dataclass(frozen=True, slots=True)
class ReplayStepResult:
    ordinal: int
    event_id: str
    decision: scheduling_pb2.DecisionRecord
    comparison: DecisionComparison


class ReplayScheduler(Protocol):
    async def schedule(
        self,
        intent: scheduling_pb2.SchedulingIntent,
        snapshot: resource_pb2.ClusterSnapshot,
        evaluation_context: scheduling_pb2.EvaluationContext | None = None,
    ) -> scheduling_pb2.DecisionRecord: ...


def _artifact_digest(parts: Iterable[bytes]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return digest.hexdigest()


def _copy_message[MessageT: Message](message: MessageT) -> MessageT:
    clone = type(message)()
    clone.CopyFrom(message)
    return clone


def _step_dynamic_observation(
    step: ReplayArtifactStep,
) -> execution_pb2.ContractObservation | None:
    if step.event.HasField("contract_observation"):
        return _copy_message(step.event.contract_observation)
    if step.intent.HasField("contract_observation"):
        return _copy_message(step.intent.contract_observation)
    return None


def synthesize_evaluation_context(
    step: ReplayArtifactStep,
) -> scheduling_pb2.EvaluationContext:
    """Create a stable compatibility context for legacy replay steps."""
    context = scheduling_pb2.EvaluationContext(
        tick_kind=scheduling_pb2.TICK_KIND_FAST,
        decision_sequence=(
            step.recorded_decision.sequence
            if step.HasField("recorded_decision") and step.recorded_decision.sequence
            else step.ordinal
        ),
        cause="replay-compat",
        observed_revision=step.snapshot.revision,
        compatibility_defaults_applied=True,
    )
    if step.HasField("recorded_decision") and step.recorded_decision.HasField("decided_at"):
        context.evaluation_time.CopyFrom(step.recorded_decision.decided_at)
    elif step.event.HasField("occurred_at"):
        context.evaluation_time.CopyFrom(step.event.occurred_at)
    else:
        context.evaluation_time.FromSeconds(0)
    observation = _step_dynamic_observation(step)
    if observation is not None:
        context.contract_observation.CopyFrom(observation)
    return context


def _synthesize_legacy_evaluation_context(
    step: ReplayArtifactStep,
) -> scheduling_pb2.EvaluationContext:
    """Reproduce the pre-v2 compatibility context only for progress migration."""
    context = scheduling_pb2.EvaluationContext(
        tick_kind=scheduling_pb2.TICK_KIND_FAST,
        decision_sequence=(
            step.recorded_decision.sequence if step.HasField("recorded_decision") else 0
        ),
        cause="replay-compat",
        observed_revision=step.snapshot.revision,
        compatibility_defaults_applied=True,
    )
    if step.HasField("recorded_decision") and step.recorded_decision.HasField("decided_at"):
        context.evaluation_time.CopyFrom(step.recorded_decision.decided_at)
    observation = _step_dynamic_observation(step)
    if observation is not None:
        context.contract_observation.CopyFrom(observation)
    return context


def resolve_evaluation_context(
    step: ReplayArtifactStep,
) -> scheduling_pb2.EvaluationContext:
    """Return an explicit replay-step context, synthesizing legacy defaults when absent."""
    if step.HasField("evaluation_context"):
        return _copy_message(step.evaluation_context)
    return synthesize_evaluation_context(step)


def _decision_artifact_canonicalizer(schema_version: str) -> str:
    if schema_version == REPLAY_ARTIFACT_SCHEMA_V1:
        return DECISION_CANONICALIZER_V1
    if schema_version == REPLAY_ARTIFACT_SCHEMA_V2:
        return DECISION_CANONICALIZER_V2
    raise ReplayArtifactError("unsupported decision artifact schema")


def encode_replay_step_artifact(
    replay_id: str, step: ReplayArtifactStep
) -> experiment_pb2.ReplayArtifact:
    """Encode one exact scheduler replay input into a persistable Proto artifact."""
    if not replay_id:
        raise ReplayArtifactError("replay_id is required")
    if step.ordinal <= 0:
        raise ReplayArtifactError("replay step ordinal must be positive")
    if not step.event.HasField("occurred_at"):
        raise ReplayArtifactError("replay step event occurred_at is required")
    encoded_step = _copy_message(step)
    encoded_step.evaluation_context.CopyFrom(resolve_evaluation_context(step))
    digest = replay_step_digest(encoded_step, schema_version=REPLAY_ARTIFACT_SCHEMA_V2)
    return experiment_pb2.ReplayArtifact(
        artifact_id=f"{replay_id}:step:{step.ordinal}:{digest[:16]}",
        replay_id=replay_id,
        kind=REPLAY_STEP_ARTIFACT_KIND,
        uri=f"inline://replays/{replay_id}/steps/{step.ordinal}",
        digest=digest,
        observed_at=step.event.occurred_at,
        schema_version=REPLAY_ARTIFACT_SCHEMA_V2,
        canonicalizer=REPLAY_STEP_CANONICALIZER_V2,
        replay_step=encoded_step,
    )


def replay_step_digest(
    step: ReplayArtifactStep, *, schema_version: str = REPLAY_ARTIFACT_SCHEMA_V2
) -> str:
    """Return the stable identity of one exact replay scheduler input."""
    event = step.event.SerializeToString(deterministic=True)
    intent = step.intent.SerializeToString(deterministic=True)
    snapshot = step.snapshot.SerializeToString(deterministic=True)
    recorded = step.recorded_decision.SerializeToString(deterministic=True)
    recorded_input = (
        b"\x01" + recorded
        if schema_version == REPLAY_ARTIFACT_SCHEMA_V2 and step.HasField("recorded_decision")
        else b"\x00"
        if schema_version == REPLAY_ARTIFACT_SCHEMA_V2
        else recorded
    )
    parts = [
        schema_version.encode(),
        str(step.ordinal).encode(),
        event,
        intent,
        snapshot,
        recorded_input,
    ]
    if schema_version == REPLAY_ARTIFACT_SCHEMA_V2:
        parts.append(resolve_evaluation_context(step).SerializeToString(deterministic=True))
    elif schema_version != REPLAY_ARTIFACT_SCHEMA_V1:
        raise ReplayArtifactError("unsupported replay artifact schema")
    return _artifact_digest(parts)


def _legacy_v2_replay_step_digest(step: ReplayArtifactStep) -> str:
    """Digest emitted by v2 writers before message-presence authentication."""
    return _artifact_digest(
        [
            REPLAY_ARTIFACT_SCHEMA_V2.encode(),
            str(step.ordinal).encode(),
            step.event.SerializeToString(deterministic=True),
            step.intent.SerializeToString(deterministic=True),
            step.snapshot.SerializeToString(deterministic=True),
            (
                step.recorded_decision.SerializeToString(deterministic=True)
                if step.HasField("recorded_decision")
                else b""
            ),
            resolve_evaluation_context(step).SerializeToString(deterministic=True),
        ]
    )


def decode_replay_step_artifact(
    artifact: experiment_pb2.ReplayArtifact, *, replay_id: str
) -> ReplayArtifactStep:
    if artifact.kind != REPLAY_STEP_ARTIFACT_KIND:
        raise ReplayArtifactError(f"unsupported replay artifact kind: {artifact.kind}")
    if artifact.replay_id != replay_id:
        raise ReplayArtifactError("artifact replay_id does not match its Replay")
    if artifact.schema_version not in {REPLAY_ARTIFACT_SCHEMA_V1, REPLAY_ARTIFACT_SCHEMA_V2}:
        raise ReplayArtifactError("unsupported replay artifact schema")
    if artifact.WhichOneof("typed_payload") != "replay_step":
        raise ReplayArtifactError("replay artifact lacks typed replay_step payload")
    typed = artifact.replay_step
    ordinal = typed.ordinal
    if ordinal <= 0 or not typed.HasField("event") or not typed.HasField("intent"):
        raise ReplayArtifactError("typed replay_step fields are incomplete")
    if not typed.HasField("snapshot"):
        raise ReplayArtifactError("typed replay_step snapshot is required")
    if artifact.schema_version == REPLAY_ARTIFACT_SCHEMA_V2 and not typed.HasField(
        "evaluation_context"
    ):
        raise ReplayArtifactError("v2 replay_step evaluation_context is required")
    event_wire = typed.event.SerializeToString(deterministic=True)
    intent_wire = typed.intent.SerializeToString(deterministic=True)
    snapshot_wire = typed.snapshot.SerializeToString(deterministic=True)
    decision_wire = typed.recorded_decision.SerializeToString(deterministic=True)
    recorded_input = (
        b"\x01" + decision_wire
        if artifact.schema_version == REPLAY_ARTIFACT_SCHEMA_V2
        and typed.HasField("recorded_decision")
        else b"\x00"
        if artifact.schema_version == REPLAY_ARTIFACT_SCHEMA_V2
        else decision_wire
    )
    digest_parts = [
        artifact.schema_version.encode(),
        str(ordinal).encode(),
        event_wire,
        intent_wire,
        snapshot_wire,
        recorded_input,
    ]
    if artifact.schema_version == REPLAY_ARTIFACT_SCHEMA_V2:
        digest_parts.append(typed.evaluation_context.SerializeToString(deterministic=True))
    expected_digest = _artifact_digest(digest_parts)
    if artifact.schema_version == REPLAY_ARTIFACT_SCHEMA_V2:
        if artifact.canonicalizer == REPLAY_STEP_CANONICALIZER_V2:
            pass
        elif not artifact.canonicalizer:
            expected_digest = _legacy_v2_replay_step_digest(typed)
        else:
            raise ReplayArtifactError("unsupported replay step canonicalizer")
    elif artifact.canonicalizer:
        raise ReplayArtifactError("v1 replay step must not declare a canonicalizer")
    if artifact.digest != expected_digest:
        raise ReplayArtifactError("replay artifact digest mismatch")
    decoded = _clone_message(typed)
    if artifact.schema_version == REPLAY_ARTIFACT_SCHEMA_V1:
        # EvaluationContext was added in v2 and is therefore not authenticated by a
        # v1 digest. Always derive it from the durable v1 fields, even if a newer
        # writer placed an unknown-to-v1 context in the payload.
        decoded.ClearField("evaluation_context")
        decoded.evaluation_context.CopyFrom(synthesize_evaluation_context(decoded))
    return decoded


def replay_step_progress_digests(
    artifact: experiment_pb2.ReplayArtifact,
    step: ReplayArtifactStep,
) -> frozenset[str]:
    """Return stable and migration-compatible durable progress identities."""
    accepted = {artifact.digest}
    if artifact.schema_version == REPLAY_ARTIFACT_SCHEMA_V1:
        # Two released readers persisted a derived v2 digest instead of the v1
        # artifact identity. Accept both context synthesis generations while new
        # writes use the authenticated, synthesis-independent artifact digest.
        accepted.add(_legacy_v2_replay_step_digest(step))
        legacy_step = _copy_message(step)
        legacy_step.evaluation_context.CopyFrom(_synthesize_legacy_evaluation_context(step))
        accepted.add(_legacy_v2_replay_step_digest(legacy_step))
    return frozenset(accepted)


def decode_replay_steps(replay: experiment_pb2.Replay) -> tuple[ReplayArtifactStep, ...]:
    artifacts = [
        artifact for artifact in replay.artifacts if artifact.kind == REPLAY_STEP_ARTIFACT_KIND
    ]
    if not artifacts:
        raise ReplayArtifactError("replay has no scheduler input artifacts")
    steps = tuple(
        sorted(
            (
                decode_replay_step_artifact(artifact, replay_id=replay.replay_id)
                for artifact in artifacts
            ),
            key=lambda step: step.ordinal,
        )
    )
    if len({step.ordinal for step in steps}) != len(steps):
        raise ReplayArtifactError("replay artifact ordinals must be unique")
    return steps


def replay_start_key_digest(idempotency_key: str) -> str:
    """Return the non-secret identity persisted for an in-flight START command."""
    if not idempotency_key:
        raise ReplayArtifactError("replay START idempotency_key is required")
    return hashlib.sha256(idempotency_key.encode()).hexdigest()


def encode_decision_artifact(
    replay_id: str,
    result: ReplayStepResult,
    *,
    observed_at: datetime,
) -> experiment_pb2.ReplayArtifact:
    if result.ordinal <= 0:
        raise ReplayArtifactError("decision artifact ordinal must be positive")
    decision_wire = result.decision.SerializeToString(deterministic=True)
    digest = _artifact_digest(
        [
            REPLAY_ARTIFACT_SCHEMA_V2.encode(),
            str(result.ordinal).encode(),
            decision_wire,
        ]
    )
    observed = timestamp_pb2.Timestamp()
    observed.FromDatetime(observed_at)
    return experiment_pb2.ReplayArtifact(
        artifact_id=f"{replay_id}:decision:{result.ordinal}:{digest[:16]}",
        replay_id=replay_id,
        kind=REPLAY_DECISION_ARTIFACT_KIND,
        uri=f"inline://replays/{replay_id}/decisions/{result.ordinal}",
        digest=digest,
        observed_at=observed,
        schema_version=REPLAY_ARTIFACT_SCHEMA_V2,
        canonicalizer=DECISION_CANONICALIZER_V2,
        replay_decision=experiment_pb2.ReplayDecisionArtifact(
            ordinal=result.ordinal,
            event_id=result.event_id,
            decision=result.decision,
            semantic_digest=result.comparison.actual_digest,
            expected_digest=result.comparison.expected_digest or "",
            comparison_status=_COMPARISON_STATUS[result.comparison.status],
        ),
    )


def decode_decision_artifacts(
    replay: experiment_pb2.Replay,
) -> tuple[scheduling_pb2.DecisionRecord, ...]:
    decoded: list[tuple[int, scheduling_pb2.DecisionRecord]] = []
    for artifact in replay.artifacts:
        if artifact.kind != REPLAY_DECISION_ARTIFACT_KIND:
            continue
        if artifact.schema_version not in {REPLAY_ARTIFACT_SCHEMA_V1, REPLAY_ARTIFACT_SCHEMA_V2}:
            raise ReplayArtifactError("unsupported decision artifact schema")
        if artifact.WhichOneof("typed_payload") != "replay_decision":
            raise ReplayArtifactError("decision artifact lacks typed replay_decision payload")
        typed = artifact.replay_decision
        if artifact.replay_id != replay.replay_id:
            raise ReplayArtifactError("decision artifact replay_id does not match its Replay")
        expected_canonicalizer = _decision_artifact_canonicalizer(artifact.schema_version)
        if artifact.canonicalizer != expected_canonicalizer:
            raise ReplayArtifactError("unsupported decision canonicalizer version")
        if typed.ordinal <= 0 or not typed.HasField("decision"):
            raise ReplayArtifactError("typed replay_decision fields are incomplete")
        ordinal = typed.ordinal
        decision_wire = typed.decision.SerializeToString(deterministic=True)
        expected = _artifact_digest(
            [
                artifact.schema_version.encode(),
                str(ordinal).encode(),
                decision_wire,
            ]
        )
        if artifact.digest != expected:
            raise ReplayArtifactError("decision artifact digest mismatch")
        decoded.append((ordinal, _clone_message(typed.decision)))
    ordinals = [ordinal for ordinal, _ in decoded]
    if len(set(ordinals)) != len(ordinals):
        raise ReplayArtifactError("decision artifact ordinals must be unique")
    return tuple(decision for _, decision in sorted(decoded))


def _clone_message[MessageT: Message](message: MessageT) -> MessageT:
    clone = type(message)()
    clone.CopyFrom(message)
    return clone


class DecisionCanonicalizer:
    """Canonicalize scheduler output while excluding volatile run-local identity."""

    version = DECISION_CANONICALIZER_V2
    legacy_version = DECISION_CANONICALIZER_V1
    _volatile_fields: ClassVar[frozenset[str]] = frozenset(
        {
            "action_id",
            "binding_id",
            "completed_at",
            "created_at",
            "cursor",
            "deadline",
            "decided_at",
            "decision_id",
            "expires_at",
            "idempotency_key",
            "plan_id",
            "sandbox_id",
            "sequence",
            "started_at",
        }
    )
    _unordered_repeated_fields: ClassVar[frozenset[str]] = frozenset(
        {"contract_evaluations", "observations", "evidence", "missing_keys"}
    )

    def semantic_bytes(self, decision: scheduling_pb2.DecisionRecord) -> bytes:
        value = json_format.MessageToDict(
            decision, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
        )
        normalized = self._strip_volatile(value)
        return json.dumps(
            normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()

    def canonical_decision(
        self, decision: scheduling_pb2.DecisionRecord
    ) -> scheduling_pb2.DecisionRecord:
        """Return a Proto decision with volatile run-local identity cleared."""
        canonical = _clone_message(decision)
        self._clear_volatile(canonical)
        return canonical

    def digest(self, decision: scheduling_pb2.DecisionRecord) -> str:
        return hashlib.sha256(self.semantic_bytes(decision)).hexdigest()

    def compare(
        self,
        expected: scheduling_pb2.DecisionRecord | None,
        actual: scheduling_pb2.DecisionRecord,
    ) -> DecisionComparison:
        actual_digest = self.digest(actual)
        if expected is None:
            return DecisionComparison(DecisionStatus.NOT_RECORDED, False, None, actual_digest)
        expected_digest = self.digest(expected)
        equivalent = expected_digest == actual_digest
        return DecisionComparison(
            DecisionStatus.EQUIVALENT if equivalent else DecisionStatus.DIFFERENT,
            equivalent,
            expected_digest,
            actual_digest,
        )

    def _strip_volatile(self, value: object, path: tuple[str, ...] = ()) -> object:
        if isinstance(value, dict):
            normalized_dict: dict[str, object] = {
                key: self._strip_volatile(item, (*path, key))
                for key, item in sorted(value.items())
                if key not in self._volatile_fields
            }
            return normalized_dict
        if isinstance(value, list):
            normalized_list = [self._strip_volatile(item, path) for item in value]
            if path and path[-1] in self._unordered_repeated_fields:
                normalized_list.sort(
                    key=lambda item: json.dumps(
                        item, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                    )
                )
            return normalized_list
        return value

    def _clear_volatile(self, message: Message) -> None:
        descriptor = getattr(message, "DESCRIPTOR", None)
        if descriptor is None:
            return
        for field in descriptor.fields:
            if field.name in self._volatile_fields:
                message.ClearField(field.name)
                continue
            value = getattr(message, field.name)
            if field.label == FieldDescriptor.LABEL_REPEATED:
                if field.message_type is not None:
                    for item in value:
                        self._clear_volatile(item)
                continue
            if field.message_type is not None and message.HasField(field.name):
                self._clear_volatile(value)


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


class ReplayController:
    """Replay normalized event clones with pause, step, and checkpoint controls."""

    def __init__(
        self,
        events: Iterable[trace_pb2.TraceEvent],
        *,
        seed: int,
        rate: float = 1.0,
        clock: VirtualClock | None = None,
        sleeper: Callable[[float], Awaitable[None]] = _sleep,
    ) -> None:
        if seed < 0:
            raise ValueError("seed must be non-negative")
        if rate <= 0:
            raise ValueError("rate must be positive")
        normalized = TraceNormalizer().normalize(events)
        self._wire_events = tuple(
            event.SerializeToString(deterministic=True) for event in normalized
        )
        digest = hashlib.sha256()
        for wire in self._wire_events:
            digest.update(len(wire).to_bytes(8, "big"))
            digest.update(wire)
        self._source_digest = digest.hexdigest()
        self.seed = seed
        self.rate = rate
        self.clock = clock or VirtualClock()
        self._sleeper = sleeper
        self._index = 0
        self._paused = False
        self._pause_generation = 0

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def position(self) -> int:
        return self._index

    @property
    def remaining(self) -> int:
        return len(self._wire_events) - self._index

    def pause(self) -> None:
        self._paused = True
        self._pause_generation += 1

    def resume(self) -> None:
        self._paused = False

    def checkpoint(self) -> ReplayCheckpoint:
        return ReplayCheckpoint(
            source_digest=self._source_digest,
            index=self._index,
            clock=self.clock.now(),
            paused=self._paused,
            rate=self.rate,
            seed=self.seed,
        )

    def restore(self, checkpoint: ReplayCheckpoint) -> None:
        if checkpoint.source_digest != self._source_digest:
            raise ReplayError("checkpoint belongs to a different event stream")
        if checkpoint.seed != self.seed:
            raise ReplayError("checkpoint seed does not match controller seed")
        if not 0 <= checkpoint.index <= len(self._wire_events):
            raise ReplayError("checkpoint index is outside the event stream")
        if checkpoint.rate <= 0:
            raise ReplayError("checkpoint rate must be positive")
        if checkpoint.clock.tzinfo is None:
            raise ReplayError("checkpoint clock must be timezone-aware")
        self._index = checkpoint.index
        self.rate = checkpoint.rate
        self._paused = checkpoint.paused
        self.clock = VirtualClock(checkpoint.clock)

    def next_event(self) -> trace_pb2.TraceEvent | None:
        """Emit immediately unless paused; virtual time follows event time."""
        if self._paused or self._index >= len(self._wire_events):
            return None
        return self._emit_next()

    def step(self) -> trace_pb2.TraceEvent | None:
        """Emit exactly one event while preserving the pause state."""
        if self._index >= len(self._wire_events):
            return None
        was_paused = self._paused
        event = self._emit_next()
        self._paused = was_paused
        return event

    def _emit_next(self) -> trace_pb2.TraceEvent:
        event = trace_pb2.TraceEvent.FromString(self._wire_events[self._index])
        event_time = event.occurred_at.ToDatetime(tzinfo=UTC)
        if event_time > self.clock.now():
            self.clock.advance_to(event_time)
        self._index += 1
        return clone_event(event)

    async def events(self) -> AsyncIterator[trace_pb2.TraceEvent]:
        """Yield until paused or exhausted, sleeping according to playback rate."""
        previous_time = self.clock.now() if self._index else None
        while not self._paused and self._index < len(self._wire_events):
            event = trace_pb2.TraceEvent.FromString(self._wire_events[self._index])
            event_time = event.occurred_at.ToDatetime(tzinfo=UTC)
            if previous_time is not None:
                delay = max(0.0, (event_time - previous_time).total_seconds() / self.rate)
                if delay:
                    pause_generation = self._pause_generation
                    await self._sleeper(delay)
                    if self._paused or pause_generation != self._pause_generation:
                        return
            emitted = self._emit_next()
            previous_time = event_time
            yield emitted


class SchedulerReplayRunner:
    """Drive the pure Scheduler preview RPC from exact recorded replay inputs."""

    def __init__(
        self,
        steps: Iterable[ReplayArtifactStep],
        *,
        scheduler: ReplayScheduler,
        seed: int,
        seed_mode: SeedMode = SeedMode.RECORDED,
        canonicalizer: DecisionCanonicalizer | None = None,
    ) -> None:
        if seed < 0:
            raise ReplayError("seed must be non-negative")
        self._steps = tuple(sorted(steps, key=lambda step: step.ordinal))
        if [step.ordinal for step in self._steps] != list(range(1, len(self._steps) + 1)):
            raise ReplayError("replay step ordinals must be contiguous from one")
        if len({step.event.event_id for step in self._steps}) != len(self._steps):
            raise ReplayError("replay steps require unique trigger events")
        self.scheduler = scheduler
        self.seed = seed
        self.seed_mode = seed_mode
        self.canonicalizer = canonicalizer or DecisionCanonicalizer()
        self._index = 0
        self._results: list[ReplayStepResult] = []
        self._source_digest = self._digest_steps()

    @property
    def results(self) -> tuple[ReplayStepResult, ...]:
        return tuple(self._results)

    @property
    def remaining(self) -> int:
        return len(self._steps) - self._index

    async def step(self) -> ReplayStepResult | None:
        if self._index >= len(self._steps):
            return None
        recorded = self._steps[self._index]
        intent = _clone_message(recorded.intent)
        snapshot = _clone_message(recorded.snapshot)
        evaluation_context = resolve_evaluation_context(recorded)
        self._apply_seed(intent, recorded.ordinal)
        decision = await self.scheduler.schedule(
            intent, snapshot, evaluation_context=_copy_message(evaluation_context)
        )
        expected = recorded.recorded_decision if recorded.HasField("recorded_decision") else None
        comparison = self.canonicalizer.compare(expected, decision)
        result = ReplayStepResult(
            recorded.ordinal,
            recorded.event.event_id,
            _clone_message(decision),
            comparison,
        )
        self._results.append(result)
        self._index += 1
        return result

    def restore_decision(self, decision: scheduling_pb2.DecisionRecord) -> ReplayStepResult:
        """Restore one contiguous durable decision without scheduling it again."""
        if self._index >= len(self._steps):
            raise ReplayError("cannot restore past the final replay step")
        recorded = self._steps[self._index]
        expected = recorded.recorded_decision if recorded.HasField("recorded_decision") else None
        result = ReplayStepResult(
            ordinal=recorded.ordinal,
            event_id=recorded.event.event_id,
            decision=_clone_message(decision),
            comparison=self.canonicalizer.compare(expected, decision),
        )
        self._results.append(result)
        self._index += 1
        return result

    async def run(self) -> tuple[ReplayStepResult, ...]:
        while await self.step() is not None:
            pass
        return self.results

    def checkpoint(self) -> ReplayCheckpoint:
        return ReplayCheckpoint(
            source_digest=self._source_digest,
            index=self._index,
            clock=datetime.fromtimestamp(0, tz=UTC),
            paused=False,
            rate=1.0,
            seed=self.seed,
            decision_count=len(self._results),
            decision_digest=self._decision_digest(),
            canonicalizer_version=self.canonicalizer.version,
            seed_mode=self.seed_mode,
        )

    def restore(self, checkpoint: ReplayCheckpoint, results: Iterable[ReplayStepResult]) -> None:
        restored_results = list(results)
        if checkpoint.source_digest != self._source_digest:
            raise ReplayError("checkpoint belongs to another replay artifact")
        if checkpoint.seed != self.seed or checkpoint.seed_mode != self.seed_mode:
            raise ReplayError("checkpoint seed configuration does not match")
        if checkpoint.canonicalizer_version != self.canonicalizer.version:
            raise ReplayError("checkpoint canonicalizer version does not match")
        if checkpoint.index != checkpoint.decision_count:
            raise ReplayError("checkpoint must be captured between scheduler steps")
        if checkpoint.index != len(restored_results):
            raise ReplayError("checkpoint result count does not match supplied decisions")
        self._results = restored_results
        if self._decision_digest() != checkpoint.decision_digest:
            self._results = []
            raise ReplayError("checkpoint decision digest does not match supplied decisions")
        self._index = checkpoint.index

    def _apply_seed(self, intent: scheduling_pb2.SchedulingIntent, ordinal: int) -> None:
        if self.seed_mode is SeedMode.RECORDED:
            if intent.deterministic_seed != self.seed:
                raise ReplayError("recorded intent seed conflicts with replay seed")
        elif self.seed_mode is SeedMode.OVERRIDE:
            intent.deterministic_seed = self.seed
        else:
            material = f"{self.seed}:{self._source_digest}:{ordinal}".encode()
            intent.deterministic_seed = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")

    def _digest_steps(self) -> str:
        digest = hashlib.sha256()
        for step in self._steps:
            wire = replay_step_digest(step, schema_version=REPLAY_ARTIFACT_SCHEMA_V2).encode()
            digest.update(len(wire).to_bytes(8, "big"))
            digest.update(wire)
        return digest.hexdigest()

    def _decision_digest(self) -> str:
        digest = hashlib.sha256()
        for result in self._results:
            semantic = self.canonicalizer.semantic_bytes(result.decision)
            digest.update(len(semantic).to_bytes(8, "big"))
            digest.update(semantic)
        return digest.hexdigest()
