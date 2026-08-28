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
from tgsrl.v1 import experiment_pb2, resource_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.trace import TraceNormalizer, clone_event


class ReplayError(ValueError):
    """Raised when replay state or a checkpoint is invalid."""


class ReplayArtifactError(ReplayError):
    """Raised when a persisted replay artifact is absent or corrupt."""


REPLAY_STEP_ARTIFACT_KIND = "scheduler-replay-step"
REPLAY_DECISION_ARTIFACT_KIND = "scheduler-decision"
REPLAY_ARTIFACT_SCHEMA = "tgsrl.replay-artifact.v1"


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
    canonicalizer_version: str = "decision-semantic-v1"
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
    ) -> scheduling_pb2.DecisionRecord: ...


def _artifact_digest(parts: Iterable[bytes]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return digest.hexdigest()


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
    digest = replay_step_digest(step)
    return experiment_pb2.ReplayArtifact(
        artifact_id=f"{replay_id}:step:{step.ordinal}:{digest[:16]}",
        replay_id=replay_id,
        kind=REPLAY_STEP_ARTIFACT_KIND,
        uri=f"inline://replays/{replay_id}/steps/{step.ordinal}",
        digest=digest,
        observed_at=step.event.occurred_at,
        schema_version=REPLAY_ARTIFACT_SCHEMA,
        replay_step=experiment_pb2.ReplayStepArtifact(
            ordinal=step.ordinal,
            event=step.event,
            intent=step.intent,
            snapshot=step.snapshot,
            recorded_decision=step.recorded_decision,
        ),
    )


def replay_step_digest(step: ReplayArtifactStep) -> str:
    """Return the stable identity of one exact replay scheduler input."""
    event = step.event.SerializeToString(deterministic=True)
    intent = step.intent.SerializeToString(deterministic=True)
    snapshot = step.snapshot.SerializeToString(deterministic=True)
    recorded = (
        step.recorded_decision.SerializeToString(deterministic=True)
        if step.HasField("recorded_decision")
        else b""
    )
    return _artifact_digest(
        [
            REPLAY_ARTIFACT_SCHEMA.encode(),
            str(step.ordinal).encode(),
            event,
            intent,
            snapshot,
            recorded,
        ]
    )


def decode_replay_step_artifact(
    artifact: experiment_pb2.ReplayArtifact, *, replay_id: str
) -> ReplayArtifactStep:
    if artifact.kind != REPLAY_STEP_ARTIFACT_KIND:
        raise ReplayArtifactError(f"unsupported replay artifact kind: {artifact.kind}")
    if artifact.replay_id != replay_id:
        raise ReplayArtifactError("artifact replay_id does not match its Replay")
    if artifact.schema_version != REPLAY_ARTIFACT_SCHEMA:
        raise ReplayArtifactError("unsupported replay artifact schema")
    if artifact.WhichOneof("typed_payload") != "replay_step":
        raise ReplayArtifactError("replay artifact lacks typed replay_step payload")
    typed = artifact.replay_step
    ordinal = typed.ordinal
    if ordinal <= 0 or not typed.HasField("event") or not typed.HasField("intent"):
        raise ReplayArtifactError("typed replay_step fields are incomplete")
    if not typed.HasField("snapshot"):
        raise ReplayArtifactError("typed replay_step snapshot is required")
    event_wire = typed.event.SerializeToString(deterministic=True)
    intent_wire = typed.intent.SerializeToString(deterministic=True)
    snapshot_wire = typed.snapshot.SerializeToString(deterministic=True)
    decision_wire = (
        typed.recorded_decision.SerializeToString(deterministic=True)
        if typed.HasField("recorded_decision")
        else b""
    )
    expected_digest = _artifact_digest(
        [
            REPLAY_ARTIFACT_SCHEMA.encode(),
            str(ordinal).encode(),
            event_wire,
            intent_wire,
            snapshot_wire,
            decision_wire,
        ]
    )
    if artifact.digest != expected_digest:
        raise ReplayArtifactError("replay artifact digest mismatch")
    return _clone_message(typed)


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
            REPLAY_ARTIFACT_SCHEMA.encode(),
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
        schema_version=REPLAY_ARTIFACT_SCHEMA,
        canonicalizer=DecisionCanonicalizer.version,
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
        if artifact.schema_version != REPLAY_ARTIFACT_SCHEMA:
            raise ReplayArtifactError("unsupported decision artifact schema")
        if artifact.WhichOneof("typed_payload") != "replay_decision":
            raise ReplayArtifactError("decision artifact lacks typed replay_decision payload")
        typed = artifact.replay_decision
        if artifact.replay_id != replay.replay_id:
            raise ReplayArtifactError("decision artifact replay_id does not match its Replay")
        if artifact.canonicalizer != DecisionCanonicalizer.version:
            raise ReplayArtifactError("unsupported decision canonicalizer version")
        if typed.ordinal <= 0 or not typed.HasField("decision"):
            raise ReplayArtifactError("typed replay_decision fields are incomplete")
        ordinal = typed.ordinal
        decision_wire = typed.decision.SerializeToString(deterministic=True)
        expected = _artifact_digest(
            [
                REPLAY_ARTIFACT_SCHEMA.encode(),
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

    version = "decision-semantic-v1"
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

    def _strip_volatile(self, value: object) -> object:
        if isinstance(value, dict):
            return {
                key: self._strip_volatile(item)
                for key, item in sorted(value.items())
                if key not in self._volatile_fields
            }
        if isinstance(value, list):
            return [self._strip_volatile(item) for item in value]
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
        self._apply_seed(intent, recorded.ordinal)
        decision = await self.scheduler.schedule(intent, snapshot)
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
            for message in (step.event, step.intent, step.snapshot):
                wire = message.SerializeToString(deterministic=True)
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
