"""Deterministic synthetic trace workloads for representative RL gaps."""

import hashlib
import random
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from tgsrl.v1 import execution_pb2, semantic_pb2, trace_pb2

from tgsrl_runtime.proto_utils import timestamp_from_datetime


class SyntheticScenario(StrEnum):
    """Synthetic scenarios supported by the local runtime."""

    TOOL_WAIT = "tool_wait"
    LONG_TAIL = "long_tail"
    TRAINER_STARVATION = "trainer_starvation"
    BUFFER_BACKLOG = "buffer_backlog"
    VERSION_DRIFT = "version_drift"


_SCENARIO_PHASES: dict[SyntheticScenario, tuple[int, str]] = {
    SyntheticScenario.TOOL_WAIT: (execution_pb2.PHASE_KIND_TOOL_WAIT, "tool-wait"),
    SyntheticScenario.LONG_TAIL: (execution_pb2.PHASE_KIND_DECODE, "decode-long-tail"),
    SyntheticScenario.TRAINER_STARVATION: (
        execution_pb2.PHASE_KIND_IDLE,
        "trainer-starvation",
    ),
    SyntheticScenario.BUFFER_BACKLOG: (execution_pb2.PHASE_KIND_IDLE, "buffer-backlog"),
    SyntheticScenario.VERSION_DRIFT: (execution_pb2.PHASE_KIND_DECODE, "version-drift"),
}

_SCENARIO_DURATION_MS: dict[SyntheticScenario, int] = {
    SyntheticScenario.TOOL_WAIT: 750,
    SyntheticScenario.LONG_TAIL: 12_000,
    SyntheticScenario.TRAINER_STARVATION: 4_000,
    SyntheticScenario.BUFFER_BACKLOG: 6_000,
    SyntheticScenario.VERSION_DRIFT: 8_000,
}

_ROLLOUT_POLICY_WINDOWS: dict[int, int] = {
    trace_pb2.ROLLOUT_MODE_SYNC: 0,
    trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC: 1,
    trace_pb2.ROLLOUT_MODE_FULLY_ASYNC: 3,
}


class SyntheticWorkload:
    """Generate byte-stable Proto traces from an explicit local PRNG seed."""

    def __init__(
        self,
        seed: int,
        *,
        algorithm: str = "grpo",
        rollout_mode: int = trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        base_time: datetime = datetime(2025, 1, 1, tzinfo=UTC),
    ) -> None:
        if seed < 0:
            raise ValueError("seed must be non-negative")
        if not algorithm:
            raise ValueError("algorithm is required")
        if base_time.tzinfo is None:
            raise ValueError("base_time must be timezone-aware")
        if rollout_mode == trace_pb2.ROLLOUT_MODE_UNKNOWN:
            raise ValueError("rollout mode must not be UNKNOWN")
        self.seed = seed
        self.algorithm = algorithm
        self.rollout_mode = rollout_mode
        self.base_time = base_time.astimezone(UTC)
        stream_material = (
            f"{seed}\0{algorithm}\0{rollout_mode}\0{self.base_time.isoformat()}".encode()
        )
        self._stream_id = hashlib.sha256(stream_material).hexdigest()[:24]

    def generate(self, scenario: SyntheticScenario | str) -> list[trace_pb2.TraceEvent]:
        """Generate a start/end pair carrying reproducibility parameters."""
        scenario = SyntheticScenario(scenario)
        scenario_seed = int.from_bytes(
            hashlib.sha256(f"{self.seed}:{scenario.value}".encode()).digest()[:8], "big"
        )
        rng = random.Random(scenario_seed)
        offset_ms = rng.randrange(0, 250)
        duration_ms = _SCENARIO_DURATION_MS[scenario] + rng.randrange(0, 250)
        sequence_base = list(SyntheticScenario).index(scenario) * 10 + 1
        start = self.base_time + timedelta(milliseconds=offset_ms + sequence_base * 20_000)
        phase_kind, raw_label = _SCENARIO_PHASES[scenario]
        phase_id = f"{scenario.value}-phase"
        attributes = {
            "data_kind": "synthetic",
            "duration_ms": str(duration_ms),
            "gap_reason": scenario.value,
            "scenario": scenario.value,
            "seed": str(self.seed),
            "source_revision": str(sequence_base),
        }
        buffer_level = 96 if scenario is SyntheticScenario.BUFFER_BACKLOG else 0
        if scenario is SyntheticScenario.LONG_TAIL:
            attributes["waiting_for"] = "slow_rank"
        elif scenario is SyntheticScenario.TRAINER_STARVATION:
            attributes["waiting_for"] = "rollout_buffer"
        policy_window = _ROLLOUT_POLICY_WINDOWS[self.rollout_mode]

        def contract_observation(
            *,
            occurred_at: datetime,
            event_type: int,
            event_id: str,
        ) -> execution_pb2.ContractObservation:
            if scenario is SyntheticScenario.VERSION_DRIFT:
                policy_lag = policy_window + 1
                sample_stale = True
            else:
                policy_lag = policy_window
                sample_stale = False
            accepted_samples: int | None = None
            expected_samples: int | None = None
            sample_count: int | None = None
            effective_sample_size: float | None = None
            effective_sample_size_ratio: float | None = None
            if self.algorithm == "ppo":
                accepted_samples = 8
                sample_count = 8
            else:
                accepted_samples = 8
                expected_samples = 8
                sample_count = 8
            if scenario in {
                SyntheticScenario.TOOL_WAIT,
                SyntheticScenario.LONG_TAIL,
                SyntheticScenario.BUFFER_BACKLOG,
            }:
                effective_sample_size = 6.0
                effective_sample_size_ratio = 0.75
            typed_facts = [
                semantic_pb2.SemanticField(
                    key="sample.policy_lag",
                    value=semantic_pb2.SemanticValue(uint64_value=policy_lag),
                ),
                semantic_pb2.SemanticField(
                    key="sample.stale",
                    value=semantic_pb2.SemanticValue(bool_value=sample_stale),
                ),
                semantic_pb2.SemanticField(
                    key="buffer.level.current",
                    value=semantic_pb2.SemanticValue(uint64_value=buffer_level),
                ),
                semantic_pb2.SemanticField(
                    key="runtime.safe_point",
                    value=semantic_pb2.SemanticValue(
                        bool_value=event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED
                    ),
                ),
                semantic_pb2.SemanticField(
                    key="sample.sample_count",
                    value=semantic_pb2.SemanticValue(uint64_value=sample_count or 0),
                ),
            ]
            if self.algorithm == "ppo":
                typed_facts.append(
                    semantic_pb2.SemanticField(
                        key="batch.accepted_samples",
                        value=semantic_pb2.SemanticValue(uint64_value=accepted_samples or 0),
                    )
                )
            else:
                typed_facts.append(
                    semantic_pb2.SemanticField(
                        key="group.accepted_samples",
                        value=semantic_pb2.SemanticValue(uint64_value=accepted_samples or 0),
                    )
                )
            if expected_samples is not None:
                typed_facts.append(
                    semantic_pb2.SemanticField(
                        key="group.expected_samples",
                        value=semantic_pb2.SemanticValue(uint64_value=expected_samples),
                    )
                )
            if effective_sample_size is not None:
                typed_facts.append(
                    semantic_pb2.SemanticField(
                        key="batch.effective_sample_size",
                        value=semantic_pb2.SemanticValue(double_value=effective_sample_size),
                    )
                )
            if effective_sample_size_ratio is not None:
                typed_facts.append(
                    semantic_pb2.SemanticField(
                        key="batch.effective_sample_size_ratio",
                        value=semantic_pb2.SemanticValue(double_value=effective_sample_size_ratio),
                    )
                )
            observation = execution_pb2.ContractObservation(
                observed_at=timestamp_from_datetime(occurred_at),
                policy_lag=policy_lag,
                sample_stale=sample_stale,
                buffer_level=buffer_level,
                accepted_samples=accepted_samples or 0,
                safe_point=event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
                source="synthetic",
                event_id=event_id,
                phase_id=phase_id,
                policy_version="policy-1",
                typed_facts=typed_facts,
                sample_count=sample_count or 0,
            )
            if expected_samples is not None:
                observation.expected_samples = expected_samples
            if effective_sample_size is not None:
                observation.effective_sample_size = effective_sample_size
            if effective_sample_size_ratio is not None:
                observation.effective_sample_size_ratio = effective_sample_size_ratio
            return observation

        def event(event_type: int, sequence: int, occurred_at: datetime) -> trace_pb2.TraceEvent:
            event_material = f"{self.seed}:{scenario.value}:{event_type}:{sequence}".encode()
            event_material = b"\0".join(
                (self._stream_id.encode(), event_material, occurred_at.isoformat().encode())
            )
            event_id = hashlib.sha256(event_material).hexdigest()[:24]
            event_attributes = dict(attributes)
            event_attributes["source_revision"] = str(sequence)
            return trace_pb2.TraceEvent(
                event_id=f"evt-{event_id}",
                job_id=f"synthetic-job-{self._stream_id}",
                execution_id=f"synthetic-execution-{self._stream_id}",
                phase_id=phase_id,
                occurred_at=timestamp_from_datetime(occurred_at),
                event_type=event_type,
                algorithm=self.algorithm,
                rollout_mode=self.rollout_mode,
                policy_version="policy-1",
                buffer_level=buffer_level,
                safe_point=event_type == trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
                decision_id=f"synthetic-decision-{self._stream_id}",
                sequence=sequence,
                attributes=event_attributes,
                phase_kind=phase_kind,
                raw_phase_label=raw_label,
                sandbox_id=f"synthetic-sandbox-{scenario.value}",
                stage_id=phase_id,
                contract_observation=contract_observation(
                    occurred_at=occurred_at,
                    event_type=event_type,
                    event_id=f"evt-{event_id}",
                ),
            )

        return [
            event(trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED, sequence_base, start),
            event(
                trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
                sequence_base + 1,
                start + timedelta(milliseconds=duration_ms),
            ),
        ]

    def generate_batch(self, scenario: SyntheticScenario | str) -> trace_pb2.TraceEventBatch:
        events = self.generate(scenario)
        return trace_pb2.TraceEventBatch(
            execution_id=events[0].execution_id,
            first_sequence=events[0].sequence,
            events=events,
        )

    def generate_all(self) -> list[trace_pb2.TraceEvent]:
        return [event for scenario in SyntheticScenario for event in self.generate(scenario)]
