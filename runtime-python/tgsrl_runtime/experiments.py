"""Executable replay and experiment lifecycle for the Python runtime."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from tgsrl.v1 import experiment_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.aggregation import TraceAggregator, TraceSummary
from tgsrl_runtime.duration import to_timestamp
from tgsrl_runtime.proto_utils import clone_message
from tgsrl_runtime.replay import (
    REPLAY_DECISION_ARTIFACT_KIND,
    ReplayArtifactStep,
    ReplayScheduler,
    ReplayStepResult,
    SchedulerReplayRunner,
    SeedMode,
    decode_decision_artifacts,
    decode_replay_steps,
    encode_decision_artifact,
    encode_replay_step_artifact,
)
from tgsrl_runtime.trace_ingest import TraceIngestor

_REPLAY_COMMAND_KEY_PREFIX = "tgsrl.replay_command."


def _fallback_trace_summary(
    steps: tuple[ReplayArtifactStep, ...],
) -> TraceSummary:
    events = [step.event for step in steps]
    latest_event = events[-1] if events else None
    latest_observation = None
    if latest_event is not None and latest_event.HasField("contract_observation"):
        latest_observation = clone_message(latest_event.contract_observation)
    phase_counts: dict[str, int] = {}
    max_buffer_level = 0
    safe_point_count = 0
    for event in events:
        phase_counts[event.phase_id] = phase_counts.get(event.phase_id, 0) + 1
        max_buffer_level = max(max_buffer_level, event.buffer_level)
        safe_point_count += int(event.safe_point)
    return TraceSummary(
        event_count=len(events),
        safe_point_count=safe_point_count,
        policy_publish_count=sum(
            int(event.event_type == trace_pb2.TRACE_EVENT_TYPE_POLICY_PUBLISHED) for event in events
        ),
        phase_counts=phase_counts,
        max_buffer_level=max_buffer_level,
        latest_buffer_level=latest_observation.buffer_level if latest_observation else 0,
        latest_safe_point=latest_observation.safe_point if latest_observation else False,
        latest_policy_version=latest_observation.policy_version if latest_observation else "",
        latest_phase_id=latest_observation.phase_id if latest_observation else "",
        latest_event_id=latest_observation.event_id if latest_observation else "",
        latest_observation=latest_observation,
        micro_stage_count=0,
        completed_micro_stage_count=0,
        incomplete_micro_stage_count=0,
        total_micro_stage_seconds=0.0,
        micro_stages=(),
    )


@dataclass
class ReplayExperimentStore:
    """In-memory replay, experiment, and exact decision-sequence store."""

    _replays: dict[str, experiment_pb2.Replay] = field(default_factory=dict)
    _experiments: dict[str, experiment_pb2.Experiment] = field(default_factory=dict)
    _decision_sequences: dict[str, tuple[scheduling_pb2.DecisionRecord, ...]] = field(
        default_factory=dict
    )

    def put_replay(self, replay: experiment_pb2.Replay) -> experiment_pb2.Replay:
        stored = clone_message(replay)
        decisions = decode_decision_artifacts(stored)
        self._replays[stored.replay_id] = stored
        self._decision_sequences[stored.replay_id] = decisions
        return clone_message(stored)

    def get_replay(self, replay_id: str) -> experiment_pb2.Replay:
        return clone_message(self._replays[replay_id])

    def list_replays(self) -> list[experiment_pb2.Replay]:
        return [clone_message(self._replays[replay_id]) for replay_id in sorted(self._replays)]

    def put_experiment(self, experiment: experiment_pb2.Experiment) -> experiment_pb2.Experiment:
        stored = clone_message(experiment)
        self._experiments[stored.experiment_id] = stored
        return clone_message(stored)

    def get_experiment(self, experiment_id: str) -> experiment_pb2.Experiment:
        return clone_message(self._experiments[experiment_id])

    def list_experiments(self) -> list[experiment_pb2.Experiment]:
        return [
            clone_message(self._experiments[experiment_id])
            for experiment_id in sorted(self._experiments)
        ]

    def put_decisions(
        self, replay_id: str, decisions: Iterable[scheduling_pb2.DecisionRecord]
    ) -> tuple[scheduling_pb2.DecisionRecord, ...]:
        stored = tuple(clone_message(decision) for decision in decisions)
        self._decision_sequences[replay_id] = stored
        return tuple(clone_message(decision) for decision in stored)

    def get_decisions(self, replay_id: str) -> tuple[scheduling_pb2.DecisionRecord, ...]:
        return tuple(
            clone_message(decision) for decision in self._decision_sequences.get(replay_id, ())
        )


@dataclass
class ExperimentCoordinator:
    """Own replay execution and comparable experiment result construction."""

    trace_ingestor: TraceIngestor
    aggregator: TraceAggregator = field(default_factory=TraceAggregator)
    store: ReplayExperimentStore = field(default_factory=ReplayExperimentStore)
    _replay_locks: dict[str, asyncio.Lock] = field(default_factory=dict)

    def lock_for_replay(self, replay_id: str) -> asyncio.Lock:
        if not replay_id:
            raise ValueError("replay_id is required for replay mutation")
        return self._replay_locks.setdefault(replay_id, asyncio.Lock())

    @staticmethod
    def command_is_idempotent(
        replay: experiment_pb2.Replay, command: int, idempotency_key: str
    ) -> bool:
        if not idempotency_key:
            return False
        annotation = (
            _REPLAY_COMMAND_KEY_PREFIX + hashlib.sha256(idempotency_key.encode()).hexdigest()
        )
        recorded = replay.annotations.get(annotation)
        if recorded is not None and recorded != str(command):
            raise ValueError("replay idempotency_key was reused for another command")
        return recorded == str(command)

    def commit_command(
        self, replay: experiment_pb2.Replay, command: int, idempotency_key: str
    ) -> experiment_pb2.Replay:
        if not idempotency_key:
            raise ValueError("replay command idempotency_key is required")
        annotation = (
            _REPLAY_COMMAND_KEY_PREFIX + hashlib.sha256(idempotency_key.encode()).hexdigest()
        )
        recorded = replay.annotations.get(annotation)
        if recorded is not None and recorded != str(command):
            raise ValueError("replay idempotency_key was reused for another command")
        replay.annotations[annotation] = str(command)
        return self.store.put_replay(replay)

    def create_replay(
        self, replay: experiment_pb2.Replay, *, created_at: datetime
    ) -> experiment_pb2.Replay:
        mutable = clone_message(replay)
        if not mutable.replay_id or not mutable.run_id or not mutable.trace_id:
            raise ValueError("replay_id, run_id, and trace_id are required")
        if mutable.speed <= 0:
            raise ValueError("replay speed must be positive")
        if not mutable.HasField("created_at"):
            mutable.created_at.CopyFrom(to_timestamp(created_at))
        if mutable.state == experiment_pb2.REPLAY_STATE_UNKNOWN:
            mutable.state = experiment_pb2.REPLAY_STATE_PENDING
        if mutable.artifacts:
            decode_replay_steps(mutable)
        return self.store.put_replay(mutable)

    def attach_replay_steps(
        self, replay_id: str, steps: Iterable[ReplayArtifactStep]
    ) -> experiment_pb2.Replay:
        """Persist exact scheduler inputs as versioned artifacts on Replay."""
        replay = self.store.get_replay(replay_id)
        encoded = [encode_replay_step_artifact(replay_id, step) for step in steps]
        if not encoded:
            raise ValueError("at least one replay step is required")
        del replay.artifacts[:]
        replay.artifacts.extend(encoded)
        return self.store.put_replay(replay)

    def replay_steps(self, replay_id: str) -> tuple[ReplayArtifactStep, ...]:
        return decode_replay_steps(self.store.get_replay(replay_id))

    def _summarize_replay_steps(self, steps: tuple[ReplayArtifactStep, ...]) -> TraceSummary:
        try:
            return self.aggregator.summarize(step.event for step in steps)
        except TypeError as error:
            if "does not support assignment" not in str(error):
                raise
            return _fallback_trace_summary(steps)

    async def start_replay(
        self,
        replay_id: str,
        *,
        scheduler: ReplayScheduler,
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    ) -> experiment_pb2.Replay:
        """Resolve persisted artifacts and execute START against Scheduler."""
        replay = self.store.get_replay(replay_id)
        steps = decode_replay_steps(replay)
        experiment_id = replay.annotations.get("experiment_id") or f"experiment:{replay_id}"
        try:
            self.store.get_experiment(experiment_id)
        except KeyError:
            self.create_experiment(
                experiment_pb2.Experiment(
                    experiment_id=experiment_id,
                    display_name=f"Replay {replay_id}",
                ),
                created_at=now(),
            )
        await self.execute_replay(
            experiment_id=experiment_id,
            replay_id=replay_id,
            steps=steps,
            scheduler=scheduler,
            now=now,
            config_hash=replay.annotations.get("config_hash", ""),
            code_revision=replay.annotations.get("code_revision", ""),
        )
        return self.store.get_replay(replay_id)

    async def complete_replay(
        self,
        *,
        replay_id: str,
        steps: Iterable[ReplayArtifactStep],
        results: tuple[ReplayStepResult, ...],
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    ) -> experiment_pb2.Replay:
        """Materialize a replay whose external schedule steps are already durable."""
        replay = self.store.get_replay(replay_id)
        recorded_steps = tuple(steps)
        if len(results) != len(recorded_steps):
            raise ValueError("cannot complete replay with unfinished scheduler steps")
        experiment_id = replay.annotations.get("experiment_id") or f"experiment:{replay_id}"
        try:
            experiment = self.store.get_experiment(experiment_id)
        except KeyError:
            experiment = self.create_experiment(
                experiment_pb2.Experiment(
                    experiment_id=experiment_id, display_name=f"Replay {replay_id}"
                ),
                created_at=now(),
            )
        completed_at = now()
        replay.data_kind = trace_pb2.DATA_KIND_REPLAY
        replay.state = experiment_pb2.REPLAY_STATE_COMPLETED
        if not replay.HasField("started_at"):
            replay.started_at.CopyFrom(to_timestamp(completed_at))
        replay.completed_at.CopyFrom(to_timestamp(completed_at))
        replay.applied_events = len(results)
        replay.emitted_decisions = len(results)
        replay.cursor = f"replay:{replay_id}:{len(results)}"
        retained = [
            artifact
            for artifact in replay.artifacts
            if artifact.kind != REPLAY_DECISION_ARTIFACT_KIND
        ]
        retained.extend(
            encode_decision_artifact(replay_id, result, observed_at=completed_at)
            for result in results
        )
        del replay.artifacts[:]
        replay.artifacts.extend(retained)
        summary = self._summarize_replay_steps(recorded_steps)
        del replay.metrics[:]
        replay.metrics.extend(self.aggregator.to_metrics(summary))
        replay.metrics.extend(
            [
                experiment_pb2.MetricValue(
                    name="decision_count", value=float(len(results)), unit="count"
                ),
                experiment_pb2.MetricValue(
                    name="fallback_count",
                    value=float(sum(result.decision.fallback for result in results)),
                    unit="count",
                ),
                experiment_pb2.MetricValue(
                    name="equivalent_decision_count",
                    value=float(sum(result.comparison.equivalent for result in results)),
                    unit="count",
                ),
            ]
        )
        replay = self.store.put_replay(replay)
        self.store.put_decisions(replay_id, (result.decision for result in results))
        self._upsert_executed_run(
            experiment, replay, results, completed_at=completed_at, config_hash="", code_revision=""
        )
        self.store.put_experiment(experiment)
        return replay

    def apply_replay_command(
        self, replay_id: str, command: int, *, now: datetime
    ) -> experiment_pb2.Replay:
        """Legacy lifecycle command; START records readiness, execute_replay does work."""
        replay = self.store.get_replay(replay_id)
        if command == experiment_pb2.REPLAY_COMMAND_TYPE_START:
            replay.state = experiment_pb2.REPLAY_STATE_RUNNING
            replay.started_at.CopyFrom(to_timestamp(now))
        elif command == experiment_pb2.REPLAY_COMMAND_TYPE_PAUSE:
            replay.state = experiment_pb2.REPLAY_STATE_PAUSED
        elif command == experiment_pb2.REPLAY_COMMAND_TYPE_RESUME:
            replay.state = experiment_pb2.REPLAY_STATE_RUNNING
        elif command == experiment_pb2.REPLAY_COMMAND_TYPE_STOP:
            replay.state = experiment_pb2.REPLAY_STATE_COMPLETED
            replay.completed_at.CopyFrom(to_timestamp(now))
        elif command == experiment_pb2.REPLAY_COMMAND_TYPE_TERMINATE:
            replay.state = experiment_pb2.REPLAY_STATE_CANCELLED
            replay.completed_at.CopyFrom(to_timestamp(now))
        else:
            raise ValueError("unknown replay command")
        events = self.trace_ingestor.list_causal(replay.run_id)
        summary = self.aggregator.summarize(events)
        replay.applied_events = summary.event_count
        del replay.metrics[:]
        replay.metrics.extend(self.aggregator.to_metrics(summary))
        return self.store.put_replay(replay)

    async def execute_replay(
        self,
        *,
        experiment_id: str,
        replay_id: str,
        steps: Iterable[ReplayArtifactStep],
        scheduler: ReplayScheduler,
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
        seed_mode: SeedMode = SeedMode.RECORDED,
        config_hash: str = "",
        code_revision: str = "",
    ) -> experiment_pb2.Experiment:
        """Execute exact recorded scheduler inputs and persist comparable decisions."""
        experiment = self.store.get_experiment(experiment_id)
        replay = self.store.get_replay(replay_id)
        recorded_steps = tuple(steps)
        if not recorded_steps:
            raise ValueError("replay execution requires recorded artifact steps")
        if replay.state in {
            experiment_pb2.REPLAY_STATE_CANCELLED,
            experiment_pb2.REPLAY_STATE_COMPLETED,
        }:
            raise ValueError("terminal replay cannot be executed")
        started_at = now()
        replay.data_kind = trace_pb2.DATA_KIND_REPLAY
        replay.state = experiment_pb2.REPLAY_STATE_RUNNING
        replay.started_at.CopyFrom(to_timestamp(started_at))
        experiment.state = experiment_pb2.EXPERIMENT_STATE_RUNNING
        self.store.put_replay(replay)
        self.store.put_experiment(experiment)
        runner = SchedulerReplayRunner(
            recorded_steps, scheduler=scheduler, seed=replay.seed, seed_mode=seed_mode
        )
        try:
            results = await runner.run()
        except Exception as error:
            completed_at = now()
            replay.state = experiment_pb2.REPLAY_STATE_FAILED
            replay.completed_at.CopyFrom(to_timestamp(completed_at))
            replay.applied_events = runner.checkpoint().index
            replay.emitted_decisions = len(runner.results)
            replay.annotations["error"] = f"{type(error).__name__}: {error}"
            experiment.state = experiment_pb2.EXPERIMENT_STATE_FAILED
            experiment.completed_at.CopyFrom(to_timestamp(completed_at))
            experiment.annotations["error"] = replay.annotations["error"]
            self.store.put_replay(replay)
            self.store.put_experiment(experiment)
            raise

        completed_at = now()
        decisions = tuple(result.decision for result in results)
        self.store.put_decisions(replay_id, decisions)
        summary = self._summarize_replay_steps(recorded_steps)
        equivalent_count = sum(result.comparison.equivalent for result in results)
        fallback_count = sum(result.decision.fallback for result in results)
        replay.state = experiment_pb2.REPLAY_STATE_COMPLETED
        replay.completed_at.CopyFrom(to_timestamp(completed_at))
        replay.applied_events = len(results)
        replay.emitted_decisions = len(decisions)
        replay.cursor = f"replay:{replay_id}:{len(results)}"
        retained_artifacts = [
            artifact
            for artifact in replay.artifacts
            if artifact.kind != REPLAY_DECISION_ARTIFACT_KIND
        ]
        retained_artifacts.extend(
            encode_decision_artifact(replay_id, result, observed_at=completed_at)
            for result in results
        )
        del replay.artifacts[:]
        replay.artifacts.extend(retained_artifacts)
        del replay.metrics[:]
        replay.metrics.extend(self.aggregator.to_metrics(summary))
        replay.metrics.extend(
            [
                experiment_pb2.MetricValue(
                    name="decision_count", value=float(len(decisions)), unit="count"
                ),
                experiment_pb2.MetricValue(
                    name="fallback_count", value=float(fallback_count), unit="count"
                ),
                experiment_pb2.MetricValue(
                    name="equivalent_decision_count",
                    value=float(equivalent_count),
                    unit="count",
                ),
            ]
        )
        self.store.put_replay(replay)
        self._upsert_executed_run(
            experiment,
            replay,
            results,
            completed_at=completed_at,
            config_hash=config_hash,
            code_revision=code_revision,
        )
        return self.store.put_experiment(experiment)

    def create_experiment(
        self, experiment: experiment_pb2.Experiment, *, created_at: datetime
    ) -> experiment_pb2.Experiment:
        mutable = clone_message(experiment)
        if not mutable.experiment_id:
            raise ValueError("experiment_id is required")
        if not mutable.HasField("created_at"):
            mutable.created_at.CopyFrom(to_timestamp(created_at))
        if mutable.state == experiment_pb2.EXPERIMENT_STATE_UNKNOWN:
            mutable.state = experiment_pb2.EXPERIMENT_STATE_PENDING
        return self.store.put_experiment(mutable)

    def reconcile_run(
        self,
        *,
        experiment_id: str,
        run_id: str,
        trace_id: str,
        kind: int,
        data_kind: int,
        policy_version: str,
        config_hash: str,
        code_revision: str,
    ) -> experiment_pb2.Experiment:
        """Backward-compatible trace-only aggregation for non-replay runs."""
        experiment = self.store.get_experiment(experiment_id)
        summary = self.aggregator.summarize(self.trace_ingestor.list_causal(run_id))
        metrics = self.aggregator.to_metrics(summary)
        run = experiment_pb2.ExperimentRun(
            experiment_run_id=f"{experiment_id}:{run_id}:{kind}",
            experiment_id=experiment_id,
            run_id=run_id,
            trace_id=trace_id,
            kind=kind,
            data_kind=data_kind,
            config_hash=config_hash,
            code_revision=code_revision,
            policy_version=policy_version,
            metrics=metrics,
        )
        result = experiment_pb2.ExperimentResult(
            result_id=f"result:{experiment_id}:{run_id}",
            experiment_id=experiment_id,
            observed_at=to_timestamp(datetime.now(tz=UTC)),
            summary=f"events={summary.event_count} safe_points={summary.safe_point_count}",
            metrics=metrics,
            trace_id=trace_id,
            data_kind=data_kind,
        )
        self._replace_run_and_result(experiment, run, result)
        experiment.state = experiment_pb2.EXPERIMENT_STATE_COMPLETED
        experiment.completed_at.CopyFrom(result.observed_at)
        experiment.summary = result.summary
        experiment.cursor = (
            f"experiment:{experiment_id}:{len(experiment.runs)}:{len(experiment.results)}"
        )
        run.result_cursor = experiment.cursor
        result.annotations["cursor"] = experiment.cursor
        return self.store.put_experiment(experiment)

    def _upsert_executed_run(
        self,
        experiment: experiment_pb2.Experiment,
        replay: experiment_pb2.Replay,
        results: tuple[ReplayStepResult, ...],
        *,
        completed_at: datetime,
        config_hash: str,
        code_revision: str,
    ) -> None:
        metrics = list(replay.metrics)
        run = experiment_pb2.ExperimentRun(
            experiment_run_id=f"{experiment.experiment_id}:{replay.replay_id}",
            experiment_id=experiment.experiment_id,
            run_id=replay.run_id,
            trace_id=replay.trace_id,
            kind=experiment_pb2.EXPERIMENT_RUN_KIND_REPLAY,
            data_kind=trace_pb2.DATA_KIND_REPLAY,
            config_hash=config_hash,
            code_revision=code_revision,
            policy_version=results[-1].decision.policy_version,
            replay_id=replay.replay_id,
            metrics=metrics,
        )
        result = experiment_pb2.ExperimentResult(
            result_id=f"result:{experiment.experiment_id}:{replay.replay_id}",
            experiment_id=experiment.experiment_id,
            observed_at=to_timestamp(completed_at),
            summary=(
                f"events={replay.applied_events} decisions={replay.emitted_decisions} "
                f"fallbacks={sum(item.decision.fallback for item in results)}"
            ),
            metrics=metrics,
            trace_id=replay.trace_id,
            data_kind=trace_pb2.DATA_KIND_REPLAY,
        )
        run.result_id = result.result_id
        run.replay_state = replay.state
        self._replace_run_and_result(experiment, run, result)
        experiment.state = experiment_pb2.EXPERIMENT_STATE_COMPLETED
        experiment.completed_at.CopyFrom(to_timestamp(completed_at))
        experiment.summary = result.summary
        experiment.cursor = (
            f"experiment:{experiment.experiment_id}:{len(experiment.runs)}:"
            f"{len(experiment.results)}"
        )
        run.result_cursor = experiment.cursor
        result.annotations["cursor"] = experiment.cursor
        self._replace_run_and_result(experiment, run, result)

    @staticmethod
    def _replace_run_and_result(
        experiment: experiment_pb2.Experiment,
        run: experiment_pb2.ExperimentRun,
        result: experiment_pb2.ExperimentResult,
    ) -> None:
        retained_runs = [
            item for item in experiment.runs if item.experiment_run_id != run.experiment_run_id
        ]
        del experiment.runs[:]
        experiment.runs.extend([*retained_runs, run])
        retained_results = [
            item for item in experiment.results if item.result_id != result.result_id
        ]
        del experiment.results[:]
        experiment.results.extend([*retained_results, result])
