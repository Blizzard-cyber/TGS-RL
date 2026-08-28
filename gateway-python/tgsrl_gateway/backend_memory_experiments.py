"""Replay and experiment fixture state for the in-memory gateway backend."""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

from tgsrl.v1 import experiment_pb2, trace_pb2

from tgsrl_gateway.errors import ConflictError, NotFoundError
from tgsrl_gateway.pagination import paginate
from tgsrl_gateway.protojson import clone_message, timestamp_from_datetime


@dataclass(slots=True)
class MemoryExperimentState:
    replay_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    experiment_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    replays: dict[str, experiment_pb2.Replay] = field(default_factory=dict)
    experiments: dict[str, experiment_pb2.Experiment] = field(default_factory=dict)

    def next_replay_id(self) -> str:
        return f"replay-{next(self.replay_counter):04d}"

    def next_experiment_id(self) -> str:
        return f"experiment-{next(self.experiment_counter):04d}"

    def list_replays(self, *, limit: int | None, page_token: str | None) -> dict[str, object]:
        replays = [clone_message(self.replays[replay_id]) for replay_id in sorted(self.replays)]
        items, next_token = paginate(
            replays,
            page_token=page_token,
            limit=limit,
            scope="replays",
        )
        return {"replays": items, "next_page_token": next_token}

    def create_replay(self, replay: experiment_pb2.Replay, *, request_id: str) -> dict[str, object]:
        normalized = clone_message(replay)
        if not normalized.replay_id:
            normalized.replay_id = self.next_replay_id()
        if normalized.replay_id in self.replays:
            raise ConflictError(
                "replay already exists",
                details={"replay_id": normalized.replay_id},
            )
        if normalized.state == experiment_pb2.REPLAY_STATE_UNKNOWN:
            normalized.state = experiment_pb2.REPLAY_STATE_PENDING
        if not normalized.HasField("created_at"):
            normalized.created_at.CopyFrom(timestamp_from_datetime())
        if normalized.data_kind == trace_pb2.DATA_KIND_UNKNOWN:
            normalized.data_kind = trace_pb2.DATA_KIND_REPLAY
        self.replays[normalized.replay_id] = normalized
        return {"replay": clone_message(normalized), "request_id": request_id}

    def get_replay(self, replay_id: str) -> experiment_pb2.Replay:
        replay = self.replays.get(replay_id)
        if replay is None:
            raise NotFoundError("replay", replay_id)
        return clone_message(replay)

    def apply_replay_command(
        self,
        replay_id: str,
        command: int,
        *,
        request_id: str,
    ) -> dict[str, object]:
        replay = self.replays.get(replay_id)
        if replay is None:
            raise NotFoundError("replay", replay_id)
        mapping = {
            experiment_pb2.REPLAY_COMMAND_TYPE_START: experiment_pb2.REPLAY_STATE_RUNNING,
            experiment_pb2.REPLAY_COMMAND_TYPE_PAUSE: experiment_pb2.REPLAY_STATE_PAUSED,
            experiment_pb2.REPLAY_COMMAND_TYPE_RESUME: experiment_pb2.REPLAY_STATE_RUNNING,
            experiment_pb2.REPLAY_COMMAND_TYPE_STOP: experiment_pb2.REPLAY_STATE_CANCELLED,
            experiment_pb2.REPLAY_COMMAND_TYPE_TERMINATE: experiment_pb2.REPLAY_STATE_CANCELLED,
        }
        replay.state = mapping[command]
        if command == experiment_pb2.REPLAY_COMMAND_TYPE_START and not replay.HasField(
            "started_at"
        ):
            replay.started_at.CopyFrom(timestamp_from_datetime())
        if command in {
            experiment_pb2.REPLAY_COMMAND_TYPE_STOP,
            experiment_pb2.REPLAY_COMMAND_TYPE_TERMINATE,
        }:
            replay.completed_at.CopyFrom(timestamp_from_datetime())
        return {"replay": clone_message(replay), "request_id": request_id}

    def list_experiments(self, *, limit: int | None, page_token: str | None) -> dict[str, object]:
        experiments = [
            clone_message(self.experiments[experiment_id])
            for experiment_id in sorted(self.experiments)
        ]
        items, next_token = paginate(
            experiments,
            page_token=page_token,
            limit=limit,
            scope="experiments",
        )
        return {"experiments": items, "next_page_token": next_token}

    def create_experiment(
        self,
        experiment: experiment_pb2.Experiment,
        *,
        request_id: str,
    ) -> dict[str, object]:
        normalized = clone_message(experiment)
        if not normalized.experiment_id:
            normalized.experiment_id = self.next_experiment_id()
        if normalized.experiment_id in self.experiments:
            raise ConflictError(
                "experiment already exists",
                details={"experiment_id": normalized.experiment_id},
            )
        if normalized.state == experiment_pb2.EXPERIMENT_STATE_UNKNOWN:
            normalized.state = experiment_pb2.EXPERIMENT_STATE_PENDING
        if not normalized.HasField("created_at"):
            normalized.created_at.CopyFrom(timestamp_from_datetime())
        self.experiments[normalized.experiment_id] = normalized
        return {"experiment": clone_message(normalized), "request_id": request_id}

    def get_experiment(self, experiment_id: str) -> experiment_pb2.Experiment:
        experiment = self.experiments.get(experiment_id)
        if experiment is None:
            raise NotFoundError("experiment", experiment_id)
        return clone_message(experiment)


@dataclass(slots=True)
class MemoryExperimentService:
    state: MemoryExperimentState

    def seed(
        self,
        *,
        replay: experiment_pb2.Replay,
        experiment: experiment_pb2.Experiment,
    ) -> None:
        self.state.replays[replay.replay_id] = replay
        self.state.experiments[experiment.experiment_id] = experiment

    def list_replays(self, *, limit: int | None, page_token: str | None) -> dict[str, object]:
        return self.state.list_replays(limit=limit, page_token=page_token)

    def create_replay(self, replay: experiment_pb2.Replay, *, request_id: str) -> dict[str, object]:
        return self.state.create_replay(replay, request_id=request_id)

    def get_replay(self, replay_id: str) -> experiment_pb2.Replay:
        return self.state.get_replay(replay_id)

    def apply_replay_command(
        self,
        replay_id: str,
        command: int,
        *,
        request_id: str,
    ) -> dict[str, object]:
        return self.state.apply_replay_command(
            replay_id,
            command,
            request_id=request_id,
        )

    def list_experiments(self, *, limit: int | None, page_token: str | None) -> dict[str, object]:
        return self.state.list_experiments(limit=limit, page_token=page_token)

    def create_experiment(
        self,
        experiment: experiment_pb2.Experiment,
        *,
        request_id: str,
    ) -> dict[str, object]:
        return self.state.create_experiment(experiment, request_id=request_id)

    def get_experiment(self, experiment_id: str) -> experiment_pb2.Experiment:
        return self.state.get_experiment(experiment_id)

    def replay_count(self) -> int:
        return len(self.state.replays)

    def experiment_count(self) -> int:
        return len(self.state.experiments)
