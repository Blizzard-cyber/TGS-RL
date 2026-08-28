"""Gateway-facing repository wrapper over the sqlite store."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tgsrl.v1 import experiment_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.storage.types import AdapterHydrationState, Page, ReplayScheduleStep

if TYPE_CHECKING:
    from tgsrl_runtime.storage.sqlite_store import SQLiteStore


class GatewayRepository:
    """Gateway-facing read/write surface over durable control-plane entities."""

    def __init__(self, store: SQLiteStore) -> None:
        self._store = store

    def record_intent(
        self, intent: scheduling_pb2.SchedulingIntent
    ) -> scheduling_pb2.SchedulingIntent:
        return self._store.record_intent_cas(intent)

    def append_trace_batch(self, batch: trace_pb2.TraceEventBatch) -> trace_pb2.TraceEventBatch:
        return self._store.append_trace_batch(batch)

    def list_trace_events(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> Page[trace_pb2.TraceEvent]:
        return self._store.list_trace_events(
            run_id=run_id,
            limit=limit,
            after_cursor=after_cursor,
        )

    def upsert_replay(self, replay: experiment_pb2.Replay) -> experiment_pb2.Replay:
        return self._store.upsert_replay(replay)

    def record_replay_schedule_step(self, step: ReplayScheduleStep) -> ReplayScheduleStep:
        return self._store.record_replay_schedule_step(step)

    def list_replay_schedule_steps(
        self, *, replay_id: str, start_key_digest: str
    ) -> list[ReplayScheduleStep]:
        return self._store.list_replay_schedule_steps(
            replay_id=replay_id, start_key_digest=start_key_digest
        )

    def list_replays(self, *, limit: int, after_cursor: str = "") -> Page[experiment_pb2.Replay]:
        return self._store.list_replays(limit=limit, after_cursor=after_cursor)

    def upsert_experiment(self, experiment: experiment_pb2.Experiment) -> experiment_pb2.Experiment:
        return self._store.upsert_experiment(experiment)

    def list_experiments(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[experiment_pb2.Experiment]:
        return self._store.list_experiments(limit=limit, after_cursor=after_cursor)

    def list_latest_intents(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[scheduling_pb2.SchedulingIntent]:
        return self._store.list_latest_intents(limit=limit, after_cursor=after_cursor)

    def list_trace_batches(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[trace_pb2.TraceEventBatch]:
        return self._store.list_trace_batches(limit=limit, after_cursor=after_cursor)

    def hydrate_adapter_state(self, *, limit_per_kind: int = 10_000) -> AdapterHydrationState:
        return self._store.hydrate_adapter_state(limit_per_kind=limit_per_kind)
