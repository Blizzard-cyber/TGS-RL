"""Gateway-facing sqlite storage methods for intents, traces, and experiments."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC
from typing import cast

from google.protobuf.message import Message
from tgsrl.v1 import experiment_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.storage.store_contracts import SQLiteStoreHelpers
from tgsrl_runtime.storage.types import (
    AdapterHydrationState,
    IntentVersionConflict,
    Page,
    ReplayScheduleStep,
    StorageCorruptionError,
)


class SQLiteGatewayStoreMixin:
    """Gateway-side persistence methods mixed into the sqlite compatibility facade."""

    def _collect_pages[T: Message](
        self,
        fetch_page: Callable[[str], Page[T]],
    ) -> list[T]:
        items: list[T] = []
        cursor = ""
        while True:
            page = fetch_page(cursor)
            items.extend(page.items)
            if not page.next_cursor:
                return items
            cursor = page.next_cursor

    def _decode_replay_schedule_step(
        self,
        *,
        replay_id: str,
        start_key_digest: str,
        ordinal: int,
        step_digest: str,
        decision_payload: bytes,
        decision_sha256: str,
        completed_at: str,
    ) -> ReplayScheduleStep:
        store = cast(SQLiteStoreHelpers, self)
        actual_digest = hashlib.sha256(decision_payload).hexdigest()
        if actual_digest != decision_sha256:
            raise StorageCorruptionError(
                f"replay schedule step {replay_id}/{ordinal} has digest mismatch"
            )
        return ReplayScheduleStep(
            replay_id=replay_id,
            start_key_digest=start_key_digest,
            ordinal=ordinal,
            step_digest=step_digest,
            decision=store._decode_message(
                scheduling_pb2.DecisionRecord,
                decision_payload,
                f"replay schedule step {replay_id}/{ordinal}",
            ),
            completed_at=store._parse_stamp(completed_at),
        )

    def record_replay_schedule_step(self, step: ReplayScheduleStep) -> ReplayScheduleStep:
        store = cast(SQLiteStoreHelpers, self)
        payload, digest = store._marshal(step.decision)
        completed_at = store._stamp(step.completed_at)
        with store._connection:
            existing = store._connection.execute(
                """
                SELECT step_digest, decision_payload, decision_sha256, completed_at
                FROM replay_schedule_steps
                WHERE replay_id = ? AND start_key_digest = ? AND ordinal = ?
                """,
                (step.replay_id, step.start_key_digest, step.ordinal),
            ).fetchone()
            if existing is not None:
                persisted = self._decode_replay_schedule_step(
                    replay_id=step.replay_id,
                    start_key_digest=step.start_key_digest,
                    ordinal=step.ordinal,
                    step_digest=str(existing["step_digest"]),
                    decision_payload=bytes(existing["decision_payload"]),
                    decision_sha256=str(existing["decision_sha256"]),
                    completed_at=str(existing["completed_at"]),
                )
                if (
                    persisted.step_digest != step.step_digest
                    or hashlib.sha256(bytes(existing["decision_payload"])).hexdigest() != digest
                ):
                    raise IntentVersionConflict(
                        "replay schedule step already completed with different payload"
                    )
                return persisted
            store._connection.execute(
                """
                INSERT INTO replay_schedule_steps(
                    replay_id, start_key_digest, ordinal, step_digest,
                    decision_payload, decision_sha256, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    step.replay_id,
                    step.start_key_digest,
                    step.ordinal,
                    step.step_digest,
                    payload,
                    digest,
                    completed_at,
                ),
            )
        return step

    def list_replay_schedule_steps(
        self, *, replay_id: str, start_key_digest: str
    ) -> list[ReplayScheduleStep]:
        store = cast(SQLiteStoreHelpers, self)
        rows = store._connection.execute(
            """
            SELECT ordinal, step_digest, decision_payload, decision_sha256, completed_at
            FROM replay_schedule_steps
            WHERE replay_id = ? AND start_key_digest = ?
            ORDER BY ordinal ASC
            """,
            (replay_id, start_key_digest),
        )
        return [
            self._decode_replay_schedule_step(
                replay_id=replay_id,
                start_key_digest=start_key_digest,
                ordinal=int(row["ordinal"]),
                step_digest=str(row["step_digest"]),
                decision_payload=bytes(row["decision_payload"]),
                decision_sha256=str(row["decision_sha256"]),
                completed_at=str(row["completed_at"]),
            )
            for row in rows
        ]

    def record_intent_cas(
        self, intent: scheduling_pb2.SchedulingIntent
    ) -> scheduling_pb2.SchedulingIntent:
        store = cast(SQLiteStoreHelpers, self)
        payload, digest = store._marshal(intent)
        created_at = store._stamp(intent.submitted_at.ToDatetime(tzinfo=UTC))
        with store._connection:
            existing = store._connection.execute(
                """
                SELECT version, payload, payload_sha256
                FROM intents
                WHERE execution_id = ? AND stage_id = ?
                ORDER BY version DESC
                LIMIT 1
                """,
                (intent.execution_id, intent.stage_id),
            ).fetchone()
            if existing is not None and existing["version"] >= intent.version:
                if existing["version"] == intent.version:
                    if existing["payload_sha256"] == digest:
                        return store._decode_message(
                            scheduling_pb2.SchedulingIntent,
                            existing["payload"],
                            (f"intent {intent.execution_id}/{intent.stage_id}/{intent.version}"),
                        )
                    raise IntentVersionConflict(
                        "intent version already exists with different payload"
                    )
                raise IntentVersionConflict("intent version must advance monotonically")
            same_key = store._connection.execute(
                "SELECT payload_sha256 FROM intents WHERE idempotency_key = ?",
                (intent.idempotency_key,),
            ).fetchone()
            if same_key is not None and same_key["payload_sha256"] != digest:
                raise IntentVersionConflict("idempotency key already used by a different intent")
            store._connection.execute(
                """
                INSERT INTO intents(
                    execution_id, stage_id, version, job_id, run_id, trace_id, idempotency_key,
                    payload, payload_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.execution_id,
                    intent.stage_id,
                    intent.version,
                    intent.job_id,
                    intent.run_id,
                    intent.trace_id,
                    intent.idempotency_key,
                    payload,
                    digest,
                    created_at,
                ),
            )
        return self.get_latest_intent(intent.execution_id, intent.stage_id)

    def get_latest_intent(
        self, execution_id: str, stage_id: str
    ) -> scheduling_pb2.SchedulingIntent:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            """
            SELECT payload
            FROM intents
            WHERE execution_id = ? AND stage_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            (execution_id, stage_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"intent not found for {execution_id}/{stage_id}")
        return store._decode_message(
            scheduling_pb2.SchedulingIntent,
            row["payload"],
            f"intent {execution_id}/{stage_id}",
        )

    def list_latest_intents(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[scheduling_pb2.SchedulingIntent]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        cursor_execution = ""
        cursor_stage = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            cursor_execution = str(decoded[0])
            cursor_stage = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT i.execution_id, i.stage_id, i.payload
                FROM intents AS i
                INNER JOIN (
                    SELECT execution_id, stage_id, MAX(version) AS latest_version
                    FROM intents
                    GROUP BY execution_id, stage_id
                ) AS latest
                    ON latest.execution_id = i.execution_id
                   AND latest.stage_id = i.stage_id
                   AND latest.latest_version = i.version
                WHERE (
                    i.execution_id > ?
                    OR (i.execution_id = ? AND i.stage_id > ?)
                )
                ORDER BY i.execution_id ASC, i.stage_id ASC
                LIMIT ?
                """,
                (cursor_execution, cursor_execution, cursor_stage, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["execution_id"], tail["stage_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                scheduling_pb2.SchedulingIntent,
                row["payload"],
                f"intent {row['execution_id']}/{row['stage_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def append_trace_batch(self, batch: trace_pb2.TraceEventBatch) -> trace_pb2.TraceEventBatch:
        store = cast(SQLiteStoreHelpers, self)
        created_at = store._stamp()
        with store._connection:
            for event in batch.events:
                payload, digest = store._marshal(event)
                existing = store._connection.execute(
                    "SELECT payload_sha256 FROM trace_events WHERE event_id = ?",
                    (event.event_id,),
                ).fetchone()
                if existing is not None:
                    if existing["payload_sha256"] != digest:
                        raise IntentVersionConflict(
                            f"trace event {event.event_id} already exists with different payload"
                        )
                    continue
                occurred_at = store._stamp(event.occurred_at.ToDatetime(tzinfo=UTC))
                store._connection.execute(
                    """
                    INSERT INTO trace_events(
                        event_id, execution_id, run_id, trace_id, job_id, sequence,
                        occurred_at, payload, payload_sha256, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.execution_id,
                        event.run_id,
                        event.trace_id,
                        event.job_id,
                        event.sequence,
                        occurred_at,
                        payload,
                        digest,
                        created_at,
                    ),
                )
        return self.get_trace_batch(batch.execution_id, batch.run_id)

    def get_trace_batch(self, execution_id: str, run_id: str) -> trace_pb2.TraceEventBatch:
        store = cast(SQLiteStoreHelpers, self)
        rows = list(
            store._connection.execute(
                """
                SELECT payload
                FROM trace_events
                WHERE execution_id = ? AND run_id = ?
                ORDER BY sequence ASC, event_id ASC
                """,
                (execution_id, run_id),
            )
        )
        if not rows:
            raise KeyError(f"trace batch not found for {execution_id}/{run_id}")
        batch = trace_pb2.TraceEventBatch(execution_id=execution_id)
        for index, row in enumerate(rows):
            event = store._decode_message(
                trace_pb2.TraceEvent,
                row["payload"],
                f"trace event #{index}",
            )
            batch.events.append(event)
        batch.first_sequence = batch.events[0].sequence
        batch.run_id = batch.events[0].run_id
        batch.trace_id = batch.events[0].trace_id
        if batch.events[0].HasField("semantic_context"):
            batch.semantic_context.CopyFrom(batch.events[0].semantic_context)
        batch.data_kind = batch.events[0].data_kind
        return batch

    def list_trace_batches(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[trace_pb2.TraceEventBatch]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        cursor_execution = ""
        cursor_run = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            cursor_execution = str(decoded[0])
            cursor_run = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT execution_id, run_id
                FROM trace_events
                GROUP BY execution_id, run_id
                HAVING (
                    execution_id > ?
                    OR (execution_id = ? AND run_id > ?)
                )
                ORDER BY execution_id ASC, run_id ASC
                LIMIT ?
                """,
                (cursor_execution, cursor_execution, cursor_run, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["execution_id"], tail["run_id"])
            rows = rows[:limit]
        items = [
            self.get_trace_batch(
                execution_id=str(row["execution_id"]),
                run_id=str(row["run_id"]),
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def list_trace_events(
        self,
        *,
        run_id: str,
        limit: int,
        after_cursor: str = "",
    ) -> Page[trace_pb2.TraceEvent]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        sequence = -1
        event_id = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            sequence = int(str(decoded[0]))
            event_id = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT sequence, event_id, payload
                FROM trace_events
                WHERE run_id = ?
                  AND (sequence > ? OR (sequence = ? AND event_id > ?))
                ORDER BY sequence ASC, event_id ASC
                LIMIT ?
                """,
                (run_id, sequence, sequence, event_id, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["sequence"], tail["event_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                trace_pb2.TraceEvent,
                row["payload"],
                f"trace event {row['event_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def upsert_replay(self, replay: experiment_pb2.Replay) -> experiment_pb2.Replay:
        store = cast(SQLiteStoreHelpers, self)
        payload, digest = store._marshal(replay)
        now = store._stamp()
        with store._connection:
            existing = store._connection.execute(
                "SELECT replay_seq FROM replays WHERE replay_id = ?",
                (replay.replay_id,),
            ).fetchone()
            if existing is None:
                store._connection.execute(
                    """
                    INSERT INTO replays(
                        replay_id,
                        run_id,
                        trace_id,
                        state,
                        payload,
                        payload_sha256,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        replay.replay_id,
                        replay.run_id,
                        replay.trace_id,
                        replay.state,
                        payload,
                        digest,
                        now,
                        now,
                    ),
                )
            else:
                store._connection.execute(
                    """
                    UPDATE replays
                    SET run_id = ?,
                        trace_id = ?,
                        state = ?,
                        payload = ?,
                        payload_sha256 = ?,
                        updated_at = ?
                    WHERE replay_id = ?
                    """,
                    (
                        replay.run_id,
                        replay.trace_id,
                        replay.state,
                        payload,
                        digest,
                        now,
                        replay.replay_id,
                    ),
                )
        return self.get_replay(replay.replay_id)

    def get_replay(self, replay_id: str) -> experiment_pb2.Replay:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            "SELECT payload FROM replays WHERE replay_id = ?",
            (replay_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"replay {replay_id} not found")
        return store._decode_message(
            experiment_pb2.Replay,
            row["payload"],
            f"replay {replay_id}",
        )

    def list_replays(self, *, limit: int, after_cursor: str = "") -> Page[experiment_pb2.Replay]:
        store = cast(SQLiteStoreHelpers, self)
        return store._page_messages(
            table="replays",
            id_column="replay_id",
            seq_column="replay_seq",
            message_type=experiment_pb2.Replay,
            limit=limit,
            after_cursor=after_cursor,
        )

    def upsert_experiment(self, experiment: experiment_pb2.Experiment) -> experiment_pb2.Experiment:
        store = cast(SQLiteStoreHelpers, self)
        payload, digest = store._marshal(experiment)
        now = store._stamp()
        with store._connection:
            existing = store._connection.execute(
                "SELECT experiment_seq FROM experiments WHERE experiment_id = ?",
                (experiment.experiment_id,),
            ).fetchone()
            if existing is None:
                store._connection.execute(
                    """
                    INSERT INTO experiments(
                        experiment_id,
                        display_name,
                        state,
                        payload,
                        payload_sha256,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        experiment.experiment_id,
                        experiment.display_name,
                        experiment.state,
                        payload,
                        digest,
                        now,
                        now,
                    ),
                )
            else:
                store._connection.execute(
                    """
                    UPDATE experiments
                    SET display_name = ?,
                        state = ?,
                        payload = ?,
                        payload_sha256 = ?,
                        updated_at = ?
                    WHERE experiment_id = ?
                    """,
                    (
                        experiment.display_name,
                        experiment.state,
                        payload,
                        digest,
                        now,
                        experiment.experiment_id,
                    ),
                )
        return self.get_experiment(experiment.experiment_id)

    def get_experiment(self, experiment_id: str) -> experiment_pb2.Experiment:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            "SELECT payload FROM experiments WHERE experiment_id = ?",
            (experiment_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"experiment {experiment_id} not found")
        return store._decode_message(
            experiment_pb2.Experiment,
            row["payload"],
            f"experiment {experiment_id}",
        )

    def list_experiments(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[experiment_pb2.Experiment]:
        store = cast(SQLiteStoreHelpers, self)
        return store._page_messages(
            table="experiments",
            id_column="experiment_id",
            seq_column="experiment_seq",
            message_type=experiment_pb2.Experiment,
            limit=limit,
            after_cursor=after_cursor,
        )

    def hydrate_adapter_state(self, *, limit_per_kind: int = 10_000) -> AdapterHydrationState:
        store = cast(SQLiteStoreHelpers, self)
        if limit_per_kind <= 0:
            raise ValueError("limit_per_kind must be positive")
        return AdapterHydrationState(
            intents=self._collect_pages(
                lambda after_cursor: self.list_latest_intents(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
            trace_batches=self._collect_pages(
                lambda after_cursor: self.list_trace_batches(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
            manifests=self._collect_pages(
                lambda after_cursor: store.list_runtime_manifests(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
            runtime_units=self._collect_pages(
                lambda after_cursor: store.list_all_runtime_units(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
            sandboxes=self._collect_pages(
                lambda after_cursor: store.list_all_sandboxes(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
            replays=self._collect_pages(
                lambda after_cursor: self.list_replays(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
            experiments=self._collect_pages(
                lambda after_cursor: self.list_experiments(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
            component_statuses=self._collect_pages(
                lambda after_cursor: store.list_all_component_statuses(
                    limit=limit_per_kind,
                    after_cursor=after_cursor,
                )
            ),
        )
