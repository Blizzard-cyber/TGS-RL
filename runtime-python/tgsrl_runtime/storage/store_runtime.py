"""Runtime-facing sqlite storage methods for manifests, sandboxes, events, and recovery."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import cast

from google.protobuf.message import Message
from tgsrl.v1 import control_pb2, runtime_pb2

from tgsrl_runtime.storage.store_contracts import SQLiteStoreHelpers
from tgsrl_runtime.storage.types import (
    IntentVersionConflict,
    Page,
    RuntimeObservationBatch,
    RuntimeObservationCommit,
    RuntimeRecoveryState,
    SequencedRuntimeEvent,
)


class SQLiteRuntimeStoreMixin:
    """Runtime-side persistence methods mixed into the sqlite compatibility facade."""

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

    def _collect_runtime_event_pages(
        self,
        *,
        run_id: str,
        limit: int,
    ) -> list[SequencedRuntimeEvent]:
        items: list[SequencedRuntimeEvent] = []
        cursor = ""
        while True:
            page_items, next_cursor = self.watch_runtime_events_with_sequences(
                run_id=run_id,
                limit=limit,
                after_cursor=cursor,
            )
            items.extend(page_items)
            if not next_cursor:
                return items
            cursor = next_cursor

    def persist_runtime_observation(
        self,
        batch: RuntimeObservationBatch,
    ) -> RuntimeObservationCommit:
        store = cast(SQLiteStoreHelpers, self)
        sandbox_payload, sandbox_digest = store._marshal(batch.sandbox)
        unit_payload, unit_digest = store._marshal(batch.runtime_unit)
        event_payload, event_digest = store._marshal(batch.event)
        status_payload, status_digest = store._marshal(batch.component_status)
        now = store._stamp()
        occurred_at = store._stamp(batch.event.occurred_at.ToDatetime(tzinfo=UTC))
        with store._connection:
            existing = store._connection.execute(
                """
                SELECT runtime_event_seq, payload, payload_sha256
                FROM runtime_events
                WHERE event_id = ?
                """,
                (batch.event.event_id,),
            ).fetchone()
            if existing is not None:
                if existing["payload_sha256"] != event_digest:
                    raise IntentVersionConflict(
                        "runtime event "
                        f"{batch.event.event_id} already exists with different payload"
                    )
                sequence = int(existing["runtime_event_seq"])
                committed_event = store._decode_message(
                    runtime_pb2.SandboxEvent,
                    existing["payload"],
                    f"runtime event {batch.event.event_id}",
                )
                status = self._load_component_status(
                    run_id=batch.component_status.run_id,
                    component=batch.component_status.component,
                )
                sandbox = self._load_sandbox(batch.sandbox.sandbox_id)
                unit = self._load_runtime_unit(
                    batch.runtime_unit.run_id,
                    batch.runtime_unit.runtime_unit_id,
                )
                return RuntimeObservationCommit(
                    sandbox=sandbox,
                    runtime_unit=unit,
                    event=committed_event,
                    component_status=status,
                    runtime_event_sequence=sequence,
                )

            store._connection.execute(
                """
                INSERT INTO sandboxes(
                    sandbox_id,
                    run_id,
                    job_id,
                    trace_id,
                    generation,
                    state,
                    payload,
                    payload_sha256,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sandbox_id) DO UPDATE SET
                    run_id = excluded.run_id,
                    job_id = excluded.job_id,
                    trace_id = excluded.trace_id,
                    generation = excluded.generation,
                    state = excluded.state,
                    payload = excluded.payload,
                    payload_sha256 = excluded.payload_sha256,
                    updated_at = excluded.updated_at
                """,
                (
                    batch.sandbox.sandbox_id,
                    batch.sandbox.run_id,
                    batch.sandbox.job_id,
                    batch.sandbox.trace_id,
                    batch.sandbox.generation,
                    batch.sandbox.state,
                    sandbox_payload,
                    sandbox_digest,
                    now,
                ),
            )
            store._connection.execute(
                """
                INSERT INTO runtime_units(
                    run_id,
                    runtime_unit_id,
                    stage_id,
                    generation,
                    state,
                    payload,
                    payload_sha256,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, runtime_unit_id) DO UPDATE SET
                    stage_id = excluded.stage_id,
                    generation = excluded.generation,
                    state = excluded.state,
                    payload = excluded.payload,
                    payload_sha256 = excluded.payload_sha256,
                    updated_at = excluded.updated_at
                """,
                (
                    batch.runtime_unit.run_id,
                    batch.runtime_unit.runtime_unit_id,
                    batch.runtime_unit.stage_id,
                    batch.runtime_unit.generation,
                    batch.runtime_unit.state,
                    unit_payload,
                    unit_digest,
                    now,
                ),
            )
            store._connection.execute(
                """
                INSERT INTO runtime_events(
                    event_id,
                    run_id,
                    job_id,
                    trace_id,
                    sandbox_id,
                    generation,
                    occurred_at,
                    payload,
                    payload_sha256,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.event.event_id,
                    batch.event.run_id,
                    batch.event.job_id,
                    batch.event.trace_id,
                    batch.event.sandbox_id,
                    batch.event.generation,
                    occurred_at,
                    event_payload,
                    event_digest,
                    now,
                ),
            )
            event_row = store._connection.execute(
                """
                SELECT runtime_event_seq, payload
                FROM runtime_events
                WHERE event_id = ?
                """,
                (batch.event.event_id,),
            ).fetchone()
            if event_row is None:
                raise KeyError("runtime event write failed")
            sequence = int(event_row["runtime_event_seq"])
            component_status = control_pb2.ComponentStatus()
            component_status.CopyFrom(batch.component_status)
            component_status.revision = sequence
            status_payload, status_digest = store._marshal(component_status)
            store._connection.execute(
                """
                INSERT INTO component_statuses(
                    run_id,
                    job_id,
                    trace_id,
                    component,
                    revision,
                    health,
                    payload,
                    payload_sha256,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, component) DO UPDATE SET
                    job_id = excluded.job_id,
                    trace_id = excluded.trace_id,
                    revision = excluded.revision,
                    health = excluded.health,
                    payload = excluded.payload,
                    payload_sha256 = excluded.payload_sha256,
                    updated_at = excluded.updated_at
                """,
                (
                    component_status.run_id,
                    component_status.job_id,
                    component_status.trace_id,
                    component_status.component,
                    component_status.revision,
                    component_status.health,
                    status_payload,
                    status_digest,
                    now,
                ),
            )
        return RuntimeObservationCommit(
            sandbox=self._load_sandbox(batch.sandbox.sandbox_id),
            runtime_unit=self._load_runtime_unit(
                batch.runtime_unit.run_id, batch.runtime_unit.runtime_unit_id
            ),
            event=store._decode_message(
                runtime_pb2.SandboxEvent,
                event_row["payload"],
                f"runtime event {batch.event.event_id}",
            ),
            component_status=self._load_component_status(
                run_id=batch.component_status.run_id,
                component=batch.component_status.component,
            ),
            runtime_event_sequence=sequence,
        )

    def _load_sandbox(self, sandbox_id: str) -> runtime_pb2.Sandbox:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            "SELECT payload FROM sandboxes WHERE sandbox_id = ?",
            (sandbox_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"sandbox {sandbox_id} not found")
        return store._decode_message(
            runtime_pb2.Sandbox,
            row["payload"],
            f"sandbox {sandbox_id}",
        )

    def _load_runtime_unit(self, run_id: str, runtime_unit_id: str) -> runtime_pb2.RuntimeUnit:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            """
            SELECT payload
            FROM runtime_units
            WHERE run_id = ? AND runtime_unit_id = ?
            """,
            (run_id, runtime_unit_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"runtime unit {run_id}/{runtime_unit_id} not found")
        return store._decode_message(
            runtime_pb2.RuntimeUnit,
            row["payload"],
            f"runtime unit {run_id}/{runtime_unit_id}",
        )

    def _load_component_status(self, *, run_id: str, component: str) -> control_pb2.ComponentStatus:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            """
            SELECT payload
            FROM component_statuses
            WHERE run_id = ? AND component = ?
            """,
            (run_id, component),
        ).fetchone()
        if row is None:
            raise KeyError(f"component status {run_id}/{component} not found")
        return store._decode_message(
            control_pb2.ComponentStatus,
            row["payload"],
            f"component status {run_id}/{component}",
        )

    def save_runtime_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        store = cast(SQLiteStoreHelpers, self)
        payload, digest = store._marshal(manifest)
        now = store._stamp()
        with store._connection:
            store._connection.execute(
                """
                INSERT INTO runtime_manifests(
                    run_id,
                    job_id,
                    trace_id,
                    payload,
                    payload_sha256,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    job_id = excluded.job_id,
                    trace_id = excluded.trace_id,
                    payload = excluded.payload,
                    payload_sha256 = excluded.payload_sha256,
                    updated_at = excluded.updated_at
                """,
                (
                    manifest.run_id,
                    manifest.job_id,
                    manifest.trace_id,
                    payload,
                    digest,
                    now,
                ),
            )
        return self.get_runtime_manifest(manifest.run_id)

    def get_runtime_manifest(self, run_id: str) -> runtime_pb2.RuntimeManifest:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            "SELECT payload FROM runtime_manifests WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"runtime manifest {run_id} not found")
        return store._decode_message(
            runtime_pb2.RuntimeManifest,
            row["payload"],
            f"runtime manifest {run_id}",
        )

    def list_runtime_manifests(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.RuntimeManifest]:
        store = cast(SQLiteStoreHelpers, self)
        return store._page_messages(
            table="runtime_manifests",
            id_column="run_id",
            seq_column="run_id",
            message_type=runtime_pb2.RuntimeManifest,
            limit=limit,
            after_cursor=after_cursor,
            string_sequence=True,
        )

    def upsert_runtime_units(
        self, units: Sequence[runtime_pb2.RuntimeUnit]
    ) -> list[runtime_pb2.RuntimeUnit]:
        store = cast(SQLiteStoreHelpers, self)
        if not units:
            return []
        now = store._stamp()
        with store._connection:
            for unit in units:
                payload, digest = store._marshal(unit)
                store._connection.execute(
                    """
                    INSERT INTO runtime_units(
                        run_id,
                        runtime_unit_id,
                        stage_id,
                        generation,
                        state,
                        payload,
                        payload_sha256,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, runtime_unit_id) DO UPDATE SET
                        stage_id = excluded.stage_id,
                        generation = excluded.generation,
                        state = excluded.state,
                        payload = excluded.payload,
                        payload_sha256 = excluded.payload_sha256,
                        updated_at = excluded.updated_at
                    """,
                    (
                        unit.run_id,
                        unit.runtime_unit_id,
                        unit.stage_id,
                        unit.generation,
                        unit.state,
                        payload,
                        digest,
                        now,
                    ),
                )
        return self.list_runtime_units(
            run_id=units[0].run_id,
            limit=max(len(units), 1),
        ).items

    def list_all_runtime_units(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.RuntimeUnit]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        cursor_run = ""
        cursor_unit = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            cursor_run = str(decoded[0])
            cursor_unit = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT run_id, runtime_unit_id, payload
                FROM runtime_units
                WHERE (
                    run_id > ?
                    OR (run_id = ? AND runtime_unit_id > ?)
                )
                ORDER BY run_id ASC, runtime_unit_id ASC
                LIMIT ?
                """,
                (cursor_run, cursor_run, cursor_unit, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["run_id"], tail["runtime_unit_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                runtime_pb2.RuntimeUnit,
                row["payload"],
                f"runtime unit {row['run_id']}/{row['runtime_unit_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def list_runtime_units(
        self,
        *,
        run_id: str,
        limit: int,
        stage_id: str = "",
        after_cursor: str = "",
    ) -> Page[runtime_pb2.RuntimeUnit]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if stage_id:
            return self._list_runtime_units_for_stage(
                run_id=run_id,
                stage_id=stage_id,
                limit=limit,
                after_cursor=after_cursor,
            )
        return self._list_runtime_units_all_stages(
            run_id=run_id,
            limit=limit,
            after_cursor=after_cursor,
        )

    def _list_runtime_units_for_stage(
        self,
        *,
        run_id: str,
        stage_id: str,
        limit: int,
        after_cursor: str,
    ) -> Page[runtime_pb2.RuntimeUnit]:
        store = cast(SQLiteStoreHelpers, self)
        cursor_unit = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 1)
            cursor_unit = str(decoded[0])
        rows = list(
            store._connection.execute(
                """
                SELECT runtime_unit_id, payload
                FROM runtime_units
                WHERE run_id = ? AND stage_id = ? AND runtime_unit_id > ?
                ORDER BY runtime_unit_id ASC
                LIMIT ?
                """,
                (run_id, stage_id, cursor_unit, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["runtime_unit_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                runtime_pb2.RuntimeUnit,
                row["payload"],
                f"runtime unit {row['runtime_unit_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def _list_runtime_units_all_stages(
        self, *, run_id: str, limit: int, after_cursor: str
    ) -> Page[runtime_pb2.RuntimeUnit]:
        store = cast(SQLiteStoreHelpers, self)
        cursor_stage = ""
        cursor_unit = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            cursor_stage = str(decoded[0])
            cursor_unit = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT stage_id, runtime_unit_id, payload
                FROM runtime_units
                WHERE run_id = ?
                  AND (stage_id > ? OR (stage_id = ? AND runtime_unit_id > ?))
                ORDER BY stage_id ASC, runtime_unit_id ASC
                LIMIT ?
                """,
                (run_id, cursor_stage, cursor_stage, cursor_unit, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["stage_id"], tail["runtime_unit_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                runtime_pb2.RuntimeUnit,
                row["payload"],
                f"runtime unit {row['runtime_unit_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def upsert_sandboxes(
        self, sandboxes: Sequence[runtime_pb2.Sandbox]
    ) -> list[runtime_pb2.Sandbox]:
        store = cast(SQLiteStoreHelpers, self)
        if not sandboxes:
            return []
        now = store._stamp()
        with store._connection:
            for sandbox in sandboxes:
                payload, digest = store._marshal(sandbox)
                store._connection.execute(
                    """
                    INSERT INTO sandboxes(
                        sandbox_id,
                        run_id,
                        job_id,
                        trace_id,
                        generation,
                        state,
                        payload,
                        payload_sha256,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sandbox_id) DO UPDATE SET
                        run_id = excluded.run_id,
                        job_id = excluded.job_id,
                        trace_id = excluded.trace_id,
                        generation = excluded.generation,
                        state = excluded.state,
                        payload = excluded.payload,
                        payload_sha256 = excluded.payload_sha256,
                        updated_at = excluded.updated_at
                    """,
                    (
                        sandbox.sandbox_id,
                        sandbox.run_id,
                        sandbox.job_id,
                        sandbox.trace_id,
                        sandbox.generation,
                        sandbox.state,
                        payload,
                        digest,
                        now,
                    ),
                )
        return self.list_sandboxes(
            run_id=sandboxes[0].run_id,
            limit=max(len(sandboxes), 1),
        ).items

    def list_sandboxes(
        self,
        *,
        run_id: str,
        limit: int,
        after_cursor: str = "",
    ) -> Page[runtime_pb2.Sandbox]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        sandbox_id = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 1)
            sandbox_id = str(decoded[0])
        rows = list(
            store._connection.execute(
                """
                SELECT sandbox_id, payload
                FROM sandboxes
                WHERE run_id = ? AND sandbox_id > ?
                ORDER BY sandbox_id ASC
                LIMIT ?
                """,
                (run_id, sandbox_id, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["sandbox_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                runtime_pb2.Sandbox,
                row["payload"],
                f"sandbox {row['sandbox_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def list_all_sandboxes(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.Sandbox]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        cursor_run = ""
        cursor_sandbox = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            cursor_run = str(decoded[0])
            cursor_sandbox = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT run_id, sandbox_id, payload
                FROM sandboxes
                WHERE (
                    run_id > ?
                    OR (run_id = ? AND sandbox_id > ?)
                )
                ORDER BY run_id ASC, sandbox_id ASC
                LIMIT ?
                """,
                (cursor_run, cursor_run, cursor_sandbox, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["run_id"], tail["sandbox_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                runtime_pb2.Sandbox,
                row["payload"],
                f"sandbox {row['run_id']}/{row['sandbox_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def publish_runtime_event(self, event: runtime_pb2.SandboxEvent) -> runtime_pb2.SandboxEvent:
        store = cast(SQLiteStoreHelpers, self)
        payload, digest = store._marshal(event)
        created_at = store._stamp()
        with store._connection:
            existing = store._connection.execute(
                "SELECT payload_sha256, payload FROM runtime_events WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if existing is not None:
                if existing["payload_sha256"] != digest:
                    raise IntentVersionConflict(
                        f"runtime event {event.event_id} already exists with different payload"
                    )
                return store._decode_message(
                    runtime_pb2.SandboxEvent,
                    existing["payload"],
                    f"runtime event {event.event_id}",
                )
            store._connection.execute(
                """
                INSERT INTO runtime_events(
                    event_id,
                    run_id,
                    job_id,
                    trace_id,
                    sandbox_id,
                    generation,
                    occurred_at,
                    payload,
                    payload_sha256,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.run_id,
                    event.job_id,
                    event.trace_id,
                    event.sandbox_id,
                    event.generation,
                    store._stamp(event.occurred_at.ToDatetime(tzinfo=UTC)),
                    payload,
                    digest,
                    created_at,
                ),
            )
        return event

    def watch_runtime_events(
        self,
        *,
        run_id: str,
        limit: int,
        after_cursor: str = "",
    ) -> Page[runtime_pb2.SandboxEvent]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        sequence = 0
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            sequence = int(str(decoded[0]))
        rows = list(
            store._connection.execute(
                """
                SELECT runtime_event_seq, event_id, payload
                FROM runtime_events
                WHERE run_id = ?
                  AND runtime_event_seq > ?
                ORDER BY runtime_event_seq ASC
                LIMIT ?
                """,
                (run_id, sequence, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["runtime_event_seq"], tail["event_id"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                runtime_pb2.SandboxEvent,
                row["payload"],
                f"runtime event {row['event_id']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def watch_runtime_events_with_sequences(
        self,
        *,
        run_id: str,
        limit: int,
        after_cursor: str = "",
    ) -> tuple[list[SequencedRuntimeEvent], str]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        sequence = 0
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            sequence = int(str(decoded[0]))
        rows = list(
            store._connection.execute(
                """
                SELECT runtime_event_seq, event_id, payload
                FROM runtime_events
                WHERE run_id = ?
                  AND runtime_event_seq > ?
                ORDER BY runtime_event_seq ASC
                LIMIT ?
                """,
                (run_id, sequence, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["runtime_event_seq"], tail["event_id"])
            rows = rows[:limit]
        items = [
            SequencedRuntimeEvent(
                sequence=int(row["runtime_event_seq"]),
                event=store._decode_message(
                    runtime_pb2.SandboxEvent,
                    row["payload"],
                    f"runtime event {row['event_id']}",
                ),
            )
            for row in rows
        ]
        return items, next_cursor

    def save_runtime_checkpoint(
        self,
        *,
        run_id: str,
        checkpoint_ref: str,
        completed_at: datetime,
    ) -> str:
        store = cast(SQLiteStoreHelpers, self)
        stamp = store._stamp()
        completed = store._stamp(completed_at)
        with store._connection:
            store._connection.execute(
                """
                INSERT INTO runtime_checkpoints(
                    run_id,
                    checkpoint_ref,
                    completed_at,
                    updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id, checkpoint_ref) DO UPDATE SET
                    completed_at = excluded.completed_at,
                    updated_at = excluded.updated_at
                """,
                (run_id, checkpoint_ref, completed, stamp),
            )
            row = store._connection.execute(
                """
                SELECT checkpoint_seq
                FROM runtime_checkpoints
                WHERE run_id = ? AND checkpoint_ref = ?
                """,
                (run_id, checkpoint_ref),
            ).fetchone()
        if row is None:
            raise KeyError("runtime checkpoint write failed")
        return store._token(row["checkpoint_seq"], checkpoint_ref)

    def list_runtime_checkpoints(self, *, run_id: str) -> list[tuple[str, datetime]]:
        store = cast(SQLiteStoreHelpers, self)
        rows = store._connection.execute(
            """
            SELECT checkpoint_ref, completed_at
            FROM runtime_checkpoints
            WHERE run_id = ?
            ORDER BY checkpoint_seq ASC
            """,
            (run_id,),
        )
        return [
            (str(row["checkpoint_ref"]), store._parse_stamp(str(row["completed_at"])))
            for row in rows
        ]

    def upsert_component_statuses(
        self, statuses: Sequence[control_pb2.ComponentStatus]
    ) -> list[control_pb2.ComponentStatus]:
        store = cast(SQLiteStoreHelpers, self)
        if not statuses:
            return []
        now = store._stamp()
        with store._connection:
            for status in statuses:
                payload, digest = store._marshal(status)
                store._connection.execute(
                    """
                    INSERT INTO component_statuses(
                        run_id,
                        job_id,
                        trace_id,
                        component,
                        revision,
                        health,
                        payload,
                        payload_sha256,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, component) DO UPDATE SET
                        job_id = excluded.job_id,
                        trace_id = excluded.trace_id,
                        revision = excluded.revision,
                        health = excluded.health,
                        payload = excluded.payload,
                        payload_sha256 = excluded.payload_sha256,
                        updated_at = excluded.updated_at
                    """,
                    (
                        status.run_id,
                        status.job_id,
                        status.trace_id,
                        status.component,
                        status.revision,
                        status.health,
                        payload,
                        digest,
                        now,
                    ),
                )
        return self.list_component_statuses(
            run_id=statuses[0].run_id,
            limit=max(len(statuses), 1),
        ).items

    def list_component_statuses(
        self,
        *,
        run_id: str,
        limit: int,
        after_cursor: str = "",
    ) -> Page[control_pb2.ComponentStatus]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        sequence = 0
        component = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            sequence = int(str(decoded[0]))
            component = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT component_seq, component, payload
                FROM component_statuses
                WHERE run_id = ?
                  AND (component_seq > ? OR (component_seq = ? AND component > ?))
                ORDER BY component_seq ASC, component ASC
                LIMIT ?
                """,
                (run_id, sequence, sequence, component, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["component_seq"], tail["component"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                control_pb2.ComponentStatus,
                row["payload"],
                f"component status {row['component']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def list_all_component_statuses(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[control_pb2.ComponentStatus]:
        store = cast(SQLiteStoreHelpers, self)
        if limit <= 0:
            raise ValueError("limit must be positive")
        cursor_run = ""
        cursor_component = ""
        if after_cursor:
            decoded = store._read_token(after_cursor, 2)
            cursor_run = str(decoded[0])
            cursor_component = str(decoded[1])
        rows = list(
            store._connection.execute(
                """
                SELECT run_id, component, payload
                FROM component_statuses
                WHERE (
                    run_id > ?
                    OR (run_id = ? AND component > ?)
                )
                ORDER BY run_id ASC, component ASC
                LIMIT ?
                """,
                (cursor_run, cursor_run, cursor_component, limit + 1),
            )
        )
        next_cursor = ""
        if len(rows) > limit:
            tail = rows[limit - 1]
            next_cursor = store._token(tail["run_id"], tail["component"])
            rows = rows[:limit]
        items = [
            store._decode_message(
                control_pb2.ComponentStatus,
                row["payload"],
                f"component status {row['run_id']}/{row['component']}",
            )
            for row in rows
        ]
        return Page(items=items, next_cursor=next_cursor)

    def load_runtime_recovery(self, run_id: str) -> RuntimeRecoveryState:
        manifest = self.get_runtime_manifest(run_id)
        page_limit = 10_000
        runtime_units = self._collect_pages(
            lambda after_cursor: self.list_runtime_units(
                run_id=run_id,
                limit=page_limit,
                after_cursor=after_cursor,
            )
        )
        sandboxes = self._collect_pages(
            lambda after_cursor: self.list_sandboxes(
                run_id=run_id,
                limit=page_limit,
                after_cursor=after_cursor,
            )
        )
        runtime_events = self._collect_runtime_event_pages(
            run_id=run_id,
            limit=page_limit,
        )
        component_statuses = self._collect_pages(
            lambda after_cursor: self.list_component_statuses(
                run_id=run_id,
                limit=page_limit,
                after_cursor=after_cursor,
            )
        )
        checkpoints = self.list_runtime_checkpoints(run_id=run_id)
        latest_generation = max(
            max((unit.generation for unit in runtime_units), default=0),
            max((sandbox.generation for sandbox in sandboxes), default=0),
        )
        latest_runtime_cursor = self._latest_runtime_cursor(run_id)
        return RuntimeRecoveryState(
            manifest=manifest,
            runtime_units=runtime_units,
            sandboxes=sandboxes,
            runtime_events=runtime_events,
            component_statuses=component_statuses,
            checkpoints=checkpoints,
            latest_generation=latest_generation,
            latest_runtime_cursor=latest_runtime_cursor,
        )

    def get_runtime_status(
        self, run_id: str
    ) -> tuple[
        runtime_pb2.RuntimeManifest,
        list[runtime_pb2.RuntimeUnit],
        list[runtime_pb2.Sandbox],
        str,
    ]:
        recovery = self.load_runtime_recovery(run_id)
        return (
            recovery.manifest,
            recovery.runtime_units,
            recovery.sandboxes,
            recovery.latest_runtime_cursor,
        )

    def _latest_runtime_cursor(self, run_id: str) -> str:
        store = cast(SQLiteStoreHelpers, self)
        row = store._connection.execute(
            """
            SELECT COALESCE(MAX(runtime_event_seq), 0) AS seq
            FROM runtime_events
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        return store._token(row["seq"], "") if row is not None else store._token(0, "")
