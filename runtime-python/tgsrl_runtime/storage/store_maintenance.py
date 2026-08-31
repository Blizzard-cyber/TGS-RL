"""Maintenance-oriented sqlite storage helpers for idempotency and retention."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import cast

from tgsrl_runtime.storage.store_contracts import SQLiteStoreHelpers
from tgsrl_runtime.storage.types import DeleteAudit, IntentVersionConflict


class SQLiteMaintenanceMixin:
    """Operational helpers mixed into the sqlite compatibility facade."""

    def remember_idempotent_response(
        self,
        *,
        scope: str,
        key: str,
        request_payload: bytes,
        response_payload: bytes,
    ) -> bytes:
        store = cast(SQLiteStoreHelpers, self)
        request_digest = hashlib.sha256(request_payload).hexdigest()
        created_at = store._stamp()
        with store._connection:
            row = store._connection.execute(
                """
                SELECT request_digest, response_payload
                FROM idempotency_records
                WHERE scope = ? AND key = ?
                """,
                (scope, key),
            ).fetchone()
            if row is not None:
                if row["request_digest"] != request_digest:
                    raise IntentVersionConflict(
                        "idempotency key was reused with a different request"
                    )
                return bytes(row["response_payload"])
            store._connection.execute(
                """
                INSERT INTO idempotency_records(
                    scope,
                    key,
                    request_digest,
                    response_payload,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (scope, key, request_digest, response_payload, created_at),
            )
        return response_payload

    def load_idempotent_response(
        self,
        *,
        scope: str,
        key: str,
        request_payload: bytes,
    ) -> bytes | None:
        store = cast(SQLiteStoreHelpers, self)
        request_digest = hashlib.sha256(request_payload).hexdigest()
        row = store._connection.execute(
            """
            SELECT request_digest, response_payload
            FROM idempotency_records
            WHERE scope = ? AND key = ?
            """,
            (scope, key),
        ).fetchone()
        if row is None:
            return None
        if row["request_digest"] != request_digest:
            raise IntentVersionConflict("idempotency key was reused with a different request")
        return bytes(row["response_payload"])

    def prune_before(self, *, cutoff: datetime, scopes: Sequence[str]) -> list[DeleteAudit]:
        store = cast(SQLiteStoreHelpers, self)
        if cutoff.tzinfo is None:
            raise ValueError("cutoff must be timezone-aware")
        cutoff_stamp = store._stamp(cutoff)
        audits: list[DeleteAudit] = []
        with store._connection:
            for scope in scopes:
                deleted = 0
                if scope == "trace_events":
                    cursor = store._connection.execute(
                        "DELETE FROM trace_events WHERE occurred_at < ?",
                        (cutoff_stamp,),
                    )
                    deleted = cursor.rowcount
                elif scope == "runtime_events":
                    cursor = store._connection.execute(
                        "DELETE FROM runtime_events WHERE occurred_at < ?",
                        (cutoff_stamp,),
                    )
                    deleted = cursor.rowcount
                elif scope == "replays":
                    replay_ids = [
                        str(row["replay_id"])
                        for row in store._connection.execute(
                            "SELECT replay_id FROM replays WHERE updated_at < ?",
                            (cutoff_stamp,),
                        )
                    ]
                    if replay_ids:
                        placeholders = ", ".join("?" for _ in replay_ids)
                        query = (
                            f"DELETE FROM replay_schedule_steps WHERE replay_id IN ({placeholders})"
                        )
                        store._connection.execute(
                            query,
                            replay_ids,
                        )
                    cursor = store._connection.execute(
                        "DELETE FROM replays WHERE updated_at < ?",
                        (cutoff_stamp,),
                    )
                    deleted = cursor.rowcount
                elif scope == "experiments":
                    cursor = store._connection.execute(
                        "DELETE FROM experiments WHERE updated_at < ?",
                        (cutoff_stamp,),
                    )
                    deleted = cursor.rowcount
                elif scope == "component_statuses":
                    cursor = store._connection.execute(
                        "DELETE FROM component_statuses WHERE updated_at < ?",
                        (cutoff_stamp,),
                    )
                    deleted = cursor.rowcount
                else:
                    raise ValueError(f"unsupported retention scope: {scope}")
                created_at = store._stamp()
                store._connection.execute(
                    """
                    INSERT INTO delete_audit(scope, cutoff, deleted_count, created_at, note)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (scope, cutoff_stamp, deleted, created_at, ""),
                )
                audits.append(
                    DeleteAudit(
                        scope=scope,
                        cutoff=cutoff.astimezone(UTC),
                        deleted_count=deleted,
                        created_at=store._parse_stamp(created_at),
                        note="",
                    )
                )
        return audits

    def list_delete_audit(self) -> list[DeleteAudit]:
        store = cast(SQLiteStoreHelpers, self)
        rows = store._connection.execute(
            """
            SELECT scope, cutoff, deleted_count, created_at, note
            FROM delete_audit
            ORDER BY audit_seq ASC
            """
        )
        return [
            DeleteAudit(
                scope=row["scope"],
                cutoff=store._parse_stamp(row["cutoff"]),
                deleted_count=row["deleted_count"],
                created_at=store._parse_stamp(row["created_at"]),
                note=row["note"],
            )
            for row in rows
        ]
