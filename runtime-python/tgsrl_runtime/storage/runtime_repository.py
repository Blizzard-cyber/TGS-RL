"""Runtime-facing repository wrapper over the sqlite store."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from tgsrl.v1 import control_pb2, runtime_pb2

from tgsrl_runtime.storage.types import (
    Page,
    RuntimeObservationBatch,
    RuntimeObservationCommit,
    RuntimeRecoveryState,
    SequencedRuntimeEvent,
)

if TYPE_CHECKING:
    from tgsrl_runtime.storage.sqlite_store import SQLiteStore


class RuntimeRepository:
    """Runtime-control-facing durable surface for manifests, units, sandboxes, and events."""

    def __init__(self, store: SQLiteStore) -> None:
        self._store = store

    def save_manifest(self, manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
        return self._store.save_runtime_manifest(manifest)

    def upsert_units(
        self, units: Sequence[runtime_pb2.RuntimeUnit]
    ) -> list[runtime_pb2.RuntimeUnit]:
        return self._store.upsert_runtime_units(units)

    def list_units(
        self,
        *,
        run_id: str,
        limit: int,
        stage_id: str = "",
        after_cursor: str = "",
    ) -> Page[runtime_pb2.RuntimeUnit]:
        return self._store.list_runtime_units(
            run_id=run_id,
            limit=limit,
            stage_id=stage_id,
            after_cursor=after_cursor,
        )

    def upsert_sandboxes(
        self, sandboxes: Sequence[runtime_pb2.Sandbox]
    ) -> list[runtime_pb2.Sandbox]:
        return self._store.upsert_sandboxes(sandboxes)

    def list_sandboxes(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.Sandbox]:
        return self._store.list_sandboxes(
            run_id=run_id,
            limit=limit,
            after_cursor=after_cursor,
        )

    def publish_event(self, event: runtime_pb2.SandboxEvent) -> runtime_pb2.SandboxEvent:
        return self._store.publish_runtime_event(event)

    def persist_runtime_observation(
        self,
        batch: RuntimeObservationBatch,
    ) -> RuntimeObservationCommit:
        return self._store.persist_runtime_observation(batch)

    def watch_events(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.SandboxEvent]:
        return self._store.watch_runtime_events(
            run_id=run_id,
            limit=limit,
            after_cursor=after_cursor,
        )

    def watch_events_with_sequences(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> tuple[list[SequencedRuntimeEvent], str]:
        return self._store.watch_runtime_events_with_sequences(
            run_id=run_id,
            limit=limit,
            after_cursor=after_cursor,
        )

    def save_checkpoint(self, *, run_id: str, checkpoint_ref: str, completed_at: datetime) -> str:
        return self._store.save_runtime_checkpoint(
            run_id=run_id,
            checkpoint_ref=checkpoint_ref,
            completed_at=completed_at,
        )

    def upsert_component_statuses(
        self, statuses: Sequence[control_pb2.ComponentStatus]
    ) -> list[control_pb2.ComponentStatus]:
        return self._store.upsert_component_statuses(statuses)

    def list_component_statuses(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> Page[control_pb2.ComponentStatus]:
        return self._store.list_component_statuses(
            run_id=run_id,
            limit=limit,
            after_cursor=after_cursor,
        )

    def list_manifests(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.RuntimeManifest]:
        return self._store.list_runtime_manifests(limit=limit, after_cursor=after_cursor)

    def list_all_units(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.RuntimeUnit]:
        return self._store.list_all_runtime_units(limit=limit, after_cursor=after_cursor)

    def list_all_sandboxes(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.Sandbox]:
        return self._store.list_all_sandboxes(limit=limit, after_cursor=after_cursor)

    def list_all_component_statuses(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[control_pb2.ComponentStatus]:
        return self._store.list_all_component_statuses(
            limit=limit,
            after_cursor=after_cursor,
        )

    def load_recovery(self, run_id: str) -> RuntimeRecoveryState:
        return self._store.load_runtime_recovery(run_id)

    def get_status(
        self, run_id: str
    ) -> tuple[
        runtime_pb2.RuntimeManifest,
        list[runtime_pb2.RuntimeUnit],
        list[runtime_pb2.Sandbox],
        str,
    ]:
        return self._store.get_runtime_status(run_id)
