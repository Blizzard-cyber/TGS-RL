"""Static typing contracts shared across sqlite storage mixins."""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from google.protobuf.message import Message
from tgsrl.v1 import control_pb2, runtime_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.storage.types import ManagedWorkerTraceCommit, Page, SequencedRuntimeEvent


class SQLiteStoreHelpers(Protocol):
    """Typing-only helper contract for composed sqlite store mixins."""

    _connection: sqlite3.Connection

    @staticmethod
    def _stamp(value: datetime | None = None) -> str: ...

    @staticmethod
    def _parse_stamp(value: str) -> datetime: ...

    @staticmethod
    def _marshal(message: Message) -> tuple[bytes, str]: ...

    @staticmethod
    def _decode_message[T: Message](message_type: type[T], payload: bytes, context: str) -> T: ...

    @staticmethod
    def _token(*parts: object) -> str: ...

    @staticmethod
    def _read_token(token: str, expected_size: int) -> tuple[object, ...]: ...

    def _page_messages[T: Message](
        self,
        *,
        table: str,
        id_column: str,
        seq_column: str,
        message_type: type[T],
        limit: int,
        after_cursor: str,
        string_sequence: bool = False,
    ) -> Page[T]: ...

    def list_runtime_manifests(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.RuntimeManifest]: ...

    def list_all_runtime_units(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.RuntimeUnit]: ...

    def list_all_sandboxes(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.Sandbox]: ...

    def list_all_component_statuses(
        self, *, limit: int, after_cursor: str = ""
    ) -> Page[control_pb2.ComponentStatus]: ...

    def upsert_runtime_units(
        self, units: Sequence[runtime_pb2.RuntimeUnit]
    ) -> list[runtime_pb2.RuntimeUnit]: ...

    def upsert_sandboxes(
        self, sandboxes: Sequence[runtime_pb2.Sandbox]
    ) -> list[runtime_pb2.Sandbox]: ...

    def list_sandboxes(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> Page[runtime_pb2.Sandbox]: ...

    def list_runtime_units(
        self,
        *,
        run_id: str,
        limit: int,
        stage_id: str = "",
        after_cursor: str = "",
    ) -> Page[runtime_pb2.RuntimeUnit]: ...

    def list_component_statuses(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> Page[control_pb2.ComponentStatus]: ...

    def persist_managed_worker_trace(
        self,
        *,
        scope: str,
        key: str,
        request_payload: bytes,
        batch: trace_pb2.TraceEventBatch,
        intents: Sequence[scheduling_pb2.SchedulingIntent],
        response_payload: bytes,
    ) -> ManagedWorkerTraceCommit: ...

    def watch_runtime_events_with_sequences(
        self, *, run_id: str, limit: int, after_cursor: str = ""
    ) -> tuple[list[SequencedRuntimeEvent], str]: ...
