"""Shared sqlite connection lifecycle and helper utilities."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

from google.protobuf.message import Message

from tgsrl_runtime.storage.codec import (
    decode_message,
    marshal_message,
    now_utc,
    parse_stamp,
    stamp_utc,
)
from tgsrl_runtime.storage.pagination import decode_cursor, encode_cursor
from tgsrl_runtime.storage.schema import apply_migrations
from tgsrl_runtime.storage.types import StorageCorruptionError


class SQLiteStoreBase:
    """Connection owner and shared sqlite helpers for storage mixins."""

    path: Path
    _connection: sqlite3.Connection

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA temp_store=MEMORY")
        self._connection.execute("PRAGMA busy_timeout=5000")
        try:
            self._migrate()
        except Exception:
            self._connection.close()
            raise
        if str(self.path) != ":memory:":
            for database_file in (self.path, Path(f"{self.path}-wal"), Path(f"{self.path}-shm")):
                if database_file.exists():
                    os.chmod(database_file, 0o600)

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SQLiteStoreBase:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _migrate(self) -> None:
        apply_migrations(self._connection, migrations_dir=Path(__file__).with_name("migrations"))

    @staticmethod
    def _now() -> datetime:
        return now_utc()

    @staticmethod
    def _stamp(value: datetime | None = None) -> str:
        return stamp_utc(value)

    @staticmethod
    def _parse_stamp(value: str) -> datetime:
        return parse_stamp(value)

    @staticmethod
    def _marshal(message: Message) -> tuple[bytes, str]:
        return marshal_message(message)

    @staticmethod
    def _decode_message[T: Message](message_type: type[T], payload: bytes, context: str) -> T:
        return decode_message(message_type, payload, context)

    @staticmethod
    def _token(*parts: object) -> str:
        return encode_cursor(*parts)

    @staticmethod
    def _read_token(token: str, expected_size: int) -> tuple[object, ...]:
        return decode_cursor(token, expected_size)

    def healthcheck(self) -> None:
        row = self._connection.execute("PRAGMA quick_check").fetchone()
        if row is None or row[0] != "ok":
            detail = row[0] if row else "unknown"
            raise StorageCorruptionError(f"sqlite quick_check failed: {detail}")
