"""Shared sqlite/protobuf encoding helpers."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from google.protobuf.message import DecodeError, Message

from tgsrl_runtime.storage.types import StorageCorruptionError


def decode_message[T: Message](message_type: type[T], payload: bytes, context: str) -> T:
    message = message_type()
    try:
        message.ParseFromString(payload)
    except DecodeError as error:
        raise StorageCorruptionError(f"{context} is corrupt") from error
    return message


def now_utc() -> datetime:
    return datetime.now(tz=UTC)


def stamp_utc(value: datetime | None = None) -> str:
    current = value or now_utc()
    if current.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return current.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def marshal_message(message: Message) -> tuple[bytes, str]:
    payload = message.SerializeToString(deterministic=True)
    return payload, hashlib.sha256(payload).hexdigest()
