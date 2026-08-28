"""Small shared protobuf/time/cursor helpers used across runtime and gateway."""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime

from google.protobuf import timestamp_pb2
from google.protobuf.message import Message


def clone_message[MessageT: Message](message: MessageT) -> MessageT:
    """Return a detached protobuf message of the same type."""
    clone = type(message)()
    clone.CopyFrom(message)
    return clone


def utc_now(*, truncate_microseconds: bool = False) -> datetime:
    """Return a timezone-aware UTC instant."""
    value = datetime.now(tz=UTC)
    if truncate_microseconds:
        value = value.replace(microsecond=0)
    return value


def ensure_utc(value: datetime) -> datetime:
    """Normalize any aware datetime to UTC."""
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def timestamp_from_datetime(value: datetime | None = None) -> timestamp_pb2.Timestamp:
    """Convert a UTC-aware datetime to protobuf Timestamp."""
    stamp = timestamp_pb2.Timestamp()
    stamp.FromDatetime(ensure_utc(value or utc_now()))
    return stamp


def stable_cursor(scope: str, *parts: object) -> str:
    """Return a deterministic opaque cursor for stable paging and idempotency surfaces."""
    payload = json.dumps([scope, *parts], separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def encode_page_token(*, scope: str, filters: tuple[str, ...], offset: int) -> str:
    """Encode one versioned, scope-bound pagination position."""
    if not scope or offset < 0:
        raise ValueError("invalid pagination token payload")
    payload = json.dumps(
        {"v": 1, "scope": scope, "filters": list(filters), "offset": offset},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_page_token(token: str, *, scope: str, filters: tuple[str, ...]) -> int:
    """Decode a page token and reject malformed or cross-query reuse."""
    if not token:
        return 0
    try:
        padding = "=" * (-len(token) % 4)
        raw = base64.b64decode((token + padding).encode("ascii"), altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid page_token") from error
    if not isinstance(payload, dict) or set(payload) != {
        "v",
        "scope",
        "filters",
        "offset",
    }:
        raise ValueError("invalid page_token")
    offset = payload["offset"]
    token_filters = payload["filters"]
    if (
        payload["v"] != 1
        or payload["scope"] != scope
        or token_filters != list(filters)
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
    ):
        raise ValueError("page_token does not match this query")
    return offset
