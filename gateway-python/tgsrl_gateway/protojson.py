"""Helpers for converting generated protobuf messages to API JSON."""

from __future__ import annotations

from datetime import UTC, datetime

from google.protobuf import json_format, timestamp_pb2
from google.protobuf.message import Message


def clone_message[MessageT: Message](message: MessageT) -> MessageT:
    """Return a detached protobuf message without depending on Runtime internals."""
    clone = type(message)()
    clone.CopyFrom(message)
    return clone


def message_to_dict(message: Message) -> dict[str, object]:
    return json_format.MessageToDict(
        message,
        preserving_proto_field_name=False,
        use_integers_for_enums=False,
    )


def parse_message[MessageT: Message](payload: dict[str, object], message: MessageT) -> MessageT:
    json_format.ParseDict(payload, message, ignore_unknown_fields=False)
    return message


def timestamp_from_datetime(value: datetime | None = None) -> timestamp_pb2.Timestamp:
    resolved = value or datetime.now(tz=UTC).replace(microsecond=0)
    if resolved.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    stamp = timestamp_pb2.Timestamp()
    stamp.FromDatetime(resolved.astimezone(UTC))
    return stamp
