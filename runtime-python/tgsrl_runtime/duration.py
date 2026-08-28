"""UTC timestamp and duration helpers for runtime services."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from google.protobuf import duration_pb2, timestamp_pb2

import tgsrl_runtime.proto_utils as proto_utils

ensure_utc = proto_utils.ensure_utc
utc_now = proto_utils.utc_now
timestamp_from_datetime = proto_utils.timestamp_from_datetime

__all__ = [
    "duration_to_timedelta",
    "ensure_utc",
    "timestamp_to_datetime",
    "to_duration",
    "to_timestamp",
    "utc_now",
]


def to_timestamp(value: datetime) -> timestamp_pb2.Timestamp:
    """Convert a UTC-aware datetime to protobuf Timestamp."""
    return timestamp_from_datetime(value)


def timestamp_to_datetime(value: timestamp_pb2.Timestamp) -> datetime:
    """Convert protobuf Timestamp to UTC datetime."""
    return value.ToDatetime(tzinfo=UTC)


def to_duration(value: timedelta) -> duration_pb2.Duration:
    """Convert timedelta to protobuf Duration."""
    output = duration_pb2.Duration()
    output.FromTimedelta(value)
    return output


def duration_to_timedelta(value: duration_pb2.Duration) -> timedelta:
    """Convert protobuf Duration to timedelta."""
    return value.ToTimedelta()
