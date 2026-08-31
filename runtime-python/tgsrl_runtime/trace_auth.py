"""Authentication helpers for Scheduler-forwarded managed-worker traces."""

from __future__ import annotations

import hashlib
import hmac

from tgsrl.v1 import runtime_pb2


def sign_trace_request(key: bytes, request: runtime_pb2.PublishTraceBatchRequest) -> bytes:
    if len(key) < 32:
        raise ValueError("worker trace signing key must contain at least 32 bytes")
    unsigned = runtime_pb2.PublishTraceBatchRequest()
    unsigned.CopyFrom(request)
    unsigned.ClearField("authentication_tag")
    return hmac.new(
        key,
        unsigned.SerializeToString(deterministic=True),
        hashlib.sha256,
    ).digest()


def verify_trace_request(key: bytes, request: runtime_pb2.PublishTraceBatchRequest) -> bool:
    if not request.authentication_tag:
        return False
    try:
        expected = sign_trace_request(key, request)
    except ValueError:
        return False
    return hmac.compare_digest(expected, request.authentication_tag)
