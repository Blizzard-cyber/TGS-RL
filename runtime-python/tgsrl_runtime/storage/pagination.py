"""Cursor helpers for stable sqlite pagination."""

from __future__ import annotations

import base64
import json


def encode_cursor(*parts: object) -> str:
    payload = json.dumps(list(parts), separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def decode_cursor(token: str, expected_size: int) -> tuple[object, ...]:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        parts = json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as error:
        raise ValueError("invalid pagination cursor") from error
    if not isinstance(parts, list) or len(parts) != expected_size:
        raise ValueError("invalid pagination cursor")
    return tuple(parts)
