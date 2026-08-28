"""Opaque stable pagination helpers."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from typing import Any

from tgsrl_gateway.errors import BadRequestError

PAGE_TOKEN_VERSION = 1
OFFSET_PAGE_TOKEN_KIND = "offset"
BACKEND_CURSOR_PAGE_TOKEN_KIND = "backend_cursor"


def _normalize_token_filters(filters: Mapping[str, object] | None) -> dict[str, Any]:
    if not filters:
        return {}

    def normalize(value: object) -> Any:
        if isinstance(value, Mapping):
            return {str(key): normalize(nested) for key, nested in sorted(value.items())}
        if isinstance(value, tuple | list):
            return [normalize(item) for item in value]
        if isinstance(value, str | int | float | bool) or value is None:
            return value
        return str(value)

    return {str(key): normalize(value) for key, value in sorted(filters.items())}


def _encode_token_payload(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii")


def _decode_token_payload(token: str | None) -> dict[str, Any]:
    if not token:
        return {}
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("page token payload must be an object")
    except (
        AttributeError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise BadRequestError("invalid page_token") from error
    return data


def encode_page_token(
    *,
    offset: int,
    scope: str,
    filters: Mapping[str, object] | None = None,
) -> str:
    return _encode_token_payload(
        {
            "version": PAGE_TOKEN_VERSION,
            "kind": OFFSET_PAGE_TOKEN_KIND,
            "offset": offset,
            "scope": scope,
            "filters": _normalize_token_filters(filters),
        }
    )


def decode_page_token(
    token: str | None,
    *,
    scope: str,
    filters: Mapping[str, object] | None = None,
) -> int:
    if not token:
        return 0
    data = _decode_token_payload(token)
    version = data.get("version")
    kind = data.get("kind")
    offset = data.get("offset")
    token_scope = data.get("scope")
    token_filters = data.get("filters")
    expected_filters = _normalize_token_filters(filters)
    if (
        version != PAGE_TOKEN_VERSION
        or kind != OFFSET_PAGE_TOKEN_KIND
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or offset < 0
        or token_scope != scope
        or token_filters != expected_filters
    ):
        raise BadRequestError("invalid page_token")
    return offset


def encode_backend_page_token(
    *,
    backend_cursor: str,
    scope: str,
    filters: Mapping[str, object] | None = None,
) -> str:
    if not backend_cursor:
        return ""
    return _encode_token_payload(
        {
            "version": PAGE_TOKEN_VERSION,
            "kind": BACKEND_CURSOR_PAGE_TOKEN_KIND,
            "backend_cursor": backend_cursor,
            "scope": scope,
            "filters": _normalize_token_filters(filters),
        }
    )


def decode_backend_page_token(
    token: str | None,
    *,
    scope: str,
    filters: Mapping[str, object] | None = None,
) -> str:
    if not token:
        return ""
    data = _decode_token_payload(token)
    version = data.get("version")
    kind = data.get("kind")
    backend_cursor = data.get("backend_cursor")
    token_scope = data.get("scope")
    token_filters = data.get("filters")
    expected_filters = _normalize_token_filters(filters)
    if (
        version != PAGE_TOKEN_VERSION
        or kind != BACKEND_CURSOR_PAGE_TOKEN_KIND
        or not isinstance(backend_cursor, str)
        or not backend_cursor
        or token_scope != scope
        or token_filters != expected_filters
    ):
        raise BadRequestError("invalid page_token")
    return backend_cursor


def paginate[T](
    items: list[T],
    *,
    page_token: str | None,
    limit: int | None,
    scope: str,
    filters: Mapping[str, object] | None = None,
    offset: int = 0,
) -> tuple[list[T], str]:
    start_offset = (
        decode_page_token(page_token, scope=scope, filters=filters) if page_token else offset
    )
    if start_offset < 0:
        raise BadRequestError("invalid page_token")
    if limit is not None and limit < 1:
        raise BadRequestError("limit must be at least 1")
    page_size = limit if limit is not None else 50
    sliced = items[start_offset : start_offset + page_size]
    next_offset = start_offset + len(sliced)
    next_token = (
        encode_page_token(offset=next_offset, scope=scope, filters=filters)
        if next_offset < len(items)
        else ""
    )
    return sliced, next_token
