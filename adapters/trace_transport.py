"""Authenticated worker-to-bootstrap transport for typed runtime traces."""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from threading import RLock
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from tgsrl.v1 import trace_pb2

TRACE_TOKEN_HEADER = "X-TGSRL-Worker-Trace-Token"


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def trace_batch_idempotency_key(batch: trace_pb2.TraceEventBatch) -> str:
    payload = batch.SerializeToString(deterministic=True)
    return "trace-sha256-" + hashlib.sha256(payload).hexdigest()


@dataclass(slots=True)
class RegistryTraceSink:
    """Batch typed events through the local workload bootstrap."""

    endpoint: str
    token: str
    timeout_seconds: float = 5.0
    max_retries: int = 3
    batch_size: int = 16
    sleeper: Callable[[float], None] = time.sleep
    _pending: list[trace_pb2.TraceEvent] = field(default_factory=list, init=False, repr=False)
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    def __post_init__(self) -> None:
        parsed = urlparse(self.endpoint.strip())
        if (
            parsed.scheme != "http"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("worker trace endpoint must be an absolute local HTTP URL")
        try:
            loopback = ipaddress.ip_address(parsed.hostname or "").is_loopback
        except ValueError:
            loopback = (parsed.hostname or "").casefold() == "localhost"
        if not loopback:
            raise ValueError("worker trace endpoint must use a loopback host")
        if not self.token.strip():
            raise ValueError("worker trace token is required")
        if self.timeout_seconds <= 0 or self.max_retries < 0 or self.batch_size <= 0:
            raise ValueError("worker trace timeout, retry count, and batch size are invalid")

    def __call__(self, event: trace_pb2.TraceEvent) -> None:
        with self._lock:
            if len(self._pending) >= self.batch_size:
                self._flush_locked()
            self._pending.append(
                trace_pb2.TraceEvent.FromString(event.SerializeToString(deterministic=True))
            )
            if len(self._pending) >= self.batch_size:
                self._flush_locked()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def close(self) -> None:
        self.flush()

    def _flush_locked(self) -> None:
        if not self._pending:
            return
        first = self._pending[0]
        if any(
            event.execution_id != first.execution_id
            or event.run_id != first.run_id
            or event.trace_id != first.trace_id
            or event.sandbox_id != first.sandbox_id
            or event.generation != first.generation
            or event.data_kind != first.data_kind
            for event in self._pending
        ):
            raise ValueError("worker trace batch spans multiple execution identities")
        batch = trace_pb2.TraceEventBatch(
            execution_id=first.execution_id,
            run_id=first.run_id,
            trace_id=first.trace_id,
            data_kind=first.data_kind,
            first_sequence=min(event.sequence for event in self._pending),
            last_sequence=max(event.sequence for event in self._pending),
            events=self._pending,
        )
        payload = json.dumps(
            {
                "sandbox_id": first.sandbox_id,
                "generation": first.generation,
                "idempotency_key": trace_batch_idempotency_key(batch),
                "batch": base64.b64encode(batch.SerializeToString(deterministic=True)).decode(
                    "ascii"
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            request = Request(
                self.endpoint,
                data=payload,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    TRACE_TOKEN_HEADER: self.token,
                },
            )
            try:
                with build_opener(_RejectRedirects).open(
                    request, timeout=self.timeout_seconds
                ) as response:
                    if 200 <= response.status < 300:
                        result = json.loads(response.read(1 << 16))
                        if (
                            not isinstance(result, dict)
                            or int(result.get("accepted_event_count", -1)) != len(self._pending)
                            or not str(result.get("cursor", "")).strip()
                        ):
                            raise RuntimeError("worker trace endpoint returned an invalid receipt")
                        self._pending.clear()
                        return
                    raise RuntimeError(f"worker trace endpoint returned HTTP {response.status}")
            except HTTPError as error:
                detail = error.read(4096).decode("utf-8", errors="replace").strip()
                if 400 <= error.code < 500:
                    raise RuntimeError(
                        f"worker trace endpoint rejected the batch with HTTP {error.code}: {detail}"
                    ) from error
                last_error = RuntimeError(
                    f"worker trace endpoint returned HTTP {error.code}: {detail}"
                )
            except (OSError, TimeoutError, URLError) as error:
                last_error = error
            if attempt < self.max_retries:
                self.sleeper(min(0.1 * (2**attempt), 1.0))
        raise RuntimeError("worker trace delivery outcome is unknown") from last_error


def registry_trace_sink_from_environment(
    environment: Mapping[str, str] | None = None,
) -> RegistryTraceSink | None:
    values = os.environ if environment is None else environment
    endpoint = str(values.get("TGSRL_WORKER_TRACE_URL", "")).strip()
    token = str(values.get("TGSRL_WORKER_TRACE_TOKEN", "")).strip()
    if not endpoint and not token:
        return None
    if not endpoint or not token:
        raise ValueError("worker trace URL and token must be configured together")
    return RegistryTraceSink(
        endpoint=endpoint,
        token=token,
        timeout_seconds=float(values.get("TGSRL_WORKER_TRACE_TIMEOUT", "5")),
        max_retries=int(values.get("TGSRL_WORKER_TRACE_RETRIES", "3")),
        batch_size=int(values.get("TGSRL_WORKER_TRACE_BATCH_SIZE", "16")),
    )
