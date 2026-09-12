"""WSGI gateway app exposing the northbound HTTP API."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from typing import Any
from urllib.parse import parse_qs
from wsgiref.types import StartResponse, WSGIApplication

from google.protobuf import json_format
from google.protobuf.message import Message
from tgsrl.v1 import experiment_pb2, job_pb2, trace_pb2

from tgsrl_gateway.backend import command_name_to_enum, optional_int, replay_command_name_to_enum
from tgsrl_gateway.config import GatewayConfig
from tgsrl_gateway.contracts import route_path
from tgsrl_gateway.errors import (
    BadRequestError,
    GatewayError,
    MethodNotAllowedError,
    NotFoundError,
)
from tgsrl_gateway.factory import build_backend
from tgsrl_gateway.openapi import build_openapi_spec
from tgsrl_gateway.protojson import message_to_dict, parse_message

MAX_REQUEST_BODY_BYTES = 1 << 20


def _json_response(
    start_response: StartResponse,
    *,
    status: str,
    payload: dict[str, object],
    extra_headers: list[tuple[str, str]] | None = None,
) -> list[bytes]:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    headers = [
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(body))),
    ]
    if extra_headers:
        headers.extend(extra_headers)
    start_response(status, headers)
    return [body]


def _read_json(environ: dict[str, Any]) -> dict[str, object]:
    try:
        length = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError as error:
        raise BadRequestError("invalid Content-Length") from error
    if length < 0:
        raise BadRequestError("Content-Length must not be negative")
    if length > MAX_REQUEST_BODY_BYTES:
        raise BadRequestError("request body exceeds 1 MiB limit")
    raw = environ["wsgi.input"].read(length) if length > 0 else b"{}"
    if not raw.strip():
        return {}
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BadRequestError("request body must be valid JSON") from error
    if not isinstance(data, dict):
        raise BadRequestError("request body must be a JSON object")
    return data


def _request_id(environ: dict[str, Any]) -> str:
    value = environ.get("HTTP_X_REQUEST_ID")
    return value if isinstance(value, str) and value else "request-anonymous"


def _idempotency_key(environ: dict[str, Any]) -> str:
    value = environ.get("HTTP_IDEMPOTENCY_KEY")
    return value if isinstance(value, str) else ""


def _optional_limit(value: str | None) -> int | None:
    limit = optional_int(value)
    if limit is not None and limit < 1:
        raise BadRequestError("limit must be at least 1", details={"limit": limit})
    return limit


def _optional_operation_type(name: str | None) -> int | None:
    if not name:
        return None
    try:
        numeric = int(name)
    except ValueError:
        numeric = None
    descriptor = job_pb2.DESCRIPTOR.pool.FindEnumTypeByName("tgsrl.v1.OperationType")
    if numeric is not None:
        if numeric == 0 or descriptor.values_by_number.get(numeric) is None:
            raise BadRequestError("unsupported operation type", details={"type": name})
        return numeric
    enum_name = f"OPERATION_TYPE_{name.strip().upper().replace('-', '_')}"
    value = descriptor.values_by_name.get(enum_name)
    if value is None or value.number == 0:
        raise BadRequestError("unsupported operation type", details={"type": name})
    return int(value.number)


def _optional_operation_state(name: str | None) -> int | None:
    if not name:
        return None
    try:
        numeric = int(name)
    except ValueError:
        numeric = None
    descriptor = job_pb2.DESCRIPTOR.pool.FindEnumTypeByName("tgsrl.v1.OperationState")
    if numeric is not None:
        if numeric == 0 or descriptor.values_by_number.get(numeric) is None:
            raise BadRequestError("unsupported operation state", details={"state": name})
        return numeric
    enum_name = f"OPERATION_STATE_{name.strip().upper().replace('-', '_')}"
    value = descriptor.values_by_name.get(enum_name)
    if value is None or value.number == 0:
        raise BadRequestError("unsupported operation state", details={"state": name})
    return int(value.number)


def _split_path(path: str) -> list[str]:
    if path == "/":
        return []
    return [segment for segment in path.split("/") if segment]


def _mutually_exclusive_cursor(
    *,
    page_token: str | None,
    after_value: str | None,
    after_name: str,
) -> None:
    if after_value and page_token:
        raise BadRequestError(
            f"{after_name} and page_token are mutually exclusive",
            details={after_name: after_value, "page_token": page_token},
        )


def _normalize_data_kind(name: str | None) -> int | None:
    if not name:
        return None
    normalized = name.strip()
    if not normalized:
        return None
    descriptor = trace_pb2.DESCRIPTOR.pool.FindEnumTypeByName("tgsrl.v1.DataKind")
    try:
        numeric_value = int(normalized)
        if (
            numeric_value == trace_pb2.DATA_KIND_UNKNOWN
            or descriptor.values_by_number.get(numeric_value) is None
        ):
            raise BadRequestError("unsupported data_kind", details={"data_kind": name})
        return numeric_value
    except ValueError:
        pass
    aliases = {
        "live": "DATA_KIND_LIVE",
        "replay": "DATA_KIND_REPLAY",
        "synthetic": "DATA_KIND_SYNTHETIC",
    }
    candidate = aliases.get(normalized.lower(), normalized.upper())
    if not candidate.startswith("DATA_KIND_"):
        candidate = f"DATA_KIND_{candidate}"
    value = descriptor.values_by_name.get(candidate)
    if value is None or value.number == trace_pb2.DATA_KIND_UNKNOWN:
        raise BadRequestError("unsupported data_kind", details={"data_kind": name})
    return int(value.number)


def _path_matches_template(path: str, template: str) -> bool:
    path_segments = _split_path(path)
    template_segments = _split_path(template)
    if len(path_segments) != len(template_segments):
        return False
    for path_segment, template_segment in zip(path_segments, template_segments, strict=True):
        if template_segment.startswith("{") and template_segment.endswith("}"):
            continue
        if path_segment != template_segment:
            return False
    return True


def _allowed_methods_for_path(path: str, spec: dict[str, object]) -> list[str]:
    allowed: set[str] = set()
    paths = spec.get("paths", {})
    if not isinstance(paths, dict):
        return []
    for template, operations in paths.items():
        if not isinstance(template, str) or not _path_matches_template(path, template):
            continue
        if not isinstance(operations, dict):
            continue
        for operation_method in operations:
            if isinstance(operation_method, str):
                allowed.add(operation_method.upper())
    return sorted(allowed)


class GatewayApplication:
    """Simple dependency-free WSGI app."""

    def __init__(self, backend: Any | None = None, *, config: GatewayConfig | None = None) -> None:
        self.config = config or GatewayConfig.from_env()
        self.backend = build_backend(self.config, backend=backend)
        self.openapi = build_openapi_spec()

    def __call__(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        try:
            result = self._dispatch(environ)
            status_value = result.pop("_status", 200)
            status_code = int(status_value) if isinstance(status_value, (int, str)) else 200
            status = f"{status_code} {'OK' if status_code < 400 else 'ERROR'}"
            return _json_response(start_response, status=status, payload=result)
        except GatewayError as error:
            status = f"{error.status} ERROR"
            return _json_response(
                start_response,
                status=status,
                payload=error.to_dict(_request_id(environ)),
                extra_headers=list(error.headers.items()),
            )
        except Exception as error:  # pragma: no cover - defensive fallback
            gateway_error = GatewayError(
                code="internal_error",
                message="internal server error",
                status=500,
                details={"exception": error.__class__.__name__},
            )
            return _json_response(
                start_response,
                status="500 ERROR",
                payload=gateway_error.to_dict(_request_id(environ)),
            )

    def _dispatch(self, environ: dict[str, Any]) -> dict[str, object]:
        method = environ["REQUEST_METHOD"].upper()
        path = environ.get("PATH_INFO", "")
        query = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        segments = _split_path(path)

        if method == "GET" and path == route_path("health"):
            return self.backend.health()
        if method == "GET" and path == route_path("openapi"):
            return self.openapi
        if method == "GET" and path == route_path("capabilities"):
            return self.backend.capabilities()
        if method == "GET" and path == route_path("get_resources"):
            result = self.backend.get_resources()
            return {"snapshot": message_to_dict(result["snapshot"])}

        if not segments or segments[0] != "v1":
            raise NotFoundError("route", path)

        if segments[1:] == ["jobs"] and method == "GET":
            page_token = query.get("page_token", [None])[0]
            after_job_id = query.get("after_job_id", [None])[0]
            _mutually_exclusive_cursor(
                page_token=page_token,
                after_value=after_job_id,
                after_name="after_job_id",
            )
            return self.backend.list_jobs_paginated(
                limit=_optional_limit(query.get("limit", [None])[0]),
                page_token=page_token,
                after_job_id=after_job_id,
                data_kind=_normalize_data_kind(query.get("data_kind", [None])[0]),
            )
        if segments[1:] == ["jobs"] and method == "POST":
            payload = _read_json(environ)
            job = parse_message(payload, job_pb2.RLTrainingJob())
            result = self.backend.create_job(
                job,
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )
            result["_status"] = 201
            return result
        if segments[1:] == ["jobs", "validate"] and method == "POST":
            payload = _read_json(environ)
            job = parse_message(payload, job_pb2.RLTrainingJob())
            return self.backend.validate_job(
                job,
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )
        if len(segments) == 3 and segments[1] == "jobs" and method == "GET":
            return {"job": self.backend.get_job(segments[2])}
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "admit"
            and method == "POST"
        ):
            payload = _read_json(environ)
            return self.backend.admit_job(
                segments[2],
                reason=str(payload.get("reason", "job admitted")),
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "runs"
            and method == "GET"
        ):
            page_token = query.get("page_token", [None])[0]
            after_run_id = query.get("after_run_id", [None])[0]
            _mutually_exclusive_cursor(
                page_token=page_token,
                after_value=after_run_id,
                after_name="after_run_id",
            )
            return self.backend.list_runs(
                segments[2],
                limit=_optional_limit(query.get("limit", [None])[0]),
                page_token=page_token,
                after_run_id=after_run_id,
            )
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "runs"
            and method == "POST"
        ):
            result = self.backend.create_job_run(
                segments[2],
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )
            result["_status"] = 201
            return result
        if (
            len(segments) == 5
            and segments[1] == "jobs"
            and segments[3] == "runs"
            and method == "GET"
        ):
            return {"run": self.backend.get_run(segments[2], segments[4])}
        if (
            len(segments) == 7
            and segments[1] == "jobs"
            and segments[3] == "runs"
            and segments[5] == "commands"
            and method == "POST"
        ):
            payload = _read_json(environ)
            return self.backend.apply_job_command(
                segments[2],
                segments[4],
                command_name_to_enum(segments[6]),
                actor=str(payload.get("actor", "gateway-cli")),
                reason=str(payload.get("reason", segments[6])),
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "timeline"
            and method == "GET"
        ):
            after_event_id = query.get("after_event_id", [None])[0]
            page_token = query.get("page_token", [None])[0]
            _mutually_exclusive_cursor(
                page_token=page_token,
                after_value=after_event_id,
                after_name="after_event_id",
            )
            return self.backend.list_timeline(
                segments[2],
                run_id=query.get("run_id", [None])[0],
                after_event_id=after_event_id,
                page_token=page_token,
                limit=_optional_limit(query.get("limit", [None])[0]),
            )
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "traces"
            and method == "GET"
        ):
            return self.backend.list_traces(
                segments[2],
                run_id=query.get("run_id", [None])[0],
                trace_id=query.get("trace_id", [None])[0],
                data_kind=_normalize_data_kind(query.get("data_kind", [None])[0]),
                page_token=query.get("page_token", [None])[0],
                limit=_optional_limit(query.get("limit", [None])[0]),
            )
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "dag"
            and method == "GET"
        ):
            result = self.backend.get_dag(segments[2], run_id=query.get("run_id", [None])[0])
            return {
                "job": message_to_dict(result["job"]),
                "run": message_to_dict(result["run"]) if result["run"] is not None else None,
                "phase_graph": message_to_dict(result["phase_graph"]),
                "dependencies": result["dependencies"],
            }
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "topology"
            and method == "GET"
        ):
            result = self.backend.get_topology(segments[2], run_id=query.get("run_id", [None])[0])
            return {
                "run": result["run"],
                "manifest": result["manifest"],
                "runtime_units": result["runtime_units"],
                "sandboxes": result["sandboxes"],
                "decision": result["decision"],
            }
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "sandboxes"
            and method == "GET"
        ):
            return self.backend.list_sandboxes(
                segments[2],
                run_id=query.get("run_id", [None])[0],
                limit=_optional_limit(query.get("limit", [None])[0]),
                page_token=query.get("page_token", [None])[0],
            )
        if (
            len(segments) == 4
            and segments[1] == "jobs"
            and segments[3] == "decisions"
            and method == "GET"
        ):
            return self.backend.list_decisions(
                segments[2],
                run_id=query.get("run_id", [None])[0],
                limit=_optional_limit(query.get("limit", [None])[0]),
                page_token=query.get("page_token", [None])[0],
            )
        if (
            len(segments) == 5
            and segments[1] == "jobs"
            and segments[3] == "decisions"
            and method == "GET"
        ):
            return {"decision": self.backend.get_decision(segments[2], segments[4])}

        if segments[1:] == ["operations"] and method == "GET":
            return self.backend.list_operations(
                job_id=query.get("job_id", [None])[0],
                run_id=query.get("run_id", [None])[0],
                operation_type=_optional_operation_type(query.get("type", [None])[0]),
                state=_optional_operation_state(query.get("state", [None])[0]),
                limit=_optional_limit(query.get("limit", [None])[0]),
                page_token=query.get("page_token", [None])[0],
            )
        if len(segments) == 3 and segments[1] == "operations" and method == "GET":
            return {"operation": self.backend.get_operation(segments[2])}

        if segments[1:] == ["replays"] and method == "GET":
            page_token = query.get("page_token", [None])[0]
            after_replay_id = query.get("after_replay_id", [None])[0]
            _mutually_exclusive_cursor(
                page_token=page_token,
                after_value=after_replay_id,
                after_name="after_replay_id",
            )
            return self.backend.list_replays(
                limit=_optional_limit(query.get("limit", [None])[0]),
                page_token=page_token,
                after_replay_id=after_replay_id,
            )
        if segments[1:] == ["replays"] and method == "POST":
            payload = _read_json(environ)
            replay = parse_message(payload, experiment_pb2.Replay())
            result = self.backend.create_replay(
                replay,
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )
            result["_status"] = 201
            return result
        if len(segments) == 3 and segments[1] == "replays" and method == "GET":
            return {"replay": self.backend.get_replay(segments[2])}
        if (
            len(segments) == 5
            and segments[1] == "replays"
            and segments[3] == "commands"
            and method == "POST"
        ):
            return self.backend.apply_replay_command(
                segments[2],
                replay_command_name_to_enum(segments[4]),
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )

        if segments[1:] == ["experiments"] and method == "GET":
            page_token = query.get("page_token", [None])[0]
            after_experiment_id = query.get("after_experiment_id", [None])[0]
            _mutually_exclusive_cursor(
                page_token=page_token,
                after_value=after_experiment_id,
                after_name="after_experiment_id",
            )
            return self.backend.list_experiments(
                limit=_optional_limit(query.get("limit", [None])[0]),
                page_token=page_token,
                after_experiment_id=after_experiment_id,
            )
        if segments[1:] == ["experiments"] and method == "POST":
            payload = _read_json(environ)
            experiment = parse_message(payload, experiment_pb2.Experiment())
            result = self.backend.create_experiment(
                experiment,
                request_id=_request_id(environ),
                idempotency_key=_idempotency_key(environ),
            )
            result["_status"] = 201
            return result
        if len(segments) == 3 and segments[1] == "experiments" and method == "GET":
            return {"experiment": self.backend.get_experiment(segments[2])}

        allow = _allowed_methods_for_path(path, self.openapi)
        if allow:
            raise MethodNotAllowedError(method, path, allow=allow)
        raise NotFoundError("route", path)


def _normalize_payload(payload: object) -> object:
    if isinstance(payload, dict):
        return {
            key: _normalize_payload(value) for key, value in payload.items() if value is not None
        }
    if isinstance(payload, list):
        return [_normalize_payload(value) for value in payload]
    if isinstance(payload, Message):
        return json_format.MessageToDict(
            payload,
            preserving_proto_field_name=False,
            use_integers_for_enums=False,
        )
    return payload


class NormalizingGatewayApplication(GatewayApplication):
    """WSGI app that serializes protobuf messages into JSON dictionaries."""

    def _dispatch(self, environ: dict[str, Any]) -> dict[str, object]:
        result = super()._dispatch(environ)
        status = result.get("_status")
        normalized = _normalize_payload(result)
        if isinstance(normalized, dict) and status is not None:
            normalized["_status"] = status
            return normalized
        if isinstance(normalized, dict):
            return normalized
        raise BadRequestError("invalid response payload")


def create_app(
    backend: Any | None = None,
    *,
    config: GatewayConfig | None = None,
) -> NormalizingGatewayApplication:
    return NormalizingGatewayApplication(backend=backend, config=config)


def main(argv: list[str] | None = None) -> int:
    from wsgiref.simple_server import make_server

    args = argv or sys.argv[1:]
    host = "127.0.0.1"
    port = 8080
    if len(args) >= 1:
        port = int(args[0])
    app: WSGIApplication = create_app().__call__
    with make_server(host, port, app) as server:
        print(f"gateway listening on http://{host}:{port}")
        server.serve_forever()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
