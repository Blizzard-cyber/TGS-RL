"""OpenAPI 3.1 document for the northbound gateway."""

from __future__ import annotations

from tgsrl_gateway.contracts import ROUTE_CONTRACTS, ContractParameter


def _parameter_spec(parameter: ContractParameter) -> dict[str, object]:
    return {
        "name": parameter.name,
        "in": parameter.location,
        "required": parameter.required,
        "schema": parameter.schema,
    }


def build_openapi_spec() -> dict[str, object]:
    json_response = {"content": {"application/json": {"schema": {"type": "object"}}}}
    error_response = {
        "description": "Error response",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ErrorEnvelope"}}},
    }
    paths: dict[str, dict[str, object]] = {}
    tags: set[str] = set()
    for route in ROUTE_CONTRACTS:
        tags.add(route.tag)
        responses: dict[str, object] = {
            str(route.success_status): {
                "description": route.success_description,
                **json_response,
            },
            "default": error_response,
        }
        for status in route.error_statuses:
            responses[str(status)] = error_response
        operation: dict[str, object] = {
            "summary": route.summary,
            "operationId": route.operation_id,
            "tags": [route.tag],
            "responses": responses,
        }
        if route.parameters:
            operation["parameters"] = [_parameter_spec(parameter) for parameter in route.parameters]
        if route.has_request_body:
            operation["requestBody"] = {
                "required": route.request_body_required,
                "content": {"application/json": {"schema": {"type": "object"}}},
            }
        paths.setdefault(route.path_template, {})[route.method.lower()] = operation
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "TGS-RL Northbound Gateway",
            "version": "0.1.0",
            "description": (
                "Gateway API for jobs, operations, runtime topology, decisions, "
                "replays, and experiments."
            ),
        },
        "servers": [{"url": "http://127.0.0.1:8080"}],
        "paths": paths,
        "components": {
            "schemas": {
                "ErrorEnvelope": {
                    "type": "object",
                    "required": ["error"],
                    "properties": {
                        "error": {
                            "type": "object",
                            "required": ["code", "message", "request_id"],
                            "properties": {
                                "code": {"type": "string"},
                                "message": {"type": "string"},
                                "details": {"type": "object", "additionalProperties": True},
                                "request_id": {"type": "string"},
                            },
                        }
                    },
                }
            }
        },
        "tags": [{"name": tag} for tag in sorted(tags)],
    }
