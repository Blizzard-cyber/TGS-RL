"""Structured API errors and HTTP mapping helpers."""

from dataclasses import dataclass, field


@dataclass(slots=True)
class GatewayError(Exception):
    """Base error that carries stable API error metadata."""

    code: str
    message: str
    status: int
    details: dict[str, object] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)

    def to_dict(self, request_id: str) -> dict[str, object]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
                "request_id": request_id,
            }
        }


class BadRequestError(GatewayError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(
            code="bad_request",
            message=message,
            status=400,
            details=details or {},
        )


class NotFoundError(GatewayError):
    def __init__(self, resource: str, resource_id: str) -> None:
        super().__init__(
            code="not_found",
            message=f"{resource} not found",
            status=404,
            details={"resource": resource, "resource_id": resource_id},
        )


class ConflictError(GatewayError):
    def __init__(self, message: str, *, details: dict[str, object] | None = None) -> None:
        super().__init__(
            code="conflict",
            message=message,
            status=409,
            details=details or {},
        )


class MethodNotAllowedError(GatewayError):
    def __init__(self, method: str, path: str, *, allow: list[str]) -> None:
        super().__init__(
            code="method_not_allowed",
            message="method not allowed",
            status=405,
            details={"method": method, "path": path, "allow": allow},
            headers={"Allow": ", ".join(allow)},
        )
