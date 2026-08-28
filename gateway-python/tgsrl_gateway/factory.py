"""Backend factory for the gateway."""

from __future__ import annotations

from typing import Any

from tgsrl_gateway.backend import InMemoryGatewayBackend
from tgsrl_gateway.config import GatewayConfig
from tgsrl_gateway.errors import BadRequestError
from tgsrl_gateway.grpc_backend import GrpcGatewayBackend


def build_backend(
    config: GatewayConfig | None = None,
    *,
    backend: Any | None = None,
) -> Any:
    """Build the requested backend.

    An explicitly supplied backend wins. Otherwise the default product path is
    the gRPC backend and the in-memory backend is available only as an
    explicit dev mode.
    """

    if backend is not None:
        return backend
    resolved = config or GatewayConfig.from_env()
    mode = resolved.backend_mode.strip().lower()
    if mode == "grpc":
        return GrpcGatewayBackend(resolved)
    if mode in {"memory", "in-memory", "dev-memory"}:
        return InMemoryGatewayBackend()
    raise BadRequestError(
        "unsupported backend mode", details={"backend_mode": resolved.backend_mode}
    )
