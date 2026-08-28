"""Async client for Runtime-to-Operator backend lifecycle control."""

from __future__ import annotations

from typing import Any

import grpc
from tgsrl.v1 import operator_pb2, operator_pb2_grpc


class OperatorClient:
    """Own one optional channel to ``RuntimeBackendControlService``."""

    def __init__(
        self, target: str | None = None, *, stub: Any | None = None, timeout: float = 5.0
    ) -> None:
        if stub is None and not target:
            raise ValueError("target is required when no stub is supplied")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._channel: grpc.aio.Channel | None = None
        if stub is None:
            self._channel = grpc.aio.insecure_channel(target or "")
            stub = operator_pb2_grpc.RuntimeBackendControlServiceStub(self._channel)
        self._stub = stub
        self._timeout = timeout

    async def close(self) -> None:
        """Close the owned channel; injected stubs have no channel to close."""
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    async def apply_runtime_control(
        self, request: operator_pb2.ApplyRuntimeControlRequest
    ) -> operator_pb2.ApplyRuntimeControlResponse:
        """Submit exactly one typed backend lifecycle request."""
        return await self._stub.ApplyRuntimeControl(request, timeout=self._timeout)
