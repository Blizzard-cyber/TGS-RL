"""Small async client for reporting runtime component status to JobControlService."""

from __future__ import annotations

from typing import Any

import grpc
from tgsrl.v1 import control_pb2, control_pb2_grpc


class JobControlClient:
    """Async JobControlService client focused on component-status reporting."""

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
            stub = control_pb2_grpc.JobControlServiceStub(self._channel)
        self._stub = stub
        self._timeout = timeout

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    async def report_component_status(
        self, component_status: control_pb2.ComponentStatus
    ) -> control_pb2.ComponentStatus:
        """Report one already-persisted Runtime observation to JobController."""
        response = await self._stub.ReportComponentStatus(
            control_pb2.ReportComponentStatusRequest(component_status=component_status),
            timeout=self._timeout,
        )
        return response.component_status
