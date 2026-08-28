"""Small async client around the generated SchedulerService stub."""

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import grpc
from google.protobuf import duration_pb2
from tgsrl.v1 import resource_pb2, scheduling_pb2, scheduling_pb2_grpc


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


class SchedulerClient:
    """Publish intents and consume a cursor-resumable, deduplicated decision stream."""

    def __init__(
        self,
        target: str | None = None,
        *,
        stub: Any | None = None,
        timeout: float = 5.0,
        max_retries: int = 3,
        sleeper: Callable[[float], Awaitable[None]] = _sleep,
    ) -> None:
        if stub is None and not target:
            raise ValueError("target is required when no stub is supplied")
        if timeout <= 0 or max_retries < 0:
            raise ValueError("timeout must be positive and max_retries non-negative")
        self._channel: grpc.aio.Channel | None = None
        if stub is None:
            self._channel = grpc.aio.insecure_channel(target or "")
            stub = scheduling_pb2_grpc.SchedulerServiceStub(self._channel)
        self._stub = stub
        self.timeout = timeout
        self.max_retries = max_retries
        self._sleeper = sleeper

    async def __aenter__(self) -> "SchedulerClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    async def publish_intent(
        self, intent: scheduling_pb2.SchedulingIntent
    ) -> scheduling_pb2.PublishIntentResponse:
        return await self._stub.PublishIntent(
            scheduling_pb2.PublishIntentRequest(intent=intent), timeout=self.timeout
        )

    async def get_snapshot(
        self, *, minimum_revision: int = 0, include_pending_units: bool = True
    ) -> resource_pb2.ClusterSnapshot:
        response = await self._stub.GetSnapshot(
            scheduling_pb2.GetSnapshotRequest(
                minimum_revision=minimum_revision,
                include_pending_units=include_pending_units,
            ),
            timeout=self.timeout,
        )
        snapshot = resource_pb2.ClusterSnapshot()
        snapshot.CopyFrom(response.snapshot)
        return snapshot

    async def schedule(
        self,
        intent: scheduling_pb2.SchedulingIntent,
        snapshot: resource_pb2.ClusterSnapshot,
    ) -> scheduling_pb2.DecisionRecord:
        response = await self._stub.Schedule(
            scheduling_pb2.ScheduleRequest(intent=intent, snapshot=snapshot),
            timeout=self.timeout,
        )
        decision = scheduling_pb2.DecisionRecord()
        decision.CopyFrom(response.decision)
        return decision

    async def watch_decisions(
        self,
        *,
        job_ids: tuple[str, ...] = (),
        after_sequence: int = 0,
        after_decision_id: str = "",
        heartbeat_seconds: float = 10.0,
    ) -> AsyncIterator[scheduling_pb2.DecisionRecord]:
        if heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        if after_sequence and after_decision_id:
            raise ValueError("after_sequence and after_decision_id are alternative cursors")
        heartbeat = duration_pb2.Duration()
        heartbeat.FromMilliseconds(round(heartbeat_seconds * 1000))
        seen: set[str] = set()
        cursor_sequence = after_sequence
        cursor_id = after_decision_id
        attempt = 0
        while True:
            request = scheduling_pb2.WatchDecisionsRequest(
                job_ids=job_ids,
                after_sequence=cursor_sequence,
                heartbeat_interval=heartbeat,
                after_decision_id=cursor_id,
            )
            try:
                stream = self._stub.WatchDecisions(request, timeout=None)
                if inspect.isawaitable(stream):
                    stream = await stream
                async for response in stream:
                    if not response.HasField("decision"):
                        continue
                    decision = response.decision
                    if not decision.decision_id:
                        if decision.sequence:
                            cursor_sequence = max(cursor_sequence, decision.sequence)
                            cursor_id = ""
                        continue
                    if decision.decision_id in seen:
                        continue
                    seen.add(decision.decision_id)
                    cursor_sequence = 0
                    cursor_id = decision.decision_id
                    attempt = 0
                    clone = scheduling_pb2.DecisionRecord()
                    clone.CopyFrom(decision)
                    yield clone
                return
            except grpc.aio.AioRpcError as error:
                retryable = error.code() in {
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                }
                if not retryable or attempt >= self.max_retries:
                    raise
                await self._sleeper(min(0.1 * (2**attempt), 2.0))
                attempt += 1
