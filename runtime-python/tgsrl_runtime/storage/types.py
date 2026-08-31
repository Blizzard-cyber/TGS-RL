"""Shared storage datatypes and exception classes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from google.protobuf.message import Message
from tgsrl.v1 import control_pb2, experiment_pb2, runtime_pb2, scheduling_pb2, trace_pb2


class StorageCorruptionError(RuntimeError):
    """Raised when persisted bytes cannot be decoded into the expected message."""


class IntentVersionConflict(ValueError):
    """Raised when an intent CAS or idempotency contract is violated."""


@dataclass(frozen=True, slots=True)
class ReplayScheduleStep:
    replay_id: str
    start_key_digest: str
    ordinal: int
    step_digest: str
    decision: scheduling_pb2.DecisionRecord
    completed_at: datetime


@dataclass(frozen=True, slots=True)
class Page[T: Message]:
    """One stable page of typed repository results."""

    items: list[T]
    next_cursor: str


@dataclass(frozen=True, slots=True)
class DeleteAudit:
    scope: str
    cutoff: datetime
    deleted_count: int
    created_at: datetime
    note: str


@dataclass(frozen=True, slots=True)
class SequencedRuntimeEvent:
    sequence: int
    event: runtime_pb2.SandboxEvent


@dataclass(frozen=True, slots=True)
class RuntimeObservationBatch:
    sandbox: runtime_pb2.Sandbox
    runtime_unit: runtime_pb2.RuntimeUnit
    event: runtime_pb2.SandboxEvent
    component_status: control_pb2.ComponentStatus


@dataclass(frozen=True, slots=True)
class RuntimeObservationCommit:
    sandbox: runtime_pb2.Sandbox
    runtime_unit: runtime_pb2.RuntimeUnit
    event: runtime_pb2.SandboxEvent
    component_status: control_pb2.ComponentStatus
    runtime_event_sequence: int


@dataclass(frozen=True, slots=True)
class ManagedWorkerTraceCommit:
    batch: trace_pb2.TraceEventBatch
    intents: list[scheduling_pb2.SchedulingIntent]
    response_payload: bytes
    idempotent: bool


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryState:
    manifest: runtime_pb2.RuntimeManifest
    runtime_units: list[runtime_pb2.RuntimeUnit]
    sandboxes: list[runtime_pb2.Sandbox]
    runtime_events: list[SequencedRuntimeEvent]
    component_statuses: list[control_pb2.ComponentStatus]
    checkpoints: list[tuple[str, datetime]]
    latest_generation: int
    latest_runtime_cursor: str


@dataclass(frozen=True, slots=True)
class AdapterHydrationState:
    intents: list[scheduling_pb2.SchedulingIntent]
    trace_batches: list[trace_pb2.TraceEventBatch]
    manifests: list[runtime_pb2.RuntimeManifest]
    runtime_units: list[runtime_pb2.RuntimeUnit]
    sandboxes: list[runtime_pb2.Sandbox]
    replays: list[experiment_pb2.Replay]
    experiments: list[experiment_pb2.Experiment]
    component_statuses: list[control_pb2.ComponentStatus]
