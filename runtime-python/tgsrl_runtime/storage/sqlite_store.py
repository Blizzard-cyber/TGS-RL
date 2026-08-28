"""Compatibility wrapper exporting the sqlite storage public API."""

from tgsrl_runtime.storage.gateway_repository import GatewayRepository
from tgsrl_runtime.storage.runtime_repository import RuntimeRepository
from tgsrl_runtime.storage.store_core import SQLiteStore
from tgsrl_runtime.storage.types import (
    AdapterHydrationState,
    DeleteAudit,
    IntentVersionConflict,
    Page,
    ReplayScheduleStep,
    RuntimeObservationBatch,
    RuntimeObservationCommit,
    RuntimeRecoveryState,
    StorageCorruptionError,
)

__all__ = [
    "AdapterHydrationState",
    "DeleteAudit",
    "GatewayRepository",
    "IntentVersionConflict",
    "Page",
    "ReplayScheduleStep",
    "RuntimeObservationBatch",
    "RuntimeObservationCommit",
    "RuntimeRecoveryState",
    "RuntimeRepository",
    "SQLiteStore",
    "StorageCorruptionError",
]
