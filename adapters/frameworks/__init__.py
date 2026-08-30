"""Framework runtime adapters."""

from adapters.frameworks.base import BaseFrameworkAdapter, FakeFrameworkAdapter
from adapters.frameworks.openrlhf import OpenRLHFFrameworkAdapter
from adapters.frameworks.verl import VerlFrameworkAdapter
from adapters.frameworks.verl_bridge import (
    ReferenceCallbacks,
    VerlWorkerBridge,
    WorkerCallbacks,
    WorkerIdentity,
    request_worker,
)

__all__ = [
    "BaseFrameworkAdapter",
    "FakeFrameworkAdapter",
    "OpenRLHFFrameworkAdapter",
    "ReferenceCallbacks",
    "VerlFrameworkAdapter",
    "VerlWorkerBridge",
    "WorkerCallbacks",
    "WorkerIdentity",
    "request_worker",
]
