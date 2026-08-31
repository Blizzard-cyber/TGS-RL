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
from adapters.frameworks.verl_runtime import (
    VerlControlHook,
    VerlObservation,
    VerlTrainerCallbacks,
    install_verl_control,
    install_verl_control_from_environment,
    observation_from_metrics,
    worker_identity_from_environment,
)

__all__ = [
    "BaseFrameworkAdapter",
    "FakeFrameworkAdapter",
    "OpenRLHFFrameworkAdapter",
    "ReferenceCallbacks",
    "VerlControlHook",
    "VerlFrameworkAdapter",
    "VerlObservation",
    "VerlTrainerCallbacks",
    "VerlWorkerBridge",
    "WorkerCallbacks",
    "WorkerIdentity",
    "install_verl_control",
    "install_verl_control_from_environment",
    "observation_from_metrics",
    "request_worker",
    "worker_identity_from_environment",
]
