"""Algorithm and rollout-mode adapters for execution contracts."""

from adapters.algorithms import AlgorithmAdapter, GRPOAdapter, PPOAdapter
from adapters.compliance.runtime import (
    AdapterConflictError,
    AdapterErrorKind,
    AdapterSupport,
    AdapterUnavailableError,
    ErrorContract,
    LaunchSpec,
    LifecycleAction,
    LifecycleCall,
    ManifestValidationError,
    RunnerKind,
    RuntimeAdapterError,
    SupportReport,
)
from adapters.contracts import (
    build_execution_contract,
    canonical_contract_id,
    validate_contract_observation,
    validate_execution_contract,
)
from adapters.control import (
    BridgeKind,
    BridgeTarget,
    CommandResult,
    ControlRequest,
    execute_control_argv,
    execute_control_request,
    resolve_bridge_target,
)
from adapters.execution import (
    BaseExecutionBackendAdapter,
    FakeExecutionBackendAdapter,
    RayExecutionBackendAdapter,
)
from adapters.frameworks import (
    BaseFrameworkAdapter,
    FakeFrameworkAdapter,
    OpenRLHFFrameworkAdapter,
    VerlFrameworkAdapter,
)
from adapters.rollout_engines import (
    BaseRolloutEngineAdapter,
    FakeRolloutEngineAdapter,
    SGLangRolloutEngineAdapter,
    VLLMRolloutEngineAdapter,
)
from adapters.rollout_modes import (
    FullAsyncRolloutAdapter,
    PartialAsyncRolloutAdapter,
    RolloutModeAdapter,
    SyncRolloutAdapter,
)
from adapters.runtime_registry import RuntimeAdapterBundle, RuntimeAdapterRegistry
from adapters.trainers import BaseTrainerAdapter, FakeTrainerAdapter, PyTorchTrainerAdapter

__all__ = [
    "AdapterConflictError",
    "AdapterErrorKind",
    "AdapterSupport",
    "AdapterUnavailableError",
    "AlgorithmAdapter",
    "BaseExecutionBackendAdapter",
    "BaseFrameworkAdapter",
    "BaseRolloutEngineAdapter",
    "BaseTrainerAdapter",
    "BridgeKind",
    "BridgeTarget",
    "CommandResult",
    "ControlRequest",
    "ErrorContract",
    "FakeExecutionBackendAdapter",
    "FakeFrameworkAdapter",
    "FakeRolloutEngineAdapter",
    "FakeTrainerAdapter",
    "FullAsyncRolloutAdapter",
    "GRPOAdapter",
    "LaunchSpec",
    "LifecycleAction",
    "LifecycleCall",
    "ManifestValidationError",
    "OpenRLHFFrameworkAdapter",
    "PPOAdapter",
    "PartialAsyncRolloutAdapter",
    "PyTorchTrainerAdapter",
    "RayExecutionBackendAdapter",
    "RolloutModeAdapter",
    "RunnerKind",
    "RuntimeAdapterBundle",
    "RuntimeAdapterError",
    "RuntimeAdapterRegistry",
    "SGLangRolloutEngineAdapter",
    "SupportReport",
    "SyncRolloutAdapter",
    "VLLMRolloutEngineAdapter",
    "VerlFrameworkAdapter",
    "build_execution_contract",
    "canonical_contract_id",
    "execute_control_argv",
    "execute_control_request",
    "resolve_bridge_target",
    "validate_contract_observation",
    "validate_execution_contract",
]
