"""Shared runtime-adapter support contracts and validation helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from importlib.util import find_spec
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from tgsrl.v1 import resource_pb2, runtime_pb2

if TYPE_CHECKING:
    from adapters.control import BridgeKind

_SHELL_PROGRAMS = frozenset({"bash", "sh", "zsh", "fish", "dash", "cmd", "powershell", "pwsh"})


class AdapterSupport(StrEnum):
    """Support levels exposed by every runtime adapter."""

    SUPPORT = "SUPPORT"
    DEGRADED = "DEGRADED"
    REQUIRES_RECREATE = "REQUIRES_RECREATE"
    UNAVAILABLE = "UNAVAILABLE"
    UNSUPPORTED = "UNSUPPORTED"


@dataclass(frozen=True)
class SupportReport:
    """Structured support signal with human-readable diagnostics."""

    status: AdapterSupport
    summary: str
    diagnostics: tuple[str, ...] = ()
    missing_dependencies: tuple[str, ...] = ()

    @property
    def available(self) -> bool:
        return self.status not in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}


@dataclass(frozen=True)
class LaunchSpec:
    """A structured process launch description, never a shell string."""

    argv: tuple[str, ...]
    env: tuple[tuple[str, str], ...] = ()
    working_directory: str = ""


class LifecycleAction(StrEnum):
    """Supported lifecycle actions across runtime adapters."""

    VALIDATE = "validate"
    COMPILE = "compile"
    PREPARE = "prepare"
    LAUNCH = "launch"
    STATUS = "status"
    PREPARE_PAUSE = "prepare_pause"
    PAUSE = "pause"
    RESUME = "resume"
    CHECKPOINT = "checkpoint"
    STOP = "stop"
    TERMINATE = "terminate"
    WEIGHT_UPDATE = "weight_update"
    SLEEP = "sleep"
    WAKE = "wake"
    RECREATE = "recreate"


class RunnerKind(StrEnum):
    """Execution primitive needed for one lifecycle action."""

    COMMAND = "command"
    PROCESS = "process"


@dataclass(frozen=True)
class LifecycleStateContract:
    """Expected request, immediate, and eventual runtime states for an action."""

    action: LifecycleAction
    requested_state: int
    immediate_observed_state: int
    eventual_observed_states: tuple[int, ...]
    requires_external_event: bool
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class LifecycleCall:
    """A concrete action invocation and the state contract attached to it."""

    action: LifecycleAction
    launch_spec: LaunchSpec
    state_contract: LifecycleStateContract
    runner_kind: RunnerKind


class AdapterErrorKind(StrEnum):
    """Stable error categories surfaced by adapters."""

    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNAVAILABLE = "UNAVAILABLE"
    CONFLICT = "CONFLICT"
    TRANSIENT = "TRANSIENT"
    BACKEND_FAILURE = "BACKEND_FAILURE"
    INTERNAL = "INTERNAL"


@dataclass(frozen=True)
class ErrorContract:
    """Mapped error contract for one adapter action failure."""

    action: LifecycleAction
    kind: AdapterErrorKind
    retryable: bool
    resulting_state: int
    summary: str


class RuntimeAdapterError(ValueError):
    """Base class for runtime-adapter failures."""


class ManifestValidationError(RuntimeAdapterError):
    """Raised when a manifest is structurally invalid for an adapter."""


class AdapterUnavailableError(RuntimeAdapterError):
    """Raised when an optional adapter is selected without its dependency."""


class AdapterConflictError(RuntimeAdapterError):
    """Raised when a lifecycle action conflicts with runtime state."""


class AdapterUnsupportedActionError(RuntimeAdapterError):
    """Raised when a component does not implement a requested lifecycle action."""


def clone_manifest(manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
    """Return a detached runtime manifest."""
    clone = runtime_pb2.RuntimeManifest()
    clone.CopyFrom(manifest)
    return clone


def ensure_nonblank(value: str, field: str) -> str:
    """Require a canonical nonblank string."""
    if value != value.strip() or not value:
        raise ManifestValidationError(f"{field} must be nonblank and trimmed")
    return value


def ensure_structured_argv(argv: tuple[str, ...], *, field: str = "argv") -> tuple[str, ...]:
    """Reject shell wrappers so adapters emit directly executable argv vectors."""
    if not argv or any(not token.strip() or token != token.strip() for token in argv):
        raise ManifestValidationError(f"{field} must contain trimmed nonblank tokens")
    if argv[0].casefold() in _SHELL_PROGRAMS:
        raise ManifestValidationError(f"{field} must be a structured argv, not a shell wrapper")
    return argv


def dependency_available(module_name: str | None) -> bool:
    """Report whether an optional dependency can be imported."""
    return module_name is None or find_spec(module_name) is not None


def annotation_flag(manifest: runtime_pb2.RuntimeManifest, key: str) -> bool:
    """Interpret common true-ish annotations."""
    value = manifest.annotations.get(key, "").strip().casefold()
    return value in {"1", "true", "yes", "on"}


def runtime_state_contract(action: LifecycleAction) -> LifecycleStateContract:
    """Return the final-product state contract for one lifecycle action."""
    if action is LifecycleAction.VALIDATE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_VALIDATING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_REQUESTED,
            eventual_observed_states=(runtime_pb2.RUNTIME_STATE_REQUESTED,),
            requires_external_event=False,
            notes=("manifest validation is local and must not fabricate runtime side effects",),
        )
    if action is LifecycleAction.COMPILE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_COMPILING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_REQUESTED,
            eventual_observed_states=(runtime_pb2.RUNTIME_STATE_REQUESTED,),
            requires_external_event=False,
            notes=("compile materializes units only and must not allocate sandboxes",),
        )
    if action is LifecycleAction.PREPARE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_PREPARING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_REQUESTED,
            eventual_observed_states=(runtime_pb2.RUNTIME_STATE_REQUESTED,),
            requires_external_event=False,
            notes=(
                "prepare may normalize local runtime inputs",
                "prepare must not fabricate bindings or mark runtime units BOUND",
            ),
        )
    if action is LifecycleAction.LAUNCH:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_STARTING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_REQUESTED,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_BOUND,
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_FAILED,
                runtime_pb2.RUNTIME_STATE_TERMINATED,
            ),
            requires_external_event=True,
            notes=(
                "start publishes provider work but does not fabricate sandbox bindings",
                "runtime stays REQUESTED until a real SandboxEvent advances observed state",
            ),
        )
    if action is LifecycleAction.STATUS:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_REQUESTED,
                runtime_pb2.RUNTIME_STATE_BOUND,
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_PAUSED,
                runtime_pb2.RUNTIME_STATE_SLEEPING,
                runtime_pb2.RUNTIME_STATE_FAILED,
                runtime_pb2.RUNTIME_STATE_TERMINATED,
            ),
            requires_external_event=False,
            notes=("status only observes backend state and must not mutate runtime",),
        )
    if action is LifecycleAction.PREPARE_PAUSE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_PAUSING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_PAUSED,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=(
                "prepare_pause asks workers to reach a safe point without pausing infrastructure",
            ),
        )
    if action is LifecycleAction.PAUSE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_PAUSING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_PAUSED,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=("pause completion is confirmed by backend/provider observation",),
        )
    if action is LifecycleAction.RESUME:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_RESUMING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=("resume completion is confirmed by backend/provider observation",),
        )
    if action is LifecycleAction.CHECKPOINT:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_CHECKPOINTING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_PAUSED,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=("checkpoint completion should be tied to a safe-point or provider callback",),
        )
    if action is LifecycleAction.STOP:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_STOPPING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_SLEEPING,
                runtime_pb2.RUNTIME_STATE_TERMINATED,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=("stop completion is confirmed by backend/provider observation",),
        )
    if action is LifecycleAction.WEIGHT_UPDATE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_RUNNING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_RUNNING,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=("weight updates are observed in-place and should not fabricate pause/resume",),
        )
    if action is LifecycleAction.SLEEP:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_STOPPING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_SLEEPING,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=("sleep is a rollout-engine offload action confirmed by backend observation",),
        )
    if action is LifecycleAction.WAKE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_STARTING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_SLEEPING,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_BOUND,
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=("wake rebinds/offloads through provider flow; running is event-driven",),
        )
    if action is LifecycleAction.RECREATE:
        return LifecycleStateContract(
            action=action,
            requested_state=runtime_pb2.RUNTIME_STATE_TERMINATING,
            immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            eventual_observed_states=(
                runtime_pb2.RUNTIME_STATE_TERMINATED,
                runtime_pb2.RUNTIME_STATE_REQUESTED,
                runtime_pb2.RUNTIME_STATE_BOUND,
                runtime_pb2.RUNTIME_STATE_RUNNING,
                runtime_pb2.RUNTIME_STATE_FAILED,
            ),
            requires_external_event=True,
            notes=(
                "recreate tears down and reissues provider work; "
                "final running state is event-driven",
            ),
        )
    return LifecycleStateContract(
        action=action,
        requested_state=runtime_pb2.RUNTIME_STATE_TERMINATING,
        immediate_observed_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
        eventual_observed_states=(
            runtime_pb2.RUNTIME_STATE_TERMINATED,
            runtime_pb2.RUNTIME_STATE_FAILED,
        ),
        requires_external_event=True,
        notes=("terminate completion is confirmed by backend/provider observation",),
    )


def runtime_env(
    manifest: runtime_pb2.RuntimeManifest,
    *,
    component: str,
    adapter_name: str,
    action: LifecycleAction,
) -> tuple[tuple[str, str], ...]:
    """Build a normalized env view from typed manifest fields."""
    resources = resource_pb2.ResourceVector()
    if manifest.HasField("resources_per_unit"):
        resources.CopyFrom(manifest.resources_per_unit)
    capability_names = ()
    if manifest.HasField("required_capabilities"):
        capability_names = tuple(manifest.required_capabilities.names)
    env = {
        "TGSRL_COMPONENT": component,
        "TGSRL_ADAPTER": adapter_name,
        "TGSRL_ACTION": action.value,
        "TGSRL_MANIFEST_ID": manifest.manifest_id,
        "TGSRL_RUN_ID": manifest.run_id,
        "TGSRL_JOB_ID": manifest.job_id,
        "TGSRL_TRACE_ID": manifest.trace_id,
        "TGSRL_DESIRED_UNITS": str(max(manifest.desired_units, 1)),
        "TGSRL_PRIORITY": str(manifest.priority),
        "TGSRL_QUEUE": manifest.queue or "default",
        "TGSRL_ROLLOUT_MODE": str(manifest.rollout_mode),
        "TGSRL_POLICY_VERSION": manifest.policy_version,
        "TGSRL_DETERMINISTIC_SEED": str(manifest.deterministic_seed),
        "TGSRL_CPU_MILLIS": str(resources.cpu_millis),
        "TGSRL_MEMORY_BYTES": str(resources.memory_bytes),
        "TGSRL_ACCELERATOR_UNITS": str(resources.accelerator_units),
        "TGSRL_REQUIRED_CAPABILITIES": ",".join(capability_names),
    }
    return tuple(sorted(env.items()))


def manifest_has_explicit_bridge_target(
    manifest: runtime_pb2.RuntimeManifest, *, component: str, adapter: str
) -> bool:
    """Return whether a component has an explicit executable bridge."""
    if component == "execution" and any(token.strip() for token in manifest.command):
        return True
    explicit_kinds = {
        "python_module",
        "module",
        "lifecycle_hook",
        "endpoint",
        "api_endpoint",
        "service_endpoint",
        "control_endpoint",
    }
    for artifact in manifest.artifacts:
        if artifact.kind.casefold() not in explicit_kinds:
            continue
        artifact_component = artifact.attributes.get("component", "").strip().casefold()
        artifact_adapter = artifact.attributes.get("adapter", "").strip().casefold()
        if artifact_component == component.casefold() and artifact_adapter == adapter.casefold():
            return True
    return False


@dataclass(frozen=True)
class ComponentLaunchMetadata:
    """Component-family metadata required to build adapter launch specs."""

    component_kind: str
    module_name: str
    preferred_bridge_kind: BridgeKind
    launch_runner_kind: RunnerKind = RunnerKind.PROCESS
    control_runner_kind: RunnerKind = RunnerKind.COMMAND


class BaseComponentAdapter:
    """Shared lifecycle/launch/error scaffolding for all adapter families."""

    component_name = ""
    dependency_name: str | None = None
    binary = "python"
    default_bridge_module = ""
    launch_metadata: ComponentLaunchMetadata
    _supported_actions: tuple[LifecycleAction, ...]

    @property
    def supported_actions(self) -> tuple[LifecycleAction, ...]:
        """Return the stable lifecycle capability contract."""
        return self._supported_actions

    def _supported_component_names(self) -> set[str]:
        return {self.component_name.casefold()}

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        """Report runtime support for the provided manifest."""
        raise NotImplementedError

    def validate_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        """Return a normalized manifest or raise a validation error."""
        raise NotImplementedError

    def _action_extra_args(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> tuple[str, ...]:
        del manifest, action
        return ()

    def build_launch_spec(self, manifest: runtime_pb2.RuntimeManifest) -> LaunchSpec:
        return self.build_action_launch_spec(manifest, LifecycleAction.LAUNCH)

    def _action_argv(
        self,
        manifest: runtime_pb2.RuntimeManifest,
        action: LifecycleAction,
        *,
        extra: Iterable[str] = (),
    ) -> tuple[str, ...]:
        return ensure_structured_argv(
            (
                self.binary,
                "-m",
                self.launch_metadata.module_name,
                "--component",
                self.launch_metadata.component_kind,
                "--adapter",
                self.component_name,
                "--action",
                action.value,
                "--run-id",
                manifest.run_id,
                "--job-id",
                manifest.job_id,
                "--trace-id",
                manifest.trace_id,
                *extra,
            )
        )

    def build_action_launch_spec(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> LaunchSpec:
        report = self.describe_support(manifest)
        if report.status in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}:
            raise AdapterUnavailableError(report.summary)
        normalized = self.validate_manifest(manifest)
        from adapters.control import bridge_target_cli_args, resolve_bridge_target

        bridge_target = resolve_bridge_target(
            normalized,
            component=self.launch_metadata.component_kind,
            adapter=self.component_name,
            action=action,
            default_module_name=(
                "adapters.fake_bridge"
                if self.component_name.casefold() == "fake"
                else self.default_bridge_module
            ),
            preferred_kind=self.launch_metadata.preferred_bridge_kind,
        )
        if self.component_name.casefold() != "fake" and not (
            bridge_target.command_argv or bridge_target.module_name
        ):
            raise AdapterUnavailableError(
                f"{self.component_name} has no executable "
                f"{self.launch_metadata.component_kind} bridge"
            )
        if (
            self.component_name.casefold() != "fake"
            and bridge_target.kind.value == "api_hook"
            and not bridge_target.endpoint
        ):
            raise AdapterUnavailableError(
                f"{self.component_name} {self.launch_metadata.component_kind} API bridge "
                f"for {action.value} is missing endpoint"
            )
        if (
            action is LifecycleAction.LAUNCH
            and bridge_target.kind.value == "command"
            and self.component_name.casefold() != "fake"
        ):
            return LaunchSpec(
                argv=bridge_target.command_argv,
                env=tuple(
                    sorted(
                        {
                            **dict(
                                runtime_env(
                                    normalized,
                                    component=self.launch_metadata.component_kind,
                                    adapter_name=self.component_name,
                                    action=action,
                                )
                            ),
                            **dict(bridge_target.env),
                        }.items()
                    )
                ),
                working_directory=bridge_target.working_directory,
            )
        return LaunchSpec(
            argv=self._action_argv(
                normalized,
                action,
                extra=(
                    *self._action_extra_args(normalized, action),
                    *bridge_target_cli_args(bridge_target),
                ),
            ),
            env=runtime_env(
                normalized,
                component=self.launch_metadata.component_kind,
                adapter_name=self.component_name,
                action=action,
            ),
        )

    def lifecycle_calls(self, manifest: runtime_pb2.RuntimeManifest) -> tuple[LifecycleCall, ...]:
        normalized = self.validate_manifest(manifest)
        return tuple(self.lifecycle_call(normalized, action) for action in self.supported_actions)

    def lifecycle_call(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> LifecycleCall:
        normalized = self.validate_manifest(manifest)
        if action not in self.supported_actions:
            raise AdapterUnsupportedActionError(
                f"{self.component_name} does not support lifecycle action {action.value}"
            )
        return LifecycleCall(
            action=action,
            launch_spec=self.build_action_launch_spec(normalized, action),
            state_contract=runtime_state_contract(action),
            runner_kind=(
                self.launch_metadata.launch_runner_kind
                if action is LifecycleAction.LAUNCH
                else self.launch_metadata.control_runner_kind
            ),
        )

    def map_error(self, action: LifecycleAction, error: Exception) -> ErrorContract:
        return map_runtime_error(action, error)


def map_runtime_error(action: LifecycleAction, error: Exception) -> ErrorContract:
    """Map one adapter failure to a stable product-facing error contract."""
    if isinstance(error, ManifestValidationError):
        return ErrorContract(
            action=action,
            kind=AdapterErrorKind.INVALID_ARGUMENT,
            retryable=False,
            resulting_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            summary=str(error),
        )
    if isinstance(
        error, AdapterUnavailableError | ModuleNotFoundError | ImportError | FileNotFoundError
    ):
        return ErrorContract(
            action=action,
            kind=AdapterErrorKind.UNAVAILABLE,
            retryable=False,
            resulting_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            summary=str(error),
        )
    if isinstance(error, AdapterUnsupportedActionError):
        return ErrorContract(
            action=action,
            kind=AdapterErrorKind.INVALID_ARGUMENT,
            retryable=False,
            resulting_state=runtime_pb2.RUNTIME_STATE_UNKNOWN,
            summary=str(error),
        )
    if isinstance(error, AdapterConflictError):
        return ErrorContract(
            action=action,
            kind=AdapterErrorKind.CONFLICT,
            retryable=False,
            resulting_state=runtime_state_contract(action).immediate_observed_state,
            summary=str(error),
        )
    if isinstance(error, TimeoutError):
        return ErrorContract(
            action=action,
            kind=AdapterErrorKind.TRANSIENT,
            retryable=True,
            resulting_state=runtime_state_contract(action).requested_state,
            summary=str(error),
        )
    if isinstance(error, RuntimeError):
        return ErrorContract(
            action=action,
            kind=AdapterErrorKind.BACKEND_FAILURE,
            retryable=False,
            resulting_state=runtime_pb2.RUNTIME_STATE_FAILED,
            summary=str(error),
        )
    return ErrorContract(
        action=action,
        kind=AdapterErrorKind.INTERNAL,
        retryable=False,
        resulting_state=runtime_pb2.RUNTIME_STATE_FAILED,
        summary=str(error),
    )


@runtime_checkable
class ComponentAdapter(Protocol):
    """Common interface for framework, execution, trainer, and rollout adapters."""

    component_name: str
    dependency_name: str | None

    @property
    def supported_actions(self) -> tuple[LifecycleAction, ...]:
        """Return the lifecycle actions implemented by the adapter."""

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        """Report runtime support for the provided manifest."""

    def validate_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        """Return a normalized manifest or raise a validation error."""

    def build_launch_spec(self, manifest: runtime_pb2.RuntimeManifest) -> LaunchSpec:
        """Return a structured launch description or raise unavailable."""

    def build_action_launch_spec(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> LaunchSpec:
        """Return the structured launch description for one lifecycle action."""

    def lifecycle_calls(self, manifest: runtime_pb2.RuntimeManifest) -> tuple[LifecycleCall, ...]:
        """Return the complete lifecycle call contract for the manifest."""

    def lifecycle_call(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> LifecycleCall:
        """Return the control contract for a single lifecycle action."""

    def map_error(self, action: LifecycleAction, error: Exception) -> ErrorContract:
        """Map one backend failure into the adapter error contract."""


class FrameworkAdapter(ComponentAdapter, Protocol):
    """Framework-specific runtime adapter protocol."""


class RolloutEngineAdapter(ComponentAdapter, Protocol):
    """Rollout engine runtime adapter protocol."""


class ExecutionBackendAdapter(ComponentAdapter, Protocol):
    """Execution backend runtime adapter protocol."""


class TrainerAdapter(ComponentAdapter, Protocol):
    """Trainer runtime adapter protocol."""
