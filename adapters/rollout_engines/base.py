"""Rollout engine adapters for fake and optional backends."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import (
    AdapterSupport,
    BaseComponentAdapter,
    ComponentLaunchMetadata,
    LifecycleAction,
    RolloutEngineAdapter,
    SupportReport,
    clone_manifest,
    dependency_available,
    ensure_nonblank,
    manifest_has_explicit_bridge_target,
)
from adapters.control import BridgeKind


class BaseRolloutEngineAdapter(BaseComponentAdapter, RolloutEngineAdapter):
    """Common rollout engine adapter behavior."""

    component_name = ""
    dependency_name: str | None = None
    launch_metadata = ComponentLaunchMetadata(
        component_kind="rollout_engine",
        module_name="adapters.control_cli",
        preferred_bridge_kind=BridgeKind.API_HOOK,
    )
    _supported_actions: tuple[LifecycleAction, ...] = (
        LifecycleAction.VALIDATE,
        LifecycleAction.COMPILE,
        LifecycleAction.PREPARE,
        LifecycleAction.LAUNCH,
        LifecycleAction.STATUS,
        LifecycleAction.PREPARE_PAUSE,
        LifecycleAction.PAUSE,
        LifecycleAction.RESUME,
        LifecycleAction.CHECKPOINT,
        LifecycleAction.STOP,
        LifecycleAction.TERMINATE,
        LifecycleAction.WEIGHT_UPDATE,
        LifecycleAction.SLEEP,
        LifecycleAction.WAKE,
        LifecycleAction.RECREATE,
    )

    def _supported_component_names(self) -> set[str]:
        names = {self.component_name.casefold()}
        if self.component_name.casefold() == "fake":
            names.add("mock")
        return names

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        normalized = self.validate_manifest(manifest)
        if normalized.rollout_engine.casefold() not in self._supported_component_names():
            return SupportReport(
                status=AdapterSupport.UNSUPPORTED,
                summary=(
                    f"manifest targets rollout engine {normalized.rollout_engine}, "
                    f"not {self.component_name}"
                ),
            )
        explicit_bridge = manifest_has_explicit_bridge_target(
            normalized, component="rollout_engine", adapter=self.component_name
        )
        if self.component_name != "fake" and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} requires an explicit rollout-engine bridge",
            )
        if not dependency_available(self.dependency_name) and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} dependency is unavailable",
                diagnostics=(f"install Python module {self.dependency_name}",),
                missing_dependencies=(self.dependency_name,) if self.dependency_name else (),
            )
        safe_point = normalized.execution_contract.safe_point_policy.enabled
        status = AdapterSupport.SUPPORT if safe_point else AdapterSupport.DEGRADED
        summary = (
            f"{self.component_name} supports the manifest"
            if status is AdapterSupport.SUPPORT
            else f"{self.component_name} is available but safe-point protection is disabled"
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(
                f"policy_version={normalized.policy_version or '<unset>'}",
                f"desired_units={max(normalized.desired_units, 1)}",
                f"queue={normalized.queue or 'default'}",
            ),
        )

    def validate_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        normalized = clone_manifest(manifest)
        ensure_nonblank(normalized.manifest_id, "manifest_id")
        ensure_nonblank(normalized.run_id, "run_id")
        ensure_nonblank(normalized.rollout_engine, "rollout_engine")
        ensure_nonblank(normalized.job_id, "job_id")
        ensure_nonblank(normalized.trace_id, "trace_id")
        return normalized

    def _action_extra_args(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> tuple[str, ...]:
        del action
        return (
            "--policy-version",
            manifest.policy_version,
            "--queue",
            manifest.queue or "default",
        )


class FakeRolloutEngineAdapter(BaseRolloutEngineAdapter):
    """Fully functional fake rollout engine adapter."""

    component_name = "fake"
    dependency_name = None
    launch_metadata = ComponentLaunchMetadata(
        component_kind="rollout_engine",
        module_name="adapters.control_cli",
        preferred_bridge_kind=BridgeKind.PYTHON_MODULE,
    )
