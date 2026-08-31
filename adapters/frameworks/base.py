"""Framework adapters for fake and optional runtime backends."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import (
    AdapterSupport,
    BaseComponentAdapter,
    ComponentLaunchMetadata,
    FrameworkAdapter,
    LifecycleAction,
    SupportReport,
    clone_manifest,
    dependency_available,
    ensure_nonblank,
    manifest_has_explicit_bridge_target,
)
from adapters.control import BridgeKind


class BaseFrameworkAdapter(BaseComponentAdapter, FrameworkAdapter):
    """Common framework adapter behavior."""

    component_name = ""
    dependency_name: str | None = None
    launch_metadata = ComponentLaunchMetadata(
        component_kind="framework",
        module_name="adapters.control_cli",
        preferred_bridge_kind=BridgeKind.PYTHON_MODULE,
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
    )

    def _supported_component_names(self) -> set[str]:
        names = {self.component_name.casefold()}
        if self.component_name.casefold() == "fake":
            names.add("mock")
        return names

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        normalized = self.validate_manifest(manifest)
        if normalized.framework.casefold() not in self._supported_component_names():
            return SupportReport(
                status=AdapterSupport.UNSUPPORTED,
                summary=(
                    f"manifest targets framework {normalized.framework}, not {self.component_name}"
                ),
                diagnostics=(
                    f"expected framework={self.component_name}",
                    f"received framework={normalized.framework}",
                ),
            )
        explicit_bridge = manifest_has_explicit_bridge_target(
            normalized, component="framework", adapter=self.component_name
        )
        if self.component_name != "fake" and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} requires an explicit framework bridge",
            )
        if not dependency_available(self.dependency_name) and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} dependency is unavailable",
                diagnostics=(
                    f"manifest framework={normalized.framework}",
                    f"install Python module {self.dependency_name}",
                ),
                missing_dependencies=(self.dependency_name,) if self.dependency_name else (),
            )
        return SupportReport(
            status=AdapterSupport.SUPPORT,
            summary=f"{self.component_name} manifest is supported",
            diagnostics=(
                f"policy_version={normalized.policy_version or '<unset>'}",
                f"deterministic_seed={normalized.deterministic_seed}",
                f"desired_units={max(normalized.desired_units, 1)}",
            ),
        )

    def validate_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        normalized = clone_manifest(manifest)
        ensure_nonblank(normalized.manifest_id, "manifest_id")
        ensure_nonblank(normalized.run_id, "run_id")
        ensure_nonblank(normalized.job_id, "job_id")
        ensure_nonblank(normalized.framework, "framework")
        ensure_nonblank(normalized.trace_id, "trace_id")
        return normalized

    def _action_extra_args(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> tuple[str, ...]:
        del action
        return (
            "--policy-version",
            manifest.policy_version,
            "--desired-units",
            str(max(manifest.desired_units, 1)),
        )


class FakeFrameworkAdapter(BaseFrameworkAdapter):
    """Fully functional local fake framework adapter."""

    component_name = "fake"
    dependency_name = None
