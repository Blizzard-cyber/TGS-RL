"""Execution backend adapters for fake and optional backends."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import (
    AdapterSupport,
    BaseComponentAdapter,
    ComponentLaunchMetadata,
    ExecutionBackendAdapter,
    LifecycleAction,
    SupportReport,
    annotation_flag,
    clone_manifest,
    dependency_available,
    ensure_nonblank,
    manifest_has_explicit_bridge_target,
)
from adapters.control import BridgeKind


class BaseExecutionBackendAdapter(BaseComponentAdapter, ExecutionBackendAdapter):
    """Common execution backend adapter behavior."""

    component_name = ""
    dependency_name: str | None = None
    launch_metadata = ComponentLaunchMetadata(
        component_kind="execution",
        module_name="adapters.control",
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
            names.update({"mock", "inprocessmock", "in-process-mock"})
        return names

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        normalized = self.validate_manifest(manifest)
        if normalized.execution_backend.casefold() not in self._supported_component_names():
            return SupportReport(
                status=AdapterSupport.UNSUPPORTED,
                summary=(
                    f"manifest targets execution backend {normalized.execution_backend}, "
                    f"not {self.component_name}"
                ),
            )
        explicit_bridge = manifest_has_explicit_bridge_target(
            normalized, component="execution", adapter=self.component_name
        )
        if self.component_name != "fake" and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} requires an explicit execution bridge",
            )
        if not dependency_available(self.dependency_name) and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} dependency is unavailable",
                diagnostics=(f"install Python module {self.dependency_name}",),
                missing_dependencies=(self.dependency_name,) if self.dependency_name else (),
            )
        recreate_only = annotation_flag(normalized, "execution.recreate_only")
        status = AdapterSupport.REQUIRES_RECREATE if recreate_only else AdapterSupport.SUPPORT
        summary = (
            f"{self.component_name} requires recreate for runtime mutations"
            if status is AdapterSupport.REQUIRES_RECREATE
            else f"{self.component_name} supports the manifest"
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(
                f"desired_units={max(normalized.desired_units, 1)}",
                f"priority={normalized.priority}",
                f"queue={normalized.queue or 'default'}",
            ),
        )

    def validate_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        normalized = clone_manifest(manifest)
        ensure_nonblank(normalized.manifest_id, "manifest_id")
        ensure_nonblank(normalized.run_id, "run_id")
        ensure_nonblank(normalized.execution_backend, "execution_backend")
        ensure_nonblank(normalized.job_id, "job_id")
        ensure_nonblank(normalized.trace_id, "trace_id")
        return normalized

    def _action_extra_args(
        self, manifest: runtime_pb2.RuntimeManifest, action: LifecycleAction
    ) -> tuple[str, ...]:
        del action
        return (
            "--desired-units",
            str(max(manifest.desired_units, 1)),
            "--queue",
            manifest.queue or "default",
        )


class FakeExecutionBackendAdapter(BaseExecutionBackendAdapter):
    """Fully functional fake execution backend adapter."""

    component_name = "fake"
    dependency_name = None
