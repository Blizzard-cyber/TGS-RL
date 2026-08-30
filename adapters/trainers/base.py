"""Trainer adapters for fake and optional backends."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import (
    AdapterSupport,
    BaseComponentAdapter,
    ComponentLaunchMetadata,
    LifecycleAction,
    SupportReport,
    TrainerAdapter,
    annotation_flag,
    clone_manifest,
    dependency_available,
    ensure_nonblank,
    manifest_has_explicit_bridge_target,
)
from adapters.control import BridgeKind


class BaseTrainerAdapter(BaseComponentAdapter, TrainerAdapter):
    """Common trainer adapter behavior."""

    component_name = ""
    dependency_name: str | None = None
    launch_metadata = ComponentLaunchMetadata(
        component_kind="trainer",
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
            names.add("mock")
        return names

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        normalized = self.validate_manifest(manifest)
        if normalized.trainer.casefold() not in self._supported_component_names():
            return SupportReport(
                status=AdapterSupport.UNSUPPORTED,
                summary=f"manifest targets trainer {normalized.trainer}, not {self.component_name}",
            )
        explicit_bridge = manifest_has_explicit_bridge_target(
            normalized, component="trainer", adapter=self.component_name
        )
        if self.component_name != "fake" and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} requires an explicit trainer bridge",
            )
        if not dependency_available(self.dependency_name) and not explicit_bridge:
            return SupportReport(
                status=AdapterSupport.UNAVAILABLE,
                summary=f"{self.component_name} dependency is unavailable",
                diagnostics=(f"install Python module {self.dependency_name}",),
                missing_dependencies=(self.dependency_name,) if self.dependency_name else (),
            )
        status = (
            AdapterSupport.REQUIRES_RECREATE
            if annotation_flag(normalized, "trainer.mutable_state")
            else AdapterSupport.SUPPORT
        )
        summary = (
            f"{self.component_name} trainer requires recreate to mutate state"
            if status is AdapterSupport.REQUIRES_RECREATE
            else f"{self.component_name} trainer supports the manifest"
        )
        return SupportReport(
            status=status,
            summary=summary,
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
        ensure_nonblank(normalized.trainer, "trainer")
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
            "--deterministic-seed",
            str(manifest.deterministic_seed),
        )


class FakeTrainerAdapter(BaseTrainerAdapter):
    """Fully functional fake trainer adapter."""

    component_name = "fake"
    dependency_name = None
