"""veRL framework adapter backed by the repository-owned lifecycle bridge."""

from tgsrl.v1 import runtime_pb2, trace_pb2

from adapters.compliance.runtime import (
    AdapterSupport,
    LifecycleAction,
    SupportReport,
    manifest_has_explicit_bridge_target,
)
from adapters.frameworks.base import BaseFrameworkAdapter


class VerlFrameworkAdapter(BaseFrameworkAdapter):
    """veRL framework adapter."""

    component_name = "verl"
    # The bridge runs inside or beside the worker environment. The control-plane
    # package therefore does not need to import veRL locally.
    dependency_name = None
    default_bridge_module = "adapters.frameworks.verl_bridge"
    _supported_actions = (
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
    )

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        normalized = self.validate_manifest(manifest)
        if normalized.framework.casefold() != self.component_name:
            return SupportReport(
                status=AdapterSupport.UNSUPPORTED,
                summary=f"manifest targets framework {normalized.framework}, not verl",
            )
        explicit_bridge = manifest_has_explicit_bridge_target(
            normalized, component="framework", adapter=self.component_name
        )
        control_socket = normalized.environment.get("TGSRL_VERL_CONTROL_SOCKET", "").strip()
        supported_mode = normalized.rollout_mode == trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC
        status = (
            AdapterSupport.SUPPORT
            if supported_mode and (explicit_bridge or control_socket)
            else AdapterSupport.DEGRADED
        )
        limitations = []
        if not supported_mode:
            limitations.append("verl prefers partially async rollout mode")
        if not explicit_bridge and not control_socket:
            limitations.append("default bridge requires TGSRL_VERL_CONTROL_SOCKET at execution")
        summary = "verl supports the manifest" if not limitations else "; ".join(limitations)
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(
                "bridge=adapters.frameworks.verl_bridge",
                f"control_socket={'configured' if control_socket else 'runtime-required'}",
                f"rollout_mode={normalized.rollout_mode}",
            ),
        )
