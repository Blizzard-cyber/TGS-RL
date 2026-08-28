"""veRL framework adapter with concrete lifecycle constraints."""

from tgsrl.v1 import runtime_pb2, trace_pb2

from adapters.compliance.runtime import AdapterSupport, SupportReport
from adapters.frameworks.base import BaseFrameworkAdapter


class VerlFrameworkAdapter(BaseFrameworkAdapter):
    """veRL framework adapter."""

    component_name = "verl"
    dependency_name = "verl"

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        report = super().describe_support(manifest)
        if report.status in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}:
            return report
        normalized = self.validate_manifest(manifest)
        status = (
            AdapterSupport.SUPPORT
            if normalized.rollout_mode == trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC
            else AdapterSupport.DEGRADED
        )
        summary = (
            "verl supports the manifest"
            if status is AdapterSupport.SUPPORT
            else "verl prefers partially async rollout mode"
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(*report.diagnostics, f"rollout_mode={normalized.rollout_mode}"),
            missing_dependencies=report.missing_dependencies,
        )
