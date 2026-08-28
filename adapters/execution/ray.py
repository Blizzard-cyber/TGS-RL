"""Ray execution backend adapter with concrete lifecycle constraints."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import AdapterSupport, SupportReport, annotation_flag
from adapters.execution.base import BaseExecutionBackendAdapter


class RayExecutionBackendAdapter(BaseExecutionBackendAdapter):
    """Ray execution backend adapter."""

    component_name = "ray"
    dependency_name = "ray"

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        report = super().describe_support(manifest)
        if report.status in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}:
            return report
        normalized = self.validate_manifest(manifest)
        if annotation_flag(normalized, "execution.recreate_only"):
            return report
        status = (
            AdapterSupport.REQUIRES_RECREATE
            if normalized.desired_units > 1
            else AdapterSupport.SUPPORT
        )
        summary = (
            "ray scale-out mutations require recreate to preserve placement determinism"
            if status is AdapterSupport.REQUIRES_RECREATE
            else "ray supports the manifest"
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(*report.diagnostics, f"desired_units={max(normalized.desired_units, 1)}"),
            missing_dependencies=report.missing_dependencies,
        )
