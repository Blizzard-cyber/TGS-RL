"""OpenRLHF framework adapter with concrete lifecycle constraints."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import AdapterSupport, SupportReport, annotation_flag
from adapters.frameworks.base import BaseFrameworkAdapter


class OpenRLHFFrameworkAdapter(BaseFrameworkAdapter):
    """OpenRLHF framework adapter."""

    component_name = "openrlhf"
    dependency_name = "openrlhf"

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        report = super().describe_support(manifest)
        if report.status in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}:
            return report
        normalized = self.validate_manifest(manifest)
        status = (
            AdapterSupport.REQUIRES_RECREATE
            if annotation_flag(normalized, "framework.recreate_on_patch")
            else AdapterSupport.SUPPORT
        )
        summary = (
            "openrlhf requires recreate when framework patching is enabled"
            if status is AdapterSupport.REQUIRES_RECREATE
            else "openrlhf supports the manifest"
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(*report.diagnostics, f"patch_set_size={len(normalized.patch_set)}"),
            missing_dependencies=report.missing_dependencies,
        )
