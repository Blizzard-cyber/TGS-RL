"""PyTorch trainer adapter with concrete lifecycle constraints."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import AdapterSupport, SupportReport
from adapters.trainers.base import BaseTrainerAdapter


class PyTorchTrainerAdapter(BaseTrainerAdapter):
    """PyTorch trainer adapter."""

    component_name = "pytorch"
    dependency_name = "torch"

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        report = super().describe_support(manifest)
        if report.status in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}:
            return report
        normalized = self.validate_manifest(manifest)
        status = AdapterSupport.DEGRADED if normalized.deterministic_seed == 0 else report.status
        summary = (
            "pytorch supports the manifest"
            if status is report.status
            else "pytorch is available but deterministic_seed is unset"
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(
                *report.diagnostics,
                f"deterministic_seed={normalized.deterministic_seed}",
            ),
            missing_dependencies=report.missing_dependencies,
        )
