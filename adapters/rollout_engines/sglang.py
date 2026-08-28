"""SGLang rollout engine adapter with concrete lifecycle constraints."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import AdapterSupport, SupportReport
from adapters.rollout_engines.base import BaseRolloutEngineAdapter


class SGLangRolloutEngineAdapter(BaseRolloutEngineAdapter):
    """SGLang rollout engine adapter."""

    component_name = "sglang"
    dependency_name = "sglang"

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        report = super().describe_support(manifest)
        if report.status in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}:
            return report
        normalized = self.validate_manifest(manifest)
        status = AdapterSupport.DEGRADED if normalized.queue == "" else report.status
        summary = (
            report.summary
            if status is report.status
            else "sglang is available but queue is unset for backend isolation"
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(*report.diagnostics, f"queue={normalized.queue or '<unset>'}"),
            missing_dependencies=report.missing_dependencies,
        )
