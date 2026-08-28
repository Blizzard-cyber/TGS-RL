"""vLLM rollout engine adapter with concrete lifecycle constraints."""

from tgsrl.v1 import runtime_pb2

from adapters.compliance.runtime import AdapterSupport, SupportReport
from adapters.rollout_engines.base import BaseRolloutEngineAdapter


class VLLMRolloutEngineAdapter(BaseRolloutEngineAdapter):
    """vLLM rollout engine adapter."""

    component_name = "vllm"
    dependency_name = "vllm"

    def describe_support(self, manifest: runtime_pb2.RuntimeManifest) -> SupportReport:
        report = super().describe_support(manifest)
        if report.status in {AdapterSupport.UNAVAILABLE, AdapterSupport.UNSUPPORTED}:
            return report
        normalized = self.validate_manifest(manifest)
        status = AdapterSupport.REQUIRES_RECREATE if normalized.desired_units > 1 else report.status
        summary = (
            "vllm requires recreate for multi-unit topology changes"
            if status is AdapterSupport.REQUIRES_RECREATE
            else report.summary
        )
        return SupportReport(
            status=status,
            summary=summary,
            diagnostics=(*report.diagnostics, f"desired_units={max(normalized.desired_units, 1)}"),
            missing_dependencies=report.missing_dependencies,
        )
