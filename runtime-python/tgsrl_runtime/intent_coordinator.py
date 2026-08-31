"""Derive scheduler intents from manifests, units, and trace summaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from tgsrl.v1 import resource_pb2, runtime_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.aggregation import TraceSummary
from tgsrl_runtime.intent import IntentBuilder


def _rollout_mode_for_manifest(manifest: runtime_pb2.RuntimeManifest) -> int:
    if manifest.rollout_mode != trace_pb2.ROLLOUT_MODE_UNKNOWN:
        return manifest.rollout_mode
    extensions = set(manifest.execution_contract.capabilities.extensions)
    if "policy-window:0" in extensions:
        return trace_pb2.ROLLOUT_MODE_SYNC
    if "policy-window:1" in extensions:
        return trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC
    return trace_pb2.ROLLOUT_MODE_FULLY_ASYNC


@dataclass
class IntentCoordinator:
    """Build scheduler intents from runtime-local state."""

    builder: IntentBuilder = field(default_factory=IntentBuilder)

    def restore_versions(self, intents: list[scheduling_pb2.SchedulingIntent]) -> None:
        for intent in intents:
            self.builder.restore_version(intent.execution_id, intent.stage_id, intent.version)

    def build_for_units(
        self,
        manifest: runtime_pb2.RuntimeManifest,
        runtime_units: list[runtime_pb2.RuntimeUnit],
        summary: TraceSummary,
    ) -> list[scheduling_pb2.SchedulingIntent]:
        """Create one immutable scheduling intent per runtime unit."""
        intents: list[scheduling_pb2.SchedulingIntent] = []
        rollout_mode = _rollout_mode_for_manifest(manifest)
        for unit in runtime_units:
            observation = summary.latest_observation_for_stage(unit.stage_id)
            labels = {
                "runtime_unit_id": unit.runtime_unit_id,
                "support_status": unit.annotations.get("component_support", ""),
            }
            preferences = {
                "max_buffer_level": float(summary.max_buffer_level),
                "latest_buffer_level": float(summary.latest_buffer_level),
                "safe_point_count": float(summary.safe_point_count),
            }
            unit_count = max(int(unit.annotations.get("unit_count", "1")), 1)
            priority = int(unit.annotations.get("priority", str(manifest.priority)))
            queue = unit.annotations.get("queue", manifest.queue or "default")
            deterministic_seed = manifest.deterministic_seed
            intents.append(
                self.builder.build(
                    execution_id=unit.execution_id,
                    stage_id=unit.stage_id,
                    job_id=manifest.job_id,
                    contract=manifest.execution_contract,
                    rollout_mode=rollout_mode,
                    phase_kind=unit.phase_kind,
                    policy_version=(
                        observation.policy_version
                        if observation is not None and observation.policy_version
                        else manifest.policy_version or "policy-1"
                    ),
                    ttl=timedelta(seconds=60),
                    resources_per_unit=unit.requested_resources or resource_pb2.ResourceVector(),
                    required_capabilities=unit.required_capabilities
                    or resource_pb2.CapabilitySet(names=["runtime-unit"]),
                    unit_count=unit_count,
                    priority=priority,
                    queue=queue,
                    labels=labels,
                    preferences=preferences,
                    deterministic_seed=deterministic_seed,
                    contract_observation=observation,
                )
            )
        return intents
