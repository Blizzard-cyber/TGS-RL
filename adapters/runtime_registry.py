"""Deterministic adapter registry and manifest-level runtime compilation."""

from __future__ import annotations

from dataclasses import dataclass

from tgsrl.v1 import execution_pb2, job_pb2, resource_pb2, runtime_pb2, trace_pb2

from adapters.compliance.runtime import (
    ExecutionBackendAdapter,
    FrameworkAdapter,
    RolloutEngineAdapter,
    SupportReport,
    TrainerAdapter,
)
from adapters.execution import FakeExecutionBackendAdapter, RayExecutionBackendAdapter
from adapters.frameworks import FakeFrameworkAdapter, OpenRLHFFrameworkAdapter, VerlFrameworkAdapter
from adapters.rollout_engines import (
    FakeRolloutEngineAdapter,
    SGLangRolloutEngineAdapter,
    VLLMRolloutEngineAdapter,
)
from adapters.trainers import FakeTrainerAdapter, PyTorchTrainerAdapter


def _clone_manifest(manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
    clone = runtime_pb2.RuntimeManifest()
    clone.CopyFrom(manifest)
    return clone


def runtime_manifest_from_job(job: job_pb2.RLTrainingJob) -> runtime_pb2.RuntimeManifest:
    """Build a runtime manifest from the latest job/runtime product inputs."""
    runtime = job.runtime
    semantic_context = trace_pb2.SemanticEnvelope(
        envelope_id=f"manifest:{job.job_id}",
        schema_version="v1",
        type_name="tgsrl.v1.RuntimeManifest",
    )
    return runtime_pb2.RuntimeManifest(
        manifest_id=f"manifest:{job.job_id}",
        run_id=f"run:{job.job_id}",
        job_id=job.job_id,
        trace_id=f"trace:{job.job_id}",
        framework=runtime.framework or "fake",
        execution_backend=runtime.execution_backend or "fake",
        trainer=runtime.trainer or "fake",
        rollout_engine=runtime.rollout_engine or "fake",
        image_digests=[runtime.image_digest] if runtime.image_digest else [],
        patch_set=list(runtime.patch_set),
        compatibility_profile=runtime.compatibility_profile,
        annotations=dict(runtime.annotations) | {"algorithm": job.algorithm},
        data_kind=job.data_kind,
        semantic_context=semantic_context,
        execution_contract=job.execution_contract,
        resources_per_unit=job.resources_per_unit,
        required_capabilities=job.required_capabilities,
        desired_units=job.desired_units or 1,
        priority=job.priority,
        queue=job.queue,
        rollout_mode=job.rollout_mode,
        policy_version=job.policy_ref,
        command=list(runtime.command),
        args=list(runtime.args),
        environment=dict(runtime.environment),
        working_directory=runtime.working_directory,
    )


def _component_capabilities(
    *,
    component: str,
    adapter_name: str,
    report: SupportReport,
    algorithm_names: tuple[str, ...] = (),
    rollout_modes: tuple[str, ...] = (),
) -> resource_pb2.CapabilitySet:
    return resource_pb2.CapabilitySet(
        names=[component, adapter_name, report.status],
        algorithms=list(algorithm_names),
        rollout_modes=list(rollout_modes),
        source="runtime-adapter",
        revision=1,
        supported_actions=["bind", "pause", "resume", "checkpoint", "release", "terminate"],
        attributes={
            "component": component,
            "adapter": adapter_name,
            "support_status": report.status,
            "summary": report.summary,
        },
        evidence=[
            resource_pb2.CapabilityEvidence(
                evidence_id=f"{component}-{adapter_name}-support",
                source="runtime-adapter",
                revision=1,
                collector="runtime_registry",
                detail="; ".join(report.diagnostics) if report.diagnostics else report.summary,
            )
        ],
    )


def _unit_kind_for_phase(phase_kind: int) -> int:
    return {
        execution_pb2.PHASE_KIND_PREFILL: runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        execution_pb2.PHASE_KIND_DECODE: runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        execution_pb2.PHASE_KIND_REFERENCE: runtime_pb2.RUNTIME_UNIT_KIND_REFERENCE,
        execution_pb2.PHASE_KIND_REWARD: runtime_pb2.RUNTIME_UNIT_KIND_REWARD,
        execution_pb2.PHASE_KIND_ACTOR: runtime_pb2.RUNTIME_UNIT_KIND_ACTOR,
        execution_pb2.PHASE_KIND_OPTIMIZER: runtime_pb2.RUNTIME_UNIT_KIND_OPTIMIZER,
        execution_pb2.PHASE_KIND_WEIGHT_SYNC: runtime_pb2.RUNTIME_UNIT_KIND_TRAINER,
    }.get(phase_kind, runtime_pb2.RUNTIME_UNIT_KIND_TRAINER)


@dataclass(frozen=True)
class RuntimeAdapterBundle:
    """All adapters required to materialize one manifest."""

    framework: FrameworkAdapter
    execution: ExecutionBackendAdapter
    trainer: TrainerAdapter
    rollout_engine: RolloutEngineAdapter

    def support_reports(self, manifest: runtime_pb2.RuntimeManifest) -> dict[str, SupportReport]:
        return {
            "framework": self.framework.describe_support(manifest),
            "execution": self.execution.describe_support(manifest),
            "trainer": self.trainer.describe_support(manifest),
            "rollout_engine": self.rollout_engine.describe_support(manifest),
        }

    def diagnostics(self, manifest: runtime_pb2.RuntimeManifest) -> list[str]:
        output: list[str] = []
        for component, report in self.support_reports(manifest).items():
            output.append(f"{component}:{report.status}:{report.summary}")
            output.extend(f"{component}:{detail}" for detail in report.diagnostics)
        return output

    def normalized_manifest(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> runtime_pb2.RuntimeManifest:
        normalized = _clone_manifest(manifest)
        normalized.CopyFrom(self.framework.validate_manifest(normalized))
        normalized.CopyFrom(self.execution.validate_manifest(normalized))
        normalized.CopyFrom(self.trainer.validate_manifest(normalized))
        normalized.CopyFrom(self.rollout_engine.validate_manifest(normalized))
        return normalized

    def runtime_units(self, manifest: runtime_pb2.RuntimeManifest) -> list[runtime_pb2.RuntimeUnit]:
        normalized = self.normalized_manifest(manifest)
        support = self.support_reports(normalized)
        units: list[runtime_pb2.RuntimeUnit] = []
        desired_units = max(normalized.desired_units, 1)
        resources = resource_pb2.ResourceVector()
        if normalized.HasField("resources_per_unit"):
            resources.CopyFrom(normalized.resources_per_unit)
        else:
            resources.cpu_millis = 1000
            resources.memory_bytes = 1 << 30
        priority = normalized.priority
        queue = normalized.queue or "default"
        for phase in normalized.execution_contract.phase_graph.phases:
            kind = _unit_kind_for_phase(phase.kind)
            component = (
                "rollout_engine" if kind == runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT else "trainer"
            )
            units.append(
                runtime_pb2.RuntimeUnit(
                    runtime_unit_id=f"{normalized.run_id}:{phase.phase_id}",
                    run_id=normalized.run_id,
                    job_id=normalized.job_id,
                    trace_id=normalized.trace_id,
                    kind=kind,
                    phase_id=phase.phase_id,
                    phase_kind=phase.kind,
                    state=runtime_pb2.RUNTIME_STATE_REQUESTED,
                    generation=1,
                    requested_resources=resources,
                    required_capabilities=(
                        normalized.required_capabilities
                        if normalized.HasField("required_capabilities")
                        else resource_pb2.CapabilitySet()
                    ),
                    execution_id=normalized.run_id,
                    stage_id=phase.phase_id,
                    data_kind=normalized.data_kind,
                    semantic_context=normalized.semantic_context,
                    annotations={
                        "component": component,
                        "component_support": support[component].status,
                        "display_name": phase.display_name,
                        "unit_count": str(desired_units),
                        "priority": str(priority),
                        "queue": queue,
                    },
                )
            )
        return units


class RuntimeAdapterRegistry:
    """Stable component registry for fake and real-boundary adapters."""

    def __init__(self) -> None:
        self._frameworks: dict[str, FrameworkAdapter] = {
            adapter.component_name: adapter
            for adapter in (
                FakeFrameworkAdapter(),
                VerlFrameworkAdapter(),
                OpenRLHFFrameworkAdapter(),
            )
        }
        self._frameworks["mock"] = self._frameworks["fake"]
        self._execution: dict[str, ExecutionBackendAdapter] = {
            adapter.component_name: adapter
            for adapter in (FakeExecutionBackendAdapter(), RayExecutionBackendAdapter())
        }
        self._execution["mock"] = self._execution["fake"]
        self._trainers: dict[str, TrainerAdapter] = {
            adapter.component_name: adapter
            for adapter in (FakeTrainerAdapter(), PyTorchTrainerAdapter())
        }
        self._trainers["mock"] = self._trainers["fake"]
        self._rollout: dict[str, RolloutEngineAdapter] = {
            adapter.component_name: adapter
            for adapter in (
                FakeRolloutEngineAdapter(),
                VLLMRolloutEngineAdapter(),
                SGLangRolloutEngineAdapter(),
            )
        }
        self._rollout["mock"] = self._rollout["fake"]

    def bundle_for(self, manifest: runtime_pb2.RuntimeManifest) -> RuntimeAdapterBundle:
        return RuntimeAdapterBundle(
            framework=self._frameworks[manifest.framework.casefold()],
            execution=self._execution[manifest.execution_backend.casefold()],
            trainer=self._trainers[manifest.trainer.casefold()],
            rollout_engine=self._rollout[manifest.rollout_engine.casefold()],
        )

    def validate(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> tuple[runtime_pb2.RuntimeManifest, list[str]]:
        bundle = self.bundle_for(manifest)
        normalized = bundle.normalized_manifest(manifest)
        return normalized, bundle.diagnostics(normalized)

    def compile(
        self, manifest: runtime_pb2.RuntimeManifest
    ) -> tuple[runtime_pb2.RuntimeManifest, list[runtime_pb2.RuntimeUnit], list[str]]:
        bundle = self.bundle_for(manifest)
        normalized = bundle.normalized_manifest(manifest)
        return normalized, bundle.runtime_units(normalized), bundle.diagnostics(normalized)
