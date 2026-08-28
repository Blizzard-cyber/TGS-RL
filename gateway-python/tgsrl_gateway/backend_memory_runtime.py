"""Runtime, topology, and scheduler-view fixture state for the in-memory backend."""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

from tgsrl.v1 import (
    control_pb2,
    execution_pb2,
    job_pb2,
    runtime_pb2,
    scheduling_pb2,
)

from tgsrl_gateway.errors import NotFoundError
from tgsrl_gateway.pagination import paginate
from tgsrl_gateway.protojson import clone_message, timestamp_from_datetime


@dataclass(slots=True)
class MemorySchedulerRuntimeState:
    decision_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))
    runtime_manifest_by_run: dict[str, runtime_pb2.RuntimeManifest] = field(default_factory=dict)
    runtime_units_by_run: dict[str, list[runtime_pb2.RuntimeUnit]] = field(default_factory=dict)
    sandboxes_by_run: dict[str, list[runtime_pb2.Sandbox]] = field(default_factory=dict)
    decisions: dict[str, scheduling_pb2.DecisionRecord] = field(default_factory=dict)
    decision_ids_by_job: dict[str, list[str]] = field(default_factory=dict)

    def next_decision_id(self) -> str:
        return f"decision-{next(self.decision_counter):04d}"

    def build_runtime_view(self, job: job_pb2.RLTrainingJob, run: control_pb2.JobRun) -> None:
        manifest = runtime_pb2.RuntimeManifest(
            manifest_id=f"manifest-{run.run_id}",
            run_id=run.run_id,
            job_id=job.job_id,
            trace_id=run.trace_id,
            framework=job.runtime.framework,
            execution_backend=job.runtime.execution_backend,
            trainer=job.runtime.trainer,
            rollout_engine=job.runtime.rollout_engine,
            image_digests=[job.runtime.image_digest],
            patch_set=list(job.runtime.patch_set),
            compatibility_profile=job.runtime.compatibility_profile,
            data_kind=job.data_kind,
            execution_contract=job.execution_contract,
        )
        self.runtime_manifest_by_run[run.run_id] = manifest
        units = [
            runtime_pb2.RuntimeUnit(
                runtime_unit_id=f"{run.run_id}-trainer-1",
                run_id=run.run_id,
                job_id=job.job_id,
                trace_id=run.trace_id,
                kind=runtime_pb2.RUNTIME_UNIT_KIND_TRAINER,
                phase_id="optimizer",
                phase_kind=execution_pb2.PHASE_KIND_OPTIMIZER,
                state=runtime_pb2.RUNTIME_STATE_REQUESTED,
                generation=1,
                requested_resources=job.resources_per_unit,
                required_capabilities=job.required_capabilities,
                sandbox_id=f"sandbox-{run.run_id}-1",
                execution_id=run.run_id,
                stage_id="optimizer",
                observed_at=timestamp_from_datetime(),
            ),
            runtime_pb2.RuntimeUnit(
                runtime_unit_id=f"{run.run_id}-rollout-1",
                run_id=run.run_id,
                job_id=job.job_id,
                trace_id=run.trace_id,
                kind=runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
                phase_id="decode",
                phase_kind=execution_pb2.PHASE_KIND_DECODE,
                state=runtime_pb2.RUNTIME_STATE_REQUESTED,
                generation=1,
                requested_resources=job.resources_per_unit,
                required_capabilities=job.required_capabilities,
                sandbox_id=f"sandbox-{run.run_id}-2",
                execution_id=run.run_id,
                stage_id="decode",
                observed_at=timestamp_from_datetime(),
            ),
        ]
        sandboxes = [
            runtime_pb2.Sandbox(
                sandbox_id=f"sandbox-{run.run_id}-1",
                run_id=run.run_id,
                job_id=job.job_id,
                trace_id=run.trace_id,
                state=runtime_pb2.RUNTIME_STATE_REQUESTED,
                generation=1,
                binding=scheduling_pb2.Binding(
                    binding_id=f"binding-{run.run_id}-1",
                    pending_unit_id=f"{run.run_id}-trainer-1",
                    device_ids=["cpu-0"],
                    resources=job.resources_per_unit,
                    sandbox_id=f"sandbox-{run.run_id}-1",
                    generation=1,
                ),
                share=1.0,
                priority=job.priority,
                safe_point=True,
                observed_at=timestamp_from_datetime(),
                data_kind=job.data_kind,
            ),
            runtime_pb2.Sandbox(
                sandbox_id=f"sandbox-{run.run_id}-2",
                run_id=run.run_id,
                job_id=job.job_id,
                trace_id=run.trace_id,
                state=runtime_pb2.RUNTIME_STATE_REQUESTED,
                generation=1,
                binding=scheduling_pb2.Binding(
                    binding_id=f"binding-{run.run_id}-2",
                    pending_unit_id=f"{run.run_id}-rollout-1",
                    device_ids=["cpu-1"],
                    resources=job.resources_per_unit,
                    sandbox_id=f"sandbox-{run.run_id}-2",
                    generation=1,
                ),
                share=1.0,
                priority=job.priority,
                safe_point=True,
                observed_at=timestamp_from_datetime(),
                data_kind=job.data_kind,
            ),
        ]
        self.runtime_units_by_run[run.run_id] = units
        self.sandboxes_by_run[run.run_id] = sandboxes
        decision_id = self.next_decision_id()
        decision = scheduling_pb2.DecisionRecord(
            decision_id=decision_id,
            sequence=len(self.decisions) + 1,
            execution_id=run.run_id,
            stage_id="decode",
            intent_version=1,
            snapshot_revision=1,
            selected_plan=scheduling_pb2.PlacementPlan(
                plan_id=f"plan-{decision_id}",
                execution_id=run.run_id,
                stage_id="decode",
                intent_version=1,
                snapshot_revision=1,
                bindings=[sandbox.binding for sandbox in sandboxes],
                created_at=timestamp_from_datetime(),
                expires_at=timestamp_from_datetime(),
                decision_id=decision_id,
                run_id=run.run_id,
                trace_id=run.trace_id,
                data_kind=run.data_kind,
            ),
            score=1.0,
            fallback=False,
            data_kind=run.data_kind,
            code_revision="gateway-python-demo",
            decided_at=timestamp_from_datetime(),
            policy_version=run.policy_version,
            deterministic_seed=1,
            config_revision="cfg-1",
            run_id=run.run_id,
            trace_id=run.trace_id,
        )
        self.decisions[decision_id] = decision
        self.decision_ids_by_job.setdefault(job.job_id, []).append(decision_id)

    def get_topology(
        self,
        *,
        job: job_pb2.RLTrainingJob,
        run: control_pb2.JobRun,
    ) -> dict[str, object]:
        manifest = clone_message(self.runtime_manifest_by_run[run.run_id])
        runtime_units = [
            clone_message(unit) for unit in self.runtime_units_by_run.get(run.run_id, [])
        ]
        sandboxes = [
            clone_message(sandbox) for sandbox in self.sandboxes_by_run.get(run.run_id, [])
        ]
        selected_decision = None
        for decision_id in self.decision_ids_by_job.get(job.job_id, []):
            decision = self.decisions[decision_id]
            if decision.run_id == run.run_id:
                selected_decision = clone_message(decision)
                break
        return {
            "run": clone_message(run),
            "manifest": manifest,
            "runtime_units": runtime_units,
            "sandboxes": sandboxes,
            "decision": selected_decision,
        }

    def list_sandboxes(
        self,
        *,
        job_id: str,
        run_ids: list[str],
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        if run_id:
            sandboxes = [
                clone_message(sandbox) for sandbox in self.sandboxes_by_run.get(run_id, [])
            ]
        else:
            sandboxes = []
            for candidate_run_id in run_ids:
                sandboxes.extend(
                    clone_message(sandbox)
                    for sandbox in self.sandboxes_by_run.get(candidate_run_id, [])
                )
        items, next_token = paginate(
            sandboxes,
            page_token=page_token,
            limit=limit,
            scope="sandboxes",
            filters={"job_id": job_id, "run_id": run_id},
        )
        return {"sandboxes": items, "next_page_token": next_token}

    def list_decisions(
        self,
        *,
        job_id: str,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        decisions = [
            clone_message(self.decisions[decision_id])
            for decision_id in self.decision_ids_by_job.get(job_id, [])
        ]
        if run_id:
            decisions = [decision for decision in decisions if decision.run_id == run_id]
        items, next_token = paginate(
            decisions,
            page_token=page_token,
            limit=limit,
            scope="decisions",
            filters={"job_id": job_id, "run_id": run_id},
        )
        return {"decisions": items, "next_page_token": next_token}

    def get_decision(self, *, job_id: str, decision_id: str) -> scheduling_pb2.DecisionRecord:
        if decision_id not in self.decision_ids_by_job.get(job_id, []):
            raise NotFoundError("decision", decision_id)
        return clone_message(self.decisions[decision_id])


@dataclass(slots=True)
class MemoryRuntimeService:
    state: MemorySchedulerRuntimeState

    def register_run(self, job: job_pb2.RLTrainingJob, run: control_pb2.JobRun) -> None:
        self.state.build_runtime_view(job, run)

    def sync_runtime_state(self, run_id: str, state: int) -> None:
        for unit in self.state.runtime_units_by_run.get(run_id, []):
            unit.state = state
            unit.observed_at.CopyFrom(timestamp_from_datetime())
        for sandbox in self.state.sandboxes_by_run.get(run_id, []):
            sandbox.state = state
            sandbox.observed_at.CopyFrom(timestamp_from_datetime())

    def get_topology(
        self,
        *,
        job: job_pb2.RLTrainingJob,
        run: control_pb2.JobRun,
    ) -> dict[str, object]:
        return self.state.get_topology(job=job, run=run)

    def list_sandboxes(
        self,
        *,
        job_id: str,
        run_ids: list[str],
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        return self.state.list_sandboxes(
            job_id=job_id,
            run_ids=run_ids,
            run_id=run_id,
            limit=limit,
            page_token=page_token,
        )

    def list_decisions(
        self,
        *,
        job_id: str,
        run_id: str | None,
        limit: int | None,
        page_token: str | None,
    ) -> dict[str, object]:
        return self.state.list_decisions(
            job_id=job_id,
            run_id=run_id,
            limit=limit,
            page_token=page_token,
        )

    def get_decision(self, *, job_id: str, decision_id: str) -> scheduling_pb2.DecisionRecord:
        return self.state.get_decision(job_id=job_id, decision_id=decision_id)

    def decision_count(self) -> int:
        return len(self.state.decisions)
