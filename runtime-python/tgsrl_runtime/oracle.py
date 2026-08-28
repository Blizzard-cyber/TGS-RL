"""Runtime observation projection and component-health summaries."""

from __future__ import annotations

from dataclasses import dataclass

from google.protobuf import timestamp_pb2
from tgsrl.v1 import control_pb2, runtime_pb2

from tgsrl_runtime.proto_utils import clone_message


@dataclass(frozen=True, slots=True)
class RuntimeObservationProjection:
    """One deterministic projection of concrete sandbox observations."""

    runtime_units: tuple[runtime_pb2.RuntimeUnit, ...]
    component_status: control_pb2.ComponentStatus


@dataclass(frozen=True)
class RuntimeOracle:
    """Aggregate concrete sandbox targets into logical runtime health."""

    def project(
        self,
        *,
        component: str,
        run_id: str,
        job_id: str,
        trace_id: str,
        runtime_units: list[runtime_pb2.RuntimeUnit],
        sandboxes: list[runtime_pb2.Sandbox],
        revision: int = 0,
        observed_at: timestamp_pb2.Timestamp | None = None,
    ) -> RuntimeObservationProjection:
        """Project all current-generation targets without last-writer-wins state."""
        projected = [clone_message(unit) for unit in runtime_units]
        units_by_id = {unit.runtime_unit_id: unit for unit in projected if unit.runtime_unit_id}
        expected_by_unit: dict[str, int] = {}
        target_set_complete = bool(projected) and len(units_by_id) == len(projected)
        for runtime_unit_id, unit in units_by_id.items():
            try:
                expected_count = int(unit.annotations.get("unit_count", "1"))
            except ValueError:
                target_set_complete = False
                continue
            if expected_count <= 0:
                target_set_complete = False
                continue
            expected_by_unit[runtime_unit_id] = expected_count

        observed_targets: dict[tuple[str, str], runtime_pb2.Sandbox] = {}
        for sandbox in sandboxes:
            runtime_unit_id = sandbox.binding.runtime_unit_id or sandbox.binding.pending_unit_id
            unit = units_by_id.get(runtime_unit_id)
            if unit is None or not sandbox.sandbox_id:
                target_set_complete = False
                continue
            if sandbox.generation != unit.generation:
                continue
            observed_targets[(runtime_unit_id, sandbox.sandbox_id)] = clone_message(sandbox)

        sandboxes_by_unit: dict[str, list[runtime_pb2.Sandbox]] = {
            runtime_unit_id: [] for runtime_unit_id in expected_by_unit
        }
        for (runtime_unit_id, _sandbox_id), sandbox in observed_targets.items():
            sandboxes_by_unit[runtime_unit_id].append(sandbox)
        target_set_complete = target_set_complete and all(
            len(sandboxes_by_unit.get(runtime_unit_id, ())) == expected_count
            for runtime_unit_id, expected_count in expected_by_unit.items()
        )

        for runtime_unit_id, unit in units_by_id.items():
            unit_sandboxes = sandboxes_by_unit.get(runtime_unit_id, [])
            unit_expected_count = expected_by_unit.get(runtime_unit_id)
            if not unit_sandboxes:
                continue
            states = {sandbox.state for sandbox in unit_sandboxes}
            if runtime_pb2.RUNTIME_STATE_FAILED in states:
                unit.state = runtime_pb2.RUNTIME_STATE_FAILED
            elif unit_expected_count == len(unit_sandboxes) and len(states) == 1:
                unit.state = next(iter(states))
            else:
                unit.state = runtime_pb2.RUNTIME_STATE_UNKNOWN
            ordered_sandboxes = sorted(unit_sandboxes, key=lambda item: item.sandbox_id)
            unit.sandbox_id = ordered_sandboxes[0].sandbox_id
            latest = max(
                ordered_sandboxes,
                key=lambda item: (item.observed_at.seconds, item.observed_at.nanos),
            )
            if latest.HasField("observed_at"):
                unit.observed_at.CopyFrom(latest.observed_at)

        observed_sandboxes = list(observed_targets.values())
        failed = any(
            sandbox.state == runtime_pb2.RUNTIME_STATE_FAILED for sandbox in observed_sandboxes
        )
        target_state = self.target_state(projected)
        converged = (
            not failed
            and target_set_complete
            and bool(observed_sandboxes)
            and target_state != runtime_pb2.RUNTIME_STATE_UNKNOWN
            and all(sandbox.state == target_state for sandbox in observed_sandboxes)
        )
        observed_state = (
            runtime_pb2.RUNTIME_STATE_FAILED
            if failed
            else target_state
            if converged
            else runtime_pb2.RUNTIME_STATE_UNKNOWN
        )
        degraded = any(unit.annotations.get("component_support") != "SUPPORT" for unit in projected)
        health = (
            control_pb2.COMPONENT_HEALTH_FAILED
            if failed
            else control_pb2.COMPONENT_HEALTH_PROGRESSING
            if not converged
            else control_pb2.COMPONENT_HEALTH_DEGRADED
            if degraded
            else control_pb2.COMPONENT_HEALTH_HEALTHY
        )
        detail = (
            "at least one runtime sandbox reported failure"
            if failed
            else f"all runtime sandboxes converged to {runtime_pb2.RuntimeState.Name(target_state)}"
            if converged
            else "runtime sandboxes have not converged to the requested observed state"
        )
        status = control_pb2.ComponentStatus(
            component=component,
            health=health,
            detail=detail,
            source="runtime-observation",
            revision=revision,
            job_id=job_id,
            run_id=run_id,
            trace_id=trace_id,
            observed_runtime_state=observed_state,
            converged=converged,
            annotations={
                "runtime.state": runtime_pb2.RuntimeState.Name(observed_state),
                "runtime.target_state": runtime_pb2.RuntimeState.Name(target_state),
                "runtime.converged": str(converged).lower(),
                "runtime.unit_count": str(len(projected)),
                "runtime.target_count": str(sum(expected_by_unit.values())),
                "runtime.observed_count": str(len(observed_sandboxes)),
            },
        )
        if observed_at is not None:
            status.observed_at.CopyFrom(observed_at)
        return RuntimeObservationProjection(
            runtime_units=tuple(projected),
            component_status=status,
        )

    def component_status(
        self,
        *,
        component: str,
        run_id: str,
        job_id: str,
        trace_id: str,
        runtime_units: list[runtime_pb2.RuntimeUnit],
        sandboxes: list[runtime_pb2.Sandbox],
    ) -> control_pb2.ComponentStatus:
        """Compatibility wrapper for callers that only need component status."""
        return self.project(
            component=component,
            run_id=run_id,
            job_id=job_id,
            trace_id=trace_id,
            runtime_units=runtime_units,
            sandboxes=sandboxes,
        ).component_status

    @staticmethod
    def target_state(runtime_units: list[runtime_pb2.RuntimeUnit]) -> int:
        """Translate one consistent desired status reason to its observed target."""
        reasons = {unit.status_reason for unit in runtime_units}
        if len(reasons) != 1:
            return runtime_pb2.RUNTIME_STATE_UNKNOWN
        return {
            "starting": runtime_pb2.RUNTIME_STATE_RUNNING,
            "pausing": runtime_pb2.RUNTIME_STATE_PAUSED,
            "resuming": runtime_pb2.RUNTIME_STATE_RUNNING,
            "stopping": runtime_pb2.RUNTIME_STATE_TERMINATED,
            "terminating": runtime_pb2.RUNTIME_STATE_TERMINATED,
        }.get(next(iter(reasons)), runtime_pb2.RUNTIME_STATE_UNKNOWN)
