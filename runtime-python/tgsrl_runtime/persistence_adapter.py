"""Persistence hook implementations and explicit hydration for runtime supervisor state."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from tgsrl.v1 import control_pb2, experiment_pb2, runtime_pb2, scheduling_pb2, trace_pb2

from tgsrl_runtime.aggregation import TraceAggregator
from tgsrl_runtime.checkpoints import CheckpointRecord, CheckpointStore
from tgsrl_runtime.experiments import ReplayExperimentStore
from tgsrl_runtime.storage import (
    AdapterHydrationState,
    GatewayRepository,
    ReplayScheduleStep,
    RuntimeObservationBatch,
    RuntimeObservationCommit,
    RuntimeRecoveryState,
    RuntimeRepository,
    SQLiteStore,
)
from tgsrl_runtime.stores import (
    RuntimeManifestStore,
    RuntimeUnitStore,
    SandboxEventStore,
    SandboxStore,
)
from tgsrl_runtime.trace_ingest import TraceIngestor
from tgsrl_runtime.version_store import VersionStore


def _digest_payload(parts: Sequence[str]) -> str:
    material = "\0".join(parts).encode()
    return hashlib.sha256(material).hexdigest()


HYDRATION_PAGE_SIZE = 10_000


@dataclass(frozen=True)
class PersistenceHydration:
    """Serializable persisted runtime state restored into in-memory stores."""

    adapter_state: AdapterHydrationState
    runtime_recoveries: tuple[RuntimeRecoveryState, ...]
    version_counters: tuple[tuple[str, int], ...]


class PersistenceHook(Protocol):
    """Persistence contract for runtime and experiment state surfaces."""

    def save_manifest(self, manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
        """Persist one runtime manifest."""

    def save_runtime_units(
        self, runtime_units: Sequence[runtime_pb2.RuntimeUnit]
    ) -> list[runtime_pb2.RuntimeUnit]:
        """Persist runtime units."""

    def save_sandboxes(self, sandboxes: Sequence[runtime_pb2.Sandbox]) -> list[runtime_pb2.Sandbox]:
        """Persist sandboxes."""

    def record_runtime_event(self, event: runtime_pb2.SandboxEvent) -> None:
        """Persist one sandbox event."""

    def record_trace_batch(self, batch: trace_pb2.TraceEventBatch) -> trace_pb2.TraceEventBatch:
        """Persist one trace batch."""

    def record_intent(self, intent: scheduling_pb2.SchedulingIntent) -> None:
        """Persist one scheduling intent."""

    def save_checkpoint(self, *, run_id: str, checkpoint_ref: str, completed_at: datetime) -> str:
        """Persist checkpoint completion metadata and return a cursor."""

    def record_replay(self, replay: experiment_pb2.Replay) -> None:
        """Persist one replay state."""

    def record_replay_schedule_step(self, step: ReplayScheduleStep) -> ReplayScheduleStep:
        """Persist one completed external scheduler replay step."""

    def list_replay_schedule_steps(
        self, *, replay_id: str, start_key_digest: str
    ) -> list[ReplayScheduleStep]:
        """Load completed external scheduler replay steps for one START."""

    def record_experiment(self, experiment: experiment_pb2.Experiment) -> None:
        """Persist one experiment state."""

    def record_component_status(self, component_status: control_pb2.ComponentStatus) -> None:
        """Persist one component status report."""

    def persist_runtime_observation(
        self,
        batch: RuntimeObservationBatch,
    ) -> RuntimeObservationCommit:
        """Persist one runtime observation atomically and return its committed sequence."""

    def hydrate_state(self) -> PersistenceHydration | None:
        """Return persisted state for in-memory restoration when supported."""


@runtime_checkable
class ClosablePersistenceHook(Protocol):
    """Narrow lifecycle surface for persistence hooks that own durable resources."""

    def close(self) -> None:
        """Release any owned resources."""


class NullPersistenceHook:
    """No-op persistence hook used until the sqlite store is available."""

    def __init__(self) -> None:
        self._runtime_event_sequence = 0

    def save_manifest(self, manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
        return manifest

    def save_runtime_units(
        self, runtime_units: Sequence[runtime_pb2.RuntimeUnit]
    ) -> list[runtime_pb2.RuntimeUnit]:
        return list(runtime_units)

    def save_sandboxes(self, sandboxes: Sequence[runtime_pb2.Sandbox]) -> list[runtime_pb2.Sandbox]:
        return list(sandboxes)

    def record_runtime_event(self, event: runtime_pb2.SandboxEvent) -> None:
        del event

    def record_trace_batch(self, batch: trace_pb2.TraceEventBatch) -> trace_pb2.TraceEventBatch:
        return batch

    def record_intent(self, intent: scheduling_pb2.SchedulingIntent) -> None:
        del intent

    def save_checkpoint(self, *, run_id: str, checkpoint_ref: str, completed_at: datetime) -> str:
        del completed_at
        return f"checkpoint:{run_id}:{checkpoint_ref}"

    def record_replay(self, replay: experiment_pb2.Replay) -> None:
        del replay

    def record_replay_schedule_step(self, step: ReplayScheduleStep) -> ReplayScheduleStep:
        return step

    def list_replay_schedule_steps(
        self, *, replay_id: str, start_key_digest: str
    ) -> list[ReplayScheduleStep]:
        del replay_id, start_key_digest
        return []

    def record_experiment(self, experiment: experiment_pb2.Experiment) -> None:
        del experiment

    def record_component_status(self, component_status: control_pb2.ComponentStatus) -> None:
        del component_status

    def persist_runtime_observation(
        self,
        batch: RuntimeObservationBatch,
    ) -> RuntimeObservationCommit:
        self._runtime_event_sequence += 1
        component_status = control_pb2.ComponentStatus()
        component_status.CopyFrom(batch.component_status)
        component_status.revision = self._runtime_event_sequence
        return RuntimeObservationCommit(
            sandbox=batch.sandbox,
            runtime_unit=batch.runtime_unit,
            event=batch.event,
            component_status=component_status,
            runtime_event_sequence=self._runtime_event_sequence,
        )

    def hydrate_state(self) -> PersistenceHydration | None:
        return None

    def close(self) -> None:
        return None


class SQLitePersistenceHook:
    """Persist runtime and experiment state through sqlite repositories."""

    def __init__(self, path: str) -> None:
        self._store = SQLiteStore(path)
        self._gateway = GatewayRepository(self._store)
        self._runtime = RuntimeRepository(self._store)

    def close(self) -> None:
        self._store.close()

    def save_manifest(self, manifest: runtime_pb2.RuntimeManifest) -> runtime_pb2.RuntimeManifest:
        return self._runtime.save_manifest(manifest)

    def save_runtime_units(
        self, runtime_units: Sequence[runtime_pb2.RuntimeUnit]
    ) -> list[runtime_pb2.RuntimeUnit]:
        return self._runtime.upsert_units(runtime_units)

    def save_sandboxes(self, sandboxes: Sequence[runtime_pb2.Sandbox]) -> list[runtime_pb2.Sandbox]:
        return self._runtime.upsert_sandboxes(sandboxes)

    def record_runtime_event(self, event: runtime_pb2.SandboxEvent) -> None:
        self._runtime.publish_event(event)

    def record_trace_batch(self, batch: trace_pb2.TraceEventBatch) -> trace_pb2.TraceEventBatch:
        return self._gateway.append_trace_batch(batch)

    def record_intent(self, intent: scheduling_pb2.SchedulingIntent) -> None:
        self._gateway.record_intent(intent)

    def save_checkpoint(self, *, run_id: str, checkpoint_ref: str, completed_at: datetime) -> str:
        return self._runtime.save_checkpoint(
            run_id=run_id, checkpoint_ref=checkpoint_ref, completed_at=completed_at
        )

    def record_replay(self, replay: experiment_pb2.Replay) -> None:
        self._gateway.upsert_replay(replay)

    def record_replay_schedule_step(self, step: ReplayScheduleStep) -> ReplayScheduleStep:
        return self._gateway.record_replay_schedule_step(step)

    def list_replay_schedule_steps(
        self, *, replay_id: str, start_key_digest: str
    ) -> list[ReplayScheduleStep]:
        return self._gateway.list_replay_schedule_steps(
            replay_id=replay_id, start_key_digest=start_key_digest
        )

    def record_experiment(self, experiment: experiment_pb2.Experiment) -> None:
        self._gateway.upsert_experiment(experiment)

    def record_component_status(self, component_status: control_pb2.ComponentStatus) -> None:
        self._runtime.upsert_component_statuses([component_status])

    def persist_runtime_observation(
        self,
        batch: RuntimeObservationBatch,
    ) -> RuntimeObservationCommit:
        return self._runtime.persist_runtime_observation(batch)

    def hydrate_state(self) -> PersistenceHydration:
        adapter_state = self._gateway.hydrate_adapter_state(limit_per_kind=HYDRATION_PAGE_SIZE)
        recoveries: list[RuntimeRecoveryState] = []
        version_counters: dict[str, int] = {}
        for manifest in adapter_state.manifests:
            recovery = self._runtime.load_recovery(manifest.run_id)
            recoveries.append(recovery)
            if recovery.checkpoints:
                version_counters["checkpoint"] = max(
                    version_counters.get("checkpoint", 0),
                    len(recovery.checkpoints),
                )
            version_counters[f"run:{manifest.run_id}:generation"] = max(
                version_counters.get(f"run:{manifest.run_id}:generation", 0),
                recovery.latest_generation,
            )
            version_counters[f"run:{manifest.run_id}:cursor"] = max(
                version_counters.get(f"run:{manifest.run_id}:cursor", 0),
                len(recovery.latest_runtime_cursor),
            )
            version_counters["runtime_event_sequence"] = max(
                version_counters.get("runtime_event_sequence", 0),
                max((item.sequence for item in recovery.runtime_events), default=0),
            )
        return PersistenceHydration(
            adapter_state=adapter_state,
            runtime_recoveries=tuple(recoveries),
            version_counters=tuple(sorted(version_counters.items())),
        )


def restore_hydrated_state(
    *,
    hydration: PersistenceHydration,
    manifests: RuntimeManifestStore,
    runtime_units: RuntimeUnitStore,
    sandboxes: SandboxStore,
    sandbox_events: SandboxEventStore,
    trace_ingestor: TraceIngestor,
    checkpoints: CheckpointStore,
    versions: VersionStore,
    experiments_store: ReplayExperimentStore,
    component_statuses: dict[tuple[str, str], control_pb2.ComponentStatus],
    aggregator: TraceAggregator,
) -> None:
    """Restore persisted state into public in-memory stores."""
    if not manifests.is_empty():
        return
    adapter_state = hydration.adapter_state
    recoveries = {recovery.manifest.run_id: recovery for recovery in hydration.runtime_recoveries}
    manifests.restore(adapter_state.manifests, replace=True)
    runtime_units.restore(
        [(run_id, list(recovery.runtime_units)) for run_id, recovery in sorted(recoveries.items())],
        replace=True,
    )
    sandboxes.restore(
        [(run_id, list(recovery.sandboxes)) for run_id, recovery in sorted(recoveries.items())],
        replace=True,
    )
    sandbox_events.restore(
        [
            (
                run_id,
                [(item.sequence, item.event) for item in recovery.runtime_events],
            )
            for run_id, recovery in sorted(recoveries.items())
        ],
        replace=True,
    )
    for batch in adapter_state.trace_batches:
        trace_ingestor.ingest(batch.run_id, list(batch.events))
    for replay in adapter_state.replays:
        experiments_store.put_replay(replay)
    for experiment in adapter_state.experiments:
        experiments_store.put_experiment(experiment)
    checkpoints.restore([], replace=True)
    for run_id, recovery in sorted(recoveries.items()):
        summary = aggregator.summarize(trace_ingestor.list(run_id))
        for checkpoint_ref, completed_at in recovery.checkpoints:
            checkpoints.add(
                CheckpointRecord(
                    checkpoint_ref=checkpoint_ref,
                    run_id=run_id,
                    completed_at=completed_at,
                    state_digest=_digest_payload(
                        [run_id, str(summary.event_count), str(summary.safe_point_count)]
                    ),
                )
            )
    versions.restore(hydration.version_counters, replace=False)
    component_statuses.clear()
    for status in adapter_state.component_statuses:
        component_statuses[(status.run_id, status.component)] = status
