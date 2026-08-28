from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from google.protobuf import timestamp_pb2
from tgsrl.v1 import control_pb2, execution_pb2, runtime_pb2, trace_pb2
from tgsrl_runtime.persistence_adapter import (
    ClosablePersistenceHook,
    NullPersistenceHook,
    SQLitePersistenceHook,
)
from tgsrl_runtime.storage import RuntimeObservationBatch


def _timestamp(value: datetime) -> timestamp_pb2.Timestamp:
    result = timestamp_pb2.Timestamp()
    result.FromDatetime(value)
    return result


def _batch(*, run_id: str = "run-1", suffix: str = "1") -> RuntimeObservationBatch:
    sandbox = runtime_pb2.Sandbox(
        sandbox_id=f"sandbox-{suffix}",
        run_id=run_id,
        job_id=f"job-{suffix}",
        trace_id=f"trace-{suffix}",
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        generation=1,
        observed_at=_timestamp(datetime(2026, 8, 27, 12, 0, 1, tzinfo=UTC)),
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    runtime_unit = runtime_pb2.RuntimeUnit(
        runtime_unit_id=f"unit-{suffix}",
        run_id=run_id,
        job_id=f"job-{suffix}",
        trace_id=f"trace-{suffix}",
        kind=runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        phase_id="decode",
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        stage_id="decode",
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        generation=1,
        observed_at=_timestamp(datetime(2026, 8, 27, 12, 0, 1, tzinfo=UTC)),
    )
    event = runtime_pb2.SandboxEvent(
        event_id=f"event-{suffix}",
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        sandbox_id=f"sandbox-{suffix}",
        run_id=run_id,
        job_id=f"job-{suffix}",
        trace_id=f"trace-{suffix}",
        generation=1,
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        occurred_at=_timestamp(datetime(2026, 8, 27, 12, 0, 1, tzinfo=UTC)),
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    status = control_pb2.ComponentStatus(
        component="runtime",
        health=control_pb2.COMPONENT_HEALTH_PROGRESSING,
        detail="runtime sandboxes have not converged to the requested observed state",
        source="runtime-observation",
        revision=0,
        observed_at=_timestamp(datetime(2026, 8, 27, 12, 0, 1, tzinfo=UTC)),
        job_id=f"job-{suffix}",
        run_id=run_id,
        trace_id=f"trace-{suffix}",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )
    return RuntimeObservationBatch(
        sandbox=sandbox,
        runtime_unit=runtime_unit,
        event=event,
        component_status=status,
    )


def test_null_persistence_hook_returns_consistent_runtime_observation_commit() -> None:
    hook = NullPersistenceHook()

    committed = hook.persist_runtime_observation(_batch())

    assert committed.runtime_event_sequence == 1
    assert committed.component_status.revision == 1
    assert committed.event.event_id == "event-1"
    assert committed.component_status.component == "runtime"


def test_null_persistence_hook_is_closable() -> None:
    hook = NullPersistenceHook()

    assert isinstance(hook, ClosablePersistenceHook)
    hook.close()


def test_sqlite_persistence_hook_returns_real_global_runtime_event_sequence(tmp_path: Path) -> None:
    path = tmp_path / "runtime.sqlite3"
    hook = SQLitePersistenceHook(str(path))
    try:
        first = hook.save_manifest(
            runtime_pb2.RuntimeManifest(
                manifest_id="manifest-1",
                run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
            )
        )
        second = hook.save_manifest(
            runtime_pb2.RuntimeManifest(
                manifest_id="manifest-2",
                run_id="run-2",
                job_id="job-2",
                trace_id="trace-2",
            )
        )
        del first, second

        committed_one = hook.persist_runtime_observation(_batch(run_id="run-1", suffix="1"))
        committed_two = hook.persist_runtime_observation(_batch(run_id="run-2", suffix="2"))

        assert committed_one.runtime_event_sequence == 1
        assert committed_one.component_status.revision == 1
        assert committed_two.runtime_event_sequence == 2
        assert committed_two.component_status.revision == 2
    finally:
        hook.close()


def test_sqlite_persistence_hook_hydrates_all_manifest_pages(tmp_path: Path) -> None:
    path = tmp_path / "runtime.sqlite3"
    hook = SQLitePersistenceHook(str(path))
    try:
        total = 10_001
        for index in range(total):
            hook.save_manifest(
                runtime_pb2.RuntimeManifest(
                    manifest_id=f"manifest-{index:05d}",
                    run_id=f"run-{index:05d}",
                    job_id=f"job-{index:05d}",
                    trace_id=f"trace-{index:05d}",
                )
            )

        hydrated = hook.hydrate_state()

        assert len(hydrated.adapter_state.manifests) == total
        assert hydrated.adapter_state.manifests[0].run_id == "run-00000"
        assert hydrated.adapter_state.manifests[-1].run_id == "run-10000"
    finally:
        hook.close()
