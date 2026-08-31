from __future__ import annotations

import sqlite3
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from google.protobuf import timestamp_pb2
from tgsrl.v1 import (
    control_pb2,
    execution_pb2,
    experiment_pb2,
    resource_pb2,
    runtime_pb2,
    scheduling_pb2,
    trace_pb2,
)
from tgsrl_runtime.storage import (
    IntentVersionConflict,
    ReplayScheduleStep,
    RuntimeObservationBatch,
    RuntimeRepository,
    SQLiteStore,
    StorageCorruptionError,
)
from tgsrl_runtime.storage.schema import apply_migrations


def _timestamp(value: datetime) -> timestamp_pb2.Timestamp:
    result = timestamp_pb2.Timestamp()
    result.FromDatetime(value)
    return result


def _intent(version: int, *, idempotency_key: str | None = None) -> scheduling_pb2.SchedulingIntent:
    now = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
    ttl = timedelta(minutes=5)
    return scheduling_pb2.SchedulingIntent(
        execution_id="execution-1",
        stage_id="stage-1",
        version=version,
        valid_until=_timestamp(now + ttl),
        ttl={"seconds": int(ttl.total_seconds())},
        idempotency_key=idempotency_key or f"intent-key-{version}",
        submitted_at=_timestamp(now),
        job_id="job-1",
        run_id="run-1",
        trace_id="trace-1",
        resources_per_unit=resource_pb2.ResourceVector(cpu_millis=500, memory_bytes=1024),
        unit_count=1,
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        rollout_mode=trace_pb2.ROLLOUT_MODE_SYNC,
        policy_version="policy-1",
        execution_contract=execution_pb2.ExecutionContract(
            contract_id="contract-1",
            version="1.0.0",
        ),
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )


def _event(event_id: str, *, sequence: int, second: int) -> trace_pb2.TraceEvent:
    return trace_pb2.TraceEvent(
        event_id=event_id,
        job_id="job-1",
        execution_id="execution-1",
        phase_id="decode",
        occurred_at=_timestamp(datetime(2026, 8, 27, 12, 0, second, tzinfo=UTC)),
        event_type=trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
        algorithm="grpo",
        rollout_mode=trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC,
        policy_version="policy-1",
        decision_id="decision-1",
        sequence=sequence,
        attributes={"source_revision": str(sequence)},
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        raw_phase_label="decode",
        stage_id="decode",
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )


def _trace_batch() -> trace_pb2.TraceEventBatch:
    return trace_pb2.TraceEventBatch(
        execution_id="execution-1",
        first_sequence=1,
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
        events=[_event("event-1", sequence=1, second=1), _event("event-2", sequence=2, second=2)],
    )


def _replay(
    replay_id: str,
    *,
    state: int = experiment_pb2.REPLAY_STATE_PENDING,
) -> experiment_pb2.Replay:
    return experiment_pb2.Replay(
        replay_id=replay_id,
        run_id="run-1",
        trace_id="trace-1",
        state=state,
        source_trace_ref="trace-ref-1",
        speed=1.0,
        seed=7,
        created_at=_timestamp(datetime(2026, 8, 27, 12, 0, tzinfo=UTC)),
        data_kind=trace_pb2.DATA_KIND_REPLAY,
    )


def _experiment(
    experiment_id: str,
    *,
    state: int = experiment_pb2.EXPERIMENT_STATE_PENDING,
) -> experiment_pb2.Experiment:
    return experiment_pb2.Experiment(
        experiment_id=experiment_id,
        display_name=f"Experiment {experiment_id}",
        state=state,
        created_at=_timestamp(datetime(2026, 8, 27, 12, 0, tzinfo=UTC)),
        summary="comparison",
    )


def _decision_record(
    *,
    replay_id: str = "replay-1",
    suffix: str = "1",
) -> scheduling_pb2.DecisionRecord:
    return scheduling_pb2.DecisionRecord(
        decision_id=f"decision-{suffix}",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        sequence=int(suffix) if suffix.isdigit() else 1,
        cursor=f"cursor-{replay_id}-{suffix}",
    )


def _replay_schedule_step(
    *,
    replay_id: str = "replay-1",
    start_key_digest: str = "start-key-1",
    ordinal: int = 1,
    step_digest: str = "step-digest-1",
    suffix: str = "1",
    completed_at: datetime | None = None,
) -> ReplayScheduleStep:
    return ReplayScheduleStep(
        replay_id=replay_id,
        start_key_digest=start_key_digest,
        ordinal=ordinal,
        step_digest=step_digest,
        decision=_decision_record(replay_id=replay_id, suffix=suffix),
        completed_at=completed_at or datetime(2026, 8, 27, 12, 0, tzinfo=UTC),
    )


def _manifest() -> runtime_pb2.RuntimeManifest:
    return runtime_pb2.RuntimeManifest(
        manifest_id="manifest-1",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        framework="torch",
        execution_backend="sandbox",
        trainer="trainer",
        rollout_engine="runtime",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )


def _unit(runtime_unit_id: str, *, stage_id: str, generation: int) -> runtime_pb2.RuntimeUnit:
    return runtime_pb2.RuntimeUnit(
        runtime_unit_id=runtime_unit_id,
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        kind=runtime_pb2.RUNTIME_UNIT_KIND_ROLLOUT,
        phase_id=stage_id,
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        stage_id=stage_id,
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        generation=generation,
        observed_at=_timestamp(datetime(2026, 8, 27, 12, 0, generation, tzinfo=UTC)),
    )


def _sandbox(sandbox_id: str, *, generation: int) -> runtime_pb2.Sandbox:
    return runtime_pb2.Sandbox(
        sandbox_id=sandbox_id,
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        generation=generation,
        observed_at=_timestamp(datetime(2026, 8, 27, 12, 0, generation, tzinfo=UTC)),
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )


def _runtime_event(event_id: str, *, generation: int) -> runtime_pb2.SandboxEvent:
    return runtime_pb2.SandboxEvent(
        event_id=event_id,
        event_type=runtime_pb2.SANDBOX_EVENT_TYPE_RUNNING,
        sandbox_id="sandbox-1",
        run_id="run-1",
        job_id="job-1",
        trace_id="trace-1",
        generation=generation,
        state=runtime_pb2.RUNTIME_STATE_RUNNING,
        occurred_at=_timestamp(datetime(2026, 8, 27, 12, 0, generation, tzinfo=UTC)),
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )


def _component_status(component: str, *, revision: int) -> control_pb2.ComponentStatus:
    second = revision % 60
    return control_pb2.ComponentStatus(
        component=component,
        health=control_pb2.COMPONENT_HEALTH_HEALTHY,
        detail="healthy",
        source="runtime-test",
        revision=revision,
        observed_at=_timestamp(datetime(2026, 8, 27, 12, 0, second, tzinfo=UTC)),
        job_id="job-1",
        run_id="run-1",
        trace_id="trace-1",
        data_kind=trace_pb2.DATA_KIND_SYNTHETIC,
    )


@pytest.fixture
def store(tmp_path: Path) -> Generator[SQLiteStore, None, None]:
    path = tmp_path / "runtime.sqlite3"
    instance = SQLiteStore(path)
    yield instance
    instance.close()


def test_restart_recovery_for_intent_trace_and_runtime_state(tmp_path: Path) -> None:
    path = tmp_path / "runtime.sqlite3"
    first = SQLiteStore(path)
    first.record_intent_cas(_intent(1))
    first.append_trace_batch(_trace_batch())
    first.save_runtime_manifest(_manifest())
    first.upsert_runtime_units([_unit("unit-1", stage_id="decode", generation=1)])
    first.upsert_sandboxes([_sandbox("sandbox-1", generation=1)])
    first.publish_runtime_event(_runtime_event("runtime-event-1", generation=1))
    first.close()

    restarted = SQLiteStore(path)
    assert restarted.get_latest_intent("execution-1", "stage-1").version == 1
    assert restarted.get_trace_batch("execution-1", "run-1").events[0].event_id == "event-1"
    manifest, units, sandboxes, cursor = restarted.get_runtime_status("run-1")
    assert manifest.manifest_id == "manifest-1"
    assert [unit.runtime_unit_id for unit in units] == ["unit-1"]
    assert [sandbox.sandbox_id for sandbox in sandboxes] == ["sandbox-1"]
    assert cursor
    restarted.close()


def test_intent_cas_and_idempotency(store: SQLiteStore) -> None:
    first = _intent(1, idempotency_key="intent-key")
    assert store.record_intent_cas(first).version == 1
    assert store.record_intent_cas(_intent(1, idempotency_key="intent-key")).version == 1

    conflicting_same_version = _intent(1, idempotency_key="intent-key")
    conflicting_same_version.priority = 99
    with pytest.raises(IntentVersionConflict, match="different payload"):
        store.record_intent_cas(conflicting_same_version)

    with pytest.raises(IntentVersionConflict, match="advance monotonically"):
        store.record_intent_cas(_intent(0, idempotency_key="older-key"))

    conflicting_key = _intent(2, idempotency_key="intent-key")
    with pytest.raises(IntentVersionConflict, match="idempotency key"):
        store.record_intent_cas(conflicting_key)


def test_managed_worker_trace_commit_is_atomic_and_idempotent(store: SQLiteStore) -> None:
    batch = _trace_batch()
    intent = _intent(1)
    request = b"request-a"
    response = runtime_pb2.PublishTraceBatchResponse(
        intents=[intent], accepted_event_count=len(batch.events), cursor="cursor-a"
    ).SerializeToString(deterministic=True)

    first = store.persist_managed_worker_trace(
        scope="managed-worker-trace",
        key="trace-key",
        request_payload=request,
        batch=batch,
        intents=[intent],
        response_payload=response,
    )
    repeated = store.persist_managed_worker_trace(
        scope="managed-worker-trace",
        key="trace-key",
        request_payload=request,
        batch=batch,
        intents=[intent],
        response_payload=b"ignored",
    )

    assert not first.idempotent
    assert repeated.idempotent
    assert repeated.response_payload == response
    assert store.get_trace_batch("execution-1", "run-1").events == batch.events
    assert store.get_latest_intent("execution-1", "stage-1") == intent
    with pytest.raises(IntentVersionConflict, match="different request"):
        store.persist_managed_worker_trace(
            scope="managed-worker-trace",
            key="trace-key",
            request_payload=b"request-b",
            batch=batch,
            intents=[intent],
            response_payload=response,
        )


def test_managed_worker_trace_commit_rolls_back_all_records(store: SQLiteStore) -> None:
    batch = _trace_batch()
    batch.events.append(_event("event-1", sequence=3, second=3))

    with pytest.raises(IntentVersionConflict, match="trace event"):
        store.persist_managed_worker_trace(
            scope="managed-worker-trace",
            key="trace-key",
            request_payload=b"request",
            batch=batch,
            intents=[_intent(1)],
            response_payload=b"response",
        )

    assert list(store._connection.execute("SELECT event_id FROM trace_events")) == []
    assert list(store._connection.execute("SELECT version FROM intents")) == []
    assert list(store._connection.execute("SELECT key FROM idempotency_records")) == []


def test_stable_trace_pagination_cursor(store: SQLiteStore) -> None:
    batch = _trace_batch()
    extra = _event("event-3", sequence=3, second=3)
    batch.events.append(extra)
    store.append_trace_batch(batch)

    first_page = store.list_trace_events(run_id="run-1", limit=2)
    assert [event.event_id for event in first_page.items] == ["event-1", "event-2"]
    assert first_page.next_cursor

    second_page = store.list_trace_events(
        run_id="run-1",
        limit=2,
        after_cursor=first_page.next_cursor,
    )
    assert [event.event_id for event in second_page.items] == ["event-3"]
    assert second_page.next_cursor == ""


def test_replay_and_experiment_pagination(store: SQLiteStore) -> None:
    for replay_id in ("replay-1", "replay-2", "replay-3"):
        store.upsert_replay(_replay(replay_id))
    first = store.list_replays(limit=2)
    assert [item.replay_id for item in first.items] == ["replay-1", "replay-2"]
    second = store.list_replays(limit=2, after_cursor=first.next_cursor)
    assert [item.replay_id for item in second.items] == ["replay-3"]

    for experiment_id in ("exp-1", "exp-2", "exp-3"):
        store.upsert_experiment(_experiment(experiment_id))
    first_exp = store.list_experiments(limit=2)
    assert [item.experiment_id for item in first_exp.items] == ["exp-1", "exp-2"]
    second_exp = store.list_experiments(limit=2, after_cursor=first_exp.next_cursor)
    assert [item.experiment_id for item in second_exp.items] == ["exp-3"]


def test_replay_schedule_steps_idempotent_and_conflict(store: SQLiteStore) -> None:
    store.upsert_replay(_replay("replay-1"))
    step = _replay_schedule_step()

    recorded = store.record_replay_schedule_step(step)
    repeated = store.record_replay_schedule_step(step)

    assert recorded.step_digest == "step-digest-1"
    assert repeated.decision.decision_id == "decision-1"

    conflicting = _replay_schedule_step(step_digest="different-step-digest")
    with pytest.raises(IntentVersionConflict, match="different payload"):
        store.record_replay_schedule_step(conflicting)

    conflicting_decision = _replay_schedule_step(suffix="2")
    with pytest.raises(IntentVersionConflict, match="different payload"):
        store.record_replay_schedule_step(conflicting_decision)


def test_list_replay_schedule_steps_detects_valid_protobuf_digest_mismatch(
    store: SQLiteStore,
) -> None:
    store.upsert_replay(_replay("replay-1"))
    step = _replay_schedule_step()
    store.record_replay_schedule_step(step)
    tampered_decision = _decision_record(suffix="2").SerializeToString(deterministic=True)
    store._connection.execute(
        """
        UPDATE replay_schedule_steps
        SET decision_payload = ?
        WHERE replay_id = ? AND start_key_digest = ? AND ordinal = ?
        """,
        (tampered_decision, step.replay_id, step.start_key_digest, step.ordinal),
    )
    store._connection.commit()

    with pytest.raises(StorageCorruptionError, match="digest mismatch"):
        store.list_replay_schedule_steps(
            replay_id=step.replay_id,
            start_key_digest=step.start_key_digest,
        )


def test_record_replay_schedule_step_detects_existing_digest_mismatch(
    store: SQLiteStore,
) -> None:
    store.upsert_replay(_replay("replay-1"))
    step = _replay_schedule_step()
    store.record_replay_schedule_step(step)
    tampered_decision = _decision_record(suffix="2").SerializeToString(deterministic=True)
    store._connection.execute(
        """
        UPDATE replay_schedule_steps
        SET decision_payload = ?
        WHERE replay_id = ? AND start_key_digest = ? AND ordinal = ?
        """,
        (tampered_decision, step.replay_id, step.start_key_digest, step.ordinal),
    )
    store._connection.commit()

    with pytest.raises(StorageCorruptionError, match="digest mismatch"):
        store.record_replay_schedule_step(step)


def test_runtime_lifecycle_interfaces_and_cursors(store: SQLiteStore) -> None:
    store.save_runtime_manifest(_manifest())
    store.upsert_runtime_units(
        [
            _unit("unit-1", stage_id="decode", generation=1),
            _unit("unit-2", stage_id="prefill", generation=2),
            _unit("unit-3", stage_id="prefill", generation=3),
        ]
    )
    store.upsert_sandboxes(
        [
            _sandbox("sandbox-1", generation=1),
            _sandbox("sandbox-2", generation=2),
        ]
    )
    store.publish_runtime_event(_runtime_event("runtime-event-1", generation=1))
    store.publish_runtime_event(_runtime_event("runtime-event-2", generation=2))
    store.upsert_component_statuses(
        [
            _component_status("scheduler", revision=1),
            _component_status("runtime", revision=2),
        ]
    )
    checkpoint_cursor = store.save_runtime_checkpoint(
        run_id="run-1",
        checkpoint_ref="ckpt-1",
        completed_at=datetime(2026, 8, 27, 12, 5, tzinfo=UTC),
    )

    unit_page = store.list_runtime_units(run_id="run-1", limit=2)
    assert [item.runtime_unit_id for item in unit_page.items] == ["unit-1", "unit-2"]
    assert unit_page.next_cursor
    next_units = store.list_runtime_units(
        run_id="run-1",
        limit=2,
        after_cursor=unit_page.next_cursor,
    )
    assert [item.runtime_unit_id for item in next_units.items] == ["unit-3"]

    sandbox_page = store.list_sandboxes(run_id="run-1", limit=1)
    assert [item.sandbox_id for item in sandbox_page.items] == ["sandbox-1"]
    next_sandbox = store.list_sandboxes(
        run_id="run-1",
        limit=2,
        after_cursor=sandbox_page.next_cursor,
    )
    assert [item.sandbox_id for item in next_sandbox.items] == ["sandbox-2"]

    events = store.watch_runtime_events(run_id="run-1", limit=1)
    assert [item.event_id for item in events.items] == ["runtime-event-1"]
    later = store.watch_runtime_events(
        run_id="run-1",
        limit=10,
        after_cursor=events.next_cursor,
    )
    assert [item.event_id for item in later.items] == ["runtime-event-2"]
    components = store.list_component_statuses(run_id="run-1", limit=10)
    assert [item.component for item in components.items] == ["scheduler", "runtime"]
    assert checkpoint_cursor


def test_runtime_repository_load_recovery(store: SQLiteStore) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    runtime_repository.upsert_units([_unit("unit-1", stage_id="decode", generation=1)])
    runtime_repository.upsert_sandboxes([_sandbox("sandbox-1", generation=1)])
    runtime_repository.publish_event(_runtime_event("runtime-event-1", generation=1))
    runtime_repository.upsert_component_statuses([_component_status("gateway", revision=1)])

    recovery = runtime_repository.load_recovery("run-1")
    assert recovery.manifest.manifest_id == "manifest-1"
    assert [unit.runtime_unit_id for unit in recovery.runtime_units] == ["unit-1"]
    assert [sandbox.sandbox_id for sandbox in recovery.sandboxes] == ["sandbox-1"]
    assert [item.event.event_id for item in recovery.runtime_events] == ["runtime-event-1"]
    assert [item.sequence for item in recovery.runtime_events] == [1]
    assert [status.component for status in recovery.component_statuses] == ["gateway"]
    assert recovery.latest_runtime_cursor


def test_runtime_repository_load_recovery_reads_all_pages(store: SQLiteStore) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    total = 10_001
    runtime_repository.upsert_units(
        [_unit(f"unit-{index:05d}", stage_id="decode", generation=1) for index in range(total)]
    )
    runtime_repository.upsert_sandboxes(
        [_sandbox(f"sandbox-{index:05d}", generation=1) for index in range(total)]
    )
    for index in range(total):
        event = _runtime_event(f"runtime-event-{index:05d}", generation=1)
        event.sandbox_id = f"sandbox-{index:05d}"
        runtime_repository.publish_event(event)
    runtime_repository.upsert_component_statuses(
        [_component_status(f"component-{index:05d}", revision=index) for index in range(total)]
    )

    recovery = runtime_repository.load_recovery("run-1")

    assert len(recovery.runtime_units) == total
    assert recovery.runtime_units[0].runtime_unit_id == "unit-00000"
    assert recovery.runtime_units[-1].runtime_unit_id == "unit-10000"
    assert len(recovery.sandboxes) == total
    assert recovery.sandboxes[0].sandbox_id == "sandbox-00000"
    assert recovery.sandboxes[-1].sandbox_id == "sandbox-10000"
    assert len(recovery.runtime_events) == total
    assert recovery.runtime_events[0].event.event_id == "runtime-event-00000"
    assert recovery.runtime_events[-1].event.event_id == "runtime-event-10000"
    assert recovery.runtime_events[-1].sequence == total
    assert len(recovery.component_statuses) == total
    assert recovery.component_statuses[0].component == "component-00000"
    assert recovery.component_statuses[-1].component == "component-10000"


def test_persist_runtime_observation_commits_all_runtime_surfaces_atomically(
    store: SQLiteStore,
) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    batch = RuntimeObservationBatch(
        sandbox=_sandbox("sandbox-1", generation=3),
        runtime_unit=_unit("unit-1", stage_id="decode", generation=3),
        event=_runtime_event("runtime-event-1", generation=3),
        component_status=_component_status("runtime", revision=0),
    )

    committed = runtime_repository.persist_runtime_observation(batch)

    assert committed.runtime_event_sequence == 1
    assert committed.component_status.revision == 1
    assert committed.sandbox.sandbox_id == "sandbox-1"
    assert committed.runtime_unit.runtime_unit_id == "unit-1"
    assert committed.event.event_id == "runtime-event-1"
    recovery = runtime_repository.load_recovery("run-1")
    assert [sandbox.sandbox_id for sandbox in recovery.sandboxes] == ["sandbox-1"]
    assert [unit.runtime_unit_id for unit in recovery.runtime_units] == ["unit-1"]
    assert [item.event.event_id for item in recovery.runtime_events] == ["runtime-event-1"]
    assert [item.sequence for item in recovery.runtime_events] == [1]
    assert [status.component for status in recovery.component_statuses] == ["runtime"]
    assert [status.revision for status in recovery.component_statuses] == [1]


def test_persist_runtime_observation_idempotent_reuses_existing_sequence_without_projection_change(
    store: SQLiteStore,
) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    batch = RuntimeObservationBatch(
        sandbox=_sandbox("sandbox-1", generation=1),
        runtime_unit=_unit("unit-1", stage_id="decode", generation=1),
        event=_runtime_event("runtime-event-1", generation=1),
        component_status=_component_status("runtime", revision=0),
    )

    first = runtime_repository.persist_runtime_observation(batch)
    second = runtime_repository.persist_runtime_observation(batch)

    assert first.runtime_event_sequence == 1
    assert second.runtime_event_sequence == 1
    assert second.component_status.revision == 1
    recovery = runtime_repository.load_recovery("run-1")
    assert [item.event.event_id for item in recovery.runtime_events] == ["runtime-event-1"]
    assert [item.sequence for item in recovery.runtime_events] == [1]
    assert [status.revision for status in recovery.component_statuses] == [1]


def test_persist_runtime_observation_conflict_does_not_change_db_projection(
    store: SQLiteStore,
) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    batch = RuntimeObservationBatch(
        sandbox=_sandbox("sandbox-1", generation=1),
        runtime_unit=_unit("unit-1", stage_id="decode", generation=1),
        event=_runtime_event("runtime-event-1", generation=1),
        component_status=_component_status("runtime", revision=0),
    )
    runtime_repository.persist_runtime_observation(batch)

    conflicting_event = _runtime_event("runtime-event-1", generation=1)
    conflicting_event.detail = "different-payload"
    conflicting = RuntimeObservationBatch(
        sandbox=_sandbox("sandbox-2", generation=9),
        runtime_unit=_unit("unit-9", stage_id="decode", generation=9),
        event=conflicting_event,
        component_status=_component_status("runtime", revision=999),
    )

    with pytest.raises(IntentVersionConflict, match="different payload"):
        runtime_repository.persist_runtime_observation(conflicting)

    recovery = runtime_repository.load_recovery("run-1")
    assert [sandbox.sandbox_id for sandbox in recovery.sandboxes] == ["sandbox-1"]
    assert [unit.runtime_unit_id for unit in recovery.runtime_units] == ["unit-1"]
    assert [item.event.event_id for item in recovery.runtime_events] == ["runtime-event-1"]
    assert [item.sequence for item in recovery.runtime_events] == [1]
    assert [status.revision for status in recovery.component_statuses] == [1]


def test_persist_runtime_observation_transaction_rollback_preserves_empty_projection(
    store: SQLiteStore,
) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    batch = RuntimeObservationBatch(
        sandbox=_sandbox("sandbox-1", generation=1),
        runtime_unit=_unit("unit-1", stage_id="decode", generation=1),
        event=_runtime_event("runtime-event-1", generation=1),
        component_status=_component_status("runtime", revision=0),
    )
    store._connection.execute(
        """
        CREATE TEMP TRIGGER fail_component_status_insert
        BEFORE INSERT ON component_statuses
        BEGIN
            SELECT RAISE(FAIL, 'inject component status failure');
        END
        """
    )
    with pytest.raises(sqlite3.IntegrityError, match="inject component status failure"):
        runtime_repository.persist_runtime_observation(batch)
    store._connection.execute("DROP TRIGGER fail_component_status_insert")

    assert runtime_repository.list_units(run_id="run-1", limit=10).items == []
    assert runtime_repository.list_sandboxes(run_id="run-1", limit=10).items == []
    assert runtime_repository.watch_events(run_id="run-1", limit=10).items == []
    assert runtime_repository.list_component_statuses(run_id="run-1", limit=10).items == []


def test_persist_runtime_observation_runtime_event_sequence_is_global_across_runs(
    store: SQLiteStore,
) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    manifest_two = _manifest()
    manifest_two.run_id = "run-2"
    manifest_two.manifest_id = "manifest-2"
    runtime_repository.save_manifest(manifest_two)

    first = runtime_repository.persist_runtime_observation(
        RuntimeObservationBatch(
            sandbox=_sandbox("sandbox-1", generation=1),
            runtime_unit=_unit("unit-1", stage_id="decode", generation=1),
            event=_runtime_event("runtime-event-1", generation=1),
            component_status=_component_status("runtime", revision=0),
        )
    )
    sandbox_two = _sandbox("sandbox-2", generation=1)
    sandbox_two.run_id = "run-2"
    sandbox_two.job_id = "job-2"
    sandbox_two.trace_id = "trace-2"
    unit_two = _unit("unit-2", stage_id="decode", generation=1)
    unit_two.run_id = "run-2"
    unit_two.job_id = "job-2"
    unit_two.trace_id = "trace-2"
    event_two = _runtime_event("runtime-event-2", generation=1)
    event_two.run_id = "run-2"
    event_two.job_id = "job-2"
    event_two.trace_id = "trace-2"
    event_two.sandbox_id = "sandbox-2"
    status_two = _component_status("runtime", revision=0)
    status_two.run_id = "run-2"
    status_two.job_id = "job-2"
    status_two.trace_id = "trace-2"

    second = runtime_repository.persist_runtime_observation(
        RuntimeObservationBatch(
            sandbox=sandbox_two,
            runtime_unit=unit_two,
            event=event_two,
            component_status=status_two,
        )
    )

    assert first.runtime_event_sequence == 1
    assert second.runtime_event_sequence == 2
    run1 = runtime_repository.load_recovery("run-1")
    run2 = runtime_repository.load_recovery("run-2")
    assert [item.sequence for item in run1.runtime_events] == [1]
    assert [item.sequence for item in run2.runtime_events] == [2]


def test_runtime_repository_load_recovery_preserves_sparse_runtime_event_sequences(
    store: SQLiteStore,
) -> None:
    runtime_repository = RuntimeRepository(store)
    runtime_repository.save_manifest(_manifest())
    runtime_repository.upsert_units([_unit("unit-1", stage_id="decode", generation=1)])
    runtime_repository.publish_event(_runtime_event("runtime-event-1", generation=1))
    runtime_repository.publish_event(_runtime_event("runtime-event-2", generation=1))
    runtime_repository.publish_event(_runtime_event("runtime-event-3", generation=1))
    store._connection.execute(
        "DELETE FROM runtime_events WHERE event_id = ?",
        ("runtime-event-2",),
    )
    runtime_repository.publish_event(_runtime_event("runtime-event-4", generation=1))

    recovery = runtime_repository.load_recovery("run-1")
    assert [item.event.event_id for item in recovery.runtime_events] == [
        "runtime-event-1",
        "runtime-event-3",
        "runtime-event-4",
    ]
    assert [item.sequence for item in recovery.runtime_events] == [1, 3, 4]


def test_adapter_hydration_enumerates_latest_surfaces(store: SQLiteStore) -> None:
    runtime_repository = RuntimeRepository(store)
    store.record_intent_cas(_intent(1))
    store.record_intent_cas(_intent(2))
    store.upsert_replay(_replay("replay-1"))
    store.upsert_experiment(_experiment("exp-1"))
    runtime_repository.save_manifest(_manifest())
    runtime_repository.upsert_units([_unit("unit-1", stage_id="decode", generation=1)])
    runtime_repository.upsert_sandboxes([_sandbox("sandbox-1", generation=1)])
    runtime_repository.upsert_component_statuses([_component_status("scheduler", revision=1)])

    hydrated = store.hydrate_adapter_state()
    assert [intent.version for intent in hydrated.intents] == [2]
    assert [manifest.run_id for manifest in hydrated.manifests] == ["run-1"]
    assert [unit.runtime_unit_id for unit in hydrated.runtime_units] == ["unit-1"]
    assert [sandbox.sandbox_id for sandbox in hydrated.sandboxes] == ["sandbox-1"]
    assert [replay.replay_id for replay in hydrated.replays] == ["replay-1"]
    assert [experiment.experiment_id for experiment in hydrated.experiments] == ["exp-1"]
    assert [status.component for status in hydrated.component_statuses] == ["scheduler"]


def test_adapter_hydration_reads_all_pages(store: SQLiteStore) -> None:
    runtime_repository = RuntimeRepository(store)
    total = 10_001
    for index in range(total):
        run_id = f"run-{index:05d}"
        manifest = _manifest()
        manifest.run_id = run_id
        manifest.manifest_id = f"manifest-{index:05d}"
        manifest.job_id = f"job-{index:05d}"
        manifest.trace_id = f"trace-{index:05d}"
        runtime_repository.save_manifest(manifest)
        store.upsert_replay(
            experiment_pb2.Replay(
                replay_id=f"replay-{index:05d}",
                run_id=run_id,
                trace_id=f"trace-{index:05d}",
                state=experiment_pb2.REPLAY_STATE_PENDING,
                source_trace_ref=f"trace-ref-{index:05d}",
                speed=1.0,
                seed=index,
                created_at=_timestamp(datetime(2026, 8, 27, 12, 0, tzinfo=UTC)),
                data_kind=trace_pb2.DATA_KIND_REPLAY,
            )
        )

    hydrated = store.hydrate_adapter_state()

    assert len(hydrated.manifests) == total
    assert hydrated.manifests[0].run_id == "run-00000"
    assert hydrated.manifests[-1].run_id == "run-10000"
    assert len(hydrated.replays) == total
    assert hydrated.replays[0].replay_id == "replay-00000"
    assert hydrated.replays[-1].replay_id == "replay-10000"


def test_retention_delete_audit(store: SQLiteStore) -> None:
    batch = _trace_batch()
    store.append_trace_batch(batch)
    store.upsert_replay(_replay("replay-1"))
    store.upsert_experiment(_experiment("exp-1"))
    cutoff = datetime.now(tz=UTC) + timedelta(days=1)
    audits = store.prune_before(cutoff=cutoff, scopes=("trace_events", "replays", "experiments"))
    assert [audit.scope for audit in audits] == ["trace_events", "replays", "experiments"]
    assert sum(audit.deleted_count for audit in audits) >= 3
    persisted = store.list_delete_audit()
    assert [entry.scope for entry in persisted] == ["trace_events", "replays", "experiments"]


def test_replay_retention_deletes_schedule_steps_and_same_id_rebuild_starts_clean(
    store: SQLiteStore,
) -> None:
    stale_replay = _replay("replay-1")
    stale_replay.created_at.CopyFrom(_timestamp(datetime(2026, 8, 20, 12, 0, tzinfo=UTC)))
    store.upsert_replay(stale_replay)
    stale_step = _replay_schedule_step(
        replay_id="replay-1",
        start_key_digest="start-key-1",
        ordinal=1,
        step_digest="stale-step",
        suffix="1",
        completed_at=datetime(2026, 8, 20, 12, 0, tzinfo=UTC),
    )
    store.record_replay_schedule_step(stale_step)
    store._connection.execute(
        "UPDATE replays SET updated_at = ? WHERE replay_id = ?",
        (store._stamp(datetime(2026, 8, 20, 12, 0, tzinfo=UTC)), "replay-1"),
    )
    store._connection.commit()

    audits = store.prune_before(
        cutoff=datetime(2026, 8, 21, 12, 0, tzinfo=UTC),
        scopes=("replays",),
    )

    assert [audit.scope for audit in audits] == ["replays"]
    assert audits[0].deleted_count == 1
    assert (
        store.list_replay_schedule_steps(
            replay_id="replay-1",
            start_key_digest="start-key-1",
        )
        == []
    )

    rebuilt_replay = _replay("replay-1")
    store.upsert_replay(rebuilt_replay)
    rebuilt_step = _replay_schedule_step(
        replay_id="replay-1",
        start_key_digest="start-key-1",
        ordinal=1,
        step_digest="fresh-step",
        suffix="2",
    )
    recorded = store.record_replay_schedule_step(rebuilt_step)
    listed = store.list_replay_schedule_steps(
        replay_id="replay-1",
        start_key_digest="start-key-1",
    )

    assert recorded.step_digest == "fresh-step"
    assert [step.step_digest for step in listed] == ["fresh-step"]
    assert [step.decision.decision_id for step in listed] == ["decision-2"]


def test_runtime_event_and_idempotent_response_are_idempotent(store: SQLiteStore) -> None:
    event = _runtime_event("runtime-event-1", generation=1)
    assert store.publish_runtime_event(event).event_id == "runtime-event-1"
    assert store.publish_runtime_event(event).event_id == "runtime-event-1"

    request = b"request-a"
    response = b"response-a"
    assert (
        store.remember_idempotent_response(
            scope="runtime.start",
            key="key-1",
            request_payload=request,
            response_payload=response,
        )
        == response
    )
    assert (
        store.remember_idempotent_response(
            scope="runtime.start",
            key="key-1",
            request_payload=request,
            response_payload=b"ignored",
        )
        == response
    )
    with pytest.raises(IntentVersionConflict, match="different request"):
        store.remember_idempotent_response(
            scope="runtime.start",
            key="key-1",
            request_payload=b"request-b",
            response_payload=response,
        )


def test_corruption_detection_on_decode(tmp_path: Path) -> None:
    path = tmp_path / "runtime.sqlite3"
    store = SQLiteStore(path)
    store.record_intent_cas(_intent(1))
    store.close()

    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE intents SET payload = ? WHERE execution_id = ? AND stage_id = ? AND version = ?",
        (b"not-protobuf", "execution-1", "stage-1", 1),
    )
    connection.commit()
    connection.close()

    restarted = SQLiteStore(path)
    with pytest.raises(StorageCorruptionError, match="intent execution-1/stage-1"):
        restarted.get_latest_intent("execution-1", "stage-1")
    restarted.close()


def test_apply_migrations_rolls_back_failed_migration_atomically(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_initial.sql").write_text(
        "CREATE TABLE stable_table (id INTEGER PRIMARY KEY);",
        encoding="utf-8",
    )
    (migrations_dir / "0002_broken.sql").write_text(
        "\n".join(
            [
                "CREATE TABLE half_applied_table (id INTEGER PRIMARY KEY);",
                "INSERT INTO missing_table(id) VALUES (1);",
            ]
        ),
        encoding="utf-8",
    )
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row

    with pytest.raises(sqlite3.OperationalError, match="no such table: missing_table"):
        apply_migrations(connection, migrations_dir=migrations_dir)

    applied = [
        row["version"]
        for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version")
    ]
    tables = {
        str(row["name"])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
    }

    assert applied == ["0001_initial.sql"]
    assert "stable_table" in tables
    assert "half_applied_table" not in tables
    connection.close()
