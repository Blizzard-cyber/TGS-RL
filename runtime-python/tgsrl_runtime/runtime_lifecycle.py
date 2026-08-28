"""Lifecycle orchestration helpers for RuntimeSupervisor."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from tgsrl.v1 import control_pb2, operator_pb2, runtime_pb2, scheduling_pb2, trace_pb2

from adapters.compliance.runtime import LifecycleAction
from tgsrl_runtime.aggregation import TraceSummary
from tgsrl_runtime.checkpoints import CheckpointRecord
from tgsrl_runtime.duration import to_timestamp
from tgsrl_runtime.executor import RuntimeExecutionResult
from tgsrl_runtime.proto_utils import stable_cursor
from tgsrl_runtime.runtime_errors import RuntimeLifecycleError

if TYPE_CHECKING:
    from tgsrl_runtime.supervisor import RuntimeSupervisor


type BackendControlRequest = (
    runtime_pb2.PauseRuntimeRequest
    | runtime_pb2.ResumeRuntimeRequest
    | runtime_pb2.StopRuntimeRequest
    | runtime_pb2.TerminateRuntimeRequest
)


def _digest_payload(parts: tuple[str, ...]) -> str:
    import hashlib

    material = "\0".join(parts).encode()
    return hashlib.sha256(material).hexdigest()


_START_IDEMPOTENCY_KEY_ANNOTATION = "tgsrl.start_idempotency_key"
_START_PUBLICATION_STATE_ANNOTATION = "tgsrl.start_publication_state"
_START_PUBLICATION_PENDING = "pending"
_START_PUBLICATION_COMPLETE = "complete"
_START_INTENT_KEY_LABEL = "tgsrl.start_idempotency_key"
_START_INTENT_ACK_ANNOTATION = "tgsrl.start_intent_ack"
type StartOutboxKey = tuple[str, int, str]
type IntentIdentity = tuple[str, str, int]


@dataclass(slots=True)
class RuntimeLifecycleCoordinator:
    """Apply runtime lifecycle authority rules on top of supervisor stores."""

    supervisor: RuntimeSupervisor
    _run_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    _start_intents: dict[StartOutboxKey, tuple[scheduling_pb2.SchedulingIntent, ...]] = field(
        default_factory=dict
    )
    _start_acks: dict[StartOutboxKey, set[IntentIdentity]] = field(default_factory=dict)

    def lock_for_run(self, run_id: str) -> asyncio.Lock:
        if not run_id:
            raise ValueError("run_id is required for runtime mutation")
        return self._run_locks.setdefault(run_id, asyncio.Lock())

    def prepare(
        self, request: runtime_pb2.PrepareRuntimeRequest
    ) -> runtime_pb2.PrepareRuntimeResponse:
        executor, manifest = self.supervisor._sync_executor(request.run_id, compile_if_missing=True)
        result = self.supervisor._require_executor_success(
            executor.prepare(request.run_id, idempotency_key=request.idempotency_key),
            action=LifecycleAction.PREPARE,
        )
        prepared_units = self._persist_control_result(result)
        self.supervisor.persistence.save_manifest(manifest)
        return runtime_pb2.PrepareRuntimeResponse(
            manifest=manifest,
            runtime_units=prepared_units,
            cursor=result.cursor,
        )

    async def start(
        self, request: runtime_pb2.StartRuntimeRequest
    ) -> runtime_pb2.StartRuntimeResponse:
        if not request.idempotency_key.strip():
            raise ValueError("Start idempotency_key is required")
        async with self.lock_for_run(request.run_id):
            return await self._start_locked(request)

    async def _start_locked(
        self, request: runtime_pb2.StartRuntimeRequest
    ) -> runtime_pb2.StartRuntimeResponse:
        manifest = self.supervisor._ensure_run(request.run_id)
        if not self.supervisor.runtime_units.list(request.run_id):
            self.prepare(
                runtime_pb2.PrepareRuntimeRequest(
                    run_id=request.run_id,
                    request_id=request.request_id,
                    idempotency_key=request.idempotency_key,
                )
            )
        executor, manifest = self.supervisor._sync_executor(
            request.run_id, compile_if_missing=False
        )
        result = self.supervisor._require_executor_success(
            executor.start(
                request.run_id,
                idempotency_key=request.idempotency_key,
                defer_idempotency=True,
            ),
            action=LifecycleAction.LAUNCH,
        )
        if result.idempotent:
            return runtime_pb2.StartRuntimeResponse(
                manifest=manifest,
                runtime_units=self.supervisor.runtime_units.list(request.run_id),
                cursor=result.cursor,
            )
        requested_units = self._persist_control_result(
            result,
            start_idempotency_key=request.idempotency_key,
            start_publication_state=_START_PUBLICATION_PENDING,
        )
        self.supervisor.persistence.save_manifest(manifest)
        outbox_key = (request.run_id, result.generation, request.idempotency_key)
        intents = self._start_intents.get(outbox_key)
        if intents is None:
            intents = self._restore_start_intents(outbox_key, requested_units)
        if intents is None:
            events = [
                self.supervisor._trace_event_for_start(manifest=manifest, runtime_unit=unit)
                for unit in requested_units
            ]
            self._record_start_trace_batch(request.run_id, manifest, events)
            intents = self._build_start_intents(
                request.run_id,
                manifest,
                requested_units,
                request.idempotency_key,
            )
            self._start_intents[outbox_key] = intents
        self._persist_start_intents(intents)
        await self._drain_start_outbox(outbox_key, intents)
        requested_units = self._mark_start_complete(
            request.run_id, request.idempotency_key, result.generation
        )
        executor.complete_start(
            request.run_id,
            idempotency_key=request.idempotency_key,
            runtime_units=requested_units,
        )
        self._start_intents.pop(outbox_key, None)
        self._start_acks.pop(outbox_key, None)
        return runtime_pb2.StartRuntimeResponse(
            manifest=manifest,
            runtime_units=requested_units,
            cursor=result.cursor,
        )

    def pause(self, request: runtime_pb2.PauseRuntimeRequest) -> runtime_pb2.PauseRuntimeResponse:
        result = self._apply_control_action(
            request.run_id,
            LifecycleAction.PAUSE,
            idempotency_key=request.idempotency_key,
        )
        return runtime_pb2.PauseRuntimeResponse(
            runtime_units=self._persist_control_result(result),
            cursor=result.cursor,
        )

    def resume(
        self, request: runtime_pb2.ResumeRuntimeRequest
    ) -> runtime_pb2.ResumeRuntimeResponse:
        result = self._apply_control_action(
            request.run_id,
            LifecycleAction.RESUME,
            idempotency_key=request.idempotency_key,
        )
        return runtime_pb2.ResumeRuntimeResponse(
            runtime_units=self._persist_control_result(result),
            cursor=result.cursor,
        )

    def checkpoint(
        self, request: runtime_pb2.CheckpointRuntimeRequest
    ) -> runtime_pb2.CheckpointRuntimeResponse:
        run_id = request.run_id
        executor, _manifest = self.supervisor._sync_executor(run_id, compile_if_missing=False)
        result = self.supervisor._require_executor_success(
            executor.checkpoint(
                run_id,
                checkpoint_ref=request.checkpoint_ref,
                idempotency_key=request.idempotency_key,
            ),
            action=LifecycleAction.CHECKPOINT,
        )
        self._persist_control_result(result)
        self.supervisor._update_runtime_status(run_id)
        summary = self.supervisor.aggregator.summarize(self.supervisor.trace_ingestor.list(run_id))
        checkpoint_ref = (
            result.checkpoint_ref
            or f"checkpoint:{run_id}:{self.supervisor.versions.next('checkpoint')}"
        )
        completed_at = datetime.now(tz=UTC)
        self.supervisor.checkpoints.add(
            CheckpointRecord(
                checkpoint_ref=checkpoint_ref,
                run_id=run_id,
                completed_at=completed_at,
                state_digest=_digest_payload(
                    (run_id, str(summary.event_count), str(summary.safe_point_count))
                ),
            )
        )
        checkpoint_cursor = self.supervisor.persistence.save_checkpoint(
            run_id=run_id,
            checkpoint_ref=checkpoint_ref,
            completed_at=completed_at,
        )
        return runtime_pb2.CheckpointRuntimeResponse(
            checkpoint_ref=checkpoint_ref,
            completed_at=to_timestamp(completed_at),
            cursor=result.cursor or checkpoint_cursor,
        )

    def stop(self, request: runtime_pb2.StopRuntimeRequest) -> runtime_pb2.StopRuntimeResponse:
        result = self._apply_control_action(
            request.run_id,
            LifecycleAction.STOP,
            idempotency_key=request.idempotency_key,
        )
        return runtime_pb2.StopRuntimeResponse(
            runtime_units=self._persist_control_result(result),
            cursor=result.cursor,
        )

    def terminate(
        self, request: runtime_pb2.TerminateRuntimeRequest
    ) -> runtime_pb2.TerminateRuntimeResponse:
        result = self._apply_control_action(
            request.run_id,
            LifecycleAction.TERMINATE,
            idempotency_key=request.idempotency_key,
        )
        return runtime_pb2.TerminateRuntimeResponse(
            runtime_units=self._persist_control_result(result),
            cursor=result.cursor,
        )

    def _apply_control_action(
        self,
        run_id: str,
        action: LifecycleAction,
        *,
        idempotency_key: str,
    ) -> RuntimeExecutionResult:
        if action not in {
            LifecycleAction.PAUSE,
            LifecycleAction.RESUME,
            LifecycleAction.STOP,
            LifecycleAction.TERMINATE,
        }:
            raise ValueError(f"unsupported backend control action: {action.value}")
        manifest = self.supervisor._ensure_run(run_id)
        units = self.supervisor.runtime_units.list(run_id)
        if not units:
            raise ValueError(f"run {run_id!r} has no compiled runtime units")
        generation = max(unit.generation for unit in units)
        return RuntimeExecutionResult(
            manifest=manifest,
            runtime_units=tuple(units),
            action=action,
            generation=generation,
            cursor=stable_cursor(action.value, run_id, generation, idempotency_key),
            component_results=(),
        )

    def build_operator_control_request(
        self,
        request: BackendControlRequest,
        *,
        action: LifecycleAction,
    ) -> operator_pb2.ApplyRuntimeControlRequest:
        """Validate and build one concrete, generation-fenced Operator request."""
        manifest = self.supervisor._ensure_run(request.run_id)
        known_unit_ids = {
            unit.runtime_unit_id
            for unit in self.supervisor.runtime_units.list(request.run_id)
            if unit.runtime_unit_id
        }
        sandboxes = self.supervisor.sandboxes.list(request.run_id)
        if not sandboxes:
            raise ValueError(f"run {request.run_id!r} has no materialized sandboxes")
        targets_by_identity: dict[tuple[str, str], operator_pb2.RuntimeControlTarget] = {}
        for sandbox in sandboxes:
            runtime_unit_id = sandbox.binding.runtime_unit_id or sandbox.binding.pending_unit_id
            if not runtime_unit_id:
                raise ValueError(f"sandbox {sandbox.sandbox_id!r} is not bound to a runtime unit")
            if runtime_unit_id not in known_unit_ids:
                raise ValueError(
                    f"sandbox {sandbox.sandbox_id!r} references unknown runtime unit "
                    f"{runtime_unit_id!r}"
                )
            if not sandbox.sandbox_id:
                raise ValueError("runtime control target is missing sandbox_id")
            if sandbox.generation == 0:
                raise ValueError(f"sandbox {sandbox.sandbox_id!r} has no workload generation fence")
            identity = (runtime_unit_id, sandbox.sandbox_id)
            previous = targets_by_identity.get(identity)
            if previous is not None and previous.expected_generation != sandbox.generation:
                raise ValueError(
                    f"sandbox {sandbox.sandbox_id!r} has conflicting workload generations"
                )
            targets_by_identity[identity] = operator_pb2.RuntimeControlTarget(
                runtime_unit_id=runtime_unit_id,
                sandbox_id=sandbox.sandbox_id,
                expected_generation=sandbox.generation,
            )
        targets = [targets_by_identity[key] for key in sorted(targets_by_identity)]
        command = {
            LifecycleAction.PAUSE: control_pb2.JOB_COMMAND_TYPE_PAUSE,
            LifecycleAction.RESUME: control_pb2.JOB_COMMAND_TYPE_RESUME,
            LifecycleAction.STOP: control_pb2.JOB_COMMAND_TYPE_STOP,
            LifecycleAction.TERMINATE: control_pb2.JOB_COMMAND_TYPE_TERMINATE,
        }.get(action)
        if command is None:
            raise ValueError(f"unsupported backend control action: {action.value}")
        return operator_pb2.ApplyRuntimeControlRequest(
            action=command,
            job_id=manifest.job_id,
            run_id=manifest.run_id,
            trace_id=manifest.trace_id,
            targets=targets,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
            reason=getattr(request, "reason", ""),
        )

    def _persist_control_result(
        self,
        result: RuntimeExecutionResult,
        *,
        start_idempotency_key: str = "",
        start_publication_state: str = "",
    ) -> list[runtime_pb2.RuntimeUnit]:
        desired_state = (
            result.component_results[0].requested_state
            if result.component_results
            else {
                LifecycleAction.PREPARE: runtime_pb2.RUNTIME_STATE_PREPARING,
                LifecycleAction.LAUNCH: runtime_pb2.RUNTIME_STATE_STARTING,
                LifecycleAction.PAUSE: runtime_pb2.RUNTIME_STATE_PAUSING,
                LifecycleAction.RESUME: runtime_pb2.RUNTIME_STATE_RESUMING,
                LifecycleAction.CHECKPOINT: runtime_pb2.RUNTIME_STATE_CHECKPOINTING,
                LifecycleAction.STOP: runtime_pb2.RUNTIME_STATE_STOPPING,
                LifecycleAction.TERMINATE: runtime_pb2.RUNTIME_STATE_TERMINATING,
                LifecycleAction.STATUS: runtime_pb2.RUNTIME_STATE_UNKNOWN,
            }[result.action]
        )
        updated_units: list[runtime_pb2.RuntimeUnit] = []
        has_component_results = bool(result.component_results)
        first_error = next(
            (item.error for item in result.component_results if item.error is not None),
            None,
        )
        for current in self.supervisor.runtime_units.list(result.manifest.run_id):
            unit = runtime_pb2.RuntimeUnit()
            unit.CopyFrom(current)
            if result.action is LifecycleAction.LAUNCH:
                unit.generation = result.generation
                if start_idempotency_key:
                    unit.annotations[_START_IDEMPOTENCY_KEY_ANNOTATION] = start_idempotency_key
                if start_publication_state:
                    preserve_ack = (
                        unit.annotations.get(_START_IDEMPOTENCY_KEY_ANNOTATION)
                        == start_idempotency_key
                        and unit.annotations.get(_START_PUBLICATION_STATE_ANNOTATION)
                        == _START_PUBLICATION_PENDING
                    )
                    unit.annotations[_START_PUBLICATION_STATE_ANNOTATION] = start_publication_state
                    if not preserve_ack:
                        unit.annotations.pop(_START_INTENT_ACK_ANNOTATION, None)
            unit.status_reason = self.supervisor._state_reason_for(desired_state)
            if has_component_results and first_error is None:
                unit.error_code = ""
                unit.error_message = ""
                unit.exit_code = 0
            elif first_error is not None:
                unit.error_code = first_error.kind
                unit.error_message = first_error.summary
                unit.exit_code = first_error.exit_code or 0
            updated_units.append(unit)
        self.supervisor.runtime_units.put_many(result.manifest.run_id, updated_units)
        self.supervisor.persistence.save_runtime_units(updated_units)
        return self.supervisor.runtime_units.list(result.manifest.run_id)

    def _record_start_trace_batch(
        self,
        run_id: str,
        manifest: runtime_pb2.RuntimeManifest,
        events: list[trace_pb2.TraceEvent],
    ) -> TraceSummary:
        trace_batch = trace_pb2.TraceEventBatch(
            execution_id=run_id,
            first_sequence=events[0].sequence if events else 0,
            run_id=run_id,
            trace_id=manifest.trace_id,
            data_kind=manifest.data_kind or trace_pb2.DATA_KIND_SYNTHETIC,
            events=events,
        )
        self.supervisor.persistence.record_trace_batch(trace_batch)
        ingested = self.supervisor.trace_ingestor.ingest(run_id, events)
        return self.supervisor.aggregator.summarize(ingested)

    def _build_start_intents(
        self,
        run_id: str,
        manifest: runtime_pb2.RuntimeManifest,
        requested_units: list[runtime_pb2.RuntimeUnit],
        start_idempotency_key: str,
    ) -> tuple[scheduling_pb2.SchedulingIntent, ...]:
        summary = self.supervisor.aggregator.summarize(self.supervisor.trace_ingestor.list(run_id))
        intents = self.supervisor.intent_coordinator.build_for_units(
            manifest, requested_units, summary
        )
        for intent in intents:
            intent.run_id = run_id
            intent.trace_id = manifest.trace_id
            intent.data_kind = manifest.data_kind or trace_pb2.DATA_KIND_SYNTHETIC
            intent.generation = requested_units[0].generation if requested_units else 0
            intent.labels["selection_strategy"] = manifest.annotations.get("selection_strategy", "")
            intent.labels["provider_source"] = manifest.annotations.get("provider_source", "")
            intent.labels["provider_kind"] = manifest.annotations.get("provider_kind", "")
            intent.labels["algorithm"] = manifest.annotations.get("algorithm", "")
            intent.labels[_START_INTENT_KEY_LABEL] = start_idempotency_key
            intent.cursor = self.supervisor._cursor_for_intent(intent)
        return tuple(intents)

    def _persist_start_intents(self, intents: tuple[scheduling_pb2.SchedulingIntent, ...]) -> None:
        for intent in intents:
            self.supervisor.persistence.record_intent(intent)
            identity = (intent.execution_id, intent.stage_id, intent.version)
            if not any(
                (known.execution_id, known.stage_id, known.version) == identity
                for known in self.supervisor.published_intents
            ):
                self.supervisor.published_intents.append(intent)

    def _restore_start_intents(
        self,
        outbox_key: StartOutboxKey,
        requested_units: list[runtime_pb2.RuntimeUnit],
    ) -> tuple[scheduling_pb2.SchedulingIntent, ...] | None:
        run_id, generation, start_key = outbox_key
        expected_stages = {unit.stage_id for unit in requested_units}
        restored = tuple(
            intent
            for intent in self.supervisor.published_intents
            if intent.run_id == run_id
            and intent.generation == generation
            and intent.labels.get(_START_INTENT_KEY_LABEL) == start_key
        )
        if {intent.stage_id for intent in restored} != expected_stages:
            return None
        self._start_intents[outbox_key] = restored
        return restored

    async def _drain_start_outbox(
        self,
        outbox_key: StartOutboxKey,
        intents: tuple[scheduling_pb2.SchedulingIntent, ...],
    ) -> None:
        # Start uses at-least-once intent delivery: after intents are durably staged locally,
        # retries or restart recovery may republish the same immutable intent identity until the
        # per-stage local ack is durably recorded. Downstream scheduler dedup on canonical
        # intent.idempotency_key is therefore part of the correctness contract; this is not
        # exactly-once delivery from the runtime side.
        if self.supervisor.scheduler_client is None:
            raise RuntimeError(
                "runtime Start requires a scheduler client; Intent outbox remains pending"
            )
        acknowledged = self._start_acks.setdefault(outbox_key, set())
        acknowledged.update(self._restore_start_acks(intents))
        for intent in intents:
            identity = (intent.execution_id, intent.stage_id, intent.version)
            if identity in acknowledged:
                continue
            response = await self.supervisor.scheduler_client.publish_intent(intent)
            if response is not None and response.status not in {
                scheduling_pb2.INTENT_PUBLISH_STATUS_ACCEPTED,
                scheduling_pb2.INTENT_PUBLISH_STATUS_DEDUPLICATED,
            }:
                raise RuntimeLifecycleError(
                    f"scheduler rejected intent {intent.execution_id}/{intent.stage_id}: "
                    f"{response.detail}"
                )
            self._persist_intent_ack(intent)
            acknowledged.add(identity)

    def _restore_start_acks(
        self, intents: tuple[scheduling_pb2.SchedulingIntent, ...]
    ) -> set[IntentIdentity]:
        acknowledgements = {
            unit.stage_id: unit.annotations.get(_START_INTENT_ACK_ANNOTATION, "")
            for unit in self.supervisor.runtime_units.list(intents[0].run_id if intents else "")
        }
        return {
            (intent.execution_id, intent.stage_id, intent.version)
            for intent in intents
            if acknowledgements.get(intent.stage_id) == intent.idempotency_key
        }

    def _persist_intent_ack(self, intent: scheduling_pb2.SchedulingIntent) -> None:
        units = self.supervisor.runtime_units.list(intent.run_id)
        matched = False
        for unit in units:
            if unit.stage_id == intent.stage_id:
                unit.annotations[_START_INTENT_ACK_ANNOTATION] = intent.idempotency_key
                matched = True
        if not matched:
            raise RuntimeError(f"no runtime unit found for acknowledged stage {intent.stage_id!r}")
        self.supervisor.persistence.save_runtime_units(units)
        self.supervisor.runtime_units.put_many(intent.run_id, units)

    def _mark_start_complete(
        self, run_id: str, start_key: str, generation: int
    ) -> list[runtime_pb2.RuntimeUnit]:
        completed = self.supervisor.runtime_units.list(run_id)
        for unit in completed:
            if unit.generation != generation:
                raise RuntimeError("runtime generation changed while Start was publishing")
            unit.annotations[_START_IDEMPOTENCY_KEY_ANNOTATION] = start_key
            unit.annotations[_START_PUBLICATION_STATE_ANNOTATION] = _START_PUBLICATION_COMPLETE
        self.supervisor.persistence.save_runtime_units(completed)
        self.supervisor.runtime_units.put_many(run_id, completed)
        return self.supervisor.runtime_units.list(run_id)
