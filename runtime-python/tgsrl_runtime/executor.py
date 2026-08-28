"""Runtime execution orchestration and compatibility re-exports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from tgsrl.v1 import runtime_pb2

import tgsrl_runtime.execution_drivers as _execution_drivers
import tgsrl_runtime.execution_types as _execution_types
from adapters import (
    AdapterErrorKind,
    AdapterUnavailableError,
    CommandResult,
    LaunchSpec,
    LifecycleAction,
    LifecycleCall,
    RunnerKind,
    RuntimeAdapterBundle,
    RuntimeAdapterRegistry,
)
from adapters.compliance.runtime import (
    ComponentAdapter,
    ErrorContract,
    runtime_state_contract,
)
from tgsrl_runtime.proto_utils import clone_message, stable_cursor

CommandDriver = _execution_types.CommandDriver
ProcessDriver = _execution_types.ProcessDriver
ProcessStatus = _execution_types.ProcessStatus
NormalizedActionError = _execution_types.NormalizedActionError
ComponentActionResult = _execution_types.ComponentActionResult
RuntimeExecutionResult = _execution_types.RuntimeExecutionResult
ComponentProcessRecord = _execution_types.ComponentProcessRecord
ExecutorSnapshot = _execution_types.ExecutorSnapshot
DriverBehavior = _execution_types.DriverBehavior

FakeProcessDriver = _execution_drivers.FakeProcessDriver
FakeCommandDriver = _execution_drivers.FakeCommandDriver
SubprocessDriver = _execution_drivers.SubprocessDriver

_START_IDEMPOTENCY_KEY_ANNOTATION = "tgsrl.start_idempotency_key"
_START_PUBLICATION_STATE_ANNOTATION = "tgsrl.start_publication_state"
_START_PUBLICATION_PENDING = "pending"
_START_PUBLICATION_COMPLETE = "complete"


def _component_adapters(bundle: RuntimeAdapterBundle) -> tuple[tuple[str, ComponentAdapter], ...]:
    return (
        ("framework", bundle.framework),
        ("execution", bundle.execution),
        ("trainer", bundle.trainer),
        ("rollout_engine", bundle.rollout_engine),
    )


def _normalize_action_error(
    contract: ErrorContract,
    *,
    exit_code: int | None = None,
) -> NormalizedActionError:
    return NormalizedActionError(
        kind=contract.kind,
        retryable=contract.retryable,
        summary=contract.summary,
        resulting_state=contract.resulting_state,
        exit_code=exit_code,
    )


def _command_failure(action: LifecycleAction, result: CommandResult) -> RuntimeError:
    detail = (
        result.stderr.strip()
        or result.stdout.strip()
        or f"{action.value} exited {result.exit_code}"
    )
    return RuntimeError(detail)


def _state_reason(state: int) -> str:
    return {
        runtime_pb2.RUNTIME_STATE_UNKNOWN: "unknown",
        runtime_pb2.RUNTIME_STATE_REQUESTED: "requested",
        runtime_pb2.RUNTIME_STATE_BOUND: "bound",
        runtime_pb2.RUNTIME_STATE_RUNNING: "running",
        runtime_pb2.RUNTIME_STATE_PAUSED: "paused",
        runtime_pb2.RUNTIME_STATE_SLEEPING: "sleeping",
        runtime_pb2.RUNTIME_STATE_FAILED: "failed",
        runtime_pb2.RUNTIME_STATE_TERMINATED: "terminated",
        runtime_pb2.RUNTIME_STATE_VALIDATING: "validating",
        runtime_pb2.RUNTIME_STATE_COMPILING: "compiling",
        runtime_pb2.RUNTIME_STATE_PREPARING: "preparing",
        runtime_pb2.RUNTIME_STATE_STARTING: "starting",
        runtime_pb2.RUNTIME_STATE_PAUSING: "pausing",
        runtime_pb2.RUNTIME_STATE_RESUMING: "resuming",
        runtime_pb2.RUNTIME_STATE_CHECKPOINTING: "checkpointing",
        runtime_pb2.RUNTIME_STATE_STOPPING: "stopping",
        runtime_pb2.RUNTIME_STATE_TERMINATING: "terminating",
    }.get(state, "unknown")


def _durable_start_retry(
    runtime_units: Sequence[runtime_pb2.RuntimeUnit],
) -> tuple[str, int] | None:
    if not runtime_units:
        return None
    keys = {unit.annotations.get(_START_IDEMPOTENCY_KEY_ANNOTATION, "") for unit in runtime_units}
    if len(keys) != 1:
        return None
    key = next(iter(keys))
    if not key:
        return None
    generations = {unit.generation for unit in runtime_units}
    if len(generations) != 1:
        return None
    generation = next(iter(generations))
    if generation <= 0:
        return None
    states = {
        unit.annotations.get(_START_PUBLICATION_STATE_ANNOTATION, "") for unit in runtime_units
    }
    if states not in ({""}, {_START_PUBLICATION_COMPLETE}):
        return None
    return key, generation


def _durable_pending_start(
    runtime_units: Sequence[runtime_pb2.RuntimeUnit],
) -> tuple[str, int] | None:
    if not runtime_units:
        return None
    states = {
        unit.annotations.get(_START_PUBLICATION_STATE_ANNOTATION, "") for unit in runtime_units
    }
    if states != {_START_PUBLICATION_PENDING}:
        return None
    keys = {unit.annotations.get(_START_IDEMPOTENCY_KEY_ANNOTATION, "") for unit in runtime_units}
    generations = {unit.generation for unit in runtime_units}
    if len(keys) != 1 or len(generations) != 1:
        return None
    key = next(iter(keys))
    generation = next(iter(generations))
    return (key, generation) if key and generation > 0 else None


@dataclass
class RuntimeExecutor:
    """Backend control helper for runtime lifecycle actions."""

    registry: RuntimeAdapterRegistry = field(default_factory=RuntimeAdapterRegistry)
    command_driver: CommandDriver | None = None
    process_driver: ProcessDriver | None = None
    default_timeout_seconds: float | None = None
    action_timeouts: Mapping[LifecycleAction, float] = field(default_factory=dict)
    _manifests: dict[str, runtime_pb2.RuntimeManifest] = field(default_factory=dict)
    _runtime_units: dict[str, tuple[runtime_pb2.RuntimeUnit, ...]] = field(default_factory=dict)
    _generations: dict[str, int] = field(default_factory=dict)
    _desired_states: dict[str, int] = field(default_factory=dict)
    _processes: dict[str, dict[str, ComponentProcessRecord]] = field(default_factory=dict)
    _idempotency_cache: dict[tuple[str, LifecycleAction, str], RuntimeExecutionResult] = field(
        default_factory=dict
    )

    def register_runtime(
        self,
        manifest: runtime_pb2.RuntimeManifest,
        runtime_units: Sequence[runtime_pb2.RuntimeUnit] | None = None,
    ) -> tuple[runtime_pb2.RuntimeManifest, tuple[runtime_pb2.RuntimeUnit, ...]]:
        normalized, compiled_units, _diagnostics = self.registry.compile(manifest)
        selected_units = (
            tuple(runtime_units) if runtime_units is not None else tuple(compiled_units)
        )
        self._manifests[normalized.run_id] = clone_message(normalized)
        self._runtime_units[normalized.run_id] = tuple(
            clone_message(unit) for unit in selected_units
        )
        self._generations.setdefault(normalized.run_id, 0)
        self._desired_states.setdefault(normalized.run_id, runtime_pb2.RUNTIME_STATE_REQUESTED)
        self._processes.setdefault(normalized.run_id, {})
        self._hydrate_start_idempotency_cache(normalized.run_id)
        return clone_message(normalized), tuple(
            clone_message(unit) for unit in self._runtime_units[normalized.run_id]
        )

    def snapshot(self) -> ExecutorSnapshot:
        return ExecutorSnapshot(
            manifests=tuple(clone_message(item) for item in self._manifests.values()),
            runtime_units=tuple(
                (run_id, tuple(clone_message(unit) for unit in units))
                for run_id, units in self._runtime_units.items()
            ),
            generations=tuple(sorted(self._generations.items())),
            desired_states=tuple(sorted(self._desired_states.items())),
            processes=tuple(
                (
                    run_id,
                    tuple(
                        ComponentProcessRecord(
                            component=record.component,
                            generation=record.generation,
                            handle=record.handle,
                            requested_state=record.requested_state,
                            observed_state=record.observed_state,
                        )
                        for record in sorted(records.values(), key=lambda item: item.component)
                    ),
                )
                for run_id, records in sorted(self._processes.items())
            ),
        )

    def restore(self, snapshot: ExecutorSnapshot) -> None:
        self._manifests = {
            manifest.run_id: clone_message(manifest) for manifest in snapshot.manifests
        }
        for run_id, units in snapshot.runtime_units:
            self._runtime_units[run_id] = tuple(clone_message(unit) for unit in units)
        self._generations = dict(snapshot.generations)
        self._desired_states = dict(snapshot.desired_states)
        self._processes = {
            run_id: {record.component: record for record in records}
            for run_id, records in snapshot.processes
        }

    def restore_runtime_generation(self, run_id: str, generation: int) -> None:
        """Restore a persisted generation watermark for one registered runtime."""
        if generation < 0:
            raise ValueError("generation must be non-negative")
        if run_id not in self._manifests:
            raise ValueError(f"unknown run_id: {run_id}")
        self._generations[run_id] = max(self._generations.get(run_id, 0), generation)

    def prepare(self, run_id: str, *, idempotency_key: str = "") -> RuntimeExecutionResult:
        return self._apply_action(run_id, LifecycleAction.PREPARE, idempotency_key=idempotency_key)

    def start(
        self,
        run_id: str,
        *,
        idempotency_key: str = "",
        defer_idempotency: bool = False,
    ) -> RuntimeExecutionResult:
        """Stage a launch; lifecycle callers defer completion until Intent delivery."""
        cached = self._cached(run_id, LifecycleAction.LAUNCH, idempotency_key)
        if cached is not None:
            return cached
        pending = _durable_pending_start(self._runtime_units.get(run_id, ()))
        if pending is not None:
            pending_key, generation = pending
            if pending_key != idempotency_key:
                raise ValueError(
                    f"run {run_id!r} has pending Start with a different idempotency key"
                )
            self._generations[run_id] = max(self._generations.get(run_id, 0), generation)
            return self._start_result(run_id, idempotency_key, generation, idempotent=False)
        self._generations[run_id] = self._generations.get(run_id, 0) + 1
        result = self._apply_action(run_id, LifecycleAction.LAUNCH, idempotency_key=idempotency_key)
        if idempotency_key and not defer_idempotency:
            self._idempotency_cache[(run_id, LifecycleAction.LAUNCH, idempotency_key)] = result
        return result

    def complete_start(
        self,
        run_id: str,
        *,
        idempotency_key: str,
        runtime_units: Sequence[runtime_pb2.RuntimeUnit],
    ) -> RuntimeExecutionResult:
        """Commit Start idempotency only after every scheduler intent is acknowledged."""
        if not idempotency_key:
            raise ValueError("Start idempotency_key is required")
        generations = {unit.generation for unit in runtime_units}
        if len(generations) != 1 or next(iter(generations)) <= 0:
            raise ValueError("completed Start requires one positive generation")
        generation = next(iter(generations))
        self._runtime_units[run_id] = tuple(clone_message(unit) for unit in runtime_units)
        result = self._start_result(run_id, idempotency_key, generation, idempotent=False)
        self._idempotency_cache[(run_id, LifecycleAction.LAUNCH, idempotency_key)] = result
        return result

    def _start_result(
        self, run_id: str, idempotency_key: str, generation: int, *, idempotent: bool
    ) -> RuntimeExecutionResult:
        return RuntimeExecutionResult(
            manifest=self._ensure_run(run_id),
            runtime_units=tuple(
                clone_message(unit) for unit in self._runtime_units.get(run_id, ())
            ),
            action=LifecycleAction.LAUNCH,
            generation=generation,
            cursor=stable_cursor(
                LifecycleAction.LAUNCH.value,
                run_id,
                generation,
                idempotency_key,
                False,
            ),
            component_results=(),
            idempotent=idempotent,
        )

    def status(self, run_id: str, *, idempotency_key: str = "") -> RuntimeExecutionResult:
        return self._apply_action(run_id, LifecycleAction.STATUS, idempotency_key=idempotency_key)

    def checkpoint(
        self,
        run_id: str,
        *,
        checkpoint_ref: str = "",
        idempotency_key: str = "",
    ) -> RuntimeExecutionResult:
        result = self._apply_action(
            run_id,
            LifecycleAction.CHECKPOINT,
            idempotency_key=idempotency_key,
        )
        ref = checkpoint_ref or f"checkpoint:{run_id}:{result.generation}"
        return RuntimeExecutionResult(
            manifest=result.manifest,
            runtime_units=result.runtime_units,
            action=result.action,
            generation=result.generation,
            cursor=result.cursor,
            component_results=result.component_results,
            idempotent=result.idempotent,
            checkpoint_ref=ref,
            reconciled=result.reconciled,
        )

    def reconcile(
        self,
        run_id: str,
        *,
        recovered_processes: Sequence[ComponentProcessRecord] = (),
        reported_action: LifecycleAction = LifecycleAction.STATUS,
    ) -> RuntimeExecutionResult:
        manifest = self._ensure_run(run_id)
        generation = self._generations.get(run_id, 0)
        records = list(self._processes.get(run_id, {}).values()) + list(recovered_processes)
        component_results: list[ComponentActionResult] = []
        for record in records:
            if record.generation != generation:
                continue
            adapter = self._component_for(manifest, record.component)
            call = adapter.lifecycle_call(manifest, LifecycleAction.STATUS)
            try:
                if self.process_driver is None:
                    raise AdapterUnavailableError("process driver is unavailable")
                status = self.process_driver.inspect(
                    handle=record.handle,
                    timeout_seconds=self._timeout_for(LifecycleAction.STATUS),
                )
                component_results.append(
                    ComponentActionResult(
                        component=record.component,
                        action=reported_action,
                        runner_kind=RunnerKind.PROCESS,
                        generation=generation,
                        requested_state=record.requested_state,
                        observed_state=status.state,
                        launch_spec=call.launch_spec,
                        process_handle=record.handle,
                        error=self._inspect_error(status),
                    )
                )
            except Exception as error:
                component_results.append(
                    self._failure_result(
                        adapter=adapter,
                        action=reported_action,
                        generation=generation,
                        error=error,
                        launch_spec=call.launch_spec,
                    )
                )
        return self._finalize(
            run_id,
            action=reported_action,
            generation=generation,
            desired_state=self._desired_states.get(run_id, runtime_pb2.RUNTIME_STATE_REQUESTED),
            component_results=component_results,
            idempotency_key="",
            reconciled=True,
        )

    def _apply_action(
        self,
        run_id: str,
        action: LifecycleAction,
        *,
        idempotency_key: str,
    ) -> RuntimeExecutionResult:
        manifest = self._ensure_run(run_id)
        cached = self._cached(run_id, action, idempotency_key)
        if cached is not None:
            return cached
        generation = self._generations.get(run_id, 0)
        desired_state = runtime_state_contract(action).requested_state
        self._desired_states[run_id] = desired_state
        if action is LifecycleAction.LAUNCH:
            return self._finalize(
                run_id,
                action=action,
                generation=generation,
                desired_state=desired_state,
                component_results=(),
                idempotency_key=idempotency_key,
                reconciled=False,
            )
        bundle = self.registry.bundle_for(manifest)
        component_results: list[ComponentActionResult] = []
        for component, adapter in _component_adapters(bundle):
            launch_spec = self._placeholder_launch_spec(manifest, component, action)
            try:
                call = adapter.lifecycle_call(manifest, action)
                launch_spec = call.launch_spec
            except Exception as error:
                component_results.append(
                    self._failure_result(
                        adapter=adapter,
                        action=action,
                        generation=generation,
                        error=error,
                        launch_spec=launch_spec,
                    )
                )
                continue
            component_results.append(
                self._run_component_command(
                    adapter=adapter,
                    generation=generation,
                    call=call,
                )
            )
        result = self._finalize(
            run_id,
            action=action,
            generation=generation,
            desired_state=desired_state,
            component_results=component_results,
            idempotency_key=idempotency_key,
            reconciled=False,
        )
        if result.ok and action is LifecycleAction.STATUS and self._processes.get(run_id):
            result = self.reconcile(run_id, reported_action=action)
            if idempotency_key:
                self._idempotency_cache[(run_id, action, idempotency_key)] = result
        return result

    def _run_component_command(
        self,
        *,
        adapter: ComponentAdapter,
        generation: int,
        call: LifecycleCall,
    ) -> ComponentActionResult:
        try:
            if self.command_driver is None:
                raise AdapterUnavailableError("command driver is unavailable")
            result = self.command_driver.run(
                argv=call.launch_spec.argv,
                env=call.launch_spec.env,
                working_directory=call.launch_spec.working_directory,
                timeout_seconds=self._timeout_for(call.action),
            )
            if result.exit_code != 0:
                return self._failure_result(
                    adapter=adapter,
                    action=call.action,
                    generation=generation,
                    error=_command_failure(call.action, result),
                    launch_spec=call.launch_spec,
                    exit_code=result.exit_code,
                    command_result=result,
                )
            observed_state = call.state_contract.immediate_observed_state
            if observed_state == runtime_pb2.RUNTIME_STATE_UNKNOWN:
                observed_state = call.state_contract.requested_state
            return ComponentActionResult(
                component=dict(call.launch_spec.env).get("TGSRL_COMPONENT", ""),
                action=call.action,
                runner_kind=RunnerKind.COMMAND,
                generation=generation,
                requested_state=call.state_contract.requested_state,
                observed_state=observed_state,
                launch_spec=call.launch_spec,
                command_result=result,
            )
        except Exception as error:
            return self._failure_result(
                adapter=adapter,
                action=call.action,
                generation=generation,
                error=error,
                launch_spec=call.launch_spec,
            )

    def _failure_result(
        self,
        *,
        adapter: ComponentAdapter,
        action: LifecycleAction,
        generation: int,
        error: Exception,
        launch_spec: LaunchSpec,
        exit_code: int | None = None,
        command_result: CommandResult | None = None,
    ) -> ComponentActionResult:
        contract = adapter.map_error(action, error)
        normalized = _normalize_action_error(contract, exit_code=exit_code)
        return ComponentActionResult(
            component=dict(launch_spec.env).get("TGSRL_COMPONENT", ""),
            action=action,
            runner_kind=(
                RunnerKind.PROCESS if action is LifecycleAction.LAUNCH else RunnerKind.COMMAND
            ),
            generation=generation,
            requested_state=runtime_state_contract(action).requested_state,
            observed_state=normalized.resulting_state,
            launch_spec=launch_spec,
            command_result=command_result,
            error=normalized,
        )

    def _inspect_error(self, status: ProcessStatus) -> NormalizedActionError | None:
        if status.state != runtime_pb2.RUNTIME_STATE_FAILED:
            return None
        return NormalizedActionError(
            kind=AdapterErrorKind.BACKEND_FAILURE,
            retryable=False,
            summary=status.detail or "process failed",
            resulting_state=runtime_pb2.RUNTIME_STATE_FAILED,
            exit_code=status.exit_code,
        )

    def _finalize(
        self,
        run_id: str,
        *,
        action: LifecycleAction,
        generation: int,
        desired_state: int,
        component_results: Sequence[ComponentActionResult],
        idempotency_key: str,
        reconciled: bool,
    ) -> RuntimeExecutionResult:
        manifest = self._ensure_run(run_id)
        records = self._processes.setdefault(run_id, {})
        for item in component_results:
            record = records.get(item.component)
            if record is None or record.generation != generation:
                continue
            records[item.component] = ComponentProcessRecord(
                component=record.component,
                generation=record.generation,
                handle=record.handle,
                requested_state=item.requested_state,
                observed_state=item.observed_state,
            )
        units = self._apply_unit_state(
            run_id,
            action=action,
            generation=generation,
            desired_state=desired_state,
            component_results=component_results,
        )
        result = RuntimeExecutionResult(
            manifest=manifest,
            runtime_units=units,
            action=action,
            generation=generation,
            cursor=stable_cursor(action.value, run_id, generation, idempotency_key, reconciled),
            component_results=tuple(component_results),
            idempotent=False,
            checkpoint_ref="",
            reconciled=reconciled,
        )
        if idempotency_key and action is not LifecycleAction.LAUNCH:
            self._idempotency_cache[(run_id, action, idempotency_key)] = result
        return result

    def _apply_unit_state(
        self,
        run_id: str,
        *,
        action: LifecycleAction,
        generation: int,
        desired_state: int,
        component_results: Sequence[ComponentActionResult],
    ) -> tuple[runtime_pb2.RuntimeUnit, ...]:
        first_error = next(
            (item.error for item in component_results if item.error is not None),
            None,
        )
        updated: list[runtime_pb2.RuntimeUnit] = []
        for existing in self._runtime_units.get(run_id, ()):
            unit = clone_message(existing)
            if action is LifecycleAction.LAUNCH:
                unit.generation = generation
            # RuntimeUnit.state is observed state and must remain SandboxEvent-driven.
            # Control actions only update desired/request metadata on the stored units.
            unit.status_reason = _state_reason(desired_state)
            if first_error is None:
                unit.error_code = ""
                unit.error_message = ""
                unit.exit_code = 0
            else:
                unit.error_code = first_error.kind
                unit.error_message = first_error.summary
                unit.exit_code = first_error.exit_code or 0
            updated.append(unit)
        self._runtime_units[run_id] = tuple(updated)
        return tuple(clone_message(unit) for unit in updated)

    def _cached(
        self,
        run_id: str,
        action: LifecycleAction,
        idempotency_key: str,
    ) -> RuntimeExecutionResult | None:
        if not idempotency_key:
            return None
        cached = self._idempotency_cache.get((run_id, action, idempotency_key))
        if cached is None:
            return None
        return RuntimeExecutionResult(
            manifest=cached.manifest,
            runtime_units=cached.runtime_units,
            action=cached.action,
            generation=cached.generation,
            cursor=cached.cursor,
            component_results=cached.component_results,
            idempotent=True,
            checkpoint_ref=cached.checkpoint_ref,
            reconciled=cached.reconciled,
        )

    def _hydrate_start_idempotency_cache(self, run_id: str) -> None:
        durable_retry = _durable_start_retry(self._runtime_units.get(run_id, ()))
        if durable_retry is None:
            return
        key, generation = durable_retry
        self._generations[run_id] = max(self._generations.get(run_id, 0), generation)
        manifest = self._ensure_run(run_id)
        runtime_units = tuple(clone_message(unit) for unit in self._runtime_units.get(run_id, ()))
        self._idempotency_cache[(run_id, LifecycleAction.LAUNCH, key)] = RuntimeExecutionResult(
            manifest=manifest,
            runtime_units=runtime_units,
            action=LifecycleAction.LAUNCH,
            generation=generation,
            cursor=stable_cursor(LifecycleAction.LAUNCH.value, run_id, generation, key, False),
            component_results=(),
            idempotent=False,
            checkpoint_ref="",
            reconciled=False,
        )

    def _timeout_for(self, action: LifecycleAction) -> float | None:
        return self.action_timeouts.get(action, self.default_timeout_seconds)

    def _ensure_run(self, run_id: str) -> runtime_pb2.RuntimeManifest:
        if run_id not in self._manifests:
            raise ValueError(f"unknown run_id: {run_id}")
        return clone_message(self._manifests[run_id])

    def _component_for(
        self,
        manifest: runtime_pb2.RuntimeManifest,
        component: str,
    ) -> ComponentAdapter:
        return dict(_component_adapters(self.registry.bundle_for(manifest)))[component]

    def _placeholder_launch_spec(
        self,
        manifest: runtime_pb2.RuntimeManifest,
        component: str,
        action: LifecycleAction,
    ) -> LaunchSpec:
        return LaunchSpec(
            argv=(),
            env=(
                ("TGSRL_COMPONENT", component),
                ("TGSRL_ACTION", action.value),
                ("TGSRL_RUN_ID", manifest.run_id),
                ("TGSRL_JOB_ID", manifest.job_id),
                ("TGSRL_TRACE_ID", manifest.trace_id),
            ),
        )
