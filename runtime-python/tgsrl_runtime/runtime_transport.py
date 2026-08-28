"""gRPC transport adapters backed by the runtime supervisor core."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Any, Protocol

import grpc
from tgsrl.v1 import (
    control_pb2,
    experiment_pb2,
    experiment_pb2_grpc,
    operator_pb2,
    runtime_pb2,
    runtime_pb2_grpc,
)

from adapters.compliance.runtime import LifecycleAction
from tgsrl_runtime.proto_utils import decode_page_token, encode_page_token
from tgsrl_runtime.runtime_errors import RuntimeLifecycleError
from tgsrl_runtime.supervisor import _cursor

if TYPE_CHECKING:
    from tgsrl_runtime.supervisor import RuntimeSupervisor

_DEFINITE_OPERATOR_REJECTION_CODES = frozenset(
    {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.UNIMPLEMENTED,
    }
)


async def _abort_mutation(context: grpc.aio.ServicerContext[Any, Any], error: Exception) -> None:
    """Map public mutation failures consistently without hiding dependency status."""
    if isinstance(error, grpc.aio.AioRpcError):
        code = error.code()
        detail = error.details() or str(error)
    elif isinstance(error, TimeoutError):
        code = grpc.StatusCode.DEADLINE_EXCEEDED
        detail = str(error)
    elif isinstance(error, RuntimeError) and not isinstance(error, RuntimeLifecycleError):
        code = grpc.StatusCode.UNAVAILABLE
        detail = str(error)
    else:
        code = grpc.StatusCode.FAILED_PRECONDITION
        detail = str(error)
    await context.abort(code, detail)


async def _abort_read_create(context: grpc.aio.ServicerContext[Any, Any], error: Exception) -> None:
    if isinstance(error, KeyError) or (
        isinstance(error, RuntimeLifecycleError) and str(error).startswith("unknown ")
    ):
        code = grpc.StatusCode.NOT_FOUND
    elif isinstance(error, RuntimeLifecycleError):
        code = grpc.StatusCode.FAILED_PRECONDITION
    else:
        code = grpc.StatusCode.INVALID_ARGUMENT
    await context.abort(code, str(error))


class OperatorControlHook(Protocol):
    """Narrow Runtime-to-Operator lifecycle control dependency."""

    async def apply_runtime_control(
        self, request: operator_pb2.ApplyRuntimeControlRequest
    ) -> operator_pb2.ApplyRuntimeControlResponse:
        """Submit one idempotent backend lifecycle request."""


class JobControlReporter(Protocol):
    """Narrow observed-status reporting dependency."""

    async def report_component_status(
        self, component_status: control_pb2.ComponentStatus
    ) -> control_pb2.ComponentStatus | None:
        """Report one Runtime-owned observed status."""


class RuntimeControlServicer(runtime_pb2_grpc.RuntimeControlServiceServicer):
    """gRPC RuntimeControlService backed by RuntimeSupervisor."""

    def __init__(
        self,
        supervisor: RuntimeSupervisor,
        *,
        operator_client: OperatorControlHook | None = None,
        job_control_reporter: JobControlReporter | None = None,
    ) -> None:
        self._supervisor = supervisor
        self._operator_client = operator_client
        self._job_control_reporter = job_control_reporter

    async def ValidateRuntime(
        self,
        request: runtime_pb2.ValidateRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.ValidateRuntimeRequest, runtime_pb2.ValidateRuntimeResponse
        ],
    ) -> runtime_pb2.ValidateRuntimeResponse:
        try:
            async with self._supervisor.lifecycle.lock_for_run(request.manifest.run_id):
                response = self._supervisor.validate_runtime(request)
                if not response.valid:
                    await context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "; ".join(response.diagnostics),
                    )
                return response
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def CompileRuntime(
        self,
        request: runtime_pb2.CompileRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.CompileRuntimeRequest, runtime_pb2.CompileRuntimeResponse
        ],
    ) -> runtime_pb2.CompileRuntimeResponse:
        try:
            async with self._supervisor.lifecycle.lock_for_run(request.manifest.run_id):
                return self._supervisor.compile_runtime(request)
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def PrepareRuntime(
        self,
        request: runtime_pb2.PrepareRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.PrepareRuntimeRequest, runtime_pb2.PrepareRuntimeResponse
        ],
    ) -> runtime_pb2.PrepareRuntimeResponse:
        try:
            async with self._supervisor.lifecycle.lock_for_run(request.run_id):
                return self._supervisor.prepare_runtime(request)
        except (grpc.aio.AioRpcError, KeyError, RuntimeError, ValueError) as error:
            await _abort_mutation(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def StartRuntime(
        self,
        request: runtime_pb2.StartRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.StartRuntimeRequest, runtime_pb2.StartRuntimeResponse
        ],
    ) -> runtime_pb2.StartRuntimeResponse:
        try:
            return await self._supervisor.start_runtime(request)
        except (grpc.aio.AioRpcError, KeyError, RuntimeError, ValueError) as error:
            await _abort_mutation(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def PauseRuntime(
        self,
        request: runtime_pb2.PauseRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.PauseRuntimeRequest, runtime_pb2.PauseRuntimeResponse
        ],
    ) -> runtime_pb2.PauseRuntimeResponse:
        try:
            return await self._stage_and_dispatch(
                request,
                LifecycleAction.PAUSE,
                lambda: self._supervisor.pause_runtime(request),
            )
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), error.details() or str(error))
            raise AssertionError("context.abort returned unexpectedly") from error
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def ResumeRuntime(
        self,
        request: runtime_pb2.ResumeRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.ResumeRuntimeRequest, runtime_pb2.ResumeRuntimeResponse
        ],
    ) -> runtime_pb2.ResumeRuntimeResponse:
        try:
            return await self._stage_and_dispatch(
                request,
                LifecycleAction.RESUME,
                lambda: self._supervisor.resume_runtime(request),
            )
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), error.details() or str(error))
            raise AssertionError("context.abort returned unexpectedly") from error
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def CheckpointRuntime(
        self,
        request: runtime_pb2.CheckpointRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.CheckpointRuntimeRequest, runtime_pb2.CheckpointRuntimeResponse
        ],
    ) -> runtime_pb2.CheckpointRuntimeResponse:
        try:
            async with self._supervisor.lifecycle.lock_for_run(request.run_id):
                return self._supervisor.checkpoint_runtime(request)
        except (grpc.aio.AioRpcError, KeyError, RuntimeError, ValueError) as error:
            await _abort_mutation(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def StopRuntime(
        self,
        request: runtime_pb2.StopRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.StopRuntimeRequest, runtime_pb2.StopRuntimeResponse
        ],
    ) -> runtime_pb2.StopRuntimeResponse:
        try:
            return await self._stage_and_dispatch(
                request,
                LifecycleAction.STOP,
                lambda: self._supervisor.stop_runtime(request),
            )
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), error.details() or str(error))
            raise AssertionError("context.abort returned unexpectedly") from error
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def TerminateRuntime(
        self,
        request: runtime_pb2.TerminateRuntimeRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.TerminateRuntimeRequest, runtime_pb2.TerminateRuntimeResponse
        ],
    ) -> runtime_pb2.TerminateRuntimeResponse:
        try:
            return await self._stage_and_dispatch(
                request,
                LifecycleAction.TERMINATE,
                lambda: self._supervisor.terminate_runtime(request),
            )
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), error.details() or str(error))
            raise AssertionError("context.abort returned unexpectedly") from error
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def GetRuntimeManifest(
        self,
        request: runtime_pb2.GetRuntimeManifestRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.GetRuntimeManifestRequest, runtime_pb2.GetRuntimeManifestResponse
        ],
    ) -> runtime_pb2.GetRuntimeManifestResponse:
        try:
            return self._supervisor.get_runtime_manifest(request)
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def ListRuntimeUnits(
        self,
        request: runtime_pb2.ListRuntimeUnitsRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.ListRuntimeUnitsRequest, runtime_pb2.ListRuntimeUnitsResponse
        ],
    ) -> runtime_pb2.ListRuntimeUnitsResponse:
        try:
            return self._supervisor.list_runtime_units(request)
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def ListSandboxes(
        self,
        request: runtime_pb2.ListSandboxesRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.ListSandboxesRequest, runtime_pb2.ListSandboxesResponse
        ],
    ) -> runtime_pb2.ListSandboxesResponse:
        try:
            return self._supervisor.list_sandboxes(request)
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def PublishSandboxEvent(
        self,
        request: runtime_pb2.PublishSandboxEventRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.PublishSandboxEventRequest, runtime_pb2.PublishSandboxEventResponse
        ],
    ) -> runtime_pb2.PublishSandboxEventResponse:
        try:
            response = self._supervisor.publish_sandbox_event(request)
            await self._report_current_observation(response.event.run_id)
            return response
        except (grpc.aio.AioRpcError, KeyError, RuntimeError, ValueError) as error:
            await _abort_mutation(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def _report_current_observation(self, run_id: str) -> None:
        if self._job_control_reporter is None:
            return
        await self._job_control_reporter.report_component_status(
            self._supervisor.get_component_status(run_id, "runtime")
        )

    async def _dispatch_backend_control(
        self,
        request: (
            runtime_pb2.PauseRuntimeRequest
            | runtime_pb2.ResumeRuntimeRequest
            | runtime_pb2.StopRuntimeRequest
            | runtime_pb2.TerminateRuntimeRequest
        ),
        action: LifecycleAction,
    ) -> None:
        if self._operator_client is None:
            raise RuntimeLifecycleError(
                "operator lifecycle control is unavailable; configure --operator-target"
            )
        control_request = self._supervisor.lifecycle.build_operator_control_request(
            request, action=action
        )
        response = await self._operator_client.apply_runtime_control(control_request)
        if not response.accepted:
            detail = response.detail or f"operator rejected {action.value} request"
            raise RuntimeLifecycleError(detail)

    async def _stage_and_dispatch[ResponseT](
        self,
        request: (
            runtime_pb2.PauseRuntimeRequest
            | runtime_pb2.ResumeRuntimeRequest
            | runtime_pb2.StopRuntimeRequest
            | runtime_pb2.TerminateRuntimeRequest
        ),
        action: LifecycleAction,
        stage: Callable[[], ResponseT],
    ) -> ResponseT:
        """Persist desired state before dispatch, rolling it back on failure."""
        lock = self._supervisor.lifecycle.lock_for_run(request.run_id)
        async with lock:
            snapshot = self._supervisor.runtime_units.list(request.run_id)
            try:
                response = stage()
                await self._dispatch_backend_control(request, action)
            except grpc.aio.AioRpcError as error:
                if error.code() in _DEFINITE_OPERATOR_REJECTION_CODES:
                    self._restore_desired_state(request.run_id, snapshot)
                raise
            except (KeyError, RuntimeLifecycleError, ValueError):
                self._restore_desired_state(request.run_id, snapshot)
                raise
            return response

    def _restore_desired_state(self, run_id: str, snapshot: list[runtime_pb2.RuntimeUnit]) -> None:
        """Restore only fields staged by lifecycle control, preserving observations."""
        original_reason = {unit.runtime_unit_id: unit.status_reason for unit in snapshot}
        restored = self._supervisor.runtime_units.list(run_id)
        for unit in restored:
            if unit.runtime_unit_id in original_reason:
                unit.status_reason = original_reason[unit.runtime_unit_id]
        self._supervisor.runtime_units.put_many(run_id, restored)
        self._supervisor.persistence.save_runtime_units(restored)

    async def GetRuntimeStatus(
        self,
        request: runtime_pb2.GetRuntimeStatusRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.GetRuntimeStatusRequest, runtime_pb2.GetRuntimeStatusResponse
        ],
    ) -> runtime_pb2.GetRuntimeStatusResponse:
        try:
            return self._supervisor.get_runtime_status(request)
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def WatchRuntimeEvents(
        self,
        request: runtime_pb2.WatchRuntimeEventsRequest,
        context: grpc.aio.ServicerContext[
            runtime_pb2.WatchRuntimeEventsRequest, runtime_pb2.WatchRuntimeEventsResponse
        ],
    ) -> AsyncIterator[runtime_pb2.WatchRuntimeEventsResponse]:
        try:
            for response in self._supervisor.watch_runtime_events(request):
                yield response
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            return


class ExperimentServicer(experiment_pb2_grpc.ExperimentServiceServicer):
    """gRPC ExperimentService backed by RuntimeSupervisor."""

    def __init__(self, supervisor: RuntimeSupervisor) -> None:
        self._supervisor = supervisor

    async def ListReplays(
        self,
        request: experiment_pb2.ListReplaysRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.ListReplaysRequest, experiment_pb2.ListReplaysResponse
        ],
    ) -> experiment_pb2.ListReplaysResponse:
        filters = (request.after_replay_id,)
        try:
            start = decode_page_token(request.page_token, scope="list-replays", filters=filters)
            items = self._supervisor.experiments.store.list_replays()
            if request.after_replay_id:
                items = [item for item in items if item.replay_id > request.after_replay_id]
            limit = int(request.limit or len(items) or 1)
            if limit <= 0:
                raise ValueError("limit must be positive")
            page = items[start : start + limit]
            next_offset = start + len(page)
            next_page_token = (
                encode_page_token(scope="list-replays", filters=filters, offset=next_offset)
                if next_offset < len(items)
                else ""
            )
            return experiment_pb2.ListReplaysResponse(
                replays=page,
                next_page_token=next_page_token,
                cursor=_cursor("list-replays", next_page_token, *filters),
            )
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def CreateReplay(
        self,
        request: experiment_pb2.CreateReplayRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.CreateReplayRequest, experiment_pb2.CreateReplayResponse
        ],
    ) -> experiment_pb2.CreateReplayResponse:
        try:
            return self._supervisor.create_replay(request)
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def GetReplay(
        self,
        request: experiment_pb2.GetReplayRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.GetReplayRequest, experiment_pb2.GetReplayResponse
        ],
    ) -> experiment_pb2.GetReplayResponse:
        try:
            replay = self._supervisor.experiments.store.get_replay(request.replay_id)
            return experiment_pb2.GetReplayResponse(replay=replay, cursor=replay.cursor)
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def ApplyReplayCommand(
        self,
        request: experiment_pb2.ApplyReplayCommandRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.ApplyReplayCommandRequest, experiment_pb2.ApplyReplayCommandResponse
        ],
    ) -> experiment_pb2.ApplyReplayCommandResponse:
        try:
            return await self._supervisor.apply_replay_command(request)
        except (grpc.aio.AioRpcError, KeyError, RuntimeError, ValueError) as error:
            await _abort_mutation(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def GetReplayStatus(
        self,
        request: experiment_pb2.GetReplayStatusRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.GetReplayStatusRequest, experiment_pb2.GetReplayStatusResponse
        ],
    ) -> experiment_pb2.GetReplayStatusResponse:
        try:
            replay = self._supervisor.experiments.store.get_replay(request.replay_id)
            return experiment_pb2.GetReplayStatusResponse(replay=replay, cursor=replay.cursor)
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def ListExperiments(
        self,
        request: experiment_pb2.ListExperimentsRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.ListExperimentsRequest, experiment_pb2.ListExperimentsResponse
        ],
    ) -> experiment_pb2.ListExperimentsResponse:
        filters = (request.after_experiment_id,)
        try:
            start = decode_page_token(request.page_token, scope="list-experiments", filters=filters)
            items = self._supervisor.experiments.store.list_experiments()
            if request.after_experiment_id:
                items = [item for item in items if item.experiment_id > request.after_experiment_id]
            limit = int(request.limit or len(items) or 1)
            if limit <= 0:
                raise ValueError("limit must be positive")
            page = items[start : start + limit]
            next_offset = start + len(page)
            next_page_token = (
                encode_page_token(scope="list-experiments", filters=filters, offset=next_offset)
                if next_offset < len(items)
                else ""
            )
            return experiment_pb2.ListExperimentsResponse(
                experiments=page,
                next_page_token=next_page_token,
                cursor=_cursor("list-experiments", next_page_token, *filters),
            )
        except ValueError as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
            raise AssertionError("context.abort returned unexpectedly") from error

    async def CreateExperiment(
        self,
        request: experiment_pb2.CreateExperimentRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.CreateExperimentRequest, experiment_pb2.CreateExperimentResponse
        ],
    ) -> experiment_pb2.CreateExperimentResponse:
        try:
            return self._supervisor.create_experiment(request)
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error

    async def GetExperiment(
        self,
        request: experiment_pb2.GetExperimentRequest,
        context: grpc.aio.ServicerContext[
            experiment_pb2.GetExperimentRequest, experiment_pb2.GetExperimentResponse
        ],
    ) -> experiment_pb2.GetExperimentResponse:
        try:
            experiment = self._supervisor.experiments.store.get_experiment(request.experiment_id)
            return experiment_pb2.GetExperimentResponse(
                experiment=experiment, cursor=experiment.cursor
            )
        except (KeyError, RuntimeLifecycleError, ValueError) as error:
            await _abort_read_create(context, error)
            raise AssertionError("context.abort returned unexpectedly") from error
