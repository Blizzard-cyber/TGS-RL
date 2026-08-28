"""Runtime execution driver implementations."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field

from tgsrl.v1 import runtime_pb2

from adapters import CommandResult, LifecycleAction, ProcessHandle
from tgsrl_runtime.execution_types import DriverBehavior, ProcessStatus


@dataclass
class FakeProcessDriver:
    """In-memory process driver used by tests and fake-runtime execution."""

    next_pid: int = 1000
    start_calls: list[tuple[tuple[str, ...], tuple[tuple[str, str], ...], str, float | None]] = (
        field(default_factory=list)
    )
    inspect_calls: list[tuple[int, float | None]] = field(default_factory=list)
    _records: dict[int, tuple[str, str, ProcessStatus]] = field(default_factory=dict)
    _latest_pid: dict[tuple[str, str], int] = field(default_factory=dict)

    def start(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> ProcessHandle:
        self.start_calls.append((argv, env, working_directory, timeout_seconds))
        self.next_pid += 1
        pid = self.next_pid
        run_id = dict(env).get("TGSRL_RUN_ID", "")
        component = dict(env).get("TGSRL_COMPONENT", "")
        self._records[pid] = (
            run_id,
            component,
            ProcessStatus(state=runtime_pb2.RUNTIME_STATE_REQUESTED),
        )
        self._latest_pid[(run_id, component)] = pid
        return ProcessHandle(pid=pid, argv=argv, env=env, working_directory=working_directory)

    def inspect(
        self,
        *,
        handle: ProcessHandle,
        timeout_seconds: float | None = None,
    ) -> ProcessStatus:
        self.inspect_calls.append((handle.pid, timeout_seconds))
        if handle.pid not in self._records:
            raise FileNotFoundError(f"unknown process pid={handle.pid}")
        return self._records[handle.pid][2]

    def latest_handle(self, *, run_id: str, component: str) -> ProcessHandle:
        pid = self._latest_pid[(run_id, component)]
        record = next(
            item
            for item in self.start_calls
            if dict(item[1]).get("TGSRL_COMPONENT", "") == component
            and dict(item[1]).get("TGSRL_RUN_ID", "") == run_id
        )
        argv, env, working_directory, _timeout = record
        return ProcessHandle(pid=pid, argv=argv, env=env, working_directory=working_directory)

    def set_state(
        self,
        *,
        handle: ProcessHandle,
        state: int,
        exit_code: int | None = None,
        detail: str = "",
    ) -> None:
        run_id, component, _current = self._records[handle.pid]
        self._records[handle.pid] = (
            run_id,
            component,
            ProcessStatus(state=state, exit_code=exit_code, detail=detail),
        )

    def update_latest_component(
        self,
        *,
        run_id: str,
        component: str,
        state: int,
        exit_code: int | None = None,
        detail: str = "",
    ) -> None:
        pid = self._latest_pid[(run_id, component)]
        self._records[pid] = (
            run_id,
            component,
            ProcessStatus(state=state, exit_code=exit_code, detail=detail),
        )


@dataclass
class FakeCommandDriver:
    """In-memory command driver that mutates the fake process driver through the same contracts."""

    process_driver: FakeProcessDriver
    behaviors: dict[tuple[str, str, str], DriverBehavior] = field(default_factory=dict)
    calls: list[tuple[tuple[str, ...], tuple[tuple[str, str], ...], str, float | None]] = field(
        default_factory=list
    )

    def set_behavior(
        self,
        *,
        action: LifecycleAction,
        component: str,
        run_id: str,
        result: CommandResult | None = None,
        exception: Exception | None = None,
    ) -> None:
        self.behaviors[(action.value, component, run_id)] = DriverBehavior(
            result=result,
            exception=exception,
        )

    def run(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        self.calls.append((argv, env, working_directory, timeout_seconds))
        environment = dict(env)
        action = environment.get("TGSRL_ACTION", "")
        component = environment.get("TGSRL_COMPONENT", "")
        run_id = environment.get("TGSRL_RUN_ID", "")
        behavior = self.behaviors.get((action, component, run_id))
        if behavior is not None:
            if behavior.exception is not None:
                raise behavior.exception
            result = behavior.result if behavior.result is not None else CommandResult(exit_code=0)
        else:
            result = CommandResult(exit_code=0)
        if result.exit_code == 0:
            self._apply_success(action=action, component=component, run_id=run_id)
        return result

    def _apply_success(self, *, action: str, component: str, run_id: str) -> None:
        for target_component in (component, "runtime"):
            key = (run_id, target_component)
            if key not in self.process_driver._latest_pid:
                continue
            if action == LifecycleAction.PAUSE.value:
                self.process_driver.update_latest_component(
                    run_id=run_id,
                    component=target_component,
                    state=runtime_pb2.RUNTIME_STATE_PAUSED,
                    detail="paused",
                )
            elif action == LifecycleAction.RESUME.value:
                self.process_driver.update_latest_component(
                    run_id=run_id,
                    component=target_component,
                    state=runtime_pb2.RUNTIME_STATE_RUNNING,
                    detail="resumed",
                )
            elif action == LifecycleAction.STOP.value:
                self.process_driver.update_latest_component(
                    run_id=run_id,
                    component=target_component,
                    state=runtime_pb2.RUNTIME_STATE_SLEEPING,
                    exit_code=0,
                    detail="stopped",
                )
            elif action == LifecycleAction.TERMINATE.value:
                self.process_driver.update_latest_component(
                    run_id=run_id,
                    component=target_component,
                    state=runtime_pb2.RUNTIME_STATE_TERMINATED,
                    exit_code=0,
                    detail="terminated",
                )
            elif action == LifecycleAction.CHECKPOINT.value:
                self.process_driver.update_latest_component(
                    run_id=run_id,
                    component=target_component,
                    state=runtime_pb2.RUNTIME_STATE_RUNNING,
                    detail="checkpointed",
                )


@dataclass
class SubprocessDriver:
    """OS-backed driver used for real runtime profiles."""

    _children: dict[int, subprocess.Popen[bytes]] = field(default_factory=dict)

    def run(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> CommandResult:
        completed = subprocess.run(
            argv,
            check=False,
            capture_output=True,
            cwd=working_directory or None,
            env=self._merged_env(env),
            text=True,
            timeout=timeout_seconds,
        )
        return CommandResult(
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )

    def start(
        self,
        *,
        argv: tuple[str, ...],
        env: tuple[tuple[str, str], ...],
        working_directory: str,
        timeout_seconds: float | None = None,
    ) -> ProcessHandle:
        del timeout_seconds
        process = subprocess.Popen(
            argv,
            cwd=working_directory or None,
            env=self._merged_env(env),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._children[process.pid] = process
        return ProcessHandle(
            pid=process.pid,
            argv=argv,
            env=env,
            working_directory=working_directory,
        )

    def inspect(
        self,
        *,
        handle: ProcessHandle,
        timeout_seconds: float | None = None,
    ) -> ProcessStatus:
        del timeout_seconds
        process = self._children.get(handle.pid)
        if process is None:
            raise FileNotFoundError(f"unknown process pid={handle.pid}")
        return_code = process.poll()
        if return_code is None:
            return ProcessStatus(state=runtime_pb2.RUNTIME_STATE_RUNNING)
        if return_code == 0:
            return ProcessStatus(
                state=runtime_pb2.RUNTIME_STATE_TERMINATED,
                exit_code=0,
                detail="process exited cleanly",
            )
        return ProcessStatus(
            state=runtime_pb2.RUNTIME_STATE_FAILED,
            exit_code=return_code,
            detail=f"process exited {return_code}",
        )

    def _merged_env(self, env: tuple[tuple[str, str], ...]) -> dict[str, str]:
        merged = dict(os.environ)
        merged.update(dict(env))
        return merged
