"""Explicit integration with the public veRL 0.9 trainer surface.

This module deliberately uses structural typing and imports no veRL package. A
real veRL process passes its initialized trainer object to :func:`install_verl_control`; CPU
tests can exercise the same contract with small worker-group doubles.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import math
import os
import re
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from adapters.frameworks.verl_bridge import VerlWorkerBridge, WorkerIdentity

VERL_RUNTIME_VERSION = "0.9.0"
_GLOBAL_STEP = re.compile(r"(?:^|/)global_step_(\d+)(?:/|$)")


def _call(target: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    function = getattr(target, method, None)
    if not callable(function):
        raise RuntimeError(f"veRL object does not provide {method}()")
    result = function(*args, **kwargs)
    if not inspect.isawaitable(result):
        return result
    awaitable = cast(Any, result)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, awaitable).result()


def _nested(root: Any, path: Sequence[str], default: Any = None) -> Any:
    current = root
    for name in path:
        current = (
            current.get(name, default)
            if isinstance(current, Mapping)
            else getattr(current, name, default)
        )
        if current is default:
            return default
    return current


@dataclass(frozen=True)
class VerlObservation:
    event_type: str
    policy_lag: int
    sample_stale: bool
    effective_sample_size: float
    accepted_samples: int
    expected_samples: int
    duration_ms: float = 0.0
    items: int = 1
    gpu_active_ms: float = 0.0


@dataclass
class VerlTrainerCallbacks:
    """Map TGS-RL lifecycle calls to veRL 0.9 trainer and worker-group APIs."""

    trainer: Any
    safe_point_event: threading.Event
    stop_event: threading.Event
    checkpoint_root: Path | None = None
    device_id: str = ""
    safe_point_timeout_seconds: float = 30.0
    _paused: bool = False
    _rollout_sleeping: bool = False
    _offloaded: bool = False
    _stop_requested: bool = False
    _pause_requested: threading.Event = field(default_factory=threading.Event, init=False)
    _resume_allowed: threading.Event = field(default_factory=threading.Event, init=False)

    def __post_init__(self) -> None:
        if self.safe_point_timeout_seconds <= 0:
            raise ValueError("safe-point timeout must be positive")
        if self.checkpoint_root is None:
            configured = _nested(self.trainer, ("config", "trainer", "default_local_dir"), "")
            if configured:
                self.checkpoint_root = Path(str(configured))
        self._resume_allowed.set()

    def prepare_pause(self) -> bool:
        self._pause_requested.set()
        self._resume_allowed.clear()
        prepared = self.safe_point_event.wait(self.safe_point_timeout_seconds)
        if not prepared:
            self._pause_requested.clear()
            self._resume_allowed.set()
        return prepared

    def cancel_prepare(self) -> None:
        self._pause_requested.clear()
        self._resume_allowed.set()

    def checkpoint(self, checkpoint_ref: str) -> str:
        step = int(getattr(self.trainer, "global_steps", 0))
        target = Path(checkpoint_ref) if checkpoint_ref else self._checkpoint_target(step)
        if target.name != f"global_step_{step}":
            raise RuntimeError(f"veRL checkpoint target must end in global_step_{step}")
        target.mkdir(parents=True, exist_ok=True)
        actor = self._required("actor_rollout_wg")
        _call(actor, "save_checkpoint", str(target / "actor"), None, step)
        if bool(getattr(self.trainer, "use_critic", False)):
            _call(
                self._required("critic_wg"),
                "save_checkpoint",
                str(target / "critic"),
                None,
                step,
            )
        return str(target)

    def pause(self) -> None:
        self._paused = True
        self._resume_allowed.clear()
        try:
            _call(self._required("checkpoint_manager"), "abort_replicas")
        except Exception:
            self.cancel_wait()
            self._paused = False
            raise

    def sleep(self) -> None:
        manager = self._required("checkpoint_manager")
        self._paused = True
        self._resume_allowed.clear()
        try:
            _call(manager, "abort_replicas")
            _call(manager, "sleep_replicas")
            self._rollout_sleeping = True
        except Exception:
            self.cancel_wait()
            self._paused = False
            raise

    def offload(self) -> None:
        manager = self._required("checkpoint_manager")
        self._paused = True
        self._resume_allowed.clear()
        _call(manager, "abort_replicas")
        _call(manager, "sleep_replicas")
        self._rollout_sleeping = True
        _call(
            self._required("actor_rollout_wg"),
            "to",
            "cpu",
            model=True,
            optimizer=True,
            grad=True,
        )
        if bool(getattr(self.trainer, "use_critic", False)):
            _call(
                self._required("critic_wg"),
                "to",
                "cpu",
                model=True,
                optimizer=True,
                grad=True,
            )
        self._offloaded = True

    def reload(self, checkpoint_ref: str, device_id: str, profile: str) -> None:
        del profile
        if device_id and self.device_id and device_id != self.device_id:
            raise RuntimeError("veRL device replacement requires a new DRA/CDI workload generation")
        if not checkpoint_ref:
            raise RuntimeError("veRL reload requires a checkpoint reference")
        target = Path(checkpoint_ref)
        match = _GLOBAL_STEP.search(target.as_posix())
        if match is None:
            raise RuntimeError("veRL checkpoint path is missing global_step_<n>")
        target_step = int(match.group(1))
        if target_step != int(getattr(self.trainer, "global_steps", target_step)):
            raise RuntimeError("veRL checkpoint step does not match trainer global_steps")
        actor_path = target / "actor"
        _call(
            self._required("actor_rollout_wg"),
            "load_checkpoint",
            str(actor_path),
            del_local_after_load=False,
        )
        if bool(getattr(self.trainer, "use_critic", False)):
            critic_path = target / "critic"
            _call(
                self._required("critic_wg"),
                "load_checkpoint",
                str(critic_path),
                del_local_after_load=False,
            )
        self.trainer.global_steps = target_step
        _call(self._required("checkpoint_manager"), "wake_up_replicas")
        self._rollout_sleeping = False
        self._offloaded = False

    def stop(self, preserve_process: bool = False) -> None:
        manager = self._required("checkpoint_manager")
        self._paused = True
        self._resume_allowed.clear()
        try:
            _call(manager, "abort_replicas")
            if not self._rollout_sleeping:
                _call(manager, "sleep_replicas")
                self._rollout_sleeping = True
        except Exception:
            self._paused = False
            self._resume_allowed.set()
            raise
        self._stop_requested = not preserve_process
        self._pause_requested.clear()
        self._resume_allowed.set()

    def resume(self) -> None:
        manager = getattr(self.trainer, "checkpoint_manager", None)
        if manager is not None:
            if self._rollout_sleeping:
                _call(manager, "wake_up_replicas")
            _call(manager, "resume_generation_replicas")
        self._paused = self._rollout_sleeping = self._offloaded = False
        self._pause_requested.clear()
        self._resume_allowed.set()

    def update_policy(self, policy_version: str) -> None:
        normalized = policy_version.removeprefix("policy-")
        if not normalized.isdigit():
            raise RuntimeError("veRL policy version must use policy-<global_step>")
        _call(self._required("checkpoint_manager"), "update_weights", int(normalized))

    def emit_observation(
        self,
        bridge: VerlWorkerBridge,
        *,
        event_type: str,
        duration_ms: float = 0.0,
        items: int = 1,
        gpu_active_ms: float = 0.0,
        policy_lag: int = 0,
        sample_stale: bool = False,
        effective_sample_size: float | None = None,
        accepted_samples: int | None = None,
        expected_samples: int | None = None,
    ) -> None:
        accepted = items if accepted_samples is None else accepted_samples
        expected = items if expected_samples is None else expected_samples
        effective = (
            float(max(accepted, 0)) if effective_sample_size is None else effective_sample_size
        )
        bridge.observe(
            event_type=event_type,
            buffer_level=self._queue_depth(),
            policy_lag=max(0, policy_lag),
            sample_stale=sample_stale,
            effective_sample_size=max(0.0, effective),
            accepted_samples=max(0, accepted),
            expected_samples=max(0, expected),
            duration_ms=duration_ms,
            items=items,
            gpu_active_ms=gpu_active_ms,
        )

    def emit_step_observation(
        self, bridge: VerlWorkerBridge, metrics: Mapping[str, Any], *, items: int = 1
    ) -> None:
        """Project a veRL step's native metrics into one typed bridge observation."""
        observation = observation_from_metrics(metrics, items=items)
        self.emit_observation(bridge, **observation.__dict__)

    def _checkpoint_target(self, step: int) -> Path:
        if self.checkpoint_root is None:
            raise RuntimeError("veRL checkpoint root is unavailable")
        return self.checkpoint_root / f"global_step_{step}"

    def _required(self, attribute: str) -> Any:
        value = getattr(self.trainer, attribute, None)
        if value is None:
            raise RuntimeError(f"veRL {attribute} is unavailable")
        return value

    def _queue_depth(self) -> int:
        queue = getattr(self.trainer, "message_queue_client", None)
        if queue is not None and callable(getattr(queue, "get_queue_size_sync", None)):
            return int(queue.get_queue_size_sync())
        buffer = getattr(self.trainer, "replay_buffer", None)
        partitions = getattr(buffer, "partitions", {}) if buffer is not None else {}
        return sum(len(values) for values in partitions.values())

    def restore_bridge_state(self, bridge: VerlWorkerBridge) -> None:
        self._offloaded = bridge.offloaded
        self._rollout_sleeping = bridge.state == "sleeping"
        self._paused = bridge.state in {"paused", "sleeping"}
        self._stop_requested = bridge.state == "terminated"
        self._pause_requested.clear()
        if self._paused and not self._stop_requested:
            self._resume_allowed.clear()
        else:
            self._resume_allowed.set()

    def wait_until_resumed(self) -> bool:
        if not self._pause_requested.is_set():
            return self._stop_requested or self.stop_event.is_set()
        while not self._stop_requested and not self.stop_event.is_set():
            if self._resume_allowed.wait(0.05):
                break
        return self._stop_requested or self.stop_event.is_set()

    def finish_safe_point(self) -> None:
        if self._stop_requested:
            self.stop_event.set()

    def cancel_wait(self) -> None:
        self.cancel_prepare()


@dataclass
class VerlControlHook:
    """Control server and explicit safe-point hook for a veRL trainer loop."""

    bridge: VerlWorkerBridge
    callbacks: VerlTrainerCallbacks
    thread: threading.Thread | None = None
    _closed: bool = field(default=False, init=False)
    _safe_point_lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("veRL control hook is closed")
        if self.thread is not None and self.thread.is_alive():
            raise RuntimeError("veRL control hook is already running")
        self.thread = threading.Thread(target=self.bridge.serve_forever, daemon=True)
        self.thread.start()

    def safe_point(
        self,
        *,
        event_type: str = "phase_completed",
        duration_ms: float = 0.0,
        items: int = 1,
        gpu_active_ms: float = 0.0,
        policy_lag: int = 0,
        sample_stale: bool = False,
        effective_sample_size: float | None = None,
        accepted_samples: int | None = None,
        expected_samples: int | None = None,
    ) -> None:
        with self._safe_point_lock:
            self.callbacks.safe_point_event.set()
            try:
                self.callbacks.emit_observation(
                    self.bridge,
                    event_type=event_type,
                    duration_ms=duration_ms,
                    items=items,
                    gpu_active_ms=gpu_active_ms,
                    policy_lag=policy_lag,
                    sample_stale=sample_stale,
                    effective_sample_size=effective_sample_size,
                    accepted_samples=accepted_samples,
                    expected_samples=expected_samples,
                )
                self.callbacks.wait_until_resumed()
            finally:
                self.callbacks.safe_point_event.clear()
                self.callbacks.finish_safe_point()

    def should_stop(self) -> bool:
        return self.callbacks._stop_requested or self.callbacks.stop_event.is_set()

    def close(self, timeout_seconds: float = 2.0) -> None:
        self._closed = True
        self.callbacks.stop_event.set()
        self.callbacks.cancel_wait()
        self.bridge.shutdown()
        if self.thread is not None:
            self.thread.join(timeout=timeout_seconds)
            if self.thread.is_alive():
                raise RuntimeError("veRL control hook did not stop")


def install_verl_control(
    trainer: Any,
    *,
    identity: WorkerIdentity,
    socket_path: str | Path,
    trace_path: str | Path,
    state_path: str | Path | None = None,
    checkpoint_root: str | Path | None = None,
    safe_point_timeout_seconds: float = 30.0,
    verify_version: bool = True,
    trace_sink: Any = None,
) -> VerlControlHook:
    """Install the bridge around an initialized veRL 0.9 trainer."""
    if verify_version:
        try:
            from importlib.metadata import PackageNotFoundError, version

            installed = version("verl")
        except PackageNotFoundError as error:
            raise RuntimeError("veRL 0.9.0 is not installed in the worker environment") from error
        if installed != VERL_RUNTIME_VERSION:
            raise RuntimeError(
                f"unsupported veRL version {installed}; expected {VERL_RUNTIME_VERSION}"
            )
    safe_point_event = threading.Event()
    stop_event = threading.Event()
    callbacks = VerlTrainerCallbacks(
        trainer=trainer,
        safe_point_event=safe_point_event,
        stop_event=stop_event,
        checkpoint_root=Path(checkpoint_root) if checkpoint_root is not None else None,
        device_id=identity.device_id,
        safe_point_timeout_seconds=safe_point_timeout_seconds,
    )
    bridge = VerlWorkerBridge(
        socket_path=Path(socket_path),
        trace_path=Path(trace_path),
        state_path=Path(state_path) if state_path is not None else None,
        identity=identity,
        callbacks=callbacks,
        trace_sink=trace_sink,
    )
    callbacks.restore_bridge_state(bridge)
    return VerlControlHook(bridge=bridge, callbacks=callbacks)


def worker_identity_from_environment(
    environment: Mapping[str, str] | None = None,
) -> WorkerIdentity:
    """Build a bridge identity from Operator/bootstrap environment values."""
    values = os.environ if environment is None else environment
    required = (
        "TGSRL_RUN_ID",
        "TGSRL_JOB_ID",
        "TGSRL_TRACE_ID",
        "TGSRL_SANDBOX_ID",
        "TGSRL_BINDING_ID",
        "TGSRL_GENERATION",
    )
    missing = [name for name in required if not str(values.get(name, "")).strip()]
    if missing:
        raise ValueError("veRL worker identity is missing " + ", ".join(missing))
    generation = int(values["TGSRL_GENERATION"])
    if generation <= 0:
        raise ValueError("TGSRL_GENERATION must be positive")
    devices = [
        value.strip() for value in values.get("TGSRL_DEVICE_IDS", "").split(",") if value.strip()
    ]
    if len(set(devices)) != len(devices):
        raise ValueError("TGSRL_DEVICE_IDS must not contain duplicates")
    share = float(values.get("TGSRL_ACCELERATOR_SHARE", "0"))
    if not math.isfinite(share) or share < 0 or share > 1:
        raise ValueError("TGSRL_ACCELERATOR_SHARE must be within [0,1]")
    rank = int(values.get("RANK", "-1"))
    local_rank = int(values.get("LOCAL_RANK", "-1"))
    world_size = int(values.get("WORLD_SIZE", "0"))
    if rank < -1:
        raise ValueError("RANK must be at least -1")
    if local_rank < -1:
        raise ValueError("LOCAL_RANK must be at least -1")
    if world_size < 0:
        raise ValueError("WORLD_SIZE must be non-negative")
    if world_size > 0 and not 0 <= rank < world_size:
        raise ValueError("RANK must be within WORLD_SIZE when WORLD_SIZE is positive")
    return WorkerIdentity(
        run_id=values["TGSRL_RUN_ID"].strip(),
        job_id=values["TGSRL_JOB_ID"].strip(),
        trace_id=values["TGSRL_TRACE_ID"].strip(),
        sandbox_id=values["TGSRL_SANDBOX_ID"].strip(),
        role=values.get("TGSRL_WORKER_ROLE", "actor_rollout").strip() or "actor_rollout",
        generation=generation,
        policy_version=values.get("TGSRL_POLICY_VERSION", "policy-0").strip() or "policy-0",
        algorithm=values.get("TGSRL_ALGORITHM", "grpo").strip() or "grpo",
        binding_id=values.get("TGSRL_BINDING_ID", "").strip(),
        device_id=devices[0] if len(devices) == 1 else "",
        share=share,
        runtime_unit_id=values.get("TGSRL_RUNTIME_UNIT_ID", "").strip(),
        worker_id=values.get("TGSRL_WORKER_ID", values.get("TGSRL_RUNTIME_UNIT_ID", "")).strip(),
        rank=rank,
        local_rank=local_rank,
        world_size=world_size,
    )


def install_verl_control_from_environment(
    trainer: Any,
    *,
    environment: Mapping[str, str] | None = None,
    socket_path: str | Path | None = None,
    trace_path: str | Path | None = None,
    state_path: str | Path | None = None,
    checkpoint_root: str | Path | None = None,
    safe_point_timeout_seconds: float = 30.0,
    verify_version: bool = True,
    trace_sink: Any = None,
) -> VerlControlHook:
    """Install the veRL hook using the managed-workload environment contract."""
    values = os.environ if environment is None else environment
    resolved_socket = socket_path or values.get("TGSRL_VERL_CONTROL_SOCKET", "")
    resolved_trace = trace_path or values.get("TGSRL_VERL_TRACE_PATH", "")
    if not str(resolved_socket).strip() or not str(resolved_trace).strip():
        raise ValueError("veRL control socket and trace path are required")
    return install_verl_control(
        trainer,
        identity=worker_identity_from_environment(values),
        socket_path=resolved_socket,
        trace_path=resolved_trace,
        state_path=state_path or values.get("TGSRL_VERL_STATE_PATH") or None,
        checkpoint_root=checkpoint_root or values.get("TGSRL_VERL_CHECKPOINT_ROOT") or None,
        safe_point_timeout_seconds=safe_point_timeout_seconds,
        verify_version=verify_version,
        trace_sink=trace_sink,
    )


def observation_from_metrics(
    metrics: Mapping[str, Any], *, items: int = 1, event_type: str = "phase_completed"
) -> VerlObservation:
    """Normalize stable veRL 0.9 metric names without importing torch or Ray."""
    if items < 0:
        raise ValueError("observation item count must be non-negative")

    def number(names: Sequence[str], default: float) -> float:
        for name in names:
            if name in metrics:
                value = float(metrics[name])
                if not math.isfinite(value):
                    raise ValueError(f"veRL metric {name} must be finite")
                return value
        return default

    policy_lag = max(
        0,
        int(
            number(
                (
                    "training/off_policy/trajectory_staleness/max",
                    "trajectory_staleness",
                    "policy_lag",
                ),
                0,
            )
        ),
    )
    effective_sample_size = max(
        0.0,
        number(
            ("rollout_is_eff_sample_size", "training/rollout_is_eff_sample_size"),
            float(items),
        ),
    )
    expected_samples = max(
        0, int(number(("expected_samples", "training/expected_samples"), float(items)))
    )
    accepted_samples = max(
        0, int(number(("accepted_samples", "training/accepted_samples"), float(items)))
    )
    duration_ms = max(0.0, number(("duration_ms", "perf/time_per_step_ms"), 0.0))
    gpu_active_ms = max(0.0, number(("gpu_active_ms",), 0.0))
    return VerlObservation(
        event_type=event_type,
        policy_lag=policy_lag,
        sample_stale=bool(metrics.get("sample_stale", policy_lag > 0)),
        effective_sample_size=effective_sample_size,
        accepted_samples=accepted_samples,
        expected_samples=expected_samples,
        duration_ms=duration_ms,
        items=items,
        gpu_active_ms=gpu_active_ms,
    )
