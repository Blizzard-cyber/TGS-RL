"""First-party veRL lifecycle and observation bridge.

The bridge is intentionally independent of veRL internals: a veRL worker owns
the training callbacks and embeds :class:`VerlWorkerBridge`. TGS-RL controls it
through the same single-line JSON Unix-socket protocol used by the NVIDIA
runtime helper. This keeps process control, checkpointing, policy versions, and
quality observations explicit without pretending that an import alone controls
a running training process.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from tgsrl.v1 import execution_pb2, trace_pb2

from adapters.control import CommandResult, ControlRequest


class WorkerCallbacks(Protocol):
    """Training-process operations supplied by the embedding veRL worker."""

    def prepare_pause(self) -> bool: ...

    def checkpoint(self, checkpoint_ref: str) -> str: ...

    def pause(self) -> None: ...

    def sleep(self) -> None: ...

    def offload(self) -> None: ...

    def reload(self, checkpoint_ref: str, device_id: str, profile: str) -> None: ...

    def stop(self, preserve_process: bool = False) -> None: ...

    def resume(self) -> None: ...

    def update_policy(self, policy_version: str) -> None: ...


@dataclass
class ReferenceCallbacks:
    """Filesystem-backed callbacks for CPU integration and conformance tests."""

    checkpoint_directory: Path

    def prepare_pause(self) -> bool:
        return True

    def checkpoint(self, checkpoint_ref: str) -> str:
        self.checkpoint_directory.mkdir(parents=True, exist_ok=True)
        target = (
            Path(checkpoint_ref) if checkpoint_ref else self.checkpoint_directory / "latest.json"
        )
        target.write_text('{"checkpoint":true}\n', encoding="utf-8")
        return str(target)

    def offload(self) -> None:
        return None

    def pause(self) -> None:
        return None

    def sleep(self) -> None:
        return None

    def reload(self, checkpoint_ref: str, device_id: str, profile: str) -> None:
        del device_id, profile
        if checkpoint_ref and not Path(checkpoint_ref).is_file():
            raise FileNotFoundError(checkpoint_ref)

    def stop(self, preserve_process: bool = False) -> None:
        del preserve_process
        return None

    def resume(self) -> None:
        return None

    def update_policy(self, policy_version: str) -> None:
        del policy_version
        return None


@dataclass
class WorkerIdentity:
    run_id: str
    job_id: str
    trace_id: str
    sandbox_id: str
    role: str
    generation: int
    policy_version: str
    algorithm: str = "grpo"
    rollout_mode: int = trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC
    data_kind: int = trace_pb2.DATA_KIND_LIVE
    phase_id: str = "decode"
    phase_kind: int = execution_pb2.PHASE_KIND_DECODE
    binding_id: str = ""
    device_id: str = ""
    share: float = 0.0


@dataclass
class VerlWorkerBridge:
    """Cooperative lifecycle server embedded in one veRL worker process."""

    socket_path: Path
    trace_path: Path
    identity: WorkerIdentity
    callbacks: WorkerCallbacks
    trace_sink: Callable[[trace_pb2.TraceEvent], None] | None = None
    state_path: Path | None = None
    state: str = "running"
    safe_point: bool = False
    offloaded: bool = False
    ready: bool = True
    checkpoint_ref: str = ""
    max_receipts: int = 4096
    _sequence: int = 0
    _responses: dict[str, tuple[str, dict[str, Any]]] = field(default_factory=dict)
    _lock: Any = field(default_factory=threading.RLock, repr=False)
    _shutdown: threading.Event = field(default_factory=threading.Event)

    def __post_init__(self) -> None:
        if self.max_receipts <= 0:
            raise ValueError("max_receipts must be positive")
        if self.state_path is None:
            self.state_path = self.trace_path.with_suffix(".state.json")
        if self.state_path.resolve() == self.trace_path.resolve():
            raise ValueError("veRL worker state path must differ from trace path")
        self._restore_state()
        self._restore_trace_sequence()

    def serve_forever(self) -> None:
        """Serve lifecycle requests until :meth:`shutdown` is called."""
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        self.socket_path.unlink(missing_ok=True)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(self.socket_path))
            os.chmod(self.socket_path, 0o600)
            server.listen(8)
            server.settimeout(0.2)
            while not self._shutdown.is_set():
                try:
                    connection, _ = server.accept()
                except TimeoutError:
                    continue
                with connection:
                    try:
                        request = _read_request(connection)
                        response = self.handle(request)
                    except (ValueError, json.JSONDecodeError) as error:
                        response = self._response(accepted=False, error=str(error))
                    try:
                        connection.sendall(
                            (
                                json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n"
                            ).encode()
                        )
                    except OSError:
                        # The durable response remains available for an idempotent retry.
                        continue
        finally:
            server.close()
            self.socket_path.unlink(missing_ok=True)

    def shutdown(self) -> None:
        self._shutdown.set()

    def handle(self, request: dict[str, Any]) -> dict[str, Any]:
        """Apply one generation-fenced, idempotent worker request."""
        with self._lock:
            action = str(request.get("action", ""))
            key = str(request.get("idempotency_key", ""))
            sandbox_id = str(request.get("sandbox_id", ""))
            generation = int(request.get("generation", 0))
            if not action or not key:
                return self._response(
                    accepted=False, error="action and idempotency_key are required"
                )
            if action == "status":
                if sandbox_id != self.identity.sandbox_id or generation != self.identity.generation:
                    return self._response(
                        accepted=False, error="worker identity or generation mismatch"
                    )
                return self._response()
            digest = hashlib.sha256(
                json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            if key in self._responses:
                previous_digest, previous_response = self._responses[key]
                if previous_digest != digest:
                    return self._response(
                        accepted=False, error="idempotency key was reused with different content"
                    )
                return dict(previous_response)
            generation_matches = generation == self.identity.generation or (
                action == "reload" and generation == self.identity.generation + 1
            )
            if sandbox_id != self.identity.sandbox_id or not generation_matches:
                return self._response(
                    accepted=False, error="worker identity or generation mismatch"
                )
            if len(self._responses) >= self.max_receipts:
                return self._response(
                    accepted=False, error="worker idempotency journal capacity is exhausted"
                )
            pending = self._response(
                accepted=False, error="worker mutation outcome is not confirmed"
            )
            self._responses[key] = (digest, pending)
            try:
                self._persist_state()
            except OSError as error:
                self._responses.pop(key, None)
                return self._response(accepted=False, error=f"persist mutation intent: {error}")
            try:
                response = self._apply(action, request)
            except Exception as error:
                response = self._response(
                    accepted=False,
                    error=f"worker mutation outcome is not confirmed: {error}",
                )
            self._responses[key] = (digest, dict(response))
            try:
                self._persist_state()
            except OSError as error:
                return self._response(
                    accepted=False, error=f"worker mutation outcome is unknown: {error}"
                )
            return response

    def _apply(self, action: str, request: dict[str, Any]) -> dict[str, Any]:
        if action == "prepare_pause":
            self.safe_point = self.callbacks.prepare_pause()
            if not self.safe_point:
                raise RuntimeError("worker did not reach a safe point")
            self.ready = False
        elif action == "checkpoint":
            if not self.safe_point:
                raise RuntimeError("checkpoint requires a safe point")
            requested = str(request.get("checkpoint_ref", ""))
            self.checkpoint_ref = self.callbacks.checkpoint(requested)
        elif action in {"pause", "sleep"}:
            if not self.safe_point:
                raise RuntimeError(f"{action} requires a safe point")
            if action == "pause":
                self.callbacks.pause()
            else:
                self.callbacks.sleep()
            self.state, self.ready = ("paused" if action == "pause" else "sleeping"), False
        elif action == "offload":
            if not self.safe_point or not self.checkpoint_ref:
                raise RuntimeError("offload requires a safe point and checkpoint")
            self.callbacks.offload()
            self.state, self.offloaded, self.ready = "sleeping", True, False
        elif action == "reload":
            checkpoint_ref = str(request.get("checkpoint_ref", "")) or self.checkpoint_ref
            if not checkpoint_ref:
                raise RuntimeError("reload requires a checkpoint reference")
            self.callbacks.reload(
                checkpoint_ref, str(request.get("device_id", "")), str(request.get("profile", ""))
            )
            self.identity.generation = int(request["generation"])
            if request.get("device_id"):
                self.identity.device_id = str(request["device_id"])
            if request.get("binding_id"):
                self.identity.binding_id = str(request["binding_id"])
            if request.get("share") is not None:
                self.identity.share = float(request["share"])
            self.checkpoint_ref, self.offloaded, self.ready = checkpoint_ref, False, False
        elif action == "resume":
            self.callbacks.resume()
            self.state, self.safe_point, self.offloaded, self.ready = "running", False, False, True
        elif action == "stop":
            self.callbacks.stop(bool(request.get("preserve_process", False)))
            self.state, self.ready = "terminated", False
        elif action == "weight_update":
            policy_version = str(request.get("policy_version", "")).strip()
            if not policy_version:
                raise RuntimeError("weight_update requires a policy version")
            self.callbacks.update_policy(policy_version)
            self.identity.policy_version = policy_version
        else:
            raise RuntimeError(f"unsupported veRL worker action {action!r}")
        self._emit(action)
        return self._response()

    def observe(
        self,
        *,
        event_type: str,
        buffer_level: int,
        policy_lag: int,
        sample_stale: bool,
        effective_sample_size: float,
        accepted_samples: int,
        expected_samples: int,
        duration_ms: float = 0.0,
        items: int = 1,
        gpu_active_ms: float = 0.0,
    ) -> None:
        """Append one raw, scheduler-consumable quality observation."""
        with self._lock:
            contract_observation = {
                "policy_lag": policy_lag,
                "sample_stale": sample_stale,
                "buffer_level": buffer_level,
                "effective_sample_size": effective_sample_size,
                "accepted_samples": accepted_samples,
                "expected_samples": expected_samples,
                "safe_point": self.safe_point,
                "policy_version": self.identity.policy_version,
            }
            emitted = self._emit(
                event_type,
                buffer_level=buffer_level,
                duration_ms=duration_ms,
                items=items,
                gpu_active_ms=gpu_active_ms,
                contract_observation=contract_observation,
            )
            role = self.identity.role
            phase_id = self.identity.phase_id
            phase_kind = self.identity.phase_kind
            algorithm = self.identity.algorithm
            rollout_mode = self.identity.rollout_mode
            data_kind = self.identity.data_kind
        if self.trace_sink is not None:
            observed_at = datetime.fromisoformat(str(emitted["occurred_at"]))
            event_id = str(emitted["event_id"])
            sequence = int(emitted["sequence"])
            policy_version = str(emitted["policy_version"])
            safe_point = bool(emitted["safe_point"])
            observation = execution_pb2.ContractObservation(
                policy_lag=policy_lag,
                sample_stale=sample_stale,
                buffer_level=buffer_level,
                effective_sample_size=effective_sample_size,
                accepted_samples=accepted_samples,
                expected_samples=expected_samples,
                safe_point=safe_point,
                source="verl-worker-bridge",
                event_id=event_id,
                phase_id=role,
                policy_version=policy_version,
                sample_count=expected_samples,
                effective_sample_size_ratio=(
                    0.0 if expected_samples == 0 else effective_sample_size / expected_samples
                ),
            )
            observation.observed_at.FromDatetime(observed_at)
            trace_event = trace_pb2.TraceEvent(
                event_id=event_id,
                job_id=str(emitted["job_id"]),
                execution_id=str(emitted["run_id"]),
                phase_id=phase_id,
                event_type=_trace_event_type(event_type),
                algorithm=algorithm,
                rollout_mode=rollout_mode,
                policy_version=policy_version,
                decision_id=f"verl-observation:{emitted['sandbox_id']}",
                buffer_level=buffer_level,
                safe_point=safe_point,
                sequence=sequence,
                phase_kind=phase_kind,
                raw_phase_label=phase_id,
                sandbox_id=str(emitted["sandbox_id"]),
                stage_id=phase_id,
                run_id=str(emitted["run_id"]),
                data_kind=data_kind,
                trace_id=str(emitted["trace_id"]),
                generation=int(emitted["generation"]),
                contract_observation=observation,
            )
            trace_event.occurred_at.FromDatetime(observed_at)
            self.trace_sink(trace_event)

    def record_action(
        self,
        action: str,
        *,
        duration_ms: float,
        succeeded: bool,
        rolled_back: bool = False,
        recovery_time_ms: float = 0.0,
    ) -> None:
        """Record an executed control action for benchmark aggregation."""
        self._emit(
            "decision_applied",
            action=action,
            duration_ms=duration_ms,
            succeeded=succeeded,
            rolled_back=rolled_back,
            recovery_time_ms=recovery_time_ms,
        )

    def record_workload_completed(
        self, *, item_count: int, elapsed_ms: float, queue_depth: int
    ) -> None:
        """Record the terminal event used for end-to-end throughput metrics."""
        self._emit(
            "workload_completed",
            item_count=item_count,
            elapsed_ms=elapsed_ms,
            queue_depth=queue_depth,
        )

    def _emit(self, event_type: str, **extra: Any) -> dict[str, Any]:
        with self._lock:
            self._sequence += 1
            event = {
                "schema_version": "tgsrl.io/verl-trace/v1alpha1",
                "event_id": f"{self.identity.sandbox_id}:{self._sequence}",
                "event_type": event_type,
                "occurred_at": datetime.now(tz=UTC).isoformat(),
                "sequence": self._sequence,
                "run_id": self.identity.run_id,
                "job_id": self.identity.job_id,
                "trace_id": self.identity.trace_id,
                "sandbox_id": self.identity.sandbox_id,
                "role": self.identity.role,
                "generation": self.identity.generation,
                "policy_version": self.identity.policy_version,
                "safe_point": self.safe_point,
                "state": self.state,
                **extra,
            }
            self.trace_path.parent.mkdir(parents=True, exist_ok=True)
            with self.trace_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
            return event

    def _state_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "tgsrl.io/verl-worker-state/v1alpha1",
            "identity": {
                "run_id": self.identity.run_id,
                "job_id": self.identity.job_id,
                "trace_id": self.identity.trace_id,
                "sandbox_id": self.identity.sandbox_id,
                "role": self.identity.role,
                "generation": self.identity.generation,
                "policy_version": self.identity.policy_version,
                "binding_id": self.identity.binding_id,
                "device_id": self.identity.device_id,
                "share": self.identity.share,
                "algorithm": self.identity.algorithm,
                "rollout_mode": self.identity.rollout_mode,
                "data_kind": self.identity.data_kind,
                "phase_id": self.identity.phase_id,
                "phase_kind": self.identity.phase_kind,
            },
            "state": self.state,
            "safe_point": self.safe_point,
            "offloaded": self.offloaded,
            "ready": self.ready,
            "checkpoint_ref": self.checkpoint_ref,
            "sequence": self._sequence,
            "responses": {
                key: {"request_digest": digest, "response": response}
                for key, (digest, response) in sorted(self._responses.items())
            },
        }

    def _persist_state(self) -> None:
        if self.state_path is None:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{self.state_path.name}.", dir=self.state_path.parent
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                descriptor = -1
                json.dump(self._state_payload(), handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.state_path)
            directory = os.open(self.state_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)

    def _restore_state(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != "tgsrl.io/verl-worker-state/v1alpha1":
            raise ValueError("unsupported veRL worker state schema")
        identity = payload.get("identity")
        if not isinstance(identity, dict):
            raise ValueError("veRL worker state is missing identity")
        expected = (
            self.identity.run_id,
            self.identity.job_id,
            self.identity.trace_id,
            self.identity.sandbox_id,
            self.identity.role,
        )
        observed = tuple(
            str(identity.get(key, ""))
            for key in ("run_id", "job_id", "trace_id", "sandbox_id", "role")
        )
        if observed != expected:
            raise ValueError("veRL worker state identity does not match the configured worker")
        expected_contract = (
            self.identity.algorithm,
            self.identity.rollout_mode,
            self.identity.data_kind,
            self.identity.phase_id,
            self.identity.phase_kind,
        )
        observed_contract = (
            str(identity.get("algorithm", "")),
            int(identity.get("rollout_mode", 0)),
            int(identity.get("data_kind", 0)),
            str(identity.get("phase_id", "")),
            int(identity.get("phase_kind", 0)),
        )
        if observed_contract != expected_contract:
            raise ValueError("veRL worker state contract does not match the configured worker")
        self.identity.generation = int(identity["generation"])
        self.identity.policy_version = str(identity["policy_version"])
        self.identity.binding_id = str(identity.get("binding_id", ""))
        self.identity.device_id = str(identity.get("device_id", ""))
        self.identity.share = float(identity.get("share", 0.0))
        self.state = str(payload["state"])
        if self.state not in {"running", "paused", "sleeping", "terminated"}:
            raise ValueError("veRL worker state has an invalid lifecycle state")
        self.safe_point = bool(payload["safe_point"])
        self.offloaded = bool(payload["offloaded"])
        self.ready = bool(payload["ready"])
        self.checkpoint_ref = str(payload.get("checkpoint_ref", ""))
        self._sequence = int(payload.get("sequence", 0))
        responses = payload.get("responses", {})
        if not isinstance(responses, dict):
            raise ValueError("veRL worker state responses must be an object")
        self._responses = {}
        for key, record in responses.items():
            if not isinstance(record, dict) or not isinstance(record.get("response"), dict):
                raise ValueError("veRL worker state contains an invalid response record")
            if not record.get("request_digest"):
                raise ValueError("veRL worker state contains an invalid request digest")
            self._responses[str(key)] = (
                str(record.get("request_digest", "")),
                dict(record["response"]),
            )

    def _restore_trace_sequence(self) -> None:
        if not self.trace_path.exists():
            return
        try:
            for line in self.trace_path.read_text(encoding="utf-8").splitlines():
                event = json.loads(line)
                if isinstance(event, dict):
                    self._sequence = max(self._sequence, int(event.get("sequence", 0)))
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("veRL trace cannot restore its sequence") from error

    def _response(self, *, accepted: bool = True, error: str = "") -> dict[str, Any]:
        return {
            "accepted": accepted,
            "state": self.state,
            "generation": self.identity.generation,
            "safe_point": self.safe_point,
            "offloaded": self.offloaded,
            "ready": self.ready,
            "checkpoint_ref": self.checkpoint_ref,
            "binding_id": self.identity.binding_id,
            "device_id": self.identity.device_id,
            "share": self.identity.share,
            "error": error,
        }


def _read_request(connection: socket.socket) -> dict[str, Any]:
    payload = bytearray()
    while not payload.endswith(b"\n"):
        chunk = connection.recv(65536)
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > 1 << 20:
            raise ValueError("veRL control request exceeds 1 MiB")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("veRL control request must be a JSON object")
    return value


def request_worker(
    socket_path: str | Path, request: dict[str, Any], *, timeout_seconds: float = 30.0
) -> dict[str, Any]:
    """Send one lifecycle request over the managed-worker protocol."""
    if timeout_seconds <= 0:
        raise ValueError("worker control timeout must be positive")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout_seconds)
        client.connect(str(socket_path))
        client.sendall((json.dumps(request, sort_keys=True) + "\n").encode())
        return _read_request(client)


def _trace_event_type(value: str) -> int:
    return {
        "phase_started": trace_pb2.TRACE_EVENT_TYPE_PHASE_STARTED,
        "phase_completed": trace_pb2.TRACE_EVENT_TYPE_PHASE_COMPLETED,
        "sample_produced": trace_pb2.TRACE_EVENT_TYPE_SAMPLE_PRODUCED,
        "sample_consumed": trace_pb2.TRACE_EVENT_TYPE_SAMPLE_CONSUMED,
        "policy_published": trace_pb2.TRACE_EVENT_TYPE_POLICY_PUBLISHED,
        "backpressure_changed": trace_pb2.TRACE_EVENT_TYPE_BACKPRESSURE_CHANGED,
        "safe_point_reached": trace_pb2.TRACE_EVENT_TYPE_SAFE_POINT_REACHED,
        "decision_applied": trace_pb2.TRACE_EVENT_TYPE_DECISION_APPLIED,
    }.get(value, trace_pb2.TRACE_EVENT_TYPE_UNKNOWN)


def _request_socket(request: ControlRequest) -> str:
    environment = _request_environment(request)
    return environment.get("TGSRL_VERL_CONTROL_SOCKET", "").strip()


def _request_environment(request: ControlRequest) -> dict[str, str]:
    return {**os.environ, **dict(request.bridge_target.env)}


def _control_worker(request: ControlRequest) -> CommandResult:
    socket_path = _request_socket(request)
    if request.action.value in {"validate", "compile", "prepare"} and not socket_path:
        return CommandResult(exit_code=0)
    if not socket_path:
        return CommandResult(exit_code=69, stderr="TGSRL_VERL_CONTROL_SOCKET is required")
    actions = {
        "launch": ("status",),
        "pause": ("prepare_pause", "pause"),
        "checkpoint": ("prepare_pause", "checkpoint", "resume"),
        "sleep": ("prepare_pause", "checkpoint", "offload"),
        "wake": ("reload", "resume"),
        "terminate": ("stop",),
    }.get(request.action.value, (request.action.value,))
    environment = _request_environment(request)
    generation = int(environment.get("TGSRL_GENERATION", "1"))
    sandbox_id = environment.get("TGSRL_SANDBOX_ID", request.run_id)
    supplied_key = environment.get("TGSRL_IDEMPOTENCY_KEY", "").strip()
    key_material = "\x00".join(
        (request.trace_id, request.component, request.action.value, str(generation), supplied_key)
    )
    base_key = "verl:" + hashlib.sha256(key_material.encode()).hexdigest()
    checkpoint_ref = environment.get("TGSRL_CHECKPOINT_REF", "")
    response: dict[str, Any] = {}
    try:
        for action in actions:
            payload = {
                "action": action,
                "sandbox_id": sandbox_id,
                "generation": generation,
                "idempotency_key": f"{base_key}:{action}",
                "checkpoint_ref": checkpoint_ref,
                "device_id": environment.get("TGSRL_DEVICE_ID", ""),
                "profile": environment.get("TGSRL_DEVICE_PROFILE", ""),
                "binding_id": environment.get("TGSRL_BINDING_ID", ""),
                "share": float(environment.get("TGSRL_ACCELERATOR_SHARE", "0")),
                "policy_version": request.policy_version,
            }
            response = request_worker(
                socket_path,
                payload,
                timeout_seconds=float(environment.get("TGSRL_VERL_CONTROL_TIMEOUT", "30")),
            )
            if not response.get("accepted"):
                detail = str(response.get("error", "worker rejected request"))
                exit_code = 75 if "outcome is not confirmed" in detail else 1
                return CommandResult(exit_code=exit_code, stderr=detail)
            checkpoint_ref = str(response.get("checkpoint_ref", "")) or checkpoint_ref
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return CommandResult(exit_code=75, stderr=f"veRL control outcome is unknown: {error}")
    return CommandResult(exit_code=0, stdout=json.dumps(response, sort_keys=True))


def handle_lifecycle(request: ControlRequest) -> CommandResult:
    """Entry point used by :mod:`adapters.control`."""
    return _control_worker(request)
