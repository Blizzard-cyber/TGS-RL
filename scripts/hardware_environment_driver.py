"""Kubernetes accelerator adapter for repository hardware validation campaigns.

The campaign executor owns experiment ordering and evidence validation. This
module performs one requested environment operation and reports facts read
from TGS-RL, Kubernetes, the selected realization backend, or the managed
workload. It never selects a device and never mutates an allocation.
"""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import math
import os
import platform
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from urllib import error, parse, request

ROOT = Path(__file__).resolve().parents[1]
REQUEST_SCHEMA = "tgsrl.io/hardware-driver-request/v1alpha1"
RESPONSE_SCHEMA = "tgsrl.io/hardware-driver-response/v1alpha1"
CONFIG_SCHEMA = "tgsrl.io/hardware-environment/v1alpha1"
HOOK_REQUEST_SCHEMA = "tgsrl.io/hardware-driver-hook-request/v1alpha1"
HOOK_RESPONSE_SCHEMA = "tgsrl.io/hardware-driver-hook-response/v1alpha1"
STATE_SCHEMA = "tgsrl.io/hardware-driver-state/v1alpha1"
NVIDIA_DRIVER = "gpu.nvidia.com"
PROFILE_CLASS = {"full-gpu": "gpu.nvidia.com", "mig": "mig.nvidia.com"}
PROFILE_TYPE = {"full-gpu": "gpu", "mig": "mig"}
EXECUTION_MODES = frozenset({"kubernetes-dra", "hami-vgpu"})
HAMI_PROFILE = "hami-vgpu"
HAMI_SCHEDULER = "hami-scheduler"
HAMI_REGISTER_ANNOTATION = "hami.io/node-nvidia-register"
HAMI_ALLOCATED_ANNOTATION = "hami.io/vgpu-devices-allocated"
HAMI_USE_UUID_ANNOTATION = "nvidia.com/use-gpuuuid"
HAMI_MODE_ANNOTATION = "nvidia.com/vgpu-mode"
HAMI_CORE_RESOURCE = "nvidia.com/gpucores"
HAMI_MEMORY_PERCENT_RESOURCE = "nvidia.com/gpumem-percentage"
HAMI_GPU_RESOURCE = "nvidia.com/gpu"
HAMI_EXPECTED_CORE_ANNOTATION = "tgsrl.io/hami-core-percent"
HAMI_EXPECTED_MEMORY_ANNOTATION = "tgsrl.io/hami-memory-mib"
RUN_OPERATIONS = frozenset(
    {
        "provision",
        "apply_action",
        "apply_worker_action",
        "launch",
        "verify_device_identity",
        "inject_fault",
        "recover_fault",
        "measure",
        "stop",
        "cleanup",
    }
)
DIRECT_COMMANDS = {"pause": "pause", "resume": "resume"}
SCHEDULER_HOOK_ACTIONS = frozenset({"rebind", "set_share", "set_priority", "offload"})
WORKER_HOOK_ACTIONS = frozenset({"checkpoint", "reload", "rollback"})
PROVIDER_ACTIONS = frozenset(
    {
        "bind",
        "rebind",
        "set_share",
        "set_priority",
        "offload",
        "pause",
        "resume",
        "sleep",
        "recreate",
        "release",
        "resize",
    }
)
UUID_PATTERN = re.compile(r"(?:GPU|MIG)-[A-Za-z0-9][A-Za-z0-9./_-]*")
PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
DNS_LABEL_PATTERN = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
MAX_JSON_BYTES = 4 << 20
MAX_STATE_BYTES = 64 << 20
MAX_TRACE_BYTES = 64 << 20
MAX_EVENTS = 100_000
SENSITIVE_NAMES = frozenset(
    {
        "api_key",
        "access_token",
        "authorization",
        "bearer_token",
        "client_secret",
        "credential",
        "credentials",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "token",
    }
)
TARGET_FIELDS = frozenset(
    {
        "gateway_url",
        "namespace",
        "kube_context",
        "kubectl",
        "job_template",
        "gpu_profile",
        "execution_mode",
        "worker_registry_url",
        "trace_command",
        "action_hooks",
        "fault_hooks",
        "operation_timeout_seconds",
        "poll_interval_seconds",
        "variables",
    }
)

JsonObject = dict[str, Any]


class DriverError(RuntimeError):
    """A fail-closed environment or contract error."""


class TerminalDriverError(DriverError):
    """A terminal operation result that polling must not retry."""


def _read_json(path: Path, *, maximum: int = MAX_JSON_BYTES) -> JsonObject:
    if path.is_symlink() or not path.is_file():
        raise DriverError(f"JSON file is missing or is a symbolic link: {path}")
    if path.stat().st_size > maximum:
        raise DriverError(f"JSON file exceeds {maximum} bytes: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DriverError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DriverError(f"{path} must contain one JSON object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        Path(temporary).unlink(missing_ok=True)


def _canonical_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _compact_cleaned_run(run: JsonObject) -> None:
    if run.get("cleaned") is not True:
        return
    receipts = _mapping(run.get("receipts", {}), label="cleaned run receipts")
    cleanup_request_id = str(run.get("cleanup_request_id", "")).strip()
    compacted_receipts = (
        {cleanup_request_id: copy.deepcopy(receipts[cleanup_request_id])}
        if cleanup_request_id in receipts
        else {}
    )
    retained = {
        key: run[key]
        for key in (
            "attempt",
            "attempt_id",
            "job_id",
            "job_request_digest",
            "run_id",
            "last_step_index",
            "cleaned",
            "cleanup_request_id",
        )
        if key in run
    }
    retained["receipts"] = compacted_receipts
    run.clear()
    run.update(retained)


def _new_attempt_id() -> str:
    return secrets.token_hex(8)


def _job_id(request_value: Mapping[str, Any], attempt: int, attempt_id: str = "") -> str:
    identity = {
        "campaign_id": request_value["campaign_id"],
        "experiment_id": request_value["experiment_id"],
        "run_key": request_value["run_key"],
        "attempt": attempt,
    }
    if attempt_id:
        identity["attempt_id"] = attempt_id
    return "hardware-" + _canonical_digest(identity)[:24]


def _operation_key(request_value: Mapping[str, Any], run: Mapping[str, Any], suffix: str) -> str:
    attempt = int(run.get("attempt", 1))
    attempt_id = str(run.get("attempt_id", "")).strip()
    identity = f"-i{attempt_id}" if attempt_id else ""
    return f"hardware-{request_value['request_id']}-a{attempt}{identity}-{suffix}"


def _is_sensitive_name(value: object) -> bool:
    normalized = str(value).casefold().replace("-", "_")
    compact = normalized.replace("_", "")
    tokens = {item.replace("_", "") for item in SENSITIVE_NAMES}
    return (
        normalized in SENSITIVE_NAMES
        or compact in tokens
        or any(normalized.endswith(f"_{item}") for item in SENSITIVE_NAMES)
    )


def _contains_sensitive_field(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            _is_sensitive_name(key) or _contains_sensitive_field(child)
            for key, child in value.items()
        )
    if isinstance(value, list):
        return any(_contains_sensitive_field(child) for child in value)
    return False


def _safe_detail(value: object) -> str:
    detail = str(value).replace("\n", " ").strip()[:1000]
    for key, secret in os.environ.items():
        if _is_sensitive_name(key) and len(secret) >= 8:
            detail = detail.replace(secret, "[REDACTED]")
    return detail


def _resolve_executable(value: object, *, label: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise DriverError(f"{label} is required")
    candidate = shutil.which(raw) if len(Path(raw).parts) == 1 else raw
    if candidate is None:
        raise DriverError(f"{label} is not available: {raw}")
    resolved = Path(candidate).expanduser().resolve()
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise DriverError(f"{label} is not executable: {resolved}")
    return str(resolved)


def _positive_number(value: object, *, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DriverError(f"{label} must be a positive finite number") from exc
    if not math.isfinite(result) or result <= 0:
        raise DriverError(f"{label} must be a positive finite number")
    return result


def _list(value: object, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise DriverError(f"{label} must be a list")
    return value


def _mapping(value: object, *, label: str) -> JsonObject:
    if not isinstance(value, dict):
        raise DriverError(f"{label} must be an object")
    return cast(JsonObject, value)


def _string(value: object, *, label: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise DriverError(f"{label} is required")
    return result


@dataclass(frozen=True, slots=True)
class TargetConfig:
    experiment_id: str
    gateway_url: str
    namespace: str
    kube_context: str
    kubectl: str
    job_template: Path
    gpu_profile: str
    execution_mode: str
    worker_registry_url: str
    trace_command: tuple[str, ...]
    action_hooks: Mapping[str, tuple[str, ...]]
    fault_hooks: Mapping[str, Mapping[str, tuple[str, ...]]]
    timeout: float
    poll_interval: float
    variables: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class DriverConfig:
    path: Path
    state_directory: Path
    targets: Mapping[str, TargetConfig]

    def target(self, experiment_id: str) -> TargetConfig:
        try:
            return self.targets[experiment_id]
        except KeyError as exc:
            raise DriverError(f"environment config has no target for {experiment_id}") from exc


def _resolve_config_path(base: Path, value: object, *, label: str) -> Path:
    raw = _string(value, label=label)
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    resolved = candidate.resolve()
    if resolved.is_symlink() or not resolved.is_file():
        raise DriverError(f"{label} is not a regular file: {resolved}")
    return resolved


def _optional_http_base_url(value: object, *, label: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = parse.urlsplit(raw)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise DriverError(f"{label} must be an HTTP(S) URL without credentials, query, or fragment")
    return raw.rstrip("/")


def _hook_map(value: object, *, label: str) -> dict[str, tuple[str, ...]]:
    if value is None:
        return {}
    raw = _mapping(value, label=label)
    result: dict[str, tuple[str, ...]] = {}
    for key, command in raw.items():
        name = _string(key, label=f"{label} key")
        argv = tuple(
            _string(item, label=f"{label}.{name} argv")
            for item in _list(command, label=f"{label}.{name}")
        )
        if not argv:
            raise DriverError(f"{label}.{name} must not be empty")
        result[name] = argv
    return result


def _command_fingerprint(argv: Sequence[str]) -> JsonObject:
    executable = Path(_resolve_executable(argv[0], label="hardware environment hook"))
    file_arguments: dict[str, str] = {}
    for index, value in enumerate(argv[1:], start=1):
        if PLACEHOLDER_PATTERN.search(value):
            continue
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = ROOT / candidate
        resolved = candidate.resolve()
        if resolved.is_file() and not resolved.is_symlink():
            file_arguments[str(index)] = _file_digest(resolved)
    return {
        "argv_digest": _canonical_digest(list(argv)),
        "executable_sha256": _file_digest(executable),
        "file_arguments_sha256": file_arguments,
    }


def _hooks_fingerprint(target: TargetConfig) -> JsonObject:
    return {
        "actions": {
            name: _command_fingerprint(argv) for name, argv in sorted(target.action_hooks.items())
        },
        "faults": {
            fault_id: {phase: _command_fingerprint(argv) for phase, argv in sorted(phases.items())}
            for fault_id, phases in sorted(target.fault_hooks.items())
        },
    }


def load_config(path: Path) -> DriverConfig:
    resolved = path.expanduser().resolve()
    raw = _read_json(resolved)
    if raw.get("schema_version") != CONFIG_SCHEMA:
        raise DriverError("hardware environment config schema is invalid")
    if _contains_sensitive_field(raw):
        raise DriverError(
            "hardware environment config must not contain inline credential-like fields"
        )
    allowed = {"schema_version", "state_directory", "defaults", "targets"}
    if set(raw) - allowed:
        raise DriverError("hardware environment config contains unsupported fields")
    state_directory = Path(
        _string(raw.get("state_directory"), label="state_directory")
    ).expanduser()
    if not state_directory.is_absolute() or state_directory.resolve() in {
        Path("/"),
        Path.home().resolve(),
        ROOT,
    }:
        raise DriverError("state_directory must be an absolute, narrow directory")
    defaults = _mapping(raw.get("defaults", {}), label="defaults")
    if set(defaults) - TARGET_FIELDS:
        raise DriverError("hardware environment defaults contain unsupported fields")
    targets_raw = _mapping(raw.get("targets"), label="targets")
    targets: dict[str, TargetConfig] = {}
    for experiment_id, override_value in targets_raw.items():
        override = _mapping(override_value, label=f"targets.{experiment_id}")
        if set(override) - TARGET_FIELDS:
            raise DriverError(f"targets.{experiment_id} contains unsupported fields")
        values = {**defaults, **override}
        raw_variables = _mapping(
            values.get("variables", {}), label=f"targets.{experiment_id}.variables"
        )
        variables = {
            _string(key, label=f"targets.{experiment_id}.variables key"): _string(
                value, label=f"targets.{experiment_id}.variables.{key}"
            )
            for key, value in raw_variables.items()
        }
        namespace = _string(values.get("namespace"), label=f"targets.{experiment_id}.namespace")
        if len(namespace) > 63 or DNS_LABEL_PATTERN.fullmatch(namespace) is None:
            raise DriverError(f"targets.{experiment_id}.namespace is not a DNS label")
        gpu_profile = _string(
            values.get("gpu_profile"), label=f"targets.{experiment_id}.gpu_profile"
        )
        if gpu_profile not in PROFILE_CLASS:
            raise DriverError(f"targets.{experiment_id}.gpu_profile is unsupported")
        execution_mode = _string(
            values.get("execution_mode", "kubernetes-dra"),
            label=f"targets.{experiment_id}.execution_mode",
        )
        if execution_mode not in EXECUTION_MODES:
            raise DriverError(f"targets.{experiment_id}.execution_mode is unsupported")
        if execution_mode == HAMI_PROFILE and gpu_profile != "full-gpu":
            raise DriverError("hami-vgpu execution requires the full-gpu physical profile")
        trace_command = tuple(
            _string(item, label=f"targets.{experiment_id}.trace_command argv")
            for item in _list(
                values.get("trace_command"),
                label=f"targets.{experiment_id}.trace_command",
            )
        )
        if not trace_command:
            raise DriverError(f"targets.{experiment_id}.trace_command must not be empty")
        fault_hooks_raw = _mapping(
            values.get("fault_hooks", {}),
            label=f"targets.{experiment_id}.fault_hooks",
        )
        fault_hooks: dict[str, dict[str, tuple[str, ...]]] = {}
        for fault_id, phases in fault_hooks_raw.items():
            fault_hooks[str(fault_id)] = _hook_map(phases, label=f"fault_hooks.{fault_id}")
        targets[str(experiment_id)] = TargetConfig(
            experiment_id=str(experiment_id),
            gateway_url=_string(
                values.get("gateway_url"),
                label=f"targets.{experiment_id}.gateway_url",
            ).rstrip("/"),
            namespace=namespace,
            kube_context=_string(
                values.get("kube_context"),
                label=f"targets.{experiment_id}.kube_context",
            ),
            kubectl=_resolve_executable(values.get("kubectl", "kubectl"), label="kubectl"),
            job_template=_resolve_config_path(
                resolved.parent,
                values.get("job_template"),
                label=f"targets.{experiment_id}.job_template",
            ),
            gpu_profile=gpu_profile,
            execution_mode=execution_mode,
            worker_registry_url=_optional_http_base_url(
                values.get("worker_registry_url"),
                label=f"targets.{experiment_id}.worker_registry_url",
            ),
            trace_command=trace_command,
            action_hooks=_hook_map(
                values.get("action_hooks", {}),
                label=f"targets.{experiment_id}.action_hooks",
            ),
            fault_hooks=fault_hooks,
            timeout=_positive_number(
                values.get("operation_timeout_seconds", 600),
                label="operation_timeout_seconds",
            ),
            poll_interval=_positive_number(
                values.get("poll_interval_seconds", 2),
                label="poll_interval_seconds",
            ),
            variables=variables,
        )
    if not targets:
        raise DriverError("hardware environment config needs at least one target")
    return DriverConfig(path=resolved, state_directory=state_directory.resolve(), targets=targets)


class Gateway:
    def __init__(self, base_url: str, timeout: float) -> None:
        parsed = parse.urlsplit(base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username
            or parsed.password
        ):
            raise DriverError("gateway_url must be an HTTP(S) URL without embedded credentials")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def call(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        idempotency_key: str = "",
        request_id: str = "",
    ) -> JsonObject:
        headers = {"Accept": "application/json"}
        payload = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            payload = json.dumps(body, separators=(",", ":")).encode()
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        if request_id:
            headers["X-Request-Id"] = request_id
        try:
            with request.urlopen(
                request.Request(self.base_url + path, data=payload, headers=headers, method=method),
                timeout=self.timeout,
            ) as response:
                raw = response.read(MAX_JSON_BYTES + 1)
        except error.HTTPError as exc:
            detail = exc.read(4096).decode(errors="replace")
            raise DriverError(
                f"gateway {method} {path} returned HTTP {exc.code}: {detail}"
            ) from exc
        except error.URLError as exc:
            raise DriverError(f"gateway {method} {path} failed: {exc.reason}") from exc
        if len(raw) > MAX_JSON_BYTES:
            raise DriverError("gateway response is oversized")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DriverError("gateway returned invalid JSON") from exc
        return _mapping(value, label="gateway response")

    def get(self, path: str) -> JsonObject:
        return self.call("GET", path)

    def post(self, path: str, body: Mapping[str, Any], key: str) -> JsonObject:
        return self.call(
            "POST",
            path,
            body=body,
            idempotency_key=key,
            request_id=key + "-request",
        )


class Kubernetes:
    def __init__(self, target: TargetConfig) -> None:
        self.target = target

    def _base(self, *, namespaced: bool) -> list[str]:
        argv = [self.target.kubectl]
        if self.target.kube_context:
            argv.extend(["--context", self.target.kube_context])
        if namespaced:
            argv.extend(["--namespace", self.target.namespace])
        return argv

    def run(
        self,
        argv: Sequence[str],
        *,
        namespaced: bool = True,
        timeout: float | None = None,
        input_text: str | None = None,
    ) -> str:
        command = [*self._base(namespaced=namespaced), *argv]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout or self.target.timeout,
                input=input_text,
            )
        except subprocess.TimeoutExpired as exc:
            raise DriverError(f"kubectl operation timed out: {argv[0]}") from exc
        if completed.returncode != 0:
            raise DriverError(
                f"kubectl {argv[0]} failed with exit {completed.returncode}: "
                + _safe_detail(completed.stderr)
            )
        if len(completed.stdout.encode()) > MAX_TRACE_BYTES:
            raise DriverError("kubectl output exceeds the driver limit")
        return completed.stdout

    def json(self, argv: Sequence[str], *, namespaced: bool = True) -> JsonObject:
        raw = self.run([*argv, "-o", "json"], namespaced=namespaced)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DriverError(f"kubectl {argv[0]} returned invalid JSON") from exc
        return _mapping(value, label="kubectl response")

    def delete_preconditioned(self, resource: str, name: str, live: Mapping[str, Any]) -> None:
        metadata = _mapping(live.get("metadata"), label=f"live {resource} metadata")
        uid = _string(metadata.get("uid"), label=f"live {resource} UID")
        resource_version = _string(
            metadata.get("resourceVersion"),
            label=f"live {resource} resourceVersion",
        )
        api_version = _string(live.get("apiVersion"), label=f"live {resource} apiVersion")
        plural = resource.split(".", 1)[0]
        escaped_namespace = parse.quote(self.target.namespace, safe="")
        escaped_name = parse.quote(name, safe="")
        if "/" in api_version:
            group, version = api_version.split("/", 1)
            raw_path = (
                f"/apis/{parse.quote(group, safe='')}/{parse.quote(version, safe='')}"
                f"/namespaces/{escaped_namespace}/{plural}/{escaped_name}"
            )
        else:
            raw_path = (
                f"/api/{parse.quote(api_version, safe='')}/namespaces/{escaped_namespace}"
                f"/{plural}/{escaped_name}"
            )
        options = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Foreground",
            "preconditions": {
                "uid": uid,
                "resourceVersion": resource_version,
            },
        }
        self.run(
            ["delete", f"--raw={raw_path}", "-f", "-"],
            input_text=json.dumps(options, separators=(",", ":")),
        )

        def deleted_or_replaced() -> bool:
            current = self.optional_json(["get", resource, name])
            if current is None:
                return True
            current_metadata = _mapping(
                current.get("metadata"), label=f"current {resource} metadata"
            )
            return str(current_metadata.get("uid", "")) != uid

        _wait(
            f"deletion of {resource}/{name}",
            deleted_or_replaced,
            timeout=self.target.timeout,
            interval=self.target.poll_interval,
        )

    def clear_finalizers_preconditioned(
        self, resource: str, name: str, live: Mapping[str, Any]
    ) -> JsonObject:
        metadata = _mapping(live.get("metadata"), label=f"live {resource} metadata")
        uid = _string(metadata.get("uid"), label=f"live {resource} UID")
        resource_version = _string(
            metadata.get("resourceVersion"),
            label=f"live {resource} resourceVersion",
        )
        patch = json.dumps(
            [
                {"op": "test", "path": "/metadata/uid", "value": uid},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": resource_version,
                },
                {"op": "add", "path": "/metadata/finalizers", "value": []},
            ],
            separators=(",", ":"),
        )
        return self.json(["patch", resource, name, "--type=json", "--patch", patch])

    def optional_json(self, argv: Sequence[str], *, namespaced: bool = True) -> JsonObject | None:
        raw = self.run(
            [*argv, "--ignore-not-found=true", "-o", "json"],
            namespaced=namespaced,
        )
        if not raw.strip():
            return None
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DriverError(f"kubectl {argv[0]} returned invalid JSON") from exc
        return _mapping(value, label="kubectl response")


class StateStore:
    def __init__(self, root: Path) -> None:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        root.chmod(0o700)
        self.path = root / "state.json"
        self.lock_path = root / "state.lock"

    def locked(self) -> _StateLock:
        return _StateLock(self)


class _StateLock:
    def __init__(self, store: StateStore) -> None:
        self.store = store
        self.handle: Any = None
        self.state: JsonObject = {}
        self.ready = False

    def __enter__(self) -> JsonObject:
        descriptor = os.open(self.store.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        self.handle = os.fdopen(descriptor, "r+", encoding="utf-8")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.handle.close()
            self.handle = None
            raise DriverError("another hardware environment driver operation is active") from exc
        try:
            if self.store.path.is_file():
                self.state = _read_json(self.store.path, maximum=MAX_STATE_BYTES)
                if self.state.get("schema_version") != STATE_SCHEMA:
                    raise DriverError("hardware driver state schema is invalid")
            else:
                self.state = {"schema_version": STATE_SCHEMA, "runs": {}}
            runs = _mapping(self.state.get("runs"), label="hardware driver state runs")
            for value in runs.values():
                _compact_cleaned_run(_mapping(value, label="hardware driver persisted run"))
            self.ready = True
            return self.state
        except Exception:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None
            raise

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        try:
            if self.ready:
                _write_json(self.store.path, self.state)
        finally:
            if self.handle is not None:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                self.handle.close()


def _wait(
    description: str,
    callback: Callable[[], Any],
    *,
    timeout: float,
    interval: float,
) -> Any:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = callback()
            if value:
                return value
        except TerminalDriverError:
            raise
        except DriverError as exc:
            last_error = exc
        time.sleep(interval)
    suffix = f"; last error: {last_error}" if last_error else ""
    raise DriverError(f"timed out waiting for {description}{suffix}")


def _items(value: JsonObject, key: str) -> list[JsonObject]:
    raw = value.get(key, [])
    if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
        raise DriverError(f"response field {key} must be a list of objects")
    return cast(list[JsonObject], raw)


def _operation_id(payload: JsonObject) -> str:
    operation = _mapping(payload.get("operation"), label="operation")
    return _string(operation.get("operationId"), label="operation.operationId")


def _action_name(action: Mapping[str, Any]) -> str:
    return str(action.get("actionType", "")).removeprefix("ACTION_TYPE_").lower()


def _decision_sequence(decision: Mapping[str, Any]) -> int:
    try:
        return int(decision.get("sequence", 0))
    except (TypeError, ValueError):
        return 0


def _decision_action(
    decision: JsonObject, action_name: str
) -> tuple[JsonObject, JsonObject] | None:
    plan = _mapping(decision.get("selectedPlan", {}), label="decision.selectedPlan")
    actions = _items(plan, "actions")
    results = {str(item.get("actionId", "")): item for item in _items(decision, "actionResults")}
    for action in actions:
        if _action_name(action) != action_name:
            continue
        result = results.get(str(action.get("actionId", "")))
        if result is not None and result.get("status") == "ACTION_RESULT_STATUS_SUCCEEDED":
            return action, result
    return None


def _plan_bindings(decision: Mapping[str, Any]) -> list[JsonObject]:
    plan = _mapping(decision.get("selectedPlan", {}), label="decision.selectedPlan")
    return _items(plan, "bindings")


def _device_ids(binding: Mapping[str, Any]) -> list[str]:
    values = binding.get("deviceIds", [])
    if not isinstance(values, list):
        raise DriverError("binding deviceIds must be a list")
    result = sorted({_string(item, label="binding device ID") for item in values})
    if not result:
        raise DriverError("binding contains no device identities")
    return result


def _attribute(attributes: Mapping[str, Any], key: str) -> str:
    raw = attributes.get(key, attributes.get(f"{NVIDIA_DRIVER}/{key}"))
    if not isinstance(raw, dict):
        return ""
    return str(raw.get("string", "")).strip()


def _merge_attributes(device: Mapping[str, Any]) -> JsonObject:
    top = _mapping(device.get("attributes", {}), label="ResourceSlice device attributes")
    basic = device.get("basic")
    nested = (
        {}
        if basic is None
        else _mapping(
            _mapping(basic, label="ResourceSlice basic").get("attributes", {}),
            label="ResourceSlice basic attributes",
        )
    )
    merged = dict(top)
    for key, value in nested.items():
        if key in merged and merged[key] != value:
            raise DriverError(f"ResourceSlice attribute {key!r} conflicts across sources")
        merged[key] = value
    return merged


def _dra_inventory(payload: JsonObject) -> list[JsonObject]:
    slices = _items(payload, "items")
    latest: dict[str, int] = {}
    for item in slices:
        spec = _mapping(item.get("spec"), label="ResourceSlice spec")
        if spec.get("driver") != NVIDIA_DRIVER:
            continue
        pool = _mapping(spec.get("pool"), label="ResourceSlice pool")
        name = _string(pool.get("name"), label="ResourceSlice pool name")
        generation = int(pool.get("generation", 0))
        latest[name] = max(latest.get(name, generation), generation)
    inventory: list[JsonObject] = []
    owners: dict[str, str] = {}
    tuples: set[tuple[str, str]] = set()
    for item in slices:
        spec = _mapping(item.get("spec"), label="ResourceSlice spec")
        if spec.get("driver") != NVIDIA_DRIVER:
            continue
        pool = _mapping(spec.get("pool"), label="ResourceSlice pool")
        pool_name = _string(pool.get("name"), label="ResourceSlice pool name")
        if int(pool.get("generation", 0)) != latest[pool_name]:
            continue
        for raw_device in _list(spec.get("devices", []), label="ResourceSlice devices"):
            device = _mapping(raw_device, label="ResourceSlice device")
            name = _string(device.get("name"), label="ResourceSlice device name")
            key = (pool_name, name)
            if key in tuples:
                raise DriverError(f"DRA device {pool_name}/{name} is duplicated")
            tuples.add(key)
            attributes = _merge_attributes(device)
            uuid = _attribute(attributes, "uuid")
            kind = _attribute(attributes, "type")
            if not uuid or kind == "vfio":
                continue
            if kind not in {"gpu", "mig"}:
                raise DriverError(f"DRA UUID {uuid!r} has unsupported type {kind!r}")
            if uuid in owners:
                raise DriverError(f"DRA UUID {uuid!r} is published more than once")
            owners[uuid] = f"{pool_name}/{name}"
            record: JsonObject = {
                "uuid": uuid,
                "type": kind,
                "device_class": ("mig.nvidia.com" if kind == "mig" else "gpu.nvidia.com"),
                "driver": NVIDIA_DRIVER,
                "pool": pool_name,
                "device": name,
                "node_id": str(spec.get("nodeName") or pool_name),
                "profile": _attribute(attributes, "profile"),
                "parent_uuid": _attribute(attributes, "parentUUID"),
            }
            if kind == "mig" and (not record["profile"] or not record["parent_uuid"]):
                raise DriverError(f"DRA MIG UUID {uuid!r} has incomplete metadata")
            inventory.append(record)
    return sorted(inventory, key=lambda item: str(item["uuid"]))


def _positive_integer_quantity(value: object) -> int:
    raw = str(value or "").strip()
    if re.fullmatch(r"[1-9][0-9]*", raw) is None:
        return 0
    return int(raw)


def _hami_node_devices(node_name: str, payload: str) -> list[JsonObject]:
    node_name = _string(node_name, label="HAMi node name")
    raw = payload.strip()
    if not raw:
        return []
    records: list[JsonObject] = []
    if raw.startswith("["):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DriverError("HAMi node registration is invalid JSON") from exc
        if not isinstance(decoded, list) or any(not isinstance(item, dict) for item in decoded):
            raise DriverError("HAMi node registration must contain a list of devices")
        for item in decoded:
            records.append(
                {
                    "uuid": str(item.get("id", "")).strip(),
                    "split_count": item.get("count"),
                    "memory_mib": item.get("devmem"),
                    "core_percent": item.get("devcore"),
                    "model": str(item.get("type", "")).strip(),
                    "healthy": item.get("health"),
                    "mode": str(item.get("mode") or "hami-core").strip(),
                    "node_id": node_name,
                }
            )
    else:
        for segment in raw.split(":"):
            fields = [field.strip() for field in segment.strip().split(",")]
            if len(fields) == 1 and not fields[0]:
                continue
            if len(fields) not in {7, 9}:
                raise DriverError(
                    f"HAMi node registration segment has {len(fields)} fields, want 7 or 9"
                )
            try:
                split_count = int(fields[1])
                memory_mib = int(fields[2])
                core_percent = int(fields[3])
            except ValueError as exc:
                raise DriverError("HAMi node registration contains invalid capacity") from exc
            records.append(
                {
                    "uuid": fields[0],
                    "split_count": split_count,
                    "memory_mib": memory_mib,
                    "core_percent": core_percent,
                    "model": fields[4],
                    "healthy": fields[6].lower() == "true",
                    "mode": fields[8] if len(fields) == 9 and fields[8] else "hami-core",
                    "node_id": node_name,
                }
            )
    for item in records:
        if (
            not str(item["uuid"]).startswith("GPU-")
            or not str(item["model"])
            or not isinstance(item["split_count"], int)
            or isinstance(item["split_count"], bool)
            or int(item["split_count"]) <= 0
            or not isinstance(item["memory_mib"], int)
            or isinstance(item["memory_mib"], bool)
            or int(item["memory_mib"]) <= 0
            or not isinstance(item["core_percent"], int)
            or isinstance(item["core_percent"], bool)
            or not 0 < int(item["core_percent"]) <= 100
            or item["healthy"] is not True
            or item["mode"] != "hami-core"
        ):
            raise DriverError(
                f"HAMi node registration for UUID {item['uuid']!r} is incomplete or unusable"
            )
    return records


def _hami_inventory(payload: JsonObject) -> list[JsonObject]:
    inventory: list[JsonObject] = []
    owners: dict[str, str] = {}
    for node in _items(payload, "items"):
        metadata = _mapping(node.get("metadata"), label="Node metadata")
        status = _mapping(node.get("status"), label="Node status")
        allocatable = _mapping(status.get("allocatable", {}), label="Node allocatable")
        if _positive_integer_quantity(allocatable.get(HAMI_GPU_RESOURCE)) == 0:
            continue
        annotations = _mapping(metadata.get("annotations", {}), label="Node annotations")
        registration = str(annotations.get(HAMI_REGISTER_ANNOTATION, "")).strip()
        if not registration:
            continue
        node_name = _string(metadata.get("name"), label="Node name")
        for item in _hami_node_devices(node_name, registration):
            uuid = str(item["uuid"])
            if uuid in owners:
                raise DriverError(
                    f"HAMi NVIDIA UUID {uuid!r} is published by nodes "
                    f"{owners[uuid]!r} and {node_name!r}"
                )
            owners[uuid] = node_name
            inventory.append(item)
    return sorted(inventory, key=lambda item: str(item["uuid"]))


def _hami_allocations(payload: str) -> list[JsonObject]:
    allocations: list[JsonObject] = []
    seen: set[str] = set()
    for container in payload.split(";"):
        for raw_device in container.split(":"):
            if not raw_device.strip():
                continue
            fields = [field.strip() for field in raw_device.split(",")]
            if len(fields) < 4:
                raise DriverError("HAMi allocation entry has fewer than four fields")
            uuid, device_type = fields[:2]
            try:
                memory_mib, core_percent = int(fields[2]), int(fields[3])
            except ValueError as exc:
                raise DriverError("HAMi allocation contains invalid memory or core values") from exc
            if (
                not uuid.startswith("GPU-")
                or not device_type
                or memory_mib <= 0
                or not 0 < core_percent <= 100
                or uuid in seen
            ):
                raise DriverError("HAMi allocation contains an invalid or duplicate device entry")
            seen.add(uuid)
            allocations.append(
                {
                    "uuid": uuid,
                    "type": device_type,
                    "memory_mib": memory_mib,
                    "core_percent": core_percent,
                }
            )
    if not allocations:
        raise DriverError("HAMi allocation annotation contains no devices")
    return sorted(allocations, key=lambda item: str(item["uuid"]))


def _worker_concurrency_overlap_ms(
    events: Sequence[Mapping[str, Any]], *, required_workers: int
) -> tuple[float, list[str]]:
    intervals: dict[str, tuple[datetime, datetime]] = {}
    starts: dict[str, datetime] = {}
    completions: dict[str, datetime] = {}
    for event in events:
        sandbox_id = str(event.get("sandbox_id", "")).strip()
        occurred_at = str(event.get("occurred_at", "")).strip()
        if not sandbox_id or not occurred_at:
            continue
        try:
            observed = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DriverError("managed-worker trace contains an invalid occurred_at") from exc
        if observed.tzinfo is None:
            raise DriverError("managed-worker trace occurred_at must include a timezone")
        if event.get("event_type") == "sample_consumed":
            duration_ms = float(event.get("duration_ms", 0.0))
            if not math.isfinite(duration_ms) or duration_ms <= 0:
                raise DriverError("managed-worker sample duration must be positive and finite")
            started = observed.timestamp() - duration_ms / 1000
            sample_start = datetime.fromtimestamp(started, tz=observed.tzinfo)
            starts[sandbox_id] = min(starts.get(sandbox_id, sample_start), sample_start)
        elif event.get("event_type") == "workload_completed":
            completions[sandbox_id] = max(completions.get(sandbox_id, observed), observed)
    for sandbox_id in sorted(set(starts) & set(completions)):
        if completions[sandbox_id] <= starts[sandbox_id]:
            raise DriverError(f"managed worker {sandbox_id} has a non-positive execution interval")
        intervals[sandbox_id] = (starts[sandbox_id], completions[sandbox_id])
    if len(intervals) != required_workers:
        raise DriverError(
            f"managed-worker trace has {len(intervals)} complete execution intervals; "
            f"need exactly {required_workers}"
        )
    maximum = 0.0
    ordered = sorted(intervals)
    for index, left_id in enumerate(ordered):
        left = intervals[left_id]
        for right_id in ordered[index + 1 :]:
            right = intervals[right_id]
            overlap = (min(left[1], right[1]) - max(left[0], right[0])).total_seconds() * 1000
            maximum = max(maximum, overlap)
    if maximum <= 0:
        raise DriverError("managed-worker execution intervals do not overlap")
    return maximum, ordered


def _visible_device_ids(output: str, profile: str) -> list[str]:
    prefix = "MIG-" if profile == "mig" else "GPU-"
    return sorted({item for item in UUID_PATTERN.findall(output) if item.startswith(prefix)})


def _expand_placeholders(value: object, variables: Mapping[str, str]) -> object:
    if isinstance(value, dict):
        return {str(key): _expand_placeholders(child, variables) for key, child in value.items()}
    if isinstance(value, list):
        return [_expand_placeholders(child, variables) for child in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in variables:
            raise DriverError(f"job template uses unsupported placeholder {name}")
        return variables[name]

    return PLACEHOLDER_PATTERN.sub(replace, value)


def _git_fingerprint() -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    )
    return commit, dirty


class HardwareEnvironmentDriver:
    def __init__(self, config: DriverConfig) -> None:
        self.config = config
        self.store = StateStore(config.state_directory)

    def execute(self, request_value: JsonObject, response_path: Path) -> JsonObject:
        self._validate_request(request_value)
        target = self.config.target(str(request_value["experiment_id"]))
        if request_value["operation"] == "preflight":
            return self._preflight(request_value, target, response_path)
        with self.store.locked() as state:
            runs = _mapping(state["runs"], label="hardware driver state runs")
            run_key = _string(request_value.get("run_key"), label="request.run_key")
            state_key = "\x00".join(
                (
                    str(request_value["campaign_id"]),
                    str(request_value["experiment_id"]),
                    run_key,
                )
            )
            run = cast(JsonObject, runs.get(state_key, {}))
            if request_value["operation"] == "provision" and run.get("cleaned") is True:
                run = {
                    "attempt": int(run.get("attempt", 1)) + 1,
                    "attempt_id": _new_attempt_id(),
                    "receipts": {},
                }
                runs[state_key] = run
            elif not run:
                run = {"attempt": 1, "attempt_id": _new_attempt_id(), "receipts": {}}
                runs[state_key] = run
            request_digest = _canonical_digest(request_value)
            receipts = _mapping(run.get("receipts", {}), label="run receipts")
            request_id = str(request_value["request_id"])
            existing = receipts.get(request_id)
            if existing is not None:
                record = _mapping(existing, label="driver receipt")
                if record.get("request_digest") != request_digest:
                    raise DriverError("request_id was reused with different content")
                replayed = _mapping(copy.deepcopy(record.get("response")), label="cached response")
                # Artifacts belong to one invocation directory. A replay in a
                # fresh directory must not claim files produced by an earlier call.
                replayed.pop("artifacts", None)
                return replayed
            if request_value["operation"] == "provision":
                job = self._render_job(request_value, target)
                job_request_digest = _canonical_digest(job)
                persisted_digest = str(run.get("job_request_digest", "")).strip()
                if persisted_digest and persisted_digest != job_request_digest:
                    raise DriverError("rendered hardware Job changed during an active attempt")
                run["job_request_digest"] = job_request_digest
                expected_job_id = _job_id(
                    request_value,
                    int(run.get("attempt", 1)),
                    str(run.get("attempt_id", "")),
                )
                if run.get("job_id") not in {None, "", expected_job_id}:
                    raise DriverError("persisted hardware Job identity is inconsistent")
                run["job_id"] = expected_job_id
                # Checkpoint the identity before the external create side effect.
                # This survives a process crash after Gateway accepts the Job.
                _write_json(self.store.path, state)
            response = self._dispatch(request_value, target, run, response_path)
            receipts[request_id] = {
                "request_digest": request_digest,
                "response": response,
            }
            run["last_step_index"] = int(request_value["step_index"])
            if request_value["operation"] == "cleanup":
                run["cleaned"] = True
                run["cleanup_request_id"] = request_id
                _compact_cleaned_run(run)
            return response

    def _validate_request(self, value: JsonObject) -> None:
        if value.get("schema_version") != REQUEST_SCHEMA:
            raise DriverError("hardware driver request schema is invalid")
        for field in ("request_id", "campaign_id", "experiment_id", "operation"):
            _string(value.get(field), label=f"request.{field}")
        operation = str(value["operation"])
        if operation != "preflight" and operation not in RUN_OPERATIONS:
            raise DriverError(f"unsupported hardware driver operation {operation!r}")
        if operation in RUN_OPERATIONS:
            for field in ("run_key", "label", "phase"):
                _string(value.get(field), label=f"request.{field}")
            step = value.get("step_index")
            if not isinstance(step, int) or isinstance(step, bool) or step <= 0:
                raise DriverError("request.step_index must be a positive integer")
        if _contains_sensitive_field(value):
            raise DriverError("hardware driver request contains credential-like fields")

    def _dispatch(
        self,
        request_value: JsonObject,
        target: TargetConfig,
        run: JsonObject,
        response_path: Path,
    ) -> JsonObject:
        operation = str(request_value["operation"])
        if operation not in {"provision", "cleanup"} and not run.get("job_id"):
            raise DriverError(f"{operation} requires a provisioned run")
        if operation == "cleanup" and not run.get("job_id"):
            return {
                "schema_version": RESPONSE_SCHEMA,
                "request_id": request_value["request_id"],
                "status": "SUCCEEDED",
                "events": [],
            }
        handlers: dict[
            str, Callable[[JsonObject, TargetConfig, JsonObject, Path], list[JsonObject]]
        ] = {
            "provision": self._provision,
            "apply_action": self._apply_action,
            "apply_worker_action": self._apply_worker_action,
            "launch": self._launch,
            "verify_device_identity": self._verify_device_identity,
            "inject_fault": self._inject_fault,
            "recover_fault": self._recover_fault,
            "measure": self._measure,
            "stop": self._stop,
            "cleanup": self._cleanup,
        }
        events = handlers[operation](request_value, target, run, response_path)
        response: JsonObject = {
            "schema_version": RESPONSE_SCHEMA,
            "request_id": request_value["request_id"],
            "status": "SUCCEEDED",
            "events": events,
        }
        artifacts = self._operation_artifacts(response_path)
        if artifacts:
            response["artifacts"] = artifacts
        if _contains_sensitive_field(response):
            raise DriverError("hardware driver generated credential-like evidence")
        return response

    def _preflight(
        self, request_value: JsonObject, target: TargetConfig, response_path: Path
    ) -> JsonObject:
        scenario = _mapping(request_value.get("scenario"), label="request.scenario")
        topology = _mapping(scenario.get("topology"), label="scenario.topology")
        profiles = _list(topology.get("gpu_profiles"), label="scenario.topology.gpu_profiles")
        if target.gpu_profile not in profiles:
            raise DriverError("configured GPU profile is not allowed by the scenario")
        gateway = Gateway(target.gateway_url, target.timeout)
        gateway.get("/health")
        capabilities = gateway.get("/v1/capabilities")
        kube = Kubernetes(target)
        namespace = kube.json(["get", "namespace", target.namespace], namespaced=False)
        namespace_metadata = _mapping(
            namespace.get("metadata"), label="validation namespace metadata"
        )
        namespace_uid = _string(namespace_metadata.get("uid"), label="validation namespace UID")
        current = kube.run(["config", "current-context"], namespaced=False).strip()
        if current != target.kube_context:
            raise DriverError("kubectl current context does not match the configured context")
        cluster_namespace = kube.json(["get", "namespace", "kube-system"], namespaced=False)
        cluster_uid = _string(
            _mapping(cluster_namespace.get("metadata"), label="kube-system namespace metadata").get(
                "uid"
            ),
            label="Kubernetes cluster UID",
        )
        version = kube.json(["version"], namespaced=False)
        server_version = _mapping(version.get("serverVersion"), label="Kubernetes serverVersion")
        server_git_version = _string(
            server_version.get("gitVersion"), label="Kubernetes server gitVersion"
        )
        if target.execution_mode == "kubernetes-dra":
            device_class = PROFILE_CLASS[target.gpu_profile]
            kube.json(["get", "deviceclasses.resource.k8s.io", device_class], namespaced=False)
            slices = kube.json(["get", "resourceslices.resource.k8s.io"], namespaced=False)
            inventory = [
                item
                for item in _dra_inventory(slices)
                if item["type"] == PROFILE_TYPE[target.gpu_profile]
            ]
        else:
            inventory = _hami_inventory(kube.json(["get", "nodes"], namespaced=False))
        minimum = int(topology.get("minimum_accelerators", 1))
        if len(inventory) < minimum:
            raise DriverError(
                f"{target.execution_mode} inventory has {len(inventory)} "
                f"{target.gpu_profile} devices; need {minimum}"
            )
        minimum_nodes = int(topology.get("minimum_nodes", 1))
        if len({str(item["node_id"]) for item in inventory}) < minimum_nodes:
            raise DriverError(f"DRA inventory spans fewer than {minimum_nodes} nodes")
        commit, dirty = _git_fingerprint()
        if dirty:
            raise DriverError("hardware campaign requires a clean target checkout")
        fingerprint = {
            "host_hash": hashlib.sha256(socket.gethostname().encode()).hexdigest()[:16],
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "git_commit": commit,
            "git_dirty": False,
            "execution_mode": target.execution_mode,
            "gpu_profile": target.gpu_profile,
            "kube_context": current,
            "cluster_uid": cluster_uid,
            "kubernetes_server_version": server_git_version,
            "namespace_uid": namespace_uid,
            "environment_config_sha256": _file_digest(self.config.path),
            "job_template_sha256": _file_digest(target.job_template),
            "trace_command_digest": _canonical_digest(list(target.trace_command)),
            "hooks_fingerprint": _hooks_fingerprint(target),
            "accelerator_count": len(inventory),
            "accelerator_inventory_digest": _canonical_digest(inventory),
        }
        self._write_artifact(
            response_path,
            "preflight.json",
            {
                "fingerprint": fingerprint,
                "gateway_capabilities": capabilities,
                "inventory": inventory,
            },
        )
        return {
            "schema_version": RESPONSE_SCHEMA,
            "request_id": request_value["request_id"],
            "status": "SUCCEEDED",
            "events": [],
            "environment_fingerprint": fingerprint,
            "artifacts": [self._artifact_record(response_path, "preflight.json")],
        }

    def _render_job(self, request_value: JsonObject, target: TargetConfig) -> JsonObject:
        template = _read_json(target.job_template)
        if _contains_sensitive_field(template):
            raise DriverError("job template must not contain inline credential-like fields")
        lock = _mapping(request_value.get("workload_lock"), label="request.workload_lock")
        variables = {
            "CAMPAIGN_ID": str(request_value["campaign_id"]),
            "EXPERIMENT_ID": str(request_value["experiment_id"]),
            "RUN_KEY": str(request_value["run_key"]),
            "LABEL": str(request_value["label"]),
            "PHASE": str(request_value["phase"]),
            "ITERATION": str(request_value["iteration"]),
            "SEED": str(lock.get("seed", "")),
            **target.variables,
        }
        job = _mapping(_expand_placeholders(template, variables), label="expanded job template")
        self._validate_job(job, request_value, target)
        labels = _mapping(job.setdefault("labels", {}), label="job.labels")
        labels.update(
            {
                "tgsrl.campaign_id": str(request_value["campaign_id"]),
                "tgsrl.experiment_id": str(request_value["experiment_id"]),
                "tgsrl.run_key": str(request_value["run_key"]),
                "tgsrl.variant": str(request_value["label"]),
                "tgsrl.phase": str(request_value["phase"]),
                "tgsrl.iteration": str(request_value["iteration"]),
            }
        )
        job["displayName"] = f"{job.get('displayName', 'hardware-gate')}-{request_value['run_key']}"
        return job

    def _provision(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        job = self._render_job(request_value, target)
        if run.get("job_request_digest") != _canonical_digest(job):
            raise DriverError("persisted hardware Job request digest is inconsistent")
        expected_job_id = _string(run.get("job_id"), label="persisted hardware Job ID")
        job["jobId"] = expected_job_id
        self._write_artifact(response_path, "rendered-job.json", job)
        gateway = Gateway(target.gateway_url, target.timeout)
        created = gateway.post(
            "/v1/jobs",
            job,
            _operation_key(request_value, run, "create"),
        )
        job_id = _string(
            _mapping(created.get("job"), label="created job").get("jobId"),
            label="created job ID",
        )
        if job_id != expected_job_id:
            raise DriverError("Gateway changed the deterministic hardware Job identity")
        admitted = gateway.post(
            f"/v1/jobs/{parse.quote(job_id, safe='')}/admit",
            {"reason": "hardware Gate admission"},
            _operation_key(request_value, run, "admit"),
        )
        admit_operation_id = _operation_id(admitted)
        run["admit_operation_id"] = admit_operation_id
        self._wait_operation(gateway, admit_operation_id, target)

        def find_run() -> JsonObject | None:
            runs = _items(gateway.get(f"/v1/jobs/{parse.quote(job_id, safe='')}/runs"), "runs")
            return runs[0] if len(runs) == 1 else None

        service_run = cast(
            JsonObject,
            _wait(
                "one admitted JobRun",
                find_run,
                timeout=target.timeout,
                interval=target.poll_interval,
            ),
        )
        run_id = _string(service_run.get("runId"), label="service run ID")
        run.update(
            {
                "job_id": job_id,
                "run_id": run_id,
                "trace_id": str(service_run.get("traceId", "")),
                "cleaned": False,
                "last_decision_sequence": 0,
            }
        )
        return []

    def _apply_action(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        action = _string(request_value.get("action"), label="request.action")
        started = time.monotonic()
        hook_result: JsonObject = {}
        if action == "bind":
            decision = self._start_and_wait_for_decision(request_value, target, run)
            source = "scheduler"
            receipt_id = str(run.get("start_operation_id", request_value["request_id"]))
        elif action in DIRECT_COMMANDS:
            operation = self._command(request_value, target, run, DIRECT_COMMANDS[action])
            decision = self._latest_decision(target, run) or {}
            source = "operator"
            receipt_id = _operation_id(operation)
        elif action in SCHEDULER_HOOK_ACTIONS:
            before = int(run.get("last_decision_sequence", 0))
            hook = target.action_hooks.get(action)
            if hook is None:
                raise DriverError(
                    f"{action} requires an explicit environment hook that submits "
                    "observations to the Scheduler"
                )
            hook_result = self._run_hook(
                hook,
                request_value,
                run,
                response_path,
                authority="scheduler-observation",
            )
            decision = self._wait_decision(target, run, action, after_sequence=before)
            source = "operator"
            receipt_id = str(hook_result.get("receipt_id") or request_value["request_id"])
        elif action in WORKER_HOOK_ACTIONS:
            hook = target.action_hooks.get(action)
            if hook is None:
                raise DriverError(f"{action} requires an explicit managed-worker control hook")
            hook_result = self._run_hook(
                hook,
                request_value,
                run,
                response_path,
                authority="managed-worker-control",
            )
            decision = self._latest_decision(target, run) or {}
            source = "operator"
            receipt_id = _string(hook_result.get("receipt_id"), label=f"{action} hook receipt_id")
        else:
            raise DriverError(f"unsupported hardware action {action!r}")
        run["last_decision_sequence"] = max(
            int(run.get("last_decision_sequence", 0)), _decision_sequence(decision)
        )
        if decision:
            run["decision"] = decision
        plan = _mapping(decision.get("selectedPlan", {}), label="selected plan") if decision else {}
        event = self._common_event(run) | {
            "event_type": "decision_applied",
            "source": source,
            "action": action,
            "duration_ms": (time.monotonic() - started) * 1000.0,
            "succeeded": True,
            "rolled_back": action == "rollback",
            "recovery_time_ms": float(hook_result.get("recovery_time_ms", 0.0)),
            "decision_id": str(decision.get("decisionId", "")),
            "plan_id": str(plan.get("planId", "")),
            "receipt_id": receipt_id,
            "transaction_id": str(plan.get("planId") or receipt_id),
            "ready": bool(hook_result.get("ready", True)),
        }
        matched = _decision_action(decision, action) if decision else None
        if action == "set_share":
            if matched is not None:
                event["observed_share"] = matched[0].get("share")
            elif "observed_share" in hook_result:
                event["observed_share"] = hook_result["observed_share"]
        if action == "set_priority":
            if matched is not None:
                event["observed_priority"] = matched[0].get("priority")
            elif "observed_priority" in hook_result:
                event["observed_priority"] = hook_result["observed_priority"]
        started_event = self._common_event(run) | {
            "event_type": "control_started",
            "source": "operator",
            "action": action,
        }
        return [started_event, event]

    def _apply_worker_action(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        del response_path
        action = _string(request_value.get("action"), label="request.action")
        if action not in {"pause", "resume", "sleep", "offload"}:
            raise DriverError(f"unsupported scoped worker action {action!r}")
        kube = Kubernetes(target)
        bundles = self._latest_bundles(self._bundles(kube, run))
        if len(bundles) != 1:
            raise DriverError("scoped worker action requires exactly one current worker")
        bundle = _mapping(
            _mapping(bundles[0].get("spec"), label="JobRunBundle spec").get("bundle"),
            label="JobRunBundle bundle",
        )
        runtime_targets = _items(bundle, "runtimeTargets")
        if len(runtime_targets) != 1:
            raise DriverError("scoped worker action requires one runtime target")
        runtime_target = runtime_targets[0]
        job = _mapping(bundle.get("job"), label="bundle Job")
        template = _mapping(
            _mapping(job.get("spec"), label="bundle Job spec").get("template"),
            label="bundle Job template",
        )
        containers = _items(_mapping(template.get("spec"), label="bundle Pod spec"), "containers")
        if len(containers) != 1:
            raise DriverError("managed-worker bundle must contain one main container")
        environment = self._literal_environment(containers[0])
        token = _string(
            environment.get("TGSRL_WORKER_REGISTRY_TOKEN"),
            label="scoped worker registry token",
        )
        bundle_registry_url = _string(
            environment.get("TGSRL_WORKER_REGISTRY_URL"),
            label="worker registry URL",
        )
        registry_url = target.worker_registry_url or bundle_registry_url
        sandbox_id = _string(runtime_target.get("sandboxId"), label="runtime target sandbox ID")
        generation = int(runtime_target.get("generation", bundle.get("generation", 0)))
        if generation <= 0:
            raise DriverError("runtime target generation must be positive")
        body = {
            "action": action,
            "sandbox_id": sandbox_id,
            "generation": generation,
            "idempotency_key": _operation_key(request_value, run, action),
        }
        started = time.monotonic()
        response = self._worker_registry_call(registry_url, token, body, target.timeout)
        worker = _mapping(response.get("worker"), label="worker registry response")
        if response.get("accepted") is not True:
            raise DriverError(f"worker registry did not accept {action}")
        if worker.get("sandbox_id") != sandbox_id or int(worker.get("generation", 0)) != generation:
            raise DriverError("worker registry response identity does not match the target")
        expected = {
            "pause": lambda value: (
                value.get("state") == "paused" and value.get("safe_point") is True
            ),
            "sleep": lambda value: value.get("state") == "sleeping",
            "offload": lambda value: (
                value.get("state") == "sleeping"
                and value.get("offloaded") is True
                and bool(str(value.get("checkpoint_ref", "")).strip())
            ),
            "resume": lambda value: (
                value.get("state") == "running"
                and value.get("ready") is True
                and value.get("offloaded") is not True
            ),
        }[action]
        if not expected(worker):
            raise DriverError(f"worker did not confirm {action}")
        event = self._common_event(run) | {
            "event_type": "control_completed",
            "source": "operator",
            "action": action,
            "sandbox_id": sandbox_id,
            "generation": generation,
            "duration_ms": (time.monotonic() - started) * 1000.0,
            "succeeded": True,
            "ready": bool(worker.get("ready", False)),
            "offloaded": bool(worker.get("offloaded", False)),
            "checkpoint_present": bool(str(worker.get("checkpoint_ref", "")).strip()),
            "receipt_id": body["idempotency_key"],
        }
        if action in {"offload", "resume"}:
            before = {
                "gpu_memory_observed": response.get("gpu_memory_observed_before"),
                "gpu_memory_allocated_bytes": response.get("gpu_memory_allocated_before_bytes", 0),
                "gpu_memory_reserved_bytes": response.get("gpu_memory_reserved_before_bytes", 0),
            }
            event.update(
                {
                    "gpu_memory_allocated_before_bytes": self._memory_bytes(
                        before, "gpu_memory_allocated_bytes"
                    ),
                    "gpu_memory_allocated_after_bytes": self._memory_bytes(
                        worker, "gpu_memory_allocated_bytes"
                    ),
                    "gpu_memory_reserved_before_bytes": self._memory_bytes(
                        before, "gpu_memory_reserved_bytes"
                    ),
                    "gpu_memory_reserved_after_bytes": self._memory_bytes(
                        worker, "gpu_memory_reserved_bytes"
                    ),
                }
            )
        return [event]

    @staticmethod
    def _literal_environment(container: Mapping[str, Any]) -> dict[str, str]:
        result: dict[str, str] = {}
        for raw in _list(container.get("env", []), label="container environment"):
            item = _mapping(raw, label="container environment entry")
            name = _string(item.get("name"), label="container environment name")
            if item.get("valueFrom") is None and isinstance(item.get("value"), str):
                result[name] = str(item["value"])
        return result

    @staticmethod
    def _worker_registry_call(
        registry_url: str, token: str, body: Mapping[str, Any], timeout: float
    ) -> JsonObject:
        return HardwareEnvironmentDriver._worker_registry_request(
            registry_url, "/v1/workers/action", token, body, timeout
        )

    @staticmethod
    def _worker_registry_request(
        registry_url: str,
        endpoint_path: str,
        token: str,
        body: Mapping[str, Any],
        timeout: float,
    ) -> JsonObject:
        parsed = parse.urlsplit(registry_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise DriverError("worker registry URL is invalid")
        endpoint = parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path.rstrip("/") + endpoint_path, "", "")
        )
        payload = json.dumps(body, separators=(",", ":")).encode()
        action_request = request.Request(
            endpoint,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-TGSRL-Worker-Token": token,
            },
            method="POST",
        )
        try:
            with request.urlopen(action_request, timeout=timeout) as response:
                raw = response.read(MAX_JSON_BYTES + 1)
        except error.HTTPError as exc:
            detail = exc.read(4096).decode(errors="replace")
            raise DriverError(
                f"worker registry action returned HTTP {exc.code}: {_safe_detail(detail)}"
            ) from exc
        except error.URLError as exc:
            raise DriverError(f"worker registry action failed: {exc.reason}") from exc
        if len(raw) > MAX_JSON_BYTES:
            raise DriverError("worker registry response is oversized")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DriverError("worker registry returned invalid JSON") from exc
        return _mapping(value, label="worker registry response")

    @staticmethod
    def _memory_bytes(worker: Mapping[str, Any], key: str) -> int:
        if worker.get("gpu_memory_observed") is not True:
            raise DriverError("worker did not report authoritative GPU memory observations")
        value = worker.get(key, 0)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise DriverError(f"worker registry omitted valid {key}")
        return value

    def _start_and_wait_for_decision(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject
    ) -> JsonObject:
        gateway = Gateway(target.gateway_url, target.timeout)
        job_id, run_id = str(run["job_id"]), str(run["run_id"])
        if not run.get("start_operation_id"):
            result = gateway.post(
                f"/v1/jobs/{parse.quote(job_id, safe='')}/runs/"
                f"{parse.quote(run_id, safe='')}/commands/start",
                {
                    "actor": "hardware-gate",
                    "reason": "hardware Gate bind and launch",
                },
                _operation_key(request_value, run, "start"),
            )
            run["start_operation_id"] = _operation_id(result)
        decision = self._wait_decision(target, run, "bind", after_sequence=0)
        run["decision"] = decision
        return decision

    def _launch(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        del response_path
        gateway = Gateway(target.gateway_url, target.timeout)
        if run.get("start_operation_id"):
            self._wait_operation(gateway, str(run["start_operation_id"]), target)
        required_workers = self._required_workers(request_value)

        def read_targets() -> list[JsonObject] | None:
            topology = self._wait_running_topology(gateway, run, target)
            targets = self._kubernetes_targets(target, run, topology, require_ready=True)
            return targets if len(targets) == required_workers else None

        targets = cast(
            list[JsonObject],
            _wait(
                f"{required_workers} managed workers to register",
                read_targets,
                timeout=target.timeout,
                interval=target.poll_interval,
            ),
        )
        run["targets"] = targets
        return [
            self._target_event(run, item) | {"event_type": "worker_registered", "source": "worker"}
            for item in targets
        ]

    def _verify_device_identity(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        gateway = Gateway(target.gateway_url, target.timeout)
        required_workers = self._required_workers(request_value)

        def read_targets() -> list[JsonObject] | None:
            topology = gateway.get(
                f"/v1/jobs/{parse.quote(str(run['job_id']), safe='')}/topology"
                f"?run_id={parse.quote(str(run['run_id']), safe='')}"
            )
            targets = self._kubernetes_targets(target, run, topology, require_ready=True)
            return targets if len(targets) == required_workers else None

        targets = cast(
            list[JsonObject],
            _wait(
                "Scheduler, DRA, and worker device identity convergence",
                read_targets,
                timeout=target.timeout,
                interval=target.poll_interval,
            ),
        )
        events: list[JsonObject] = []
        evidence: list[JsonObject] = []
        for item in targets:
            scheduler_ids = sorted(cast(list[str], item["scheduler_device_ids"]))
            allocation_ids = sorted(cast(list[str], item["allocated_device_ids"]))
            worker_ids = sorted(cast(list[str], item["worker_device_ids"]))
            if scheduler_ids != allocation_ids or scheduler_ids != worker_ids:
                raise DriverError(
                    "device identity differs across Scheduler, infrastructure "
                    "allocation, and worker"
                )
            event = self._target_event(run, item) | {
                "event_type": "device_identity_verified",
                "source": "worker",
                "scheduler_device_ids": scheduler_ids,
                "allocated_device_ids": allocation_ids,
                "worker_device_ids": worker_ids,
                "device_class": item["device_class"],
                "parent_uuid": item.get("parent_uuid", ""),
            }
            for key in (
                "allocation_mode",
                "requested_core_percent",
                "allocated_core_percent",
                "requested_memory_mib",
                "allocated_memory_mib",
            ):
                if key in item:
                    event[key] = item[key]
            events.append(event)
            evidence.append(
                {
                    key: event[key]
                    for key in (
                        "node_id",
                        "runtime_unit_id",
                        "scheduler_device_ids",
                        "allocated_device_ids",
                        "worker_device_ids",
                        "device_class",
                        "parent_uuid",
                        "allocation_mode",
                        "requested_core_percent",
                        "allocated_core_percent",
                        "requested_memory_mib",
                        "allocated_memory_mib",
                    )
                    if key in event
                }
            )
        self._write_artifact(response_path, "device-identity.json", {"targets": evidence})
        run["targets"] = targets
        return events

    def _measure(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        targets = cast(list[JsonObject], run.get("targets", []))
        required_workers = self._required_workers(request_value)
        if len(targets) != required_workers:
            raise DriverError("measure requires verified worker targets")
        kube = Kubernetes(target)

        def collect() -> list[JsonObject] | None:
            events: list[JsonObject] = []
            for item in targets:
                output = kube.run(
                    [
                        "exec",
                        str(item["pod_name"]),
                        "-c",
                        "main",
                        "--",
                        *target.trace_command,
                    ]
                )
                worker_events = self._parse_worker_events(output, run, item)
                if not any(event.get("event_type") == "sample_consumed" for event in worker_events):
                    return None
                if not any(
                    event.get("event_type") == "workload_completed" for event in worker_events
                ):
                    return None
                events.extend(worker_events)
            return events

        events = cast(
            list[JsonObject],
            _wait(
                "managed-worker measurement trace",
                collect,
                timeout=target.timeout,
                interval=target.poll_interval,
            ),
        )
        if len(events) > MAX_EVENTS:
            raise DriverError("managed-worker trace contains too many events")
        if self._requires_worker_concurrency(request_value):
            overlap_ms, sandbox_ids = _worker_concurrency_overlap_ms(
                events, required_workers=required_workers
            )
            physical_ids = sorted(
                {
                    str(device_id)
                    for item in targets
                    for device_id in cast(list[str], item["worker_device_ids"])
                }
            )
            requested_core_total = sum(
                int(item.get("requested_core_percent", 0)) for item in targets
            )
            allocated_core_total = sum(
                int(item.get("allocated_core_percent", 0)) for item in targets
            )
            events.append(
                self._common_event(run)
                | {
                    "event_type": "worker_concurrency_verified",
                    "source": "operator",
                    "worker_count": required_workers,
                    "sandbox_ids": sandbox_ids,
                    "shared_device_ids": physical_ids,
                    "requested_core_percent_total": requested_core_total,
                    "allocated_core_percent_total": allocated_core_total,
                    "overlap_ms": overlap_ms,
                }
            )
        self._write_artifact(response_path, "worker-measurement.json", {"events": events})
        return events

    def _stop(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        del response_path
        result = self._command(request_value, target, run, "stop")
        return [
            self._common_event(run)
            | {
                "event_type": "control_completed",
                "source": "operator",
                "action": "stop",
                "succeeded": True,
                "receipt_id": _operation_id(result),
            }
        ]

    def _cleanup(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        del response_path
        if run.get("job_id") and not run.get("run_id"):
            gateway = Gateway(target.gateway_url, target.timeout)
            runs = _items(
                gateway.get(f"/v1/jobs/{parse.quote(str(run['job_id']), safe='')}/runs"),
                "runs",
            )
            if len(runs) > 1:
                raise DriverError("provisioned Job has multiple runs; refusing ambiguous cleanup")
            if len(runs) == 1:
                run["run_id"] = _string(runs[0].get("runId"), label="cleanup service run ID")
            else:
                # Job definitions remain as audit records; no admitted run means
                # the Operator could not have materialized cluster resources.
                return []
        kube = Kubernetes(target)
        bundles = self._bundles(kube, run, required=False)
        if not run.get("stop_operation_id"):
            try:
                self._command(request_value, target, run, "terminate")
            except DriverError as exc:
                normalized = str(exc).lower()
                no_materialized_sandboxes = (
                    "no materialized sandboxes" in normalized and not bundles
                )
                if not no_materialized_sandboxes and not any(
                    marker in normalized
                    for marker in (
                        "invalid transition",
                        "cannot terminate run in state",
                        "not found",
                        "http 404",
                    )
                ):
                    raise
        for bundle in bundles:
            bundle_spec = _mapping(
                _mapping(bundle.get("spec"), label="JobRunBundle spec").get("bundle"),
                label="JobRunBundle bundle",
            )
            if bundle_spec.get("sourceJobId") != run.get("job_id") or bundle_spec.get(
                "sourceRunId"
            ) != run.get("run_id"):
                raise DriverError("cleanup bundle identity changed after selection")
            resources = (
                ("jobs.batch", bundle_spec.get("job")),
                ("workloads.kueue.x-k8s.io", bundle_spec.get("workload")),
                (
                    "resourceclaimtemplates.resource.k8s.io",
                    bundle_spec.get("resourceClaimTemplate"),
                ),
                (
                    "resourceclaims.resource.k8s.io",
                    bundle_spec.get("resourceClaim"),
                ),
            )
            for resource, value in resources:
                if not isinstance(value, dict):
                    continue
                name = _string(
                    _mapping(value.get("metadata"), label=f"{resource} metadata").get("name"),
                    label=f"{resource} name",
                )
                live = kube.optional_json(["get", resource, name])
                if live is None:
                    continue
                labels = _mapping(
                    _mapping(live.get("metadata"), label=f"live {resource} metadata").get(
                        "labels", {}
                    ),
                    label=f"live {resource} labels",
                )
                if labels.get("tgsrl.io/job-id") != run.get("job_id") or labels.get(
                    "tgsrl.io/run-id"
                ) != run.get("run_id"):
                    raise DriverError(
                        f"refusing to delete {resource}/{name}: ownership labels do not match"
                    )
                kube.delete_preconditioned(resource, name, live)
            metadata = _mapping(bundle.get("metadata"), label="JobRunBundle metadata")
            name = _string(metadata.get("name"), label="JobRunBundle name")
            # Child names and ownership labels were verified above. Removing
            # this test-owned marker finalizer after its children are gone
            # prevents cleanup from hanging if the external Operator misses a
            # deletion watch or is stopped during teardown.
            live_bundle = kube.optional_json(["get", "jobrunbundles.tgsrl.io", name])
            if live_bundle is not None:
                live_spec = _mapping(
                    _mapping(live_bundle.get("spec"), label="live JobRunBundle spec").get("bundle"),
                    label="live JobRunBundle bundle",
                )
                if live_spec.get("sourceJobId") != run.get("job_id") or live_spec.get(
                    "sourceRunId"
                ) != run.get("run_id"):
                    raise DriverError(
                        f"refusing to delete jobrunbundles.tgsrl.io/{name}: identity changed"
                    )
                patched = kube.clear_finalizers_preconditioned(
                    "jobrunbundles.tgsrl.io", name, live_bundle
                )
                kube.delete_preconditioned("jobrunbundles.tgsrl.io", name, patched)
        return []

    def _inject_fault(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        return self._fault(request_value, target, run, response_path, "inject", "fault_injected")

    def _recover_fault(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, response_path: Path
    ) -> list[JsonObject]:
        started = time.monotonic()
        events = self._fault(
            request_value, target, run, response_path, "recover", "fault_recovered"
        )
        events[0]["recovery_time_ms"] = (time.monotonic() - started) * 1000.0
        return events

    def _fault(
        self,
        request_value: JsonObject,
        target: TargetConfig,
        run: JsonObject,
        response_path: Path,
        phase: str,
        event_type: str,
    ) -> list[JsonObject]:
        fault_id = _string(request_value.get("fault_id"), label="request.fault_id")
        hook = target.fault_hooks.get(fault_id, {}).get(phase)
        if hook is None:
            raise DriverError(f"fault {fault_id!r} has no explicit {phase} hook")
        hook_result = self._run_hook(
            hook,
            request_value,
            run,
            response_path,
            authority="target-environment",
        )
        event = self._common_event(run) | {
            "event_type": event_type,
            "source": "operator",
            "fault_id": fault_id,
        }
        if "recovery_time_ms" in hook_result:
            event["recovery_time_ms"] = hook_result["recovery_time_ms"]
        return [event]

    def _command(
        self, request_value: JsonObject, target: TargetConfig, run: JsonObject, command: str
    ) -> JsonObject:
        gateway = Gateway(target.gateway_url, target.timeout)
        key = _operation_key(request_value, run, command)
        result = gateway.post(
            f"/v1/jobs/{parse.quote(str(run['job_id']), safe='')}/runs/"
            f"{parse.quote(str(run['run_id']), safe='')}/commands/{command}",
            {"actor": "hardware-gate", "reason": f"hardware Gate {command}"},
            key,
        )
        operation_id = _operation_id(result)
        run[f"{command}_operation_id"] = operation_id
        self._wait_operation(gateway, operation_id, target)
        return result

    @staticmethod
    def _read_operation(gateway: Gateway, operation_id: str) -> JsonObject:
        payload = gateway.get(f"/v1/operations/{parse.quote(operation_id, safe='')}")
        operation = _mapping(payload.get("operation"), label="operation")
        if operation.get("state") == "OPERATION_STATE_FAILED":
            raise TerminalDriverError(
                f"TGS-RL operation {operation_id} failed: {operation.get('errorMessage', '')}"
            )
        return payload

    def _wait_operation(
        self, gateway: Gateway, operation_id: str, target: TargetConfig
    ) -> JsonObject:
        def poll() -> JsonObject | None:
            payload = self._read_operation(gateway, operation_id)
            operation = _mapping(payload.get("operation"), label="operation")
            return payload if operation.get("state") == "OPERATION_STATE_SUCCEEDED" else None

        return cast(
            JsonObject,
            _wait(
                f"operation {operation_id}",
                poll,
                timeout=target.timeout,
                interval=target.poll_interval,
            ),
        )

    def _latest_decision(self, target: TargetConfig, run: JsonObject) -> JsonObject | None:
        gateway = Gateway(target.gateway_url, target.timeout)
        payload = gateway.get(
            f"/v1/jobs/{parse.quote(str(run['job_id']), safe='')}/decisions"
            f"?run_id={parse.quote(str(run['run_id']), safe='')}&limit=100"
        )
        decisions = [item for item in _items(payload, "decisions") if not item.get("fallback")]
        return max(decisions, key=_decision_sequence) if decisions else None

    def _wait_decision(
        self,
        target: TargetConfig,
        run: JsonObject,
        action: str,
        *,
        after_sequence: int,
    ) -> JsonObject:
        def poll() -> JsonObject | None:
            gateway = Gateway(target.gateway_url, target.timeout)
            if action == "bind" and run.get("start_operation_id"):
                self._read_operation(gateway, str(run["start_operation_id"]))
            payload = gateway.get(
                f"/v1/jobs/{parse.quote(str(run['job_id']), safe='')}/decisions"
                f"?run_id={parse.quote(str(run['run_id']), safe='')}&limit=100"
            )
            decisions = sorted(_items(payload, "decisions"), key=_decision_sequence, reverse=True)
            for decision in decisions:
                if decision.get("fallback") or _decision_sequence(decision) <= after_sequence:
                    continue
                if _decision_action(decision, action) is None:
                    continue
                if action == "rebind":
                    previous = cast(JsonObject, run.get("decision", {}))
                    before = {tuple(_device_ids(item)) for item in _plan_bindings(previous)}
                    after = {tuple(_device_ids(item)) for item in _plan_bindings(decision)}
                    if not after or after == before:
                        continue
                return decision
            return None

        return cast(
            JsonObject,
            _wait(
                f"successful Scheduler {action} decision",
                poll,
                timeout=target.timeout,
                interval=target.poll_interval,
            ),
        )

    def _wait_running_topology(
        self, gateway: Gateway, run: JsonObject, target: TargetConfig
    ) -> JsonObject:
        def poll() -> JsonObject | None:
            topology = gateway.get(
                f"/v1/jobs/{parse.quote(str(run['job_id']), safe='')}/topology"
                f"?run_id={parse.quote(str(run['run_id']), safe='')}"
            )
            sandboxes = _items(topology, "sandboxes")
            if sandboxes and all(
                item.get("state") == "RUNTIME_STATE_RUNNING" for item in sandboxes
            ):
                return topology
            return None

        return cast(
            JsonObject,
            _wait(
                "Runtime RUNNING after worker registration",
                poll,
                timeout=target.timeout,
                interval=target.poll_interval,
            ),
        )

    def _bundles(
        self, kube: Kubernetes, run: Mapping[str, Any], *, required: bool = True
    ) -> list[JsonObject]:
        payload = kube.json(["get", "jobrunbundles.tgsrl.io"])
        matches = []
        for item in _items(payload, "items"):
            bundle = _mapping(
                _mapping(item.get("spec"), label="JobRunBundle spec").get("bundle"),
                label="JobRunBundle bundle",
            )
            if bundle.get("sourceJobId") == run.get("job_id") and bundle.get(
                "sourceRunId"
            ) == run.get("run_id"):
                matches.append(item)
        if required and not matches:
            raise DriverError("no JobRunBundle matches the service job/run")
        return matches

    def _kubernetes_targets(
        self,
        target: TargetConfig,
        run: JsonObject,
        topology: JsonObject,
        *,
        require_ready: bool,
    ) -> list[JsonObject]:
        kube = Kubernetes(target)
        by_tuple: dict[tuple[str, str], JsonObject] = {}
        if target.execution_mode == "kubernetes-dra":
            inventory = _dra_inventory(
                kube.json(["get", "resourceslices.resource.k8s.io"], namespaced=False)
            )
            by_tuple = {(str(item["pool"]), str(item["device"])): item for item in inventory}
        pods_payload = kube.json(["get", "pods"])
        pods = []
        for item in _items(pods_payload, "items"):
            metadata = _mapping(item.get("metadata"), label="Pod metadata")
            labels = _mapping(metadata.get("labels", {}), label="Pod labels")
            if labels.get("tgsrl.io/job-id") == run.get("job_id") and labels.get(
                "tgsrl.io/run-id"
            ) == run.get("run_id"):
                pods.append(item)
        topology_sandboxes = {
            str(item.get("sandboxId")): item for item in _items(topology, "sandboxes")
        }
        results: list[JsonObject] = []
        for bundle_object in self._latest_bundles(self._bundles(kube, run)):
            bundle = _mapping(
                _mapping(bundle_object.get("spec"), label="JobRunBundle spec").get("bundle"),
                label="JobRunBundle bundle",
            )
            runtime_targets = _items(bundle, "runtimeTargets")
            if len(runtime_targets) != 1:
                raise DriverError("each hardware JobRunBundle must contain one runtime target")
            runtime_target = runtime_targets[0]
            sandbox_id = _string(runtime_target.get("sandboxId"), label="runtime target sandbox ID")
            sandbox = topology_sandboxes.get(sandbox_id)
            if sandbox is None:
                raise DriverError(f"Runtime topology omitted sandbox {sandbox_id}")
            binding = _mapping(sandbox.get("binding"), label="sandbox binding")
            scheduler_ids = _device_ids(binding)
            job = _mapping(bundle.get("job"), label="bundle job")
            job_name = _string(
                _mapping(job.get("metadata"), label="bundle Job metadata").get("name"),
                label="bundle Job name",
            )
            matching_pods = []
            for pod in pods:
                metadata = _mapping(pod.get("metadata"), label="Pod metadata")
                owners = _list(metadata.get("ownerReferences", []), label="Pod ownerReferences")
                if any(
                    isinstance(owner, dict)
                    and owner.get("kind") == "Job"
                    and owner.get("name") == job_name
                    for owner in owners
                ):
                    matching_pods.append(pod)
            if len(matching_pods) != 1:
                raise DriverError(
                    f"expected one Pod owned by Job {job_name}, found {len(matching_pods)}"
                )
            pod = matching_pods[0]
            pod_metadata = _mapping(pod.get("metadata"), label="Pod metadata")
            if require_ready and not self._pod_ready(pod):
                raise DriverError(f"Pod {pod_metadata.get('name')} is not Ready")
            if target.execution_mode == "kubernetes-dra":
                allocated_ids, expected_class, parent_ids, allocation_evidence = (
                    self._dra_target_allocation(kube, pod, scheduler_ids, target, by_tuple)
                )
            else:
                allocated_ids, expected_class, parent_ids, allocation_evidence = (
                    self._hami_target_allocation(bundle, pod, scheduler_ids)
                )
            output = kube.run(
                [
                    "exec",
                    str(pod_metadata["name"]),
                    "-c",
                    "main",
                    "--",
                    "nvidia-smi",
                    "-L",
                ]
            )
            worker_ids = _visible_device_ids(output, target.gpu_profile)
            if target.gpu_profile == "mig" and len(parent_ids) != 1:
                raise DriverError("MIG allocation does not identify one parent UUID")
            results.append(
                {
                    "sandbox_id": sandbox_id,
                    "binding_id": str(binding.get("bindingId", "")),
                    "generation": int(sandbox.get("generation", 0)),
                    "runtime_unit_id": str(
                        binding.get("runtimeUnitId")
                        or binding.get("pendingUnitId")
                        or runtime_target.get("runtimeUnitId", "")
                    ),
                    "worker_id": _string(
                        binding.get("pendingUnitId"),
                        label="binding pending unit ID",
                    ),
                    "node_id": _string(
                        _mapping(pod.get("spec"), label="Pod spec").get("nodeName"),
                        label="Pod node name",
                    ),
                    "pod_name": str(pod_metadata["name"]),
                    "scheduler_device_ids": scheduler_ids,
                    "allocated_device_ids": allocated_ids,
                    "worker_device_ids": worker_ids,
                    "device_class": expected_class,
                    "parent_uuid": next(iter(parent_ids), ""),
                    **allocation_evidence,
                }
            )
        return results

    def _dra_target_allocation(
        self,
        kube: Kubernetes,
        pod: JsonObject,
        scheduler_ids: list[str],
        target: TargetConfig,
        by_tuple: Mapping[tuple[str, str], JsonObject],
    ) -> tuple[list[str], str, set[str], JsonObject]:
        claim_name = self._pod_generated_claim_name(pod)
        live_claim = kube.json(["get", "resourceclaims.resource.k8s.io", claim_name])
        status = _mapping(live_claim.get("status"), label="ResourceClaim status")
        allocation = _mapping(status.get("allocation"), label="ResourceClaim allocation")
        allocation_results = _items(
            _mapping(allocation.get("devices"), label="ResourceClaim allocation devices"),
            "results",
        )
        if len(allocation_results) != len(scheduler_ids):
            raise DriverError("ResourceClaim allocation count does not match the Scheduler binding")
        allocated: list[JsonObject] = []
        for allocation_result in allocation_results:
            if allocation_result.get("driver") != NVIDIA_DRIVER:
                raise DriverError("ResourceClaim allocation is not owned by the NVIDIA DRA driver")
            key = (
                str(allocation_result.get("pool", "")),
                str(allocation_result.get("device", "")),
            )
            if key not in by_tuple:
                raise DriverError(
                    f"allocated DRA device {key[0]}/{key[1]} is absent from the latest "
                    "ResourceSlice generation"
                )
            allocated.append(by_tuple[key])
        allocated_ids = sorted(str(item["uuid"]) for item in allocated)
        expected_class = PROFILE_CLASS[target.gpu_profile]
        claim_class = self._claim_class(live_claim)
        if claim_class != expected_class or any(
            item["device_class"] != expected_class for item in allocated
        ):
            raise DriverError("ResourceClaim DeviceClass does not match the configured GPU profile")
        parent_ids = {
            str(item.get("parent_uuid", "")) for item in allocated if item.get("parent_uuid")
        }
        return allocated_ids, expected_class, parent_ids, {"allocation_mode": "kubernetes-dra"}

    @staticmethod
    def _hami_target_allocation(
        bundle: JsonObject, pod: JsonObject, scheduler_ids: list[str]
    ) -> tuple[list[str], str, set[str], JsonObject]:
        if len(scheduler_ids) != 1:
            raise DriverError("HAMi workload requires exactly one Scheduler device UUID")
        if bundle.get("gpuProfile") != HAMI_PROFILE:
            raise DriverError("JobRunBundle did not select the hami-vgpu realization profile")
        job = _mapping(bundle.get("job"), label="bundle Job")
        template = _mapping(
            _mapping(job.get("spec"), label="bundle Job spec").get("template"),
            label="bundle Job template",
        )
        metadata = _mapping(template.get("metadata"), label="bundle Pod metadata")
        annotations = _mapping(metadata.get("annotations", {}), label="bundle Pod annotations")
        pod_spec = _mapping(template.get("spec"), label="bundle Pod spec")
        if pod_spec.get("schedulerName") != HAMI_SCHEDULER:
            raise DriverError("HAMi workload does not use the hami-scheduler")
        if (
            annotations.get(HAMI_USE_UUID_ANNOTATION) != scheduler_ids[0]
            or annotations.get(HAMI_MODE_ANNOTATION) != "hami-core"
        ):
            raise DriverError("HAMi workload annotations do not match the Scheduler binding")
        requested_core = _positive_integer_quantity(annotations.get(HAMI_EXPECTED_CORE_ANNOTATION))
        requested_memory = _positive_integer_quantity(
            annotations.get(HAMI_EXPECTED_MEMORY_ANNOTATION)
        )
        if requested_core == 0 or requested_memory == 0:
            raise DriverError("HAMi workload expected allocation annotations are missing")
        containers = _items(pod_spec, "containers")
        if len(containers) != 1:
            raise DriverError("HAMi workload must contain exactly one main container")
        limits = _mapping(
            _mapping(containers[0].get("resources"), label="HAMi container resources").get(
                "limits"
            ),
            label="HAMi container resource limits",
        )
        if (
            str(limits.get(HAMI_GPU_RESOURCE, "")) != "1"
            or _positive_integer_quantity(limits.get(HAMI_CORE_RESOURCE)) != requested_core
            or _positive_integer_quantity(limits.get(HAMI_MEMORY_PERCENT_RESOURCE))
            != requested_core
        ):
            raise DriverError("HAMi workload resources do not match the expected share")
        pod_metadata = _mapping(pod.get("metadata"), label="Pod metadata")
        live_annotations = _mapping(pod_metadata.get("annotations", {}), label="Pod annotations")
        allocations = _hami_allocations(
            _string(
                live_annotations.get(HAMI_ALLOCATED_ANNOTATION),
                label="HAMi allocation annotation",
            )
        )
        if len(allocations) != 1:
            raise DriverError("HAMi workload must report exactly one allocated physical GPU")
        allocation = allocations[0]
        allocated_ids = [str(allocation["uuid"])]
        if allocated_ids != scheduler_ids:
            raise DriverError("HAMi allocation does not match the Scheduler binding")
        if (
            int(allocation["memory_mib"]) != requested_memory
            or int(allocation["core_percent"]) != requested_core
        ):
            raise DriverError("HAMi allocation does not match the requested memory/core share")
        return (
            allocated_ids,
            "",
            set(),
            {
                "allocation_mode": HAMI_PROFILE,
                "requested_core_percent": requested_core,
                "allocated_core_percent": int(allocation["core_percent"]),
                "requested_memory_mib": requested_memory,
                "allocated_memory_mib": int(allocation["memory_mib"]),
            },
        )

    @staticmethod
    def _pod_generated_claim_name(pod: Mapping[str, Any]) -> str:
        status = _mapping(pod.get("status"), label="Pod status")
        claim_statuses = _items(status, "resourceClaimStatuses")
        names = {
            _string(item.get("resourceClaimName"), label="generated ResourceClaim name")
            for item in claim_statuses
            if item.get("name") == "accelerator" and item.get("resourceClaimName")
        }
        if len(names) != 1:
            raise DriverError(
                "Pod must report exactly one generated accelerator "
                f"ResourceClaim, found {len(names)}"
            )
        return next(iter(names))

    @staticmethod
    def _latest_bundles(values: Sequence[JsonObject]) -> list[JsonObject]:
        latest: dict[str, tuple[int, JsonObject]] = {}
        for value in values:
            bundle = _mapping(
                _mapping(value.get("spec"), label="JobRunBundle spec").get("bundle"),
                label="JobRunBundle bundle",
            )
            targets = _items(bundle, "runtimeTargets")
            if len(targets) != 1:
                raise DriverError("each hardware JobRunBundle must contain one runtime target")
            sandbox_id = _string(targets[0].get("sandboxId"), label="runtime target sandbox ID")
            generation = int(bundle.get("generation", 0))
            if generation <= 0:
                raise DriverError("JobRunBundle generation must be positive")
            current = latest.get(sandbox_id)
            if current is None or generation > current[0]:
                latest[sandbox_id] = (generation, value)
            elif generation == current[0]:
                raise DriverError(
                    f"sandbox {sandbox_id} has duplicate latest JobRunBundle generation"
                )
        return [latest[key][1] for key in sorted(latest)]

    @staticmethod
    def _pod_ready(pod: Mapping[str, Any]) -> bool:
        status = pod.get("status", {})
        if not isinstance(status, dict):
            return False
        conditions = status.get("conditions", [])
        return isinstance(conditions, list) and any(
            isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True"
            for item in conditions
        )

    @staticmethod
    def _claim_class(claim: Mapping[str, Any]) -> str:
        spec = _mapping(claim.get("spec"), label="ResourceClaim spec")
        requests = _items(_mapping(spec.get("devices"), label="ResourceClaim devices"), "requests")
        if len(requests) != 1:
            raise DriverError("ResourceClaim must contain exactly one device request")
        exact = requests[0].get("exactly")
        source = (
            requests[0] if exact is None else _mapping(exact, label="ResourceClaim exact request")
        )
        return _string(source.get("deviceClassName"), label="ResourceClaim DeviceClass")

    def _parse_worker_events(
        self, output: str, run: Mapping[str, Any], target: Mapping[str, Any]
    ) -> list[JsonObject]:
        result: list[JsonObject] = []
        for line in output.splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise DriverError("managed-worker trace contains invalid NDJSON") from exc
            event = _mapping(event, label="managed-worker trace event")
            if _contains_sensitive_field(event):
                raise DriverError("managed-worker trace contains credential-like fields")
            if event.get("run_id") != run.get("run_id") or event.get("job_id") != run.get("job_id"):
                raise DriverError("managed-worker trace job/run identity does not match")
            if event.get("sandbox_id") != target.get("sandbox_id") or int(
                event.get("generation", 0)
            ) != int(target.get("generation", 0)):
                raise DriverError("managed-worker trace sandbox generation does not match")
            for field in ("runtime_unit_id", "worker_id"):
                if event.get(field) not in {None, "", target.get(field)}:
                    raise DriverError(
                        f"managed-worker trace {field} does not match the registered worker"
                    )
            raw_device_ids = event.get("device_ids", [])
            if not isinstance(raw_device_ids, list) or sorted(
                str(item) for item in raw_device_ids
            ) != sorted(cast(list[str], target["worker_device_ids"])):
                raise DriverError("managed-worker trace device identity does not match")
            event.update(self._target_event(run, target))
            event["source"] = "worker"
            result.append(event)
        return result

    def _run_hook(
        self,
        argv_template: Sequence[str],
        request_value: JsonObject,
        run: JsonObject,
        response_path: Path,
        *,
        authority: str,
    ) -> JsonObject:
        hook_root = response_path.parent / "hook"
        hook_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        hook_request = hook_root / "request.json"
        hook_response = hook_root / "response.json"
        payload = {
            "schema_version": HOOK_REQUEST_SCHEMA,
            "request_id": request_value["request_id"],
            "campaign_id": request_value["campaign_id"],
            "experiment_id": request_value["experiment_id"],
            "run_key": request_value["run_key"],
            "job_id": run["job_id"],
            "run_id": run["run_id"],
            "action": request_value.get("action", ""),
            "fault_id": request_value.get("fault_id", ""),
        }
        _write_json(hook_request, payload)
        variables = {
            "REQUEST_PATH": str(hook_request),
            "RESPONSE_PATH": str(hook_response),
        }
        argv = [cast(str, _expand_placeholders(item, variables)) for item in argv_template]
        argv[0] = _resolve_executable(argv[0], label="hardware environment hook")
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.config.target(str(request_value["experiment_id"])).timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise DriverError("hardware environment hook timed out") from exc
        if completed.returncode != 0:
            raise DriverError(
                f"hardware environment hook exited {completed.returncode}: "
                + _safe_detail(completed.stderr)
            )
        hook_result = _read_json(hook_response)
        if (
            hook_result.get("schema_version") != HOOK_RESPONSE_SCHEMA
            or hook_result.get("request_id") != request_value["request_id"]
            or hook_result.get("status") != "SUCCEEDED"
            or hook_result.get("authority") != authority
        ):
            raise DriverError("hardware environment hook returned an invalid authority receipt")
        if _contains_sensitive_field(hook_result):
            raise DriverError("hardware environment hook response contains credential-like fields")
        return hook_result

    def _validate_job(
        self, job: JsonObject, request_value: JsonObject, target: TargetConfig
    ) -> None:
        scenario = _mapping(request_value.get("scenario"), label="request.scenario")
        expected = _mapping(scenario.get("workload"), label="scenario.workload")
        runtime = _mapping(job.get("runtime"), label="job.runtime")
        image_digest = _string(runtime.get("imageDigest"), label="job.runtime.imageDigest")
        image_reference = _string(runtime.get("artifactUri"), label="job.runtime.artifactUri")
        if re.fullmatch(
            r"sha256:[a-f0-9]{64}", image_digest
        ) is None or not image_reference.endswith("@" + image_digest):
            raise DriverError(
                "job template must use a pullable immutable workload image ending in @imageDigest"
            )
        field_map = {
            "framework": "framework",
            "execution_backend": "executionBackend",
            "trainer": "trainer",
            "rollout_engine": "rolloutEngine",
        }
        for scenario_field, job_field in field_map.items():
            if scenario_field in expected and runtime.get(job_field) != expected[scenario_field]:
                raise DriverError(f"job template {job_field} does not match the scenario workload")
        lock = _mapping(request_value.get("workload_lock"), label="request.workload_lock")
        if job.get("algorithm") != lock.get("algorithm"):
            raise DriverError("job template algorithm does not match workload_lock")
        rollout = str(job.get("rolloutMode", "")).removeprefix("ROLLOUT_MODE_").lower()
        if rollout != lock.get("rollout_mode"):
            raise DriverError("job template rolloutMode does not match workload_lock")
        if job.get("dataKind") != "DATA_KIND_LIVE":
            raise DriverError("hardware campaign job template must use DATA_KIND_LIVE")
        resources = _mapping(job.get("resourcesPerUnit"), label="job.resourcesPerUnit")
        accelerator_units = float(resources.get("acceleratorUnits", 0))
        if target.execution_mode == "kubernetes-dra" and accelerator_units != 1:
            raise DriverError(
                "Kubernetes DRA hardware jobs require one whole GPU or MIG device per unit"
            )
        if target.execution_mode == HAMI_PROFILE and not 0 < accelerator_units <= 1:
            raise DriverError("HAMi hardware jobs require acceleratorUnits within (0,1]")
        required_workers = self._required_workers(request_value)
        if int(job.get("desiredUnits", 0)) != required_workers:
            raise DriverError("job template desiredUnits is below the scenario worker requirement")
        version_lock = _mapping(lock.get("version_lock"), label="workload_lock.version_lock")
        if runtime.get("compatibilityProfile") != version_lock.get("compatibility_profile"):
            raise DriverError("job template compatibilityProfile does not match workload_lock")
        capabilities = _mapping(job.get("requiredCapabilities"), label="job.requiredCapabilities")
        names = capabilities.get("names", [])
        if not isinstance(names, list) or "nvidia-gpu" not in names:
            raise DriverError("job template must require the nvidia-gpu capability")
        execution_plan = _mapping(scenario.get("execution_plan"), label="scenario.execution_plan")
        variant_steps = _list(
            execution_plan.get("variant"), label="scenario.execution_plan.variant"
        )
        required_actions_raw = [
            step.get("action")
            for raw_step in variant_steps
            if (step := _mapping(raw_step, label="scenario execution step")).get("operation")
            == "apply_action"
        ]
        supported = capabilities.get("supportedActions", [])
        if not isinstance(supported, list) or not (
            {"bind"} | {str(item) for item in required_actions_raw if str(item) in PROVIDER_ACTIONS}
        ).issubset(set(supported)):
            raise DriverError("job template does not declare the scenario actions")
        if target.gpu_profile == "mig" and "nvidia-mig" not in names:
            raise DriverError("MIG job template must require the nvidia-mig capability")

    @staticmethod
    def _required_workers(request_value: Mapping[str, Any]) -> int:
        scenario = _mapping(request_value.get("scenario"), label="request.scenario")
        topology = _mapping(scenario.get("topology"), label="scenario.topology")
        value = topology.get("minimum_workers", topology.get("minimum_nodes", 1))
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise DriverError("scenario.topology.minimum_workers must be a positive integer")
        return value

    @staticmethod
    def _requires_worker_concurrency(request_value: Mapping[str, Any]) -> bool:
        scenario = _mapping(request_value.get("scenario"), label="request.scenario")
        required = scenario.get("required_evidence", [])
        if not isinstance(required, list):
            raise DriverError("scenario.required_evidence must be a list")
        return "worker-concurrency" in required

    @staticmethod
    def _common_event(run: Mapping[str, Any]) -> JsonObject:
        return {
            "service_job_id": run["job_id"],
            "service_run_id": run["run_id"],
            "device": "cuda",
        }

    @classmethod
    def _target_event(cls, run: Mapping[str, Any], target: Mapping[str, Any]) -> JsonObject:
        return cls._common_event(run) | {
            "node_id": target["node_id"],
            "runtime_unit_id": target["runtime_unit_id"],
            "worker_id": target["worker_id"],
            "sandbox_id": target["sandbox_id"],
            "binding_id": target["binding_id"],
            "generation": target["generation"],
        }

    @staticmethod
    def _write_artifact(response_path: Path, name: str, value: Mapping[str, Any]) -> None:
        _write_json(response_path.parent / name, value)

    @staticmethod
    def _artifact_record(response_path: Path, name: str) -> JsonObject:
        path = response_path.parent / name
        return {
            "path": name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }

    @staticmethod
    def _operation_artifacts(response_path: Path) -> list[JsonObject]:
        root = response_path.parent.resolve()
        reserved = {"request.json", "response.json", "stdout.log", "stderr.log"}
        result: list[JsonObject] = []
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.is_symlink() or path.name in reserved:
                continue
            resolved = path.resolve()
            if root != resolved and root not in resolved.parents:
                raise DriverError("hardware driver artifact escapes the operation directory")
            payload = resolved.read_bytes()
            for key, secret in os.environ.items():
                if _is_sensitive_name(key) and len(secret) >= 8 and secret.encode() in payload:
                    raise DriverError(
                        f"hardware driver artifact {path.name} contains a sensitive "
                        "environment value"
                    )
            result.append(
                {
                    "path": resolved.relative_to(root).as_posix(),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
        if len(result) > 256:
            raise DriverError("hardware driver produced too many artifacts")
        if sum((root / item["path"]).stat().st_size for item in result) > (512 << 20):
            raise DriverError("hardware driver artifacts exceed the size limit")
        return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--response", required=True)
    parser.add_argument("--config", default=os.environ.get("TGSRL_HARDWARE_DRIVER_CONFIG", ""))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    response_path = Path(args.response).expanduser().resolve()
    request_id = "unknown"
    try:
        if not args.config:
            raise DriverError("--config or TGSRL_HARDWARE_DRIVER_CONFIG is required")
        request_value = _read_json(Path(args.request).expanduser().resolve())
        request_id = str(request_value.get("request_id", "unknown"))
        response = HardwareEnvironmentDriver(load_config(Path(args.config))).execute(
            request_value, response_path
        )
    except (DriverError, OSError, subprocess.SubprocessError, ValueError) as exc:
        response = {
            "schema_version": RESPONSE_SCHEMA,
            "request_id": request_id,
            "status": "FAILED",
            "detail": _safe_detail(exc),
            "events": [],
        }
    _write_json(response_path, response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
