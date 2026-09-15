#!/usr/bin/env python3
"""Execute one repository-defined hardware scenario through an atomic environment driver."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
GATE_TOOLS_PATH = ROOT / "scripts" / "gate-tools.py"
DRIVER_RESPONSE_SCHEMA = "tgsrl.io/hardware-driver-response/v1alpha1"
DRIVER_REQUEST_SCHEMA = "tgsrl.io/hardware-driver-request/v1alpha1"
ALLOWED_OPERATIONS = frozenset(
    {
        "preflight",
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
MAX_RESPONSE_BYTES = 4 << 20
MAX_EVENTS_PER_RESPONSE = 10_000
MAX_DRIVER_ARTIFACTS = 256
MAX_DRIVER_ARTIFACT_BYTES = 512 << 20
SENSITIVE_FIELD_NAMES = frozenset(
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
        "session_token",
        "secret",
        "token",
    }
)
SENSITIVE_FIELD_TOKENS = frozenset(name.replace("_", "") for name in SENSITIVE_FIELD_NAMES)


class ExecutionError(ValueError):
    """Raised when a driver or scenario violates the execution contract."""


@dataclass(frozen=True, slots=True)
class DriverFailure(ExecutionError):
    message: str
    artifacts: tuple[dict[str, Any], ...] = ()

    def __str__(self) -> str:
        return self.message


def _load_gate_tools() -> Any:
    spec = importlib.util.spec_from_file_location("tgsrl_gate_tools_executor", GATE_TOOLS_PATH)
    if spec is None or spec.loader is None:
        raise ExecutionError("cannot load repository Gate tooling")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


GATE_TOOLS = _load_gate_tools()


def _fingerprint_value_present(value: object) -> bool:
    if value is None or value == "":
        return False
    return not isinstance(value, (dict, list, tuple, set)) or bool(value)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size > MAX_RESPONSE_BYTES:
        raise ExecutionError(f"missing or oversized JSON artifact: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ExecutionError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ExecutionError(f"{path} must contain one JSON object")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o600)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _is_sensitive_name(value: object) -> bool:
    normalized = str(value).casefold().replace("-", "_")
    compact = normalized.replace("_", "")
    return (
        normalized in SENSITIVE_FIELD_NAMES
        or compact in SENSITIVE_FIELD_TOKENS
        or any(normalized.endswith(f"_{name}") for name in SENSITIVE_FIELD_NAMES)
    )


def _contains_sensitive_field(value: object) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            if _is_sensitive_name(key) or _contains_sensitive_field(child):
                return True
    elif isinstance(value, list):
        return any(_contains_sensitive_field(child) for child in value)
    return False


def event_int(event: dict[str, Any], field: str) -> int:
    value = event.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        return 0
    return value


def event_string_set(event: dict[str, Any], field: str) -> set[str]:
    value = event.get(field)
    if not isinstance(value, list):
        return set()
    return {str(item).strip() for item in value if str(item).strip()}


def event_positive_number(event: dict[str, Any], field: str) -> bool:
    value = event.get(field)
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) > 0
    )


def _redact_sensitive_environment_values(payload: bytes) -> bytes:
    result = payload
    for key, value in os.environ.items():
        if _is_sensitive_name(key):
            encoded = value.encode()
            if len(encoded) >= 8:
                result = result.replace(encoded, b"[REDACTED]")
    return result


def _resolve_file(path: str, *, label: str, executable: bool = False) -> Path:
    raw = path.strip()
    if not raw:
        raise ExecutionError(f"{label} is required")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute() and len(candidate.parts) == 1:
        discovered = shutil.which(raw)
        if discovered is not None:
            candidate = Path(discovered)
    resolved = candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()
    if not resolved.is_file() or (executable and not os.access(resolved, os.X_OK)):
        raise ExecutionError(f"{label} is not an executable file: {resolved}")
    return resolved


def _scenario_plan(
    scenario: dict[str, Any], experiment: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    plan = scenario.get("execution_plan")
    if not isinstance(plan, dict) or set(plan) != {"baseline", "variant"}:
        raise ExecutionError("scenario execution_plan must define baseline and variant")
    result: dict[str, list[dict[str, Any]]] = {}
    for label in ("baseline", "variant"):
        steps = plan.get(label)
        if not isinstance(steps, list) or not steps:
            raise ExecutionError(f"scenario execution_plan.{label} must be non-empty")
        normalized: list[dict[str, Any]] = []
        for index, step in enumerate(steps):
            if not isinstance(step, dict) or set(step) - {"operation", "action", "fault_id"}:
                raise ExecutionError(f"scenario {label} step {index} is invalid")
            operation = step.get("operation")
            if operation not in ALLOWED_OPERATIONS - {"preflight"}:
                raise ExecutionError(f"scenario {label} step {index} has invalid operation")
            action = str(step.get("action", "")).strip()
            fault_id = str(step.get("fault_id", "")).strip()
            if operation in {"apply_action", "apply_worker_action"}:
                if not action or fault_id:
                    raise ExecutionError(f"scenario {label} action step {index} is invalid")
            elif action:
                raise ExecutionError(f"scenario {label} step {index} has an unexpected action")
            if operation in {"inject_fault", "recover_fault"} and (not fault_id or action):
                raise ExecutionError(f"scenario {label} fault step {index} is invalid")
            if operation not in {"inject_fault", "recover_fault"} and fault_id:
                raise ExecutionError(f"scenario {label} step {index} has an unexpected fault_id")
            normalized.append(dict(step))
        operations = [str(step["operation"]) for step in normalized]
        for required in (
            "provision",
            "launch",
            "verify_device_identity",
            "measure",
            "stop",
            "cleanup",
        ):
            if operations.count(required) != 1:
                raise ExecutionError(
                    f"scenario execution_plan.{label} must contain one {required} step"
                )
        ordered = [
            operations.index(operation)
            for operation in (
                "provision",
                "launch",
                "verify_device_identity",
                "measure",
                "stop",
                "cleanup",
            )
        ]
        if (
            operations[0] != "provision"
            or operations[-2:] != ["stop", "cleanup"]
            or ordered != sorted(ordered)
        ):
            raise ExecutionError(f"scenario execution_plan.{label} has unsafe ordering")
        bind_indexes = [
            index
            for index, step in enumerate(normalized)
            if step.get("operation") == "apply_action" and step.get("action") == "bind"
        ]
        if len(bind_indexes) != 1 or not (
            operations.index("provision") < bind_indexes[0] < operations.index("launch")
        ):
            raise ExecutionError(
                f"scenario execution_plan.{label} must bind once after provision and before launch"
            )
        allowed_actions = {"bind"}
        if label == "variant":
            allowed_actions.update(experiment["requirements"]["required_actions"])
        action_steps = [
            (index, str(step.get("action")))
            for index, step in enumerate(normalized)
            if step["operation"] in {"apply_action", "apply_worker_action"}
        ]
        if any(action not in allowed_actions for _index, action in action_steps):
            raise ExecutionError(f"scenario execution_plan.{label} has an undeclared action")
        if any(
            index <= operations.index("verify_device_identity")
            or index >= operations.index("measure")
            for index, action in action_steps
            if action not in {"bind", "rebind"}
        ):
            raise ExecutionError(
                f"scenario execution_plan.{label} controls workers outside the running window"
            )
        rebind_indexes = [index for index, action in action_steps if action == "rebind"]
        if rebind_indexes and not all(
            operations.index("launch") < index < operations.index("verify_device_identity")
            for index in rebind_indexes
        ):
            raise ExecutionError(
                f"scenario execution_plan.{label} must verify device identity after rebind"
            )
        fault_steps = [
            index
            for index, step in enumerate(normalized)
            if step["operation"] in {"inject_fault", "recover_fault"}
        ]
        if label == "baseline" and fault_steps:
            raise ExecutionError("scenario baseline must not inject or recover faults")
        if any(
            index <= operations.index("verify_device_identity") or index >= operations.index("stop")
            for index in fault_steps
        ):
            raise ExecutionError(
                f"scenario execution_plan.{label} faults must stay inside the running window"
            )
        if any(
            index >= operations.index("measure")
            for index, step in enumerate(normalized)
            if step["operation"] == "inject_fault"
        ):
            raise ExecutionError(
                f"scenario execution_plan.{label} must inject faults before measurement"
            )
        result[label] = normalized
    variant = result["variant"]
    planned_actions = [
        str(step.get("action"))
        for step in variant
        if step["operation"] in {"apply_action", "apply_worker_action"}
    ]
    missing_actions = set(experiment["requirements"]["required_actions"]) - set(planned_actions)
    if missing_actions:
        raise ExecutionError(
            "scenario variant omits required actions: " + ", ".join(sorted(missing_actions))
        )
    duplicated_actions = sorted(
        action for action in set(planned_actions) if planned_actions.count(action) != 1
    )
    if duplicated_actions:
        raise ExecutionError("scenario variant repeats actions: " + ", ".join(duplicated_actions))
    declared_faults = {
        str(record.get("fault_id"))
        for record in scenario.get("faults", [])
        if isinstance(record, dict) and record.get("fault_id")
    }
    planned_faults = {
        str(step.get("fault_id"))
        for step in variant
        if step["operation"] in {"inject_fault", "recover_fault"}
    }
    if planned_faults != declared_faults:
        raise ExecutionError("scenario variant fault steps do not match declared faults")
    for fault_id in experiment["requirements"]["required_faults"]:
        injected = [
            index
            for index, step in enumerate(variant)
            if step.get("operation") == "inject_fault" and step.get("fault_id") == fault_id
        ]
        recovered = [
            index
            for index, step in enumerate(variant)
            if step.get("operation") == "recover_fault" and step.get("fault_id") == fault_id
        ]
        if len(injected) != 1 or len(recovered) != 1 or recovered[0] <= injected[0]:
            raise ExecutionError(f"scenario variant must inject then recover fault {fault_id}")
    return result


def _validate_driver_response(
    response: dict[str, Any],
    *,
    request_id: str,
    operation: str,
    action: str = "",
    fault_id: str = "",
    minimum_nodes: int = 1,
    minimum_workers: int = 1,
    gpu_profile: str = "",
    execution_mode: str = "",
    shared_device_identity: bool = False,
    concurrency_required: bool = False,
    expected_total_core_percent: int = 0,
) -> list[dict[str, Any]]:
    minimum_workers = max(minimum_workers, minimum_nodes)
    if not execution_mode and gpu_profile:
        execution_mode = "kubernetes-dra"
    if _contains_sensitive_field(response):
        raise ExecutionError("hardware driver response contains credential-like fields")
    allowed_fields = {
        "schema_version",
        "request_id",
        "status",
        "detail",
        "events",
        "artifacts",
        "environment_fingerprint",
    }
    if set(response) - allowed_fields:
        raise ExecutionError("hardware driver response contains unsupported fields")
    if response.get("schema_version") != DRIVER_RESPONSE_SCHEMA:
        raise ExecutionError("hardware driver response schema is invalid")
    if response.get("request_id") != request_id:
        raise ExecutionError("hardware driver response request_id does not match")
    if response.get("status") != "SUCCEEDED":
        raise ExecutionError(
            f"hardware driver {operation} failed: {response.get('detail', 'no detail')}"
        )
    events = response.get("events", [])
    if (
        not isinstance(events, list)
        or len(events) > MAX_EVENTS_PER_RESPONSE
        or any(not isinstance(event, dict) for event in events)
    ):
        raise ExecutionError("hardware driver response events are invalid")
    expected_event = {
        "launch": "worker_registered",
        "verify_device_identity": "device_identity_verified",
        "inject_fault": "fault_injected",
        "recover_fault": "fault_recovered",
        "measure": "sample_consumed",
    }.get(operation)
    if expected_event and not any(event.get("event_type") == expected_event for event in events):
        raise ExecutionError(f"hardware driver {operation} omitted {expected_event} evidence")
    if operation == "launch":
        registered_workers = {
            str(event.get("sandbox_id", ""))
            for event in events
            if event.get("event_type") == "worker_registered"
            and event.get("source") == "worker"
            and event.get("sandbox_id")
        }
        if len(registered_workers) != minimum_workers:
            raise ExecutionError(
                "hardware driver launch returned the wrong number of worker identities"
            )
    if operation == "provision":
        artifacts = response.get("artifacts", [])
        if not isinstance(artifacts, list) or not any(
            isinstance(record, dict)
            and record.get("path") == "rendered-job.json"
            and str(record.get("sha256", "")).strip()
            for record in artifacts
        ):
            raise ExecutionError("hardware driver provision omitted rendered-job.json evidence")
    if operation == "measure" and not any(
        event.get("event_type") == "workload_completed" for event in events
    ):
        raise ExecutionError("hardware driver measure omitted workload_completed evidence")
    if operation == "measure":
        sample_nodes = {
            str(event.get("node_id", ""))
            for event in events
            if event.get("event_type") == "sample_consumed" and event.get("node_id")
        }
        completed_nodes = {
            str(event.get("node_id", ""))
            for event in events
            if event.get("event_type") == "workload_completed" and event.get("node_id")
        }
        if (
            len(sample_nodes) < minimum_nodes
            or len(completed_nodes) < minimum_nodes
            or sample_nodes != completed_nodes
        ):
            raise ExecutionError("hardware driver measure returned too few node identities")
        sample_workers = {
            str(event.get("sandbox_id", ""))
            for event in events
            if event.get("event_type") == "sample_consumed" and event.get("sandbox_id")
        }
        completed_workers = {
            str(event.get("sandbox_id", ""))
            for event in events
            if event.get("event_type") == "workload_completed" and event.get("sandbox_id")
        }
        if (
            len(sample_workers) < minimum_workers
            or len(completed_workers) < minimum_workers
            or sample_workers != completed_workers
        ):
            raise ExecutionError("hardware driver measure returned too few worker identities")
        if concurrency_required:
            concurrency = [
                event
                for event in events
                if event.get("event_type") == "worker_concurrency_verified"
                and event.get("source") == "operator"
            ]
            if len(concurrency) != 1:
                raise ExecutionError(
                    "hardware driver measure omitted one concurrency verification event"
                )
            evidence = concurrency[0]
            if (
                event_int(evidence, "worker_count") != minimum_workers
                or len(event_string_set(evidence, "sandbox_ids")) != minimum_workers
                or len(event_string_set(evidence, "shared_device_ids")) != 1
                or not event_positive_number(evidence, "overlap_ms")
            ):
                raise ExecutionError("hardware driver concurrency evidence is incomplete")
            requested_total = event_int(evidence, "requested_core_percent_total")
            allocated_total = event_int(evidence, "allocated_core_percent_total")
            if (
                requested_total <= 0
                or requested_total != allocated_total
                or requested_total > 100
                or (
                    expected_total_core_percent > 0
                    and requested_total != expected_total_core_percent
                )
            ):
                raise ExecutionError(
                    "hardware driver shared HAMi allocation has an invalid aggregate share"
                )
    if operation == "verify_device_identity":
        identities = [
            event for event in events if event.get("event_type") == "device_identity_verified"
        ]
        nodes = {str(event.get("node_id", "")) for event in identities if event.get("node_id")}
        if len(nodes) < minimum_nodes:
            raise ExecutionError("hardware driver device identity returned too few node identities")
        seen_devices: set[str] = set()
        seen_sandboxes: set[str] = set()
        seen_workers: set[str] = set()
        expected_device_class = (
            "mig.nvidia.com"
            if execution_mode == "kubernetes-dra" and gpu_profile == "mig"
            else "gpu.nvidia.com"
            if execution_mode == "kubernetes-dra"
            else ""
        )
        for event in identities:
            scheduler_ids = {str(value) for value in event.get("scheduler_device_ids", [])}
            allocated_ids = {str(value) for value in event.get("allocated_device_ids", [])}
            worker_ids = {str(value) for value in event.get("worker_device_ids", [])}
            if not scheduler_ids or scheduler_ids != allocated_ids or scheduler_ids != worker_ids:
                raise ExecutionError(
                    "hardware driver device identity disagrees across scheduler, "
                    "allocation, and worker"
                )
            sandbox_id = str(event.get("sandbox_id", "")).strip()
            worker_id = str(event.get("worker_id", "")).strip()
            if not sandbox_id or sandbox_id in seen_sandboxes:
                raise ExecutionError(
                    "hardware driver device identity has duplicate sandbox identity"
                )
            if not worker_id or worker_id in seen_workers:
                raise ExecutionError(
                    "hardware driver device identity has duplicate worker identity"
                )
            seen_sandboxes.add(sandbox_id)
            seen_workers.add(worker_id)
            if not shared_device_identity and seen_devices & worker_ids:
                raise ExecutionError("hardware driver device identity overlaps across nodes")
            seen_devices.update(worker_ids)
            if expected_device_class and event.get("device_class") != expected_device_class:
                raise ExecutionError(
                    "hardware driver device class does not match the preflight GPU profile"
                )
            if execution_mode == "hami-vgpu" and (
                event.get("allocation_mode") != "hami-vgpu"
                or event.get("requested_core_percent") != event.get("allocated_core_percent")
                or event.get("requested_memory_mib") != event.get("allocated_memory_mib")
            ):
                raise ExecutionError(
                    "hardware driver HAMi allocation does not match the requested share"
                )
            if gpu_profile == "mig" and not str(event.get("parent_uuid", "")).strip():
                raise ExecutionError("hardware driver MIG identity omitted parent UUID")
        if len(seen_sandboxes) != minimum_workers:
            raise ExecutionError("hardware driver device identity returned the wrong worker count")
        if shared_device_identity and (execution_mode != "hami-vgpu" or len(seen_devices) != 1):
            raise ExecutionError(
                "shared device identity requires one HAMi physical GPU across all workers"
            )
        if shared_device_identity:
            requested_total = sum(
                event_int(event, "requested_core_percent") for event in identities
            )
            allocated_total = sum(
                event_int(event, "allocated_core_percent") for event in identities
            )
            if (
                requested_total <= 0
                or requested_total != allocated_total
                or requested_total > 100
                or (
                    expected_total_core_percent > 0
                    and requested_total != expected_total_core_percent
                )
            ):
                raise ExecutionError(
                    "hardware driver shared HAMi identity has an invalid aggregate share"
                )
    expected_action_source = "scheduler" if action == "bind" else "operator"
    if operation in {"apply_action", "apply_worker_action"} and not any(
        event.get("event_type") in {"decision_applied", "control_completed"}
        and event.get("source") == expected_action_source
        and event.get("action") == action
        and event.get("succeeded") is True
        for event in events
    ):
        raise ExecutionError(f"hardware driver omitted successful {action} action evidence")
    if operation == "apply_worker_action" and action in {"offload", "reload", "resume"}:
        controls = [
            event
            for event in events
            if event.get("event_type") == "control_completed"
            and event.get("source") == "operator"
            and event.get("action") == action
            and event.get("succeeded") is True
        ]
        if len(controls) != 1:
            raise ExecutionError(f"hardware driver returned ambiguous {action} evidence")
        before = controls[0].get("gpu_memory_allocated_before_bytes")
        after = controls[0].get("gpu_memory_allocated_after_bytes")
        reserved_before = controls[0].get("gpu_memory_reserved_before_bytes")
        reserved_after = controls[0].get("gpu_memory_reserved_after_bytes")
        values = (before, after, reserved_before, reserved_after)
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values
        ):
            raise ExecutionError(f"hardware driver {action} omitted GPU memory evidence")
        if action == "offload" and not (
            int(after) < int(before) and int(reserved_after) < int(reserved_before)
        ):
            raise ExecutionError(
                "managed worker offload did not reduce allocated/reserved GPU memory"
            )
        restores_memory = action == "reload" or (
            action == "resume" and controls[0].get("offloaded_before", True) is True
        )
        if restores_memory and not (
            int(after) > int(before) and int(reserved_after) > int(reserved_before)
        ):
            raise ExecutionError(
                f"managed worker {action} did not restore allocated/reserved GPU memory"
            )
    fault_event = {
        "inject_fault": "fault_injected",
        "recover_fault": "fault_recovered",
    }.get(operation)
    if fault_event and not any(
        event.get("event_type") == fault_event
        and event.get("fault_id") == fault_id
        and event.get("source") == "operator"
        for event in events
    ):
        raise ExecutionError(f"hardware driver {operation} returned the wrong fault identity")
    source_expectations = {
        "launch": ("worker_registered", "worker"),
        "verify_device_identity": ("device_identity_verified", "worker"),
        "measure": ("sample_consumed", "worker"),
    }
    expected_source = source_expectations.get(operation)
    if expected_source and not any(
        event.get("event_type") == expected_source[0] and event.get("source") == expected_source[1]
        for event in events
    ):
        raise ExecutionError(
            f"hardware driver {operation} omitted authoritative {expected_source[1]} evidence"
        )
    if operation == "measure" and not any(
        event.get("event_type") == "workload_completed" and event.get("source") == "worker"
        for event in events
    ):
        raise ExecutionError(
            "hardware driver measure omitted authoritative worker completion evidence"
        )
    return cast(list[dict[str, Any]], events)


def _run_driver(
    driver: Path, request: dict[str, Any], operation_root: Path, timeout: float
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    operation_root.mkdir(parents=True, exist_ok=False)
    operation_root.chmod(0o700)
    request_path = operation_root / "request.json"
    response_path = operation_root / "response.json"
    _write_json(request_path, request)
    stdout_path, stderr_path = operation_root / "stdout.log", operation_root / "stderr.log"
    try:
        completed = subprocess.run(
            [str(driver), "--request", str(request_path), "--response", str(response_path)],
            cwd=ROOT,
            capture_output=True,
            check=False,
            timeout=timeout,
            env=dict(os.environ),
        )
        stdout, stderr, exit_code = completed.stdout, completed.stderr, completed.returncode
    except subprocess.TimeoutExpired as error:
        stdout, stderr, exit_code = error.stdout or b"", error.stderr or b"", 124
    stdout_path.write_bytes(_redact_sensitive_environment_values(stdout))
    stderr_path.write_bytes(_redact_sensitive_environment_values(stderr))
    stdout_path.chmod(0o600)
    stderr_path.chmod(0o600)
    artifacts = [
        {"path": path, "sha256": _sha256(path)} for path in (request_path, stdout_path, stderr_path)
    ]
    if exit_code != 0:
        raise DriverFailure(
            f"hardware driver exited {exit_code} for {request['operation']}",
            tuple(artifacts),
        )
    if response_path.is_symlink():
        raise DriverFailure(
            "hardware driver response must not be a symbolic link", tuple(artifacts)
        )
    resolved_response = response_path.resolve()
    if operation_root != resolved_response and operation_root not in resolved_response.parents:
        raise DriverFailure("hardware driver response escapes its operation root", tuple(artifacts))
    raw_response = response_path.read_bytes()
    redacted_response = _redact_sensitive_environment_values(raw_response)
    if redacted_response != raw_response:
        response_path.write_bytes(
            json.dumps(
                {
                    "schema_version": DRIVER_RESPONSE_SCHEMA,
                    "request_id": request["request_id"],
                    "status": "REJECTED",
                    "detail": "sensitive environment values were redacted",
                },
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        response_path.chmod(0o600)
        artifacts.append({"path": response_path, "sha256": _sha256(response_path)})
        raise DriverFailure(
            "hardware driver response contains sensitive environment values",
            tuple(artifacts),
        )
    try:
        response = _read_json(response_path)
    except ExecutionError as error:
        raise DriverFailure(str(error), tuple(artifacts)) from error
    response_path.chmod(0o600)
    if _contains_sensitive_field(response):
        _write_json(
            response_path,
            {
                "schema_version": DRIVER_RESPONSE_SCHEMA,
                "request_id": request["request_id"],
                "status": "REJECTED",
                "detail": "credential-like fields were redacted",
            },
        )
        artifacts.append({"path": response_path, "sha256": _sha256(response_path)})
        raise DriverFailure(
            "hardware driver response contains credential-like fields", tuple(artifacts)
        )
    artifacts.append({"path": response_path, "sha256": _sha256(response_path)})
    driver_artifacts = response.get("artifacts", [])
    if not isinstance(driver_artifacts, list) or len(driver_artifacts) > MAX_DRIVER_ARTIFACTS:
        raise DriverFailure("hardware driver response artifacts are invalid", tuple(artifacts))
    artifact_bytes = 0
    for index, record in enumerate(driver_artifacts):
        if not isinstance(record, dict):
            raise DriverFailure(
                f"hardware driver artifact {index} must be an object", tuple(artifacts)
            )
        relative = Path(str(record.get("path", "")))
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise DriverFailure(
                f"hardware driver artifact {index} path is invalid", tuple(artifacts)
            )
        artifact = (operation_root / relative).resolve()
        if operation_root != artifact and operation_root not in artifact.parents:
            raise DriverFailure(
                f"hardware driver artifact {index} escapes its operation root",
                tuple(artifacts),
            )
        unresolved_artifact = operation_root / relative
        if (
            unresolved_artifact.is_symlink()
            or not artifact.is_file()
            or record.get("sha256") != _sha256(artifact)
        ):
            raise DriverFailure(
                f"hardware driver artifact {index} is missing or changed", tuple(artifacts)
            )
        artifact_bytes += artifact.stat().st_size
        if artifact_bytes > MAX_DRIVER_ARTIFACT_BYTES:
            raise DriverFailure("hardware driver artifacts exceed the size limit", tuple(artifacts))
        if _redact_sensitive_environment_values(artifact.read_bytes()) != artifact.read_bytes():
            raise DriverFailure(
                f"hardware driver artifact {index} contains sensitive environment values",
                tuple(artifacts),
            )
        artifacts.append({"path": artifact, "sha256": str(record["sha256"])})
    return response, artifacts


def _event_scope(
    event: dict[str, Any],
    *,
    experiment_id: str,
    label: str,
    phase: str,
    iteration: int,
    operation: str,
    request_id: str,
    step_index: int,
) -> dict[str, Any]:
    expected: dict[str, object] = {
        "experiment_id": experiment_id,
        "label": label,
        "phase": phase,
        "iteration": iteration,
        "driver_operation": operation,
        "driver_request_id": request_id,
        "driver_step_index": step_index,
    }
    scoped = dict(event)
    for key, value in expected.items():
        if key in scoped and scoped[key] != value:
            raise ExecutionError(f"hardware driver event {key} does not match request scope")
        scoped[key] = value
    if (
        not str(scoped.get("service_job_id", "")).strip()
        or not str(scoped.get("service_run_id", "")).strip()
    ):
        raise ExecutionError("hardware driver event is missing service job/run identity")
    if scoped.get("device") != "cuda":
        raise ExecutionError("hardware driver event must identify a CUDA observation")
    return scoped


def _run_iteration(
    *,
    driver: Path,
    campaign: dict[str, Any],
    experiment: dict[str, Any],
    scenario: dict[str, Any],
    gate_manifest: Any,
    output_root: Path,
    label: str,
    phase: str,
    iteration: int,
    plan: list[dict[str, Any]],
    timeout: float,
    gpu_profile: str,
    execution_mode: str,
    minimum_workers: int,
    shared_device_identity: bool,
    concurrency_required: bool,
    expected_total_core_percent: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    run_key = f"{experiment['experiment_id'].lower()}-{label}-{phase}-{iteration}"
    operations_root = output_root / "artifacts" / "services" / "hardware-driver" / run_key
    events: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    completed_operations: list[str] = []
    failure: Exception | None = None
    for step_index, step in enumerate(plan, start=1):
        operation = str(step["operation"])
        request_id = hashlib.sha256(
            f"{campaign['campaign_id']}\0{run_key}\0{step_index}\0{operation}".encode()
        ).hexdigest()
        request = {
            "schema_version": DRIVER_REQUEST_SCHEMA,
            "request_id": request_id,
            "campaign_id": campaign["campaign_id"],
            "experiment_id": experiment["experiment_id"],
            "evidence": experiment["minimum_evidence"],
            "label": label,
            "phase": phase,
            "iteration": iteration,
            "run_key": run_key,
            "operation": operation,
            "step_index": step_index,
            "action": step.get("action", ""),
            "fault_id": step.get("fault_id", ""),
            "workload_lock": gate_manifest.data["workload_lock"],
            "scenario": scenario,
        }
        try:
            response, produced = _run_driver(
                driver, request, operations_root / f"{step_index:02d}-{operation}", timeout
            )
            artifacts.extend(produced)
            operation_events = _validate_driver_response(
                response,
                request_id=request_id,
                operation=operation,
                action=str(step.get("action", "")),
                fault_id=str(step.get("fault_id", "")),
                minimum_nodes=int(experiment["requirements"]["minimum_nodes"]),
                minimum_workers=minimum_workers,
                gpu_profile=gpu_profile,
                execution_mode=execution_mode,
                shared_device_identity=shared_device_identity,
                concurrency_required=concurrency_required,
                expected_total_core_percent=expected_total_core_percent,
            )
            for event in operation_events:
                events.append(
                    _event_scope(
                        event,
                        experiment_id=str(experiment["experiment_id"]),
                        label=label,
                        phase=phase,
                        iteration=iteration,
                        operation=operation,
                        request_id=request_id,
                        step_index=step_index,
                    )
                )
            completed_operations.append(operation)
        except (ExecutionError, OSError, subprocess.SubprocessError) as error:
            if isinstance(error, DriverFailure):
                artifacts.extend(error.artifacts)
            failure = error
            break
    if failure is not None and (not completed_operations or completed_operations[-1] != "cleanup"):
        cleanup_index = len(plan) + 1
        cleanup_id = hashlib.sha256(
            f"{campaign['campaign_id']}\0{run_key}\0cleanup-after-failure".encode()
        ).hexdigest()
        cleanup_request = {
            "schema_version": DRIVER_REQUEST_SCHEMA,
            "request_id": cleanup_id,
            "campaign_id": campaign["campaign_id"],
            "experiment_id": experiment["experiment_id"],
            "evidence": experiment["minimum_evidence"],
            "label": label,
            "phase": phase,
            "iteration": iteration,
            "run_key": run_key,
            "operation": "cleanup",
            "step_index": cleanup_index,
            "action": "",
            "fault_id": "",
            "workload_lock": gate_manifest.data["workload_lock"],
            "scenario": scenario,
        }
        try:
            cleanup_response, produced = _run_driver(
                driver, cleanup_request, operations_root / f"{cleanup_index:02d}-cleanup", timeout
            )
            artifacts.extend(produced)
            _validate_driver_response(cleanup_response, request_id=cleanup_id, operation="cleanup")
        except (ExecutionError, OSError, subprocess.SubprocessError) as cleanup_error:
            if isinstance(cleanup_error, DriverFailure):
                artifacts.extend(cleanup_error.artifacts)
            failure = ExecutionError(f"{failure}; cleanup failed: {cleanup_error}")
    if failure is not None:
        raise DriverFailure(str(failure), tuple(artifacts))
    return (
        events,
        {
            "executed": True,
            "exit_code": 0,
            "timed_out": False,
            "label": label,
            "phase": phase,
            "iteration": iteration,
            "operation_count": len(completed_operations),
        },
        artifacts,
    )


def _label_requires_concurrency(
    experiment: dict[str, Any], scenario: dict[str, Any], label: str
) -> bool:
    topology = scenario.get("topology", {})
    if not isinstance(topology, dict):
        raise ExecutionError("scenario topology must be an object")
    configured = topology.get("concurrency_required_labels")
    required = experiment["requirements"].get("concurrency_required_labels", configured)
    if required is None:
        return bool(experiment["requirements"].get("shared_device_identity", False))
    if (
        not isinstance(required, list)
        or any(value not in {"baseline", "variant"} for value in required)
        or len(set(required)) != len(required)
    ):
        raise ExecutionError("concurrency_required_labels must contain unique baseline/variant")
    if configured != required:
        raise ExecutionError(
            "scenario topology concurrency_required_labels does not match requirements"
        )
    return label in required


def _append_e5_interference_evidence(traces: dict[str, dict[str, Any]]) -> None:
    baseline_events = traces["baseline"]["events"]
    variant_events = traces["variant"]["events"]
    baseline_by_iteration: dict[int, list[float]] = {}
    variant_by_iteration: dict[int, list[float]] = {}

    def worker_rates(events: list[dict[str, Any]]) -> dict[int, list[float]]:
        rates: dict[int, list[float]] = {}
        for event in events:
            if (
                event.get("phase") != "measurement"
                or event.get("event_type") != "workload_completed"
            ):
                continue
            elapsed_ms = float(event.get("elapsed_ms", 0.0))
            item_count = int(event.get("item_count", 0))
            if elapsed_ms <= 0 or item_count <= 0:
                raise ExecutionError("E5 workload completion must contain positive work and time")
            rates.setdefault(int(event.get("iteration", 0)), []).append(
                item_count * 1000.0 / elapsed_ms
            )
        return rates

    baseline_by_iteration = worker_rates(baseline_events)
    variant_by_iteration = worker_rates(variant_events)
    if set(baseline_by_iteration) != set(variant_by_iteration):
        raise ExecutionError("E5 baseline and variant iterations do not match")

    def maximum_overlap(events: list[dict[str, Any]], iteration: int) -> float:
        starts: dict[str, datetime] = {}
        completions: dict[str, datetime] = {}
        for event in events:
            if event.get("phase") != "measurement" or int(event.get("iteration", 0)) != iteration:
                continue
            sandbox_id = str(event.get("sandbox_id", ""))
            occurred_at = str(event.get("occurred_at", ""))
            if not sandbox_id or not occurred_at:
                continue
            try:
                observed = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
            except ValueError as error:
                raise ExecutionError("E5 trace contains an invalid occurred_at") from error
            if observed.tzinfo is None:
                raise ExecutionError("E5 trace occurred_at must include a timezone")
            if event.get("event_type") == "sample_consumed":
                duration_ms = float(event.get("duration_ms", 0.0))
                started = datetime.fromtimestamp(
                    observed.timestamp() - duration_ms / 1000.0,
                    tz=observed.tzinfo,
                )
                starts[sandbox_id] = min(starts.get(sandbox_id, started), started)
            elif event.get("event_type") == "workload_completed":
                completions[sandbox_id] = max(completions.get(sandbox_id, observed), observed)
        intervals = [
            (starts[sandbox_id], completions[sandbox_id])
            for sandbox_id in sorted(set(starts) & set(completions))
        ]
        if len(intervals) != 2:
            raise ExecutionError("E5 requires two complete worker execution intervals")
        return max(
            0.0,
            (
                min(intervals[0][1], intervals[1][1]) - max(intervals[0][0], intervals[1][0])
            ).total_seconds()
            * 1000.0,
        )

    for iteration in sorted(baseline_by_iteration):
        baseline_rates = baseline_by_iteration[iteration]
        variant_rates = variant_by_iteration[iteration]
        if len(baseline_rates) != 2 or len(variant_rates) != 2:
            raise ExecutionError("E5 requires exactly two worker throughput observations per side")
        if maximum_overlap(baseline_events, iteration) > 0:
            raise ExecutionError("E5 baseline worker execution intervals overlap")
        if maximum_overlap(variant_events, iteration) <= 0:
            raise ExecutionError("E5 variant worker execution intervals do not overlap")
        baseline_per_worker = sum(baseline_rates) / len(baseline_rates)
        variant_per_worker = sum(variant_rates) / len(variant_rates)
        interference = max(0.0, 1.0 - variant_per_worker / baseline_per_worker)
        common = next(
            event
            for event in variant_events
            if event.get("phase") == "measurement" and int(event.get("iteration", 0)) == iteration
        )
        variant_events.append(
            {
                key: common[key]
                for key in (
                    "experiment_id",
                    "label",
                    "phase",
                    "iteration",
                    "service_job_id",
                    "service_run_id",
                    "device",
                    "node_id",
                )
                if key in common
            }
            | {
                "event_type": "interference_observed",
                "source": "orchestrator",
                "derivation": "1-variant_mean_worker_throughput/baseline_mean_worker_throughput",
                "interference_ratio": interference,
                "baseline_mean_worker_throughput_items_per_s": baseline_per_worker,
                "variant_mean_worker_throughput_items_per_s": variant_per_worker,
                "worker_count": 2,
            }
        )
    traces["variant"]["metrics"] = GATE_TOOLS._metrics_from_events(variant_events)


def execute(args: argparse.Namespace) -> int:
    campaign_path = Path(args.campaign).expanduser().resolve()
    gate_manifest_path = Path(args.gate_manifest).expanduser().resolve()
    scenario_path = Path(args.scenario).expanduser().resolve()
    output_root = Path(args.output_dir).expanduser().resolve()
    campaign = GATE_TOOLS.load_campaign(campaign_path)
    experiment = next(
        (item for item in campaign["experiments"] if item["experiment_id"] == args.experiment),
        None,
    )
    if experiment is None:
        raise ExecutionError(f"unknown campaign experiment {args.experiment}")
    if gate_manifest_path != (ROOT / experiment["gate_manifest"]).resolve():
        raise ExecutionError("gate manifest path does not match the campaign")
    if scenario_path != (ROOT / experiment["scenario_manifest"]).resolve():
        raise ExecutionError("scenario path does not match the campaign")
    if args.evidence != experiment["minimum_evidence"]:
        raise ExecutionError("evidence does not match the campaign experiment")
    gate_manifest = GATE_TOOLS.load_manifest(gate_manifest_path)
    scenario = _read_json(scenario_path)
    plan = _scenario_plan(scenario, experiment)
    driver = _resolve_file(args.driver, label="hardware environment driver", executable=True)
    if output_root in {Path("/").resolve(), Path.home().resolve(), ROOT.resolve()}:
        raise ExecutionError("hardware campaign output directory is too broad")
    output_root.mkdir(parents=True, exist_ok=True)

    preflight_request = {
        "schema_version": DRIVER_REQUEST_SCHEMA,
        "request_id": hashlib.sha256(
            f"{campaign['campaign_id']}\0{args.experiment}\0preflight".encode()
        ).hexdigest(),
        "campaign_id": campaign["campaign_id"],
        "experiment_id": args.experiment,
        "evidence": args.evidence,
        "operation": "preflight",
        "scenario": scenario,
        "workload_lock": gate_manifest.data["workload_lock"],
    }
    preflight, artifacts = _run_driver(
        driver,
        preflight_request,
        output_root / "artifacts" / "services" / "hardware-driver" / "preflight",
        args.timeout_seconds,
    )
    _validate_driver_response(
        preflight, request_id=str(preflight_request["request_id"]), operation="preflight"
    )
    fingerprint = preflight.get("environment_fingerprint")
    if not isinstance(fingerprint, dict):
        raise ExecutionError("hardware driver preflight omitted environment_fingerprint")
    required_fingerprint = set(gate_manifest.data["environment_fingerprint"]["required_fields"])
    missing_fingerprint = [
        field
        for field in sorted(required_fingerprint)
        if not _fingerprint_value_present(fingerprint.get(field))
    ]
    if missing_fingerprint:
        raise ExecutionError(
            "hardware driver preflight omitted fingerprint fields: "
            + ", ".join(missing_fingerprint)
        )
    if fingerprint.get("git_commit") != GATE_TOOLS.git_commit():
        raise ExecutionError("hardware driver checkout does not match the campaign checkout")
    if fingerprint.get("git_dirty") is not False:
        raise ExecutionError("hardware campaign requires a clean target checkout")
    if fingerprint.get("execution_mode") not in experiment["requirements"]["execution_modes"]:
        raise ExecutionError("hardware driver execution mode does not satisfy the scenario")
    if fingerprint.get("gpu_profile") not in experiment["requirements"]["gpu_profiles"]:
        raise ExecutionError("hardware driver GPU profile does not satisfy the scenario")
    accelerator_count = fingerprint.get("accelerator_count")
    if (
        not isinstance(accelerator_count, int)
        or isinstance(accelerator_count, bool)
        or accelerator_count < int(experiment["requirements"]["minimum_accelerators"])
    ):
        raise ExecutionError("hardware driver accelerator inventory is insufficient")
    if not str(fingerprint.get("accelerator_inventory_digest", "")).strip():
        raise ExecutionError("hardware driver preflight omitted accelerator inventory digest")

    traces: dict[str, dict[str, Any]] = {}
    executions: list[dict[str, Any]] = []
    run_ids: set[str] = set()
    try:
        for label in ("baseline", "variant"):
            events: list[dict[str, Any]] = []
            for phase, count in (
                ("warmup", int(gate_manifest.data["comparisons"]["warmup_runs"])),
                ("measurement", int(gate_manifest.data["comparisons"]["measurement_runs"])),
            ):
                for iteration in range(1, count + 1):
                    run_events, execution, produced = _run_iteration(
                        driver=driver,
                        campaign=campaign,
                        experiment=experiment,
                        scenario=scenario,
                        gate_manifest=gate_manifest,
                        output_root=output_root,
                        label=label,
                        phase=phase,
                        iteration=iteration,
                        plan=plan[label],
                        timeout=args.timeout_seconds,
                        gpu_profile=str(fingerprint["gpu_profile"]),
                        execution_mode=str(fingerprint["execution_mode"]),
                        minimum_workers=max(
                            int(experiment["requirements"].get("minimum_workers", 1)),
                            int(experiment["requirements"]["minimum_nodes"]),
                        ),
                        shared_device_identity=bool(
                            experiment["requirements"].get("shared_device_identity", False)
                        ),
                        concurrency_required=_label_requires_concurrency(
                            experiment, scenario, label
                        ),
                        expected_total_core_percent=int(
                            experiment["requirements"].get("expected_total_core_percent", 0)
                        ),
                    )
                    events.extend(run_events)
                    service_runs = {str(event.get("service_run_id", "")) for event in run_events}
                    if len(service_runs) != 1 or "" in service_runs:
                        raise DriverFailure(
                            "hardware iteration must identify exactly one service run",
                            tuple(produced),
                        )
                    service_run_id = next(iter(service_runs))
                    if service_run_id in run_ids:
                        raise DriverFailure(
                            "hardware iterations must use distinct service run identities",
                            tuple(produced),
                        )
                    run_ids.add(service_run_id)
                    executions.append(execution)
                    artifacts.extend(produced)
            traces[label] = {
                "schema_version": "tgsrl.io/gate-trace/v1alpha1",
                "suite_id": gate_manifest.data["suite_id"],
                "label": label,
                "seed": gate_manifest.data["workload_lock"]["seed"],
                "events": events,
                "metrics": GATE_TOOLS._metrics_from_events(events),
            }
        if args.experiment == "E5-STATIC":
            _append_e5_interference_evidence(traces)
    except DriverFailure as error:
        artifacts.extend(error.artifacts)
        raise

    paths = GATE_TOOLS.artifact_paths(gate_manifest, output_root)
    _write_json(paths.baseline_trace, traces["baseline"])
    _write_json(paths.variant_trace, traces["variant"])
    service_logs = [
        {
            "path": cast(Path, record["path"]).relative_to(output_root).as_posix(),
            "sha256": str(record["sha256"]),
        }
        for record in artifacts
    ]
    unique_service_logs = {record["path"]: record for record in service_logs}
    report = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": gate_manifest.data["suite_id"],
        "experiment_id": args.experiment,
        "evidence": args.evidence,
        "status": "PASSED",
        "simulated": False,
        "workload_lock": gate_manifest.data["workload_lock"],
        "environment_fingerprint": fingerprint,
        "warmup_runs": gate_manifest.data["comparisons"]["warmup_runs"],
        "measurement_runs": gate_manifest.data["comparisons"]["measurement_runs"],
        "baseline_trace": paths.baseline_trace.relative_to(output_root).as_posix(),
        "variant_trace": paths.variant_trace.relative_to(output_root).as_posix(),
        "metrics": {label: trace["metrics"] for label, trace in traces.items()},
        "executions": executions,
        "trace_capture": {
            "baseline_digest": _sha256(paths.baseline_trace),
            "variant_digest": _sha256(paths.variant_trace),
        },
        "service_log_artifacts": [
            unique_service_logs[path] for path in sorted(unique_service_logs)
        ],
        "orchestrator": {
            "schema_version": "tgsrl.io/hardware-orchestrator/v1alpha1",
            "executor_sha256": _sha256(Path(__file__).resolve()),
            "gate_tools_sha256": _sha256(GATE_TOOLS_PATH),
            "driver_sha256": _sha256(driver),
            "campaign_sha256": _sha256(campaign_path),
            "gate_manifest_sha256": _sha256(gate_manifest_path),
            "scenario_sha256": _sha256(scenario_path),
        },
    }
    evaluation = GATE_TOOLS.evaluate_gate(gate_manifest, report)
    report["status"] = evaluation["status"]
    _write_json(paths.report, report)
    return 0 if report["status"] == "PASSED" else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--gate-manifest", required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--evidence", choices=sorted(GATE_TOOLS.REAL_GPU_EVIDENCE), required=True)
    parser.add_argument("--driver", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        print("error: timeout must be positive and finite", file=sys.stderr)
        return 1
    try:
        return execute(args)
    except (ExecutionError, ValueError, OSError, subprocess.SubprocessError) as error:
        safe_error = _redact_sensitive_environment_values(str(error).encode()).decode(
            "utf-8", errors="replace"
        )
        print(f"error: {safe_error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
