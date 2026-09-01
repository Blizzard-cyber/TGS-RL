#!/usr/bin/env python3
"""Generate, ingest, and evaluate Gate G/I evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import shlex
import shutil
import socket
import statistics
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "configs" / "gates" / "gate-gi.json"
DEFAULT_OUTPUT_DIR = ROOT / ".cache" / "tgsrl" / "gate-gi"
DEFAULT_CAMPAIGN = ROOT / "configs" / "gates" / "e1-e8.json"
DEFAULT_CAMPAIGN_REPORTS = ROOT / ".cache" / "tgsrl" / "e1-e8"
DEFAULT_CAMPAIGN_EXECUTOR = ROOT / "scripts" / "hardware-campaign-executor.py"
MAX_CAMPAIGN_DIAGNOSTIC_FILES = 10_000
MAX_CAMPAIGN_DIAGNOSTIC_BYTES = 512 << 20
SENSITIVE_ENV_SUFFIXES = ("_API_KEY", "_AUTHORIZATION", "_PASSWORD", "_SECRET", "_TOKEN")

STATUS_VALUES = {"NOT_RUN", "BLOCKED", "INVALID", "PASSED", "FAILED"}
EVIDENCE_VALUES = {
    "SIMULATED",
    "CPU_INTEGRATION",
    "GPU_SINGLE_NODE",
    "GPU_MULTI_NODE",
}
REAL_GPU_EVIDENCE = {"GPU_SINGLE_NODE", "GPU_MULTI_NODE"}
REAL_EVIDENCE = {"CPU_INTEGRATION", *REAL_GPU_EVIDENCE}
EVIDENCE_RANK = {
    "SIMULATED": 0,
    "CPU_INTEGRATION": 1,
    "GPU_SINGLE_NODE": 2,
    "GPU_MULTI_NODE": 3,
}
CAMPAIGN_EVIDENCE_REQUIREMENTS = {
    "scheduler-binding",
    "dra-allocation",
    "worker-device-identity",
    "mig-device-class",
    "parent-uuid",
    "throughput",
    "gpu-active-time",
    "useful-gpu-time",
    "policy-lag",
    "sample-staleness",
    "effective-sample-size",
    "interference-ratio",
    "share-readback",
    "priority-readback",
    "action-start",
    "action-receipt",
    "readiness",
    "fault-injected",
    "durable-receipt",
    "rollback",
    "fault-recovered",
    "node-identities",
    "recovery",
    "convergence-quality",
}


class GateToolError(ValueError):
    """Raised when gate tooling input is invalid."""


@dataclass(frozen=True, slots=True)
class LoadedManifest:
    path: Path
    data: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ArtifactPaths:
    root: Path
    archive: Path
    baseline_trace: Path
    variant_trace: Path
    report: Path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GateToolError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GateToolError(f"{path} must contain a JSON object")
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _relative_path(value: object, field: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise GateToolError(f"{field} must be a non-empty relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise GateToolError(f"{field} must stay inside the output directory")
    return path


def load_manifest(path: Path) -> LoadedManifest:
    data = _read_json(path)
    if data.get("schema_version") != "tgsrl.io/gate-suite/v1alpha1":
        raise GateToolError("manifest schema_version must be tgsrl.io/gate-suite/v1alpha1")
    if not isinstance(data.get("rules"), list) or not data["rules"]:
        raise GateToolError("manifest rules must be a non-empty list")
    rule_ids: set[str] = set()
    for index, rule in enumerate(data["rules"]):
        if not isinstance(rule, dict):
            raise GateToolError(f"manifest rule {index} must be an object")
        rule_id = rule.get("rule_id")
        if not isinstance(rule_id, str) or not rule_id or rule_id in rule_ids:
            raise GateToolError(f"manifest rule {index} must have a unique non-empty rule_id")
        rule_ids.add(rule_id)
        if rule.get("gate") not in {"G", "I"}:
            raise GateToolError(f"manifest rule {index} must target gate G or I")
        if rule.get("operator") != ">=":
            raise GateToolError(f"manifest rule {index} must use the supported >= operator")
        if rule.get("comparison") not in {None, "variant_over_baseline_ratio"}:
            raise GateToolError(f"manifest rule {index} uses an unsupported comparison")
        if not _is_finite_number(rule.get("threshold")):
            raise GateToolError(f"manifest rule {index} threshold must be a finite number")
    if sorted(data.get("status_values", [])) != sorted(STATUS_VALUES):
        raise GateToolError("manifest status_values must exactly match supported statuses")
    if sorted(data.get("evidence_values", [])) != sorted(EVIDENCE_VALUES):
        raise GateToolError("manifest evidence_values must exactly match supported evidence types")
    comparisons = data.get("comparisons")
    if not isinstance(comparisons, dict):
        raise GateToolError("manifest comparisons must be an object")
    warmup_runs = comparisons.get("warmup_runs")
    measurement_runs = comparisons.get("measurement_runs")
    if not isinstance(warmup_runs, int) or warmup_runs < 0:
        raise GateToolError("comparisons.warmup_runs must be a non-negative integer")
    if not isinstance(measurement_runs, int) or measurement_runs <= 0:
        raise GateToolError("comparisons.measurement_runs must be a positive integer")
    metrics_schema = data.get("metrics_schema")
    if not isinstance(metrics_schema, dict) or not isinstance(
        metrics_schema.get("required_metrics"), list
    ):
        raise GateToolError("manifest metrics_schema.required_metrics must be a list")
    required_metrics = metrics_schema["required_metrics"]
    if (
        not required_metrics
        or not all(isinstance(metric, str) and metric for metric in required_metrics)
        or len(set(required_metrics)) != len(required_metrics)
    ):
        raise GateToolError("manifest required metrics must be unique non-empty strings")
    for index, rule in enumerate(data["rules"]):
        if rule.get("metric") not in required_metrics:
            raise GateToolError(f"manifest rule {index} references an undeclared metric")
    runner = data.get("workload_runner")
    if not isinstance(runner, dict):
        raise GateToolError("manifest workload_runner must be an object")
    if (
        not isinstance(runner.get("argv"), list)
        or not runner["argv"]
        or not all(isinstance(token, str) and token for token in runner["argv"])
    ):
        raise GateToolError("manifest workload_runner.argv must be a structured argv list")
    if not isinstance(runner.get("items_per_run"), int) or runner["items_per_run"] <= 0:
        raise GateToolError("manifest workload_runner.items_per_run must be positive")
    if (
        not isinstance(runner.get("timeout_seconds"), (int, float))
        or runner["timeout_seconds"] <= 0
    ):
        raise GateToolError("manifest workload_runner.timeout_seconds must be positive")
    if runner.get("evidence_mode", "workload") not in {"workload", "full_stack"}:
        raise GateToolError("manifest workload_runner.evidence_mode is invalid")
    variants = runner.get("variants")
    if not isinstance(variants, dict) or set(variants) != {"baseline", "variant"}:
        raise GateToolError("manifest workload_runner.variants must define baseline and variant")
    for label, variant in variants.items():
        if not isinstance(variant, dict) or variant.get("control_mode") not in {"static", "tgsrl"}:
            raise GateToolError(f"manifest workload_runner.variants.{label} is invalid")
    required_variant_actions = runner.get("required_variant_actions", ["pause", "resume"])
    if not isinstance(required_variant_actions, list) or any(
        not isinstance(action, str) or not action for action in required_variant_actions
    ):
        raise GateToolError("manifest workload_runner.required_variant_actions is invalid")
    artifacts = data.get("artifacts")
    if not isinstance(artifacts, dict):
        raise GateToolError("manifest artifacts must be an object")
    for name in ("archive", "baseline_trace", "variant_trace", "report"):
        _relative_path(artifacts.get(name), f"artifacts.{name}")
    return LoadedManifest(path=path.resolve(), data=data)


def artifact_paths(manifest: LoadedManifest, output_root: Path) -> ArtifactPaths:
    root = output_root.expanduser().resolve()
    artifacts = manifest.data["artifacts"]
    return ArtifactPaths(
        root=root,
        archive=root / _relative_path(artifacts["archive"], "artifacts.archive"),
        baseline_trace=root
        / _relative_path(artifacts["baseline_trace"], "artifacts.baseline_trace"),
        variant_trace=root / _relative_path(artifacts["variant_trace"], "artifacts.variant_trace"),
        report=root / _relative_path(artifacts["report"], "artifacts.report"),
    )


def _artifact_name(paths: ArtifactPaths, path: Path) -> str:
    return path.relative_to(paths.root).as_posix()


def git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def git_dirty() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode != 0 or bool(result.stdout.strip())


def build_environment_fingerprint(*, execution_mode: str) -> dict[str, Any]:
    host_hash = hashlib.sha256(socket.gethostname().encode()).hexdigest()[:16]
    return {
        "captured_at": datetime.now(tz=UTC).isoformat(),
        "host_hash": host_hash,
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "git_commit": git_commit(),
        "git_dirty": git_dirty(),
        "execution_mode": execution_mode,
    }


def build_gpu_environment_fingerprint() -> dict[str, Any]:
    fingerprint = build_environment_fingerprint(execution_mode="gpu-runner")
    completed = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=uuid,name,driver_version",
            "--format=csv,noheader,nounits",
        ],
        cwd=ROOT,
        capture_output=True,
        check=False,
        timeout=30,
    )
    rows = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if completed.returncode != 0 or not rows:
        raise GateToolError("GPU runner cannot discover NVIDIA devices")
    fingerprint["accelerator_count"] = len(rows)
    fingerprint["accelerator_inventory_digest"] = hashlib.sha256(
        b"\n".join(sorted(rows))
    ).hexdigest()
    return fingerprint


def _simulated_trace(
    label: str, *, suite_id: str, seed: int, measurement_runs: int
) -> dict[str, Any]:
    latency = 100 if label == "baseline" else 97
    throughput = 200.0 if label == "baseline" else 194.0
    return {
        "schema_version": "tgsrl.io/gate-trace/v1alpha1",
        "suite_id": suite_id,
        "label": label,
        "seed": seed,
        "events": [
            {
                "event_id": f"{label}-warmup",
                "phase": "warmup",
                "iteration": 1,
                "duration_ms": latency - 10,
            },
            {
                "event_id": f"{label}-measurement",
                "phase": "measurement",
                "iteration": measurement_runs,
                "duration_ms": latency,
            },
        ],
        "metrics": {
            "latency_ms_p50": float(latency),
            "latency_ms_p95": float(latency + 10),
            "latency_ms_p99": float(latency + 15),
            "throughput_items_per_s": throughput,
            "decision_count": float(measurement_runs),
            "end_to_end_iteration_ms_p50": float(latency),
            "scheduling_latency_ms_p95": 1.0,
            "gpu_active_time_ms": 0.0,
            "valuable_useful_gpu_ratio": 0.0,
            "interference_ratio": 0.0,
            "convergence_quality": 0.0,
            "queue_depth_max": 1.0,
            "policy_lag_p95": 1.0,
            "sample_staleness_ratio": 0.0,
            "effective_sample_size_mean": 1.0,
            "pause_latency_ms": 1.0,
            "checkpoint_latency_ms": 1.0,
            "reload_latency_ms": 1.0,
            "action_success_rate": 1.0,
            "rollback_rate": 0.0,
            "transaction_recovery_time_ms": 0.0,
        },
    }


def _archive_run(paths: ArtifactPaths) -> None:
    paths.archive.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(paths.archive, "w:gz") as handle:
        for artifact in sorted(paths.root.rglob("*")):
            if artifact.is_file() and artifact != paths.archive:
                handle.add(artifact, arcname=artifact.relative_to(paths.root))


def _reset_managed_artifacts(paths: ArtifactPaths) -> None:
    """Remove only files and directories owned by one Gate output layout."""
    protected = {Path("/").resolve(), Path.home().resolve(), ROOT.resolve()}
    if paths.root in protected:
        raise GateToolError("output directory is too broad for managed artifact cleanup")
    for path in (paths.report, paths.baseline_trace, paths.variant_trace, paths.archive):
        path.unlink(missing_ok=True)
    for directory in (
        paths.root / "artifacts" / "logs",
        paths.root / "artifacts" / "config",
        paths.root / "artifacts" / "services",
    ):
        if directory.is_dir():
            shutil.rmtree(directory)


def _base_run(
    manifest: LoadedManifest,
    paths: ArtifactPaths,
    *,
    evidence: str,
    simulated: bool,
    execution_mode: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    seed = int(manifest.data["workload_lock"]["seed"])
    suite_id = str(manifest.data["suite_id"])
    measurement_runs = int(manifest.data["comparisons"]["measurement_runs"])
    baseline = _simulated_trace(
        "baseline", suite_id=suite_id, seed=seed, measurement_runs=measurement_runs
    )
    variant = _simulated_trace(
        "variant", suite_id=suite_id, seed=seed, measurement_runs=measurement_runs
    )
    _write_json(paths.baseline_trace, baseline)
    _write_json(paths.variant_trace, variant)
    run = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": suite_id,
        "evidence": evidence,
        "status": "NOT_RUN",
        "simulated": simulated,
        "workload_lock": manifest.data["workload_lock"],
        "environment_fingerprint": build_environment_fingerprint(execution_mode=execution_mode),
        "warmup_runs": int(manifest.data["comparisons"]["warmup_runs"]),
        "measurement_runs": measurement_runs,
        "baseline_trace": _artifact_name(paths, paths.baseline_trace),
        "variant_trace": _artifact_name(paths, paths.variant_trace),
        "metrics": {"baseline": baseline["metrics"], "variant": variant["metrics"]},
        "trace_capture": {
            "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
            "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
        },
    }
    return run, baseline, variant


def _print_artifacts(paths: ArtifactPaths) -> None:
    print(json.dumps({"report": str(paths.report), "archive": str(paths.archive)}))


def cmd_simulate(args: argparse.Namespace) -> int:
    manifest = load_manifest(Path(args.manifest))
    paths = artifact_paths(manifest, Path(args.output_dir))
    _reset_managed_artifacts(paths)
    run, _baseline, _variant = _base_run(
        manifest, paths, evidence="SIMULATED", simulated=True, execution_mode="simulator"
    )
    run["notes"] = ["Simulator output is synthetic and must not claim a real GPU gate pass."]
    _write_json(paths.report, run)
    _archive_run(paths)
    if not getattr(args, "quiet", False):
        _print_artifacts(paths)
    return 0


def _run_command(
    argv: list[str], timeout_seconds: float, *, environment: dict[str, str] | None = None
) -> tuple[dict[str, Any], bool, bytes, bytes]:
    if not argv or any(not value for value in argv):
        raise GateToolError("workload argv must not be empty")
    try:
        completed = subprocess.run(
            argv,
            cwd=ROOT,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
            env=environment,
        )
        exit_code = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        exit_code = 124
        stdout = exc.stdout or b""
        stderr = exc.stderr or b""
        timed_out = True
    record = {
        "executable": Path(argv[0]).name,
        "argv_digest": hashlib.sha256("\0".join(argv).encode()).hexdigest(),
        "stdout_digest": hashlib.sha256(stdout).hexdigest(),
        "stderr_digest": hashlib.sha256(stderr).hexdigest(),
        "exit_code": exit_code,
        "executed": True,
        "timed_out": timed_out,
    }
    return record, exit_code == 0, stdout, stderr


def _run_smoke(command: str, timeout_seconds: float) -> tuple[dict[str, Any], bool]:
    record, succeeded, _stdout, _stderr = _run_command(shlex.split(command), timeout_seconds)
    return record, succeeded


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, math.ceil(quantile * len(ordered)) - 1)
    return float(ordered[rank])


def _is_finite_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _parse_workload_events(
    payload: bytes, *, label: str, phase: str, iteration: int
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line_number, raw in enumerate(payload.decode("utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as error:
            raise GateToolError(
                f"workload emitted invalid JSON on line {line_number}: {error}"
            ) from error
        if (
            not isinstance(event, dict)
            or event.get("label") != label
            or event.get("phase") != phase
            or event.get("iteration") != iteration
        ):
            raise GateToolError(f"workload event identity mismatch on line {line_number}")
        events.append(event)
    if not events:
        raise GateToolError("workload emitted no events")
    return events


def _metrics_from_events(events: list[dict[str, Any]]) -> dict[str, float]:
    measured = [event for event in events if event.get("phase") == "measurement"]
    consumed = [event for event in measured if event.get("event_type") == "sample_consumed"]
    actions = [event for event in measured if event.get("event_type") == "decision_applied"]
    completed = [event for event in measured if event.get("event_type") == "workload_completed"]
    recoveries = [event for event in measured if event.get("event_type") == "fault_recovered"]
    if not consumed or not completed:
        raise GateToolError("measurement trace requires consumed samples and completed iterations")
    latencies = [float(event.get("duration_ms", 0.0)) for event in consumed]
    elapsed_ms = sum(float(event.get("elapsed_ms", 0.0)) for event in completed)
    item_count = sum(int(event.get("item_count", 0)) for event in completed)
    observations = [event.get("contract_observation", {}) for event in consumed]
    action_count = len(actions)
    scheduler_actions = [event for event in actions if event.get("source") == "scheduler"]
    decision_count = len(scheduler_actions) if scheduler_actions else len(actions)
    gpu_active_time_ms = sum(float(event.get("gpu_active_ms", 0.0)) for event in consumed)
    useful_gpu_time_ms = sum(float(event.get("useful_gpu_time_ms", 0.0)) for event in consumed)
    interference = [
        float(event.get("interference_ratio", 0.0))
        for event in measured
        if event.get("event_type") == "interference_observed"
    ]
    convergence = [
        float(event.get("convergence_quality", 0.0))
        for event in completed
        if "convergence_quality" in event
    ]
    metrics = {
        "latency_ms_p50": _percentile(latencies, 0.50),
        "latency_ms_p95": _percentile(latencies, 0.95),
        "latency_ms_p99": _percentile(latencies, 0.99),
        "throughput_items_per_s": 0.0 if elapsed_ms <= 0 else item_count * 1000.0 / elapsed_ms,
        "decision_count": float(decision_count),
        "end_to_end_iteration_ms_p50": _percentile(
            [float(event.get("elapsed_ms", 0.0)) for event in completed], 0.50
        ),
        "scheduling_latency_ms_p95": _percentile(
            [float(event.get("duration_ms", 0.0)) for event in (scheduler_actions or actions)],
            0.95,
        ),
        "gpu_active_time_ms": gpu_active_time_ms,
        "valuable_useful_gpu_ratio": (
            0.0 if gpu_active_time_ms <= 0 else useful_gpu_time_ms / gpu_active_time_ms
        ),
        "interference_ratio": statistics.fmean(interference) if interference else 0.0,
        "convergence_quality": statistics.fmean(convergence) if convergence else 0.0,
        "queue_depth_max": float(max(int(event.get("buffer_level", 0)) for event in measured)),
        "policy_lag_p95": _percentile(
            [float(observation.get("policy_lag", 0.0)) for observation in observations], 0.95
        ),
        "sample_staleness_ratio": sum(
            bool(observation.get("sample_stale")) for observation in observations
        )
        / len(observations),
        "effective_sample_size_mean": statistics.fmean(
            float(observation.get("effective_sample_size", 0.0)) for observation in observations
        ),
        "pause_latency_ms": _percentile(
            [
                float(event.get("duration_ms", 0.0))
                for event in actions
                if event.get("action") in {"prepare_pause", "pause"}
            ],
            0.50,
        ),
        "checkpoint_latency_ms": _percentile(
            [
                float(event.get("duration_ms", 0.0))
                for event in actions
                if event.get("action") == "checkpoint"
            ],
            0.50,
        ),
        "reload_latency_ms": _percentile(
            [
                float(event.get("duration_ms", 0.0))
                for event in actions
                if event.get("action") == "reload"
            ],
            0.50,
        ),
        "action_success_rate": 0.0
        if action_count == 0
        else sum(bool(event.get("succeeded")) for event in actions) / action_count,
        "rollback_rate": 0.0
        if action_count == 0
        else sum(bool(event.get("rolled_back")) for event in actions) / action_count,
        "transaction_recovery_time_ms": sum(
            float(event.get("recovery_time_ms", 0.0)) for event in actions
        )
        + sum(float(event.get("recovery_time_ms", 0.0)) for event in recoveries),
    }
    if any(not _is_finite_number(value) for value in metrics.values()):
        raise GateToolError("measurement trace produced a non-finite metric")
    return metrics


def _validate_full_stack_events(
    events: list[dict[str, Any]], *, label: str, required_variant_actions: list[str]
) -> None:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for event in events:
        key = (str(event.get("phase", "")), int(event.get("iteration", 0)))
        grouped.setdefault(key, []).append(event)
    for (phase, iteration), run_events in grouped.items():
        run_ids = {str(event.get("service_run_id", "")) for event in run_events}
        job_ids = {str(event.get("service_job_id", "")) for event in run_events}
        scope = f"{label} {phase} iteration {iteration}"
        if len(run_ids) != 1 or "" in run_ids or len(job_ids) != 1 or "" in job_ids:
            raise GateToolError(f"{scope} must identify exactly one service run/job")
        scheduler_events = [
            event
            for event in run_events
            if event.get("event_type") == "decision_applied" and event.get("source") == "scheduler"
        ]
        if not scheduler_events or any(
            not event.get("decision_id") or not event.get("plan_id") for event in scheduler_events
        ):
            raise GateToolError(f"{scope} requires a Scheduler decision and selected plan")
        worker_events = [event for event in run_events if event.get("source") == "worker"]
        if not worker_events or any(
            not event.get("runtime_unit_id") or not event.get("worker_id")
            for event in worker_events
        ):
            raise GateToolError(f"{scope} requires managed-worker runtime identities")
        if label == "variant" and required_variant_actions:
            operator_actions = {
                str(event.get("action"))
                for event in run_events
                if event.get("event_type") == "decision_applied"
                and event.get("source") == "operator"
                and event.get("succeeded") is True
            }
            missing = set(required_variant_actions) - operator_actions
            if missing:
                raise GateToolError(
                    f"{scope} requires successful Operator actions: " + ", ".join(sorted(missing))
                )


def _workload_argv(
    manifest: LoadedManifest, *, label: str, phase: str, iteration: int, device: str
) -> list[str]:
    runner = manifest.data["workload_runner"]
    control_mode = runner["variants"][label]["control_mode"]
    argv = [sys.executable if token == "{python}" else token for token in runner["argv"]]
    return [
        *argv,
        "--label",
        label,
        "--phase",
        phase,
        "--iteration",
        str(iteration),
        "--seed",
        str(manifest.data["workload_lock"]["seed"]),
        "--items",
        str(runner["items_per_run"]),
        "--device",
        device,
        "--control-mode",
        control_mode,
    ]


def _execute_workloads(
    manifest: LoadedManifest, paths: ArtifactPaths, *, label: str, device: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    comparisons = manifest.data["comparisons"]
    timeout = float(manifest.data["workload_runner"]["timeout_seconds"])
    events: list[dict[str, Any]] = []
    executions: list[dict[str, Any]] = []
    for phase, repetitions in (
        ("warmup", int(comparisons["warmup_runs"])),
        ("measurement", int(comparisons["measurement_runs"])),
    ):
        for iteration in range(1, repetitions + 1):
            argv = _workload_argv(
                manifest, label=label, phase=phase, iteration=iteration, device=device
            )
            record, succeeded, stdout, stderr = _run_command(argv, timeout)
            record.update({"label": label, "phase": phase, "iteration": iteration})
            log_root = paths.root / "artifacts" / "logs"
            log_root.mkdir(parents=True, exist_ok=True)
            stdout_path = log_root / f"{label}-{phase}-{iteration}.stdout.ndjson"
            stderr_path = log_root / f"{label}-{phase}-{iteration}.stderr.log"
            stdout_path.write_bytes(stdout)
            stderr_path.write_bytes(stderr)
            record["stdout_artifact"] = _artifact_name(paths, stdout_path)
            record["stderr_artifact"] = _artifact_name(paths, stderr_path)
            executions.append(record)
            if not succeeded:
                raise GateToolError(
                    f"{label} {phase} workload iteration {iteration} failed with "
                    f"exit {record['exit_code']}"
                )
            events.extend(
                _parse_workload_events(stdout, label=label, phase=phase, iteration=iteration)
            )
    if manifest.data["workload_runner"].get("evidence_mode") == "full_stack":
        _validate_full_stack_events(
            events,
            label=label,
            required_variant_actions=list(
                manifest.data["workload_runner"].get(
                    "required_variant_actions", ["pause", "resume"]
                )
            ),
        )
    trace = {
        "schema_version": "tgsrl.io/gate-trace/v1alpha1",
        "suite_id": manifest.data["suite_id"],
        "label": label,
        "seed": manifest.data["workload_lock"]["seed"],
        "events": events,
        "metrics": _metrics_from_events(events),
    }
    _write_json(paths.baseline_trace if label == "baseline" else paths.variant_trace, trace)
    return trace, executions


def _capture_locked_inputs(manifest: LoadedManifest, paths: ArtifactPaths) -> None:
    target = paths.root / "artifacts" / "config"
    target.mkdir(parents=True, exist_ok=True)
    shutil.copy2(manifest.path, target / "gate-manifest.json")
    scenario = (
        ROOT / _relative_path(manifest.data["scenario_manifest"], "scenario_manifest")
    ).resolve()
    if ROOT not in scenario.parents or not scenario.is_file():
        raise GateToolError("scenario_manifest must resolve to a repository file")
    shutil.copy2(scenario, target / scenario.name)


def _capture_full_stack_logs(
    manifest: LoadedManifest, paths: ArtifactPaths
) -> list[dict[str, str]]:
    if manifest.data["workload_runner"].get("evidence_mode") != "full_stack":
        return []
    captured: list[dict[str, str]] = []
    target = paths.root / "artifacts" / "services"
    for variable in ("TGSRL_GATE_SERVICE_LOG_DIR", "TGSRL_GATE_PROCESS_LOG_ROOT"):
        raw = os.environ.get(variable, "").strip()
        if not raw:
            raise GateToolError(f"full-stack evidence requires {variable}")
        source = Path(raw).resolve()
        if not source.is_dir():
            raise GateToolError(f"full-stack evidence directory is missing: {variable}")
        prefix = "control-plane" if variable == "TGSRL_GATE_SERVICE_LOG_DIR" else "workloads"
        for log in sorted(source.rglob("*.log")):
            destination = target / prefix / log.relative_to(source)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(log, destination)
            captured.append(
                {
                    "path": _artifact_name(paths, destination),
                    "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                }
            )
    if not captured:
        raise GateToolError("full-stack evidence captured no service or workload logs")
    return captured


def cmd_cpu_smoke(args: argparse.Namespace) -> int:
    manifest = load_manifest(Path(args.manifest))
    paths = artifact_paths(manifest, Path(args.output_dir))
    _reset_managed_artifacts(paths)
    smoke, succeeded = _run_smoke(args.smoke_command, args.smoke_timeout)
    if not succeeded:
        zero_metrics = {
            metric: 0.0 for metric in manifest.data["metrics_schema"]["required_metrics"]
        }
        traces: dict[str, dict[str, Any]] = {}
        for label, path in (
            ("baseline", paths.baseline_trace),
            ("variant", paths.variant_trace),
        ):
            trace = {
                "schema_version": "tgsrl.io/gate-trace/v1alpha1",
                "suite_id": manifest.data["suite_id"],
                "label": label,
                "seed": manifest.data["workload_lock"]["seed"],
                "events": [],
                "metrics": zero_metrics,
            }
            _write_json(path, trace)
            traces[label] = trace
        run = {
            "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
            "suite_id": manifest.data["suite_id"],
            "evidence": "CPU_INTEGRATION",
            "status": "BLOCKED",
            "simulated": False,
            "workload_lock": manifest.data["workload_lock"],
            "environment_fingerprint": build_environment_fingerprint(execution_mode="cpu-runner"),
            "warmup_runs": int(manifest.data["comparisons"]["warmup_runs"]),
            "measurement_runs": int(manifest.data["comparisons"]["measurement_runs"]),
            "baseline_trace": _artifact_name(paths, paths.baseline_trace),
            "variant_trace": _artifact_name(paths, paths.variant_trace),
            "metrics": {label: trace["metrics"] for label, trace in traces.items()},
            "trace_capture": {
                "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
                "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
            },
            "smoke": smoke,
            "notes": ["CPU prerequisite failed before the workload runner was started."],
        }
        _capture_locked_inputs(manifest, paths)
        _write_json(paths.report, run)
        _archive_run(paths)
        _print_artifacts(paths)
        return 1
    baseline, baseline_executions = _execute_workloads(
        manifest, paths, label="baseline", device="cpu"
    )
    variant, variant_executions = _execute_workloads(manifest, paths, label="variant", device="cpu")
    run = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": manifest.data["suite_id"],
        "evidence": "CPU_INTEGRATION",
        "status": "NOT_RUN",
        "simulated": False,
        "workload_lock": manifest.data["workload_lock"],
        "environment_fingerprint": build_environment_fingerprint(
            execution_mode=(
                "cpu-full-stack"
                if manifest.data["workload_runner"].get("evidence_mode") == "full_stack"
                else "cpu-runner"
            )
        ),
        "warmup_runs": int(manifest.data["comparisons"]["warmup_runs"]),
        "measurement_runs": int(manifest.data["comparisons"]["measurement_runs"]),
        "baseline_trace": _artifact_name(paths, paths.baseline_trace),
        "variant_trace": _artifact_name(paths, paths.variant_trace),
        "metrics": {"baseline": baseline["metrics"], "variant": variant["metrics"]},
        "executions": [*baseline_executions, *variant_executions],
        "smoke": smoke,
        "notes": [
            (
                "CPU integration executed Gateway, Job Controller, Runtime, Scheduler, "
                "Operator process backend, bootstrap, registry, and veRL callback doubles; "
                "real veRL packages, Kubernetes, and GPU gates remain not run."
                if manifest.data["workload_runner"].get("evidence_mode") == "full_stack"
                else "CPU integration executed the locked workload; GPU gates remain not run."
            )
        ],
    }
    run["trace_capture"] = {
        "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
        "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
    }
    _capture_locked_inputs(manifest, paths)
    service_logs = _capture_full_stack_logs(manifest, paths)
    if service_logs:
        run["service_log_artifacts"] = service_logs
    _write_json(paths.report, run)
    errors = _validate_report(manifest, paths, run)
    if errors:
        raise GateToolError("; ".join(errors))
    _archive_run(paths)
    if not getattr(args, "quiet", False):
        _print_artifacts(paths)
    return 0


def cmd_hardware_run(args: argparse.Namespace) -> int:
    manifest = load_manifest(Path(args.manifest))
    paths = artifact_paths(manifest, Path(args.output_dir))
    _reset_managed_artifacts(paths)
    baseline, baseline_executions = _execute_workloads(
        manifest, paths, label="baseline", device="cuda"
    )
    variant, variant_executions = _execute_workloads(
        manifest, paths, label="variant", device="cuda"
    )
    full_stack = manifest.data["workload_runner"].get("evidence_mode") == "full_stack"
    report = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": manifest.data["suite_id"],
        "evidence": args.evidence,
        "status": "PASSED" if full_stack else "NOT_RUN",
        "simulated": False,
        "workload_lock": manifest.data["workload_lock"],
        "environment_fingerprint": build_gpu_environment_fingerprint(),
        "warmup_runs": int(manifest.data["comparisons"]["warmup_runs"]),
        "measurement_runs": int(manifest.data["comparisons"]["measurement_runs"]),
        "baseline_trace": _artifact_name(paths, paths.baseline_trace),
        "variant_trace": _artifact_name(paths, paths.variant_trace),
        "metrics": {"baseline": baseline["metrics"], "variant": variant["metrics"]},
        "executions": [*baseline_executions, *variant_executions],
        "trace_capture": {
            "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
            "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
        },
    }
    if not full_stack:
        report["notes"] = [
            "CUDA workload conformance ran without the full TGS-RL service chain; "
            "the GPU Gate remains not run."
        ]
    _capture_locked_inputs(manifest, paths)
    evaluation = evaluate_gate(manifest, report)
    report["status"] = evaluation["status"]
    _write_json(paths.report, report)
    errors = _validate_report(manifest, paths, report)
    if errors:
        raise GateToolError("; ".join(errors))
    _archive_run(paths)
    _print_artifacts(paths)
    return 0 if report["status"] in {"PASSED", "NOT_RUN"} else 1


def _resolve_external_artifact(source: Path, value: object, field: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise GateToolError(f"external report must include {field}")
    candidate = Path(value).expanduser()
    resolved = (
        candidate.resolve() if candidate.is_absolute() else (source.parent / candidate).resolve()
    )
    if not resolved.is_file():
        raise GateToolError(f"required artifact is missing: {field}")
    return resolved


def cmd_ingest(args: argparse.Namespace) -> int:
    manifest = load_manifest(Path(args.manifest))
    paths = artifact_paths(manifest, Path(args.output_dir))
    source = Path(args.report).expanduser().resolve()
    report = _read_json(source)
    evidence = report.get("evidence")
    if evidence not in REAL_EVIDENCE:
        raise GateToolError("ingest only accepts CPU_INTEGRATION or GPU evidence")
    if report.get("simulated") is not False:
        raise GateToolError("ingested real evidence must set simulated=false")
    if evidence == "CPU_INTEGRATION" and report.get("status") == "PASSED":
        raise GateToolError("CPU_INTEGRATION cannot mark a GPU gate passed")
    fingerprint = report.get("environment_fingerprint")
    if not isinstance(fingerprint, dict):
        raise GateToolError("ingested report must include environment_fingerprint")
    for field in manifest.data["environment_fingerprint"]["required_fields"]:
        if field not in fingerprint or fingerprint[field] in {None, ""}:
            raise GateToolError(f"environment_fingerprint missing required field {field}")
    baseline_source = _resolve_external_artifact(
        source, report.get("baseline_trace"), "baseline_trace"
    )
    variant_source = _resolve_external_artifact(
        source, report.get("variant_trace"), "variant_trace"
    )
    if report.get("trace_capture") is None:
        raise GateToolError("external report must include trace_capture digests")
    supplied_capture = report.get("trace_capture")
    baseline_bytes = baseline_source.read_bytes()
    variant_bytes = variant_source.read_bytes()
    if isinstance(supplied_capture, dict):
        for side, payload in (("baseline", baseline_bytes), ("variant", variant_bytes)):
            digest = hashlib.sha256(payload).hexdigest()
            if supplied_capture.get(f"{side}_digest") != digest:
                raise GateToolError(f"external {side} trace digest does not match the artifact")
    service_log_payloads: list[tuple[Path, bytes, str]] = []
    raw_service_logs = report.get("service_log_artifacts", [])
    if not isinstance(raw_service_logs, list):
        raise GateToolError("external service_log_artifacts must be a list")
    seen_log_paths: set[Path] = set()
    for index, record in enumerate(raw_service_logs):
        if not isinstance(record, dict):
            raise GateToolError(f"external service log artifact {index} must be an object")
        relative = _relative_path(record.get("path"), f"service_log_artifacts[{index}].path")
        if relative.parts[:2] != ("artifacts", "services"):
            raise GateToolError(
                f"service_log_artifacts[{index}].path must stay under artifacts/services"
            )
        if relative in seen_log_paths:
            raise GateToolError("external service log artifact paths must be unique")
        seen_log_paths.add(relative)
        artifact = _resolve_external_artifact(
            source, record.get("path"), f"service_log_artifacts[{index}].path"
        )
        payload = artifact.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if record.get("sha256") != digest:
            raise GateToolError(f"external service log artifact {index} digest does not match")
        service_log_payloads.append((relative, payload, digest))
    _reset_managed_artifacts(paths)
    paths.baseline_trace.parent.mkdir(parents=True, exist_ok=True)
    paths.baseline_trace.write_bytes(baseline_bytes)
    paths.variant_trace.write_bytes(variant_bytes)
    report["baseline_trace"] = _artifact_name(paths, paths.baseline_trace)
    report["variant_trace"] = _artifact_name(paths, paths.variant_trace)
    if service_log_payloads:
        copied_logs: list[dict[str, str]] = []
        for relative, payload, digest in service_log_payloads:
            destination = paths.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
            copied_logs.append({"path": relative.as_posix(), "sha256": digest})
        report["service_log_artifacts"] = copied_logs
    _write_json(paths.report, report)
    errors = _validate_report(manifest, paths, report)
    if errors:
        raise GateToolError("; ".join(errors))
    _archive_run(paths)
    if not getattr(args, "quiet", False):
        _print_artifacts(paths)
    return 0


def _validate_report(
    manifest: LoadedManifest, paths: ArtifactPaths, report: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    evidence = report.get("evidence")
    status = report.get("status")
    if report.get("schema_version") != "tgsrl.io/gate-run-record/v1alpha1":
        errors.append("report schema_version is invalid")
    if report.get("suite_id") != manifest.data["suite_id"]:
        errors.append("report suite_id does not match the gate manifest")
    if evidence not in EVIDENCE_VALUES:
        errors.append(f"unsupported evidence value: {evidence!r}")
    if status not in STATUS_VALUES:
        errors.append(f"unsupported status value: {status!r}")
    metrics = report.get("metrics")
    required_metrics = manifest.data["metrics_schema"]["required_metrics"]
    if not isinstance(metrics, dict):
        errors.append("report metrics must be an object")
    else:
        for side in ("baseline", "variant"):
            values = metrics.get(side)
            if not isinstance(values, dict):
                errors.append(f"report metrics.{side} must be an object")
                continue
            for metric in required_metrics:
                value = values.get(metric)
                if not _is_finite_number(value):
                    errors.append(f"report metrics.{side}.{metric} must be a finite number")
    trace_payloads: dict[str, dict[str, Any]] = {}
    for field_name in ("baseline_trace", "variant_trace"):
        try:
            artifact = paths.root / _relative_path(report.get(field_name), field_name)
        except GateToolError as exc:
            errors.append(str(exc))
        else:
            if not artifact.is_file():
                errors.append(f"report artifact is missing: {field_name}")
            else:
                try:
                    trace_payloads[field_name.removesuffix("_trace")] = _read_json(artifact)
                except GateToolError as error:
                    errors.append(str(error))
    if report.get("simulated"):
        if evidence != "SIMULATED":
            errors.append("simulator reports must use SIMULATED evidence only")
        if status not in {"NOT_RUN", "INVALID"}:
            errors.append("simulator final status must be NOT_RUN or INVALID")
    if evidence == "CPU_INTEGRATION":
        smoke = report.get("smoke")
        if not isinstance(smoke, dict) or not smoke.get("executed"):
            errors.append("CPU_INTEGRATION requires an executed smoke record")
        elif status != "BLOCKED" and smoke.get("exit_code") != 0:
            errors.append("CPU_INTEGRATION requires a successful executed smoke record")
        if status == "PASSED":
            errors.append("CPU_INTEGRATION cannot mark a GPU gate passed")
        if manifest.data["workload_runner"].get("evidence_mode") == "full_stack":
            fingerprint = report.get("environment_fingerprint")
            if (
                not isinstance(fingerprint, dict)
                or fingerprint.get("execution_mode") != "cpu-full-stack"
            ):
                errors.append("full-stack CPU evidence requires execution_mode=cpu-full-stack")
    if (
        evidence in REAL_EVIDENCE
        and manifest.data["workload_runner"].get("evidence_mode") == "full_stack"
    ):
        logs = report.get("service_log_artifacts")
        if not isinstance(logs, list) or not logs:
            errors.append("full-stack evidence requires service log artifacts")
        else:
            for index, record in enumerate(logs):
                if not isinstance(record, dict):
                    errors.append(f"service log artifact {index} must be an object")
                    continue
                try:
                    artifact = paths.root / _relative_path(
                        record.get("path"), f"service_log_artifacts[{index}].path"
                    )
                    if artifact.relative_to(paths.root).parts[:2] != (
                        "artifacts",
                        "services",
                    ):
                        raise GateToolError(
                            f"service_log_artifacts[{index}].path must stay under "
                            "artifacts/services"
                        )
                    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
                except (GateToolError, OSError) as error:
                    errors.append(f"cannot verify service log artifact {index}: {error}")
                    continue
                if record.get("sha256") != digest:
                    errors.append(f"service log artifact {index} digest does not match")
    if evidence in REAL_EVIDENCE:
        if report.get("workload_lock") != manifest.data["workload_lock"]:
            errors.append("real evidence workload_lock does not match the gate manifest")
        for count_name in ("warmup_runs", "measurement_runs"):
            if report.get(count_name) != manifest.data["comparisons"][count_name]:
                errors.append(f"real evidence {count_name} does not match the gate manifest")
        for side, trace in trace_payloads.items():
            events = trace.get("events")
            if (
                trace.get("schema_version") != "tgsrl.io/gate-trace/v1alpha1"
                or trace.get("suite_id") != manifest.data["suite_id"]
                or trace.get("label") != side
                or not isinstance(events, list)
            ):
                errors.append(f"{side} trace has an invalid schema or event list")
                continue
            if trace.get("seed") != manifest.data["workload_lock"]["seed"]:
                errors.append(f"{side} trace seed does not match the workload lock")
            if status == "BLOCKED" and not events:
                continue
            if manifest.data["workload_runner"].get("evidence_mode") == "full_stack":
                try:
                    _validate_full_stack_events(
                        events,
                        label=side,
                        required_variant_actions=list(
                            manifest.data["workload_runner"].get(
                                "required_variant_actions", ["pause", "resume"]
                            )
                        ),
                    )
                except GateToolError as error:
                    errors.append(str(error))
            if status == "PASSED" or (evidence == "CPU_INTEGRATION" and status == "NOT_RUN"):
                expected_iterations = {
                    "warmup": set(range(1, int(manifest.data["comparisons"]["warmup_runs"]) + 1)),
                    "measurement": set(
                        range(1, int(manifest.data["comparisons"]["measurement_runs"]) + 1)
                    ),
                }
                for phase, expected in expected_iterations.items():
                    observed = {
                        int(event.get("iteration", 0))
                        for event in events
                        if event.get("phase") == phase
                    }
                    if observed != expected:
                        errors.append(f"{side} trace has incomplete {phase} iterations")
            try:
                derived = _metrics_from_events(events)
            except (GateToolError, TypeError, ValueError) as error:
                errors.append(f"{side} trace metrics cannot be derived: {error}")
                continue
            reported = metrics.get(side, {}) if isinstance(metrics, dict) else {}
            for metric in required_metrics:
                if _is_finite_number(reported.get(metric)) and not math.isclose(
                    float(reported[metric]), float(derived[metric]), rel_tol=1e-9, abs_tol=1e-9
                ):
                    errors.append(f"report metrics.{side}.{metric} does not match raw trace")
        capture = report.get("trace_capture")
        if not isinstance(capture, dict):
            errors.append("real evidence requires trace_capture digests")
        else:
            for side, field_name in (("baseline", "baseline_trace"), ("variant", "variant_trace")):
                try:
                    artifact = paths.root / _relative_path(report.get(field_name), field_name)
                    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
                except (GateToolError, OSError) as error:
                    errors.append(f"cannot verify {side} trace digest: {error}")
                    continue
                if capture.get(f"{side}_digest") != digest:
                    errors.append(f"{side} trace digest does not match the artifact")
        if status == "PASSED" or (evidence == "CPU_INTEGRATION" and status == "NOT_RUN"):
            executions = report.get("executions")
            expected_executions = 2 * (
                int(manifest.data["comparisons"]["warmup_runs"])
                + int(manifest.data["comparisons"]["measurement_runs"])
            )
            if not isinstance(executions, list) or len(executions) != expected_executions:
                errors.append("real evidence has an incomplete execution record set")
            elif any(
                not isinstance(record, dict)
                or not record.get("executed")
                or record.get("exit_code") != 0
                or record.get("timed_out")
                for record in executions
            ):
                errors.append("real evidence contains a failed or timed-out workload execution")
    if evidence in REAL_GPU_EVIDENCE:
        if report.get("simulated") is not False:
            errors.append("GPU evidence requires simulated=false")
        fingerprint = report.get("environment_fingerprint")
        if not isinstance(fingerprint, dict):
            errors.append("GPU evidence requires environment_fingerprint")
        else:
            for field in manifest.data["environment_fingerprint"]["required_fields"]:
                if field not in fingerprint or fingerprint[field] in {None, ""}:
                    errors.append(f"GPU evidence missing environment_fingerprint.{field}")
            if (
                not isinstance(fingerprint.get("accelerator_count"), int)
                or int(fingerprint["accelerator_count"]) <= 0
            ):
                errors.append("GPU evidence requires a positive accelerator_count")
            if not fingerprint.get("accelerator_inventory_digest"):
                errors.append("GPU evidence requires accelerator_inventory_digest")
            if fingerprint.get("git_dirty") is not False:
                errors.append("GPU evidence requires a clean git worktree")
        node_sets: dict[str, set[str]] = {}
        for side, trace in trace_payloads.items():
            events = trace.get("events", [])
            node_sets[side] = {
                str(event.get("node_id", ""))
                for event in events
                if event.get("phase") == "measurement" and event.get("node_id")
            }
            side_metrics = metrics.get(side, {}) if isinstance(metrics, dict) else {}
            if (
                status == "PASSED"
                and _is_finite_number(side_metrics.get("gpu_active_time_ms"))
                and float(side_metrics["gpu_active_time_ms"]) <= 0
            ):
                errors.append(f"GPU evidence requires positive {side} gpu_active_time_ms")
            if status == "PASSED" and any(
                event.get("phase") == "measurement" and event.get("device") != "cuda"
                for event in events
            ):
                errors.append(f"GPU evidence contains a non-CUDA {side} measurement event")
        if status == "PASSED":
            required_nodes = 2 if evidence == "GPU_MULTI_NODE" else 1
            for side in ("baseline", "variant"):
                if len(node_sets.get(side, set())) < required_nodes:
                    errors.append(
                        f"{evidence} requires at least {required_nodes} {side} node identities"
                    )
            if node_sets.get("baseline") != node_sets.get("variant"):
                errors.append("baseline and variant GPU node identities must match")
    if not errors and status == "PASSED" and evaluate_gate(manifest, report)["status"] != "PASSED":
        errors.append("report status PASSED does not satisfy the configured gate rules")
    return errors


def evaluate_gate(manifest: LoadedManifest, report: dict[str, Any]) -> dict[str, Any]:
    baseline = report["metrics"]["baseline"]
    variant = report["metrics"]["variant"]
    decisions: list[dict[str, Any]] = []
    rules_passed = True
    for rule in manifest.data["rules"]:
        metric = rule["metric"]
        if rule.get("comparison") == "variant_over_baseline_ratio":
            denominator = float(baseline[metric])
            actual = 0.0 if denominator == 0 else float(variant[metric]) / denominator
        else:
            actual = float(variant[metric])
        passed = actual >= float(rule["threshold"])
        rules_passed = rules_passed and passed
        decisions.append(
            {
                "rule_id": rule["rule_id"],
                "gate": rule["gate"],
                "metric": metric,
                "actual": actual,
                "threshold": float(rule["threshold"]),
                "status": "PASSED" if passed else "FAILED",
            }
        )
    reported_status = report["status"]
    if reported_status in {"NOT_RUN", "BLOCKED", "INVALID", "FAILED"}:
        final_status = reported_status
    elif report.get("simulated") or report.get("evidence") == "CPU_INTEGRATION":
        final_status = "NOT_RUN" if rules_passed else "FAILED"
    else:
        final_status = "PASSED" if rules_passed else "FAILED"
    return {
        "status": final_status,
        "evidence": report["evidence"],
        "simulated": bool(report.get("simulated")),
        "rules": decisions,
    }


def load_campaign(path: Path) -> dict[str, Any]:
    campaign = _read_json(path)
    if campaign.get("schema_version") != "tgsrl.io/gate-campaign/v1alpha1":
        raise GateToolError("campaign schema_version must be tgsrl.io/gate-campaign/v1alpha1")
    experiments = campaign.get("experiments")
    if not isinstance(experiments, list):
        raise GateToolError("campaign experiments must be a list")
    experiment_ids = [item.get("experiment_id") for item in experiments if isinstance(item, dict)]
    if experiment_ids != [f"E{index}" for index in range(1, 9)]:
        raise GateToolError("campaign experiments must define E1 through E8 in order")
    output_directories: set[str] = set()
    for experiment in experiments:
        experiment_id = str(experiment["experiment_id"])
        output_directory = _relative_path(
            experiment.get("output_directory"),
            f"campaign {experiment_id} output_directory",
        ).as_posix()
        if output_directory in output_directories:
            raise GateToolError("campaign output directories must be unique")
        output_directories.add(output_directory)
        gate_manifest_path = ROOT / _relative_path(
            experiment.get("gate_manifest"), f"campaign {experiment_id} gate_manifest"
        )
        if not gate_manifest_path.is_file():
            raise GateToolError(f"campaign {experiment_id} gate_manifest does not exist")
        gate_manifest = load_manifest(gate_manifest_path)
        scenario_path = ROOT / _relative_path(
            experiment.get("scenario_manifest"),
            f"campaign {experiment_id} scenario_manifest",
        )
        if not scenario_path.is_file():
            raise GateToolError(f"campaign {experiment_id} scenario_manifest does not exist")
        scenario = _read_json(scenario_path)
        if (
            scenario.get("schema_version") != "tgsrl.io/hardware-scenario/v1alpha1"
            or scenario.get("experiment_id") != experiment_id
        ):
            raise GateToolError(f"campaign {experiment_id} scenario identity is invalid")
        minimum_evidence = experiment.get("minimum_evidence")
        if minimum_evidence not in REAL_GPU_EVIDENCE:
            raise GateToolError(
                f"campaign {experiment_id} must require GPU_SINGLE_NODE or GPU_MULTI_NODE"
            )
        requirements = experiment.get("requirements")
        if not isinstance(requirements, dict):
            raise GateToolError(f"campaign {experiment_id} requirements must be an object")
        for field in (
            "required_actions",
            "required_faults",
            "required_events",
            "gpu_profiles",
            "execution_modes",
        ):
            values = requirements.get(field, [])
            if not isinstance(values, list) or any(
                not isinstance(value, str) or not value for value in values
            ):
                raise GateToolError(
                    f"campaign {experiment_id} requirements.{field} must contain strings"
                )
        for field in ("minimum_nodes", "minimum_accelerators"):
            value = requirements.get(field, 0)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise GateToolError(
                    f"campaign {experiment_id} requirements.{field} must be non-negative"
                )
        topology = scenario.get("topology")
        if not isinstance(topology, dict):
            raise GateToolError(f"campaign {experiment_id} scenario topology must be an object")
        for field in ("minimum_nodes", "minimum_accelerators", "gpu_profiles"):
            if topology.get(field) != requirements.get(field):
                raise GateToolError(
                    f"campaign {experiment_id} scenario topology.{field} "
                    "does not match requirements"
                )
        faults = scenario.get("faults")
        if not isinstance(faults, list) or any(not isinstance(fault, dict) for fault in faults):
            raise GateToolError(f"campaign {experiment_id} scenario faults must be objects")
        scenario_faults = [fault.get("fault_id") for fault in faults]
        if scenario_faults != requirements.get("required_faults"):
            raise GateToolError(
                f"campaign {experiment_id} scenario faults do not match requirements"
            )
        required_evidence = scenario.get("required_evidence")
        if (
            not isinstance(required_evidence, list)
            or not required_evidence
            or any(not isinstance(value, str) or not value for value in required_evidence)
        ):
            raise GateToolError(
                f"campaign {experiment_id} scenario required_evidence must contain strings"
            )
        if len(set(required_evidence)) != len(required_evidence):
            raise GateToolError(
                f"campaign {experiment_id} scenario required_evidence must be unique"
            )
        unknown_evidence = sorted(set(required_evidence) - CAMPAIGN_EVIDENCE_REQUIREMENTS)
        if unknown_evidence:
            raise GateToolError(
                f"campaign {experiment_id} scenario required_evidence is unsupported: "
                + ", ".join(unknown_evidence)
            )
        execution_plan = scenario.get("execution_plan")
        if not isinstance(execution_plan, dict) or set(execution_plan) != {
            "baseline",
            "variant",
        }:
            raise GateToolError(
                f"campaign {experiment_id} scenario execution_plan must define baseline and variant"
            )
        for label in ("baseline", "variant"):
            steps = execution_plan[label]
            if not isinstance(steps, list) or not steps:
                raise GateToolError(
                    f"campaign {experiment_id} scenario execution_plan.{label} must be non-empty"
                )
        rules = experiment.get("rules")
        if not isinstance(rules, list) or not rules:
            raise GateToolError(f"campaign {experiment_id} rules must be non-empty")
        rule_ids: set[str] = set()
        for rule in rules:
            if not isinstance(rule, dict):
                raise GateToolError(f"campaign {experiment_id} rule must be an object")
            rule_id = rule.get("rule_id")
            if not isinstance(rule_id, str) or not rule_id or rule_id in rule_ids:
                raise GateToolError(f"campaign {experiment_id} rule IDs must be unique")
            rule_ids.add(rule_id)
            if not isinstance(rule.get("metric"), str) or not rule["metric"]:
                raise GateToolError(f"campaign {experiment_id} rule {rule_id} needs a metric")
            if rule["metric"] not in gate_manifest.data["metrics_schema"]["required_metrics"]:
                raise GateToolError(
                    f"campaign {experiment_id} rule {rule_id} metric is not declared "
                    "by its gate manifest"
                )
            if rule.get("operator") not in {">=", "<=", ">", "<", "=="}:
                raise GateToolError(f"campaign {experiment_id} rule {rule_id} has invalid operator")
            if rule.get("comparison") not in {
                None,
                "baseline",
                "variant",
                "variant_over_baseline_ratio",
                "variant_minus_baseline",
            }:
                raise GateToolError(
                    f"campaign {experiment_id} rule {rule_id} has invalid comparison"
                )
            threshold = rule.get("threshold")
            calibration_required = rule.get("calibration_required") is True
            if threshold is None and not calibration_required:
                raise GateToolError(
                    f"campaign {experiment_id} rule {rule_id} needs a threshold "
                    "or calibration_required"
                )
            if threshold is not None and not _is_finite_number(threshold):
                raise GateToolError(
                    f"campaign {experiment_id} rule {rule_id} threshold must be finite"
                )
    return campaign


def _campaign_rule_value(rule: dict[str, Any], report: dict[str, Any]) -> float:
    metrics = report.get("metrics", {})
    baseline = metrics.get("baseline", {}) if isinstance(metrics, dict) else {}
    variant = metrics.get("variant", {}) if isinstance(metrics, dict) else {}
    metric = str(rule.get("metric", ""))
    comparison = rule.get("comparison", "variant")
    left = variant.get(metric) if isinstance(variant, dict) else None
    right = baseline.get(metric) if isinstance(baseline, dict) else None
    if comparison == "baseline":
        value = right
    elif comparison == "variant":
        value = left
    elif comparison == "variant_over_baseline_ratio":
        if not _is_finite_number(right):
            raise GateToolError(f"campaign metric {metric} has a zero or invalid baseline")
        right_value = float(cast(int | float, right))
        if right_value == 0 or not _is_finite_number(left):
            raise GateToolError(f"campaign metric {metric} has a zero or invalid baseline")
        value = float(cast(int | float, left)) / right_value
    elif comparison == "variant_minus_baseline":
        if not _is_finite_number(left) or not _is_finite_number(right):
            raise GateToolError(f"campaign metric {metric} must be a finite number")
        value = float(cast(int | float, left)) - float(cast(int | float, right))
    else:
        value = left
    if not _is_finite_number(value):
        raise GateToolError(f"campaign metric {metric} must be a finite number")
    return float(cast(int | float, value))


def _compare_metric(actual: float, operator: str, threshold: float) -> bool:
    return {
        ">=": actual >= threshold,
        "<=": actual <= threshold,
        ">": actual > threshold,
        "<": actual < threshold,
        "==": math.isclose(actual, threshold, rel_tol=1e-9, abs_tol=1e-9),
    }[operator]


def _campaign_measurement_events(
    paths: ArtifactPaths,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    baseline = _read_json(paths.baseline_trace).get("events", [])
    variant = _read_json(paths.variant_trace).get("events", [])
    if not isinstance(baseline, list) or not isinstance(variant, list):
        raise GateToolError("campaign trace events must be lists")
    return (
        [
            event
            for event in baseline
            if isinstance(event, dict) and event.get("phase") == "measurement"
        ],
        [
            event
            for event in variant
            if isinstance(event, dict) and event.get("phase") == "measurement"
        ],
    )


def _validate_campaign_requirements(
    experiment: dict[str, Any], report: dict[str, Any], paths: ArtifactPaths
) -> list[str]:
    requirements = experiment["requirements"]
    errors: list[str] = []
    scenario_record = report.get("campaign_scenario")
    if not isinstance(scenario_record, dict):
        errors.append("campaign scenario artifact is missing")
    else:
        try:
            scenario_artifact = paths.root / _relative_path(
                scenario_record.get("path"), "campaign_scenario.path"
            )
            scenario_digest = hashlib.sha256(scenario_artifact.read_bytes()).hexdigest()
            expected_digest = hashlib.sha256(
                (ROOT / experiment["scenario_manifest"]).read_bytes()
            ).hexdigest()
            if scenario_record.get("sha256") != scenario_digest:
                errors.append("campaign scenario artifact digest does not match")
            elif scenario_digest != expected_digest:
                errors.append("campaign scenario artifact does not match the configured scenario")
        except (GateToolError, OSError) as error:
            errors.append(f"cannot verify campaign scenario artifact: {error}")
    fingerprint = report.get("environment_fingerprint", {})
    if not isinstance(fingerprint, dict):
        fingerprint = {}
    if fingerprint.get("git_commit") != git_commit():
        errors.append("evidence git commit does not match the campaign checkout")
    if int(fingerprint.get("accelerator_count", 0) or 0) < int(
        requirements.get("minimum_accelerators", 0)
    ):
        errors.append("accelerator count is below the experiment requirement")
    profiles = requirements.get("gpu_profiles", [])
    if profiles and fingerprint.get("gpu_profile") not in profiles:
        errors.append("environment gpu_profile does not match the experiment")
    execution_modes = requirements.get("execution_modes", [])
    if execution_modes and fingerprint.get("execution_mode") not in execution_modes:
        errors.append("environment execution_mode does not match the experiment")
    try:
        baseline_events, variant_events = _campaign_measurement_events(paths)
    except GateToolError as error:
        return [str(error)]
    for label, events in (("baseline", baseline_events), ("variant", variant_events)):
        nodes = {str(event.get("node_id")) for event in events if event.get("node_id")}
        if len(nodes) < int(requirements.get("minimum_nodes", 0)):
            errors.append(f"{label} node count is below the experiment requirement")
    observed_actions = {
        str(event.get("action"))
        for event in variant_events
        if event.get("event_type") in {"decision_applied", "control_completed"}
        and event.get("succeeded") is True
        and event.get("source") == ("scheduler" if event.get("action") == "bind" else "operator")
    }
    for action in requirements.get("required_actions", []):
        if action not in observed_actions:
            errors.append(f"required action {action} is missing from variant evidence")
    authoritative_sources = {
        "device_identity_verified": "worker",
        "fault_injected": "operator",
        "fault_recovered": "operator",
        "interference_observed": "worker",
        "sample_consumed": "worker",
        "worker_registered": "worker",
        "workload_completed": "worker",
    }
    observed_event_types = {
        str(event.get("event_type"))
        for event in variant_events
        if authoritative_sources.get(str(event.get("event_type")), event.get("source"))
        == event.get("source")
    }
    for event_type in requirements.get("required_events", []):
        if event_type not in observed_event_types:
            errors.append(f"required event {event_type} is missing from variant evidence")
    injected_faults = {
        str(event.get("fault_id"))
        for event in variant_events
        if event.get("event_type") == "fault_injected"
        and event.get("source") == "operator"
        and event.get("fault_id")
    }
    recovered_faults = {
        str(event.get("fault_id"))
        for event in variant_events
        if event.get("event_type") == "fault_recovered"
        and event.get("source") == "operator"
        and event.get("fault_id")
    }
    for fault in requirements.get("required_faults", []):
        if fault not in injected_faults or fault not in recovered_faults:
            errors.append(f"required fault {fault} lacks injected and recovered evidence")
    if requirements.get("exact_device_identity") is True:
        expected_device_class = {
            "full-gpu": "gpu.nvidia.com",
            "mig": "mig.nvidia.com",
        }.get(str(fingerprint.get("gpu_profile", "")))
        for label, events in (("baseline", baseline_events), ("variant", variant_events)):
            identities = [
                event
                for event in events
                if event.get("event_type") == "device_identity_verified"
                and event.get("source") == "worker"
            ]
            if not identities:
                errors.append(f"{label} exact device identity evidence is missing")
                continue
            iterations = {int(event.get("iteration", 0)) for event in events}
            identity_iterations = {int(event.get("iteration", 0)) for event in identities}
            if identity_iterations != iterations:
                errors.append(f"{label} exact device identity evidence is incomplete by iteration")
            for event in identities:
                scheduler_ids = sorted(
                    str(value) for value in event.get("scheduler_device_ids", [])
                )
                allocated_ids = sorted(
                    str(value) for value in event.get("allocated_device_ids", [])
                )
                worker_ids = sorted(str(value) for value in event.get("worker_device_ids", []))
                if (
                    not scheduler_ids
                    or scheduler_ids != allocated_ids
                    or scheduler_ids != worker_ids
                ):
                    errors.append(
                        f"{label} scheduler, allocation, and worker device identities differ"
                    )
                    break
                if expected_device_class and event.get("device_class") != expected_device_class:
                    errors.append(
                        f"{label} device class does not match the environment GPU profile"
                    )
                    break
            if int(requirements.get("minimum_nodes", 0)) > 1:
                for iteration in sorted({int(event.get("iteration", 0)) for event in events}):
                    by_node: dict[str, set[str]] = {}
                    for event in identities:
                        if int(event.get("iteration", 0)) != iteration:
                            continue
                        node_id = str(event.get("node_id", ""))
                        by_node.setdefault(node_id, set()).update(
                            str(value) for value in event.get("worker_device_ids", [])
                        )
                    if (
                        "" in by_node
                        or len(by_node) < int(requirements["minimum_nodes"])
                        or any(not device_ids for device_ids in by_node.values())
                        or sum(len(device_ids) for device_ids in by_node.values())
                        != len(set().union(*by_node.values()))
                    ):
                        errors.append(
                            f"{label} multi-node device identities are missing or overlap"
                        )
                        break
    experiment_id = experiment["experiment_id"]
    if any(event.get("experiment_id") != experiment_id for event in baseline_events):
        errors.append("baseline events do not match the campaign experiment identity")
    if any(event.get("experiment_id") != experiment_id for event in variant_events):
        errors.append("variant events do not match the campaign experiment identity")
    errors.extend(
        _validate_named_campaign_evidence(
            experiment=experiment,
            baseline_events=baseline_events,
            variant_events=variant_events,
            required_actions=requirements.get("required_actions", []),
            minimum_nodes=int(requirements.get("minimum_nodes", 0)),
        )
    )
    return errors


def _validate_named_campaign_evidence(
    *,
    experiment: dict[str, Any],
    baseline_events: list[dict[str, Any]],
    variant_events: list[dict[str, Any]],
    required_actions: list[str],
    minimum_nodes: int,
) -> list[str]:
    scenario = _read_json(ROOT / experiment["scenario_manifest"])
    required = scenario.get("required_evidence", [])
    if not isinstance(required, list):
        return []

    variant_actions = [
        event
        for event in variant_events
        if event.get("event_type") in {"decision_applied", "control_completed"}
        and event.get("succeeded") is True
        and event.get("source") == ("scheduler" if event.get("action") == "bind" else "operator")
    ]
    injected = [
        event
        for event in variant_events
        if event.get("event_type") == "fault_injected" and event.get("source") == "operator"
    ]
    recovered = [
        event
        for event in variant_events
        if event.get("event_type") == "fault_recovered" and event.get("source") == "operator"
    ]

    def has_finite(events: list[dict[str, Any]], field: str) -> bool:
        return any(_is_finite_number(event.get(field)) for event in events)

    def both_sides(predicate: Any) -> bool:
        return all(
            any(predicate(event) for event in events)
            for events in (baseline_events, variant_events)
        )

    def action_has(action: str, field: str, predicate: object | None = None) -> bool:
        for event in variant_actions:
            if event.get("action") != action:
                continue
            value = event.get(field)
            if predicate is None and value is not None and value != "":
                return True
            if callable(predicate) and predicate(value):
                return True
        return False

    checks = {
        "scheduler-binding": both_sides(
            lambda event: (
                event.get("event_type") == "device_identity_verified"
                and event.get("source") == "worker"
                and bool(event.get("scheduler_device_ids"))
            )
        ),
        "dra-allocation": both_sides(
            lambda event: (
                event.get("event_type") == "device_identity_verified"
                and event.get("source") == "worker"
                and bool(event.get("allocated_device_ids"))
            )
        ),
        "worker-device-identity": both_sides(
            lambda event: (
                event.get("event_type") == "device_identity_verified"
                and event.get("source") == "worker"
                and bool(event.get("worker_device_ids"))
            )
        ),
        "mig-device-class": both_sides(
            lambda event: (
                event.get("event_type") == "device_identity_verified"
                and event.get("source") == "worker"
                and event.get("device_class") == "mig.nvidia.com"
            )
        ),
        "parent-uuid": both_sides(
            lambda event: (
                event.get("event_type") == "device_identity_verified"
                and event.get("source") == "worker"
                and bool(event.get("parent_uuid"))
            )
        ),
        "throughput": both_sides(
            lambda event: (
                event.get("event_type") == "workload_completed"
                and event.get("source") == "worker"
                and _is_finite_number(event.get("elapsed_ms"))
                and float(event["elapsed_ms"]) > 0
                and isinstance(event.get("item_count"), int)
                and not isinstance(event.get("item_count"), bool)
                and int(event["item_count"]) > 0
            )
        ),
        "gpu-active-time": both_sides(
            lambda event: (
                event.get("event_type") == "sample_consumed"
                and event.get("source") == "worker"
                and _is_finite_number(event.get("gpu_active_ms"))
                and float(event["gpu_active_ms"]) > 0
            )
        ),
        "useful-gpu-time": both_sides(
            lambda event: (
                event.get("event_type") == "sample_consumed"
                and event.get("source") == "worker"
                and _is_finite_number(event.get("useful_gpu_time_ms"))
                and float(event["useful_gpu_time_ms"]) > 0
            )
        ),
        "policy-lag": both_sides(
            lambda event: (
                event.get("event_type") == "sample_consumed"
                and event.get("source") == "worker"
                and isinstance(event.get("contract_observation"), dict)
                and _is_finite_number(event["contract_observation"].get("policy_lag"))
            )
        ),
        "sample-staleness": both_sides(
            lambda event: (
                event.get("event_type") == "sample_consumed"
                and event.get("source") == "worker"
                and isinstance(event.get("contract_observation"), dict)
                and isinstance(event["contract_observation"].get("sample_stale"), bool)
            )
        ),
        "effective-sample-size": both_sides(
            lambda event: (
                event.get("event_type") == "sample_consumed"
                and event.get("source") == "worker"
                and isinstance(event.get("contract_observation"), dict)
                and _is_finite_number(event["contract_observation"].get("effective_sample_size"))
            )
        ),
        "interference-ratio": has_finite(
            [
                event
                for event in variant_events
                if event.get("event_type") == "interference_observed"
                and event.get("source") == "worker"
            ],
            "interference_ratio",
        ),
        "share-readback": action_has(
            "set_share",
            "observed_share",
            lambda value: _is_finite_number(value) and 0 < float(value) <= 1,
        ),
        "priority-readback": action_has(
            "set_priority",
            "observed_priority",
            lambda value: isinstance(value, int) and not isinstance(value, bool),
        ),
        "action-start": all(
            any(
                event.get("event_type") == "control_started"
                and event.get("source") == "operator"
                and event.get("action") == action
                for event in variant_events
            )
            for action in required_actions
        ),
        "action-receipt": all(action_has(action, "receipt_id") for action in required_actions),
        "readiness": any(
            event.get("action") in {"reload", "resume"} and event.get("ready") is True
            for event in variant_actions
        ),
        "fault-injected": bool(injected),
        "durable-receipt": all(
            action_has(action, "receipt_id") and action_has(action, "transaction_id")
            for action in required_actions
        ),
        "rollback": any(event.get("action") == "rollback" for event in variant_actions),
        "fault-recovered": bool(recovered),
        "node-identities": all(
            len({str(event.get("node_id")) for event in events if event.get("node_id")})
            >= minimum_nodes
            for events in (baseline_events, variant_events)
        ),
        "recovery": any(_is_finite_number(event.get("recovery_time_ms")) for event in recovered),
        "convergence-quality": both_sides(
            lambda event: (
                event.get("event_type") == "workload_completed"
                and event.get("source") == "worker"
                and _is_finite_number(event.get("convergence_quality"))
            )
        ),
    }
    return [
        f"required evidence {name} is missing or incomplete"
        for name in required
        if not checks[name]
    ]


def evaluate_campaign(campaign: dict[str, Any], reports_root: Path) -> dict[str, Any]:
    reports_root = reports_root.expanduser().resolve()
    if reports_root in {Path("/").resolve(), Path.home().resolve(), ROOT.resolve()}:
        raise GateToolError("campaign reports directory is too broad")
    results: list[dict[str, Any]] = []
    for experiment in campaign["experiments"]:
        experiment_id = experiment["experiment_id"]
        gate_manifest = load_manifest(ROOT / experiment["gate_manifest"])
        paths = artifact_paths(
            gate_manifest,
            _campaign_output_directory(reports_root, str(experiment["output_directory"])),
        )
        if not paths.report.is_file():
            results.append(
                {
                    "experiment_id": experiment_id,
                    "status": "NOT_RUN",
                    "evidence": None,
                    "blockers": ["evidence report is missing"],
                    "rules": [],
                }
            )
            continue
        try:
            report = _read_json(paths.report)
            validation_errors = _validate_report(gate_manifest, paths, report)
        except GateToolError as error:
            report = {}
            validation_errors = [str(error)]
        evidence = report.get("evidence")
        blockers = list(validation_errors)
        if report.get("experiment_id") != experiment_id:
            validation_errors.append("report experiment_id does not match the campaign")
            blockers.append("report experiment_id does not match the campaign")
        minimum_evidence = experiment["minimum_evidence"]
        evidence_insufficient = (
            evidence not in EVIDENCE_RANK
            or EVIDENCE_RANK[evidence] < EVIDENCE_RANK[minimum_evidence]
        )
        if evidence_insufficient:
            blockers.append(f"requires at least {minimum_evidence} evidence")
        requirement_errors: list[str] = []
        if report and not evidence_insufficient:
            try:
                requirement_errors = _validate_campaign_requirements(experiment, report, paths)
            except GateToolError as error:
                requirement_errors = [str(error)]
            blockers.extend(requirement_errors)
        rule_results: list[dict[str, Any]] = []
        rule_failed = False
        calibration_pending = False
        for rule in experiment["rules"]:
            result = {"rule_id": rule["rule_id"], "metric": rule["metric"]}
            if rule.get("threshold") is None:
                result.update({"status": "BLOCKED", "reason": "calibration_required"})
                calibration_pending = True
            else:
                try:
                    actual = _campaign_rule_value(rule, report)
                    passed = _compare_metric(actual, rule["operator"], float(rule["threshold"]))
                    result.update(
                        {
                            "actual": actual,
                            "operator": rule["operator"],
                            "threshold": float(rule["threshold"]),
                            "status": "PASSED" if passed else "FAILED",
                        }
                    )
                    rule_failed = rule_failed or not passed
                except (GateToolError, KeyError, TypeError, ValueError) as error:
                    result.update({"status": "INVALID", "reason": str(error)})
                    blockers.append(str(error))
            rule_results.append(result)
        source_status = report.get("status")
        if validation_errors or requirement_errors:
            experiment_status = "INVALID"
        elif source_status in {"INVALID", "FAILED", "BLOCKED", "NOT_RUN"}:
            experiment_status = source_status
        elif evidence_insufficient:
            experiment_status = "NOT_RUN"
        elif rule_failed:
            experiment_status = "FAILED"
        elif calibration_pending:
            experiment_status = "BLOCKED"
        else:
            experiment_status = "PASSED"
        results.append(
            {
                "experiment_id": experiment_id,
                "title": experiment["title"],
                "status": experiment_status,
                "evidence": evidence,
                "blockers": sorted(set(blockers)),
                "rules": rule_results,
            }
        )
    statuses = {result["status"] for result in results}
    overall = next(
        (status for status in ("INVALID", "FAILED", "BLOCKED", "NOT_RUN") if status in statuses),
        "PASSED",
    )
    return {
        "schema_version": "tgsrl.io/gate-campaign-result/v1alpha1",
        "campaign_id": campaign["campaign_id"],
        "status": overall,
        "experiments": results,
    }


def cmd_campaign_plan(args: argparse.Namespace) -> int:
    campaign = load_campaign(Path(args.campaign))
    print(json.dumps(campaign, indent=2, sort_keys=True))
    return 0


def cmd_campaign_evaluate(args: argparse.Namespace) -> int:
    campaign = load_campaign(Path(args.campaign))
    result = evaluate_campaign(campaign, Path(args.reports_dir).expanduser().resolve())
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output == "-":
        print(payload, end="")
    else:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload, encoding="utf-8")
    return 1 if args.require_pass and result["status"] != "PASSED" else 0


def build_calibration_report(campaign: dict[str, Any], reports_root: Path) -> dict[str, Any]:
    """Render observed values for null-threshold rules without mutating policy."""
    reports_root = reports_root.expanduser().resolve()
    if reports_root in {Path("/").resolve(), Path.home().resolve(), ROOT.resolve()}:
        raise GateToolError("campaign reports directory is too broad")
    observations: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    for experiment in campaign["experiments"]:
        calibration_rules = [
            rule
            for rule in experiment["rules"]
            if rule.get("threshold") is None and rule.get("calibration_required") is True
        ]
        if not calibration_rules:
            continue
        manifest = load_manifest(ROOT / experiment["gate_manifest"])
        paths = artifact_paths(
            manifest,
            _campaign_output_directory(reports_root, str(experiment["output_directory"])),
        )
        if not paths.report.is_file():
            missing.extend(
                {"experiment_id": experiment["experiment_id"], "rule_id": rule["rule_id"]}
                for rule in calibration_rules
            )
            continue
        report = _read_json(paths.report)
        validation_errors = _validate_report(manifest, paths, report)
        validation_errors.extend(_validate_campaign_requirements(experiment, report, paths))
        if validation_errors:
            raise GateToolError(
                f"campaign {experiment['experiment_id']} calibration evidence is invalid: "
                + "; ".join(validation_errors)
            )
        for rule in calibration_rules:
            observations.append(
                {
                    "experiment_id": experiment["experiment_id"],
                    "rule_id": rule["rule_id"],
                    "metric": rule["metric"],
                    "comparison": rule.get("comparison", "variant"),
                    "operator": rule["operator"],
                    "observed_value": _campaign_rule_value(rule, report),
                    "evidence": report["evidence"],
                    "report_sha256": hashlib.sha256(paths.report.read_bytes()).hexdigest(),
                }
            )
    return {
        "schema_version": "tgsrl.io/gate-calibration-report/v1alpha1",
        "campaign_id": campaign["campaign_id"],
        "campaign_revision": campaign["campaign_revision"],
        "campaign_sha256": hashlib.sha256(
            json.dumps(campaign, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "status": "READY_FOR_REVIEW" if observations and not missing else "INCOMPLETE",
        "observations": observations,
        "missing": missing,
        "policy_mutated": False,
        "review_required": True,
    }


def cmd_campaign_calibrate(args: argparse.Namespace) -> int:
    campaign = load_campaign(Path(args.campaign))
    result = build_calibration_report(campaign, Path(args.reports_dir).expanduser().resolve())
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output == "-":
        print(payload, end="")
    else:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload, encoding="utf-8")
    return 0


def _campaign_executable(value: str, *, label: str) -> Path:
    raw = value.strip()
    if not raw:
        raise GateToolError(f"{label} is required")
    candidate = Path(raw).expanduser()
    if candidate.is_absolute() or len(candidate.parts) > 1:
        resolved = candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()
    else:
        discovered = shutil.which(raw)
        if discovered is None:
            raise GateToolError(f"{label} is not available: {raw}")
        resolved = Path(discovered).resolve()
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise GateToolError(f"{label} is not executable: {resolved}")
    return resolved


def _write_campaign_execution_log(
    root: Path, record: dict[str, Any], stdout: bytes, stderr: bytes
) -> list[dict[str, str]]:
    log_root = root / "artifacts" / "services" / "campaign-executor"
    log_root.mkdir(parents=True, exist_ok=True)
    artifacts: list[dict[str, str]] = []
    for name, payload in (("stdout.log", stdout), ("stderr.log", stderr)):
        for key, value in os.environ.items():
            if key.upper().endswith(SENSITIVE_ENV_SUFFIXES):
                encoded = value.encode()
                if len(encoded) >= 8:
                    payload = payload.replace(encoded, b"[REDACTED]")
        path = log_root / name
        path.write_bytes(payload)
        path.chmod(0o600)
        artifacts.append(
            {
                "path": path.relative_to(root).as_posix(),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    record["stdout_artifact"] = artifacts[0]["path"]
    record["stderr_artifact"] = artifacts[1]["path"]
    return artifacts


def _write_campaign_execution_summary(
    campaign: dict[str, Any],
    reports_root: Path,
    executions: list[dict[str, Any]],
    status: str,
    *,
    error: str = "",
) -> Path:
    output = reports_root / "campaign-execution.json"
    payload = {
        "schema_version": "tgsrl.io/gate-campaign-execution/v1alpha1",
        "campaign_id": campaign["campaign_id"],
        "status": status,
        "executions": executions,
    }
    if error:
        payload["error"] = error
    _write_json(output, payload)
    return output


def _copy_campaign_failure_diagnostics(
    source_root: Path, destination_root: Path
) -> list[dict[str, str]]:
    source = source_root / "artifacts" / "services" / "hardware-driver"
    if not source.is_dir():
        return []
    destination = destination_root / "artifacts" / "services" / "hardware-driver"
    allowed_names = {"request.json", "response.json", "stdout.log", "stderr.log"}
    files = [
        path
        for path in source.rglob("*")
        if path.name in allowed_names and (path.is_file() or path.is_symlink())
    ]
    if len(files) > MAX_CAMPAIGN_DIAGNOSTIC_FILES:
        raise GateToolError("campaign failure diagnostics contain too many files")
    total_bytes = 0
    for path in files:
        if path.is_symlink():
            raise GateToolError("campaign failure diagnostics must not contain symbolic links")
        resolved = path.resolve()
        if source != resolved and source not in resolved.parents:
            raise GateToolError("campaign failure diagnostic escapes its output directory")
        total_bytes += path.stat().st_size
        if total_bytes > MAX_CAMPAIGN_DIAGNOSTIC_BYTES:
            raise GateToolError("campaign failure diagnostics exceed the size limit")
    if destination.is_dir():
        shutil.rmtree(destination)
    copied: list[dict[str, str]] = []
    for path in files:
        target = destination / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        copied.append(
            {
                "path": target.relative_to(destination_root).as_posix(),
                "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
            }
        )
    return copied


def _validate_campaign_executor_output(report: dict[str, Any], source_root: Path) -> None:
    references: list[tuple[object, str]] = [
        (report.get("baseline_trace"), "baseline_trace"),
        (report.get("variant_trace"), "variant_trace"),
    ]
    service_logs = report.get("service_log_artifacts", [])
    if not isinstance(service_logs, list):
        raise GateToolError("campaign executor report service_log_artifacts must be a list")
    for index, record in enumerate(service_logs):
        if not isinstance(record, dict):
            raise GateToolError(f"campaign executor service log {index} must be an object")
        references.append((record.get("path"), f"service_log_artifacts[{index}].path"))
    for value, field in references:
        relative = _relative_path(value, f"campaign executor {field}")
        resolved = (source_root / relative).resolve()
        if source_root != resolved and source_root not in resolved.parents:
            raise GateToolError(f"campaign executor {field} escapes its output directory")
        if not resolved.is_file():
            raise GateToolError(f"campaign executor artifact is missing: {field}")


def _validate_campaign_orchestrator(
    report: dict[str, Any],
    *,
    executor_digest: str,
    gate_tools_digest: str,
    driver_digest: str,
    campaign_digest: str,
    gate_manifest_digest: str,
    scenario_digest: str,
) -> None:
    orchestrator = report.get("orchestrator")
    expected = {
        "schema_version": "tgsrl.io/hardware-orchestrator/v1alpha1",
        "executor_sha256": executor_digest,
        "gate_tools_sha256": gate_tools_digest,
        "driver_sha256": driver_digest,
        "campaign_sha256": campaign_digest,
        "gate_manifest_sha256": gate_manifest_digest,
        "scenario_sha256": scenario_digest,
    }
    if not isinstance(orchestrator, dict) or any(
        orchestrator.get(field) != value for field, value in expected.items()
    ):
        raise GateToolError("campaign executor report does not match the locked execution inputs")


def _campaign_output_directory(reports_root: Path, relative: str) -> Path:
    candidate = reports_root / relative
    if candidate.is_symlink():
        raise GateToolError("campaign output directory must not be a symbolic link")
    resolved = candidate.resolve()
    if reports_root != resolved and reports_root not in resolved.parents:
        raise GateToolError("campaign output directory escapes the reports directory")
    return resolved


def cmd_campaign_run(args: argparse.Namespace) -> int:
    """Execute E1-E8 through the repository orchestrator and an environment driver."""
    campaign_path = Path(args.campaign).expanduser().resolve()
    campaign = load_campaign(campaign_path)
    reports_root = Path(args.reports_dir).expanduser().resolve()
    if reports_root in {Path("/").resolve(), Path.home().resolve(), ROOT.resolve()}:
        raise GateToolError("campaign reports directory is too broad")
    reports_root.mkdir(parents=True, exist_ok=True)
    execution_summary = reports_root / "campaign-execution.json"
    campaign_summary = reports_root / "campaign-report.json"
    execution_summary.unlink(missing_ok=True)
    campaign_summary.unlink(missing_ok=True)
    try:
        executor = _campaign_executable(
            str(DEFAULT_CAMPAIGN_EXECUTOR), label="repository campaign executor"
        )
        driver = _campaign_executable(args.driver, label="campaign environment driver")
    except GateToolError as error:
        _write_campaign_execution_summary(campaign, reports_root, [], "FAILED", error=str(error))
        raise
    requested = args.experiment or [item["experiment_id"] for item in campaign["experiments"]]
    if args.require_pass and args.experiment:
        error = "--require-pass requires executing the complete E1-E8 campaign"
        _write_campaign_execution_summary(campaign, reports_root, [], "FAILED", error=error)
        raise GateToolError(error)
    if len(set(requested)) != len(requested):
        raise GateToolError("campaign experiments must not be repeated")
    selected = set(requested)
    experiments = [item for item in campaign["experiments"] if item["experiment_id"] in selected]
    if len(experiments) != len(selected):
        raise GateToolError("campaign selection contains an unknown experiment")
    timeout = float(args.timeout_seconds)
    if not math.isfinite(timeout) or timeout <= 0:
        raise GateToolError("campaign executor timeout must be positive and finite")

    executions: list[dict[str, Any]] = []
    campaign_digest = hashlib.sha256(campaign_path.read_bytes()).hexdigest()
    gate_tools_path = Path(__file__).resolve()
    gate_tools_digest = hashlib.sha256(gate_tools_path.read_bytes()).hexdigest()
    executor_digest = hashlib.sha256(executor.read_bytes()).hexdigest()
    driver_digest = hashlib.sha256(driver.read_bytes()).hexdigest()
    campaign_inputs = {
        campaign_path: campaign_digest,
        gate_tools_path: gate_tools_digest,
        executor: executor_digest,
        driver: driver_digest,
    }
    for experiment in campaign["experiments"]:
        for field in ("gate_manifest", "scenario_manifest"):
            path = (ROOT / experiment[field]).resolve()
            campaign_inputs[path] = hashlib.sha256(path.read_bytes()).hexdigest()
    for experiment in experiments:
        experiment_id = str(experiment["experiment_id"])
        gate_manifest_path = (ROOT / experiment["gate_manifest"]).resolve()
        scenario_path = (ROOT / experiment["scenario_manifest"]).resolve()
        destination = _campaign_output_directory(reports_root, str(experiment["output_directory"]))
        gate_manifest = load_manifest(gate_manifest_path)
        _reset_managed_artifacts(artifact_paths(gate_manifest, destination))
        with tempfile.TemporaryDirectory(prefix=f"tgsrl-{experiment_id.lower()}-") as directory:
            source_root = Path(directory).resolve()
            argv = [
                str(executor),
                "--experiment",
                experiment_id,
                "--campaign",
                str(campaign_path),
                "--gate-manifest",
                str(gate_manifest_path),
                "--scenario",
                str(scenario_path),
                "--output-dir",
                str(source_root),
                "--evidence",
                str(experiment["minimum_evidence"]),
                "--driver",
                str(driver),
            ]
            environment = dict(os.environ)
            environment.update(
                {
                    "TGSRL_CAMPAIGN_ID": str(campaign["campaign_id"]),
                    "TGSRL_EXPERIMENT_ID": experiment_id,
                    "TGSRL_GATE_MANIFEST": str(gate_manifest_path),
                    "TGSRL_SCENARIO_MANIFEST": str(scenario_path),
                    "TGSRL_GATE_OUTPUT_DIR": str(source_root),
                    "TGSRL_GATE_EVIDENCE": str(experiment["minimum_evidence"]),
                    "TGSRL_CAMPAIGN_DRIVER": str(driver),
                }
            )
            record, succeeded, stdout, stderr = _run_command(argv, timeout, environment=environment)
            record.update(
                {
                    "experiment_id": experiment_id,
                    "evidence": experiment["minimum_evidence"],
                    "output_directory": experiment["output_directory"],
                    "executor_sha256": executor_digest,
                    "driver_sha256": driver_digest,
                    "campaign_sha256": campaign_digest,
                    "gate_tools_sha256": gate_tools_digest,
                    "gate_manifest_sha256": campaign_inputs[gate_manifest_path],
                    "scenario_sha256": campaign_inputs[scenario_path],
                    "status": "SUCCEEDED" if succeeded else "FAILED",
                }
            )
            changed_input = next(
                (
                    path
                    for path, digest in campaign_inputs.items()
                    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest
                ),
                None,
            )
            if changed_input is not None:
                record["status"] = "FAILED"
                record["error"] = f"campaign input changed during execution: {changed_input.name}"
                record["log_artifacts"] = _write_campaign_execution_log(
                    destination, record, stdout, stderr
                )
                executions.append(record)
                _write_campaign_execution_summary(campaign, reports_root, executions, "FAILED")
                raise GateToolError(record["error"])
            if not succeeded:
                try:
                    record["driver_diagnostics"] = _copy_campaign_failure_diagnostics(
                        source_root, destination
                    )
                except GateToolError as diagnostic_error:
                    record["diagnostic_error"] = str(diagnostic_error)
                record["log_artifacts"] = _write_campaign_execution_log(
                    destination, record, stdout, stderr
                )
                executions.append(record)
                _write_campaign_execution_summary(campaign, reports_root, executions, "FAILED")
                raise GateToolError(
                    f"campaign executor failed for {experiment_id} with exit {record['exit_code']}"
                )
            source_paths = artifact_paths(gate_manifest, source_root)
            report_path = source_paths.report.resolve()
            if source_root != report_path and source_root not in report_path.parents:
                record["status"] = "FAILED"
                executions.append(record)
                _write_campaign_execution_summary(campaign, reports_root, executions, "FAILED")
                raise GateToolError(
                    f"campaign executor report escapes its output directory for {experiment_id}"
                )
            if not report_path.is_file():
                record["status"] = "FAILED"
                executions.append(record)
                _write_campaign_execution_summary(campaign, reports_root, executions, "FAILED")
                raise GateToolError(f"campaign executor produced no report for {experiment_id}")
            try:
                source_report = _read_json(source_paths.report)
                _validate_campaign_executor_output(source_report, source_root)
                _validate_campaign_orchestrator(
                    source_report,
                    executor_digest=executor_digest,
                    gate_tools_digest=gate_tools_digest,
                    driver_digest=driver_digest,
                    campaign_digest=campaign_digest,
                    gate_manifest_digest=campaign_inputs[gate_manifest_path],
                    scenario_digest=campaign_inputs[scenario_path],
                )
                cmd_campaign_ingest(
                    argparse.Namespace(
                        campaign=str(campaign_path),
                        reports_dir=str(reports_root),
                        experiment=experiment_id,
                        report=str(source_paths.report),
                        quiet=True,
                    )
                )
            except (GateToolError, OSError) as error:
                record["status"] = "FAILED"
                record["error"] = str(error)
                try:
                    record["driver_diagnostics"] = _copy_campaign_failure_diagnostics(
                        source_root, destination
                    )
                except GateToolError as diagnostic_error:
                    record["diagnostic_error"] = str(diagnostic_error)
                record["log_artifacts"] = _write_campaign_execution_log(
                    destination, record, stdout, stderr
                )
                executions.append(record)
                _write_campaign_execution_summary(campaign, reports_root, executions, "FAILED")
                raise GateToolError(
                    f"campaign evidence was rejected for {experiment_id}: {error}"
                ) from error
            target_paths = artifact_paths(gate_manifest, destination)
            record["log_artifacts"] = _write_campaign_execution_log(
                destination, record, stdout, stderr
            )
            ingested_report = _read_json(target_paths.report)
            service_logs = ingested_report.setdefault("service_log_artifacts", [])
            if not isinstance(service_logs, list):
                raise GateToolError("campaign executor report service_log_artifacts must be a list")
            service_logs.extend(record["log_artifacts"])
            ingested_report["campaign_execution"] = record
            _write_json(target_paths.report, ingested_report)
            validation_errors = _validate_report(gate_manifest, target_paths, ingested_report)
            validation_errors.extend(
                _validate_campaign_requirements(experiment, ingested_report, target_paths)
            )
            if validation_errors:
                record["status"] = "FAILED"
                record["error"] = "; ".join(validation_errors)
                executions.append(record)
                _write_campaign_execution_summary(campaign, reports_root, executions, "FAILED")
                raise GateToolError(record["error"])
            _archive_run(target_paths)
            executions.append(record)
            _write_campaign_execution_summary(campaign, reports_root, executions, "RUNNING")

    result = evaluate_campaign(campaign, reports_root)
    _write_json(campaign_summary, result)
    _write_campaign_execution_summary(campaign, reports_root, executions, result["status"])
    print(
        json.dumps(
            {
                "campaign_id": campaign["campaign_id"],
                "status": result["status"],
                "report": str(campaign_summary),
                "execution_report": str(execution_summary),
            },
            sort_keys=True,
        )
    )
    if result["status"] in {"INVALID", "FAILED"}:
        return 1
    return 1 if args.require_pass and result["status"] != "PASSED" else 0


def cmd_campaign_ingest(args: argparse.Namespace) -> int:
    campaign = load_campaign(Path(args.campaign))
    experiment = next(
        (item for item in campaign["experiments"] if item["experiment_id"] == args.experiment),
        None,
    )
    if experiment is None:
        raise GateToolError(f"unknown campaign experiment {args.experiment}")
    source = Path(args.report).expanduser().resolve()
    report = _read_json(source)
    if report.get("experiment_id") != args.experiment:
        raise GateToolError("external report experiment_id does not match the selected experiment")
    evidence = report.get("evidence")
    minimum_evidence = experiment["minimum_evidence"]
    if evidence not in EVIDENCE_RANK or EVIDENCE_RANK[evidence] < EVIDENCE_RANK[minimum_evidence]:
        raise GateToolError(
            f"campaign experiment {args.experiment} requires at least {minimum_evidence} evidence"
        )
    reports_root = Path(args.reports_dir).expanduser().resolve()
    if reports_root in {Path("/").resolve(), Path.home().resolve(), ROOT.resolve()}:
        raise GateToolError("campaign reports directory is too broad")
    reports_root.mkdir(parents=True, exist_ok=True)
    target_root = _campaign_output_directory(reports_root, str(experiment["output_directory"]))
    with tempfile.TemporaryDirectory(prefix="tgsrl-campaign-ingest-") as directory:
        temporary_root = Path(directory)
        result = cmd_ingest(
            argparse.Namespace(
                manifest=str(ROOT / experiment["gate_manifest"]),
                output_dir=str(temporary_root),
                report=str(source),
                quiet=True,
            )
        )
        if result != 0:
            return result
        manifest = load_manifest(ROOT / experiment["gate_manifest"])
        temporary_paths = artifact_paths(manifest, temporary_root)
        ingested_report = _read_json(temporary_paths.report)
        scenario_source = ROOT / experiment["scenario_manifest"]
        scenario_destination = temporary_root / "artifacts" / "config" / scenario_source.name
        scenario_destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(scenario_source, scenario_destination)
        ingested_report["campaign_scenario"] = {
            "path": scenario_destination.relative_to(temporary_root).as_posix(),
            "sha256": hashlib.sha256(scenario_destination.read_bytes()).hexdigest(),
        }
        _write_json(temporary_paths.report, ingested_report)
        requirement_errors = _validate_campaign_requirements(
            experiment, ingested_report, temporary_paths
        )
        if requirement_errors:
            raise GateToolError("; ".join(requirement_errors))
        _archive_run(temporary_paths)
        target_paths = artifact_paths(manifest, target_root)
        _reset_managed_artifacts(target_paths)
        for artifact in temporary_root.rglob("*"):
            if artifact.is_file():
                destination = target_root / artifact.relative_to(temporary_root)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(artifact, destination)
    if not getattr(args, "quiet", False):
        print(
            json.dumps(
                {"experiment_id": args.experiment, "report": str(target_root / "report.json")}
            )
        )
    return 0


def _load_current_report(
    args: argparse.Namespace,
) -> tuple[LoadedManifest, ArtifactPaths, dict[str, Any]]:
    manifest = load_manifest(Path(args.manifest))
    paths = artifact_paths(manifest, Path(args.output_dir))
    report = _read_json(paths.report)
    errors = _validate_report(manifest, paths, report)
    if errors:
        raise GateToolError("; ".join(errors))
    return manifest, paths, report


def cmd_report(args: argparse.Namespace) -> int:
    manifest, paths, report = _load_current_report(args)
    print(
        json.dumps(
            {
                "suite_id": manifest.data["suite_id"],
                "report": str(paths.report),
                "summary": evaluate_gate(manifest, report),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_evaluate(args: argparse.Namespace) -> int:
    manifest, _paths, report = _load_current_report(args)
    print(json.dumps(evaluate_gate(manifest, report), indent=2, sort_keys=True))
    return 0


def cmd_replay_entry(args: argparse.Namespace) -> int:
    manifest = load_manifest(Path(args.manifest))
    artifacts = manifest.data["artifacts"]
    print(
        json.dumps(
            {
                "schema_version": "tgsrl.io/gate-replay-entry/v1alpha1",
                "suite_id": manifest.data["suite_id"],
                "scenario_manifest": manifest.data["scenario_manifest"],
                "baseline_trace": artifacts["baseline_trace"],
                "variant_trace": artifacts["variant_trace"],
                "expected_evidence": sorted(EVIDENCE_VALUES),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_fingerprint(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            build_environment_fingerprint(execution_mode=args.execution_mode),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    subparsers = parser.add_subparsers(dest="command", required=True)
    simulate = subparsers.add_parser("simulate", help="Generate simulated artifacts")
    simulate.set_defaults(func=cmd_simulate)
    report = subparsers.add_parser("report", help="Render a run summary")
    report.set_defaults(func=cmd_report)
    evaluate = subparsers.add_parser("evaluate", help="Evaluate the current report")
    evaluate.set_defaults(func=cmd_evaluate)
    replay = subparsers.add_parser("replay-entry", help="Print the replay entry")
    replay.set_defaults(func=cmd_replay_entry)
    cpu = subparsers.add_parser("cpu-smoke", help="Capture CPU integration evidence")
    cpu.add_argument("--smoke-command", default="python -c 'print(\"cpu smoke\")'")
    cpu.add_argument("--smoke-timeout", type=float, default=300.0)
    cpu.set_defaults(func=cmd_cpu_smoke)
    hardware = subparsers.add_parser(
        "hardware-run", help="Execute the locked workload on a CUDA runner"
    )
    hardware.add_argument("--evidence", choices=sorted(REAL_GPU_EVIDENCE), required=True)
    hardware.set_defaults(func=cmd_hardware_run)
    ingest = subparsers.add_parser("ingest", help="Ingest an external run record")
    ingest.add_argument("--report", required=True)
    ingest.set_defaults(func=cmd_ingest)
    fingerprint = subparsers.add_parser("fingerprint", help="Print an environment fingerprint")
    fingerprint.add_argument(
        "--execution-mode",
        default="manual",
        choices=[
            "simulator",
            "cpu-smoke",
            "cpu-runner",
            "cpu-full-stack",
            "manual",
            "ci",
            "gpu-manual",
            "gpu-runner",
        ],
    )
    fingerprint.set_defaults(func=cmd_fingerprint)
    campaign_plan = subparsers.add_parser(
        "campaign-plan", help="Validate and print the E1-E8 campaign contract"
    )
    campaign_plan.add_argument("--campaign", default=str(DEFAULT_CAMPAIGN))
    campaign_plan.set_defaults(func=cmd_campaign_plan)
    campaign_evaluate = subparsers.add_parser(
        "campaign-evaluate", help="Evaluate E1-E8 evidence reports"
    )
    campaign_evaluate.add_argument("--campaign", default=str(DEFAULT_CAMPAIGN))
    campaign_evaluate.add_argument("--reports-dir", default=str(DEFAULT_CAMPAIGN_REPORTS))
    campaign_evaluate.add_argument("--output", default="-")
    campaign_evaluate.add_argument("--require-pass", action="store_true")
    campaign_evaluate.set_defaults(func=cmd_campaign_evaluate)
    campaign_run = subparsers.add_parser(
        "campaign-run", help="Execute E1-E8 through the repository orchestrator"
    )
    campaign_run.add_argument("--campaign", default=str(DEFAULT_CAMPAIGN))
    campaign_run.add_argument("--reports-dir", default=str(DEFAULT_CAMPAIGN_REPORTS))
    campaign_run.add_argument(
        "--driver",
        default=os.environ.get("TGSRL_CAMPAIGN_DRIVER", ""),
        help="target-environment atomic operation driver",
    )
    campaign_run.add_argument(
        "--experiment", action="append", choices=[f"E{index}" for index in range(1, 9)]
    )
    campaign_run.add_argument("--timeout-seconds", type=float, default=7200.0)
    campaign_run.add_argument("--require-pass", action="store_true")
    campaign_run.set_defaults(func=cmd_campaign_run)
    campaign_calibrate = subparsers.add_parser(
        "campaign-calibrate",
        help="Render observed values for manual threshold calibration",
    )
    campaign_calibrate.add_argument("--campaign", default=str(DEFAULT_CAMPAIGN))
    campaign_calibrate.add_argument("--reports-dir", default=str(DEFAULT_CAMPAIGN_REPORTS))
    campaign_calibrate.add_argument("--output", default="-")
    campaign_calibrate.set_defaults(func=cmd_campaign_calibrate)
    campaign_ingest = subparsers.add_parser(
        "campaign-ingest", help="Validate and store one E1-E8 evidence report"
    )
    campaign_ingest.add_argument("experiment", choices=[f"E{index}" for index in range(1, 9)])
    campaign_ingest.add_argument("--campaign", default=str(DEFAULT_CAMPAIGN))
    campaign_ingest.add_argument("--reports-dir", default=str(DEFAULT_CAMPAIGN_REPORTS))
    campaign_ingest.add_argument("--report", required=True)
    campaign_ingest.set_defaults(func=cmd_campaign_ingest)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except GateToolError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
