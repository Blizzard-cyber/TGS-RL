#!/usr/bin/env python3
"""Generate, ingest, and evaluate Gate G/I evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import shlex
import shutil
import socket
import statistics
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "configs" / "gates" / "gate-gi.json"
DEFAULT_OUTPUT_DIR = ROOT / ".cache" / "tgsrl" / "gate-gi"

STATUS_VALUES = {"NOT_RUN", "BLOCKED", "INVALID", "PASSED", "FAILED"}
EVIDENCE_VALUES = {
    "SIMULATED",
    "CPU_INTEGRATION",
    "GPU_SINGLE_NODE",
    "GPU_MULTI_NODE",
}
REAL_GPU_EVIDENCE = {"GPU_SINGLE_NODE", "GPU_MULTI_NODE"}
REAL_EVIDENCE = {"CPU_INTEGRATION", *REAL_GPU_EVIDENCE}


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
    variants = runner.get("variants")
    if not isinstance(variants, dict) or set(variants) != {"baseline", "variant"}:
        raise GateToolError("manifest workload_runner.variants must define baseline and variant")
    for label, variant in variants.items():
        if not isinstance(variant, dict) or variant.get("control_mode") not in {"static", "tgsrl"}:
            raise GateToolError(f"manifest workload_runner.variants.{label} is invalid")
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
    for directory in (paths.root / "artifacts" / "logs", paths.root / "artifacts" / "config"):
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
    _print_artifacts(paths)
    return 0


def _run_command(
    argv: list[str], timeout_seconds: float
) -> tuple[dict[str, Any], bool, bytes, bytes]:
    if not argv or any(not value for value in argv):
        raise GateToolError("workload argv must not be empty")
    try:
        completed = subprocess.run(
            argv, cwd=ROOT, capture_output=True, check=False, timeout=timeout_seconds
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
    if not consumed or not completed:
        raise GateToolError("measurement trace requires consumed samples and completed iterations")
    latencies = [float(event.get("duration_ms", 0.0)) for event in consumed]
    elapsed_ms = sum(float(event.get("elapsed_ms", 0.0)) for event in completed)
    item_count = sum(int(event.get("item_count", 0)) for event in completed)
    observations = [event.get("contract_observation", {}) for event in consumed]
    action_count = len(actions)
    return {
        "latency_ms_p50": _percentile(latencies, 0.50),
        "latency_ms_p95": _percentile(latencies, 0.95),
        "latency_ms_p99": _percentile(latencies, 0.99),
        "throughput_items_per_s": 0.0 if elapsed_ms <= 0 else item_count * 1000.0 / elapsed_ms,
        "decision_count": float(action_count),
        "end_to_end_iteration_ms_p50": _percentile(
            [float(event.get("elapsed_ms", 0.0)) for event in completed], 0.50
        ),
        "scheduling_latency_ms_p95": _percentile(
            [float(event.get("duration_ms", 0.0)) for event in actions], 0.95
        ),
        "gpu_active_time_ms": sum(float(event.get("gpu_active_ms", 0.0)) for event in consumed),
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
                if event.get("action") == "prepare_pause"
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
        ),
    }


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
        "environment_fingerprint": build_environment_fingerprint(execution_mode="cpu-runner"),
        "warmup_runs": int(manifest.data["comparisons"]["warmup_runs"]),
        "measurement_runs": int(manifest.data["comparisons"]["measurement_runs"]),
        "baseline_trace": _artifact_name(paths, paths.baseline_trace),
        "variant_trace": _artifact_name(paths, paths.variant_trace),
        "metrics": {"baseline": baseline["metrics"], "variant": variant["metrics"]},
        "executions": [*baseline_executions, *variant_executions],
        "smoke": smoke,
        "notes": ["CPU integration executed the locked workload; GPU gates remain not run."],
    }
    run["trace_capture"] = {
        "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
        "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
    }
    _capture_locked_inputs(manifest, paths)
    _write_json(paths.report, run)
    errors = _validate_report(manifest, paths, run)
    if errors:
        raise GateToolError("; ".join(errors))
    _archive_run(paths)
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
    report = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": manifest.data["suite_id"],
        "evidence": args.evidence,
        "status": "PASSED",
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
    _capture_locked_inputs(manifest, paths)
    evaluation = evaluate_gate(manifest, report)
    report["status"] = evaluation["status"]
    _write_json(paths.report, report)
    errors = _validate_report(manifest, paths, report)
    if errors:
        raise GateToolError("; ".join(errors))
    _archive_run(paths)
    _print_artifacts(paths)
    return 0 if report["status"] == "PASSED" else 1


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
    _reset_managed_artifacts(paths)
    paths.baseline_trace.parent.mkdir(parents=True, exist_ok=True)
    paths.baseline_trace.write_bytes(baseline_bytes)
    paths.variant_trace.write_bytes(variant_bytes)
    report["baseline_trace"] = _artifact_name(paths, paths.baseline_trace)
    report["variant_trace"] = _artifact_name(paths, paths.variant_trace)
    _write_json(paths.report, report)
    errors = _validate_report(manifest, paths, report)
    if errors:
        raise GateToolError("; ".join(errors))
    _archive_run(paths)
    _print_artifacts(paths)
    return 0


def _validate_report(
    manifest: LoadedManifest, paths: ArtifactPaths, report: dict[str, Any]
) -> list[str]:
    errors: list[str] = []
    evidence = report.get("evidence")
    status = report.get("status")
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
                if not isinstance(values.get(metric), (int, float)):
                    errors.append(f"report metrics.{side}.{metric} must be numeric")
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
                if metric in reported and not math.isclose(
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
            if status == "PASSED" and float(side_metrics.get("gpu_active_time_ms", 0.0)) <= 0:
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
            "manual",
            "ci",
            "gpu-manual",
            "gpu-runner",
        ],
    )
    fingerprint.set_defaults(func=cmd_fingerprint)
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
