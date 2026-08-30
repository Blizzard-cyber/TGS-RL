#!/usr/bin/env python3
"""Generate, ingest, and evaluate Gate G/I evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shlex
import shutil
import socket
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


def build_environment_fingerprint(*, execution_mode: str) -> dict[str, Any]:
    host_hash = hashlib.sha256(socket.gethostname().encode()).hexdigest()[:16]
    return {
        "captured_at": datetime.now(tz=UTC).isoformat(),
        "host_hash": host_hash,
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "git_commit": git_commit(),
        "execution_mode": execution_mode,
    }


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
            "throughput_items_per_s": throughput,
            "decision_count": float(measurement_runs),
        },
    }


def _archive_run(paths: ArtifactPaths) -> None:
    paths.archive.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(paths.archive, "w:gz") as handle:
        handle.add(paths.baseline_trace, arcname=paths.baseline_trace.name)
        handle.add(paths.variant_trace, arcname=paths.variant_trace.name)
        handle.add(paths.report, arcname=paths.report.name)


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
    run, _baseline, _variant = _base_run(
        manifest, paths, evidence="SIMULATED", simulated=True, execution_mode="simulator"
    )
    run["notes"] = ["Simulator output is synthetic and must not claim a real GPU gate pass."]
    _write_json(paths.report, run)
    _archive_run(paths)
    _print_artifacts(paths)
    return 0


def _run_smoke(command: str, timeout_seconds: float) -> tuple[dict[str, Any], bool]:
    argv = shlex.split(command)
    if not argv:
        raise GateToolError("smoke command must not be empty")
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
    return record, exit_code == 0


def cmd_cpu_smoke(args: argparse.Namespace) -> int:
    manifest = load_manifest(Path(args.manifest))
    paths = artifact_paths(manifest, Path(args.output_dir))
    run, baseline, variant = _base_run(
        manifest,
        paths,
        evidence="CPU_INTEGRATION",
        simulated=False,
        execution_mode="cpu-smoke",
    )
    baseline["metrics"].update(latency_ms_p50=101.0, throughput_items_per_s=180.0)
    variant["metrics"].update(latency_ms_p50=100.0, throughput_items_per_s=176.0)
    _write_json(paths.baseline_trace, baseline)
    _write_json(paths.variant_trace, variant)
    run["metrics"] = {"baseline": baseline["metrics"], "variant": variant["metrics"]}
    run["trace_capture"] = {
        "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
        "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
    }
    smoke, succeeded = _run_smoke(args.smoke_command, args.smoke_timeout)
    run["smoke"] = smoke
    run["status"] = "NOT_RUN" if succeeded else "BLOCKED"
    run["notes"] = ["CPU integration validates the workflow only; GPU gates remain not run."]
    _write_json(paths.report, run)
    _archive_run(paths)
    _print_artifacts(paths)
    return 0 if succeeded else 1


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
        if not fingerprint.get(field):
            raise GateToolError(f"environment_fingerprint missing required field {field}")
    baseline_source = _resolve_external_artifact(
        source, report.get("baseline_trace"), "baseline_trace"
    )
    variant_source = _resolve_external_artifact(
        source, report.get("variant_trace"), "variant_trace"
    )
    paths.baseline_trace.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(baseline_source, paths.baseline_trace)
    shutil.copy2(variant_source, paths.variant_trace)
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
    for field_name in ("baseline_trace", "variant_trace"):
        try:
            artifact = paths.root / _relative_path(report.get(field_name), field_name)
        except GateToolError as exc:
            errors.append(str(exc))
        else:
            if not artifact.is_file():
                errors.append(f"report artifact is missing: {field_name}")
    if report.get("simulated"):
        if evidence != "SIMULATED":
            errors.append("simulator reports must use SIMULATED evidence only")
        if status not in {"NOT_RUN", "INVALID"}:
            errors.append("simulator final status must be NOT_RUN or INVALID")
    if evidence == "CPU_INTEGRATION":
        smoke = report.get("smoke")
        if not isinstance(smoke, dict) or not smoke.get("executed") or smoke.get("exit_code") != 0:
            errors.append("CPU_INTEGRATION requires a successful executed smoke record")
        if status == "PASSED":
            errors.append("CPU_INTEGRATION cannot mark a GPU gate passed")
    if evidence in REAL_GPU_EVIDENCE:
        if report.get("simulated") is not False:
            errors.append("GPU evidence requires simulated=false")
        fingerprint = report.get("environment_fingerprint")
        if not isinstance(fingerprint, dict):
            errors.append("GPU evidence requires environment_fingerprint")
        else:
            for field in manifest.data["environment_fingerprint"]["required_fields"]:
                if not fingerprint.get(field):
                    errors.append(f"GPU evidence missing environment_fingerprint.{field}")
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
    ingest = subparsers.add_parser("ingest", help="Ingest an external run record")
    ingest.add_argument("--report", required=True)
    ingest.set_defaults(func=cmd_ingest)
    fingerprint = subparsers.add_parser("fingerprint", help="Print an environment fingerprint")
    fingerprint.add_argument(
        "--execution-mode",
        default="manual",
        choices=["simulator", "cpu-smoke", "manual", "ci", "gpu-manual"],
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
