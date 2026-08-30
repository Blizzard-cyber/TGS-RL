from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "gate-tools.py"
MANIFEST = ROOT / "configs" / "gates" / "gate-gi.json"


def run_tool(output_dir: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--manifest",
            str(MANIFEST),
            "--output-dir",
            str(output_dir),
            *arguments,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def read_report(output_dir: Path) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads((output_dir / "report.json").read_text(encoding="utf-8")),
    )


def test_simulator_generates_ephemeral_artifacts_without_gpu_claim(tmp_path: Path) -> None:
    simulated = run_tool(tmp_path, "simulate")
    assert simulated.returncode == 0, simulated.stderr
    report = read_report(tmp_path)
    assert report["evidence"] == "SIMULATED"
    assert report["simulated"] is True
    assert report["status"] == "NOT_RUN"
    assert (tmp_path / "artifacts" / "raw" / "archive.tar.gz").is_file()
    assert not (ROOT / "experiments").exists()


def test_report_and_evaluate_emit_machine_readable_summary(tmp_path: Path) -> None:
    assert run_tool(tmp_path, "simulate").returncode == 0

    report_result = run_tool(tmp_path, "report")
    assert report_result.returncode == 0, report_result.stderr
    rendered = json.loads(report_result.stdout)
    assert rendered["suite_id"] == "gate-g-i"
    assert rendered["summary"]["status"] == "NOT_RUN"

    evaluate_result = run_tool(tmp_path, "evaluate")
    assert evaluate_result.returncode == 0, evaluate_result.stderr
    assert json.loads(evaluate_result.stdout)["status"] == "NOT_RUN"


def test_replay_entry_is_derived_from_manifest(tmp_path: Path) -> None:
    result = run_tool(tmp_path, "replay-entry")
    assert result.returncode == 0, result.stderr
    replay_entry = json.loads(result.stdout)
    assert replay_entry["suite_id"] == "gate-g-i"
    assert replay_entry["scenario_manifest"] == "configs/scenarios/gate-gi.yaml"
    assert replay_entry["baseline_trace"].endswith("baseline-trace.json")


def test_invalid_manifest_rules_are_rejected(tmp_path: Path) -> None:
    bad_manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    bad_manifest["comparisons"]["measurement_runs"] = 0
    path = tmp_path / "bad-manifest.json"
    path.write_text(json.dumps(bad_manifest), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--manifest", str(path), "simulate"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "measurement_runs must be a positive integer" in result.stderr


def test_cpu_smoke_executes_command_and_records_only_digests(tmp_path: Path) -> None:
    result = run_tool(
        tmp_path,
        "cpu-smoke",
        "--smoke-command",
        f"{sys.executable} -c 'print(\"cpu smoke\")'",
    )
    assert result.returncode == 0, result.stderr

    report = read_report(tmp_path)
    smoke = report["smoke"]
    assert report["evidence"] == "CPU_INTEGRATION"
    assert report["status"] == "NOT_RUN"
    assert smoke["executed"] is True
    assert smoke["exit_code"] == 0
    assert "command" not in smoke


def test_cpu_smoke_failure_is_blocked_and_returns_failure(tmp_path: Path) -> None:
    result = run_tool(
        tmp_path,
        "cpu-smoke",
        "--smoke-command",
        f"{sys.executable} -c 'raise SystemExit(7)'",
    )
    assert result.returncode == 1
    report = read_report(tmp_path)
    assert report["status"] == "BLOCKED"
    assert report["smoke"]["exit_code"] == 7


def test_gpu_ingest_copies_external_artifacts_and_preserves_not_run(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "baseline.json").write_text('{"trace": "baseline"}', encoding="utf-8")
    (source_dir / "variant.json").write_text('{"trace": "variant"}', encoding="utf-8")
    report = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": "gate-g-i",
        "evidence": "GPU_SINGLE_NODE",
        "status": "NOT_RUN",
        "simulated": False,
        "baseline_trace": "baseline.json",
        "variant_trace": "variant.json",
        "environment_fingerprint": {
            "host_hash": "host-digest",
            "platform": "linux",
            "python_version": "3.12",
            "git_commit": "abc123",
            "execution_mode": "gpu-manual",
        },
        "metrics": {
            side: {
                "latency_ms_p50": 1.0,
                "latency_ms_p95": 2.0,
                "throughput_items_per_s": 3.0,
                "decision_count": 1.0,
            }
            for side in ("baseline", "variant")
        },
    }
    report_path = source_dir / "report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    ingested = run_tool(output_dir, "ingest", "--report", str(report_path))
    assert ingested.returncode == 0, ingested.stderr
    evaluated = run_tool(output_dir, "evaluate")
    assert evaluated.returncode == 0, evaluated.stderr
    assert json.loads(evaluated.stdout)["status"] == "NOT_RUN"
    stored = read_report(output_dir)
    assert stored["baseline_trace"] == "artifacts/traces/baseline-trace.json"
    assert (output_dir / stored["baseline_trace"]).is_file()
