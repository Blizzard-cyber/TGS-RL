from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "gate-tools.py"
MANIFEST = ROOT / "configs" / "gates" / "gate-gi.json"
FULL_STACK_MANIFEST = ROOT / "configs" / "gates" / "gate-gi-process.json"
CAMPAIGN = ROOT / "configs" / "gates" / "e1-e8.json"
SPEC = importlib.util.spec_from_file_location("tgsrl_gate_tools", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
GATE_TOOLS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE_TOOLS
SPEC.loader.exec_module(GATE_TOOLS)


def run_tool(output_dir: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return run_tool_with_manifest(MANIFEST, output_dir, *arguments)


def run_tool_with_manifest(
    manifest: Path, output_dir: Path, *arguments: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--manifest",
            str(manifest),
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


def campaign_result(reports_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-evaluate",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(reports_dir),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_full_stack_gpu_report(output: Path, experiment_id: str, profile: str) -> None:
    manifest = GATE_TOOLS.load_manifest(FULL_STACK_MANIFEST)
    paths = GATE_TOOLS.artifact_paths(manifest, output)
    paths.baseline_trace.parent.mkdir(parents=True)
    traces: dict[str, dict[str, Any]] = {}
    for label in ("baseline", "variant"):
        events: list[dict[str, Any]] = []
        for phase, count in (("warmup", 1), ("measurement", 3)):
            for iteration in range(1, count + 1):
                common = {
                    "experiment_id": experiment_id,
                    "label": label,
                    "phase": phase,
                    "iteration": iteration,
                    "device": "cuda",
                    "node_id": "gpu-node-1",
                    "service_job_id": f"job-{label}-{phase}-{iteration}",
                    "service_run_id": f"run-{label}-{phase}-{iteration}",
                }
                events.extend(
                    [
                        common
                        | {
                            "event_type": "sample_consumed",
                            "source": "worker",
                            "runtime_unit_id": "unit-1",
                            "worker_id": "worker-1",
                            "duration_ms": 1.0,
                            "gpu_active_ms": 2.0,
                            "useful_gpu_time_ms": 1.5,
                            "contract_observation": {
                                "policy_lag": 0,
                                "sample_stale": False,
                                "effective_sample_size": 1.0,
                            },
                        },
                        common
                        | {
                            "event_type": "workload_completed",
                            "source": "worker",
                            "runtime_unit_id": "unit-1",
                            "worker_id": "worker-1",
                            "elapsed_ms": 10.0,
                            "item_count": 1,
                            "convergence_quality": 1.0,
                        },
                        common
                        | {
                            "event_type": "decision_applied",
                            "source": "scheduler",
                            "action": "bind",
                            "decision_id": "decision-1",
                            "plan_id": "plan-1",
                            "duration_ms": 1.0,
                            "succeeded": True,
                        },
                    ]
                )
                events.extend(
                    [
                        common
                        | {
                            "event_type": "worker_registered",
                            "source": "worker",
                            "runtime_unit_id": "unit-1",
                            "worker_id": "worker-1",
                        },
                        common
                        | {
                            "event_type": "device_identity_verified",
                            "source": "worker",
                            "runtime_unit_id": "unit-1",
                            "worker_id": "worker-1",
                            "scheduler_device_ids": ["GPU-1"],
                            "allocated_device_ids": ["GPU-1"],
                            "worker_device_ids": ["GPU-1"],
                        },
                    ]
                )
                if label == "variant":
                    events.extend(
                        common
                        | {
                            "event_type": "decision_applied",
                            "source": "operator",
                            "action": action,
                            "duration_ms": 1.0,
                            "succeeded": True,
                        }
                        for action in ("pause", "resume")
                    )
        metrics = GATE_TOOLS._metrics_from_events(events)
        trace = {
            "schema_version": "tgsrl.io/gate-trace/v1alpha1",
            "suite_id": manifest.data["suite_id"],
            "label": label,
            "seed": manifest.data["workload_lock"]["seed"],
            "events": events,
            "metrics": metrics,
        }
        path = paths.baseline_trace if label == "baseline" else paths.variant_trace
        path.write_text(json.dumps(trace), encoding="utf-8")
        traces[label] = trace
    report = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": manifest.data["suite_id"],
        "experiment_id": experiment_id,
        "evidence": "GPU_SINGLE_NODE",
        "status": "PASSED",
        "simulated": False,
        "workload_lock": manifest.data["workload_lock"],
        "environment_fingerprint": {
            "host_hash": "host-digest",
            "platform": "linux",
            "python_version": "3.12",
            "git_commit": "abc123",
            "git_dirty": False,
            "execution_mode": "kubernetes-dra",
            "gpu_profile": profile,
            "accelerator_count": 1,
            "accelerator_inventory_digest": "gpu-inventory-digest",
        },
        "warmup_runs": manifest.data["comparisons"]["warmup_runs"],
        "measurement_runs": manifest.data["comparisons"]["measurement_runs"],
        "baseline_trace": paths.baseline_trace.relative_to(output).as_posix(),
        "variant_trace": paths.variant_trace.relative_to(output).as_posix(),
        "metrics": {label: trace["metrics"] for label, trace in traces.items()},
        "trace_capture": {
            "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
            "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
        },
        "executions": [{"executed": True, "exit_code": 0, "timed_out": False} for _ in range(8)],
    }
    service_log = output / "artifacts/services/control-plane/scheduler.log"
    service_log.parent.mkdir(parents=True)
    service_log.write_text("scheduler decision recorded\n", encoding="utf-8")
    report["service_log_artifacts"] = [
        {
            "path": service_log.relative_to(output).as_posix(),
            "sha256": hashlib.sha256(service_log.read_bytes()).hexdigest(),
        }
    ]
    campaign = json.loads(CAMPAIGN.read_text(encoding="utf-8"))
    experiment = next(
        item for item in campaign["experiments"] if item["experiment_id"] == experiment_id
    )
    scenario_source = ROOT / experiment["scenario_manifest"]
    scenario = output / "artifacts/config" / scenario_source.name
    scenario.parent.mkdir(parents=True, exist_ok=True)
    scenario.write_bytes(scenario_source.read_bytes())
    report["campaign_scenario"] = {
        "path": scenario.relative_to(output).as_posix(),
        "sha256": hashlib.sha256(scenario.read_bytes()).hexdigest(),
    }
    paths.report.write_text(json.dumps(report), encoding="utf-8")


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


def test_campaign_defines_e1_through_e8_and_missing_evidence_is_not_run(
    tmp_path: Path,
) -> None:
    planned = subprocess.run(
        [sys.executable, str(SCRIPT), "campaign-plan", "--campaign", str(CAMPAIGN)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert planned.returncode == 0, planned.stderr
    plan = json.loads(planned.stdout)
    assert [item["experiment_id"] for item in plan["experiments"]] == [
        f"E{index}" for index in range(1, 9)
    ]

    evaluated = campaign_result(tmp_path)
    assert evaluated.returncode == 0, evaluated.stderr
    result = json.loads(evaluated.stdout)
    assert result["status"] == "NOT_RUN"
    assert {item["status"] for item in result["experiments"]} == {"NOT_RUN"}


def test_campaign_does_not_promote_cpu_evidence_to_gpu_pass(tmp_path: Path) -> None:
    output = tmp_path / "e1-full-gpu"
    simulated = run_tool_with_manifest(FULL_STACK_MANIFEST, output, "simulate")
    assert simulated.returncode == 0, simulated.stderr
    report = read_report(output)
    report["experiment_id"] = "E1"
    report["evidence"] = "CPU_INTEGRATION"
    report["simulated"] = False
    report["smoke"] = {"executed": True, "exit_code": 0}
    report["environment_fingerprint"]["execution_mode"] = "cpu-full-stack"
    (output / "report.json").write_text(json.dumps(report), encoding="utf-8")

    evaluated = campaign_result(tmp_path)
    assert evaluated.returncode == 0, evaluated.stderr
    e1 = json.loads(evaluated.stdout)["experiments"][0]
    assert e1["status"] == "INVALID"
    assert "requires at least GPU_SINGLE_NODE evidence" in e1["blockers"]


def test_campaign_blocks_uncalibrated_threshold_and_rejects_missing_fault_evidence(
    tmp_path: Path,
) -> None:
    _write_full_stack_gpu_report(tmp_path / "e3-throughput-vug", "E3", "full-gpu")
    _write_full_stack_gpu_report(tmp_path / "e7-recovery", "E7", "full-gpu")

    evaluated = campaign_result(tmp_path)
    assert evaluated.returncode == 0, evaluated.stderr
    by_id = {item["experiment_id"]: item for item in json.loads(evaluated.stdout)["experiments"]}
    assert by_id["E3"]["status"] == "BLOCKED"
    assert any(rule.get("reason") == "calibration_required" for rule in by_id["E3"]["rules"])
    assert by_id["E7"]["status"] == "INVALID"
    assert any("required fault worker-exit" in blocker for blocker in by_id["E7"]["blockers"])


def test_campaign_accepts_complete_exact_device_evidence(tmp_path: Path) -> None:
    _write_full_stack_gpu_report(tmp_path / "e1-full-gpu", "E1", "full-gpu")

    evaluated = campaign_result(tmp_path)

    assert evaluated.returncode == 0, evaluated.stderr
    e1 = json.loads(evaluated.stdout)["experiments"][0]
    assert e1["status"] == "PASSED"
    assert e1["blockers"] == []


def test_campaign_rejects_device_identity_mismatch(tmp_path: Path) -> None:
    output = tmp_path / "e1-full-gpu"
    _write_full_stack_gpu_report(output, "E1", "full-gpu")
    report = read_report(output)
    variant_path = output / report["variant_trace"]
    variant = json.loads(variant_path.read_text(encoding="utf-8"))
    identity = next(
        event
        for event in variant["events"]
        if event["phase"] == "measurement" and event["event_type"] == "device_identity_verified"
    )
    identity["worker_device_ids"] = ["GPU-other"]
    variant_path.write_text(json.dumps(variant), encoding="utf-8")
    report["trace_capture"]["variant_digest"] = hashlib.sha256(
        variant_path.read_bytes()
    ).hexdigest()
    (output / "report.json").write_text(json.dumps(report), encoding="utf-8")

    evaluated = campaign_result(tmp_path)

    assert evaluated.returncode == 0, evaluated.stderr
    e1 = json.loads(evaluated.stdout)["experiments"][0]
    assert e1["status"] == "INVALID"
    assert any(
        "scheduler, allocation, and worker device identities differ" in blocker
        for blocker in e1["blockers"]
    )


def test_campaign_ingest_rejects_wrong_experiment_identity(tmp_path: Path) -> None:
    output = tmp_path / "source"
    _write_full_stack_gpu_report(output, "E2", "full-gpu")
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-ingest",
            "E1",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(tmp_path / "reports"),
            "--report",
            str(output / "report.json"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "experiment_id does not match" in result.stderr


def test_campaign_ingest_rejects_cpu_evidence(tmp_path: Path) -> None:
    source = tmp_path / "source"
    simulated = run_tool_with_manifest(FULL_STACK_MANIFEST, source, "simulate")
    assert simulated.returncode == 0, simulated.stderr
    report = read_report(source)
    report["experiment_id"] = "E1"
    report["evidence"] = "CPU_INTEGRATION"
    report["simulated"] = False
    (source / "report.json").write_text(json.dumps(report), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-ingest",
            "E1",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(tmp_path / "reports"),
            "--report",
            str(source / "report.json"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "requires at least GPU_SINGLE_NODE evidence" in result.stderr


def test_campaign_ingest_copies_self_contained_evidence(tmp_path: Path) -> None:
    source = tmp_path / "source"
    reports = tmp_path / "reports"
    _write_full_stack_gpu_report(source, "E1", "full-gpu")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-ingest",
            "E1",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(reports),
            "--report",
            str(source / "report.json"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    target = reports / "e1-full-gpu"
    report = read_report(target)
    assert (target / report["campaign_scenario"]["path"]).is_file()
    assert all((target / record["path"]).is_file() for record in report["service_log_artifacts"])
    evaluated = campaign_result(reports)
    assert evaluated.returncode == 0, evaluated.stderr
    assert json.loads(evaluated.stdout)["experiments"][0]["status"] == "PASSED"


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

    bad_manifest["comparisons"]["measurement_runs"] = 1
    bad_manifest["rules"][0]["operator"] = "<="
    path.write_text(json.dumps(bad_manifest), encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--manifest", str(path), "simulate"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 1
    assert "must use the supported >= operator" in result.stderr

    bad_manifest["rules"][0]["operator"] = ">="
    bad_manifest["rules"][0]["metric"] = "undeclared_metric"
    path.write_text(json.dumps(bad_manifest), encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--manifest", str(path), "simulate"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 1
    assert "references an undeclared metric" in result.stderr


def test_runner_rejects_repository_root_as_output_directory() -> None:
    result = run_tool(ROOT, "cpu-smoke")

    assert result.returncode == 1
    assert "output directory is too broad" in result.stderr


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
    assert len(report["executions"]) == 8
    assert report["metrics"]["baseline"]["decision_count"] == 0
    assert report["metrics"]["variant"]["decision_count"] == 15
    baseline = json.loads((tmp_path / report["baseline_trace"]).read_text())
    variant = json.loads((tmp_path / report["variant_trace"]).read_text())
    assert baseline["events"]
    assert variant["events"]
    assert all(event["phase"] in {"warmup", "measurement"} for event in variant["events"])
    assert (tmp_path / "artifacts/config/gate-manifest.json").is_file()
    assert (tmp_path / "artifacts/config/gate-gi.yaml").is_file()
    assert (tmp_path / report["executions"][0]["stdout_artifact"]).is_file()


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
    rendered = run_tool(tmp_path, "report")
    assert rendered.returncode == 0, rendered.stderr
    assert json.loads(rendered.stdout)["summary"]["status"] == "BLOCKED"


def test_full_stack_gate_rejects_process_local_reference_events(tmp_path: Path) -> None:
    manifest = json.loads(FULL_STACK_MANIFEST.read_text(encoding="utf-8"))
    manifest["comparisons"] = {
        "baseline": "baseline",
        "variant": "variant",
        "warmup_runs": 0,
        "measurement_runs": 1,
    }
    manifest["workload_runner"]["argv"] = [
        "{python}",
        "scripts/verl-reference-workload.py",
    ]
    path = tmp_path / "full-stack.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    result = run_tool_with_manifest(
        path,
        tmp_path / "output",
        "cpu-smoke",
        "--smoke-command",
        f"{sys.executable} -c 'print(1)'",
    )

    assert result.returncode == 1
    assert "must identify exactly one service run/job" in result.stderr


def test_full_stack_report_rejects_missing_or_tampered_service_logs(tmp_path: Path) -> None:
    manifest = GATE_TOOLS.load_manifest(FULL_STACK_MANIFEST)
    paths = GATE_TOOLS.artifact_paths(manifest, tmp_path)
    paths.baseline_trace.parent.mkdir(parents=True)
    events: list[dict[str, object]] = []
    for label in ("baseline", "variant"):
        for phase, count in (("warmup", 1), ("measurement", 3)):
            for iteration in range(1, count + 1):
                common = {
                    "label": label,
                    "phase": phase,
                    "iteration": iteration,
                    "service_run_id": f"run-{label}-{phase}-{iteration}",
                    "service_job_id": f"job-{label}-{phase}-{iteration}",
                }
                events.extend(
                    [
                        common
                        | {
                            "event_type": "sample_consumed",
                            "source": "worker",
                            "runtime_unit_id": "unit-1",
                            "worker_id": "worker-1",
                            "duration_ms": 1.0,
                            "contract_observation": {},
                        },
                        common
                        | {
                            "event_type": "workload_completed",
                            "source": "worker",
                            "runtime_unit_id": "unit-1",
                            "worker_id": "worker-1",
                            "elapsed_ms": 2.0,
                            "item_count": 1,
                        },
                        common
                        | {
                            "event_type": "decision_applied",
                            "source": "scheduler",
                            "decision_id": "decision-1",
                            "plan_id": "plan-1",
                            "duration_ms": 1.0,
                            "succeeded": True,
                        },
                    ]
                )
                if label == "variant":
                    events.extend(
                        common
                        | {
                            "event_type": "decision_applied",
                            "source": "operator",
                            "action": action,
                            "duration_ms": 1.0,
                            "succeeded": True,
                        }
                        for action in ("pause", "resume")
                    )
    by_label = {
        label: [event for event in events if event["label"] == label]
        for label in ("baseline", "variant")
    }
    for label, path in (("baseline", paths.baseline_trace), ("variant", paths.variant_trace)):
        path.write_text(
            json.dumps(
                {
                    "schema_version": "tgsrl.io/gate-trace/v1alpha1",
                    "suite_id": manifest.data["suite_id"],
                    "label": label,
                    "seed": manifest.data["workload_lock"]["seed"],
                    "events": by_label[label],
                    "metrics": GATE_TOOLS._metrics_from_events(by_label[label]),
                }
            ),
            encoding="utf-8",
        )
    log = tmp_path / "artifacts/services/control-plane/scheduler.log"
    log.parent.mkdir(parents=True)
    log.write_text("decision recorded\n", encoding="utf-8")
    metrics = {label: GATE_TOOLS._metrics_from_events(by_label[label]) for label in by_label}
    report = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": manifest.data["suite_id"],
        "evidence": "CPU_INTEGRATION",
        "status": "NOT_RUN",
        "simulated": False,
        "workload_lock": manifest.data["workload_lock"],
        "environment_fingerprint": {"execution_mode": "cpu-full-stack"},
        "warmup_runs": 1,
        "measurement_runs": 3,
        "baseline_trace": paths.baseline_trace.relative_to(tmp_path).as_posix(),
        "variant_trace": paths.variant_trace.relative_to(tmp_path).as_posix(),
        "metrics": metrics,
        "trace_capture": {
            "baseline_digest": hashlib.sha256(paths.baseline_trace.read_bytes()).hexdigest(),
            "variant_digest": hashlib.sha256(paths.variant_trace.read_bytes()).hexdigest(),
        },
        "smoke": {"executed": True, "exit_code": 0},
        "executions": [{"executed": True, "exit_code": 0, "timed_out": False} for _ in range(8)],
        "service_log_artifacts": [
            {
                "path": log.relative_to(tmp_path).as_posix(),
                "sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
            }
        ],
    }
    paths.report.write_text(json.dumps(report), encoding="utf-8")
    assert run_tool_with_manifest(FULL_STACK_MANIFEST, tmp_path, "report").returncode == 0

    log.write_text("tampered\n", encoding="utf-8")
    result = run_tool_with_manifest(FULL_STACK_MANIFEST, tmp_path, "report")
    assert result.returncode == 1
    assert "service log artifact 0 digest does not match" in result.stderr


def test_gpu_ingest_copies_external_artifacts_and_preserves_not_run(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    manifest = json.loads(MANIFEST.read_text())

    def trace(label: str) -> dict[str, Any]:
        events = [
            {
                "label": label,
                "phase": "measurement",
                "iteration": 1,
                "event_type": "sample_consumed",
                "duration_ms": 1.0,
                "gpu_active_ms": 2.0,
                "buffer_level": 1,
                "node_id": "gpu-node-1",
                "contract_observation": {
                    "policy_lag": 0,
                    "sample_stale": False,
                    "effective_sample_size": 1.0,
                },
            },
            *[
                {
                    "label": label,
                    "phase": "measurement",
                    "iteration": 1,
                    "event_type": "decision_applied",
                    "action": action,
                    "duration_ms": 1.0,
                    "succeeded": True,
                    "rolled_back": False,
                    "recovery_time_ms": 0.0,
                    "node_id": "gpu-node-1",
                }
                for action in ("prepare_pause", "checkpoint", "reload")
            ],
            {
                "label": label,
                "phase": "measurement",
                "iteration": 1,
                "event_type": "workload_completed",
                "elapsed_ms": 10.0,
                "item_count": 1,
                "node_id": "gpu-node-1",
            },
        ]
        metrics = {
            "latency_ms_p50": 1.0,
            "latency_ms_p95": 1.0,
            "latency_ms_p99": 1.0,
            "throughput_items_per_s": 100.0,
            "decision_count": 3.0,
            "end_to_end_iteration_ms_p50": 10.0,
            "scheduling_latency_ms_p95": 1.0,
            "gpu_active_time_ms": 2.0,
            "valuable_useful_gpu_ratio": 0.0,
            "interference_ratio": 0.0,
            "convergence_quality": 0.0,
            "queue_depth_max": 1.0,
            "policy_lag_p95": 0.0,
            "sample_staleness_ratio": 0.0,
            "effective_sample_size_mean": 1.0,
            "pause_latency_ms": 1.0,
            "checkpoint_latency_ms": 1.0,
            "reload_latency_ms": 1.0,
            "action_success_rate": 1.0,
            "rollback_rate": 0.0,
            "transaction_recovery_time_ms": 0.0,
        }
        return {
            "schema_version": "tgsrl.io/gate-trace/v1alpha1",
            "suite_id": "gate-g-i",
            "label": label,
            "seed": manifest["workload_lock"]["seed"],
            "events": events,
            "metrics": metrics,
        }

    baseline, variant = trace("baseline"), trace("variant")
    baseline_path, variant_path = source_dir / "baseline.json", source_dir / "variant.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    variant_path.write_text(json.dumps(variant), encoding="utf-8")
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
            "git_dirty": False,
            "execution_mode": "gpu-manual",
            "accelerator_count": 1,
            "accelerator_inventory_digest": "gpu-inventory-digest",
        },
        "workload_lock": manifest["workload_lock"],
        "warmup_runs": manifest["comparisons"]["warmup_runs"],
        "measurement_runs": manifest["comparisons"]["measurement_runs"],
        "metrics": {
            "baseline": baseline["metrics"],
            "variant": variant["metrics"],
        },
        "trace_capture": {
            "baseline_digest": hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
            "variant_digest": hashlib.sha256(variant_path.read_bytes()).hexdigest(),
        },
        "executions": [
            {
                "executed": True,
                "exit_code": 0,
                "timed_out": False,
                "label": label,
                "phase": phase,
                "iteration": iteration,
            }
            for label in ("baseline", "variant")
            for phase, count in (("warmup", 1), ("measurement", 3))
            for iteration in range(1, count + 1)
        ],
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


def test_gpu_ingest_rejects_tampered_trace_digest(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    baseline = source_dir / "baseline.json"
    variant = source_dir / "variant.json"
    baseline.write_text("{}", encoding="utf-8")
    variant.write_text("{}", encoding="utf-8")
    manifest = json.loads(MANIFEST.read_text())
    report = {
        "schema_version": "tgsrl.io/gate-run-record/v1alpha1",
        "suite_id": "gate-g-i",
        "evidence": "GPU_SINGLE_NODE",
        "status": "NOT_RUN",
        "simulated": False,
        "baseline_trace": baseline.name,
        "variant_trace": variant.name,
        "environment_fingerprint": {
            "host_hash": "host",
            "platform": "linux",
            "python_version": "3.12",
            "git_commit": "commit",
            "git_dirty": False,
            "execution_mode": "gpu-manual",
        },
        "workload_lock": manifest["workload_lock"],
        "warmup_runs": manifest["comparisons"]["warmup_runs"],
        "measurement_runs": manifest["comparisons"]["measurement_runs"],
        "metrics": {"baseline": {}, "variant": {}},
        "trace_capture": {"baseline_digest": "tampered", "variant_digest": "tampered"},
    }
    report_path = source_dir / "report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    result = run_tool(tmp_path / "output", "ingest", "--report", str(report_path))

    assert result.returncode == 1
    assert "trace digest does not match" in result.stderr


def test_ingest_can_replace_report_in_its_output_directory(tmp_path: Path) -> None:
    output = tmp_path / "output"
    initial = run_tool(output, "cpu-smoke", "--smoke-command", f"{sys.executable} -c 'print(1)'")
    assert initial.returncode == 0, initial.stderr
    report = read_report(output)
    report["baseline_trace"] = str((output / report["baseline_trace"]).resolve())
    report["variant_trace"] = str((output / report["variant_trace"]).resolve())
    source_report = output / "report.json"
    source_report.write_text(json.dumps(report), encoding="utf-8")

    ingested = run_tool(output, "ingest", "--report", str(source_report))

    assert ingested.returncode == 0, ingested.stderr
    assert read_report(output)["evidence"] == "CPU_INTEGRATION"


def test_report_rejects_non_finite_and_boolean_metrics(tmp_path: Path) -> None:
    generated = run_tool(
        tmp_path,
        "cpu-smoke",
        "--smoke-command",
        f"{sys.executable} -c 'print(1)'",
    )
    assert generated.returncode == 0, generated.stderr

    report = read_report(tmp_path)
    report["metrics"]["baseline"]["latency_ms_p50"] = float("nan")
    report["metrics"]["variant"]["latency_ms_p50"] = True
    (tmp_path / "report.json").write_text(json.dumps(report), encoding="utf-8")

    result = run_tool(tmp_path, "report")

    assert result.returncode == 1
    assert "metrics.baseline.latency_ms_p50 must be a finite number" in result.stderr
    assert "metrics.variant.latency_ms_p50 must be a finite number" in result.stderr
