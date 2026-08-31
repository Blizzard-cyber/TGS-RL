from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "gate-tools.py"
HARDWARE_EXECUTOR = ROOT / "scripts" / "hardware-campaign-executor.py"
MANIFEST = ROOT / "configs" / "gates" / "gate-gi.json"
FULL_STACK_MANIFEST = ROOT / "configs" / "gates" / "gate-gi-process.json"
HARDWARE_MANIFEST = ROOT / "configs" / "gates" / "gate-e1-e8-hardware.json"
CAMPAIGN = ROOT / "configs" / "gates" / "e1-e8.json"
SPEC = importlib.util.spec_from_file_location("tgsrl_gate_tools", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
GATE_TOOLS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE_TOOLS
SPEC.loader.exec_module(GATE_TOOLS)
EXECUTOR_SPEC = importlib.util.spec_from_file_location(
    "tgsrl_hardware_campaign_executor", HARDWARE_EXECUTOR
)
assert EXECUTOR_SPEC is not None and EXECUTOR_SPEC.loader is not None
HARDWARE_TOOLS = importlib.util.module_from_spec(EXECUTOR_SPEC)
sys.modules[EXECUTOR_SPEC.name] = HARDWARE_TOOLS
EXECUTOR_SPEC.loader.exec_module(HARDWARE_TOOLS)


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


def _write_full_stack_gpu_report(
    output: Path, experiment_id: str, profile: str, *, complete_requirements: bool = False
) -> None:
    manifest = GATE_TOOLS.load_manifest(HARDWARE_MANIFEST)
    paths = GATE_TOOLS.artifact_paths(manifest, output)
    paths.baseline_trace.parent.mkdir(parents=True)
    campaign = json.loads(CAMPAIGN.read_text(encoding="utf-8"))
    experiment = next(
        item for item in campaign["experiments"] if item["experiment_id"] == experiment_id
    )
    requirements = experiment["requirements"]
    node_count = requirements["minimum_nodes"] if complete_requirements else 1
    nodes = [f"gpu-node-{index}" for index in range(1, node_count + 1)]
    device_id = "MIG-1/1/0" if profile == "mig" else "GPU-1"
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
                    "node_id": nodes[0],
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
                            "scheduler_device_ids": [device_id],
                            "allocated_device_ids": [device_id],
                            "worker_device_ids": [device_id],
                            "device_class": (
                                "mig.nvidia.com" if profile == "mig" else "gpu.nvidia.com"
                            ),
                            "parent_uuid": "GPU-parent" if profile == "mig" else "",
                        },
                    ]
                )
                for node_index, node_id in enumerate(nodes[1:], start=2):
                    extra_device_id = f"GPU-{node_index}"
                    events.append(
                        common
                        | {
                            "event_type": "device_identity_verified",
                            "source": "worker",
                            "runtime_unit_id": f"unit-{node_index}",
                            "worker_id": f"worker-{node_index}",
                            "node_id": node_id,
                            "scheduler_device_ids": [extra_device_id],
                            "allocated_device_ids": [extra_device_id],
                            "worker_device_ids": [extra_device_id],
                            "device_class": "gpu.nvidia.com",
                            "parent_uuid": "",
                        }
                    )
                if label == "variant":
                    actions = (
                        sorted({"pause", "resume", *requirements["required_actions"]})
                        if complete_requirements
                        else ["pause", "resume"]
                    )
                    events.extend(
                        common
                        | {
                            "event_type": "decision_applied",
                            "source": "operator",
                            "action": action,
                            "duration_ms": 1.0,
                            "succeeded": True,
                            "receipt_id": f"receipt-{action}" if complete_requirements else "",
                            "transaction_id": (
                                f"transaction-{iteration}" if complete_requirements else ""
                            ),
                            "ready": complete_requirements,
                            "observed_share": 0.5 if action == "set_share" else None,
                            "observed_priority": 1 if action == "set_priority" else None,
                        }
                        for action in actions
                    )
                    if complete_requirements:
                        events.extend(
                            common
                            | {
                                "event_type": "control_started",
                                "source": "operator",
                                "action": action,
                            }
                            for action in requirements["required_actions"]
                        )
                        for event_type in requirements["required_events"]:
                            if event_type in {
                                "sample_consumed",
                                "workload_completed",
                                "worker_registered",
                                "device_identity_verified",
                                "fault_injected",
                                "fault_recovered",
                            }:
                                continue
                            events.append(
                                common
                                | {
                                    "event_type": event_type,
                                    "source": "worker",
                                    "runtime_unit_id": "unit-1",
                                    "worker_id": "worker-1",
                                    "interference_ratio": 0.1,
                                }
                            )
                        for fault_id in requirements["required_faults"]:
                            events.extend(
                                [
                                    common
                                    | {
                                        "event_type": "fault_injected",
                                        "source": "operator",
                                        "fault_id": fault_id,
                                    },
                                    common
                                    | {
                                        "event_type": "fault_recovered",
                                        "source": "operator",
                                        "fault_id": fault_id,
                                        "recovery_time_ms": 1.0,
                                    },
                                ]
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
        "evidence": (
            experiment["minimum_evidence"] if complete_requirements else "GPU_SINGLE_NODE"
        ),
        "status": "PASSED",
        "simulated": False,
        "workload_lock": manifest.data["workload_lock"],
        "environment_fingerprint": {
            "host_hash": "host-digest",
            "platform": "linux",
            "python_version": "3.12",
            "git_commit": GATE_TOOLS.git_commit(),
            "git_dirty": False,
            "execution_mode": "kubernetes-dra",
            "gpu_profile": profile,
            "accelerator_count": (
                requirements["minimum_accelerators"] if complete_requirements else 1
            ),
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
        "orchestrator": {
            "schema_version": "tgsrl.io/hardware-orchestrator/v1alpha1",
            "executor_sha256": "fixture-executor",
            "driver_sha256": "fixture-driver",
            "scenario_sha256": hashlib.sha256(
                (ROOT / experiment["scenario_manifest"]).read_bytes()
            ).hexdigest(),
        },
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
    scenario_source = ROOT / experiment["scenario_manifest"]
    scenario = output / "artifacts/config" / scenario_source.name
    scenario.parent.mkdir(parents=True, exist_ok=True)
    scenario.write_bytes(scenario_source.read_bytes())
    report["campaign_scenario"] = {
        "path": scenario.relative_to(output).as_posix(),
        "sha256": hashlib.sha256(scenario.read_bytes()).hexdigest(),
    }
    paths.report.write_text(json.dumps(report), encoding="utf-8")


def _write_hardware_driver(
    path: Path,
    calls: Path,
    *,
    fail_operation: str = "",
    fail_experiment: str = "",
    invalid_cleanup: bool = False,
    preflight_inventory_digest: str = "gpu-inventory-digest",
    preflight_accelerator_count: object = 2,
) -> None:
    path.write_text(
        f"""#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--request", required=True)
parser.add_argument("--response", required=True)
args = parser.parse_args()
request = json.loads(Path(args.request).read_text(encoding="utf-8"))
with Path({str(calls)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(request, sort_keys=True) + "\\n")
if (
    request["operation"] == {fail_operation!r}
    and (not {fail_experiment!r} or request["experiment_id"] == {fail_experiment!r})
):
    raise SystemExit(7)
operation = request["operation"]
events = []
if operation == "preflight":
    profile = "mig" if request["experiment_id"] == "E2" else "full-gpu"
    response = {{
        "schema_version": "tgsrl.io/hardware-driver-response/v1alpha1",
        "request_id": request["request_id"], "status": "SUCCEEDED", "events": [],
        "environment_fingerprint": {{
            "host_hash": "host-digest", "platform": "linux",
            "python_version": "3.12", "git_commit": {str(GATE_TOOLS.git_commit())!r},
            "git_dirty": False, "execution_mode": "kubernetes-dra",
            "gpu_profile": profile, "accelerator_count": {preflight_accelerator_count!r},
            "accelerator_inventory_digest": {preflight_inventory_digest!r},
        }},
    }}
    Path(args.response).write_text(json.dumps(response), encoding="utf-8")
    raise SystemExit(0)
common = {{
    "service_job_id": "job-" + request["run_key"],
    "service_run_id": "run-" + request["run_key"],
    "device": "cuda",
    "node_id": "gpu-node-1",
    "runtime_unit_id": "unit-1",
    "worker_id": "worker-1",
}}
if request["experiment_id"] == "E8":
    nodes = ["gpu-node-1", "gpu-node-2"]
else:
    nodes = ["gpu-node-1"]
def for_nodes(event):
    return [event | {{"node_id": node}} for node in nodes]
if operation == "launch":
    events.extend(for_nodes(common | {{"event_type": "worker_registered", "source": "worker"}}))
elif operation == "verify_device_identity":
    profile = "mig" if request["experiment_id"] == "E2" else "full-gpu"
    for node_index, node in enumerate(nodes, start=1):
        device = f"MIG-{{node_index}}/1/0" if profile == "mig" else f"GPU-{{node_index}}"
        events.append(common | {{
            "event_type": "device_identity_verified", "source": "worker",
            "node_id": node,
            "scheduler_device_ids": [device], "allocated_device_ids": [device],
            "worker_device_ids": [device],
            "device_class": "mig.nvidia.com" if profile == "mig" else "gpu.nvidia.com",
            "parent_uuid": f"GPU-parent-{{node_index}}" if profile == "mig" else "",
        }})
elif operation == "apply_action":
    events.append(common | {{
        "event_type": "decision_applied",
        "source": "scheduler" if request["action"] == "bind" else "operator",
        "action": request["action"], "duration_ms": 1.0, "succeeded": True,
        "decision_id": "decision-" + request["request_id"],
        "plan_id": "plan-" + request["request_id"],
        "receipt_id": "receipt-" + request["request_id"],
        "transaction_id": "transaction-" + request["run_key"], "ready": True,
        "observed_share": 0.5 if request["action"] == "set_share" else None,
        "observed_priority": 1 if request["action"] == "set_priority" else None,
    }})
    events.append(common | {{
        "event_type": "control_started", "source": "operator",
        "action": request["action"],
    }})
elif operation == "inject_fault":
    events.append(common | {{
        "event_type": "fault_injected", "source": "operator",
        "fault_id": request["fault_id"],
    }})
    if request["fault_id"] == "colocation-interference":
        events.append(common | {{
            "event_type": "interference_observed", "source": "worker",
            "interference_ratio": 0.1,
        }})
elif operation == "recover_fault":
    events.append(common | {{
        "event_type": "fault_recovered", "source": "operator",
        "fault_id": request["fault_id"], "recovery_time_ms": 1.0,
    }})
elif operation == "measure":
    for node in nodes:
        events.extend([
        common | {{
            "event_type": "sample_consumed", "source": "worker",
            "node_id": node,
            "duration_ms": 1.0, "gpu_active_ms": 2.0, "useful_gpu_time_ms": 1.5,
            "contract_observation": {{
                "policy_lag": 0, "sample_stale": False,
                "effective_sample_size": 1.0,
            }},
        }},
        common | {{
            "event_type": "workload_completed", "source": "worker",
            "node_id": node, "elapsed_ms": 10.0, "item_count": 1,
            "convergence_quality": 1.0,
        }},
        ])
response = {{
    "schema_version": "tgsrl.io/hardware-driver-response/v1alpha1",
    "request_id": request["request_id"],
    "status": ("FAILED" if {invalid_cleanup!r} and operation == "cleanup" else "SUCCEEDED"),
    "events": events,
}}
Path(args.response).write_text(json.dumps(response), encoding="utf-8")
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


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


def test_campaign_run_executes_all_scenarios_and_ingests_evidence(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    campaign = json.loads(CAMPAIGN.read_text(encoding="utf-8"))
    driver, calls = tmp_path / "driver.py", tmp_path / "calls.ndjson"
    _write_hardware_driver(driver, calls)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-run",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(reports),
            "--driver",
            str(driver),
            "--timeout-seconds",
            "10",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout)
    assert summary["status"] == "BLOCKED"
    called = [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()]
    assert list(dict.fromkeys(record["experiment_id"] for record in called)) == [
        f"E{index}" for index in range(1, 9)
    ]
    measurement_plans: dict[str, list[str]] = {}
    for experiment_id in ("E2", "E4", "E5", "E6", "E7", "E8"):
        measurement_plans[experiment_id] = [
            (
                f"{record['operation']}:{record['action']}"
                if record.get("action")
                else f"{record['operation']}:{record['fault_id']}"
                if record.get("fault_id")
                else str(record["operation"])
            )
            for record in called
            if record.get("experiment_id") == experiment_id
            and record.get("label") == "variant"
            and record.get("phase") == "measurement"
            and record.get("iteration") == 1
        ]
    assert measurement_plans == {
        "E2": [
            "provision",
            "apply_action:bind",
            "launch",
            "apply_action:rebind",
            "verify_device_identity",
            "measure",
            "stop",
            "cleanup",
        ],
        "E4": [
            "provision",
            "apply_action:bind",
            "launch",
            "verify_device_identity",
            "inject_fault:staleness-pressure",
            "apply_action:set_share",
            "measure",
            "recover_fault:staleness-pressure",
            "stop",
            "cleanup",
        ],
        "E5": [
            "provision",
            "apply_action:bind",
            "launch",
            "verify_device_identity",
            "inject_fault:colocation-interference",
            "apply_action:set_share",
            "apply_action:set_priority",
            "measure",
            "recover_fault:colocation-interference",
            "stop",
            "cleanup",
        ],
        "E6": [
            "provision",
            "apply_action:bind",
            "launch",
            "verify_device_identity",
            "apply_action:pause",
            "apply_action:checkpoint",
            "apply_action:offload",
            "apply_action:reload",
            "apply_action:resume",
            "measure",
            "stop",
            "cleanup",
        ],
        "E7": [
            "provision",
            "apply_action:bind",
            "launch",
            "verify_device_identity",
            "inject_fault:worker-exit",
            "apply_action:rollback",
            "recover_fault:worker-exit",
            "inject_fault:control-response-loss",
            "apply_action:resume",
            "recover_fault:control-response-loss",
            "measure",
            "stop",
            "cleanup",
        ],
        "E8": [
            "provision",
            "apply_action:bind",
            "launch",
            "verify_device_identity",
            "inject_fault:node-loss",
            "apply_action:pause",
            "recover_fault:node-loss",
            "apply_action:resume",
            "measure",
            "stop",
            "cleanup",
        ],
    }
    execution = json.loads((reports / "campaign-execution.json").read_text(encoding="utf-8"))
    assert len(execution["executions"]) == 8
    assert execution["status"] == "BLOCKED"
    assert all(record["status"] == "SUCCEEDED" for record in execution["executions"])
    evaluated = json.loads((reports / "campaign-report.json").read_text(encoding="utf-8"))
    by_id = {item["experiment_id"]: item for item in evaluated["experiments"]}
    assert by_id["E1"]["status"] == "PASSED"
    assert by_id["E2"]["status"] == "PASSED"
    assert by_id["E3"]["status"] == "BLOCKED"
    assert {item["status"] for item in evaluated["experiments"]} <= {"PASSED", "BLOCKED"}
    for experiment in campaign["experiments"]:
        output = reports / experiment["output_directory"]
        report = read_report(output)
        assert report["campaign_execution"]["status"] == "SUCCEEDED"
        assert (output / "artifacts/services/campaign-executor/stdout.log").is_file()
        assert (output / "artifacts/raw/archive.tar.gz").is_file()

    strict = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-run",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(reports),
            "--experiment",
            "E1",
            "--driver",
            str(driver),
            "--timeout-seconds",
            "10",
            "--require-pass",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert strict.returncode == 1
    assert "--require-pass requires executing the complete E1-E8 campaign" in strict.stderr
    strict_execution = json.loads((reports / "campaign-execution.json").read_text(encoding="utf-8"))
    assert strict_execution["status"] == "FAILED"


def test_campaign_run_stops_on_executor_failure_and_keeps_diagnostics(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    driver, calls = tmp_path / "driver.py", tmp_path / "calls.ndjson"
    _write_hardware_driver(driver, calls, fail_operation="preflight", fail_experiment="E2")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-run",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(reports),
            "--driver",
            str(driver),
            "--timeout-seconds",
            "10",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "campaign executor failed for E2 with exit 1" in result.stderr
    called = [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()]
    assert list(dict.fromkeys(record["experiment_id"] for record in called)) == ["E1", "E2"]
    execution = json.loads((reports / "campaign-execution.json").read_text(encoding="utf-8"))
    assert execution["status"] == "FAILED"
    assert execution["executions"][-1]["status"] == "FAILED"
    assert execution["executions"][-1]["driver_diagnostics"]
    assert all(
        (reports / "e2-mig" / record["path"]).is_file()
        for record in execution["executions"][-1]["driver_diagnostics"]
    )
    assert (reports / "e2-mig/artifacts/services/campaign-executor/stderr.log").is_file()
    assert (reports / "e2-mig/artifacts/services/hardware-driver/preflight/request.json").is_file()
    assert (reports / "e2-mig/artifacts/services/hardware-driver/preflight/stderr.log").is_file()


def test_campaign_run_missing_driver_writes_preflight_failure(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    missing = tmp_path / "missing-executor"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "campaign-run",
            "--campaign",
            str(CAMPAIGN),
            "--reports-dir",
            str(reports),
            "--driver",
            str(missing),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "campaign environment driver is not executable" in result.stderr
    execution = json.loads((reports / "campaign-execution.json").read_text(encoding="utf-8"))
    assert execution["status"] == "FAILED"
    assert execution["executions"] == []
    assert "campaign environment driver is not executable" in execution["error"]


def test_repository_hardware_executor_owns_scenario_order_and_cleanup(
    tmp_path: Path,
) -> None:
    driver, calls = tmp_path / "driver.py", tmp_path / "calls.ndjson"
    _write_hardware_driver(driver, calls)
    output = tmp_path / "hardware-output"

    result = subprocess.run(
        [
            sys.executable,
            str(HARDWARE_EXECUTOR),
            "--experiment",
            "E1",
            "--campaign",
            str(CAMPAIGN),
            "--gate-manifest",
            str(HARDWARE_MANIFEST),
            "--scenario",
            str(ROOT / "configs/scenarios/e1-full-gpu.yaml"),
            "--output-dir",
            str(output),
            "--evidence",
            "GPU_SINGLE_NODE",
            "--driver",
            str(driver),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    requests = [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()]
    assert requests[0]["operation"] == "preflight"
    runs: dict[tuple[str, str, int], list[str]] = {}
    for request in requests[1:]:
        key = (request["label"], request["phase"], request["iteration"])
        runs.setdefault(key, []).append(request["operation"])
    expected = [
        "provision",
        "apply_action",
        "launch",
        "verify_device_identity",
        "measure",
        "stop",
        "cleanup",
    ]
    assert len(runs) == 8
    assert all(operations == expected for operations in runs.values())
    report = read_report(output)
    assert report["status"] == "PASSED"
    assert (
        report["orchestrator"]["driver_sha256"] == hashlib.sha256(driver.read_bytes()).hexdigest()
    )
    assert (
        report["orchestrator"]["gate_tools_sha256"]
        == hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    )
    assert (
        report["orchestrator"]["campaign_sha256"]
        == hashlib.sha256(CAMPAIGN.read_bytes()).hexdigest()
    )
    assert (
        report["orchestrator"]["gate_manifest_sha256"]
        == hashlib.sha256(HARDWARE_MANIFEST.read_bytes()).hexdigest()
    )


def test_repository_hardware_executor_runs_cleanup_after_driver_failure(
    tmp_path: Path,
) -> None:
    driver, calls = tmp_path / "driver.py", tmp_path / "calls.ndjson"
    _write_hardware_driver(driver, calls, fail_operation="measure")

    result = subprocess.run(
        [
            sys.executable,
            str(HARDWARE_EXECUTOR),
            "--experiment",
            "E1",
            "--campaign",
            str(CAMPAIGN),
            "--gate-manifest",
            str(HARDWARE_MANIFEST),
            "--scenario",
            str(ROOT / "configs/scenarios/e1-full-gpu.yaml"),
            "--output-dir",
            str(tmp_path / "failed-output"),
            "--evidence",
            "GPU_SINGLE_NODE",
            "--driver",
            str(driver),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    operations = [
        json.loads(line)["operation"] for line in calls.read_text(encoding="utf-8").splitlines()
    ]
    assert operations[-2:] == ["measure", "cleanup"]


def test_repository_hardware_executor_reports_cleanup_failure(tmp_path: Path) -> None:
    driver, calls = tmp_path / "driver.py", tmp_path / "calls.ndjson"
    _write_hardware_driver(driver, calls, fail_operation="measure", invalid_cleanup=True)

    result = subprocess.run(
        [
            sys.executable,
            str(HARDWARE_EXECUTOR),
            "--experiment",
            "E1",
            "--campaign",
            str(CAMPAIGN),
            "--gate-manifest",
            str(HARDWARE_MANIFEST),
            "--scenario",
            str(ROOT / "configs/scenarios/e1-full-gpu.yaml"),
            "--output-dir",
            str(tmp_path / "failed-output"),
            "--evidence",
            "GPU_SINGLE_NODE",
            "--driver",
            str(driver),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "cleanup failed" in result.stderr
    assert "hardware driver cleanup failed" in result.stderr


def test_repository_hardware_executor_rejects_unsafe_scenario_order() -> None:
    campaign = GATE_TOOLS.load_campaign(CAMPAIGN)
    experiment = campaign["experiments"][0]
    scenario = json.loads((ROOT / experiment["scenario_manifest"]).read_text(encoding="utf-8"))
    scenario["execution_plan"]["variant"][-3:] = [
        {"operation": "stop"},
        {"operation": "measure"},
        {"operation": "cleanup"},
    ]

    with pytest.raises(HARDWARE_TOOLS.ExecutionError, match="unsafe ordering"):
        HARDWARE_TOOLS._scenario_plan(scenario, experiment)


def test_repository_hardware_executor_rejects_nested_credentials() -> None:
    response = {
        "schema_version": HARDWARE_TOOLS.DRIVER_RESPONSE_SCHEMA,
        "request_id": "request-1",
        "status": "SUCCEEDED",
        "events": [
            {
                "event_type": "worker_registered",
                "metadata": {"accessToken": "must-not-be-archived"},
            }
        ],
    }

    with pytest.raises(HARDWARE_TOOLS.ExecutionError, match="credential-like fields"):
        HARDWARE_TOOLS._validate_driver_response(
            response, request_id="request-1", operation="launch"
        )


def test_repository_hardware_executor_rejects_device_class_mismatch() -> None:
    response = {
        "schema_version": HARDWARE_TOOLS.DRIVER_RESPONSE_SCHEMA,
        "request_id": "request-1",
        "status": "SUCCEEDED",
        "events": [
            {
                "event_type": "device_identity_verified",
                "source": "worker",
                "node_id": "gpu-node-1",
                "scheduler_device_ids": ["MIG-1/1/0"],
                "allocated_device_ids": ["MIG-1/1/0"],
                "worker_device_ids": ["MIG-1/1/0"],
                "device_class": "gpu.nvidia.com",
                "parent_uuid": "GPU-parent",
            }
        ],
    }

    with pytest.raises(HARDWARE_TOOLS.ExecutionError, match="preflight GPU profile"):
        HARDWARE_TOOLS._validate_driver_response(
            response,
            request_id="request-1",
            operation="verify_device_identity",
            gpu_profile="mig",
        )


def test_repository_hardware_executor_rejects_bad_artifact_digest(tmp_path: Path) -> None:
    driver = tmp_path / "driver.py"
    driver.write_text(
        """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--request", required=True)
parser.add_argument("--response", required=True)
args = parser.parse_args()
request = json.loads(Path(args.request).read_text(encoding="utf-8"))
artifact = Path(args.response).parent / "driver.log"
artifact.write_text("evidence\\n", encoding="utf-8")
Path(args.response).write_text(json.dumps({
    "schema_version": "tgsrl.io/hardware-driver-response/v1alpha1",
    "request_id": request["request_id"],
    "status": "SUCCEEDED",
    "events": [],
    "artifacts": [{"path": "driver.log", "sha256": "wrong"}],
}), encoding="utf-8")
""",
        encoding="utf-8",
    )
    driver.chmod(0o755)

    with pytest.raises(HARDWARE_TOOLS.DriverFailure, match="missing or changed"):
        HARDWARE_TOOLS._run_driver(
            driver,
            {"request_id": "request-1", "operation": "cleanup"},
            tmp_path / "operation",
            10.0,
        )


def test_repository_hardware_executor_redacts_sensitive_driver_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = tmp_path / "driver.py"
    driver.write_text(
        """#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--request", required=True)
parser.add_argument("--response", required=True)
args = parser.parse_args()
request = json.loads(Path(args.request).read_text(encoding="utf-8"))
Path(args.response).write_text(json.dumps({
    "schema_version": "tgsrl.io/hardware-driver-response/v1alpha1",
    "request_id": request["request_id"],
    "status": "SUCCEEDED",
    "detail": os.environ["TARGET_ACCESS_TOKEN"],
    "events": [],
}), encoding="utf-8")
""",
        encoding="utf-8",
    )
    driver.chmod(0o755)
    monkeypatch.setenv("TARGET_ACCESS_TOKEN", "sensitive-value-123")
    operation_root = tmp_path / "operation"

    with pytest.raises(HARDWARE_TOOLS.DriverFailure, match="sensitive environment values"):
        HARDWARE_TOOLS._run_driver(
            driver,
            {"request_id": "request-1", "operation": "cleanup"},
            operation_root,
            10.0,
        )

    stored = (operation_root / "response.json").read_text(encoding="utf-8")
    assert "sensitive-value-123" not in stored
    assert "REJECTED" in stored


@pytest.mark.parametrize(
    ("accelerator_count", "inventory_digest", "message"),
    [
        (True, "gpu-inventory-digest", "accelerator inventory is insufficient"),
        (2, "", "omitted fingerprint fields: accelerator_inventory_digest"),
    ],
)
def test_repository_hardware_executor_rejects_invalid_preflight_fingerprint(
    tmp_path: Path, accelerator_count: object, inventory_digest: str, message: str
) -> None:
    driver, calls = tmp_path / "driver.py", tmp_path / "calls.ndjson"
    _write_hardware_driver(
        driver,
        calls,
        preflight_accelerator_count=accelerator_count,
        preflight_inventory_digest=inventory_digest,
    )

    result = subprocess.run(
        [
            sys.executable,
            str(HARDWARE_EXECUTOR),
            "--experiment",
            "E1",
            "--campaign",
            str(CAMPAIGN),
            "--gate-manifest",
            str(HARDWARE_MANIFEST),
            "--scenario",
            str(ROOT / "configs/scenarios/e1-full-gpu.yaml"),
            "--output-dir",
            str(tmp_path / "hardware-output"),
            "--evidence",
            "GPU_SINGLE_NODE",
            "--driver",
            str(driver),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert message in result.stderr
    requests = [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()]
    assert [request["operation"] for request in requests] == ["preflight"]


def test_campaign_rejects_missing_named_evidence_and_stale_commit(tmp_path: Path) -> None:
    output = tmp_path / "e2-mig"
    _write_full_stack_gpu_report(output, "E2", "mig", complete_requirements=True)
    report = read_report(output)
    variant_path = output / report["variant_trace"]
    variant = json.loads(variant_path.read_text(encoding="utf-8"))
    for event in variant["events"]:
        if event.get("event_type") == "device_identity_verified":
            event["parent_uuid"] = ""
    variant_path.write_text(json.dumps(variant), encoding="utf-8")
    report["trace_capture"]["variant_digest"] = hashlib.sha256(
        variant_path.read_bytes()
    ).hexdigest()
    report["environment_fingerprint"]["git_commit"] = "stale-commit"
    (output / "report.json").write_text(json.dumps(report), encoding="utf-8")

    evaluated = campaign_result(tmp_path)

    assert evaluated.returncode == 0, evaluated.stderr
    e2 = json.loads(evaluated.stdout)["experiments"][1]
    assert e2["status"] == "INVALID"
    assert "evidence git commit does not match the campaign checkout" in e2["blockers"]
    assert "required evidence parent-uuid is missing or incomplete" in e2["blockers"]


def test_campaign_rejects_overlapping_multi_node_device_identity(tmp_path: Path) -> None:
    output = tmp_path / "e8-multi-node-convergence"
    _write_full_stack_gpu_report(output, "E8", "full-gpu", complete_requirements=True)
    report = read_report(output)
    variant_path = output / report["variant_trace"]
    variant = json.loads(variant_path.read_text(encoding="utf-8"))
    for event in variant["events"]:
        if (
            event.get("phase") == "measurement"
            and event.get("event_type") == "device_identity_verified"
        ):
            event["scheduler_device_ids"] = ["GPU-overlap"]
            event["allocated_device_ids"] = ["GPU-overlap"]
            event["worker_device_ids"] = ["GPU-overlap"]
    variant_path.write_text(json.dumps(variant), encoding="utf-8")
    report["trace_capture"]["variant_digest"] = hashlib.sha256(
        variant_path.read_bytes()
    ).hexdigest()
    report["metrics"]["variant"] = GATE_TOOLS._metrics_from_events(variant["events"])
    (output / "report.json").write_text(json.dumps(report), encoding="utf-8")

    evaluated = campaign_result(tmp_path)

    assert evaluated.returncode == 0, evaluated.stderr
    e8 = json.loads(evaluated.stdout)["experiments"][-1]
    assert e8["status"] == "INVALID"
    assert "variant multi-node device identities are missing or overlap" in e8["blockers"]


def test_recovery_metric_includes_fault_recovery_events() -> None:
    common = {"phase": "measurement", "iteration": 1}
    metrics = GATE_TOOLS._metrics_from_events(
        [
            common
            | {
                "event_type": "sample_consumed",
                "duration_ms": 1.0,
                "contract_observation": {},
            },
            common | {"event_type": "workload_completed", "elapsed_ms": 10, "item_count": 1},
            common
            | {
                "event_type": "decision_applied",
                "action": "rollback",
                "succeeded": True,
                "recovery_time_ms": 2.0,
            },
            common | {"event_type": "fault_recovered", "recovery_time_ms": 3.0},
        ]
    )

    assert metrics["transaction_recovery_time_ms"] == 5.0


def test_campaign_executor_output_rejects_symlink_escape(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    escaped = source / "baseline.json"
    escaped.symlink_to(outside)

    with pytest.raises(GATE_TOOLS.GateToolError, match="escapes its output directory"):
        GATE_TOOLS._validate_campaign_executor_output(
            {
                "baseline_trace": escaped.name,
                "variant_trace": escaped.name,
                "service_log_artifacts": [],
            },
            source.resolve(),
        )


def test_campaign_rejects_mismatched_orchestrator_digest() -> None:
    expected = {
        "executor_digest": "executor",
        "gate_tools_digest": "gate-tools",
        "driver_digest": "driver",
        "campaign_digest": "campaign",
        "gate_manifest_digest": "gate-manifest",
        "scenario_digest": "scenario",
    }
    report = {
        "orchestrator": {
            "schema_version": "tgsrl.io/hardware-orchestrator/v1alpha1",
            "executor_sha256": "executor",
            "gate_tools_sha256": "gate-tools",
            "driver_sha256": "driver",
            "campaign_sha256": "campaign",
            "gate_manifest_sha256": "gate-manifest",
            "scenario_sha256": "scenario",
        }
    }
    GATE_TOOLS._validate_campaign_orchestrator(report, **expected)
    report["orchestrator"]["gate_tools_sha256"] = "changed"

    with pytest.raises(GATE_TOOLS.GateToolError, match="locked execution inputs"):
        GATE_TOOLS._validate_campaign_orchestrator(report, **expected)


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
