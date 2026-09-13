#!/usr/bin/env python3
"""Run bounded engineering failure-path checks and write an auditable report."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / ".cache" / "tgsrl" / "engineering-fault-readiness"
ARCHIVE = ROOT / ".cache" / "tgsrl" / "archive"


def run_check(name: str, argv: list[str]) -> dict[str, Any]:
    log_root = OUTPUT / "logs"
    log_root.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    timed_out = False
    try:
        completed = subprocess.run(
            argv,
            cwd=ROOT,
            capture_output=True,
            check=False,
            text=False,
            timeout=180,
        )
        stdout_payload = completed.stdout
        stderr_payload = completed.stderr
        exit_code = completed.returncode
        if argv[:2] == ["go", "test"] and b"=== RUN" not in stdout_payload:
            stderr_payload += b"\nselected Go tests did not execute\n"
            exit_code = 3
    except subprocess.TimeoutExpired as error:
        timed_out = True
        stdout_payload = error.stdout or b""
        stderr_payload = error.stderr or b""
        exit_code = 124
    stdout = log_root / f"{name}.stdout.log"
    stderr = log_root / f"{name}.stderr.log"
    stdout.write_bytes(stdout_payload)
    stderr.write_bytes(stderr_payload)
    stdout.chmod(0o600)
    stderr.chmod(0o600)
    return {
        "name": name,
        "status": "PASSED" if exit_code == 0 else "FAILED",
        "argv": argv,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "duration_ms": (time.monotonic() - started) * 1000.0,
        "artifacts": [
            {
                "path": stdout.relative_to(OUTPUT).as_posix(),
                "sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
            },
            {
                "path": stderr.relative_to(OUTPUT).as_posix(),
                "sha256": hashlib.sha256(stderr.read_bytes()).hexdigest(),
            },
        ],
    }


def main() -> int:
    if OUTPUT.is_dir() and any(OUTPUT.iterdir()):
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        short_sha = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            check=True,
            text=True,
        ).stdout.strip()
        ARCHIVE.mkdir(parents=True, exist_ok=True)
        destination = Path(
            tempfile.mkdtemp(
                prefix=f"engineering-fault-readiness-{stamp}-{short_sha}-",
                dir=ARCHIVE,
            )
        )
        destination.rmdir()
        OUTPUT.rename(destination)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    python = sys.executable
    checks = [
        (
            "worker_exit_propagation",
            [
                "go",
                "test",
                "./cmd/tgsrl-worker-bootstrap",
                "-run",
                "^TestRunWorkerReportsUnexpectedCrash$",
                "-count=1",
                "-v",
            ],
        ),
        (
            "control_response_loss_retry",
            [
                "go",
                "test",
                "./cmd/tgsrl-worker-bootstrap",
                "-run",
                "^(TestRunWorkerRetriesLostRegistrationResponse|TestSupervisorRetriesCooperativeMutationAfterUnknownTimeout)$",
                "-count=1",
                "-v",
            ],
        ),
        (
            "managed_worker_restart_reconciliation",
            [
                "go",
                "test",
                "./internal/managedworker",
                "-run",
                "^(TestStoreRejectsRemoteRestartUntilPendingReceiptIsReconciled|TestControllerReconcilesPendingManagedWorkerReceiptAfterRestart)$",
                "-count=1",
                "-v",
            ],
        ),
        (
            "operator_partial_failure_restart",
            [
                "go",
                "test",
                "./operator-go/backend",
                "-run",
                "^(TestKubernetesBackendPersistsPartialProgressAndRetriesOnlyFailedBundle|TestKubernetesBackendResumesPartialProgressAfterRestart)$",
                "-count=1",
                "-v",
            ],
        ),
        (
            "cpu_memory_gpu_capacity_rejection",
            [
                "go",
                "test",
                "./scheduler-go/scheduler",
                "-run",
                "^TestEvaluateHardConstraints$",
                "-count=1",
                "-v",
            ],
        ),
        (
            "resource_unavailable_fallback",
            [
                "go",
                "test",
                "./scheduler-go/service",
                "-run",
                "^TestProviderUnavailableEmitsNoOpFallbackAndKeepsIntent$",
                "-count=1",
                "-v",
            ],
        ),
        (
            "runtime_receipt_restart",
            [
                python,
                "-m",
                "pytest",
                "-q",
                "tests/python/test_runtime_supervisor.py::test_managed_worker_trace_recovers_after_receipt_crash_and_restart",
                "tests/python/test_runtime_supervisor.py::test_supervisor_safe_point_checkpoint_offload_reload_round_trip_persists_across_restart",
            ],
        ),
    ]
    results = [run_check(name, argv) for name, argv in checks]
    status = "PASSED" if all(item["status"] == "PASSED" for item in results) else "FAILED"
    payload = {
        "schema_version": "tgsrl.io/engineering-fault-readiness/v1alpha1",
        "status": status,
        "evidence": "CPU_REAL_PROCESS_AND_COMPONENT_INTEGRATION",
        "git_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            check=True,
            text=True,
        ).stdout.strip(),
        "git_dirty": bool(
            subprocess.run(
                ["git", "status", "--porcelain", "--untracked-files=all"],
                cwd=ROOT,
                capture_output=True,
                check=True,
                text=True,
            ).stdout.strip()
        ),
        "checks": results,
        "gpu_faults": [
            {
                "fault": "worker Pod forced exit and replacement",
                "status": "NOT_RUN",
                "reason": "no safe Kubernetes fault/recovery hook is configured",
            },
            {
                "fault": "GPU capacity exhaustion",
                "status": "NOT_RUN",
                "reason": "requires a dedicated target-environment workload and cleanup hook",
            },
            {
                "fault": "node or network loss",
                "status": "NOT_RUN",
                "reason": "single-node readiness cannot safely prove recovery",
            },
        ],
    }
    report = OUTPUT / "report.json"
    report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report.chmod(0o600)
    print(report.relative_to(ROOT))
    return 0 if status == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
