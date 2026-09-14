from __future__ import annotations

import fcntl
import importlib.util
import json
import stat
import sys
import threading
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]
DRIVER_PATH = ROOT / "scripts" / "hardware_environment_driver.py"
SPEC = importlib.util.spec_from_file_location("tgsrl_hardware_environment_driver", DRIVER_PATH)
assert SPEC is not None and SPEC.loader is not None
DRIVER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DRIVER
SPEC.loader.exec_module(DRIVER)

JsonObject = dict[str, Any]


def _decision(sequence: int, action: str, device_id: str, generation: int) -> JsonObject:
    return {
        "decisionId": f"decision-{sequence}",
        "sequence": str(sequence),
        "fallback": False,
        "selectedPlan": {
            "planId": f"plan-{sequence}",
            "bindings": [
                {
                    "bindingId": f"binding-{generation}",
                    "pendingUnitId": "unit-a",
                    "runtimeUnitId": "unit-a",
                    "sandboxId": "sandbox-a",
                    "generation": str(generation),
                    "deviceIds": [device_id],
                }
            ],
            "actions": [
                {
                    "actionId": f"action-{sequence}",
                    "actionType": f"ACTION_TYPE_{action.upper()}",
                }
            ],
        },
        "actionResults": [
            {
                "actionId": f"action-{sequence}",
                "status": "ACTION_RESULT_STATUS_SUCCEEDED",
            }
        ],
    }


class _GatewayState:
    def __init__(self, marker: Path, job_id: str) -> None:
        self.marker = marker
        self.job_id = job_id
        self.requests: list[tuple[str, str]] = []
        self.created_job_ids: list[str] = []
        self.idempotency_keys: list[tuple[str, str]] = []

    @property
    def generation(self) -> int:
        return 2 if self.marker.exists() else 1

    @property
    def device_id(self) -> str:
        return f"MIG-{self.generation}/1/0"


def _gateway_server(
    state: _GatewayState,
) -> tuple[ThreadingHTTPServer, threading.Thread]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *_args: object) -> None:
            return

        def _write(self, value: JsonObject, status: int = 200) -> None:
            payload = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            state.requests.append(("GET", self.path))
            if path in {"/health", "/v1/capabilities"}:
                self._write({"status": "SERVING"})
            elif path == "/v1/operations/op-admit":
                self._write(
                    {
                        "operation": {
                            "operationId": "op-admit",
                            "state": "OPERATION_STATE_SUCCEEDED",
                        }
                    }
                )
            elif path == "/v1/operations/op-start":
                self._write(
                    {
                        "operation": {
                            "operationId": "op-start",
                            "state": "OPERATION_STATE_SUCCEEDED",
                        }
                    }
                )
            elif path == "/v1/operations/op-stop":
                self._write(
                    {
                        "operation": {
                            "operationId": "op-stop",
                            "state": "OPERATION_STATE_SUCCEEDED",
                        }
                    }
                )
            elif (
                path.startswith("/v1/jobs/")
                and path.endswith("/runs")
                and path.removeprefix("/v1/jobs/").removesuffix("/runs").strip("/")
                in state.created_job_ids
            ):
                self._write(
                    {
                        "runs": [
                            {
                                "runId": "run-a",
                                "traceId": "trace-a",
                                "runState": "JOB_RUN_STATE_WAITING",
                            }
                        ]
                    }
                )
            elif (
                path.startswith("/v1/jobs/")
                and path.endswith("/decisions")
                and path.removeprefix("/v1/jobs/").removesuffix("/decisions").strip("/")
                in state.created_job_ids
            ):
                decisions = [_decision(1, "bind", "MIG-1/1/0", 1)]
                if state.marker.exists():
                    decisions.append(_decision(2, "rebind", "MIG-2/1/0", 2))
                self._write({"decisions": decisions})
            elif (
                path.startswith("/v1/jobs/")
                and path.endswith("/topology")
                and path.removeprefix("/v1/jobs/").removesuffix("/topology").strip("/")
                in state.created_job_ids
            ):
                generation = state.generation
                self._write(
                    {
                        "sandboxes": [
                            {
                                "sandboxId": "sandbox-a",
                                "generation": str(generation),
                                "state": "RUNTIME_STATE_RUNNING",
                                "binding": {
                                    "bindingId": f"binding-{generation}",
                                    "pendingUnitId": "unit-a",
                                    "runtimeUnitId": "unit-a",
                                    "sandboxId": "sandbox-a",
                                    "generation": str(generation),
                                    "deviceIds": [state.device_id],
                                },
                            }
                        ]
                    }
                )
            else:
                self._write({"error": path}, 404)

        def do_POST(self) -> None:
            state.requests.append(("POST", self.path))
            state.idempotency_keys.append((self.path, str(self.headers.get("Idempotency-Key", ""))))
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/v1/jobs":
                assert body["dataKind"] == "DATA_KIND_LIVE"
                state.created_job_ids.append(body["jobId"])
                self._write({"job": {"jobId": body["jobId"]}}, 201)
            elif (
                self.path.startswith("/v1/jobs/")
                and self.path.endswith("/admit")
                and self.path.removeprefix("/v1/jobs/").removesuffix("/admit").strip("/")
                in state.created_job_ids
            ):
                self._write(
                    {
                        "operation": {
                            "operationId": "op-admit",
                            "state": "OPERATION_STATE_RUNNING",
                        }
                    }
                )
            elif self.path.endswith("/commands/start"):
                self._write(
                    {
                        "operation": {
                            "operationId": "op-start",
                            "state": "OPERATION_STATE_RUNNING",
                        }
                    }
                )
            elif self.path.endswith("/commands/stop"):
                self._write(
                    {
                        "operation": {
                            "operationId": "op-stop",
                            "state": "OPERATION_STATE_RUNNING",
                        }
                    }
                )
            else:
                self._write({"error": self.path}, 404)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _write_executable(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _write_fake_kubectl(path: Path) -> None:
    _write_executable(
        path,
        r"""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

args = sys.argv[1:]
for option in ("--context", "--namespace"):
    while option in args:
        index = args.index(option)
        del args[index:index + 2]
if args[-2:] == ["-o", "json"]:
    args = args[:-2]
if "--ignore-not-found=true" in args:
    args.remove("--ignore-not-found=true")
marker = Path(os.environ["TGSRL_FAKE_REBIND_MARKER"])
deleted_path = marker.with_suffix(".deleted.json")
deleted = set(json.loads(deleted_path.read_text()) if deleted_path.exists() else [])
job_id = os.environ["TGSRL_FAKE_JOB_ID"]
generation = 2 if marker.exists() else 1
device_id = f"MIG-{generation}/1/0"
device_name = f"mig-{generation}"
job_name = f"kube-job-{generation}"
claim_name = f"claim-{generation}"

def emit(value):
    print(json.dumps(value, sort_keys=True))

def metadata(name):
    return {"name": name, "uid": f"uid-{name}", "resourceVersion": "1"}

if args == ["config", "current-context"]:
    print("test-context")
elif args == ["version"]:
    emit({"serverVersion": {"gitVersion": "v1.35.1"}})
elif args[:2] == ["get", "namespace"]:
    emit({"apiVersion": "v1", "kind": "Namespace", "metadata": metadata(args[2])})
elif args[:2] == ["get", "deviceclasses.resource.k8s.io"]:
    emit({"metadata": {"name": args[2]}})
elif args == ["get", "nodes"]:
    emit({"items": [{
        "metadata": {
            "name": "gpu-node-a",
            "annotations": {
                "hami.io/node-nvidia-register": json.dumps([{
                    "id": "GPU-full",
                    "count": 10,
                    "devmem": 23028,
                    "devcore": 100,
                    "type": "NVIDIA A10",
                    "health": True,
                    "mode": "hami-core",
                }]),
            },
        },
        "status": {
            "allocatable": {
                "nvidia.com/gpu": "10",
                "nvidia.com/gpucores": "1000",
                "nvidia.com/gpumem-percentage": "1000",
            },
        },
    }]})
elif args == ["get", "resourceslices.resource.k8s.io"]:
    devices = [{
        "name": "gpu-1",
        "attributes": {
            "uuid": {"string": "GPU-full"},
            "type": {"string": "gpu"},
        },
    }]
    for current in (1, 2):
        devices.append({
            "name": f"mig-{current}",
            "attributes": {
                "uuid": {"string": f"MIG-{current}/1/0"},
                "type": {"string": "mig"},
                "profile": {"string": "1g.10gb"},
                "parentUUID": {"string": "GPU-parent"},
            },
        })
    emit({"items": [{"spec": {
        "driver": "gpu.nvidia.com",
        "nodeName": "gpu-node-a",
        "pool": {"name": "gpu-node-a", "generation": 1},
        "devices": devices,
    }}]})
elif args == ["get", "jobrunbundles.tgsrl.io"]:
    def bundle(current):
        name = f"bundle-{current}"
        return {
            "apiVersion": "tgsrl.io/v1alpha1",
            "kind": "JobRunBundle",
            "metadata": metadata(name),
            "spec": {"bundle": {
                "generation": current,
                "sourceJobId": job_id,
                "sourceRunId": "run-a",
                "runtimeTargets": [{
                    "runtimeUnitId": "unit-a",
                    "sandboxId": "sandbox-a",
                    "generation": current,
                }],
                "job": {"metadata": {"name": f"kube-job-{current}"}},
                "workload": {"metadata": {"name": f"workload-{current}"}},
                "resourceClaimTemplate": {"metadata": {"name": f"claim-template-{current}"}},
            }}
        }
    emit({"items": [
        bundle(current)
        for current in range(1, generation + 1)
        if f"bundle-{current}" not in deleted
    ]})
elif len(args) == 3 and args[:2] == ["get", "jobrunbundles.tgsrl.io"]:
    current = int(args[2].removeprefix("bundle-"))
    if args[2] not in deleted:
        name = args[2]
        emit({
            "apiVersion": "tgsrl.io/v1alpha1",
            "kind": "JobRunBundle",
            "metadata": metadata(name),
            "spec": {"bundle": {
                "generation": current,
                "sourceJobId": job_id,
                "sourceRunId": "run-a",
                "runtimeTargets": [{
                    "runtimeUnitId": "unit-a",
                    "sandboxId": "sandbox-a",
                    "generation": current,
                }],
                "job": {"metadata": {"name": f"kube-job-{current}"}},
                "workload": {"metadata": {"name": f"workload-{current}"}},
                "resourceClaimTemplate": {"metadata": {"name": f"claim-template-{current}"}},
            }},
        })
elif args == ["get", "pods"]:
    emit({"items": [{
        "metadata": {
            "name": f"pod-{generation}",
            "labels": {"tgsrl.io/job-id": job_id, "tgsrl.io/run-id": "run-a"},
            "ownerReferences": [{"kind": "Job", "name": job_name}],
        },
        "spec": {"nodeName": "gpu-node-a"},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "resourceClaimStatuses": [{
                "name": "accelerator",
                "resourceClaimName": claim_name,
            }],
        },
    }]})
elif len(args) == 3 and args[:2] == ["get", "resourceclaims.resource.k8s.io"]:
    if args[2] in deleted:
        raise SystemExit(0)
    owner = "other-job" if os.environ.get("TGSRL_FAKE_BAD_OWNER") else job_id
    emit({
        "apiVersion": "resource.k8s.io/v1",
        "kind": "ResourceClaim",
        "metadata": {
            **metadata(claim_name),
            "labels": {"tgsrl.io/job-id": owner, "tgsrl.io/run-id": "run-a"},
        },
        "spec": {"devices": {"requests": [{
            "name": "accelerator",
            "exactly": {"deviceClassName": "mig.nvidia.com"},
        }]}},
        "status": {"allocation": {"devices": {"results": [{
            "request": "accelerator",
            "driver": "gpu.nvidia.com",
            "pool": "gpu-node-a",
            "device": device_name,
        }]}}},
    })
elif len(args) == 3 and args[0] == "get" and args[1] in {
    "jobs.batch",
    "workloads.kueue.x-k8s.io",
    "resourceclaimtemplates.resource.k8s.io",
}:
    if args[2] in deleted:
        raise SystemExit(0)
    owner = "other-job" if os.environ.get("TGSRL_FAKE_BAD_OWNER") else job_id
    api_version, kind = {
        "jobs.batch": ("batch/v1", "Job"),
        "workloads.kueue.x-k8s.io": ("kueue.x-k8s.io/v1beta2", "Workload"),
        "resourceclaimtemplates.resource.k8s.io": ("resource.k8s.io/v1", "ResourceClaimTemplate"),
    }[args[1]]
    emit({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {
            **metadata(args[2]),
            "labels": {"tgsrl.io/job-id": owner, "tgsrl.io/run-id": "run-a"},
        }
    })
elif args[:1] == ["exec"] and args[-2:] == ["nvidia-smi", "-L"]:
    print(f"MIG 1g.10gb Device 0: (UUID: {device_id})")
elif args[:1] == ["exec"]:
    common = {
        "job_id": job_id,
        "run_id": "run-a",
        "sandbox_id": "sandbox-a",
        "generation": generation,
        "runtime_unit_id": "unit-a",
        "worker_id": "unit-a",
        "device_ids": [device_id],
    }
    events = [
        common | {
            "event_type": "sample_consumed",
            "duration_ms": 1.0,
            "gpu_active_ms": 2.0,
            "useful_gpu_time_ms": 1.5,
            "contract_observation": {
                "policy_lag": 0,
                "sample_stale": False,
                "effective_sample_size": 1.0,
            },
        },
        common | {
            "event_type": "workload_completed",
            "elapsed_ms": 10.0,
            "item_count": 1,
            "convergence_quality": 1.0,
        },
    ]
    print("\n".join(json.dumps(event, sort_keys=True) for event in events))
elif args[:1] == ["delete"] and any(item.startswith("--raw=") for item in args):
    raw = next(item.removeprefix("--raw=") for item in args if item.startswith("--raw="))
    name = unquote(urlsplit(raw).path.rsplit("/", 1)[-1])
    options = json.loads(sys.stdin.read())
    expected = {"uid": f"uid-{name}", "resourceVersion": "1"}
    if options.get("preconditions") != expected:
        print("precondition mismatch", file=sys.stderr)
        raise SystemExit(1)
    deleted.add(name)
    deleted_path.write_text(json.dumps(sorted(deleted)))
    emit({})
elif args[:1] == ["patch"]:
    name = args[2]
    patch = json.loads(args[args.index("--patch") + 1])
    expected = [
        {"op": "test", "path": "/metadata/uid", "value": f"uid-{name}"},
        {"op": "test", "path": "/metadata/resourceVersion", "value": "1"},
        {"op": "add", "path": "/metadata/finalizers", "value": []},
    ]
    if patch != expected:
        print("patch precondition mismatch", file=sys.stderr)
        raise SystemExit(1)
    current = int(name.removeprefix("bundle-"))
    emit({
        "apiVersion": "tgsrl.io/v1alpha1",
        "kind": "JobRunBundle",
        "metadata": metadata(name),
        "spec": {"bundle": {
            "generation": current,
            "sourceJobId": job_id,
            "sourceRunId": "run-a",
            "runtimeTargets": [{
                "runtimeUnitId": "unit-a",
                "sandboxId": "sandbox-a",
                "generation": current,
            }],
            "job": {"metadata": {"name": f"kube-job-{current}"}},
            "workload": {"metadata": {"name": f"workload-{current}"}},
            "resourceClaimTemplate": {"metadata": {"name": f"claim-template-{current}"}},
        }},
    })
else:
    print("unsupported fake kubectl argv: " + repr(args), file=sys.stderr)
    raise SystemExit(3)
""",
    )


def _write_rebind_hook(path: Path, marker: Path) -> None:
    _write_executable(
        path,
        f"""#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument("--request", required=True)
parser.add_argument("--response", required=True)
args = parser.parse_args()
request = json.loads(Path(args.request).read_text(encoding="utf-8"))
Path({str(marker)!r}).write_text("rebound\\n", encoding="utf-8")
Path(args.response).write_text(json.dumps({{
    "schema_version": "tgsrl.io/hardware-driver-hook-response/v1alpha1",
    "request_id": request["request_id"],
    "status": "SUCCEEDED",
    "authority": "scheduler-observation",
    "receipt_id": "observation-rebind",
}}), encoding="utf-8")
""",
    )


def _write_invalid_hook(path: Path) -> None:
    _write_executable(
        path,
        """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument("--request", required=True)
parser.add_argument("--response", required=True)
args = parser.parse_args()
request = json.loads(Path(args.request).read_text(encoding="utf-8"))
Path(args.response).write_text(json.dumps({
    "schema_version": "tgsrl.io/hardware-driver-hook-response/v1alpha1",
    "request_id": request["request_id"],
    "status": "SUCCEEDED",
    "authority": "resourceclaim-patch",
}), encoding="utf-8")
""",
    )


def _write_worker_hook(path: Path) -> None:
    _write_executable(
        path,
        """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument("--request", required=True)
parser.add_argument("--response", required=True)
args = parser.parse_args()
request = json.loads(Path(args.request).read_text(encoding="utf-8"))
Path(args.response).write_text(json.dumps({
    "schema_version": "tgsrl.io/hardware-driver-hook-response/v1alpha1",
    "request_id": request["request_id"],
    "status": "SUCCEEDED",
    "authority": "managed-worker-control",
    "receipt_id": "worker-receipt",
    "ready": True,
}), encoding="utf-8")
""",
    )


def _job_template(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "displayName": "hardware-verl",
                "protocolVersion": "v0.3",
                "algorithm": "grpo",
                "runtime": {
                    "framework": "verl",
                    "frameworkVersion": "0.9.0",
                    "executionBackend": "ray",
                    "executionBackendVersion": "2.0.0",
                    "trainer": "pytorch",
                    "trainerVersion": "2.0.0",
                    "rolloutEngine": "vllm",
                    "rolloutEngineVersion": "1.0.0",
                    "imageDigest": "sha256:" + "1" * 64,
                    "artifactUri": "registry.example.test/verl@sha256:" + "1" * 64,
                    "compatibilityProfile": "gpu-smoke-verl-v1",
                    "command": ["python", "-m", "hardware_workload"],
                    "args": ["--seed", "${SEED}"],
                },
                "executionContract": {
                    "contractId": "hardware-contract",
                    "version": "1.0.0",
                    "phaseGraph": {
                        "phases": [
                            {
                                "phaseId": "decode",
                                "displayName": "Decode",
                                "kind": "PHASE_KIND_DECODE",
                                "parallelism": 1,
                                "maxAttempts": 1,
                            }
                        ],
                        "entryPhaseIds": ["decode"],
                    },
                },
                "resourcesPerUnit": {
                    "cpuMillis": "1000",
                    "memoryBytes": str(1 << 30),
                    "acceleratorUnits": 1,
                },
                "requiredCapabilities": {
                    "names": ["nvidia-gpu", "nvidia-mig"],
                    "algorithms": ["grpo"],
                    "rolloutModes": ["partially_async"],
                    "supportedActions": ["bind", "rebind"],
                },
                "desiredUnits": 1,
                "priority": 1,
                "queue": "default",
                "rolloutMode": "ROLLOUT_MODE_PARTIALLY_ASYNC",
                "policyRef": "1",
                "dataKind": "DATA_KIND_LIVE",
            }
        ),
        encoding="utf-8",
    )


def _scenario() -> JsonObject:
    return cast(
        JsonObject,
        json.loads((ROOT / "configs/scenarios/e2-mig.yaml").read_text(encoding="utf-8")),
    )


def _request(operation: str, step: int, *, action: str = "") -> JsonObject:
    return {
        "schema_version": "tgsrl.io/hardware-driver-request/v1alpha1",
        "request_id": f"request-{step}-{operation}-{action}",
        "campaign_id": "campaign-a",
        "experiment_id": "E2",
        "evidence": "GPU_SINGLE_NODE",
        "label": "variant",
        "phase": "measurement",
        "iteration": 1,
        "run_key": "e2-variant-measurement-1",
        "operation": operation,
        "step_index": step,
        "action": action,
        "fault_id": "",
        "workload_lock": {
            "algorithm": "grpo",
            "rollout_mode": "partially_async",
            "seed": 20260829,
            "version_lock": {
                "policy": "static",
                "scenario_revision": 1,
                "compatibility_profile": "gpu-smoke-verl-v1",
            },
        },
        "scenario": _scenario(),
    }


@pytest.fixture
def driver_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Any, _GatewayState, Path]:
    marker = tmp_path / "rebound"
    attempt_ids = iter(("0123456789abcdef", "fedcba9876543210"))
    monkeypatch.setattr(DRIVER, "_new_attempt_id", lambda: next(attempt_ids))
    expected_job_id = DRIVER._job_id(_request("provision", 1), 1, "0123456789abcdef")
    state = _GatewayState(marker, expected_job_id)
    server, thread = _gateway_server(state)
    kubectl = tmp_path / "kubectl"
    hook = tmp_path / "rebind-hook"
    template = tmp_path / "job.json"
    config = tmp_path / "environment.json"
    _write_fake_kubectl(kubectl)
    _write_rebind_hook(hook, marker)
    _job_template(template)
    config.write_text(
        json.dumps(
            {
                "schema_version": "tgsrl.io/hardware-environment/v1alpha1",
                "state_directory": str(tmp_path / "state"),
                "defaults": {
                    "gateway_url": f"http://127.0.0.1:{server.server_port}",
                    "namespace": "tgsrl-workloads",
                    "kube_context": "test-context",
                    "kubectl": str(kubectl),
                    "job_template": str(template),
                    "trace_command": ["export-gate-trace"],
                    "operation_timeout_seconds": 10,
                    "poll_interval_seconds": 0.01,
                },
                "targets": {
                    "E1": {"gpu_profile": "full-gpu"},
                    "E2": {
                        "gpu_profile": "mig",
                        "action_hooks": {
                            "rebind": [
                                str(hook),
                                "--request",
                                "${REQUEST_PATH}",
                                "--response",
                                "${RESPONSE_PATH}",
                            ]
                        },
                    },
                    "H1": {
                        "gpu_profile": "full-gpu",
                        "execution_mode": "hami-vgpu",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TGSRL_FAKE_REBIND_MARKER", str(marker))
    monkeypatch.setenv("TGSRL_FAKE_JOB_ID", expected_job_id)
    monkeypatch.setattr(DRIVER, "_git_fingerprint", lambda: ("commit-a", False))
    selected = DRIVER.HardwareEnvironmentDriver(DRIVER.load_config(config))
    try:
        yield selected, state, tmp_path
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def _execute(driver: Any, root: Path, request: JsonObject) -> JsonObject:
    operation_root = root / request["request_id"]
    operation_root.mkdir()
    return cast(JsonObject, driver.execute(request, operation_root / "response.json"))


def test_driver_state_lock_fails_fast_when_another_operation_is_active(tmp_path: Path) -> None:
    store = DRIVER.StateStore(tmp_path / "state")
    with (
        store.lock_path.open("a+", encoding="utf-8") as held,
        pytest.raises(DRIVER.DriverError, match="another hardware environment driver"),
    ):
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with store.locked():
            pass


def test_driver_state_compacts_legacy_cleaned_receipts_on_load(tmp_path: Path) -> None:
    store = DRIVER.StateStore(tmp_path / "state")
    cleanup_response = {
        "schema_version": DRIVER.RESPONSE_SCHEMA,
        "request_id": "cleanup-request",
        "status": "SUCCEEDED",
        "events": [],
    }
    DRIVER._write_json(
        store.path,
        {
            "schema_version": DRIVER.STATE_SCHEMA,
            "runs": {
                "campaign\u0000H2\u0000run": {
                    "attempt": 2,
                    "job_id": "job-a",
                    "run_id": "run-a",
                    "cleaned": True,
                    "cleanup_request_id": "cleanup-request",
                    "receipts": {
                        "measure-request": {
                            "request_digest": "measure-digest",
                            "response": {"events": [{"payload": "x" * DRIVER.MAX_JSON_BYTES}]},
                        },
                        "cleanup-request": {
                            "request_digest": "cleanup-digest",
                            "response": cleanup_response,
                        },
                    },
                }
            },
        },
    )
    assert store.path.stat().st_size > DRIVER.MAX_JSON_BYTES

    with store.locked() as state:
        run = state["runs"]["campaign\u0000H2\u0000run"]
        assert run["receipts"] == {
            "cleanup-request": {
                "request_digest": "cleanup-digest",
                "response": cleanup_response,
            }
        }

    assert store.path.stat().st_size < DRIVER.MAX_JSON_BYTES


def test_hardware_job_identity_is_stable_and_attempt_scoped() -> None:
    request = _request("provision", 1)
    first = DRIVER._job_id(request, 1, "attempt-a")
    assert first == DRIVER._job_id(request, 1, "attempt-a")
    assert first != DRIVER._job_id(request, 2, "attempt-a")
    assert first != DRIVER._job_id(request, 1, "attempt-b")
    assert DRIVER._operation_key(request, {"attempt": 1, "attempt_id": "attempt-a"}, "create") != (
        DRIVER._operation_key(request, {"attempt": 1, "attempt_id": "attempt-b"}, "create")
    )
    changed = dict(request)
    changed["run_key"] = "other-run"
    assert first != DRIVER._job_id(changed, 1, "attempt-a")
    assert len(first) <= 63


def test_driver_checkpoints_job_identity_before_gateway_create(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    request = _request("provision", 1)
    calls: list[tuple[str, str]] = []

    def fail_create(_self: Any, path: str, body: JsonObject, key: str) -> JsonObject:
        calls.append((body["jobId"], key))
        raise DRIVER.DriverError("response lost after create")

    monkeypatch.setattr(DRIVER.Gateway, "post", fail_create)
    with pytest.raises(DRIVER.DriverError, match="response lost after create"):
        _execute(driver, root, request)
    state = json.loads((root / "state/state.json").read_text(encoding="utf-8"))
    persisted = next(iter(state["runs"].values()))
    assert persisted["attempt_id"] == "0123456789abcdef"
    assert persisted["job_id"] == DRIVER._job_id(request, 1, persisted["attempt_id"])
    assert persisted["job_request_digest"]
    retry_root = root / "retry-after-response-loss"
    retry_root.mkdir()
    with pytest.raises(DRIVER.DriverError, match="response lost after create"):
        driver.execute(request, retry_root / "response.json")
    assert calls[0] == calls[1]


def test_driver_runs_e2_through_gateway_dra_worker_and_scheduler_hook(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, state, root = driver_environment
    preflight = _request("preflight", 0)
    for field in ("label", "phase", "iteration", "run_key", "step_index"):
        preflight.pop(field)
    response = _execute(driver, root, preflight)
    assert response["status"] == "SUCCEEDED"
    assert response["environment_fingerprint"]["gpu_profile"] == "mig"
    assert response["environment_fingerprint"]["accelerator_count"] == 2

    operations = [
        _request("provision", 1),
        _request("apply_action", 2, action="bind"),
        _request("launch", 3),
        _request("apply_action", 4, action="rebind"),
        _request("verify_device_identity", 5),
        _request("measure", 6),
        _request("stop", 7),
        _request("cleanup", 8),
    ]
    responses = [_execute(driver, root, value) for value in operations]
    assert all(value["status"] == "SUCCEEDED" for value in responses)
    assert state.marker.is_file()
    action = responses[3]["events"][-1]
    assert action["action"] == "rebind" and action["decision_id"] == "decision-2"
    identity = responses[4]["events"][0]
    assert identity["scheduler_device_ids"] == ["MIG-2/1/0"]
    assert identity["allocated_device_ids"] == identity["worker_device_ids"]
    assert identity["device_class"] == "mig.nvidia.com"
    assert identity["parent_uuid"] == "GPU-parent"
    measured = {event["event_type"] for event in responses[5]["events"]}
    assert {"sample_consumed", "workload_completed"}.issubset(measured)
    assert any(
        method == "POST" and path.endswith("/commands/start") for method, path in state.requests
    )
    assert any(
        method == "POST" and path.endswith("/commands/stop") for method, path in state.requests
    )


@pytest.mark.parametrize("pending_polls", [0, 2])
def test_bind_stops_polling_when_start_operation_fails(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
    pending_polls: int,
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    original_get = DRIVER.Gateway.get
    operation_polls = 0
    decision_polls = 0

    def get(self: Any, path: str) -> JsonObject:
        nonlocal operation_polls, decision_polls
        if path == "/v1/operations/op-start":
            operation_polls += 1
            return {
                "operation": {
                    "state": (
                        "OPERATION_STATE_RUNNING"
                        if operation_polls <= pending_polls
                        else "OPERATION_STATE_FAILED"
                    ),
                    "errorMessage": "invalid execution contract",
                }
            }
        if "/decisions?" in path:
            decision_polls += 1
            return {"decisions": []}
        return cast(JsonObject, original_get(self, path))

    monkeypatch.setattr(DRIVER.Gateway, "get", get)
    with pytest.raises(DRIVER.TerminalDriverError, match="invalid execution contract"):
        _execute(driver, root, _request("apply_action", 2, action="bind"))
    assert operation_polls == pending_polls + 1
    assert decision_polls == pending_polls


def test_bind_does_not_wait_for_start_convergence_before_accepting_decision(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    original_get = DRIVER.Gateway.get

    def get(self: Any, path: str) -> JsonObject:
        if path == "/v1/operations/op-start":
            return {"operation": {"state": "OPERATION_STATE_RUNNING"}}
        return cast(JsonObject, original_get(self, path))

    monkeypatch.setattr(DRIVER.Gateway, "get", get)
    response = _execute(driver, root, _request("apply_action", 2, action="bind"))
    assert response["status"] == "SUCCEEDED"
    assert response["events"][-1]["decision_id"] == "decision-1"


def test_driver_replays_receipt_without_repeating_side_effect(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, state, root = driver_environment
    request = _request("provision", 1)
    first = _execute(driver, root, request)
    create_count = state.requests.count(("POST", "/v1/jobs"))
    replay = cast(JsonObject, driver.execute(request, root / "replay" / "response.json"))
    assert first["artifacts"][0]["path"] == "rendered-job.json"
    assert replay == {key: value for key, value in first.items() if key != "artifacts"}
    assert state.requests.count(("POST", "/v1/jobs")) == create_count


def test_driver_starts_new_attempt_after_completed_cleanup(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, state, root = driver_environment
    provision = _request("provision", 1)
    first = _execute(driver, root, provision)
    _execute(driver, root, _request("apply_action", 2, action="bind"))
    _execute(driver, root, _request("cleanup", 8))

    replay_root = root / "second-attempt"
    replay_root.mkdir()
    second = cast(
        JsonObject,
        driver.execute(provision, replay_root / "response.json"),
    )
    bind_root = root / "second-bind"
    bind_root.mkdir()
    driver.execute(_request("apply_action", 2, action="bind"), bind_root / "response.json")

    assert first["status"] == second["status"] == "SUCCEEDED"
    assert state.requests.count(("POST", "/v1/jobs")) == 2
    assert state.created_job_ids == [
        DRIVER._job_id(provision, 1, "0123456789abcdef"),
        DRIVER._job_id(provision, 2, "fedcba9876543210"),
    ]
    persisted = json.loads((root / "state/state.json").read_text(encoding="utf-8"))
    run = next(iter(persisted["runs"].values()))
    assert run["attempt"] == 2
    assert run["attempt_id"] == "fedcba9876543210"
    assert run["job_id"] == DRIVER._job_id(provision, 2, "fedcba9876543210")
    start_keys = [key for path, key in state.idempotency_keys if path.endswith("/commands/start")]
    assert len(start_keys) == 2
    assert start_keys[0].endswith("-a1-i0123456789abcdef-start")
    assert start_keys[1].endswith("-a2-ifedcba9876543210-start")

    cleanup = _request("cleanup", 8)
    cleanup_root = root / "second-cleanup"
    cleanup_root.mkdir()
    first_cleanup = cast(
        JsonObject,
        driver.execute(cleanup, cleanup_root / "response.json"),
    )
    replay_root = root / "replayed-cleanup"
    replay_root.mkdir()
    replayed_cleanup = cast(
        JsonObject,
        driver.execute(cleanup, replay_root / "response.json"),
    )
    assert replayed_cleanup == first_cleanup
    persisted = json.loads((root / "state/state.json").read_text(encoding="utf-8"))
    run = next(iter(persisted["runs"].values()))
    assert list(run["receipts"]) == [cleanup["request_id"]]


def test_cleanup_accepts_unmaterialized_failed_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = object.__new__(DRIVER.HardwareEnvironmentDriver)
    target = SimpleNamespace()
    monkeypatch.setattr(driver, "_bundles", lambda *_args, **_kwargs: [])

    def no_materialized(*_args: object, **_kwargs: object) -> None:
        raise DRIVER.DriverError("run 'run-a' has no materialized sandboxes")

    monkeypatch.setattr(driver, "_command", no_materialized)

    assert (
        driver._cleanup(
            {},
            target,
            {"job_id": "job-a", "run_id": "run-a"},
            Path("/tmp/unused-response.json"),
        )
        == []
    )


def test_cleanup_rejects_unmaterialized_error_when_bundle_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = object.__new__(DRIVER.HardwareEnvironmentDriver)
    target = SimpleNamespace()
    monkeypatch.setattr(driver, "_bundles", lambda *_args, **_kwargs: [{}])

    def no_materialized(*_args: object, **_kwargs: object) -> None:
        raise DRIVER.DriverError("run 'run-a' has no materialized sandboxes")

    monkeypatch.setattr(driver, "_command", no_materialized)

    with pytest.raises(DRIVER.DriverError, match="no materialized sandboxes"):
        driver._cleanup(
            {},
            target,
            {"job_id": "job-a", "run_id": "run-a"},
            Path("/tmp/unused-response.json"),
        )


def test_cleanup_accepts_already_terminal_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = object.__new__(DRIVER.HardwareEnvironmentDriver)
    target = SimpleNamespace()
    monkeypatch.setattr(driver, "_bundles", lambda *_args, **_kwargs: [])

    def already_failed(*_args: object, **_kwargs: object) -> None:
        raise DRIVER.DriverError("cannot terminate run in state JOB_RUN_STATE_FAILED")

    monkeypatch.setattr(driver, "_command", already_failed)

    assert (
        driver._cleanup(
            {},
            target,
            {"job_id": "job-a", "run_id": "run-a"},
            Path("/tmp/unused-response.json"),
        )
        == []
    )


def test_driver_preflight_accepts_full_gpu_inventory(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, _state, root = driver_environment
    request = _request("preflight", 0)
    request["experiment_id"] = "E1"
    request["scenario"] = json.loads(
        (ROOT / "configs/scenarios/e1-full-gpu.yaml").read_text(encoding="utf-8")
    )
    for field in ("label", "phase", "iteration", "run_key", "step_index"):
        request.pop(field)
    response = _execute(driver, root, request)
    assert response["status"] == "SUCCEEDED"
    fingerprint = response["environment_fingerprint"]
    assert fingerprint["gpu_profile"] == "full-gpu"
    assert fingerprint["accelerator_count"] == 1
    assert fingerprint["kube_context"] == "test-context"
    assert fingerprint["cluster_uid"] == "uid-kube-system"
    assert fingerprint["kubernetes_server_version"] == "v1.35.1"
    assert fingerprint["namespace_uid"] == "uid-tgsrl-workloads"
    assert fingerprint["environment_config_sha256"] == DRIVER._file_digest(driver.config.path)
    target = driver.config.targets["E1"]
    assert fingerprint["job_template_sha256"] == DRIVER._file_digest(target.job_template)
    assert fingerprint["trace_command_digest"] == DRIVER._canonical_digest(
        list(target.trace_command)
    )
    assert fingerprint["hooks_fingerprint"] == {"actions": {}, "faults": {}}


def test_driver_preflight_accepts_hami_inventory(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, _state, root = driver_environment
    request = _request("preflight", 0)
    request["experiment_id"] = "H1"
    request["scenario"] = json.loads(
        (ROOT / "configs/scenarios/h1-hami-vgpu.yaml").read_text(encoding="utf-8")
    )
    for field in ("label", "phase", "iteration", "run_key", "step_index"):
        request.pop(field)

    response = _execute(driver, root, request)

    assert response["status"] == "SUCCEEDED"
    fingerprint = response["environment_fingerprint"]
    assert fingerprint["execution_mode"] == "hami-vgpu"
    assert fingerprint["gpu_profile"] == "full-gpu"
    assert fingerprint["accelerator_count"] == 1
    preflight = json.loads(
        (root / request["request_id"] / "preflight.json").read_text(encoding="utf-8")
    )
    assert preflight["inventory"][0]["uuid"] == "GPU-full"
    assert preflight["inventory"][0]["memory_mib"] == 23028


def test_hami_inventory_and_allocation_parsers_preserve_share_identity() -> None:
    registration = json.dumps(
        [
            {
                "id": "GPU-a10",
                "count": 10,
                "devmem": 23028,
                "devcore": 100,
                "type": "NVIDIA A10",
                "health": True,
                "mode": "hami-core",
            }
        ]
    )
    inventory = DRIVER._hami_inventory(
        {
            "items": [
                {
                    "metadata": {
                        "name": "gpu-node-a",
                        "annotations": {DRIVER.HAMI_REGISTER_ANNOTATION: registration},
                    },
                    "status": {"allocatable": {DRIVER.HAMI_GPU_RESOURCE: "10"}},
                }
            ]
        }
    )
    assert inventory == [
        {
            "uuid": "GPU-a10",
            "split_count": 10,
            "memory_mib": 23028,
            "core_percent": 100,
            "model": "NVIDIA A10",
            "healthy": True,
            "mode": "hami-core",
            "node_id": "gpu-node-a",
        }
    ]
    assert DRIVER._hami_allocations("GPU-a10,NVIDIA A10,9211,40:;") == [
        {
            "uuid": "GPU-a10",
            "type": "NVIDIA A10",
            "memory_mib": 9211,
            "core_percent": 40,
        }
    ]


def _hami_bundle_and_pod(*, allocated_core: int = 40) -> tuple[JsonObject, JsonObject]:
    annotations = {
        DRIVER.HAMI_USE_UUID_ANNOTATION: "GPU-a10",
        DRIVER.HAMI_MODE_ANNOTATION: "hami-core",
        DRIVER.HAMI_EXPECTED_CORE_ANNOTATION: "40",
        DRIVER.HAMI_EXPECTED_MEMORY_ANNOTATION: "9211",
    }
    bundle = {
        "gpuProfile": DRIVER.HAMI_PROFILE,
        "job": {
            "spec": {
                "template": {
                    "metadata": {"annotations": annotations},
                    "spec": {
                        "schedulerName": DRIVER.HAMI_SCHEDULER,
                        "containers": [
                            {
                                "resources": {
                                    "limits": {
                                        DRIVER.HAMI_GPU_RESOURCE: "1",
                                        DRIVER.HAMI_CORE_RESOURCE: "40",
                                        DRIVER.HAMI_MEMORY_PERCENT_RESOURCE: "40",
                                    }
                                }
                            }
                        ],
                    },
                }
            }
        },
    }
    pod = {
        "metadata": {
            "annotations": {
                DRIVER.HAMI_ALLOCATED_ANNOTATION: (f"GPU-a10,NVIDIA A10,9211,{allocated_core}:;")
            }
        }
    }
    return cast(JsonObject, bundle), cast(JsonObject, pod)


def test_hami_target_allocation_matches_scheduler_uuid_and_requested_share() -> None:
    bundle, pod = _hami_bundle_and_pod()

    allocated, device_class, parents, evidence = (
        DRIVER.HardwareEnvironmentDriver._hami_target_allocation(bundle, pod, ["GPU-a10"])
    )

    assert allocated == ["GPU-a10"]
    assert device_class == ""
    assert parents == set()
    assert evidence == {
        "allocation_mode": "hami-vgpu",
        "requested_core_percent": 40,
        "allocated_core_percent": 40,
        "requested_memory_mib": 9211,
        "allocated_memory_mib": 9211,
    }


def test_hami_target_allocation_rejects_actual_share_mismatch() -> None:
    bundle, pod = _hami_bundle_and_pod(allocated_core=39)

    with pytest.raises(DRIVER.DriverError, match="requested memory/core share"):
        DRIVER.HardwareEnvironmentDriver._hami_target_allocation(bundle, pod, ["GPU-a10"])


def test_hami_target_allocation_rejects_multiple_scheduler_devices() -> None:
    bundle, pod = _hami_bundle_and_pod()

    with pytest.raises(DRIVER.DriverError, match="exactly one Scheduler device UUID"):
        DRIVER.HardwareEnvironmentDriver._hami_target_allocation(bundle, pod, ["GPU-a10", "GPU-b"])


def test_worker_concurrency_requires_overlapping_complete_intervals() -> None:
    events = [
        {
            "event_type": "sample_consumed",
            "sandbox_id": "sandbox-a",
            "occurred_at": "2026-09-12T10:00:01Z",
            "duration_ms": 1000.0,
        },
        {
            "event_type": "workload_completed",
            "sandbox_id": "sandbox-a",
            "occurred_at": "2026-09-12T10:00:05Z",
        },
        {
            "event_type": "sample_consumed",
            "sandbox_id": "sandbox-b",
            "occurred_at": "2026-09-12T10:00:03Z",
            "duration_ms": 1000.0,
        },
        {
            "event_type": "workload_completed",
            "sandbox_id": "sandbox-b",
            "occurred_at": "2026-09-12T10:00:06Z",
        },
    ]

    overlap_ms, sandboxes = DRIVER._worker_concurrency_overlap_ms(events, required_workers=2)

    assert overlap_ms == 3000.0
    assert sandboxes == ["sandbox-a", "sandbox-b"]


def test_worker_concurrency_rejects_sequential_workers() -> None:
    events = [
        {
            "event_type": "sample_consumed",
            "sandbox_id": "sandbox-a",
            "occurred_at": "2026-09-12T10:00:01Z",
            "duration_ms": 1000.0,
        },
        {
            "event_type": "workload_completed",
            "sandbox_id": "sandbox-a",
            "occurred_at": "2026-09-12T10:00:02Z",
        },
        {
            "event_type": "sample_consumed",
            "sandbox_id": "sandbox-b",
            "occurred_at": "2026-09-12T10:00:04Z",
            "duration_ms": 1000.0,
        },
        {
            "event_type": "workload_completed",
            "sandbox_id": "sandbox-b",
            "occurred_at": "2026-09-12T10:00:05Z",
        },
    ]

    with pytest.raises(DRIVER.DriverError, match="do not overlap"):
        DRIVER._worker_concurrency_overlap_ms(events, required_workers=2)


def test_hami_identity_event_includes_allocation_share_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = object.__new__(DRIVER.HardwareEnvironmentDriver)
    target = SimpleNamespace(
        gateway_url="http://127.0.0.1:1",
        timeout=1,
        poll_interval=0.01,
    )
    allocation = {
        "sandbox_id": "sandbox-a",
        "binding_id": "binding-a",
        "generation": 1,
        "node_id": "gpu-node-a",
        "runtime_unit_id": "unit-a",
        "worker_id": "unit-a",
        "scheduler_device_ids": ["GPU-a10"],
        "allocated_device_ids": ["GPU-a10"],
        "worker_device_ids": ["GPU-a10"],
        "device_class": "",
        "parent_uuid": "",
        "allocation_mode": "hami-vgpu",
        "requested_core_percent": 40,
        "allocated_core_percent": 40,
        "requested_memory_mib": 9211,
        "allocated_memory_mib": 9211,
    }
    monkeypatch.setattr(
        DRIVER,
        "Gateway",
        lambda *_args: SimpleNamespace(get=lambda _path: {"sandboxes": []}),
    )
    monkeypatch.setattr(
        driver,
        "_kubernetes_targets",
        lambda *_args, **_kwargs: [allocation],
    )

    events = driver._verify_device_identity(
        {"scenario": {"topology": {"minimum_nodes": 1}}},
        target,
        {"job_id": "job-a", "run_id": "run-a"},
        tmp_path / "response.json",
    )

    assert events[0]["allocation_mode"] == "hami-vgpu"
    assert events[0]["requested_core_percent"] == 40
    assert events[0]["allocated_memory_mib"] == 9211
    artifact = json.loads((tmp_path / "device-identity.json").read_text(encoding="utf-8"))
    assert artifact["targets"][0]["allocated_core_percent"] == 40


def test_driver_fails_closed_when_rebind_hook_is_missing(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    _execute(driver, root, _request("apply_action", 2, action="bind"))
    driver.config.targets["E2"].action_hooks.clear()
    with pytest.raises(DRIVER.DriverError, match="submits observations to the Scheduler"):
        _execute(driver, root, _request("apply_action", 4, action="rebind"))


def test_driver_rejects_hook_without_scheduler_observation_authority(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    _execute(driver, root, _request("apply_action", 2, action="bind"))
    invalid_hook = root / "invalid-hook"
    _write_invalid_hook(invalid_hook)
    driver.config.targets["E2"].action_hooks["rebind"] = (
        str(invalid_hook),
        "--request",
        "${REQUEST_PATH}",
        "--response",
        "${RESPONSE_PATH}",
    )
    with pytest.raises(DRIVER.DriverError, match="invalid authority receipt"):
        _execute(driver, root, _request("apply_action", 4, action="rebind"))


def test_worker_lifecycle_hook_uses_receipt_without_fake_scheduler_action(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    _execute(driver, root, _request("apply_action", 2, action="bind"))
    hook = root / "worker-hook"
    _write_worker_hook(hook)
    driver.config.targets["E2"].action_hooks["checkpoint"] = (
        str(hook),
        "--request",
        "${REQUEST_PATH}",
        "--response",
        "${RESPONSE_PATH}",
    )
    response = _execute(driver, root, _request("apply_action", 4, action="checkpoint"))
    event = response["events"][-1]
    assert event["action"] == "checkpoint"
    assert event["receipt_id"] == "worker-receipt"
    assert event["decision_id"] == "decision-1"


def test_scoped_worker_action_uses_bundle_credential_without_exposing_it(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    token = "scoped-worker-token-that-must-not-leak"
    bundle_registry_url = "http://tgsrl-scheduler:50091"
    registry_url = "http://127.0.0.1:15091"
    driver.config.targets["E2"] = replace(
        driver.config.targets["E2"], worker_registry_url=registry_url
    )
    bundle = {
        "spec": {
            "bundle": {
                "generation": 1,
                "runtimeTargets": [
                    {
                        "runtimeUnitId": "unit-a",
                        "sandboxId": "sandbox-a",
                        "generation": 1,
                    }
                ],
                "job": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "env": [
                                            {
                                                "name": "TGSRL_WORKER_REGISTRY_TOKEN",
                                                "value": token,
                                            },
                                            {
                                                "name": "TGSRL_WORKER_REGISTRY_URL",
                                                "value": bundle_registry_url,
                                            },
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        }
    }
    captured: dict[str, Any] = {}
    monkeypatch.setattr(driver, "_bundles", lambda *_args, **_kwargs: [bundle])

    def registry_request(
        actual_url: str,
        endpoint: str,
        actual_token: str,
        body: JsonObject,
        timeout: float,
    ) -> JsonObject:
        captured.setdefault("calls", []).append(
            {
                "url": actual_url,
                "endpoint": endpoint,
                "token": actual_token,
                "body": body,
                "timeout": timeout,
            }
        )
        return {
            "accepted": True,
            "gpu_memory_observed_before": True,
            "gpu_memory_allocated_before_bytes": 536870912,
            "gpu_memory_reserved_before_bytes": 603979776,
            "worker": {
                "sandbox_id": "sandbox-a",
                "generation": 1,
                "state": "sleeping",
                "safe_point": True,
                "offloaded": True,
                "ready": False,
                "checkpoint_ref": "/tmp/checkpoints/global_step_1",
                "gpu_memory_observed": True,
                "gpu_memory_allocated_bytes": 0,
                "gpu_memory_reserved_bytes": 0,
            },
        }

    monkeypatch.setattr(
        DRIVER.HardwareEnvironmentDriver,
        "_worker_registry_request",
        staticmethod(registry_request),
    )
    run = {"job_id": "job-a", "run_id": "run-a", "attempt": 1}
    response_root = root / "worker-action"
    response_root.mkdir()
    events = driver._apply_worker_action(
        _request("apply_worker_action", 4, action="offload"),
        driver.config.targets["E2"],
        run,
        response_root / "response.json",
    )

    assert [call["endpoint"] for call in captured["calls"]] == ["/v1/workers/action"]
    action_call = captured["calls"][0]
    assert action_call["url"] == registry_url
    assert action_call["token"] == token
    assert action_call["body"] == {
        "action": "offload",
        "sandbox_id": "sandbox-a",
        "generation": 1,
        "idempotency_key": "hardware-request-4-apply_worker_action-offload-a1-offload",
    }
    assert len(events) == 1
    assert events[0]["action"] == "offload"
    assert events[0]["checkpoint_present"] is True
    assert events[0]["gpu_memory_allocated_before_bytes"] == 536870912
    assert events[0]["gpu_memory_allocated_after_bytes"] == 0
    assert events[0]["gpu_memory_reserved_before_bytes"] == 603979776
    assert events[0]["gpu_memory_reserved_after_bytes"] == 0
    assert token not in json.dumps(events)


def test_worker_registry_url_rejects_credentials() -> None:
    with pytest.raises(DRIVER.DriverError, match="URL is invalid"):
        DRIVER.HardwareEnvironmentDriver._worker_registry_call(
            "http://user:password@127.0.0.1:50091",
            "token",
            {},
            1,
        )


@pytest.mark.parametrize(
    "value",
    (
        "http://user:password@registry.example.test",
        "http://registry.example.test?token=value",
        "ftp://registry.example.test",
    ),
)
def test_worker_registry_override_rejects_unsafe_urls(value: str) -> None:
    with pytest.raises(DRIVER.DriverError, match="worker_registry_url"):
        DRIVER._optional_http_base_url(value, label="targets.E1.worker_registry_url")


def test_scoped_worker_action_rejects_action_identity_mismatch(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    bundle = {
        "spec": {
            "bundle": {
                "generation": 1,
                "runtimeTargets": [{"sandboxId": "sandbox-a", "generation": 1}],
                "job": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "env": [
                                            {
                                                "name": "TGSRL_WORKER_REGISTRY_TOKEN",
                                                "value": "scoped-worker-token",
                                            },
                                            {
                                                "name": "TGSRL_WORKER_REGISTRY_URL",
                                                "value": "http://127.0.0.1:50091",
                                            },
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        }
    }
    monkeypatch.setattr(driver, "_bundles", lambda *_args, **_kwargs: [bundle])
    monkeypatch.setattr(
        DRIVER.HardwareEnvironmentDriver,
        "_worker_registry_request",
        staticmethod(
            lambda *_args: {
                "accepted": True,
                "gpu_memory_observed_before": True,
                "worker": {
                    "sandbox_id": "sandbox-other",
                    "generation": 1,
                    "gpu_memory_observed": True,
                },
            }
        ),
    )

    with pytest.raises(DRIVER.DriverError, match="response identity"):
        driver._apply_worker_action(
            _request("apply_worker_action", 4, action="offload"),
            driver.config.targets["E2"],
            {"job_id": "job-a", "run_id": "run-a", "attempt": 1},
            root / "unused-response.json",
        )


def test_scoped_pause_does_not_require_gpu_memory_observation(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    bundle = {
        "spec": {
            "bundle": {
                "generation": 1,
                "runtimeTargets": [{"sandboxId": "sandbox-a", "generation": 1}],
                "job": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "env": [
                                            {
                                                "name": "TGSRL_WORKER_REGISTRY_TOKEN",
                                                "value": "scoped-worker-token",
                                            },
                                            {
                                                "name": "TGSRL_WORKER_REGISTRY_URL",
                                                "value": "http://127.0.0.1:50091",
                                            },
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        }
    }
    monkeypatch.setattr(driver, "_bundles", lambda *_args, **_kwargs: [bundle])
    monkeypatch.setattr(
        DRIVER.HardwareEnvironmentDriver,
        "_worker_registry_request",
        staticmethod(
            lambda *_args: {
                "accepted": True,
                "worker": {
                    "sandbox_id": "sandbox-a",
                    "generation": 1,
                    "state": "paused",
                    "safe_point": True,
                    "ready": False,
                },
            }
        ),
    )

    events = driver._apply_worker_action(
        _request("apply_worker_action", 4, action="pause"),
        driver.config.targets["E2"],
        {"job_id": "job-a", "run_id": "run-a", "attempt": 1},
        root / "unused-response.json",
    )

    assert events[0]["action"] == "pause"
    assert "gpu_memory_allocated_before_bytes" not in events[0]


def test_driver_rejects_resourceclaim_device_identity_mismatch(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    _execute(driver, root, _request("apply_action", 2, action="bind"))
    _execute(driver, root, _request("launch", 3))
    original = DRIVER._visible_device_ids
    monkeypatch.setattr(DRIVER, "_visible_device_ids", lambda _output, _profile: ["MIG-other"])
    with pytest.raises(DRIVER.DriverError, match="differs across Scheduler"):
        _execute(driver, root, _request("verify_device_identity", 5))
    monkeypatch.setattr(DRIVER, "_visible_device_ids", original)


def test_driver_cleanup_rejects_reused_object_name(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    monkeypatch.setenv("TGSRL_FAKE_BAD_OWNER", "1")
    with pytest.raises(DRIVER.DriverError, match="ownership labels do not match"):
        _execute(driver, root, _request("cleanup", 8))


def test_driver_cleanup_removes_bundle_finalizer_before_delete(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, root = driver_environment
    _execute(driver, root, _request("provision", 1))
    calls: list[list[str]] = []
    original_run = DRIVER.Kubernetes.run

    def record_run(self: Any, argv: list[str], **kwargs: Any) -> str:
        calls.append(list(argv))
        return cast(str, original_run(self, argv, **kwargs))

    monkeypatch.setattr(DRIVER.Kubernetes, "run", record_run)
    _execute(driver, root, _request("cleanup", 8))
    patch_index = next(
        index for index, argv in enumerate(calls) if argv[:2] == ["patch", "jobrunbundles.tgsrl.io"]
    )
    delete_index = next(
        index
        for index, argv in enumerate(calls)
        if argv[:1] == ["delete"]
        and any("/jobrunbundles/" in item for item in argv if item.startswith("--raw="))
    )
    patch_argv = calls[patch_index]
    patch = json.loads(patch_argv[patch_argv.index("--patch") + 1])
    assert patch == [
        {"op": "test", "path": "/metadata/uid", "value": "uid-bundle-1"},
        {"op": "test", "path": "/metadata/resourceVersion", "value": "1"},
        {"op": "add", "path": "/metadata/finalizers", "value": []},
    ]
    assert patch_index < delete_index
    raw_deletes = [argv for argv in calls if argv[:1] == ["delete"]]
    assert raw_deletes
    assert all(any(item.startswith("--raw=") for item in argv) for argv in raw_deletes)


def test_preconditioned_delete_uses_server_identity_and_waits_for_original_object(
    driver_environment: tuple[Any, _GatewayState, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _state, _root = driver_environment
    kube = DRIVER.Kubernetes(driver.config.targets["E2"])
    calls: list[tuple[list[str], str | None]] = []
    original_run = DRIVER.Kubernetes.run

    def record_run(self: Any, argv: list[str], **kwargs: Any) -> str:
        calls.append((list(argv), kwargs.get("input_text")))
        return cast(str, original_run(self, argv, **kwargs))

    monkeypatch.setattr(DRIVER.Kubernetes, "run", record_run)
    live = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "kube-job-1",
            "uid": "uid-kube-job-1",
            "resourceVersion": "1",
        },
    }
    kube.delete_preconditioned("jobs.batch", "kube-job-1", live)
    delete_argv, delete_input = next(item for item in calls if item[0][:1] == ["delete"])
    assert delete_argv[1] == ("--raw=/apis/batch/v1/namespaces/tgsrl-workloads/jobs/kube-job-1")
    assert json.loads(cast(str, delete_input))["preconditions"] == {
        "uid": "uid-kube-job-1",
        "resourceVersion": "1",
    }


def test_preconditioned_delete_rejects_missing_server_identity(
    driver_environment: tuple[Any, _GatewayState, Path],
) -> None:
    driver, _state, _root = driver_environment
    kube = DRIVER.Kubernetes(driver.config.targets["E2"])
    with pytest.raises(DRIVER.DriverError, match="UID"):
        kube.delete_preconditioned(
            "jobs.batch",
            "kube-job-1",
            {"apiVersion": "batch/v1", "metadata": {"resourceVersion": "1"}},
        )
