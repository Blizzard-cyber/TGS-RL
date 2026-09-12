#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
IMAGE_ENV=${TGSRL_IMAGE_ENV_FILE:-.cache/tgsrl/gpu-images.env}
OUTPUT=${TGSRL_HARDWARE_DRIVER_CONFIG:-configs/hardware/environment.json}
MINIKUBE_PROFILE=${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}
HOST_GATEWAY=${TGSRL_HOST_GATEWAY:-}
KUBE_CONTEXT=${TGSRL_KUBE_CONTEXT:-$(kubectl config current-context)}
NAMESPACE=${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}
RUNTIME_ENV=${TGSRL_GPU_RUNTIME_ENV:-.cache/tgsrl/gpu-runtime.env}

[[ -f "$IMAGE_ENV" ]] || { echo "missing image output: $IMAGE_ENV; run make gpu-build-images" >&2; exit 1; }
if [[ -z "$HOST_GATEWAY" ]]; then
  HOST_GATEWAY=$(minikube ssh -p "$MINIKUBE_PROFILE" -- 'ip route show default' | awk '/^default / {print $3; exit}')
fi
[[ "$HOST_GATEWAY" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || { echo 'cannot resolve a pod-routable host gateway; set TGSRL_HOST_GATEWAY explicitly' >&2; exit 1; }
set -a
# shellcheck disable=SC1090
. "$IMAGE_ENV"
set +a
: "${GPU_SMOKE_IMAGE:?GPU_SMOKE_IMAGE is missing from $IMAGE_ENV}"
: "${GPU_SMOKE_DIGEST:?GPU_SMOKE_DIGEST is missing from $IMAGE_ENV}"
: "${WORKER_BOOTSTRAP_IMAGE:?WORKER_BOOTSTRAP_IMAGE is missing from $IMAGE_ENV}"
[[ "$GPU_SMOKE_DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]] || { echo 'GPU_SMOKE_DIGEST is invalid' >&2; exit 1; }
[[ "$GPU_SMOKE_IMAGE" == *@"$GPU_SMOKE_DIGEST" ]] || { echo 'GPU_SMOKE_IMAGE does not match GPU_SMOKE_DIGEST' >&2; exit 1; }
[[ "$WORKER_BOOTSTRAP_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]] || { echo 'WORKER_BOOTSTRAP_IMAGE is not immutable' >&2; exit 1; }

mkdir -p "$(dirname "$OUTPUT")" "$(dirname "$RUNTIME_ENV")" .cache/tgsrl/hardware-driver
TGSRL_GPU_RUNTIME_ENV=$RUNTIME_ENV WORKER_BOOTSTRAP_IMAGE=$WORKER_BOOTSTRAP_IMAGE python3 - "$OUTPUT" "$HOST_GATEWAY" "$KUBE_CONTEXT" "$NAMESPACE" "$GPU_SMOKE_IMAGE" "$GPU_SMOKE_DIGEST" <<'PY'
import json
import sys
from pathlib import Path

output, host, context, namespace, image, digest = sys.argv[1:]
payload = {
    "schema_version": "tgsrl.io/hardware-environment/v1alpha1",
    "state_directory": str(Path(".cache/tgsrl/hardware-driver").resolve()),
    "defaults": {
        "gateway_url": "http://127.0.0.1:8080",
        "namespace": namespace,
        "kube_context": context,
        "kubectl": "kubectl",
        "job_template": str(Path("configs/hardware/verl-job.example.json").resolve()),
        "trace_command": ["sh", "-c", "test -f /tmp/tgsrl/verl.ndjson && cat /tmp/tgsrl/verl.ndjson"],
        "variables": {"WORKLOAD_IMAGE": image, "WORKLOAD_IMAGE_DIGEST": digest},
        "operation_timeout_seconds": 1800,
        "poll_interval_seconds": 2
    },
    "targets": {
        "E1": {
            "gpu_profile": "full-gpu",
            "execution_mode": "kubernetes-dra",
        },
        "H1": {
            "gpu_profile": "full-gpu",
            "execution_mode": "hami-vgpu",
            "job_template": str(Path("configs/hardware/hami-job.example.json").resolve()),
        },
    }
}
Path(output).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
env_path = Path(__import__("os").environ.get("TGSRL_GPU_RUNTIME_ENV", ".cache/tgsrl/gpu-runtime.env"))
env_path.write_text(
    "TGSRL_HARDWARE_DRIVER_CONFIG=" + str(Path(output).resolve()) + "\n"
    + "TGSRL_WORKER_REGISTRY_URL=http://" + host + ":50091\n"
    + "TGSRL_BOOTSTRAP_IMAGE=" + __import__("os").environ["WORKER_BOOTSTRAP_IMAGE"] + "\n"
    + "TGSRL_GPU_SMOKE_IMAGE=" + image + "\n",
    encoding="utf-8",
)
env_path.chmod(0o600)
print(f"wrote {output} and {env_path}")
PY
