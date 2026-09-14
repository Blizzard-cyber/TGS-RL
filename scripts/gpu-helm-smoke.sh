#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
VALUES=${TGSRL_GPU_HELM_VALUES:-.cache/tgsrl/gpu-helm-values.yaml}
NAMESPACE=${TGSRL_HELM_NAMESPACE:-${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}}
OUTPUT=${TGSRL_GPU_HELM_EVIDENCE_DIR:-.cache/tgsrl/helm-smoke}
TIMEOUT=${TGSRL_HELM_TIMEOUT:-15m}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}}
GATEWAY_PORT=${TGSRL_HELM_GATEWAY_PORT:-18080}
CONSOLE_PORT=${TGSRL_HELM_CONSOLE_PORT:-14173}
REGISTRY_PORT=${TGSRL_HELM_REGISTRY_PORT:-15091}
DRIVER_CONFIG=${TGSRL_GPU_HELM_DRIVER_CONFIG:-$OUTPUT/hardware-environment.json}
DRIVER_STATE=${TGSRL_GPU_HELM_DRIVER_STATE_DIR:-$OUTPUT/hardware-driver-state}
RUNTIME_ENV=${TGSRL_GPU_HELM_RUNTIME_ENV:-$OUTPUT/runtime.env}
RELEASE=${TGSRL_HELM_RELEASE:-tgsrl}
KEY_FILE=${TGSRL_GPU_REGISTRY_KEY_FILE:-.cache/tgsrl/worker-registry.key}
export TGSRL_HELM_NAMESPACE="$NAMESPACE"
export TGSRL_WORKLOAD_NAMESPACE="$NAMESPACE"

[[ -f "$VALUES" ]] || { echo "missing $VALUES; run make gpu-render-helm-values" >&2; exit 1; }
current_context=$(kubectl config current-context)
[[ "$current_context" == "$EXPECTED_CONTEXT" ]] || { echo "refusing to deploy to context $current_context; expected $EXPECTED_CONTEXT" >&2; exit 1; }
if [[ -d "$OUTPUT" && -n $(find "$OUTPUT" -mindepth 1 -print -quit) ]]; then
  echo "refusing to overwrite Helm evidence in $OUTPUT; archive it or set TGSRL_GPU_HELM_EVIDENCE_DIR" >&2
  exit 1
fi
mkdir -p "$OUTPUT"
gateway_pid=
console_pid=
registry_pid=

capture_evidence() {
  local exit_code=$1
  helm status "$RELEASE" --namespace "$NAMESPACE" -o json \
    >"$OUTPUT/helm-status.json" 2>"$OUTPUT/helm-status.stderr.log" || true
  helm get values "$RELEASE" --namespace "$NAMESPACE" --all \
    >"$OUTPUT/helm-values.yaml" 2>"$OUTPUT/helm-values.stderr.log" || true
  kubectl -n "$NAMESPACE" get deployment,service,pvc,networkpolicy -o json \
    >"$OUTPUT/resources.json" 2>"$OUTPUT/resources.stderr.log" || true
  kubectl -n "$NAMESPACE" get pods \
    -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,READY:.status.containerStatuses[*].ready,NODE:.spec.nodeName,RESTARTS:.status.containerStatuses[*].restartCount' \
    >"$OUTPUT/pods.txt" 2>"$OUTPUT/pods.stderr.log" || true
  kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/part-of=tgsrl \
    --all-containers=true --prefix=true --tail=1000 \
    >"$OUTPUT/control-plane.log" 2>"$OUTPUT/control-plane.stderr.log" || true
  python3 - "$OUTPUT" "$exit_code" "$KEY_FILE" <<'PY'
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
exit_code = int(sys.argv[2])
key_path = Path(sys.argv[3])
secrets = []
if key_path.is_file():
    value = key_path.read_bytes().strip()
    if len(value) >= 8:
        secrets.append(value)
text_suffixes = {".json", ".log", ".txt", ".yaml", ".yml"}
secret_patterns = (
    (
        re.compile(
            rb"(?i)(authorization|bearer_token|password|private_key|refresh_token|session_token)"
            rb"[\"'= :]+[^,\s\"']+"
        ),
        rb"\1=[REDACTED]",
    ),
    (
        re.compile(
            rb"(?i)(TGSRL_WORKER_REGISTRY_(TOKEN|SIGNING_KEY)|X-TGSRL-Worker-Token)"
            rb"[\"'= :]+[A-Za-z0-9_-]{20,}"
        ),
        rb"\1=[REDACTED]",
    ),
    (
        re.compile(
            rb'(?i)("name"\s*:\s*"TGSRL_WORKER_(REGISTRY|TRACE)_TOKEN"\s*,'
            rb'\s*"value"\s*:\s*")[^"]+(")'
        ),
        rb"\1[REDACTED]\3",
    ),
)
files = []
for path in sorted(root.rglob("*")):
    if not path.is_file() or path.name == "evidence-index.json":
        continue
    if path.suffix.lower() in text_suffixes:
        payload = path.read_bytes()
        for secret in secrets:
            payload = payload.replace(secret, b"[REDACTED]")
        for pattern, replacement in secret_patterns:
            payload = pattern.sub(replacement, payload)
        if payload != path.read_bytes():
            path.write_bytes(payload)
            path.chmod(0o600)
    files.append(
        {
            "path": path.relative_to(root).as_posix(),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size_bytes": path.stat().st_size,
        }
    )
payload = {
    "schema_version": "tgsrl.io/helm-smoke-evidence/v1alpha1",
    "status": "PASSED" if exit_code == 0 else "FAILED",
    "exit_code": exit_code,
    "git_commit": subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip(),
    "files": files,
}
index = root / "evidence-index.json"
index.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
index.chmod(0o600)
PY
}

finalize() {
  local status=$? capture_status=0
  trap - EXIT
  [[ -z "$gateway_pid" ]] || kill "$gateway_pid" >/dev/null 2>&1 || true
  [[ -z "$console_pid" ]] || kill "$console_pid" >/dev/null 2>&1 || true
  [[ -z "$registry_pid" ]] || kill "$registry_pid" >/dev/null 2>&1 || true
  [[ -z "$gateway_pid" ]] || wait "$gateway_pid" 2>/dev/null || true
  [[ -z "$console_pid" ]] || wait "$console_pid" 2>/dev/null || true
  [[ -z "$registry_pid" ]] || wait "$registry_pid" 2>/dev/null || true
  set +e
  capture_evidence "$status" || capture_status=$?
  if (( status == 0 && capture_status != 0 )); then
    exit "$capture_status"
  fi
  exit "$status"
}
trap finalize EXIT

scripts/deploy-full-stack.sh render "$VALUES" >"$OUTPUT/rendered.yaml"
if helm status "$RELEASE" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  scripts/deploy-full-stack.sh upgrade "$VALUES"
else
  scripts/deploy-full-stack.sh install "$VALUES"
fi
deployments=(
  deployment/tgsrl-scheduler
  deployment/tgsrl-runtime
  deployment/tgsrl-job-controller
  deployment/tgsrl-operator
  deployment/tgsrl-gateway
  deployment/tgsrl-console
)
for deployment in "${deployments[@]}"; do
  kubectl -n "$NAMESPACE" rollout status "$deployment" --timeout="$TIMEOUT"
done
kubectl -n "$NAMESPACE" get deployment tgsrl-scheduler -o json \
  | jq -e '.spec.template.spec.runtimeClassName != null and (.spec.template.spec.containers[0].args | index("--nvidia-driver-v2=true") != null)' >/dev/null
kubectl -n "$NAMESPACE" get networkpolicy tgsrl-control-plane-ingress tgsrl-console-ingress >/dev/null
TGSRL_HARDWARE_DRIVER_CONFIG="$DRIVER_CONFIG" \
TGSRL_GPU_RUNTIME_ENV="$RUNTIME_ENV" \
TGSRL_HARDWARE_GATEWAY_URL="http://127.0.0.1:$GATEWAY_PORT" \
TGSRL_HARDWARE_WORKER_REGISTRY_URL="http://127.0.0.1:$REGISTRY_PORT" \
TGSRL_HARDWARE_DRIVER_STATE_DIR="$DRIVER_STATE" \
  scripts/gpu-render-config.sh
gateway_log="$OUTPUT/gateway-port-forward.log"
console_log="$OUTPUT/console-port-forward.log"
registry_log="$OUTPUT/registry-port-forward.log"
kubectl -n "$NAMESPACE" port-forward service/tgsrl-gateway "$GATEWAY_PORT:8080" >"$gateway_log" 2>&1 &
gateway_pid=$!
kubectl -n "$NAMESPACE" port-forward service/tgsrl-console "$CONSOLE_PORT:8080" >"$console_log" 2>&1 &
console_pid=$!
kubectl -n "$NAMESPACE" port-forward service/tgsrl-scheduler "$REGISTRY_PORT:50091" >"$registry_log" 2>&1 &
registry_pid=$!
gateway_health_tmp="$OUTPUT/gateway-health.tmp.json"
for _ in $(seq 1 60); do
  if curl -fsS --connect-timeout 2 "http://127.0.0.1:$GATEWAY_PORT/health" >"$gateway_health_tmp" && \
    jq -e '
      .status == "ok"
      and .backend == "grpc"
      and ([.dependencies[] | select(.serving == true) | .name] | sort)
        == ["experiment", "job_control", "runtime", "scheduler"]
    ' "$gateway_health_tmp" >/dev/null && \
    curl -fsS --connect-timeout 2 "http://127.0.0.1:$CONSOLE_PORT/healthz" >/dev/null && \
    curl -fsS --connect-timeout 2 "http://127.0.0.1:$REGISTRY_PORT/healthz" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS --connect-timeout 2 "http://127.0.0.1:$GATEWAY_PORT/health" >"$OUTPUT/gateway-health.json"
jq -e '
  .status == "ok"
  and .backend == "grpc"
  and ([.dependencies[] | select(.serving == true) | .name] | sort)
    == ["experiment", "job_control", "runtime", "scheduler"]
' "$OUTPUT/gateway-health.json" >/dev/null
rm -f "$gateway_health_tmp"
curl -fsS --connect-timeout 2 "http://127.0.0.1:$CONSOLE_PORT/healthz" >"$OUTPUT/console-health.txt"
make gpu-a10-full-readiness \
  TGSRL_HARDWARE_DRIVER_CONFIG="$DRIVER_CONFIG" \
  A10_READINESS_REPORTS="$OUTPUT/a10-readiness"
printf 'single-node NVIDIA Helm and A10 lifecycle smoke passed; evidence: %s\n' "$OUTPUT"
