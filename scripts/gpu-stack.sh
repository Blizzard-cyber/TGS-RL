#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
ACTION=${1:-up}
RUNTIME_ENV=${TGSRL_GPU_RUNTIME_ENV:-.cache/tgsrl/gpu-runtime.env}
KEY_FILE=${TGSRL_GPU_REGISTRY_KEY_FILE:-.cache/tgsrl/worker-registry.key}
KUBECONFIG_FILE=${TGSRL_GPU_KUBECONFIG:-.cache/tgsrl/gpu-kubeconfig}
export TGSRL_REPO_ROOT="$ROOT_DIR"
compose=(docker compose -f compose.yaml -f compose.gpu.yaml)

load_environment() {
  local create_key=${1:-0}
  [[ -f "$RUNTIME_ENV" ]] || { echo "missing $RUNTIME_ENV; run make gpu-render-config" >&2; exit 1; }
  [[ -f "$KUBECONFIG_FILE" ]] || { echo "missing $KUBECONFIG_FILE; run make gpu-configure-access" >&2; exit 1; }
  mkdir -p .cache/tgsrl
  if [[ ! -s "$KEY_FILE" ]]; then
    [[ "$create_key" == 1 ]] || { echo "missing $KEY_FILE; start the stack once with make gpu-up" >&2; exit 1; }
    umask 077
    openssl rand -hex 32 >"$KEY_FILE"
  fi
  set -a
  # shellcheck disable=SC1090
  . "$RUNTIME_ENV"
  set +a
  export TGSRL_WORKER_REGISTRY_SIGNING_KEY
  TGSRL_WORKER_REGISTRY_SIGNING_KEY=$(tr -d '\r\n' <"$KEY_FILE")
  export TGSRL_GPU_REGISTRY_KEY_FILE
  TGSRL_GPU_REGISTRY_KEY_FILE=$(cd "$(dirname "$KEY_FILE")" && pwd)/$(basename "$KEY_FILE")
  export KUBECONFIG
  KUBECONFIG=$(cd "$(dirname "$KUBECONFIG_FILE")" && pwd)/$(basename "$KUBECONFIG_FILE")
}

case "$ACTION" in
  up)
    load_environment 1
    if "${compose[@]}" ps --status running --services | grep -q .; then
      echo 'GPU control-plane containers are already running; use make gpu-down before starting a fresh evidence run' >&2
      exit 1
    fi
    "${compose[@]}" up -d --build --wait scheduler runtime job-controller operator gateway console
    curl -fsS --connect-timeout 3 "${TGSRL_WORKER_REGISTRY_URL%/}/healthz" >/dev/null || {
      echo "worker registry is not reachable at $TGSRL_WORKER_REGISTRY_URL" >&2
      exit 1
    }
    printf 'GPU control plane is ready; Console: http://127.0.0.1:4173\n'
    ;;
  status)
    load_environment
    "${compose[@]}" ps
    kubectl get deviceclass,resourceslice
    exec kubectl -n "${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}" get resourceclaim,workload,job,pod,jobrunbundle
    ;;
  down)
    if [[ -f "$RUNTIME_ENV" && -f "$KUBECONFIG_FILE" && -s "$KEY_FILE" ]]; then
      load_environment
    else
      export TGSRL_WORKER_REGISTRY_SIGNING_KEY=unused-for-compose-down
      export TGSRL_BOOTSTRAP_IMAGE=example.invalid/bootstrap@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      export TGSRL_WORKER_REGISTRY_URL=http://127.0.0.1:50091
      export TGSRL_GPU_REGISTRY_KEY_FILE=/dev/null
      export KUBECONFIG=/dev/null
    fi
    exec "${compose[@]}" down
    ;;
  *) echo 'usage: scripts/gpu-stack.sh up|status|down' >&2; exit 2 ;;
esac
