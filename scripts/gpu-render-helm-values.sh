#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
IMAGE_ENV=${TGSRL_IMAGE_ENV_FILE:-.cache/tgsrl/gpu-images.env}
OUTPUT=${TGSRL_GPU_HELM_VALUES:-.cache/tgsrl/gpu-helm-values.yaml}
KEY_FILE=${TGSRL_GPU_REGISTRY_KEY_FILE:-.cache/tgsrl/worker-registry.key}
NAMESPACE=${TGSRL_HELM_NAMESPACE:-${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}}
current_context=$(kubectl config current-context)
[[ "$current_context" == "$EXPECTED_CONTEXT" ]] || { echo "refusing to configure context $current_context; expected $EXPECTED_CONTEXT" >&2; exit 1; }
NODE_NAME=${TGSRL_GPU_NODE_NAME:-$(kubectl get nodes -o json | jq -r '.items | if length == 1 then .[0].metadata.name else "" end')}
RUNTIME_CLASS=${TGSRL_NVIDIA_RUNTIME_CLASS:-nvidia}
SECRET=${TGSRL_WORKER_REGISTRY_SECRET:-tgsrl-worker-registry}

[[ -f "$IMAGE_ENV" ]] || { echo "missing $IMAGE_ENV; run make gpu-build-images" >&2; exit 1; }
if [[ ! -s "$KEY_FILE" ]]; then
  mkdir -p "$(dirname "$KEY_FILE")"
  umask 077
  openssl rand -hex 32 >"$KEY_FILE"
fi
[[ -n "$NODE_NAME" ]] || { echo 'single-node Helm smoke requires exactly one Kubernetes node or TGSRL_GPU_NODE_NAME' >&2; exit 1; }
kubectl get runtimeclass.node.k8s.io "$RUNTIME_CLASS" >/dev/null 2>&1 || {
  echo "NVIDIA RuntimeClass $RUNTIME_CLASS is missing" >&2
  exit 1
}
set -a
# shellcheck disable=SC1090
. "$IMAGE_ENV"
set +a
for name in SCHEDULER_NVIDIA_IMAGE JOB_CONTROLLER_IMAGE OPERATOR_IMAGE RUNTIME_IMAGE GATEWAY_IMAGE CONSOLE_IMAGE WORKER_BOOTSTRAP_IMAGE; do
  value=${!name:-}
  [[ "$value" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]] || { echo "$name is missing or not immutable" >&2; exit 1; }
done

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create secret generic "$SECRET" \
  --from-file=signing-key="$KEY_FILE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
PULL_SECRET=
if [[ ${TGSRL_PUBLIC_IMAGE_REGISTRY:-0} != 1 ]]; then
  PULL_SECRET=${TGSRL_IMAGE_PULL_SECRET:-tgsrl-registry}
  kubectl -n "$NAMESPACE" get secret "$PULL_SECRET" >/dev/null 2>&1 || {
    echo "missing image pull secret $PULL_SECRET; run make gpu-configure-registry or set TGSRL_PUBLIC_IMAGE_REGISTRY=1" >&2
    exit 1
  }
fi

mkdir -p "$(dirname "$OUTPUT")"
python3 - "$OUTPUT" "$NODE_NAME" "$RUNTIME_CLASS" "$SECRET" "$PULL_SECRET" \
  "$SCHEDULER_NVIDIA_IMAGE" "$JOB_CONTROLLER_IMAGE" "$OPERATOR_IMAGE" \
  "$RUNTIME_IMAGE" "$GATEWAY_IMAGE" "$CONSOLE_IMAGE" "$WORKER_BOOTSTRAP_IMAGE" <<'PY'
import sys
from pathlib import Path

output, node, runtime_class, secret, pull_secret, *images = sys.argv[1:]
names = ("scheduler", "jobController", "operator", "runtime", "gateway", "console", "bootstrap")
parsed = {}
for name, value in zip(names, images, strict=True):
    repository, digest = value.rsplit("@", 1)
    parsed[name] = (repository, digest)

def image(name: str, indent: int) -> list[str]:
    repository, digest = parsed[name]
    spaces = " " * indent
    return [f"{spaces}repository: {repository}", f"{spaces}tag: unused", f"{spaces}digest: {digest}"]

pull_secret_lines = [f"    - name: {pull_secret}"]
global_pull_secret = (
    ["  imagePullSecrets:", *pull_secret_lines]
    if pull_secret
    else ["  imagePullSecrets: []"]
)
operator_pull_secret = (
    ["  imagePullSecrets:", *pull_secret_lines]
    if pull_secret
    else ["  imagePullSecrets: []"]
)
lines = [
    "global:",
    "  imagePullPolicy: IfNotPresent",
    *global_pull_secret,
    "scheduler:",
    "  image:",
    *image("scheduler", 4),
    "  manifest: compatibility/manifests/gpu-smoke-verl.yaml",
    "  workerRegistry:",
    "    enabled: true",
    f"    signingKeySecret: {secret}",
    "  nvidia:",
    "    enabled: true",
    "    image:",
    *image("scheduler", 6),
    "    partitionMode: auto",
    f"    runtimeClassName: {runtime_class}",
    "    nodeSelector:",
    f"      kubernetes.io/hostname: {node}",
    "runtime:",
    "  image:",
    *image("runtime", 4),
    "jobController:",
    "  image:",
    *image("jobController", 4),
    "gateway:",
    "  image:",
    *image("gateway", 4),
    "console:",
    "  image:",
    *image("console", 4),
    "operator:",
    *operator_pull_secret,
    "  image:",
    *image("operator", 4),
    "  controller:",
    "    gpuProfile: kubernetes-dra",
    "    nodeSelector:",
    f"      kubernetes.io/hostname: {node}",
    "    workerBootstrap:",
    "      enabled: true",
    f"      installerImage: {parsed['bootstrap'][0]}@{parsed['bootstrap'][1]}",
    "      registryURL: http://tgsrl-scheduler:50091",
    f"      registrySigningKeySecret: {secret}",
    "      verifyDeviceIdentities: true",
]
target = Path(output)
target.write_text("\n".join(lines) + "\n", encoding="utf-8")
target.chmod(0o600)
print(f"wrote {target}")
PY
