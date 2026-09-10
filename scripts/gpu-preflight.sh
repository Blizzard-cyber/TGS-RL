#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
NAMESPACE=${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}
PROFILE=${TGSRL_GPU_PROFILE:-full-gpu}
MIN_DRIVER_VERSION=${TGSRL_MIN_NVIDIA_DRIVER_VERSION:-580.95.05}
KUBERNETES_VERSION=${TGSRL_KUBERNETES_VERSION:-v1.35.1}
KUEUE_VERSION=${TGSRL_KUEUE_VERSION:-v0.19.2}
DRA_VERSION=${TGSRL_NVIDIA_DRA_VERSION:-0.5.0}
UV_VERSION=${TGSRL_UV_VERSION:-0.12.7}
HELM_VERSION=${TGSRL_HELM_VERSION:-v4.2.4}
MINIKUBE_VERSION=${TGSRL_MINIKUBE_VERSION:-v1.38.1}
RUNTIME_TEST_IMAGE=${TGSRL_GPU_RUNTIME_TEST_IMAGE:-nvidia/cuda:13.0.2-base-ubuntu24.04@sha256:2ab6381d970b211fb93853796dc6707eb8a72575a375c422b17cf4d8b2641701}
KUBECONFIG_FILE=${TGSRL_GPU_KUBECONFIG:-.cache/tgsrl/gpu-kubeconfig}
IMAGE_PULL_SECRET=${TGSRL_IMAGE_PULL_SECRET:-tgsrl-registry}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}}
failures=0

pass() { printf 'ok: %s\n' "$1"; }
fail() { printf 'error: %s\n' "$1" >&2; failures=$((failures + 1)); }

require_command() {
  if command -v "$1" >/dev/null 2>&1; then
    pass "$1 is available"
  else
    fail "$1 is required"
  fi
}

check() {
  local success=$1 failure=$2
  shift 2
  if "$@" >/dev/null 2>&1; then
    pass "$success"
  else
    fail "$failure"
  fi
}

for command_name in docker kubectl helm minikube nvidia-smi nvidia-ctk jq curl git uv; do
  require_command "$command_name"
done

if (( failures != 0 )); then
  exit 1
fi
client_version=$(kubectl version --client -o json | jq -r '.clientVersion.gitVersion // ""')
if [[ "$client_version" == "$KUBERNETES_VERSION" ]]; then
  pass "kubectl is locked to $client_version"
else
  fail "kubectl $KUBERNETES_VERSION is required; found $client_version"
fi
helm_version=$(helm version --short | sed 's/+.*//')
if [[ "$helm_version" == "$HELM_VERSION" ]]; then
  pass "Helm is locked to $helm_version"
else
  fail "Helm $HELM_VERSION is required; found $helm_version"
fi
minikube_version=$(minikube version --short)
if [[ "$minikube_version" == "$MINIKUBE_VERSION" ]]; then
  pass "Minikube is locked to $minikube_version"
else
  fail "Minikube $MINIKUBE_VERSION is required; found $minikube_version"
fi
toolkit_version=$(nvidia-ctk --version | sed -nE 's/.*version:?[[:space:]]+v?([0-9]+([.][0-9]+){1,2}).*/\1/p' | head -n 1)
if [[ -n "$toolkit_version" && "$(printf '%s\n%s\n' 1.18.0 "$toolkit_version" | sort -V | head -n 1)" == 1.18.0 ]]; then
  pass "NVIDIA Container Toolkit $toolkit_version is supported"
else
  fail "NVIDIA Container Toolkit 1.18.0 or newer is required; found ${toolkit_version:-unknown}"
fi

check 'Docker daemon is reachable' 'Docker daemon is not reachable' docker info
check 'Docker Compose v2 is available' 'Docker Compose v2 is required' docker compose version
check 'Docker Buildx is available' 'Docker Buildx is required' docker buildx version
if [[ $(uv --version | awk '{print $2}') == "$UV_VERSION" ]]; then
  pass "uv $UV_VERSION is installed"
else
  fail "uv $UV_VERSION is required"
fi
check 'host NVIDIA devices are visible' 'nvidia-smi cannot enumerate host GPUs' nvidia-smi -L
if nvidia-ctk cdi list 2>/dev/null | grep -q '^nvidia.com/gpu='; then
  pass 'NVIDIA CDI devices are published'
else
  fail 'NVIDIA CDI specification is missing; rerun make gpu-install-host'
fi

driver_version=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -n 1 | tr -d '[:space:]')
if [[ "$driver_version" =~ ^[0-9]+([.][0-9]+){1,2}$ ]] && \
  [[ "$(printf '%s\n%s\n' "$MIN_DRIVER_VERSION" "$driver_version" | sort -V | head -n 1)" == "$MIN_DRIVER_VERSION" ]]; then
  pass "NVIDIA driver $driver_version satisfies the DRA minimum"
else
  fail "NVIDIA driver $driver_version is older than required $MIN_DRIVER_VERSION"
fi

if [[ ${TGSRL_SKIP_DOCKER_GPU_CHECK:-0} == 1 ]]; then
  pass 'Docker GPU runtime image check was explicitly skipped; DRA workload remains authoritative'
elif [[ ! "$RUNTIME_TEST_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  fail 'TGSRL_GPU_RUNTIME_TEST_IMAGE must be immutable'
elif docker image inspect "$RUNTIME_TEST_IMAGE" >/dev/null 2>&1; then
  check 'Docker NVIDIA runtime works' 'Docker cannot expose GPUs; install/configure NVIDIA Container Toolkit' \
    docker run --rm --pull=never --gpus all --entrypoint nvidia-smi "$RUNTIME_TEST_IMAGE" -L
elif [[ ${TGSRL_ALLOW_IMAGE_PULL:-0} == 1 ]]; then
  check 'Docker NVIDIA runtime works' 'Docker cannot expose GPUs; install/configure NVIDIA Container Toolkit' \
    docker run --rm --gpus all --entrypoint nvidia-smi "$RUNTIME_TEST_IMAGE" -L
else
  fail 'GPU runtime test image is not local; pull it explicitly or set TGSRL_ALLOW_IMAGE_PULL=1'
fi
for path in /usr/bin/nvidia-smi /dev/nvidiactl /dev/nvidia-uvm; do
  if [[ -e "$path" ]]; then
    pass "Scheduler GPU discovery path exists: $path"
  else
    fail "required Scheduler GPU discovery path is missing: $path"
  fi
done
gpu_device_count=$(find /dev -maxdepth 1 -type c -name 'nvidia[0-9]*' | wc -l | tr -d ' ')
if [[ "$gpu_device_count" =~ ^[0-9]+$ ]] && (( gpu_device_count > 0 )); then
  pass "$gpu_device_count NVIDIA GPU device node(s) are available"
else
  fail 'no physical NVIDIA GPU device node is available'
fi

check 'Kubernetes API is reachable' 'Kubernetes API is unreachable' kubectl cluster-info
current_context=$(kubectl config current-context)
if [[ "$current_context" == "$EXPECTED_CONTEXT" ]]; then
  pass "kubectl context is $current_context"
else
  fail "kubectl context $current_context does not match expected $EXPECTED_CONTEXT"
fi
server_version=$(kubectl version -o json | jq -r '.serverVersion.gitVersion // ""')
if [[ "$server_version" == "$KUBERNETES_VERSION" ]]; then
  pass "Kubernetes server is locked to $server_version"
else
  fail "Kubernetes server $server_version does not match locked version $KUBERNETES_VERSION"
fi
if kubectl -n kueue-system get deployment kueue-controller-manager -o json | \
  jq -e --arg version "$KUEUE_VERSION" '[.spec.template.spec.containers[].image] | any(endswith(":" + $version))' >/dev/null; then
  pass "Kueue $KUEUE_VERSION is deployed"
else
  fail "Kueue deployment does not use locked version $KUEUE_VERSION"
fi
if kubectl -n dra-driver-nvidia-gpu get deployment,daemonset -o json | \
  jq -e --arg version "$DRA_VERSION" '[.items[].spec.template.spec.containers[].image] | any(endswith(":v" + $version) or endswith(":" + $version))' >/dev/null; then
  pass "NVIDIA DRA $DRA_VERSION is deployed"
else
  fail "NVIDIA DRA objects do not use locked version $DRA_VERSION"
fi

check "workload namespace $NAMESPACE exists" "workload namespace $NAMESPACE does not exist" kubectl get namespace "$NAMESPACE"
check 'Kueue LocalQueue default exists' 'Kueue LocalQueue default is missing' kubectl -n "$NAMESPACE" get localqueue.kueue.x-k8s.io default
if kubectl get clusterqueue.kueue.x-k8s.io tgsrl-default -o json | \
  jq -e '[.spec.resourceGroups[].coveredResources[]] | index("tgsrl.io/gpu") != null' >/dev/null; then
  pass 'Kueue ClusterQueue covers tgsrl.io/gpu'
else
  fail 'Kueue ClusterQueue does not cover tgsrl.io/gpu'
fi
if kubectl api-resources --api-group=kueue.x-k8s.io -o name | grep -qx 'workloads.kueue.x-k8s.io'; then
  pass 'Kueue Workload API is served'
else
  fail 'Kueue Workload API is not served'
fi
if kubectl api-resources --api-group=resource.k8s.io -o name | grep -qx 'resourceclaims.resource.k8s.io'; then
  pass 'Kubernetes ResourceClaim API is served'
else
  fail 'Kubernetes ResourceClaim API is not served'
fi

device_class=gpu.nvidia.com
device_type=gpu
if [[ "$PROFILE" == mig ]]; then
  device_class=mig.nvidia.com
  device_type=mig
elif [[ "$PROFILE" != full-gpu ]]; then
  fail "TGSRL_GPU_PROFILE must be full-gpu or mig"
fi
check "DeviceClass $device_class exists" "DeviceClass $device_class is missing" kubectl get deviceclass.resource.k8s.io "$device_class"

slice_json=$(kubectl get resourceslices.resource.k8s.io -o json 2>/dev/null || true)
device_count=$(printf '%s' "$slice_json" | jq --arg type "$device_type" '[.items[] | select(.spec.driver == "gpu.nvidia.com") | .spec.devices[]? | ((.attributes // {}) + (.basic.attributes // {})) | select((.type.string // .["gpu.nvidia.com/type"].string) == $type and ((.uuid.string // .["gpu.nvidia.com/uuid"].string // "") | length > 0))] | length' 2>/dev/null || printf '0')
if [[ "$device_count" =~ ^[0-9]+$ ]] && (( device_count > 0 )); then
  pass "NVIDIA DRA publishes $device_count $PROFILE device(s)"
else
  fail "NVIDIA DRA publishes no typed $PROFILE UUIDs"
fi
if kubectl -n kueue-system get configmap kueue-manager-config -o jsonpath='{.data.controller_manager_config\.yaml}' | \
  grep -Fq 'name: tgsrl.io/gpu'; then
  pass 'Kueue maps NVIDIA DeviceClasses to tgsrl.io/gpu'
else
  fail 'Kueue deviceClassMappings does not contain tgsrl.io/gpu'
fi

if [[ -n ${TGSRL_WORKER_REGISTRY_URL:-} ]]; then
  registry_host=$(printf '%s' "$TGSRL_WORKER_REGISTRY_URL" | sed -E 's#^https?://([^:/]+).*#\1#')
  if [[ "$registry_host" != "127.0.0.1" && "$registry_host" != localhost ]]; then
    pass "worker registry uses a pod-routable host: $registry_host"
  else
    fail 'TGSRL_WORKER_REGISTRY_URL must not use localhost or 127.0.0.1'
  fi
else
  fail 'TGSRL_WORKER_REGISTRY_URL is required; source .cache/tgsrl/gpu-runtime.env'
fi
if [[ -n ${TGSRL_BOOTSTRAP_IMAGE:-} && "$TGSRL_BOOTSTRAP_IMAGE" == *@sha256:* ]]; then
  pass 'bootstrap image is immutable'
else
  fail 'TGSRL_BOOTSTRAP_IMAGE must be an immutable image reference'
fi
if [[ -n ${TGSRL_GPU_SMOKE_IMAGE:-} && "$TGSRL_GPU_SMOKE_IMAGE" == *@sha256:* ]]; then
  pass 'GPU smoke workload image is immutable'
else
  fail 'TGSRL_GPU_SMOKE_IMAGE must be an immutable image reference'
fi
if [[ ${TGSRL_PUBLIC_IMAGE_REGISTRY:-0} == 1 ]]; then
  pass 'public image registry mode explicitly skips imagePullSecret checks'
else
  check "image pull secret $IMAGE_PULL_SECRET exists" \
    "image pull secret $IMAGE_PULL_SECRET is missing; run make gpu-configure-registry or set TGSRL_PUBLIC_IMAGE_REGISTRY=1" \
    kubectl -n "$NAMESPACE" get secret "$IMAGE_PULL_SECRET"
  if kubectl -n "$NAMESPACE" get serviceaccount default -o json | \
    jq -e --arg secret "$IMAGE_PULL_SECRET" '[.imagePullSecrets[]?.name] | index($secret) != null' >/dev/null; then
    pass "default service account uses image pull secret $IMAGE_PULL_SECRET"
  else
    fail "default service account does not use image pull secret $IMAGE_PULL_SECRET"
  fi
fi
[[ -f "$KUBECONFIG_FILE" ]] || fail "scoped Operator kubeconfig is missing: $KUBECONFIG_FILE"
if [[ -f "$KUBECONFIG_FILE" ]]; then
  for check in \
    'create jobs.batch' 'delete jobs.batch' \
    'create workloads.kueue.x-k8s.io' 'delete workloads.kueue.x-k8s.io' \
    'create resourceclaims.resource.k8s.io' 'delete resourceclaims.resource.k8s.io' \
    'create jobrunbundles.tgsrl.io' 'delete jobrunbundles.tgsrl.io'; do
    read -r verb resource <<<"$check"
    if KUBECONFIG="$KUBECONFIG_FILE" kubectl auth can-i "$verb" "$resource" -n "$NAMESPACE" | grep -qx yes; then
      pass "external Operator can $verb $resource"
    else
      fail "external Operator cannot $verb $resource in $NAMESPACE"
    fi
  done
  for resource in nodes runtimeclasses.node.k8s.io deviceclasses.resource.k8s.io resourceslices.resource.k8s.io; do
    if KUBECONFIG="$KUBECONFIG_FILE" kubectl auth can-i list "$resource" | grep -qx yes; then
      pass "external Operator can list $resource"
    else
      fail "external Operator cannot list $resource"
    fi
  done
fi

if (( failures != 0 )); then
  printf 'GPU preflight failed with %d error(s)\n' "$failures" >&2
  exit 1
fi
printf 'GPU preflight passed for profile %s\n' "$PROFILE"
