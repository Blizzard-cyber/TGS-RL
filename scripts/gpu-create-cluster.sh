#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. "$ROOT_DIR/scripts/lib/network-profile.sh"
tgsrl_load_network_profile
PROFILE=${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}
KUBERNETES_VERSION=${TGSRL_KUBERNETES_VERSION:-v1.35.1}
CPUS=${TGSRL_MINIKUBE_CPUS:-8}
MEMORY=${TGSRL_MINIKUBE_MEMORY:-24576}
ALLOW_ROOT=${TGSRL_MINIKUBE_ALLOW_ROOT:-0}
MINIKUBE_HOME=${MINIKUBE_HOME:-$HOME/.minikube}
REGISTRY_PROXY_VERSION=${TGSRL_MINIKUBE_REGISTRY_PROXY_VERSION:-v0.0.11}
REGISTRY_PROXY_DIGEST=${TGSRL_MINIKUBE_REGISTRY_PROXY_DIGEST:-sha256:e321acf067df0a78fba3ff97748c10029ca2c413c5b7207e4ca000c62fcdac93}
REGISTRY_FORWARD_PID=${TGSRL_REGISTRY_FORWARD_PID:-$HOME/.cache/tgsrl/registry-forward.pid}
REGISTRY_FORWARD_LOG=${TGSRL_REGISTRY_FORWARD_LOG:-$HOME/.cache/tgsrl/registry-forward.log}

if ! command -v nvidia-smi >/dev/null 2>&1 || ! nvidia-smi -L >/dev/null 2>&1; then
  echo 'a working NVIDIA host driver is required' >&2
  exit 1
fi
docker info >/dev/null 2>&1 || { echo 'Docker is unavailable to the current user; log out/in after gpu-install-host' >&2; exit 1; }

preload_kubernetes_binaries() {
  local cache_dir="$MINIKUBE_HOME/cache/linux/amd64/$KUBERNETES_VERSION" component
  mkdir -p "$cache_dir"
  for component in kubeadm kubelet kubectl; do
    tgsrl_download_verified "$component" "$KUBERNETES_VERSION" \
      "https://dl.k8s.io/release/${KUBERNETES_VERSION}/bin/linux/amd64/${component}" \
      "https://dl.k8s.io/release/${KUBERNETES_VERSION}/bin/linux/amd64/${component}.sha256" \
      "$cache_dir/$component"
    chmod 0755 "$cache_dir/$component"
  done
}

preload_kubernetes_binaries

if minikube status -p "$PROFILE" >/dev/null 2>&1; then
  printf 'minikube profile %s already exists; reusing it\n' "$PROFILE"
else
  start_args=(minikube start -p "$PROFILE" \
    --driver=docker \
    --container-runtime=docker \
    --gpus=nvidia.com \
    --kubernetes-version="$KUBERNETES_VERSION" \
    --cpus="$CPUS" \
    --memory="${MEMORY}mb")
  [[ -z ${TGSRL_MINIKUBE_IMAGE_MIRROR_COUNTRY:-} ]] || \
    start_args+=(--image-mirror-country="$TGSRL_MINIKUBE_IMAGE_MIRROR_COUNTRY")
  [[ -z ${TGSRL_MINIKUBE_IMAGE_REPOSITORY:-} ]] || \
    start_args+=(--image-repository="$TGSRL_MINIKUBE_IMAGE_REPOSITORY")
  [[ -z ${TGSRL_MINIKUBE_BASE_IMAGE:-} ]] || \
    start_args+=(--base-image="$TGSRL_MINIKUBE_BASE_IMAGE")
  if [[ -n ${TGSRL_DOCKER_REGISTRY_MIRRORS:-} ]]; then
    IFS=',' read -r -a registry_mirrors <<<"$TGSRL_DOCKER_REGISTRY_MIRRORS"
    for mirror in "${registry_mirrors[@]}"; do
      start_args+=(--registry-mirror="$mirror")
    done
  fi
  if (( EUID == 0 )); then
    [[ $ALLOW_ROOT == 1 ]] || {
      echo 'Minikube Docker driver should run as a non-root user; on a dedicated disposable test ECS, explicitly set TGSRL_MINIKUBE_ALLOW_ROOT=1' >&2
      exit 2
    }
    start_args+=(--force)
  fi
  "${start_args[@]}"
fi
minikube update-context -p "$PROFILE"
kubectl config use-context "$PROFILE" >/dev/null
kubectl wait --for=condition=Ready nodes --all --timeout=5m

minikube addons enable registry -p "$PROFILE" >/dev/null
registry_proxy_repository=registry.k8s.io/minikube/kube-registry-proxy
if [[ $TGSRL_NETWORK_PROFILE == cn ]]; then
  registry_proxy_repository="${TGSRL_K8S_IMAGE_REGISTRY}/minikube/kube-registry-proxy"
fi
registry_proxy_image="${registry_proxy_repository}:${REGISTRY_PROXY_VERSION}@${REGISTRY_PROXY_DIGEST}"
if kubectl -n kube-system get daemonset registry-proxy >/dev/null 2>&1; then
  kubectl -n kube-system set image daemonset/registry-proxy registry-proxy="$registry_proxy_image" >/dev/null
  kubectl -n kube-system rollout status daemonset/registry-proxy --timeout=5m
fi
kubectl -n kube-system wait --for=condition=Available deployment/registry --timeout=5m
mkdir -p "$(dirname "$REGISTRY_FORWARD_PID")" "$(dirname "$REGISTRY_FORWARD_LOG")"
if [[ -s $REGISTRY_FORWARD_PID ]]; then
  old_pid=$(cat "$REGISTRY_FORWARD_PID")
  if [[ $old_pid =~ ^[0-9]+$ && -r /proc/$old_pid/cmdline ]] &&
    tr '\0' ' ' <"/proc/$old_pid/cmdline" | grep -q 'kubectl.*port-forward.*service/registry'; then
    kill "$old_pid" || true
  fi
fi
nohup kubectl -n kube-system port-forward --address=127.0.0.1 service/registry 5000:80 \
  >"$REGISTRY_FORWARD_LOG" 2>&1 </dev/null &
registry_forward_pid=$!
printf '%s\n' "$registry_forward_pid" >"$REGISTRY_FORWARD_PID"
for _ in $(seq 1 30); do
  if curl -fsS --connect-timeout 2 http://127.0.0.1:5000/v2/ >/dev/null; then
    break
  fi
  sleep 1
done
kill -0 "$registry_forward_pid" 2>/dev/null &&
  curl -fsS --connect-timeout 2 http://127.0.0.1:5000/v2/ >/dev/null ||
  { echo "Minikube registry host forward failed; see $REGISTRY_FORWARD_LOG" >&2; exit 1; }

server_version=$(kubectl version -o json | jq -r '.serverVersion.gitVersion')
[[ "$server_version" == "$KUBERNETES_VERSION" ]] || {
  echo "existing profile runs Kubernetes $server_version, expected $KUBERNETES_VERSION; delete only that profile with: minikube delete -p $PROFILE" >&2
  exit 1
}
node_count=$(kubectl get nodes -o json | jq '.items | length')
[[ "$node_count" == 1 ]] || { echo "first GPU smoke requires a one-node cluster; found $node_count nodes" >&2; exit 1; }
runtime=$(kubectl get nodes -o json | jq -r '.items[0].status.nodeInfo.containerRuntimeVersion')
[[ "$runtime" == docker://* ]] || { echo "minikube must use the Docker container runtime; found $runtime" >&2; exit 1; }
printf 'Kubernetes cluster %s is ready\n' "$PROFILE"
