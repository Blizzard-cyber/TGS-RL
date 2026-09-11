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
