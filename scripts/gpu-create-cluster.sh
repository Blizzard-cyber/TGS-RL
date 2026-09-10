#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
PROFILE=${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}
KUBERNETES_VERSION=${TGSRL_KUBERNETES_VERSION:-v1.35.1}
CPUS=${TGSRL_MINIKUBE_CPUS:-8}
MEMORY=${TGSRL_MINIKUBE_MEMORY:-24576}

if ! command -v nvidia-smi >/dev/null 2>&1 || ! nvidia-smi -L >/dev/null 2>&1; then
  echo 'a working NVIDIA host driver is required' >&2
  exit 1
fi
docker info >/dev/null 2>&1 || { echo 'Docker is unavailable to the current user; log out/in after gpu-install-host' >&2; exit 1; }

if minikube status -p "$PROFILE" >/dev/null 2>&1; then
  printf 'minikube profile %s already exists; reusing it\n' "$PROFILE"
else
  minikube start -p "$PROFILE" \
    --driver=docker \
    --container-runtime=docker \
    --gpus=nvidia.com \
    --kubernetes-version="$KUBERNETES_VERSION" \
    --cpus="$CPUS" \
    --memory="${MEMORY}mb"
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
