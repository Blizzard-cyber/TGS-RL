#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
KUEUE_VERSION=${TGSRL_KUEUE_VERSION:-v0.19.2}
DRA_VERSION=${TGSRL_NVIDIA_DRA_VERSION:-0.5.0}
NFD_VERSION=${TGSRL_NFD_VERSION:-0.18.3}
NAMESPACE=${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}}
current_context=$(kubectl config current-context)
[[ "$current_context" == "$EXPECTED_CONTEXT" ]] || { echo "refusing to modify context $current_context; expected $EXPECTED_CONTEXT" >&2; exit 1; }

kubectl apply --server-side -f "https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml"
kubectl wait --for=condition=Available -n kueue-system deployment/kueue-controller-manager --timeout=5m

helm upgrade --install nfd \
  oci://registry.k8s.io/nfd/charts/node-feature-discovery \
  --version "${NFD_VERSION}" \
  --namespace node-feature-discovery \
  --create-namespace \
  --wait --timeout 10m

helm upgrade --install dra-driver-nvidia-gpu \
  oci://registry.k8s.io/dra-driver-nvidia/charts/dra-driver-nvidia-gpu \
  --version "${DRA_VERSION}" \
  --namespace dra-driver-nvidia-gpu \
  --create-namespace \
  --set gpuResourcesEnabledOverride=true \
  --wait --timeout 10m

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

config=$(mktemp "${TMPDIR:-/tmp}/tgsrl-kueue-config.XXXXXX")
trap 'rm -f "$config"' EXIT
kubectl get configmap kueue-manager-config -n kueue-system -o jsonpath='{.data.controller_manager_config\.yaml}' >"$config"
python3 scripts/patch-kueue-config.py --input "$config" --output "$config"
kubectl create configmap kueue-manager-config -n kueue-system \
  --from-file=controller_manager_config.yaml="$config" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/kueue-controller-manager -n kueue-system
kubectl rollout status deployment/kueue-controller-manager -n kueue-system --timeout=5m
kubectl apply -f deploy/kubernetes/gpu-smoke-queue.yaml
kubectl wait --for=condition=Available -n dra-driver-nvidia-gpu deployment --all --timeout=5m
kubectl rollout status -n dra-driver-nvidia-gpu daemonset --all --timeout=5m

printf 'cluster prerequisites installed; run make gpu-preflight next\n'
