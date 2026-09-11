#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. "$ROOT_DIR/scripts/lib/network-profile.sh"
tgsrl_load_network_profile
KUEUE_VERSION=${TGSRL_KUEUE_VERSION:-v0.19.2}
DRA_VERSION=${TGSRL_NVIDIA_DRA_VERSION:-0.5.0}
NFD_VERSION=${TGSRL_NFD_VERSION:-0.18.3}
NAMESPACE=${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}}
K8S_OCI_REGISTRY=${TGSRL_K8S_OCI_REGISTRY:-registry.k8s.io}
K8S_IMAGE_REGISTRY=${TGSRL_K8S_IMAGE_REGISTRY:-registry.k8s.io}
current_context=$(kubectl config current-context)
[[ "$current_context" == "$EXPECTED_CONTEXT" ]] || { echo "refusing to modify context $current_context; expected $EXPECTED_CONTEXT" >&2; exit 1; }

kueue_manifest=$(mktemp "${TMPDIR:-/tmp}/tgsrl-kueue-manifest.XXXXXX")
trap 'rm -f "$kueue_manifest" "${config:-}"' EXIT
tgsrl_download "https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml" "$kueue_manifest"
if [[ $K8S_IMAGE_REGISTRY != registry.k8s.io ]]; then
  sed -i "s#registry.k8s.io/kueue/#${K8S_IMAGE_REGISTRY}/kueue/#g" "$kueue_manifest"
fi
kubectl apply --server-side -f "$kueue_manifest"
kubectl wait --for=condition=Available -n kueue-system deployment/kueue-controller-manager --timeout=5m

helm upgrade --install nfd \
  "oci://${K8S_OCI_REGISTRY}/nfd/charts/node-feature-discovery" \
  --version "${NFD_VERSION}" \
  --set "image.repository=${K8S_IMAGE_REGISTRY}/nfd/node-feature-discovery" \
  --namespace node-feature-discovery \
  --create-namespace \
  --wait --timeout 10m

helm upgrade --install dra-driver-nvidia-gpu \
  "oci://${K8S_OCI_REGISTRY}/dra-driver-nvidia/charts/dra-driver-nvidia-gpu" \
  --version "${DRA_VERSION}" \
  --namespace dra-driver-nvidia-gpu \
  --create-namespace \
  --set gpuResourcesEnabledOverride=true \
  --set "image.repository=${K8S_IMAGE_REGISTRY}/dra-driver-nvidia/dra-driver-nvidia-gpu" \
  --wait --timeout 10m

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

config=$(mktemp "${TMPDIR:-/tmp}/tgsrl-kueue-config.XXXXXX")
kubectl get configmap kueue-manager-config -n kueue-system -o jsonpath='{.data.controller_manager_config\.yaml}' >"$config"
python3 scripts/patch-kueue-config.py --input "$config" --output "$config"
kubectl create configmap kueue-manager-config -n kueue-system \
  --from-file=controller_manager_config.yaml="$config" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/kueue-controller-manager -n kueue-system
kubectl rollout status deployment/kueue-controller-manager -n kueue-system --timeout=5m
for attempt in $(seq 1 30); do
  webhook_endpoint=$(kubectl -n kueue-system get endpoints kueue-webhook-service \
    -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || true)
  [[ -n $webhook_endpoint ]] && break
  sleep 2
done
[[ -n ${webhook_endpoint:-} ]] || { echo 'Kueue webhook has no ready endpoint' >&2; exit 1; }
for attempt in $(seq 1 10); do
  if kubectl apply -f deploy/kubernetes/gpu-smoke-queue.yaml; then
    break
  fi
  (( attempt < 10 )) || { echo 'Kueue queue resources failed after webhook readiness retries' >&2; exit 1; }
  sleep 2
done
kubectl wait --for=condition=Available -n dra-driver-nvidia-gpu deployment --all --timeout=5m
mapfile -t dra_daemonsets < <(kubectl -n dra-driver-nvidia-gpu get daemonset -o name)
(( ${#dra_daemonsets[@]} > 0 )) || { echo 'NVIDIA DRA chart created no DaemonSet' >&2; exit 1; }
for daemonset in "${dra_daemonsets[@]}"; do
  kubectl rollout status -n dra-driver-nvidia-gpu "$daemonset" --timeout=5m
done

printf 'cluster prerequisites installed; run make gpu-preflight next\n'
