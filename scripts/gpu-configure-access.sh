#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
NAMESPACE=${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}
[[ "$NAMESPACE" == tgsrl-system ]] || { echo 'first GPU smoke currently requires TGSRL_WORKLOAD_NAMESPACE=tgsrl-system' >&2; exit 2; }
OUTPUT=${TGSRL_GPU_KUBECONFIG:-.cache/tgsrl/gpu-kubeconfig}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}}
current_context=$(kubectl config current-context)
[[ "$current_context" == "$EXPECTED_CONTEXT" ]] || { echo "refusing to configure context $current_context; expected $EXPECTED_CONTEXT" >&2; exit 1; }

kubectl apply -f deploy/crds/tgsrl_jobrunbundles.yaml
kubectl apply -f deploy/kubernetes/gpu-smoke-operator-access.yaml
token=$(kubectl -n "$NAMESPACE" create token tgsrl-external-operator --duration=24h)
server=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')
ca_data=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
if [[ -z "$ca_data" ]]; then
  ca_path=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority}')
  [[ -f "$ca_path" ]] || { echo 'current kubeconfig has no readable cluster CA' >&2; exit 1; }
  ca_data=$(base64 <"$ca_path" | tr -d '\n')
fi
mkdir -p "$(dirname "$OUTPUT")"
python3 - "$OUTPUT" "$server" "$ca_data" "$token" "$NAMESPACE" <<'PY'
import sys
from pathlib import Path

path, server, ca_data, token, namespace = sys.argv[1:]
content = f"""apiVersion: v1
kind: Config
clusters:
- name: tgsrl-gpu
  cluster:
    server: {server}
    certificate-authority-data: {ca_data}
users:
- name: tgsrl-external-operator
  user:
    token: {token}
contexts:
- name: tgsrl-gpu
  context:
    cluster: tgsrl-gpu
    user: tgsrl-external-operator
    namespace: {namespace}
current-context: tgsrl-gpu
"""
target = Path(path)
target.write_text(content, encoding="utf-8")
target.chmod(0o600)
PY
printf 'wrote scoped Operator kubeconfig to %s (token lifetime: 24h)\n' "$OUTPUT"
