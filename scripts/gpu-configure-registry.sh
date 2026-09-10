#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"

NAMESPACE=${TGSRL_WORKLOAD_NAMESPACE:-tgsrl-system}
SECRET=${TGSRL_IMAGE_PULL_SECRET:-tgsrl-registry}
REGISTRY=${TGSRL_IMAGE_REGISTRY:?set TGSRL_IMAGE_REGISTRY to the same repository prefix used by gpu-build-images}
DOCKER_CONFIG_DIR=${DOCKER_CONFIG:-.cache/tgsrl/docker}
CONFIG_FILE=${TGSRL_DOCKER_CONFIG_JSON:-${DOCKER_CONFIG_DIR}/config.json}
EXPECTED_CONTEXT=${TGSRL_KUBE_CONTEXT:-${TGSRL_MINIKUBE_PROFILE:-tgsrl-gpu}}

[[ "$SECRET" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]] || { echo 'TGSRL_IMAGE_PULL_SECRET must be a DNS label' >&2; exit 2; }
[[ "$REGISTRY" =~ ^[a-z0-9.-]+(:[0-9]+)?(/[a-z0-9._-]+)+$ ]] || { echo 'TGSRL_IMAGE_REGISTRY must be a lowercase registry/repository prefix' >&2; exit 2; }
[[ -f "$CONFIG_FILE" ]] || {
  echo "missing Docker credential file $CONFIG_FILE; use a dedicated DOCKER_CONFIG and run docker login first" >&2
  exit 1
}
registry_host=${REGISTRY%%/*}
jq -e --arg host "$registry_host" '.auths | type == "object" and (has($host) or has("https://" + $host) or has("http://" + $host))' "$CONFIG_FILE" >/dev/null || {
  echo "$CONFIG_FILE contains no inline auth for $registry_host; credential helpers cannot be copied into Kubernetes" >&2
  exit 1
}
current_context=$(kubectl config current-context)
[[ "$current_context" == "$EXPECTED_CONTEXT" ]] || { echo "refusing to modify context $current_context; expected $EXPECTED_CONTEXT" >&2; exit 1; }

kubectl -n "$NAMESPACE" create secret generic "$SECRET" \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson="$CONFIG_FILE" \
  --dry-run=client -o yaml | kubectl apply -f -

existing=$(kubectl -n "$NAMESPACE" get serviceaccount default -o json | jq -r --arg secret "$SECRET" '[.imagePullSecrets[]?.name] | index($secret) != null')
if [[ "$existing" != true ]]; then
  count=$(kubectl -n "$NAMESPACE" get serviceaccount default -o json | jq '.imagePullSecrets // [] | length')
  if (( count == 0 )); then
    patch='[{"op":"add","path":"/imagePullSecrets","value":[{"name":"'"$SECRET"'"}]}]'
  else
    patch='[{"op":"add","path":"/imagePullSecrets/-","value":{"name":"'"$SECRET"'"}}]'
  fi
  kubectl -n "$NAMESPACE" patch serviceaccount default --type=json -p "$patch" >/dev/null
fi
printf 'configured imagePullSecret %s on %s/default\n' "$SECRET" "$NAMESPACE"
