#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
RELEASE=${TGSRL_HELM_RELEASE:-tgsrl}
NAMESPACE=${TGSRL_HELM_NAMESPACE:-tgsrl-system}
TIMEOUT=${TGSRL_HELM_TIMEOUT:-10m}
ACTION=${1:-render}
ARGUMENT=${2:-}

usage() {
  cat <<'EOF'
usage: scripts/deploy-full-stack.sh render [values.yaml]
       scripts/deploy-full-stack.sh install [values.yaml]
       scripts/deploy-full-stack.sh upgrade [values.yaml]
       scripts/deploy-full-stack.sh rollback REVISION
       scripts/deploy-full-stack.sh status

Environment: TGSRL_HELM_RELEASE, TGSRL_HELM_NAMESPACE, TGSRL_HELM_TIMEOUT.
EOF
}

command -v helm >/dev/null 2>&1 || { echo 'error: helm is required' >&2; exit 1; }

case "$ACTION" in
  render|install|upgrade) ;;
  rollback)
    [[ "$ARGUMENT" =~ ^[1-9][0-9]*$ ]] || { echo 'error: rollback requires a positive revision' >&2; exit 2; }
    exec helm rollback "$RELEASE" "$ARGUMENT" --namespace "$NAMESPACE" --wait --timeout "$TIMEOUT"
    ;;
  status)
    exec helm status "$RELEASE" --namespace "$NAMESPACE"
    ;;
  -h|--help|help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

if [[ -n "$ARGUMENT" && ! -f "$ARGUMENT" ]]; then
  echo "error: values file does not exist: $ARGUMENT" >&2
  exit 2
fi

STAGE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/tgsrl-helm.XXXXXX")
trap 'rm -rf "$STAGE_DIR"' EXIT
mkdir -p "$STAGE_DIR/helm"
cp -R "$ROOT_DIR/deploy/helm/operator" "$STAGE_DIR/helm/operator"
cp -R "$ROOT_DIR/deploy/helm/tgsrl" "$STAGE_DIR/helm/tgsrl"
CHART_DIR="$STAGE_DIR/helm/tgsrl"
helm dependency build "$CHART_DIR" >/dev/null

case "$ACTION" in
  render)
    if [[ -n "$ARGUMENT" ]]; then
      helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --include-crds --values "$ARGUMENT"
    else
      helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --include-crds
    fi
    ;;
  install)
    if [[ -n "$ARGUMENT" ]]; then
      helm install "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --create-namespace --atomic --wait --timeout "$TIMEOUT" --values "$ARGUMENT"
    else
      helm install "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --create-namespace --atomic --wait --timeout "$TIMEOUT"
    fi
    ;;
  upgrade)
    if [[ -n "$ARGUMENT" ]]; then
      helm upgrade "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --atomic --wait --timeout "$TIMEOUT" --values "$ARGUMENT"
    else
      helm upgrade "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --atomic --wait --timeout "$TIMEOUT"
    fi
    ;;
esac
