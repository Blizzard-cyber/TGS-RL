#!/bin/sh
set -eu

mode=${1:-local}
case "$mode" in
  local|dev|kubernetes) ;;
  *)
    printf 'usage: %s [local|dev|kubernetes]\n' "$0" >&2
    exit 2
    ;;
esac

failures=0
warnings=0

pass() { printf 'ok: %s\n' "$1"; }
warn() { printf 'warning: %s\n' "$1" >&2; warnings=$((warnings + 1)); }
fail() { printf 'error: %s\n' "$1" >&2; failures=$((failures + 1)); }

require_command() {
  name=$1
  if command -v "$name" >/dev/null 2>&1; then
    pass "$name is available"
  else
    fail "$name is required"
  fi
}

case "$mode" in
  local) required_commands="docker" ;;
  dev) required_commands="go python3 uv node npm buf helm ruby git curl" ;;
  kubernetes) required_commands="go python3 uv node npm docker helm kubectl minikube buf ruby git curl" ;;
esac

for command_name in $required_commands; do
  require_command "$command_name"
done

if [ "$mode" != local ] && command -v go >/dev/null 2>&1; then
  go_version=$(go env GOVERSION | sed 's/^go//')
  go_minor=$(printf '%s' "$go_version" | awk -F. '{print $1 "." $2}')
  [ "$go_minor" = "1.26" ] || warn "Go $go_version is installed; release builds use 1.26.4"
fi

if [ "$mode" != local ] && command -v buf >/dev/null 2>&1; then
  buf_version=$(buf --version)
  [ "$buf_version" = "1.72.0" ] || fail "Buf 1.72.0 is required, found $buf_version"
fi

if [ "$mode" != local ] && command -v helm >/dev/null 2>&1; then
  helm_version=$(helm version --short | sed 's/^v//' | sed 's/+.*//')
  [ "$helm_version" = "4.2.4" ] || warn "Helm $helm_version is installed; deployment validation uses 4.2.4"
fi

if [ "$mode" = kubernetes ] && command -v minikube >/dev/null 2>&1; then
  minikube_version=$(minikube version --short | sed 's/^v//')
  [ "$minikube_version" = "1.38.1" ] || warn "minikube $minikube_version is installed; local integration uses 1.38.1"
fi

if [ "$mode" != local ] && command -v python3 >/dev/null 2>&1; then
  python_minor=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
  [ "$python_minor" = "3.12" ] || warn "Python $python_minor is installed; the locked runtime uses 3.12.14"
fi

if [ "$mode" != local ] && command -v uv >/dev/null 2>&1; then
  uv_version=$(uv --version | awk '{print $2}')
  [ "$uv_version" = "0.12.7" ] || fail "uv 0.12.7 is required, found $uv_version"
fi

if [ "$mode" != local ] && command -v node >/dev/null 2>&1; then
  node_version=$(node --version | sed 's/^v//')
  [ "$node_version" = "24.20.0" ] || warn "Node $node_version is installed; CI and Compose use 24.20.0"
fi

if [ "$mode" != dev ] && command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    pass "Docker daemon is reachable"
  else
    fail "Docker daemon is not reachable"
  fi
  if docker compose version >/dev/null 2>&1; then
    pass "Docker Compose v2 is available"
  else
    fail "Docker Compose v2 is required"
  fi
fi

if [ "$mode" = kubernetes ] && command -v kubectl >/dev/null 2>&1; then
  kubectl_versions=$(kubectl version -o json 2>/dev/null | python3 -c 'import json, sys; data = json.load(sys.stdin); print(data["clientVersion"]["minor"].rstrip("+")); print(data.get("serverVersion", {}).get("minor", "").rstrip("+"))' || true)
  client_minor=$(printf '%s\n' "$kubectl_versions" | sed -n '1p')
  server_minor=$(printf '%s\n' "$kubectl_versions" | sed -n '2p')
  if [ -n "$client_minor" ] && [ -n "$server_minor" ]; then
    difference=$((client_minor - server_minor))
    [ "$difference" -lt 0 ] && difference=$((-difference))
    [ "$difference" -le 1 ] || warn "kubectl/server minor skew is $difference; Kubernetes supports a skew of at most one minor"
  fi
fi

if [ "$failures" -ne 0 ]; then
  printf 'environment check failed: %d error(s), %d warning(s)\n' "$failures" "$warnings" >&2
  exit 1
fi
printf '%s environment check passed with %d warning(s)\n' "$mode" "$warnings"
