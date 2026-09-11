#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
# shellcheck disable=SC1091
. "$ROOT_DIR/scripts/lib/network-profile.sh"
tgsrl_load_network_profile
REGISTRY=${TGSRL_IMAGE_REGISTRY:?set TGSRL_IMAGE_REGISTRY, for example localhost:5000/tgsrl}
VERL_BASE_IMAGE=${TGSRL_VERL_BASE_IMAGE:-nvidia/cuda:13.0.2-devel-ubuntu24.04@sha256:5dc1bca23d05bd37b011be68ec470c03b403a5da07ec3a86e41af9470e9d0cc6}
GO_BASE_IMAGE=${TGSRL_GO_BASE_IMAGE:-golang:1.26.4-bookworm@sha256:b305420a68d0f229d91eb3b3ed9e519fcf2cf5461da4bef997bf927e8c0bfd2b}
DISTROLESS_BASE_IMAGE=${TGSRL_DISTROLESS_BASE_IMAGE:-gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab}
GOPROXY=${TGSRL_GOPROXY:-https://proxy.golang.org,direct}
PYPI_INDEX_URL=${TGSRL_PYPI_INDEX_URL:-https://pypi.org/simple}
VERSION=${TGSRL_IMAGE_VERSION:-$(git rev-parse --short=12 HEAD)}
PLATFORM=${TGSRL_IMAGE_PLATFORM:-linux/amd64}
PUSH=${TGSRL_PUSH_IMAGES:-1}
[[ "$PUSH" == 1 ]] || { echo 'GPU smoke images must be pushed to a registry reachable by Kubernetes; TGSRL_PUSH_IMAGES=0 is unsupported' >&2; exit 2; }
[[ "$PLATFORM" == linux/amd64 ]] || { echo 'the locked GPU workload currently supports TGSRL_IMAGE_PLATFORM=linux/amd64 only' >&2; exit 2; }

[[ "$REGISTRY" =~ ^[a-z0-9.-]+(:[0-9]+)?(/[a-z0-9._-]+)+$ ]] || { echo 'TGSRL_IMAGE_REGISTRY must be a lowercase registry/repository prefix without scheme, digest, or trailing slash' >&2; exit 2; }
[[ "$VERL_BASE_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]] || { echo 'TGSRL_VERL_BASE_IMAGE must be an immutable repository@sha256 reference' >&2; exit 2; }
[[ "$GO_BASE_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]] || { echo 'TGSRL_GO_BASE_IMAGE must be immutable' >&2; exit 2; }
[[ "$DISTROLESS_BASE_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]] || { echo 'TGSRL_DISTROLESS_BASE_IMAGE must be immutable' >&2; exit 2; }
[[ "$VERSION" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || { echo 'TGSRL_IMAGE_VERSION is not a valid OCI tag' >&2; exit 2; }
docker buildx version >/dev/null 2>&1 || { echo 'Docker Buildx is required' >&2; exit 1; }
[[ -z $(git status --porcelain=v1 --untracked-files=all) ]] || { echo 'GPU evidence images require a clean checkout' >&2; exit 1; }

build() {
  local name=$1 dockerfile=$2
  shift 2
  local image="${REGISTRY}/${name}:${VERSION}"
  local args=(docker buildx build --platform "$PLATFORM" -f "$dockerfile" -t "$image")
  local build_arg
  for build_arg in "$@"; do
    args+=(--build-arg "$build_arg")
  done
  args+=(--push)
  args+=(.)
  "${args[@]}" >&2
  local digest
  digest=$(docker buildx imagetools inspect "$image" --raw | sha256sum | awk '{print "sha256:" $1}')
  [[ "$digest" =~ ^sha256:[a-f0-9]{64}$ ]] || { echo "cannot resolve immutable digest for $image" >&2; exit 1; }
  local variable_name
  variable_name=$(printf '%s' "$name" | tr '[:lower:]-' '[:upper:]_')
  printf '%s_IMAGE=%s@%s\n%s_DIGEST=%s\n' "$variable_name" "${REGISTRY}/${name}" "$digest" "$variable_name" "$digest"
}

output=${TGSRL_IMAGE_ENV_FILE:-.cache/tgsrl/gpu-images.env}
mkdir -p "$(dirname "$output")"
temp=$(mktemp "${TMPDIR:-/tmp}/tgsrl-gpu-images.XXXXXX")
trap 'rm -f "$temp"' EXIT
{
  build worker-bootstrap Dockerfile.worker-bootstrap \
    "TGSRL_GO_BASE_IMAGE=$GO_BASE_IMAGE" \
    "TGSRL_DISTROLESS_BASE_IMAGE=$DISTROLESS_BASE_IMAGE" \
    "TGSRL_GOPROXY=$GOPROXY"
  build gpu-smoke Dockerfile.gpu-smoke \
    "TGSRL_VERL_BASE_IMAGE=$VERL_BASE_IMAGE" \
    "TGSRL_PYPI_INDEX_URL=$PYPI_INDEX_URL"
} >"$temp"
chmod 0600 "$temp"
mv "$temp" "$output"
trap - EXIT
printf 'wrote immutable image references to %s\n' "$output"
