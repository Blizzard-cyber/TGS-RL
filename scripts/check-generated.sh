#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT_DIR
BASELINE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tgsrl-generated.XXXXXX")"
readonly BASELINE_DIR
trap 'rm -rf "${BASELINE_DIR}"' EXIT

if [[ -d "${ROOT_DIR}/gen" ]]; then
  cp -R "${ROOT_DIR}/gen" "${BASELINE_DIR}/gen"
else
  mkdir -p "${BASELINE_DIR}/gen"
fi
find "${BASELINE_DIR}/gen" -type d -name __pycache__ -prune -exec rm -rf {} +
find "${BASELINE_DIR}/gen" -type f -name '*.py[co]' -delete

"${ROOT_DIR}/scripts/generate-proto.sh"

if ! diff -ruN --exclude='__pycache__' --exclude='*.pyc' --exclude='*.pyo' \
  "${BASELINE_DIR}/gen" "${ROOT_DIR}/gen"; then
  printf 'generated protobuf files were stale; run scripts/generate-proto.sh\n' >&2
  exit 1
fi

printf 'generated protobuf files are up to date\n'
