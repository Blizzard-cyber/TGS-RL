#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT_DIR
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tgsrl-openapi.XXXXXX")"
readonly TMP_DIR
trap 'rm -rf "${TMP_DIR}"' EXIT

OUTPUT="${TMP_DIR}/openapi.json"

if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

PYTHONPATH="${ROOT_DIR}/gateway-python:${ROOT_DIR}/runtime-python:${ROOT_DIR}/gen/python:${ROOT_DIR}" \
  "${PYTHON_BIN}" -m tgsrl_gateway openapi --output "${OUTPUT}"

if ! diff -u "${ROOT_DIR}/api/openapi.json" "${OUTPUT}"; then
  printf 'error: api/openapi.json is stale; run python -m tgsrl_gateway openapi --output api/openapi.json\n' >&2
  exit 1
fi

printf 'openapi artifact is up to date\n'
