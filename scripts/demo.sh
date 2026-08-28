#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT_DIR
SCHEDULER_ADDRESS="${TGSRL_DEMO_ADDRESS:-${TGS_RL_DEMO_ADDRESS:-127.0.0.1:50051}}"
DEMO_TIMEOUT_SECONDS="${TGSRL_DEMO_TIMEOUT_SECONDS:-${TGS_RL_DEMO_TIMEOUT_SECONDS:-30}}"
DEMO_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tgsrl-demo.XXXXXX")"
readonly DEMO_DIR
readonly SCHEDULER_LOG="${DEMO_DIR}/scheduler.log"
readonly DEMO_OUTPUT="${DEMO_DIR}/result.json"
scheduler_pid=

cleanup() {
  local exit_code=$?
  if [[ -n "${scheduler_pid}" ]]; then
    kill -TERM "${scheduler_pid}" 2>/dev/null || true
    wait "${scheduler_pid}" 2>/dev/null || true
  fi
  if [[ ${exit_code} -ne 0 && -s "${SCHEDULER_LOG}" ]]; then
    printf '%s\n' 'scheduler log:' >&2
    sed -n '1,200p' "${SCHEDULER_LOG}" >&2
  fi
  rm -rf "${DEMO_DIR}"
  return "${exit_code}"
}
trap cleanup EXIT INT TERM

case "${SCHEDULER_ADDRESS}" in
  *:*) ;;
  *)
    printf 'error: TGSRL_DEMO_ADDRESS/TGS_RL_DEMO_ADDRESS must be host:port, got %s\n' "${SCHEDULER_ADDRESS}" >&2
    exit 2
    ;;
esac

SCHEDULER_HOST="${SCHEDULER_ADDRESS%:*}"
SCHEDULER_PORT="${SCHEDULER_ADDRESS##*:}"

cd "${ROOT_DIR}"
command -v go >/dev/null 2>&1 || { printf '%s\n' 'error: go is required' >&2; exit 1; }
command -v uv >/dev/null 2>&1 || { printf '%s\n' 'error: uv is required' >&2; exit 1; }

go build -trimpath -o "${DEMO_DIR}/tgsrl-scheduler" ./scheduler-go/cmd/scheduler
"${DEMO_DIR}/tgsrl-scheduler" \
  -listen "${SCHEDULER_ADDRESS}" \
  -fallback noop >"${SCHEDULER_LOG}" 2>&1 &
scheduler_pid=$!

python3 - "${SCHEDULER_HOST}" "${SCHEDULER_PORT}" "${DEMO_TIMEOUT_SECONDS}" <<'PY'
import socket
import sys
import time

host, raw_port, raw_timeout = sys.argv[1:]
port = int(raw_port)
deadline = time.monotonic() + float(raw_timeout)
while True:
    try:
        with socket.create_connection((host, port), timeout=0.25):
            break
    except OSError:
        if time.monotonic() >= deadline:
            raise SystemExit(f"scheduler did not listen on {host}:{port} before timeout")
        time.sleep(0.1)
PY

if ! kill -0 "${scheduler_pid}" 2>/dev/null; then
  printf '%s\n' 'error: scheduler exited before the demo request' >&2
  exit 1
fi

uv run --frozen python -m tgsrl_runtime.demo \
  --target "${SCHEDULER_ADDRESS}" \
  --timeout "${DEMO_TIMEOUT_SECONDS}" >"${DEMO_OUTPUT}"

python3 - "${DEMO_OUTPUT}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    result = json.load(stream)
published = result.get("publish_response", {})
decision = result.get("decision", {})
if published.get("status") != "INTENT_PUBLISH_STATUS_ACCEPTED":
    raise SystemExit(f"intent was not accepted: {published!r}")
if decision.get("fallback", False):
    raise SystemExit(f"scheduler used fallback: {decision.get('fallback_reason', '')}")
action_results = decision.get("action_results", [])
if not action_results or any(
    item.get("status") != "ACTION_RESULT_STATUS_SUCCEEDED" for item in action_results
):
    raise SystemExit(f"scheduler actions did not succeed: {action_results!r}")
PY

cat "${DEMO_OUTPUT}"
