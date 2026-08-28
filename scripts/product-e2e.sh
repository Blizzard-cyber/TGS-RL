#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/tgsrl-product-e2e.XXXXXX")
BIN_DIR="$WORK_DIR/bin"
LOG_DIR="$WORK_DIR/logs"
STATE_DIR="$WORK_DIR/state"
mkdir -p "$BIN_DIR" "$LOG_DIR" "$STATE_DIR/controller" "$STATE_DIR/scheduler" "$STATE_DIR/operator"

PIDS=()
NAMES=()
PROCESS_GROUPS=()
PROCESS_COUNT=0
FAILED=0
KEEP_ARTIFACTS=${TGSRL_PRODUCT_E2E_KEEP_ARTIFACTS:-0}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  set +m
  exec 3>&2
  exec 2>/dev/null
  if (( PROCESS_COUNT > 0 )); then
    for ((index=PROCESS_COUNT-1; index>=0; index--)); do
      if kill -0 "${PIDS[$index]}" 2>/dev/null; then
        { kill -TERM -- "-${PROCESS_GROUPS[$index]}" 2>/dev/null || true; } 2>/dev/null
      fi
    done
  fi
  local deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    local alive=0
    for ((index=0; index<PROCESS_COUNT; index++)); do
      if kill -0 "${PIDS[$index]}" 2>/dev/null; then
        alive=1
      fi
    done
    (( alive == 0 )) && break
    sleep 0.1
  done
  for ((index=0; index<PROCESS_COUNT; index++)); do
    local pid=${PIDS[$index]}
    if kill -0 "$pid" 2>/dev/null; then
      { kill -KILL -- "-${PROCESS_GROUPS[$index]}" 2>/dev/null || true; } 2>/dev/null
    fi
    { wait "$pid" 2>/dev/null || true; } 2>/dev/null
  done
  if (( exit_code != 0 || FAILED != 0 )); then
    printf '\nproduct E2E failed; process logs follow (artifacts: %s)\n' "$WORK_DIR" >&3
    for logfile in "$LOG_DIR"/*.log; do
      [[ -e "$logfile" ]] || continue
      printf '\n===== %s =====\n' "$(basename "$logfile" .log)" >&3
      tail -n 200 "$logfile" >&3 || true
    done
  elif [[ "$KEEP_ARTIFACTS" == "1" ]]; then
    printf 'product E2E artifacts retained at %s\n' "$WORK_DIR"
  else
    rm -rf -- "$WORK_DIR"
  fi
  if (( exit_code == 0 && FAILED != 0 )); then
    exit_code=1
  fi
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

free_port() {
  "$PYTHON_BIN" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_tcp() {
  local name=$1
  local port=$2
  local pid=$3
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if ! kill -0 "$pid" 2>/dev/null; then
      printf '%s exited before becoming ready\n' "$name" >&2
      return 1
    fi
    if "$PYTHON_BIN" - "$port" <<'PY' >/dev/null 2>&1
import socket
import sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.2):
    pass
PY
    then
      return 0
    fi
    sleep 0.1
  done
  printf 'timed out waiting for %s on 127.0.0.1:%s\n' "$name" "$port" >&2
  return 1
}

start_process() {
  local name=$1
  shift
  "$PYTHON_BIN" - "$LOG_DIR/$name.log" "$@" <<'PY' >/dev/null 2>&1 &
import os
import sys

log = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.setsid()
os.dup2(log, 1)
os.dup2(log, 2)
os.close(log)
os.execvp(sys.argv[2], sys.argv[2:])
PY
  local pid=$!
  NAMES+=("$name")
  PIDS+=("$pid")
  PROCESS_GROUPS+=("$pid")
  PROCESS_COUNT=$((PROCESS_COUNT + 1))
  LAST_PID=$pid
}

stop_processes() {
  local index
  for ((index=PROCESS_COUNT-1; index>=0; index--)); do
    if kill -0 "${PIDS[$index]}" 2>/dev/null; then
      { kill -TERM -- "-${PROCESS_GROUPS[$index]}" 2>/dev/null || true; } 2>/dev/null
    fi
  done
  local deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    local alive=0
    for ((index=0; index<PROCESS_COUNT; index++)); do
      kill -0 "${PIDS[$index]}" 2>/dev/null && alive=1
    done
    (( alive == 0 )) && break
    sleep 0.1
  done
  for ((index=0; index<PROCESS_COUNT; index++)); do
    if kill -0 "${PIDS[$index]}" 2>/dev/null; then
      { kill -KILL -- "-${PROCESS_GROUPS[$index]}" 2>/dev/null || true; } 2>/dev/null
    fi
    { wait "${PIDS[$index]}" 2>/dev/null || true; } 2>/dev/null
  done
  PIDS=()
  NAMES=()
  PROCESS_GROUPS=()
  PROCESS_COUNT=0
}

start_stack() {
  local suffix=$1
  start_process "scheduler$suffix" "$BIN_DIR/scheduler" \
    -listen "$SCHEDULER_TARGET" \
    -config-root "$ROOT_DIR" \
    -state-dir "$STATE_DIR/scheduler" \
    -metrics-listen ""
  SCHEDULER_PID=$LAST_PID
  wait_for_tcp scheduler "$SCHEDULER_PORT" "$SCHEDULER_PID"

  start_process "runtime$suffix" "$PYTHON_BIN" -m tgsrl_runtime.runtime_app \
    --bind "$RUNTIME_TARGET" \
    --scheduler-target "$SCHEDULER_TARGET" \
    --operator-target "$OPERATOR_TARGET" \
    --job-control-target "$CONTROLLER_TARGET" \
    --state-db "$RUNTIME_DB" \
    --config-root "$ROOT_DIR"
  RUNTIME_PID=$LAST_PID
  wait_for_tcp runtime "$RUNTIME_PORT" "$RUNTIME_PID"

  start_process "controller$suffix" "$BIN_DIR/job-controller" \
    -listen "$CONTROLLER_TARGET" \
    -runtime-target "$RUNTIME_TARGET" \
    -state-dir "$STATE_DIR/controller"
  CONTROLLER_PID=$LAST_PID
  wait_for_tcp controller "$CONTROLLER_PORT" "$CONTROLLER_PID"

  start_process "operator$suffix" "$BIN_DIR/operator" \
    -mode fake \
    -listen "$OPERATOR_TARGET" \
    -scheduler "$SCHEDULER_TARGET" \
    -control "$CONTROLLER_TARGET" \
    -runtime "$RUNTIME_TARGET" \
    -cursor-dir "$STATE_DIR/operator"
  OPERATOR_PID=$LAST_PID
  sleep 0.2
  if ! kill -0 "$OPERATOR_PID" 2>/dev/null; then
    printf 'operator exited instead of running its decision worker\n' >&2
    return 1
  fi
  wait_for_tcp operator "$OPERATOR_PORT" "$OPERATOR_PID"

  start_process "gateway$suffix" "$PYTHON_BIN" -m tgsrl_gateway serve \
    --host 127.0.0.1 \
    --port "$GATEWAY_PORT" \
    --backend-mode grpc \
    --job-control-target "$CONTROLLER_TARGET" \
    --scheduler-target "$SCHEDULER_TARGET" \
    --runtime-target "$RUNTIME_TARGET" \
    --experiment-target "$RUNTIME_TARGET"
  GATEWAY_PID=$LAST_PID
  wait_for_tcp gateway "$GATEWAY_PORT" "$GATEWAY_PID"
}

cd "$ROOT_DIR"
command -v go >/dev/null 2>&1 || { printf 'error: go is required\n' >&2; exit 1; }
if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  printf 'error: locked Python environment is missing; run uv sync --frozen first\n' >&2
  exit 1
fi

export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/runtime-python:$ROOT_DIR/gateway-python:$ROOT_DIR/gen/python${PYTHONPATH:+:$PYTHONPATH}"
export CGO_ENABLED=0

SCHEDULER_PORT=$(free_port)
RUNTIME_PORT=$(free_port)
CONTROLLER_PORT=$(free_port)
OPERATOR_PORT=$(free_port)
GATEWAY_PORT=$(free_port)
SCHEDULER_TARGET="127.0.0.1:$SCHEDULER_PORT"
RUNTIME_TARGET="127.0.0.1:$RUNTIME_PORT"
CONTROLLER_TARGET="127.0.0.1:$CONTROLLER_PORT"
OPERATOR_TARGET="127.0.0.1:$OPERATOR_PORT"
GATEWAY_URL="http://127.0.0.1:$GATEWAY_PORT"
RUNTIME_DB="$STATE_DIR/runtime.db"
DRIVER_STATE="$STATE_DIR/product-e2e.json"
OPERATOR_CURSOR="$STATE_DIR/operator/decision-cursor.json"

printf 'building product processes...\n'
go build -o "$BIN_DIR/scheduler" ./scheduler-go/cmd/scheduler
go build -o "$BIN_DIR/job-controller" ./job-controller-go/cmd/job-controller
go build -o "$BIN_DIR/operator" ./cmd/operator

start_stack ""

printf 'running product lifecycle on real HTTP and gRPC boundaries...\n'
if ! "$PYTHON_BIN" tests/e2e/product_e2e.py before-restart \
  --gateway-url "$GATEWAY_URL" \
  --runtime-target "$RUNTIME_TARGET" \
  --control-target "$CONTROLLER_TARGET" \
  --state-db "$RUNTIME_DB" \
  --state-file "$DRIVER_STATE" \
  --operator-cursor "$OPERATOR_CURSOR" \
  --timeout "${TGSRL_PRODUCT_E2E_TIMEOUT:-30}" | tee "$LOG_DIR/driver.log"; then
  FAILED=1
  exit 1
fi

printf 'restarting durable product services...\n'
stop_processes
start_stack "-restart"

printf 'verifying recovered product state and idempotency...\n'
if ! "$PYTHON_BIN" tests/e2e/product_e2e.py after-restart \
  --gateway-url "$GATEWAY_URL" \
  --runtime-target "$RUNTIME_TARGET" \
  --control-target "$CONTROLLER_TARGET" \
  --state-db "$RUNTIME_DB" \
  --state-file "$DRIVER_STATE" \
  --operator-cursor "$OPERATOR_CURSOR" \
  --timeout "${TGSRL_PRODUCT_E2E_TIMEOUT:-30}" | tee -a "$LOG_DIR/driver.log"; then
  FAILED=1
  exit 1
fi

for ((index=0; index<PROCESS_COUNT; index++)); do
  if ! kill -0 "${PIDS[$index]}" 2>/dev/null; then
    printf '%s exited unexpectedly during the product flow\n' "${NAMES[$index]}" >&2
    FAILED=1
    exit 1
  fi
done
printf 'product E2E passed\n'
