#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
WORK_DIR=$(mktemp -d /tmp/tgsrl-gate.XXXXXX)
BIN_DIR="$WORK_DIR/bin"
LOG_DIR="$WORK_DIR/logs"
STATE_DIR="$WORK_DIR/state"
OUTPUT_DIR=${TGSRL_GATE_OUTPUT_DIR:-"$ROOT_DIR/.cache/tgsrl/gate-gi-process"}
mkdir -p "$BIN_DIR" "$LOG_DIR" "$STATE_DIR/controller" "$STATE_DIR/scheduler" "$STATE_DIR/operator" "$STATE_DIR/workloads"

PIDS=()
PROCESS_GROUPS=()
PROCESS_COUNT=0
FAILED=0

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  set +m
  for ((index=PROCESS_COUNT-1; index>=0; index--)); do
    if kill -0 "${PIDS[$index]}" 2>/dev/null; then
      kill -TERM -- "-${PROCESS_GROUPS[$index]}" 2>/dev/null || true
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
      kill -KILL -- "-${PROCESS_GROUPS[$index]}" 2>/dev/null || true
    fi
    wait "${PIDS[$index]}" 2>/dev/null || true
  done
  if (( exit_code != 0 || FAILED != 0 )); then
    printf 'full-stack Gate failed; logs: %s\n' "$LOG_DIR" >&2
    for logfile in "$LOG_DIR"/*.log; do
      [[ -e "$logfile" ]] || continue
      printf '\n===== %s =====\n' "$(basename "$logfile")" >&2
      tail -n 200 "$logfile" >&2 || true
    done
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
  local name=$1 port=$2 pid=$3 deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    kill -0 "$pid" 2>/dev/null || { printf '%s exited before ready\n' "$name" >&2; return 1; }
    if "$PYTHON_BIN" - "$port" <<'PY' >/dev/null 2>&1
import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.2):
    pass
PY
    then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

start_process() {
  local name=$1
  shift
  "$PYTHON_BIN" - "$LOG_DIR/$name.log" "$@" <<'PY' >/dev/null 2>&1 &
import os, sys
log = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.setsid()
os.dup2(log, 1)
os.dup2(log, 2)
os.close(log)
os.execvp(sys.argv[2], sys.argv[2:])
PY
  local pid=$!
  PIDS+=("$pid")
  PROCESS_GROUPS+=("$pid")
  PROCESS_COUNT=$((PROCESS_COUNT + 1))
  LAST_PID=$pid
}

cd "$ROOT_DIR"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
[[ -x "$PYTHON_BIN" ]] || { printf 'error: run uv sync --frozen first\n' >&2; exit 1; }
export PATH="$ROOT_DIR/.venv/bin:$PATH"
export PYTHONPATH="$ROOT_DIR:$ROOT_DIR/runtime-python:$ROOT_DIR/gateway-python:$ROOT_DIR/gen/python${PYTHONPATH:+:$PYTHONPATH}"
export CGO_ENABLED=0
export GRPC_ENABLE_FORK_SUPPORT=0
TGSRL_WORKER_REGISTRY_SIGNING_KEY=$("$PYTHON_BIN" -c 'import secrets; print(secrets.token_hex(32))')
export TGSRL_WORKER_REGISTRY_SIGNING_KEY

SCHEDULER_PORT=$(free_port)
REGISTRY_PORT=$(free_port)
RUNTIME_PORT=$(free_port)
CONTROLLER_PORT=$(free_port)
OPERATOR_PORT=$(free_port)
GATEWAY_PORT=$(free_port)
SCHEDULER_TARGET="127.0.0.1:$SCHEDULER_PORT"
REGISTRY_URL="http://127.0.0.1:$REGISTRY_PORT"
RUNTIME_TARGET="127.0.0.1:$RUNTIME_PORT"
CONTROLLER_TARGET="127.0.0.1:$CONTROLLER_PORT"
OPERATOR_TARGET="127.0.0.1:$OPERATOR_PORT"
GATEWAY_URL="http://127.0.0.1:$GATEWAY_PORT"

go build -o "$BIN_DIR/scheduler" ./scheduler-go/cmd/scheduler
go build -o "$BIN_DIR/job-controller" ./job-controller-go/cmd/job-controller
go build -o "$BIN_DIR/operator" ./cmd/operator
go build -o "$BIN_DIR/tgsrl-worker-bootstrap" ./cmd/tgsrl-worker-bootstrap

start_process scheduler "$BIN_DIR/scheduler" \
  -listen "$SCHEDULER_TARGET" \
  -config-root "$ROOT_DIR" \
  -manifest compatibility/manifests/cpu-process-verl.yaml \
  -state-dir "$STATE_DIR/scheduler" \
  -metrics-listen "" \
  -worker-registry-listen "127.0.0.1:$REGISTRY_PORT" \
  -worker-registry-runtime-target "$RUNTIME_TARGET" \
  -worker-registry-state "$STATE_DIR/worker-registry.json"
SCHEDULER_PID=$LAST_PID
wait_for_tcp scheduler "$SCHEDULER_PORT" "$SCHEDULER_PID"

start_process runtime "$PYTHON_BIN" -m tgsrl_runtime.runtime_app \
  --bind "$RUNTIME_TARGET" \
  --scheduler-target "$SCHEDULER_TARGET" \
  --operator-target "$OPERATOR_TARGET" \
  --job-control-target "$CONTROLLER_TARGET" \
  --state-db "$STATE_DIR/runtime.db" \
  --config-root "$ROOT_DIR" \
  --manifest compatibility/manifests/cpu-process-verl.yaml
RUNTIME_PID=$LAST_PID
wait_for_tcp runtime "$RUNTIME_PORT" "$RUNTIME_PID"

start_process controller "$BIN_DIR/job-controller" \
  -listen "$CONTROLLER_TARGET" \
  -runtime-target "$RUNTIME_TARGET" \
  -state-dir "$STATE_DIR/controller"
CONTROLLER_PID=$LAST_PID
wait_for_tcp controller "$CONTROLLER_PORT" "$CONTROLLER_PID"

printf '%s\n' "$TGSRL_WORKER_REGISTRY_SIGNING_KEY" > "$STATE_DIR/registry-key"
chmod 600 "$STATE_DIR/registry-key"
start_process operator "$BIN_DIR/operator" \
  -mode process \
  -listen "$OPERATOR_TARGET" \
  -scheduler "$SCHEDULER_TARGET" \
  -control "$CONTROLLER_TARGET" \
  -runtime "$RUNTIME_TARGET" \
  -cursor-dir "$STATE_DIR/operator" \
  -worker-bootstrap-binary "$BIN_DIR/tgsrl-worker-bootstrap" \
  -worker-registry-url "$REGISTRY_URL" \
  -worker-registry-signing-key-file "$STATE_DIR/registry-key" \
  -process-state-dir "$STATE_DIR/workloads"
OPERATOR_PID=$LAST_PID
wait_for_tcp operator "$OPERATOR_PORT" "$OPERATOR_PID"

start_process gateway "$PYTHON_BIN" -m tgsrl_gateway serve \
  --host 127.0.0.1 \
  --port "$GATEWAY_PORT" \
  --backend-mode grpc \
  --job-control-target "$CONTROLLER_TARGET" \
  --scheduler-target "$SCHEDULER_TARGET" \
  --runtime-target "$RUNTIME_TARGET" \
  --experiment-target "$RUNTIME_TARGET"
GATEWAY_PID=$LAST_PID
wait_for_tcp gateway "$GATEWAY_PORT" "$GATEWAY_PID"

export TGSRL_GATE_GATEWAY_URL="$GATEWAY_URL"
export TGSRL_GATE_RUNTIME_TARGET="$RUNTIME_TARGET"
export TGSRL_GATE_WORK_DIR="$STATE_DIR/gate-workloads"
export TGSRL_GATE_SERVICE_LOG_DIR="$LOG_DIR"
export TGSRL_GATE_PROCESS_LOG_ROOT="$STATE_DIR/workloads"
export TGSRL_GATE_PYTHON="$PYTHON_BIN"
export TGSRL_GATE_REPO_ROOT="$ROOT_DIR"
export TGSRL_GATE_TIMEOUT=${TGSRL_GATE_TIMEOUT:-30}

if ! "$PYTHON_BIN" scripts/gate-tools.py \
  --manifest configs/gates/gate-gi-process.json \
  --output-dir "$OUTPUT_DIR" \
  cpu-smoke --smoke-command "$PYTHON_BIN -c 'print(\"full-stack ready\")'"; then
  FAILED=1
  exit 1
fi
"$PYTHON_BIN" scripts/gate-tools.py \
  --manifest configs/gates/gate-gi-process.json \
  --output-dir "$OUTPUT_DIR" report
printf 'full-stack Gate CPU integration passed\n'
