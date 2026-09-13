#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
ACTION=${1:-all}
OUTPUT=${TGSRL_A10_READINESS_DIR:-.cache/tgsrl/a10-readiness}
ARCHIVE_ROOT=${TGSRL_A10_ARCHIVE_DIR:-.cache/tgsrl/archive}
HAMI_OUTPUT=.cache/tgsrl/hami-concurrency-smoke
RESTORE_OUTPUT=.cache/tgsrl/gpu-smoke
FAULT_OUTPUT=.cache/tgsrl/engineering-fault-readiness
RUNTIME_ENV=${TGSRL_GPU_RUNTIME_ENV:-.cache/tgsrl/gpu-runtime.env}
DRIVER_STATE=${TGSRL_HARDWARE_DRIVER_STATE_DIR:-.cache/tgsrl/hardware-driver}
stack_active=0
hami_active=0

require_clean() {
  [[ -z $(git status --porcelain=v1 --untracked-files=all) ]] || {
    echo 'A10 readiness requires a clean checkout' >&2
    return 1
  }
}

load_runtime_environment() {
  [[ -f "$RUNTIME_ENV" ]] || {
    echo "missing $RUNTIME_ENV; run make gpu-render-config" >&2
    return 1
  }
  set -a
  # shellcheck disable=SC1090
  . "$RUNTIME_ENV"
  set +a
}

verify_a10() {
  local evidence_root=${1:-$OUTPUT}
  command -v nvidia-smi >/dev/null 2>&1 || {
    echo 'A10 readiness requires nvidia-smi' >&2
    return 1
  }
  mkdir -p "$evidence_root"
  local inventory
  inventory=$(nvidia-smi --query-gpu=uuid,name,driver_version,memory.total \
    --format=csv,noheader,nounits)
  python3 - "$evidence_root/host-gpu.json" "$inventory" <<'PY'
import csv
import json
import sys
from pathlib import Path

rows = list(csv.reader(sys.argv[2].splitlines()))
if len(rows) != 1:
    raise SystemExit(f"A10 readiness requires exactly one GPU, found {len(rows)}")
uuid, name, driver, memory_mib = (value.strip() for value in rows[0])
if name != "NVIDIA A10":
    raise SystemExit(f"A10 readiness requires NVIDIA A10, found {name or 'unknown'}")
payload = {
    "schema_version": "tgsrl.io/a10-host/v1alpha1",
    "gpu_count": 1,
    "uuid": uuid,
    "name": name,
    "driver_version": driver,
    "memory_mib": int(memory_mib),
}
path = Path(sys.argv[1])
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
path.chmod(0o600)
PY
}

archive_existing() {
  local timestamp short_sha archive path
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  short_sha=$(git rev-parse --short=12 HEAD)
  mkdir -p "$ARCHIVE_ROOT"
  archive=$(mktemp -d "$ARCHIVE_ROOT/a10-readiness-${timestamp}-${short_sha}.XXXXXX")
  for path in "$@"; do
    if [[ -e "$path" ]]; then
      mkdir -p "$archive"
      mv "$path" "$archive/$(basename "$path")"
    fi
  done
}

cleanup() {
  local status=$?
  trap - EXIT
  if (( stack_active == 1 )); then
    make gpu-down >/dev/null 2>&1 || true
  fi
  if (( hami_active == 1 )); then
    make gpu-restore-dra >/dev/null 2>&1 || true
  fi
  exit "$status"
}

run_hami() {
  verify_a10 "$HAMI_OUTPUT"
  make gpu-down >/dev/null 2>&1 || true
  hami_active=1
  make gpu-prepare-hami
  make gpu-hami-status
  stack_active=1
  make gpu-hami-up
  make gpu-hami-concurrency-smoke
  make gpu-down
  stack_active=0
  make gpu-restore-dra
  hami_active=0
}

write_summary() {
  python3 - "$OUTPUT" "$HAMI_OUTPUT" "$RESTORE_OUTPUT" "$FAULT_OUTPUT" <<'PY'
import hashlib
import json
import subprocess
import sys
from pathlib import Path

root, hami_root, restore_root, fault_root = (Path(value) for value in sys.argv[1:])
host = json.loads((root / "host-gpu.json").read_text(encoding="utf-8"))

def component(name: str, report: Path, experiment_id: str) -> dict[str, object]:
    if not report.is_file():
        return {"name": name, "status": "NOT_RUN", "report": str(report)}
    value = json.loads(report.read_text(encoding="utf-8"))
    selected = next(
        (item for item in value.get("experiments", []) if item.get("experiment_id") == experiment_id),
        None,
    )
    if selected is None:
        return {"name": name, "status": "INVALID", "report": str(report)}
    return {
        "name": name,
        "status": selected.get("status", "INVALID"),
        "evidence": selected.get("evidence"),
        "report": str(report),
        "sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
    }

def direct_component(name: str, report: Path) -> dict[str, object]:
    if not report.is_file():
        return {"name": name, "status": "NOT_RUN", "report": str(report)}
    value = json.loads(report.read_text(encoding="utf-8"))
    return {
        "name": name,
        "status": value.get("status", "INVALID"),
        "evidence": value.get("evidence"),
        "report": str(report),
        "sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
    }

components = [
    direct_component("engineering_failure_paths", fault_root / "report.json"),
    component("full_gpu_lifecycle", root / "campaign-report.json", "A10-FULL"),
    component("hami_two_worker_concurrency", hami_root / "campaign-report.json", "H2"),
    component("dra_restore_e1_regression", restore_root / "campaign-report.json", "E1"),
]
status = "PASSED" if all(item["status"] == "PASSED" for item in components) else "INCOMPLETE"
payload = {
    "schema_version": "tgsrl.io/a10-readiness-summary/v1alpha1",
    "status": status,
    "git_commit": subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip(),
    "git_dirty": bool(
        subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    ),
    "host_gpu": host,
    "components": components,
    "not_run": [
        "MIG: NVIDIA A10 does not expose a usable MIG topology",
        "GPU multi-node: the readiness environment is single-node",
        "GPU Pod/node/network fault injection: no safe target-environment hooks are configured",
    ],
}
path = root / "readiness-summary.json"
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
path.chmod(0o600)
if status != "PASSED":
    raise SystemExit("A10 readiness summary is incomplete")
PY
}

trap cleanup EXIT
case "$ACTION" in
  verify)
    if [[ -f "$OUTPUT/campaign-report.json" ]]; then
      echo "refusing to overwrite A10 Full evidence in $OUTPUT; archive it or set A10_READINESS_REPORTS" >&2
      exit 1
    fi
    verify_a10
    ;;
  hami)
    require_clean
    archive_existing "$HAMI_OUTPUT" "$DRIVER_STATE"
    load_runtime_environment
    run_hami
    ;;
  all)
    require_clean
    archive_existing "$OUTPUT" "$HAMI_OUTPUT" "$RESTORE_OUTPUT" "$FAULT_OUTPUT" "$DRIVER_STATE"
    load_runtime_environment
    make engineering-fault-readiness
    verify_a10
    make gpu-preflight
    stack_active=1
    make gpu-up
    make gpu-a10-full-readiness A10_READINESS_REPORTS="$OUTPUT"
    make gpu-down
    stack_active=0
    run_hami
    make gpu-preflight
    stack_active=1
    make gpu-up
    make gpu-smoke
    make gpu-down
    stack_active=0
    write_summary
    ;;
  *)
    echo 'usage: scripts/gpu-a10-readiness.sh verify|hami|all' >&2
    exit 2
    ;;
esac
