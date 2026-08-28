#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-"$ROOT/.venv/bin/python"}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "python interpreter not found: $PYTHON_BIN" >&2
  exit 1
fi

DB_DIR="$(mktemp -d)"
trap 'rm -rf "$DB_DIR"' EXIT
DB_PATH="$DB_DIR/migrations.sqlite3"

PYTHONPATH="$ROOT/gen/python:$ROOT/runtime-python:$ROOT" "$PYTHON_BIN" - <<'PY' "$DB_PATH"
from pathlib import Path
import sqlite3
import sys

from tgsrl_runtime.storage.sqlite_store import SQLiteStore

path = Path(sys.argv[1])
store = SQLiteStore(path)
store.healthcheck()
connection = sqlite3.connect(path)
versions = [row[0] for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version")]
tables = {
    row[0]
    for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    )
}
connection.close()
store.close()

required_versions = {"0001_initial.sql", "0002_component_statuses.sql"}
required_tables = {
    "schema_migrations",
    "intents",
    "trace_events",
    "replays",
    "experiments",
    "runtime_manifests",
    "runtime_units",
    "sandboxes",
    "runtime_events",
    "runtime_checkpoints",
    "component_statuses",
    "idempotency_records",
    "delete_audit",
}

missing_versions = required_versions.difference(versions)
missing_tables = required_tables.difference(tables)
if missing_versions:
    raise SystemExit(f"missing schema migrations: {sorted(missing_versions)}")
if missing_tables:
    raise SystemExit(f"missing tables: {sorted(missing_tables)}")
print("migrations-ok")
PY
