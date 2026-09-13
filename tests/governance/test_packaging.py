from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[2]


def test_sdist_wheel_contains_migrations_and_runs_outside_checkout(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for name in ("pyproject.toml", "README.md", "LICENSE"):
        shutil.copy2(ROOT / name, source / name)
    for name in ("adapters", "runtime-python", "gateway-python", "gen/python"):
        shutil.copytree(
            ROOT / name,
            source / name,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.egg-info"),
        )
    artifacts = tmp_path / "artifacts"
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)

    def run(*argv: str) -> str:
        result = subprocess.run(
            argv,
            cwd=tmp_path,
            env=environment,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return result.stdout

    run("uv", "build", "--offline", "--sdist", str(source), "--out-dir", str(artifacts))
    archive = next(artifacts.glob("*.tar.gz"))
    run("uv", "build", "--offline", "--wheel", str(archive), "--out-dir", str(artifacts))
    wheel = next(artifacts.glob("*.whl"))
    with ZipFile(wheel) as package:
        migrations = {
            Path(name).name
            for name in package.namelist()
            if name.startswith("tgsrl_runtime/storage/migrations/") and name.endswith(".sql")
        }
        assert migrations == {
            path.name
            for path in (ROOT / "runtime-python/tgsrl_runtime/storage/migrations").glob("*.sql")
        }
        assert any(name.endswith("/licenses/LICENSE") for name in package.namelist())

    installed = tmp_path / "installed"
    run("uv", "pip", "install", "--offline", "--no-deps", "--target", str(installed), str(wheel))
    result = run(
        sys.executable,
        "-I",
        "-c",
        """
import json
import sqlite3
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import tgsrl_runtime
from tgsrl_runtime.storage import SQLiteStore
from tgsrl.v1 import runtime_pb2
assert Path(tgsrl_runtime.__file__).is_relative_to(Path(sys.argv[1]))
path = Path(sys.argv[2])
manifest = runtime_pb2.RuntimeManifest(run_id='installed-run', job_id='installed-job')
with SQLiteStore(path) as store:
    store.save_runtime_manifest(manifest)
    store.healthcheck()
with sqlite3.connect(path) as connection:
    migrations = [row[0] for row in connection.execute('SELECT version FROM schema_migrations')]
    tables = {row[0] for row in connection.execute('SELECT name FROM sqlite_master')}
    required = {'runtime_manifests', 'trace_events', 'component_statuses', 'replay_schedule_steps'}
    assert required <= tables
with SQLiteStore(path) as store:
    assert store.get_runtime_manifest('installed-run') == manifest
    store.healthcheck()
print(json.dumps(sorted(migrations)))
""",
        str(installed),
        str(tmp_path / "installed.sqlite3"),
    )
    assert set(json.loads(result)) == migrations


def test_docker_context_final_exclusions_protect_local_configuration() -> None:
    patterns = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    last_include = max(index for index, pattern in enumerate(patterns) if pattern.startswith("!"))
    exclusions = set(patterns[last_include + 1 :])
    assert {
        "configs/hardware/environment.json",
        "configs/hardware/*-job.json",
        "**/.env",
        "**/.env.*",
        "**/kubeconfig",
        "**/kubeconfig.*",
        "**/credentials.json",
        "**/*.credentials.json",
        "**/*.key",
        "**/*.pem",
        "**/*.pid",
        "**/*.sock",
        "**/*.egg-info",
    } <= exclusions
    assert "!configs/**" in patterns
    assert "configs/hardware/*.json" not in exclusions
    assert "!LICENSE" in patterns
