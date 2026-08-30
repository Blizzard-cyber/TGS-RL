from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def run_script(name: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / name), *arguments],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_sbom_is_current_and_covers_all_lockfile_ecosystems(tmp_path: Path) -> None:
    checked = run_script("generate-sbom.py", "--check")
    assert checked.returncode == 0, checked.stderr

    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    assert run_script("generate-sbom.py", "--output", str(first)).returncode == 0
    assert run_script("generate-sbom.py", "--output", str(second)).returncode == 0
    assert first.read_bytes() == second.read_bytes()

    document = json.loads(first.read_text(encoding="utf-8"))
    assert document["spdxVersion"] == "SPDX-2.3"
    ecosystems = {item["properties"]["ecosystem"] for item in document["packages"]}
    assert ecosystems == {"golang", "npm", "pypi"}
    package_ids = [item["SPDXID"] for item in document["packages"]]
    assert package_ids == sorted(package_ids)
    assert len(package_ids) == len(set(package_ids))


def test_compatibility_claims_are_evidence_backed() -> None:
    result = run_script("check-compatibility.py")
    assert result.returncode == 0, result.stderr

    matrix = json.loads((ROOT / "compatibility" / "matrix.json").read_text(encoding="utf-8"))
    statuses = {item["id"]: item["status"] for item in matrix["combinations"]}
    assert statuses["local-product-contract"] == "supported"
    assert statuses["verl-ray-pytorch-vllm-nvidia"] == "conditional"
    assert statuses["openrlhf-ray-pytorch-sglang-nvidia"] == "conditional"


def test_dockerfile_platform_flag_does_not_hide_unpinned_images() -> None:
    source = (ROOT / "scripts" / "check-compatibility.py").read_text(encoding="utf-8")
    namespace: dict[str, object] = {
        "__name__": "compatibility_check_test",
        "__file__": str(ROOT / "scripts" / "check-compatibility.py"),
    }
    exec(compile(source, "check-compatibility.py", "exec"), namespace)
    parse_images = namespace["parse_dockerfile_images"]
    assert callable(parse_images)

    assert parse_images("FROM --platform=$BUILDPLATFORM image.example/base@sha256:" + "a" * 64) == [
        "image.example/base@sha256:" + "a" * 64
    ]
    assert parse_images("FROM --platform=linux/amd64 image.example/unpinned:latest") == [
        "image.example/unpinned:latest"
    ]


def test_empty_patch_ledger_passes_replay_gate() -> None:
    result = run_script("check-upstream-patches.py")
    assert result.returncode == 0, result.stderr
    assert "0 registered patches" in result.stdout
