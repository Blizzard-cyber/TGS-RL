from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HARDWARE_WORKFLOW = ROOT / ".github" / "workflows" / "hardware-validation.yaml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yaml"


def run_script(name: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / name), *arguments],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_documentation_links_and_commands_match_repository() -> None:
    checked = run_script("check-docs.py")
    assert checked.returncode == 0, checked.stderr
    assert "documentation-ok" in checked.stdout


def test_docs_check_handles_unstaged_deleted_document(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    removed = tmp_path / "removed.md"
    removed.write_text("# Removed\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(tmp_path), "add", "removed.md"], check=True)
    removed.unlink()
    retained = tmp_path / "retained.md"
    retained.write_text("# Retained\n", encoding="utf-8")
    spec = importlib.util.spec_from_file_location("check_docs", ROOT / "scripts/check-docs.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ROOT = tmp_path
    assert module.repository_markdown() == [retained]


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
    assert any(
        item["name"] == "vllm"
        and item["versionInfo"] == "0.28.0"
        and item["properties"]["scope"] == "gpu-workload"
        for item in document["packages"]
    )
    protobuf_scopes = {
        item["properties"]["scope"] for item in document["packages"] if item["name"] == "protobuf"
    }
    assert protobuf_scopes == {"locked", "gpu-workload"}
    assert {item["path"] for item in document["lockfiles"]} >= {
        "configs/hardware/gpu-requirements.lock"
    }
    package_ids = [item["SPDXID"] for item in document["packages"]]
    assert package_ids == sorted(package_ids)
    assert len(package_ids) == len(set(package_ids))


def test_compatibility_claims_are_evidence_backed() -> None:
    result = run_script("check-compatibility.py")
    assert result.returncode == 0, result.stderr

    hardware_status = "hardware-verified-single-node"
    for path in (
        ROOT / "compatibility" / "manifests" / "gpu-smoke-verl.yaml",
        ROOT / "compatibility" / "manifests" / "hami-smoke-verl.yaml",
        ROOT / "compatibility" / "manifests" / "hami-concurrency-verl.yaml",
        ROOT / "compatibility" / "profiles" / "gpu-smoke-verl-v1.yaml",
        ROOT / "compatibility" / "profiles" / "hami-smoke-verl-v1.yaml",
    ):
        assert f'status: "{hardware_status}"' in path.read_text(encoding="utf-8")
    assert 'status: "hardware-verified-single-node"' in (
        ROOT / "configs" / "scenarios" / "gpu-smoke-verl.yaml"
    ).read_text(encoding="utf-8")
    assert 'verification_status: "hardware-verified-single-node"' in (
        ROOT / "configs" / "capabilities" / "nvidia-gpu-smoke.yaml"
    ).read_text(encoding="utf-8")

    matrix = json.loads((ROOT / "compatibility" / "matrix.json").read_text(encoding="utf-8"))
    assert hardware_status in matrix["policy"]
    statuses = {item["id"]: item["status"] for item in matrix["combinations"]}
    assert statuses["local-product-contract"] == "supported"
    assert statuses["verl-ray-pytorch-vllm-nvidia"] == "conditional"
    assert statuses["openrlhf-ray-pytorch-sglang-nvidia"] == "conditional"
    verl = next(
        item for item in matrix["combinations"] if item["id"] == "verl-ray-pytorch-vllm-nvidia"
    )
    assert verl["missing_dependencies"] == ["nvidia"]


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
    assert parse_images("ARG BASE=image.example/base@sha256:" + "b" * 64 + "\nFROM ${BASE}") == [
        "image.example/base@sha256:" + "b" * 64
    ]
    assert "gpu-preflight.sh" in source


def test_public_content_private_ip_pattern_does_not_match_dependency_versions() -> None:
    checker = (ROOT / "scripts" / "check-public-content.sh").read_text(encoding="utf-8")

    assert "private_ip_prefix" in checker
    assert "dependency versions are filtered separately" in checker
    assert '"versionInfo"' in checker
    assert "self_check" in checker


def test_hardware_workflow_runs_locked_workloads_without_cross_claiming_evidence() -> None:
    text = HARDWARE_WORKFLOW.read_text(encoding="utf-8")

    assert "hardware-run --evidence" in text
    assert "GPU_SINGLE_NODE" in text
    assert "gpu-multi-node" not in text
    assert "Manual operator instructions" not in text
    assert "python3 scripts/gate-tools.py simulate" in text
    assert '--evidence "${{ github.event.inputs.evidence }}"' not in text


def test_hardware_workflow_has_cpu_and_automated_self_hosted_gpu_entries() -> None:
    text = HARDWARE_WORKFLOW.read_text(encoding="utf-8")

    assert "cpu-integration:" in text
    assert "make gate-cpu-integration" in text
    assert ".cache/tgsrl/gate-gi-process/artifacts/logs/**" in text
    assert "gpu-runner:" in text
    assert "runs-on: [self-hosted, gpu]" in text
    assert "hardware-run --evidence" in text
    assert "e1-e8-evaluate" in text
    assert "evidence_run_id" in text
    assert "campaign-evaluate" in text
    assert "--require-pass" in text
    assert "actions: read" in text


def test_hardware_workflow_can_execute_the_complete_campaign() -> None:
    text = HARDWARE_WORKFLOW.read_text(encoding="utf-8")

    assert "e1-e8-run" in text
    assert "campaign-runner:" in text
    assert "runs-on: [self-hosted, gpu]" in text
    assert "environment: hardware-validation" in text
    assert "scripts/gate-tools.py campaign-run" in text
    assert "--executor" not in text
    assert '--driver "${CAMPAIGN_DRIVER}"' in text
    assert "vars.TGSRL_CAMPAIGN_DRIVER" in text
    assert "gate-e1-e8-evidence" in text


def test_ci_enforces_static_analysis_proto_compatibility_and_browser_smoke() -> None:
    text = CI_WORKFLOW.read_text(encoding="utf-8")

    assert "go install honnef.co/go/tools/cmd/staticcheck@v0.7.0" in text
    assert "run: make staticcheck" in text
    assert "github.event.before" in text
    assert "if: github.event_name == 'pull_request'" not in text
    assert "npx playwright install --with-deps chromium" in text
    assert "npm run test:browser" in text
