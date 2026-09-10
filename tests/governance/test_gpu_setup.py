from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "patch-kueue-config.py"
SPEC = importlib.util.spec_from_file_location("tgsrl_patch_kueue_config", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_kueue_patch_adds_resources_and_is_idempotent() -> None:
    original = "apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\n"
    patched = MODULE.patch(original)

    assert "resources:\n  deviceClassMappings:\n" in patched
    assert patched.count("name: tgsrl.io/gpu") == 1
    lines = patched.splitlines()
    mapping_index = lines.index("  deviceClassMappings:")
    assert lines[mapping_index + 1 : mapping_index + 3] == [
        "    - name: tgsrl.io/gpu",
        "      deviceClassNames: [gpu.nvidia.com, mig.nvidia.com]",
    ]
    assert MODULE.patch(patched) == patched


def test_kueue_patch_preserves_existing_resources() -> None:
    original = (
        "apiVersion: config.kueue.x-k8s.io/v1beta2\n"
        "kind: Configuration\n"
        "resources:\n"
        "  excludeResourcePrefixes: [example.com]\n"
    )

    patched = MODULE.patch(original)

    assert patched.count("resources:") == 1
    assert "excludeResourcePrefixes: [example.com]" in patched
    assert "deviceClassNames: [gpu.nvidia.com, mig.nvidia.com]" in patched
    lines = patched.splitlines()
    assert lines.index("  deviceClassMappings:") > lines.index("resources:")
    assert "    - name: tgsrl.io/gpu" in lines


def test_gpu_job_template_uses_current_canonical_contract_id() -> None:
    from google.protobuf import json_format
    from tgsrl.v1 import execution_pb2

    from adapters.contracts import canonical_contract_id

    job = json.loads((ROOT / "configs/hardware/verl-job.example.json").read_text(encoding="utf-8"))
    contract = execution_pb2.ExecutionContract()
    json_format.ParseDict(job["executionContract"], contract)

    assert contract.contract_id == canonical_contract_id(contract)


def test_gpu_build_script_forwards_only_an_immutable_base_image() -> None:
    text = (ROOT / "scripts" / "gpu-build-images.sh").read_text(encoding="utf-8")

    assert "TGSRL_VERL_BASE_IMAGE" in text
    assert "@sha256:" in text
    assert '--build-arg "$build_arg"' in text
    assert "TGSRL_IMAGE_PLATFORM=linux/amd64" in text
    assert "GPU evidence images require a clean checkout" in text


def test_gpu_workload_lock_matches_declared_direct_versions() -> None:
    declared = {}
    for line in (
        (ROOT / "configs" / "hardware" / "gpu-requirements.in")
        .read_text(encoding="utf-8")
        .splitlines()
    ):
        if "==" in line:
            name, version = line.split("==", 1)
            declared[name.split("[", 1)[0]] = version
    locked = (ROOT / "configs" / "hardware" / "gpu-requirements.lock").read_text(encoding="utf-8")

    for name, version in declared.items():
        locked_line = next(
            (line for line in locked.splitlines() if line.startswith(f"{name}==")), None
        )
        assert locked_line is not None
        locked_version = locked_line.split("==", 1)[1].split()[0]
        assert locked_version.split("+", 1)[0] == version
    dockerfile = (ROOT / "Dockerfile.gpu-smoke").read_text(encoding="utf-8")
    assert "https://download.pytorch.org/whl/cu130" in dockerfile
    assert "--only-binary=:all:" in dockerfile
    assert "--index-strategy" not in dockerfile


def test_gpu_runtime_test_image_is_bom_pinned() -> None:
    preflight = (ROOT / "scripts" / "gpu-preflight.sh").read_text(encoding="utf-8")
    bom = (ROOT / "compatibility" / "bom" / "runtime.yaml").read_text(encoding="utf-8")
    match = re.search(r"(nvidia/cuda:13[.]0[.]2-base-ubuntu24[.]04@sha256:[a-f0-9]{64})", preflight)
    assert match is not None
    repository, digest = match.group(1).split("@", 1)

    assert f'image: "docker.io/{repository}"' in bom
    assert f'image_digest: "{digest}"' in bom


def test_gpu_shell_scripts_are_syntactically_valid() -> None:
    scripts = sorted((ROOT / "scripts").glob("gpu-*.sh"))
    assert scripts
    for script in scripts:
        subprocess.run(["bash", "-n", str(script)], check=True)
        assert os.access(script, os.X_OK), f"{script.name} must be executable"


def test_bootstrap_mount_does_not_shadow_gpu_workload() -> None:
    source = (ROOT / "operator-go" / "compiler" / "materialize.go").read_text(encoding="utf-8")

    assert 'WorkerBootstrapMountPath  = "/var/run/tgsrl-bootstrap"' in source
    assert 'WorkerBootstrapMountPath  = "/opt/tgsrl"' not in source


def test_gpu_smoke_requires_e1_to_pass() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("gpu-smoke:", 1)[1].split("\ngpu-down:", 1)[0]

    assert "--experiment E1" in target
    assert 'select(.experiment_id == "E1")' in target
    assert '.status == "PASSED"' in target
