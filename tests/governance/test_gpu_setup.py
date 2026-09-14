from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

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


@pytest.mark.parametrize(
    "template",
    [
        "verl-job.example.json",
        "verl-lifecycle-job.example.json",
        "hami-job.example.json",
        "hami-concurrency-job.example.json",
    ],
)
def test_gpu_job_template_uses_current_canonical_contract_id(template: str) -> None:
    from google.protobuf import json_format
    from tgsrl.v1 import execution_pb2

    from adapters.contracts import canonical_contract_id, validate_execution_contract

    job = json.loads((ROOT / "configs" / "hardware" / template).read_text(encoding="utf-8"))
    contract = execution_pb2.ExecutionContract()
    json_format.ParseDict(job["executionContract"], contract)

    validate_execution_contract(contract)
    assert contract.contract_id == canonical_contract_id(contract)


def test_gpu_build_script_forwards_only_an_immutable_base_image() -> None:
    text = (ROOT / "scripts" / "gpu-build-images.sh").read_text(encoding="utf-8")

    assert "TGSRL_VERL_BASE_IMAGE" in text
    assert "TGSRL_GO_BASE_IMAGE" in text
    assert "TGSRL_DISTROLESS_BASE_IMAGE" in text
    assert "TGSRL_PYPI_INDEX_URL" in text
    assert "TGSRL_PYTORCH_INDEX_URL" in text
    assert "@sha256:" in text
    assert 'args+=(--build-arg "$build_arg")' in text
    assert "TGSRL_IMAGE_PLATFORM=linux/amd64" in text
    assert "GPU evidence images require a clean checkout" in text
    for target in (
        "scheduler-nvidia",
        "job-controller",
        "operator",
        "runtime",
        "gateway",
        "console",
    ):
        assert f"build_target {target}" in text


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
    assert "--no-binary=antlr4-python3-runtime" in dockerfile
    assert "--mount=type=cache,target=/root/.cache/pip" in dockerfile
    assert "--index-url ${TGSRL_PYTORCH_INDEX_URL}" in dockerfile
    assert "--extra-index-url ${TGSRL_PYPI_INDEX_URL}" in dockerfile
    assert dockerfile.index("ENV PYTHONPATH=") < dockerfile.index(
        "from tgsrl.v1 import runtime_pb2"
    )
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


def test_gpu_network_profiles_keep_integrity_checks_and_are_explicit() -> None:
    helper = (ROOT / "scripts" / "lib" / "network-profile.sh").read_text(encoding="utf-8")
    install = (ROOT / "scripts" / "gpu-install-host.sh").read_text(encoding="utf-8")
    create = (ROOT / "scripts" / "gpu-create-cluster.sh").read_text(encoding="utf-8")
    prepare = (ROOT / "scripts" / "gpu-prepare-cluster.sh").read_text(encoding="utf-8")
    cn_profile = (ROOT / "configs" / "network" / "cn.env").read_text(encoding="utf-8")

    subprocess.run(["bash", "-n", str(ROOT / "scripts" / "lib" / "network-profile.sh")], check=True)
    subprocess.run(["bash", "-n", str(ROOT / "configs" / "network" / "cn.env")], check=True)
    assert "official or cn" in helper
    assert "--continue-at -" in helper
    assert "checksum verification failed" in helper
    assert "sha256sum" in helper
    assert "tgsrl_download_verified" in install
    assert "TGSRL_DOCKER_REGISTRY_MIRRORS" in install
    assert "TGSRL_UV_PYTHON_INSTALL_MIRROR" in install
    assert "uv_python_args+=(--mirror" in install
    assert "MIN_DRIVER_VERSION" in install
    assert "python3 python3-pip python3-venv ruby socat xz-utils" in install
    assert "GO_VERSION" in install and "NODE_VERSION" in install
    assert "BUF_VERSION" in install and "STATICCHECK_VERSION" in install
    assert "STATICCHECK_MODULE_VERSION=0.7.0" in install
    assert "tgsrl-go-mod.XXXXXX" in install
    assert "/usr/local/bin/go mod download all" in install
    assert "npm --prefix console ci --registry" in install
    assert "1153d3d50e0ac764b447adfe05c2bcf08e889d42a02e0fe0259bd47f6733ad7f" in install
    assert "2f2c0da162318f0de47665410c7c8c2ed3d36c8f3105de4bbc61176c70a7cbf2" in install
    assert "8720830e26a733da55bb89bcd3cb44849c0965fc0c44fb5d691cccdc64dca5af" in install
    assert "br_netfilter" in install
    assert "net.ipv4.ip_forward=1" in install
    assert "tgsrl_load_network_profile" in create
    assert "TGSRL_MINIKUBE_IMAGE_REPOSITORY" in create
    assert "TGSRL_MINIKUBE_BASE_IMAGE" in create
    assert "TGSRL_MINIKUBE_ALLOW_ROOT" in create
    assert "start_args+=(--force)" in create
    assert "preload_kubernetes_binaries" in create
    assert "component in kubeadm kubelet kubectl" in create
    assert "$HOME/.cache/tgsrl/registry-forward.pid" in create
    assert "$HOME/.cache/tgsrl/registry-forward.log" in create
    assert "TGSRL_K8S_OCI_REGISTRY" in prepare
    assert "TGSRL_K8S_IMAGE_REGISTRY" in prepare
    assert "image.repository=${K8S_IMAGE_REGISTRY}/nfd/node-feature-discovery" in prepare
    assert "image.repository=${K8S_IMAGE_REGISTRY}/dra-driver-nvidia" in prepare
    assert "Kueue webhook has no ready endpoint" in prepare
    assert "Kueue queue resources failed after webhook readiness retries" in prepare
    assert "dra_daemonsets" in prepare
    assert 'rollout status -n dra-driver-nvidia-gpu "$daemonset"' in prepare
    assert "daemonset --all" not in prepare
    assert "https://files.m.daocloud.io" in cn_profile
    assert "https://pypi.tuna.tsinghua.edu.cn/simple" in cn_profile
    assert "m.daocloud.io/gcr.io/distroless" in cn_profile
    assert "m.daocloud.io/docker.io/nvidia/cuda" in cn_profile
    assert "m.daocloud.io/gcr.io/k8s-minikube/kicbase" in cn_profile
    assert "TGSRL_MINIKUBE_IMAGE_REPOSITORY:=auto" in cn_profile
    assert "http://" not in cn_profile

    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                f"ROOT_DIR={ROOT!s}; "
                "TGSRL_NETWORK_PROFILE=cn; "
                "source scripts/lib/network-profile.sh; "
                "tgsrl_load_network_profile; "
                "tgsrl_mirror_url https://dl.k8s.io/release/test"
            ),
        ],
        check=True,
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert result.stdout.strip() == "https://files.m.daocloud.io/dl.k8s.io/release/test"

    compose_env = subprocess.run(
        [
            "bash",
            "-c",
            (
                f"ROOT_DIR={ROOT!s}; "
                "TGSRL_NETWORK_PROFILE=cn; "
                "source scripts/lib/network-profile.sh; "
                "tgsrl_load_network_profile; "
                'printf \'%s\n\' "$TGSRL_GO_BASE_IMAGE" "$TGSRL_NPM_REGISTRY"'
            ),
        ],
        check=True,
        cwd=ROOT,
        text=True,
        capture_output=True,
    ).stdout.splitlines()
    assert compose_env[0].startswith("m.daocloud.io/docker.io/library/golang:")
    assert compose_env[1] == "https://registry.npmmirror.com"

    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    local_dockerfile = (ROOT / "Dockerfile.local").read_text(encoding="utf-8")
    bootstrap_dockerfile = (ROOT / "Dockerfile.worker-bootstrap").read_text(encoding="utf-8")
    gpu_dockerfile = (ROOT / "Dockerfile.gpu-smoke").read_text(encoding="utf-8")
    assert "x-tgsrl-build-args: &tgsrl-build-args" in compose
    assert "args: *tgsrl-build-args" in compose
    assert 'GOPROXY="${TGSRL_GOPROXY}" go mod download' in local_dockerfile
    assert "ENV UV_PROJECT_ENVIRONMENT=/opt/tgsrl/venv" in local_dockerfile
    assert "ENV PATH=/opt/tgsrl/venv/bin:$PATH" in local_dockerfile
    assert "/workspace/.venv" not in local_dockerfile
    assert "FROM ${TGSRL_DISTROLESS_BASE_IMAGE}" in bootstrap_dockerfile
    assert "TGSRL_PYPI_INDEX_URL" in gpu_dockerfile
    services_dockerfile = (ROOT / "Dockerfile.services").read_text(encoding="utf-8")
    for target in (
        "scheduler-nvidia",
        "job-controller",
        "operator",
        "runtime",
        "gateway",
        "console",
    ):
        assert f" AS {target}" in services_dockerfile


def test_hami_smoke_installer_is_pinned_reversible_and_fail_closed() -> None:
    source = (ROOT / "scripts" / "gpu-prepare-hami.sh").read_text(encoding="utf-8")
    bom = (ROOT / "compatibility" / "bom" / "runtime.yaml").read_text(encoding="utf-8")
    queue = (ROOT / "deploy" / "kubernetes" / "hami-smoke-queue.yaml").read_text(encoding="utf-8")

    assert "HAMI_VERSION=${TGSRL_HAMI_VERSION:-2.10.0}" in source
    assert "github.com/Project-HAMi/HAMi/releases/download/" in source
    assert "e1d8429b2270da1a5c26343d6d6fd2a0099ba7b944cf0f52eb20d6b9f14e5099" in source
    assert "require_context" in source and "require_idle_namespace" in source
    assert "minikube addons disable nvidia-device-plugin" in source
    assert "minikube addons enable nvidia-device-plugin" in source
    assert "rollback_failed_install" in source
    assert "preexisting-hami-annotations.json" in source
    assert "preexisting-gpu-capacity.json" in source
    assert "wait_for_gpu_capacity_restore" in source
    assert "scheduler.kubeScheduler.image.registry" in source
    assert "scheduler.kubeScheduler.image.repository" in source
    assert "deploy/kubernetes/hami-smoke-queue.yaml" in source
    assert 'hami_chart_version: "2.10.0"' in bom
    assert "hami_chart_sha256:" in bom
    for resource in (
        "nvidia.com/gpu",
        "nvidia.com/gpucores",
        "nvidia.com/gpumem-percentage",
    ):
        assert resource in queue


def test_bootstrap_mount_does_not_shadow_gpu_workload() -> None:
    source = (ROOT / "operator-go" / "compiler" / "materialize.go").read_text(encoding="utf-8")

    assert 'WorkerBootstrapMountPath  = "/var/run/tgsrl-bootstrap"' in source
    assert 'WorkerBootstrapMountPath  = "/opt/tgsrl"' not in source


def test_gpu_compose_keeps_console_dependencies_from_the_image() -> None:
    source = (ROOT / "compose.gpu.yaml").read_text(encoding="utf-8")
    console = source.split("  console:", 1)[1]

    assert "volumes: !override []" in console
    assert "/workspace/console:ro" not in console


def test_gpu_smoke_requires_e1_to_pass() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("gpu-smoke:", 1)[1].split("\ngpu-down:", 1)[0]

    assert "--experiment E1" in target
    assert 'select(.experiment_id == "E1")' in target
    assert '.status == "PASSED"' in target


def test_a10_full_readiness_requires_lifecycle_campaign_to_pass() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("gpu-a10-full-readiness:", 1)[1].split("\ngpu-a10-hami-readiness:", 1)[
        0
    ]

    assert "--campaign configs/gates/a10-readiness.json" in target
    assert "A10_READINESS_REPORTS ?= .cache/tgsrl/a10-readiness" in makefile
    assert '--reports-dir "$(A10_READINESS_REPORTS)"' in target
    assert "--experiment A10-FULL" in target
    assert 'select(.experiment_id == "A10-FULL")' in target
    assert '.status == "PASSED"' in target


def test_a10_readiness_aggregates_full_hami_and_dra_restore() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("gpu-a10-readiness:", 1)[1].split("\ngpu-helm-smoke:", 1)[0]
    script = (ROOT / "scripts" / "gpu-a10-readiness.sh").read_text(encoding="utf-8")

    assert "./scripts/gpu-a10-readiness.sh all" in target
    assert 'name != "NVIDIA A10"' in script
    assert "make engineering-fault-readiness" in script
    assert "make gpu-a10-full-readiness" in script
    assert "make gpu-hami-concurrency-smoke" in script
    assert "make gpu-restore-dra" in script
    assert "make gpu-smoke" in script
    assert '"MIG: NVIDIA A10 does not expose a usable MIG topology"' in script


@pytest.mark.parametrize(
    ("name", "returncode"),
    [("NVIDIA A10", 0), ("NVIDIA L4", 1)],
)
def test_a10_readiness_verifies_the_physical_gpu_model(
    tmp_path: Path, name: str, returncode: int
) -> None:
    nvidia_smi = tmp_path / "nvidia-smi"
    nvidia_smi.write_text(
        f"#!/usr/bin/env bash\nprintf '%s\\n' 'GPU-a10, {name}, 580.178.04, 23028'\n",
        encoding="utf-8",
    )
    nvidia_smi.chmod(0o755)
    output = tmp_path / "evidence"
    result = subprocess.run(
        [str(ROOT / "scripts/gpu-a10-readiness.sh"), "verify"],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "TGSRL_A10_READINESS_DIR": str(output),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == returncode
    if returncode == 0:
        evidence = json.loads((output / "host-gpu.json").read_text(encoding="utf-8"))
        assert evidence["name"] == "NVIDIA A10"
        assert evidence["memory_mib"] == 23028
    else:
        assert "requires NVIDIA A10" in result.stderr


def test_gpu_render_config_supports_helm_port_forward_overrides(tmp_path: Path) -> None:
    digest = "sha256:" + "a" * 64
    image_env = tmp_path / "images.env"
    image_env.write_text(
        f"GPU_SMOKE_IMAGE=registry.example.test/gpu-smoke@{digest}\n"
        f"GPU_SMOKE_DIGEST={digest}\n"
        f"WORKER_BOOTSTRAP_IMAGE=registry.example.test/bootstrap@{digest}\n",
        encoding="utf-8",
    )
    output = tmp_path / "environment.json"
    runtime_env = tmp_path / "runtime.env"
    state_dir = tmp_path / "driver-state"

    subprocess.run(
        [str(ROOT / "scripts/gpu-render-config.sh")],
        cwd=ROOT,
        env={
            **os.environ,
            "TGSRL_IMAGE_ENV_FILE": str(image_env),
            "TGSRL_HARDWARE_DRIVER_CONFIG": str(output),
            "TGSRL_GPU_RUNTIME_ENV": str(runtime_env),
            "TGSRL_HOST_GATEWAY": "192.0.2.10",
            "TGSRL_KUBE_CONTEXT": "test-context",
            "TGSRL_HARDWARE_GATEWAY_URL": "http://127.0.0.1:18080",
            "TGSRL_HARDWARE_WORKER_REGISTRY_URL": "http://127.0.0.1:15091",
            "TGSRL_HARDWARE_DRIVER_STATE_DIR": str(state_dir),
        },
        text=True,
        capture_output=True,
        check=True,
    )

    rendered = json.loads(output.read_text(encoding="utf-8"))
    assert rendered["state_directory"] == str(state_dir)
    assert rendered["defaults"]["gateway_url"] == "http://127.0.0.1:18080"
    assert rendered["defaults"]["worker_registry_url"] == "http://127.0.0.1:15091"
    assert rendered["targets"]["A10-FULL"]["job_template"].endswith(
        "configs/hardware/verl-lifecycle-job.example.json"
    )


def test_gpu_helm_smoke_builds_all_services_and_runs_a10_readiness() -> None:
    build = (ROOT / "scripts" / "gpu-build-images.sh").read_text(encoding="utf-8")
    render = (ROOT / "scripts" / "gpu-render-helm-values.sh").read_text(encoding="utf-8")
    smoke = (ROOT / "scripts" / "gpu-helm-smoke.sh").read_text(encoding="utf-8")

    for target in (
        "scheduler-nvidia",
        "job-controller",
        "operator",
        "runtime",
        "gateway",
        "console",
    ):
        assert f"build_target {target}" in build
    assert "WORKER_BOOTSTRAP_IMAGE" in render
    assert "TGSRL_KUBE_CONTEXT" in render and "TGSRL_KUBE_CONTEXT" in smoke
    assert "scripts/deploy-full-stack.sh install" in smoke
    assert "scripts/deploy-full-stack.sh upgrade" in smoke
    assert "rollout status deployment --all" not in smoke
    assert "deployment/tgsrl-scheduler" in smoke
    assert "deployment/tgsrl-console" in smoke
    assert 'rollout status "$deployment"' in smoke
    assert "make gpu-a10-full-readiness" in smoke
    assert 'A10_READINESS_REPORTS="$OUTPUT/a10-readiness"' in smoke
    assert "TGSRL_HARDWARE_WORKER_REGISTRY_URL" in smoke
    assert "TGSRL_HARDWARE_DRIVER_STATE_DIR" in smoke
    assert "custom-columns=" in smoke
    assert "deployment,service,pvc,networkpolicy" in smoke
    assert "deployment,service,pod,pvc,networkpolicy" not in smoke


def test_gpu_helm_values_renderer_produces_a_valid_immutable_chart(tmp_path: Path) -> None:
    kubectl = tmp_path / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env bash
set -eu
case "$*" in
  "config current-context")
    printf '%s\\n' tgsrl-gpu
    ;;
  "get nodes -o json")
    printf '%s\\n' '{"items":[{"metadata":{"name":"gpu-node-a"}}]}'
    ;;
  "get runtimeclass.node.k8s.io nvidia")
    ;;
  "create namespace tgsrl-system --dry-run=client -o yaml")
    printf '%s\\n' 'apiVersion: v1' 'kind: Namespace' 'metadata: {name: tgsrl-system}'
    ;;
  "apply -f -")
    cat >/dev/null
    ;;
  "-n tgsrl-system create secret generic tgsrl-worker-registry --from-file=signing-key="*)
    printf '%s\\n' 'apiVersion: v1' 'kind: Secret' 'metadata: {name: tgsrl-worker-registry}'
    ;;
  "-n tgsrl-system get secret tgsrl-registry")
    ;;
  *)
    printf 'unexpected kubectl args: %s\\n' "$*" >&2
    exit 3
    ;;
esac
""",
        encoding="utf-8",
    )
    kubectl.chmod(0o755)
    digest = "sha256:" + "a" * 64
    image_env = tmp_path / "images.env"

    def image_line(name: str) -> str:
        repository = name.lower().replace("_image", "").replace("_", "-")
        return f"{name}=registry.example.test/tgsrl/{repository}@{digest}\n"

    image_env.write_text(
        "".join(
            image_line(name)
            for name in (
                "SCHEDULER_NVIDIA_IMAGE",
                "JOB_CONTROLLER_IMAGE",
                "OPERATOR_IMAGE",
                "RUNTIME_IMAGE",
                "GATEWAY_IMAGE",
                "CONSOLE_IMAGE",
                "WORKER_BOOTSTRAP_IMAGE",
            )
        ),
        encoding="utf-8",
    )
    key = tmp_path / "worker-registry.key"
    key.write_text("a" * 64 + "\n", encoding="utf-8")
    values = tmp_path / "values.yaml"

    subprocess.run(
        [str(ROOT / "scripts/gpu-render-helm-values.sh")],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "TGSRL_IMAGE_ENV_FILE": str(image_env),
            "TGSRL_GPU_HELM_VALUES": str(values),
            "TGSRL_GPU_REGISTRY_KEY_FILE": str(key),
        },
        text=True,
        capture_output=True,
        check=True,
    )
    rendered = subprocess.run(
        [str(ROOT / "scripts/deploy-full-stack.sh"), "render", str(values)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert rendered.returncode == 0, rendered.stderr
    for repository in (
        "scheduler-nvidia",
        "job-controller",
        "operator",
        "runtime",
        "gateway",
        "console",
    ):
        assert f"registry.example.test/tgsrl/{repository}@sha256:" in rendered.stdout
    assert "name: tgsrl-registry" in rendered.stdout


def test_hami_smoke_requires_h1_to_pass() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("gpu-hami-smoke:", 1)[1].split("\ngpu-restore-dra:", 1)[0]

    assert "--campaign configs/gates/hami-smoke.json" in target
    assert "--experiment H1" in target
    assert 'select(.experiment_id == "H1")' in target
    assert '.status == "PASSED"' in target


def test_hami_control_plane_selects_profile_and_two_worker_manifest_together() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("gpu-hami-up:", 1)[1].split("\ngpu-hami-status:", 1)[0]

    assert "TGSRL_OPERATOR_GPU_PROFILES=hami-vgpu" in target
    assert "TGSRL_GPU_MANIFEST=compatibility/manifests/hami-concurrency-verl.yaml" in target
    assert "./scripts/gpu-stack.sh up" in target


def test_hami_concurrency_smoke_requires_h2_to_pass() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("gpu-hami-concurrency-smoke:", 1)[1].split("\ngpu-restore-dra:", 1)[0]

    assert "--campaign configs/gates/hami-concurrency-smoke.json" in target
    assert "--experiment H2" in target
    assert 'select(.experiment_id == "H2")' in target
    assert '.status == "PASSED"' in target
