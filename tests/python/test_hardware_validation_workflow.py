from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "hardware-validation.yaml"


def test_workflow_runs_locked_workloads_without_cross_claiming_evidence() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "hardware-run --evidence" in text
    assert "GPU_SINGLE_NODE" in text
    assert "gpu-multi-node" not in text
    assert "Manual operator instructions" not in text
    assert "python3 scripts/gate-tools.py simulate" in text
    assert '--evidence "${{ github.event.inputs.evidence }}"' not in text


def test_workflow_has_cpu_and_automated_self_hosted_gpu_entries() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "cpu-integration:" in text
    assert "scripts/gate-tools.py cpu-smoke" in text
    assert "gpu-runner:" in text
    assert "runs-on: [self-hosted, gpu]" in text
    assert "hardware-run --evidence" in text
