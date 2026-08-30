from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "hardware-validation.yaml"


def test_workflow_simulator_does_not_offer_gpu_evidence_selection() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "GPU_SINGLE_NODE" not in text.split("jobs:")[0]
    assert "GPU_MULTI_NODE" not in text.split("jobs:")[0]
    assert "python3 scripts/gate-tools.py simulate" in text
    assert '--evidence "${{ github.event.inputs.evidence }}"' not in text


def test_workflow_has_cpu_entry_and_self_hosted_gpu_manual_entry() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "cpu-integration:" in text
    assert "python3 scripts/gate-tools.py cpu-smoke" in text
    assert "gpu-manual:" in text
    assert "runs-on: [self-hosted, gpu]" in text
    assert "python3 scripts/gate-tools.py ingest --report" in text
