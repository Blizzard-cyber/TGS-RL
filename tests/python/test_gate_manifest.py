import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "configs" / "gates" / "gate-gi.json"
SCENARIO = ROOT / "configs" / "scenarios" / "gate-gi.yaml"


def test_gate_manifest_references_expected_scenario_and_enums() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tgsrl.io/gate-suite/v1alpha1"
    assert manifest["scenario_manifest"] == "configs/scenarios/gate-gi.yaml"
    assert SCENARIO.exists()
    assert manifest["status_values"] == [
        "NOT_RUN",
        "BLOCKED",
        "INVALID",
        "PASSED",
        "FAILED",
    ]
    assert manifest["evidence_values"] == [
        "SIMULATED",
        "CPU_INTEGRATION",
        "GPU_SINGLE_NODE",
        "GPU_MULTI_NODE",
    ]
    assert manifest["comparisons"]["warmup_runs"] == 1
    assert manifest["comparisons"]["measurement_runs"] == 3


def test_gate_manifest_separates_configuration_from_runtime_artifacts() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert manifest["comparisons"]["baseline"] == "baseline"
    assert manifest["comparisons"]["variant"] == "variant"
    assert len(manifest["rules"]) >= 2
    assert set(manifest["artifacts"]) == {
        "archive",
        "baseline_trace",
        "variant_trace",
        "report",
    }
    assert all("experiments/" not in value for value in manifest["artifacts"].values())
