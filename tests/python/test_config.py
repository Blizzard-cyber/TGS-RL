from __future__ import annotations

import importlib.util
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _REPO_ROOT / "runtime-python" / "tgsrl_runtime" / "config.py"
_SPEC = importlib.util.spec_from_file_location("tgsrl_runtime_config_test", _CONFIG_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"unable to load config module from {_CONFIG_PATH}")
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

ConfigError = _MODULE.ConfigError
DEFAULT_REPO_ROOT = _MODULE.DEFAULT_REPO_ROOT
_load_yaml_subset = _MODULE._load_yaml_subset
load_bom = _MODULE.load_bom
load_bundle = _MODULE.load_bundle
load_bundle_with_options = _MODULE.load_bundle_with_options
LoadOptions = _MODULE.LoadOptions
load_policy = _MODULE.load_policy
load_profile = _MODULE.load_profile
runtime_projection = _MODULE.runtime_projection


class ConfigLoaderTests(unittest.TestCase):
    def test_load_bundle_reads_real_config_graph(self) -> None:
        bundle = load_bundle()

        self.assertEqual(bundle.root, DEFAULT_REPO_ROOT)
        self.assertEqual(bundle.manifest.manifest_id, "cpu-mock")
        self.assertEqual(bundle.bom.bom_id, "runtime")
        self.assertEqual(bundle.profile.profile_id, "cpu-mock-v1")
        self.assertEqual(bundle.capabilities.capability_id, "cpu-mock-v1")
        self.assertEqual(bundle.policy.policy_id, "static")
        self.assertEqual(bundle.scenario.scenario_id, "tool-wait-v1")
        self.assertEqual(bundle.scenario.workload.algorithm, "grpo")
        self.assertEqual(bundle.scenario.workload.rollout_mode, "partially_async")
        self.assertTrue(bundle.policy.constraints.require_capacity)
        self.assertTrue(bundle.policy.constraints.require_capability)
        self.assertTrue(bundle.policy.constraints.require_safe_point)
        self.assertEqual(bundle.policy.protection.cooldown, "500ms")
        self.assertAlmostEqual(bundle.policy.protection.hysteresis, 0.05)
        self.assertEqual(bundle.policy.protection.max_actions_per_window, 8)
        self.assertFalse(bundle.policy.preemption.enabled)
        self.assertEqual(bundle.policy.preemption.strategy, "noop")
        self.assertTrue(bundle.policy.preemption.require_safe_point)
        self.assertTrue(bundle.policy.determinism.explicit_seed_required)
        self.assertTrue(bundle.policy.determinism.virtual_clock_required_for_replay)
        self.assertTrue(bundle.policy.determinism.deterministic_proto_serialization)
        self.assertFalse(bundle.policy.determinism.wall_clock_in_canonical_output)
        self.assertFalse(bundle.policy.safety.reward_value_allowed)
        self.assertFalse(bundle.policy.safety.stale_intent_allowed)
        self.assertFalse(bundle.policy.safety.stale_snapshot_commit_allowed)
        self.assertFalse(bundle.policy.safety.partial_failure_is_success)
        self.assertEqual(bundle.profile.gpu_management_profile.maximum_active_profiles, 1)
        self.assertEqual(bundle.capabilities.source, "mock")
        self.assertEqual(bundle.capabilities.source, bundle.scenario.topology.provider_source)
        self.assertEqual(bundle.overrides.env, {})

    def test_yaml_subset_parser_handles_block_scalars_and_nested_lists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "sample.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        'title: "sample"',
                        "description: >-",
                        "  hello",
                        "  world",
                        "items:",
                        "  - name: one",
                        "    values:",
                        "      - alpha",
                        "      - beta",
                        "empty_map: {}",
                        "empty_list: []",
                    ]
                ),
                encoding="utf-8",
            )

            loaded = _load_yaml_subset(config_path)

        self.assertEqual(loaded["title"], "sample")
        self.assertEqual(loaded["description"], "hello world")
        self.assertEqual(loaded["items"][0]["name"], "one")
        self.assertEqual(loaded["items"][0]["values"], ["alpha", "beta"])
        self.assertEqual(loaded["empty_map"], {})
        self.assertEqual(loaded["empty_list"], [])

    def test_load_bom_rejects_floating_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_bom = Path(temp_dir) / "runtime.yaml"
            bad_bom.write_text(
                "\n".join(
                    [
                        'schema_version: "tgsrl.io/bom/v1alpha1"',
                        'bom_id: "runtime"',
                        "revision: 1",
                        'status: "current"',
                        'scope: "test"',
                        "pinning_policy:",
                        "  floating_versions_allowed: false",
                        "  immutable_image_digest_required_for_release: true",
                        "  unknown_commit_or_digest_value: null",
                        '  note: "test"',
                        "toolchain:",
                        '  - name: "go-toolchain"',
                        '    version: "latest"',
                        '    source: "https://go.dev"',
                        "    source_commit: null",
                        "    artifact_digest: null",
                        "runtime_dependencies:",
                        '  - name: "grpc"',
                        '    version: "1.0.0"',
                        '    source: "https://example.com"',
                        "    source_commit: null",
                        "    artifact_digest: null",
                        "development_images:",
                        '  - name: "python"',
                        '    image: "docker.io/library/python:3.12.14-slim-bookworm"',
                        '    image_digest: "sha256:'
                        '0f5b26b9518d002b6173fd61daad821fa340635ebfec5bba471013f9ca114579"',
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ConfigError, "floating markers"):
                load_bom(bad_bom)

    def test_load_profile_rejects_multiple_gpu_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_profile = Path(temp_dir) / "profile.yaml"
            bad_profile.write_text(
                "\n".join(
                    [
                        'schema_version: "tgsrl.io/compatibility-profile/v1alpha1"',
                        'profile_id: "cpu-mock-v1"',
                        "revision: 1",
                        'status: "supported-with-limitations"',
                        'description: "test"',
                        "provider:",
                        '  kind: "MockResourceProvider"',
                        '  source: "mock"',
                        "  live_hardware: false",
                        '  authoritative_for: "simulated-resource-state-only"',
                        "runtime_scope:",
                        "  required_host_resources:",
                        '    - "cpu"',
                        "  data_kinds:",
                        '    - "synthetic"',
                        "  algorithms:",
                        '    - "grpo"',
                        "  rollout_modes:",
                        '    - "partially_async"',
                        "  lifecycle_actions:",
                        '    - "bind"',
                        "gpu_management_profile:",
                        '  selected: "none"',
                        "  allowed_values:",
                        '    - "none"',
                        '    - "nvidia-device-plugin"',
                        "  mutually_exclusive: true",
                        "  maximum_active_profiles: 1",
                        "  admission_rejects_multiple_profiles: true",
                        "  enabled_profiles:",
                        '    - "nvidia-device-plugin"',
                        '    - "none"',
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ConfigError, "at most one active profile"):
                load_profile(bad_profile)

    def test_load_policy_rejects_unknown_fallback_rule(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_policy = Path(temp_dir) / "policy.yaml"
            bad_policy.write_text(
                (_REPO_ROOT / "configs" / "policies" / "static.yaml")
                .read_text(encoding="utf-8")
                .replace('    - "last-valid-intent"', '    - "latest"', 1),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ConfigError, "unsupported fallback values"):
                load_policy(bad_policy)

    def test_load_policy_rejects_unknown_top_level_field(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_policy = Path(temp_dir) / "policy.yaml"
            bad_policy.write_text(
                (_REPO_ROOT / "configs" / "policies" / "static.yaml").read_text(encoding="utf-8")
                + "\nfuture_toggle: true\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ConfigError, "unknown field"):
                load_policy(bad_policy)

    def test_load_policy_rejects_unknown_nested_field(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_policy = Path(temp_dir) / "policy.yaml"
            bad_policy.write_text(
                (_REPO_ROOT / "configs" / "policies" / "static.yaml")
                .read_text(encoding="utf-8")
                .replace("  top_k: 1", "  top_k: 1\n  unknown_mode: true"),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ConfigError, "unknown field"):
                load_policy(bad_policy)

    def test_env_override_can_swap_manifest_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            shutil.copytree(_REPO_ROOT / "compatibility", repo / "compatibility")
            shutil.copytree(_REPO_ROOT / "configs", repo / "configs")
            shutil.copytree(_REPO_ROOT / "upstream", repo / "upstream")

            alt_manifest = repo / "compatibility" / "manifests" / "override.yaml"
            alt_manifest.write_text(
                (_REPO_ROOT / "compatibility" / "manifests" / "cpu-mock.yaml").read_text(
                    encoding="utf-8"
                ),
                encoding="utf-8",
            )
            scenario_path = repo / "configs" / "scenarios" / "tool-wait.yaml"
            scenario_path.write_text(
                scenario_path.read_text(encoding="utf-8").replace(
                    "compatibility/manifests/cpu-mock.yaml",
                    "compatibility/manifests/override.yaml",
                ),
                encoding="utf-8",
            )

            bundle = load_bundle_with_options(
                LoadOptions(
                    root=repo,
                    env={
                        "TGSRL_CONFIG_MANIFEST_PATH": "compatibility/manifests/override.yaml",
                        "TGSRL_CONFIG_API_TOKEN": "super-secret-token",
                    },
                )
            )

        self.assertEqual(bundle.manifest.path.name, "override.yaml")
        self.assertEqual(bundle.overrides.manifest_path, "compatibility/manifests/override.yaml")
        self.assertEqual(
            bundle.overrides.env["TGSRL_CONFIG_MANIFEST_PATH"],
            "compatibility/manifests/override.yaml",
        )
        self.assertEqual(bundle.overrides.env["TGSRL_CONFIG_API_TOKEN"], "<redacted>")

    def test_missing_override_reports_secret_safe_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            shutil.copytree(_REPO_ROOT / "compatibility", repo / "compatibility")
            shutil.copytree(_REPO_ROOT / "configs", repo / "configs")
            shutil.copytree(_REPO_ROOT / "upstream", repo / "upstream")
            with self.assertRaises(ConfigError) as raised:
                load_bundle_with_options(
                    LoadOptions(
                        root=repo,
                        env={
                            "TGSRL_CONFIG_SCENARIO_PATH": "configs/scenarios/missing.yaml",
                            "TGSRL_CONFIG_SECRET_KEY": "abc123",
                        },
                    )
                )

        diagnostic = raised.exception.diagnostic()
        self.assertEqual(diagnostic["code"], "missing_reference")
        self.assertIn("missing.yaml", diagnostic["message"])
        self.assertNotIn("abc123", str(diagnostic))

    def test_runtime_projection_derives_runtime_contract(self) -> None:
        bundle = load_bundle()

        projection = runtime_projection(bundle)

        self.assertEqual(projection.framework, "mock")
        self.assertEqual(projection.execution_backend, "inprocessmock")
        self.assertEqual(projection.trainer, "fake")
        self.assertEqual(projection.rollout_engine, "fake")
        self.assertEqual(projection.compatibility_profile, "cpu-mock-v1")
        self.assertEqual(projection.provider_kind, "MockResourceProvider")
        self.assertEqual(projection.provider_source, "mock")
        self.assertEqual(projection.selection_strategy, "stablefirstfit")
        self.assertEqual(projection.policy_version, "1")
        self.assertEqual(projection.data_kind, "synthetic")
        self.assertEqual(projection.algorithm, "grpo")
        self.assertEqual(projection.rollout_mode, "partially_async")
        self.assertEqual(projection.desired_units, 2)

    def test_load_bundle_accepts_top_k_greater_than_one(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            shutil.copytree(_REPO_ROOT / "compatibility", repo / "compatibility")
            shutil.copytree(_REPO_ROOT / "configs", repo / "configs")
            shutil.copytree(_REPO_ROOT / "upstream", repo / "upstream")
            policy_path = repo / "configs" / "policies" / "static.yaml"
            policy_path.write_text(
                policy_path.read_text(encoding="utf-8").replace("  top_k: 1", "  top_k: 3"),
                encoding="utf-8",
            )

            bundle = load_bundle(repo)

        self.assertEqual(bundle.policy.selection.top_k, 3)

    def test_runtime_projection_accepts_real_component_provider_combination(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir)
            shutil.copytree(_REPO_ROOT / "compatibility", repo / "compatibility")
            shutil.copytree(_REPO_ROOT / "configs", repo / "configs")
            shutil.copytree(_REPO_ROOT / "upstream", repo / "upstream")

            manifest_path = repo / "compatibility" / "manifests" / "cpu-mock.yaml"
            manifest_path.write_text(
                manifest_path.read_text(encoding="utf-8")
                .replace('name: "mock"', 'name: "verl"', 1)
                .replace('name: "in-process-mock"', 'name: "ray"', 1)
                .replace('name: "fake"', 'name: "pytorch"', 1)
                .replace('name: "fake"', 'name: "vllm"', 1)
                .replace('name: "MockResourceProvider"', 'name: "NvidiaProvider"')
                .replace('source: "mock"', 'source: "nvidia"', 1),
                encoding="utf-8",
            )

            profile_path = repo / "compatibility" / "profiles" / "cpu-mock-v1.yaml"
            profile_path.write_text(
                profile_path.read_text(encoding="utf-8")
                .replace('kind: "MockResourceProvider"', 'kind: "NvidiaProvider"')
                .replace('source: "mock"', 'source: "nvidia"')
                .replace("live_hardware: false", "live_hardware: true"),
                encoding="utf-8",
            )

            capabilities_path = repo / "configs" / "capabilities" / "mock.yaml"
            capabilities_path.write_text(
                capabilities_path.read_text(encoding="utf-8").replace(
                    'source: "mock"', 'source: "nvidia"'
                ),
                encoding="utf-8",
            )

            scenario_path = repo / "configs" / "scenarios" / "tool-wait.yaml"
            scenario_path.write_text(
                scenario_path.read_text(encoding="utf-8").replace(
                    'provider_source: "mock"', 'provider_source: "nvidia"'
                ),
                encoding="utf-8",
            )

            bundle = load_bundle(repo)
            projection = runtime_projection(bundle)

        self.assertEqual(projection.framework, "verl")
        self.assertEqual(projection.execution_backend, "ray")
        self.assertEqual(projection.trainer, "pytorch")
        self.assertEqual(projection.rollout_engine, "vllm")
        self.assertEqual(projection.provider_kind, "NvidiaProvider")
        self.assertEqual(projection.provider_source, "nvidia")


if __name__ == "__main__":
    unittest.main()
