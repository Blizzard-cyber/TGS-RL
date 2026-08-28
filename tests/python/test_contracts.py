"""Contract composition tests across the complete algorithm/mode matrix."""

import itertools
import re
from collections.abc import Callable

import pytest
from google.protobuf import duration_pb2
from tgsrl.v1 import execution_pb2

from adapters import (
    FullAsyncRolloutAdapter,
    GRPOAdapter,
    PartialAsyncRolloutAdapter,
    PPOAdapter,
    SyncRolloutAdapter,
    build_execution_contract,
    validate_execution_contract,
)
from adapters.algorithms import AlgorithmAdapter
from adapters.contracts import (
    ContractValidationError,
    canonical_contract_id,
    normalize_version,
    versions_compatible,
)
from adapters.rollout_modes import RolloutModeAdapter


@pytest.mark.parametrize(
    ("algorithm", "mode"),
    itertools.product(
        (PPOAdapter(), GRPOAdapter()),
        (SyncRolloutAdapter(), PartialAsyncRolloutAdapter(), FullAsyncRolloutAdapter()),
    ),
)
def test_all_six_combinations_are_valid_generated_contracts(
    algorithm: AlgorithmAdapter, mode: RolloutModeAdapter
) -> None:
    contract = build_execution_contract(algorithm, mode)
    validate_execution_contract(contract)
    assert isinstance(contract, execution_pb2.ExecutionContract)
    assert [phase.phase_id for phase in contract.phase_graph.phases[:2]] == ["prefill", "decode"]
    assert contract.capabilities.deterministic_replay
    assert not hasattr(contract, "algorithm")


def test_contracts_are_byte_stable() -> None:
    first = build_execution_contract(GRPOAdapter(), PartialAsyncRolloutAdapter())
    second = build_execution_contract(GRPOAdapter(), PartialAsyncRolloutAdapter())
    assert first.SerializeToString(deterministic=True) == second.SerializeToString(
        deterministic=True
    )
    assert re.fullmatch(r"contract-sha256-[0-9a-f]{64}", first.contract_id)


def test_contract_id_is_derived_from_semantic_content() -> None:
    class ModifiedPPO(PPOAdapter):
        name = "ppo"

        def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
            rules = list(super().validity_rules())
            rules[0].expression = "sample.policy_lag <= 999"
            return tuple(rules)

    baseline = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    changed = build_execution_contract(ModifiedPPO(), SyncRolloutAdapter())
    assert baseline.contract_id != changed.contract_id


def _mutate_entry_with_predecessor(contract: execution_pb2.ExecutionContract) -> None:
    contract.phase_graph.entry_phase_ids[:] = ["decode"]


def _mutate_unknown_phase_kind(contract: execution_pb2.ExecutionContract) -> None:
    contract.phase_graph.phases[0].kind = execution_pb2.PHASE_KIND_UNKNOWN


def _mutate_zero_parallelism(contract: execution_pb2.ExecutionContract) -> None:
    contract.phase_graph.phases[0].parallelism = 0


def _mutate_unknown_failure_mode(contract: execution_pb2.ExecutionContract) -> None:
    contract.validity_rules[0].failure_mode = execution_pb2.VALIDITY_FAILURE_MODE_UNKNOWN


def _mutate_missing_safe_point_phase(contract: execution_pb2.ExecutionContract) -> None:
    contract.safe_point_policy.required_phase_ids[:] = ["missing"]


def _mutate_zero_commit_timeout(contract: execution_pb2.ExecutionContract) -> None:
    contract.commit_policy.commit_timeout.Clear()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (_mutate_entry_with_predecessor, "entry"),
        (_mutate_unknown_phase_kind, "kind"),
        (_mutate_zero_parallelism, "parallelism"),
        (_mutate_unknown_failure_mode, "failure.mode"),
        (_mutate_missing_safe_point_phase, "safe.point"),
        (_mutate_zero_commit_timeout, "commit.timeout"),
    ],
)
def test_contract_validation_rejects_structurally_unusable_contracts(
    mutation: Callable[[execution_pb2.ExecutionContract], None], message: str
) -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    mutation(contract)
    with pytest.raises(ContractValidationError, match=message):
        validate_execution_contract(contract)


def test_invalid_cycle_is_rejected() -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    contract.phase_graph.edges.add(from_phase_id="weight-sync", to_phase_id="prefill")
    with pytest.raises(ContractValidationError, match=r"acyclic|entry phases"):
        validate_execution_contract(contract)


def _refresh_contract_id(contract: execution_pb2.ExecutionContract) -> None:
    contract.contract_id = canonical_contract_id(contract)


@pytest.mark.parametrize(
    ("value", "normalized"),
    [
        ("0.3", "0.3.0"),
        ("v0.3", "0.3.0"),
        ("V0.3.0", "0.3.0"),
        ("0.3.1-rc.2", "0.3.1-rc.2"),
        ("001.002", "1.2.0"),
        ("4294967295.0.0", "4294967295.0.0"),
    ],
)
def test_version_normalization_matches_go(value: str, normalized: str) -> None:
    assert normalize_version(value) == normalized


@pytest.mark.parametrize(
    "value",
    [
        "",
        " 0.3",
        "0.3 ",
        "1",
        "1.x",
        "1.2.3.4",
        "1.2-",
        "4294967296.0",
    ],
)
def test_version_normalization_rejects_non_go_syntax(value: str) -> None:
    with pytest.raises(ContractValidationError, match="version"):
        normalize_version(value)


@pytest.mark.parametrize(
    ("current", "required", "compatible"),
    [
        ("0.3.0", "0.3.99", True),
        ("0.3.0", "v0.4", False),
        ("1.2.3", "1.99.0", True),
        ("1.2.3", "2.0.0", False),
    ],
)
def test_compatible_operator_version_window(current: str, required: str, compatible: bool) -> None:
    assert versions_compatible(current, required) is compatible


@pytest.mark.parametrize(
    ("operator", "version"),
    [
        (execution_pb2.VERSION_OPERATOR_EXACT, "v0.3"),
        (execution_pb2.VERSION_OPERATOR_SEMVER, "V0.3.0"),
        (execution_pb2.VERSION_OPERATOR_COMPATIBLE, "0.3.999-prerelease"),
    ],
)
def test_protocol_constraint_accepts_scheduler_compatible_versions(
    operator: int, version: str
) -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    constraint = contract.version_constraints[0]
    constraint.component = "PrOtOcOl"
    constraint.operator = operator
    constraint.version = version
    _refresh_contract_id(contract)
    validate_execution_contract(contract)


@pytest.mark.parametrize(
    ("operator", "version"),
    [
        (execution_pb2.VERSION_OPERATOR_EXACT, "0.3.1"),
        (execution_pb2.VERSION_OPERATOR_SEMVER, "0.3.1"),
        (execution_pb2.VERSION_OPERATOR_COMPATIBLE, "0.4.0"),
        (execution_pb2.VERSION_OPERATOR_COMPATIBLE, "1.3.0"),
    ],
)
def test_protocol_constraint_rejects_scheduler_incompatible_versions(
    operator: int, version: str
) -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    constraint = contract.version_constraints[0]
    constraint.operator = operator
    constraint.version = version
    _refresh_contract_id(contract)
    with pytest.raises(ContractValidationError, match="incompatible"):
        validate_execution_contract(contract)


def test_protocol_constraint_is_required() -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    contract.version_constraints[0].component = "runtime"
    _refresh_contract_id(contract)
    with pytest.raises(ContractValidationError, match=r"protocol.*required"):
        validate_execution_contract(contract)


def test_version_constraint_components_deduplicate_case_insensitively() -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    duplicate = contract.version_constraints.add()
    duplicate.CopyFrom(contract.version_constraints[0])
    duplicate.component = "PROTOCOL"
    _refresh_contract_id(contract)
    with pytest.raises(ContractValidationError, match="unique ignoring case"):
        validate_execution_contract(contract)


def _prefix_contract_id(contract: execution_pb2.ExecutionContract) -> None:
    contract.contract_id = f" {contract.contract_id}"


def _prefix_phase_id(contract: execution_pb2.ExecutionContract) -> None:
    contract.phase_graph.phases[0].phase_id = " prefill"


def _suffix_rule_id(contract: execution_pb2.ExecutionContract) -> None:
    contract.validity_rules[0].rule_id = "rule "


def _prefix_component(contract: execution_pb2.ExecutionContract) -> None:
    contract.version_constraints[0].component = " protocol"


@pytest.mark.parametrize(
    "mutation",
    [_prefix_contract_id, _prefix_phase_id, _suffix_rule_id, _prefix_component],
)
def test_identifier_fields_reject_surrounding_whitespace(
    mutation: Callable[[execution_pb2.ExecutionContract], None],
) -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    mutation(contract)
    with pytest.raises(ContractValidationError):
        validate_execution_contract(contract)


def test_present_non_interval_interval_validates_duration_shape_first() -> None:
    contract = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    contract.safe_point_policy.interval.CopyFrom(duration_pb2.Duration(seconds=1, nanos=-1))
    _refresh_contract_id(contract)
    with pytest.raises(ContractValidationError, match="valid protobuf duration"):
        validate_execution_contract(contract)


def test_canonical_hash_preserves_original_wire_version_spelling() -> None:
    baseline = build_execution_contract(PPOAdapter(), SyncRolloutAdapter())
    alternate = execution_pb2.ExecutionContract()
    alternate.CopyFrom(baseline)
    alternate.version_constraints[0].version = "v0.3.0"
    _refresh_contract_id(alternate)

    validate_execution_contract(alternate)
    assert alternate.contract_id != baseline.contract_id
