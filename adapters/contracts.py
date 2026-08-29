"""Deterministic composition and validation of execution contracts."""

import hashlib
import math
import re
from itertools import pairwise

from google.protobuf import duration_pb2, timestamp_pb2
from tgsrl.v1 import execution_pb2, semantic_pb2

from adapters.algorithms import AlgorithmAdapter
from adapters.rollout_modes import RolloutModeAdapter

_CONTRACT_VERSION = re.compile(r"^1\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
_VERSION_IDENTIFIER = re.compile(r"^[0-9A-Za-z-]+$")
_MAX_DURATION_SECONDS = 315_576_000_000
_MAX_UINT32 = (1 << 32) - 1
_CONTRACT_ID_PREFIX = "contract-sha256-"
SCHEDULER_PROTOCOL_VERSION = "0.3.0"
_EXPRESSION_LANGUAGE = "tgsrl.condition/v1"
_FACT_PATH_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
_ALLOWED_FACT_PATHS: dict[str, frozenset[str]] = {
    "bool": frozenset({"sample.stale", "runtime.safe_point"}),
    "uint64": frozenset(
        {
            "sample.policy_lag",
            "buffer.level.current",
            "sample.sample_count",
            "batch.accepted_samples",
            "group.accepted_samples",
            "group.expected_samples",
        }
    ),
    "double": frozenset(
        {
            "batch.effective_sample_size",
            "batch.effective_sample_size_ratio",
        }
    ),
}
_ALL_ALLOWED_FACT_PATHS = frozenset().union(*_ALLOWED_FACT_PATHS.values())
_COMPONENT_KIND_ALIASES = {
    "protocol": execution_pb2.COMPONENT_KIND_PROTOCOL,
    "scheduler": execution_pb2.COMPONENT_KIND_SCHEDULER,
    "runtime": execution_pb2.COMPONENT_KIND_RUNTIME,
    "operator": execution_pb2.COMPONENT_KIND_OPERATOR,
    "provider": execution_pb2.COMPONENT_KIND_PROVIDER,
    "framework_adapter": execution_pb2.COMPONENT_KIND_FRAMEWORK_ADAPTER,
    "rollout_engine": execution_pb2.COMPONENT_KIND_ROLLOUT_ENGINE,
    "trainer": execution_pb2.COMPONENT_KIND_TRAINER,
    "cuda_driver": execution_pb2.COMPONENT_KIND_CUDA_DRIVER,
    "execution_backend": execution_pb2.COMPONENT_KIND_EXECUTION_BACKEND,
}
_LEAF_OPERATORS = frozenset(
    {
        execution_pb2.CONDITION_OPERATOR_EQ,
        execution_pb2.CONDITION_OPERATOR_NE,
        execution_pb2.CONDITION_OPERATOR_GT,
        execution_pb2.CONDITION_OPERATOR_GE,
        execution_pb2.CONDITION_OPERATOR_LT,
        execution_pb2.CONDITION_OPERATOR_LE,
        execution_pb2.CONDITION_OPERATOR_IN,
        execution_pb2.CONDITION_OPERATOR_NOT_IN,
        execution_pb2.CONDITION_OPERATOR_EXISTS,
    }
)
_COMPOSITE_OPERATORS = frozenset(
    {
        execution_pb2.CONDITION_OPERATOR_AND,
        execution_pb2.CONDITION_OPERATOR_OR,
        execution_pb2.CONDITION_OPERATOR_NOT,
    }
)
_EQUALITY_OPERATORS = frozenset(
    {
        execution_pb2.CONDITION_OPERATOR_EQ,
        execution_pb2.CONDITION_OPERATOR_NE,
    }
)
_ORDERING_OPERATORS = frozenset(
    {
        execution_pb2.CONDITION_OPERATOR_GT,
        execution_pb2.CONDITION_OPERATOR_GE,
        execution_pb2.CONDITION_OPERATOR_LT,
        execution_pb2.CONDITION_OPERATOR_LE,
    }
)
_SET_OPERATORS = frozenset(
    {
        execution_pb2.CONDITION_OPERATOR_IN,
        execution_pb2.CONDITION_OPERATOR_NOT_IN,
    }
)


class ContractValidationError(ValueError):
    """Raised when an adapter emits a malformed execution contract."""


def _edges(phase_ids: list[str]) -> list[execution_pb2.PhaseEdge]:
    return [
        execution_pb2.PhaseEdge(from_phase_id=source, to_phase_id=target)
        for source, target in pairwise(phase_ids)
    ]


def canonical_contract_id(contract: execution_pb2.ExecutionContract) -> str:
    """Derive an ID from every wire-visible contract field except the ID itself."""
    canonical = execution_pb2.ExecutionContract()
    canonical.CopyFrom(contract)
    canonical.ClearField("contract_id")
    digest = hashlib.sha256(canonical.SerializeToString(deterministic=True)).hexdigest()
    return f"{_CONTRACT_ID_PREFIX}{digest}"


def build_execution_contract(
    algorithm: AlgorithmAdapter,
    rollout_mode: RolloutModeAdapter,
) -> execution_pb2.ExecutionContract:
    """Compose orthogonal adapters into one scheduler-generic Proto contract."""
    phases = [*rollout_mode.phases(), *algorithm.phases()]
    phase_ids = [phase.phase_id for phase in phases]
    contract = execution_pb2.ExecutionContract(
        version="1.0.0",
        phase_graph=execution_pb2.PhaseGraph(
            phases=phases,
            edges=_edges(phase_ids),
            entry_phase_ids=[phase_ids[0]],
        ),
        validity_rules=[
            *algorithm.validity_rules(),
            *rollout_mode.validity_rules(),
        ],
        version_constraints=algorithm.version_constraints(),
        commit_policy=rollout_mode.commit_policy(),
        backpressure_policy=rollout_mode.backpressure_policy(),
        safe_point_policy=rollout_mode.safe_point_policy(),
        capabilities=rollout_mode.capabilities(),
    )
    contract.contract_id = canonical_contract_id(contract)
    validate_execution_contract(contract)
    return contract


def _duration_nanoseconds(duration: duration_pb2.Duration, field: str) -> int:
    seconds = duration.seconds
    nanos = duration.nanos
    valid_shape = (
        -_MAX_DURATION_SECONDS <= seconds <= _MAX_DURATION_SECONDS
        and -999_999_999 <= nanos <= 999_999_999
        and not (seconds > 0 and nanos < 0)
        and not (seconds < 0 and nanos > 0)
    )
    if not valid_shape:
        raise ContractValidationError(f"{field} is not a valid protobuf duration")
    return duration.ToNanoseconds()


def _validate_positive_duration(duration: duration_pb2.Duration, field: str) -> None:
    if _duration_nanoseconds(duration, field) <= 0:
        raise ContractValidationError(f"{field} must be positive")


def _canonical_nonempty(value: str) -> bool:
    return bool(value) and value == value.strip()


def _validate_timestamp_field(value: timestamp_pb2.Timestamp, field: str) -> None:
    try:
        value.ToDatetime()
    except (OverflowError, ValueError) as error:
        raise ContractValidationError(f"{field} is not a valid protobuf timestamp") from error


def _fact_path_type(path: str) -> str:
    for type_name, paths in _ALLOWED_FACT_PATHS.items():
        if path in paths:
            return type_name
    raise ContractValidationError(f"fact path {path!r} is not allowed")


def _semantic_value_kind(value: semantic_pb2.SemanticValue) -> str:
    kind = value.WhichOneof("kind")
    if kind is None:
        raise ContractValidationError("semantic values must set a kind")
    if kind == "double_value" and not math.isfinite(value.double_value):
        raise ContractValidationError("double semantic values must be finite")
    return kind


def _validate_semantic_value(
    value: semantic_pb2.SemanticValue,
    *,
    expected_fact_type: str | None = None,
    field: str,
) -> None:
    kind = _semantic_value_kind(value)
    if kind == "object_value":
        keys: list[str] = []
        for nested in value.object_value.fields:
            if not _canonical_nonempty(nested.key):
                raise ContractValidationError(f"{field} object field keys must be nonblank")
            keys.append(nested.key)
            _validate_semantic_value(nested.value, field=f"{field}.{nested.key}")
        _validate_unique_ids(keys, f"{field} object field keys")
    elif kind == "list_value":
        if not value.list_value.values:
            raise ContractValidationError(f"{field} list operands must not be empty")
        for index, item in enumerate(value.list_value.values):
            _validate_semantic_value(item, field=f"{field}[{index}]")
    if expected_fact_type is None:
        return
    allowed_kinds = {
        "bool": {"bool_value"},
        "uint64": {"uint64_value"},
        "double": {"double_value"},
    }[expected_fact_type]
    if kind not in allowed_kinds:
        raise ContractValidationError(
            f"{field} must use {expected_fact_type} semantic values for the selected fact path"
        )


def _validate_condition(
    condition: execution_pb2.Condition,
    *,
    field: str,
    require_id: bool,
) -> None:
    if require_id and not _canonical_nonempty(condition.condition_id):
        raise ContractValidationError(f"{field} requires a nonblank condition_id")
    if condition.condition_id and not _canonical_nonempty(condition.condition_id):
        raise ContractValidationError(f"{field} condition_id must have no surrounding whitespace")
    if condition.display_name and not _canonical_nonempty(condition.display_name):
        raise ContractValidationError(f"{field} display_name must have no surrounding whitespace")
    if condition.expression and not _canonical_nonempty(condition.expression):
        raise ContractValidationError(f"{field} expression must have no surrounding whitespace")
    if condition.language and condition.language != _EXPRESSION_LANGUAGE:
        raise ContractValidationError(f"{field} language must be {_EXPRESSION_LANGUAGE}")
    if (
        condition.operator == execution_pb2.CONDITION_OPERATOR_UNKNOWN
        or condition.operator not in execution_pb2.ConditionOperator.values()
    ):
        raise ContractValidationError(f"{field} operator must be recognized")
    if any(
        not _canonical_nonempty(key) or not _canonical_nonempty(value)
        for key, value in condition.attributes.items()
    ):
        raise ContractValidationError(f"{field} attributes must be nonblank")
    if condition.fact_path:
        if not _canonical_nonempty(condition.fact_path):
            raise ContractValidationError(f"{field} fact_path must have no surrounding whitespace")
        if not _FACT_PATH_PATTERN.fullmatch(condition.fact_path):
            raise ContractValidationError(f"{field} fact_path must use canonical dotted syntax")
        _fact_path_type(condition.fact_path)
    if condition.comparison_fact_path:
        if not _canonical_nonempty(condition.comparison_fact_path):
            raise ContractValidationError(
                f"{field} comparison_fact_path must have no surrounding whitespace"
            )
        if not _FACT_PATH_PATTERN.fullmatch(condition.comparison_fact_path):
            raise ContractValidationError(
                f"{field} comparison_fact_path must use canonical dotted syntax"
            )
        _fact_path_type(condition.comparison_fact_path)

    if condition.operator in _COMPOSITE_OPERATORS:
        if condition.fact_path or condition.comparison_fact_path or condition.operands:
            raise ContractValidationError(
                f"{field} composite predicates must not carry fact paths or operands"
            )
        if condition.operator == execution_pb2.CONDITION_OPERATOR_NOT:
            if len(condition.predicates) != 1:
                raise ContractValidationError(f"{field} NOT predicates require exactly one child")
        elif len(condition.predicates) < 2:
            raise ContractValidationError(
                f"{field} AND/OR predicates require at least two child predicates"
            )
        for index, predicate in enumerate(condition.predicates):
            _validate_condition(
                predicate,
                field=f"{field}.predicates[{index}]",
                require_id=False,
            )
        if not condition.expression:
            raise ContractValidationError(f"{field} composite predicates require expression")
        return

    if condition.operator not in _LEAF_OPERATORS:
        raise ContractValidationError(f"{field} uses an unsupported predicate operator")
    if condition.predicates:
        raise ContractValidationError(f"{field} leaf predicates must not contain child predicates")
    if not condition.fact_path:
        raise ContractValidationError(f"{field} leaf predicates require fact_path")
    fact_type = _fact_path_type(condition.fact_path)
    if condition.operator == execution_pb2.CONDITION_OPERATOR_EXISTS:
        if condition.operands or condition.comparison_fact_path:
            raise ContractValidationError(
                f"{field} EXISTS predicates must not carry operands or comparison_fact_path"
            )
    elif condition.comparison_fact_path:
        comparison_type = _fact_path_type(condition.comparison_fact_path)
        if comparison_type != fact_type:
            raise ContractValidationError(f"{field} comparison_fact_path type must match fact_path")
        if condition.operands:
            raise ContractValidationError(
                f"{field} must not set both operands and comparison_fact_path"
            )
        if condition.operator in _SET_OPERATORS:
            raise ContractValidationError(
                f"{field} IN predicates require literal list operands, not comparison facts"
            )
    else:
        if not condition.operands:
            raise ContractValidationError(
                f"{field} predicates require operands or comparison_fact_path"
            )
        if condition.operator in _ORDERING_OPERATORS | _EQUALITY_OPERATORS:
            if len(condition.operands) != 1:
                raise ContractValidationError(
                    f"{field} comparison predicates require exactly one operand"
                )
            _validate_semantic_value(
                condition.operands[0],
                expected_fact_type=fact_type,
                field=f"{field}.operands[0]",
            )
        elif condition.operator in _SET_OPERATORS:
            if len(condition.operands) != 1:
                raise ContractValidationError(f"{field} IN predicates require one list operand")
            operand = condition.operands[0]
            if operand.WhichOneof("kind") != "list_value":
                raise ContractValidationError(f"{field} IN predicates require a list operand")
            for index, item in enumerate(operand.list_value.values):
                _validate_semantic_value(
                    item,
                    expected_fact_type=fact_type,
                    field=f"{field}.operands[0][{index}]",
                )
    if not condition.expression:
        raise ContractValidationError(f"{field} requires expression for expression fallback")


def _validate_semantic_field(
    field_message: semantic_pb2.SemanticField,
    *,
    field: str,
    allow_contract_paths: bool = False,
) -> None:
    if not _canonical_nonempty(field_message.key):
        raise ContractValidationError(f"{field} key must be nonblank")
    if not _FACT_PATH_PATTERN.fullmatch(field_message.key):
        raise ContractValidationError(f"{field} key must use canonical dotted syntax")
    if field_message.key not in _ALL_ALLOWED_FACT_PATHS and (
        not allow_contract_paths or not field_message.key.startswith("contract.")
    ):
        raise ContractValidationError(f"{field} key {field_message.key!r} is not allowed")
    _validate_semantic_value(
        field_message.value,
        expected_fact_type=_fact_path_type(field_message.key),
        field=f"{field}.{field_message.key}",
    )


def validate_component_version(
    version: execution_pb2.ComponentVersion,
    *,
    field: str = "component_version",
) -> None:
    """Validate one sourced component-version observation."""
    if (
        version.kind == execution_pb2.COMPONENT_KIND_UNKNOWN
        or version.kind not in execution_pb2.ComponentKind.values()
    ):
        raise ContractValidationError(f"{field}.kind must be recognized and non-UNKNOWN")
    for field_name in ("name", "version", "source"):
        if not _canonical_nonempty(getattr(version, field_name)):
            raise ContractValidationError(f"{field}.{field_name} must be nonblank and canonical")
    if not version.HasField("observed_at"):
        raise ContractValidationError(f"{field}.observed_at is required")
    _validate_timestamp_field(version.observed_at, f"{field}.observed_at")
    if any(
        not _canonical_nonempty(key) or not _canonical_nonempty(value)
        for key, value in version.attributes.items()
    ):
        raise ContractValidationError(f"{field}.attributes must be nonblank and canonical")
    _component_identity(version.kind, version.name, field=field, allow_legacy_alias=False)


def component_version_identity(
    version: execution_pb2.ComponentVersion,
    *,
    field: str = "component_version",
) -> tuple[int, str]:
    """Validate a component version and return its canonical kind/name identity."""
    validate_component_version(version, field=field)
    identity, _ = _component_identity(
        version.kind, version.name, field=field, allow_legacy_alias=False
    )
    return identity


def _validate_unique_ids(values: list[str], field: str, *, case_insensitive: bool = False) -> None:
    if any(not _canonical_nonempty(value) for value in values):
        raise ContractValidationError(
            f"{field} values must be nonempty and have no surrounding whitespace"
        )
    comparison_values = [value.lower() if case_insensitive else value for value in values]
    if len(set(comparison_values)) != len(comparison_values):
        raise ContractValidationError(f"{field} values must be unique")


def _parse_version(
    value: str,
) -> tuple[int, int, int, tuple[str, ...], tuple[str, ...]]:
    if not _canonical_nonempty(value):
        raise ContractValidationError("version must be non-empty and canonical")
    without_prefix = value[1:] if value.startswith(("v", "V")) else value
    core_and_prerelease, build_separator, build_text = without_prefix.partition("+")
    if build_separator and (not build_text or "+" in build_text):
        raise ContractValidationError("version build metadata is invalid")
    core, prerelease_separator, prerelease_text = core_and_prerelease.partition("-")
    components = core.split(".")
    if len(components) not in {2, 3}:
        raise ContractValidationError("version must have 2 or 3 numeric components")
    numbers: list[int] = []
    for component in components:
        if (
            not component.isascii()
            or not component.isdecimal()
            or (len(component) > 1 and component.startswith("0"))
        ):
            raise ContractValidationError("version components must be canonical uint32 values")
        number = int(component, 10)
        if number > _MAX_UINT32:
            raise ContractValidationError("version components must be canonical uint32 values")
        numbers.append(number)
    if len(numbers) == 2:
        numbers.append(0)

    prerelease = tuple(prerelease_text.split(".")) if prerelease_separator else ()
    build = tuple(build_text.split(".")) if build_separator else ()
    for identifiers, field, reject_numeric_leading_zero in (
        (prerelease, "prerelease", True),
        (build, "build metadata", False),
    ):
        if any(
            not identifier
            or _VERSION_IDENTIFIER.fullmatch(identifier) is None
            or (
                reject_numeric_leading_zero
                and identifier.isdecimal()
                and len(identifier) > 1
                and identifier.startswith("0")
            )
            for identifier in identifiers
        ):
            raise ContractValidationError(f"version {field} is invalid")
    return numbers[0], numbers[1], numbers[2], prerelease, build


def normalize_version(value: str) -> str:
    """Normalize the scheduler's accepted SemVer syntax."""
    major, minor, patch, prerelease, build = _parse_version(value)
    result = f"{major}.{minor}.{patch}"
    if prerelease:
        result += "-" + ".".join(prerelease)
    if build:
        result += "+" + ".".join(build)
    return result


def _compare_versions(
    left: tuple[int, int, int, tuple[str, ...], tuple[str, ...]],
    right: tuple[int, int, int, tuple[str, ...], tuple[str, ...]],
) -> int:
    left_core = left[:3]
    right_core = right[:3]
    if left_core != right_core:
        return -1 if left_core < right_core else 1
    left_prerelease = left[3]
    right_prerelease = right[3]
    if not left_prerelease or not right_prerelease:
        if not left_prerelease and not right_prerelease:
            return 0
        return 1 if not left_prerelease else -1
    for left_part, right_part in zip(left_prerelease, right_prerelease, strict=False):
        if left_part == right_part:
            continue
        left_numeric = left_part.isdecimal()
        right_numeric = right_part.isdecimal()
        if left_numeric != right_numeric:
            return -1 if left_numeric else 1
        if left_numeric:
            return -1 if int(left_part) < int(right_part) else 1
        return -1 if left_part < right_part else 1
    if len(left_prerelease) == len(right_prerelease):
        return 0
    return -1 if len(left_prerelease) < len(right_prerelease) else 1


def versions_compatible(current: str, required: str) -> bool:
    """Apply Go scheduler compatibility: pre-1.0 minor, post-1.0 major."""
    current_version = _parse_version(current)
    required_version = _parse_version(required)
    if current_version[0] != required_version[0]:
        return False
    if _compare_versions(current_version, required_version) < 0:
        return False
    if current_version[0] == 0:
        return current_version[1] == required_version[1]
    return True


def _validate_observation_policy(policy: execution_pb2.ObservationPolicy, *, field: str) -> None:
    if policy.HasField("maximum_age"):
        _validate_positive_duration(policy.maximum_age, f"{field}.maximum_age")
    for name in ("missing", "stale"):
        disposition = getattr(policy, name)
        if (
            disposition == execution_pb2.OBSERVATION_DISPOSITION_UNKNOWN
            or disposition not in execution_pb2.ObservationDisposition.values()
        ):
            raise ContractValidationError(f"{field}.{name} must be recognized and non-UNKNOWN")


def _component_identity(
    kind: int,
    name: str,
    *,
    field: str,
    allow_legacy_alias: bool,
) -> tuple[tuple[int, str], bool]:
    if not _canonical_nonempty(name):
        raise ContractValidationError(f"{field}.name must be nonblank and canonical")
    canonical = name.casefold().replace("-", "_")
    alias_kind = _COMPONENT_KIND_ALIASES.get(canonical)
    if kind == execution_pb2.COMPONENT_KIND_UNKNOWN:
        if not allow_legacy_alias or alias_kind is None:
            raise ContractValidationError(f"{field}.kind must be recognized and non-UNKNOWN")
        return (alias_kind, canonical), True
    if kind not in execution_pb2.ComponentKind.values():
        raise ContractValidationError(f"{field}.kind must be recognized")
    if alias_kind is not None and alias_kind != kind:
        raise ContractValidationError(f"{field}.name alias conflicts with kind")
    return (kind, canonical), False


def _version_constraint_identity(
    constraint: execution_pb2.VersionConstraint, *, field: str
) -> tuple[tuple[int, str], bool]:
    if not _canonical_nonempty(constraint.component):
        raise ContractValidationError(f"{field}.component must be nonblank and canonical")
    try:
        return _component_identity(
            constraint.component_kind,
            constraint.component,
            field=field,
            allow_legacy_alias=True,
        )
    except ContractValidationError as error:
        message = (
            str(error)
            .replace(f"{field}.name", f"{field}.component")
            .replace(f"{field}.kind", f"{field}.component_kind")
        )
        message = message.replace("conflicts with kind", "conflicts with component_kind")
        if constraint.component_kind == execution_pb2.COMPONENT_KIND_UNKNOWN:
            message = f"{field}.component legacy component must use a recognized canonical alias"
        raise ContractValidationError(message) from error


def validate_execution_contract(contract: execution_pb2.ExecutionContract) -> None:
    """Validate the complete generic contract without inspecting algorithm names."""
    if not _canonical_nonempty(contract.contract_id) or not contract.version:
        raise ContractValidationError("contract_id and version are required")
    if not _CONTRACT_VERSION.fullmatch(contract.version):
        raise ContractValidationError("contract version must use supported major version 1")
    for field in (
        "phase_graph",
        "commit_policy",
        "backpressure_policy",
        "safe_point_policy",
        "capabilities",
    ):
        if not contract.HasField(field):
            raise ContractValidationError(f"{field} is required")

    phases: dict[str, execution_pb2.Phase] = {}
    for phase in contract.phase_graph.phases:
        phase_id = phase.phase_id
        if not _canonical_nonempty(phase_id) or not phase.display_name.strip():
            raise ContractValidationError("phases require nonblank IDs and display names")
        if phase_id in phases:
            raise ContractValidationError("phase IDs must be unique")
        if (
            phase.kind == execution_pb2.PHASE_KIND_UNKNOWN
            or phase.kind not in execution_pb2.PhaseKind.values()
        ):
            raise ContractValidationError(f"phase {phase_id} kind must be recognized")
        if phase.parallelism <= 0 or phase.max_attempts <= 0:
            raise ContractValidationError(
                f"phase {phase_id} parallelism and max_attempts must be positive"
            )
        if any(not _canonical_nonempty(key) for key in phase.labels):
            raise ContractValidationError(
                f"phase {phase_id} label keys must have no surrounding whitespace"
            )
        phases[phase_id] = phase
    if not phases:
        raise ContractValidationError("phase graph must contain named phases")

    entries = list(contract.phase_graph.entry_phase_ids)
    _validate_unique_ids(entries, "entry_phase_ids")
    if not entries:
        raise ContractValidationError("at least one entry phase is required")
    if any(entry not in phases for entry in entries):
        raise ContractValidationError("entry phase must exist")

    outgoing: dict[str, set[str]] = {phase_id: set() for phase_id in phases}
    indegree = dict.fromkeys(phases, 0)
    edges: set[tuple[str, str]] = set()
    for edge in contract.phase_graph.edges:
        endpoints = (edge.from_phase_id, edge.to_phase_id)
        if endpoints[0] not in phases or endpoints[1] not in phases:
            raise ContractValidationError("edge endpoint must exist")
        if endpoints[0] == endpoints[1]:
            raise ContractValidationError("self edges are forbidden")
        if endpoints in edges:
            raise ContractValidationError("phase edges must be unique")
        edges.add(endpoints)
        outgoing[endpoints[0]].add(endpoints[1])
        indegree[endpoints[1]] += 1

    roots = {phase_id for phase_id, degree in indegree.items() if degree == 0}
    if set(entries) != roots:
        raise ContractValidationError("entry phases must exactly identify graph roots")
    remaining_indegree = dict(indegree)
    queue = sorted(roots)
    visited: set[str] = set()
    while queue:
        phase_id = queue.pop(0)
        visited.add(phase_id)
        for target in sorted(outgoing[phase_id]):
            remaining_indegree[target] -= 1
            if remaining_indegree[target] == 0:
                queue.append(target)
                queue.sort()
    if len(visited) != len(phases):
        raise ContractValidationError("phase graph must be acyclic and reachable from entries")

    if not contract.validity_rules:
        raise ContractValidationError("at least one validity rule is required")
    rule_ids: list[str] = []
    for rule in contract.validity_rules:
        if not _canonical_nonempty(rule.rule_id) or not _canonical_nonempty(rule.description):
            raise ContractValidationError("validity rules require nonblank ID and description")
        if not (rule.expression or rule.HasField("predicate")):
            raise ContractValidationError("validity rules require expression or predicate")
        if rule.expression and not _canonical_nonempty(rule.expression):
            raise ContractValidationError("validity rule expression must have no whitespace")
        if (
            rule.failure_mode == execution_pb2.VALIDITY_FAILURE_MODE_UNKNOWN
            or rule.failure_mode not in execution_pb2.ValidityFailureMode.values()
        ):
            raise ContractValidationError("validity rule failure mode must be recognized")
        if rule.HasField("predicate"):
            _validate_condition(
                rule.predicate,
                field=f"validity_rules[{rule.rule_id}]",
                require_id=True,
            )
            if rule.expression and rule.predicate.expression != rule.expression:
                raise ContractValidationError(
                    "validity rule predicate expression must match expression fallback"
                )
        rule_ids.append(rule.rule_id)
    _validate_unique_ids(rule_ids, "validity rule IDs")

    condition_ids: list[str] = []
    for index, condition in enumerate(contract.conditions):
        _validate_condition(
            condition,
            field=f"conditions[{index}]",
            require_id=True,
        )
        condition_ids.append(condition.condition_id)
    _validate_unique_ids(condition_ids, "condition IDs")

    if not contract.version_constraints:
        raise ContractValidationError("at least one version constraint is required")
    components: set[tuple[int, str]] = set()
    protocol_found = False
    for index, constraint in enumerate(contract.version_constraints):
        field = f"version_constraints[{index}]"
        if (
            constraint.operator == execution_pb2.VERSION_OPERATOR_UNKNOWN
            or constraint.operator not in execution_pb2.VersionOperator.values()
        ):
            raise ContractValidationError("version constraint operator must be recognized")
        if constraint.operator == execution_pb2.VERSION_OPERATOR_EXACT:
            if not _canonical_nonempty(constraint.version):
                raise ContractValidationError(f"{field}.version must be nonblank and canonical")
            required_version = constraint.version
        else:
            required_version = normalize_version(constraint.version)
        if constraint.source and not _canonical_nonempty(constraint.source):
            raise ContractValidationError(f"{field}.source must be canonical when present")
        identity, legacy = _version_constraint_identity(constraint, field=field)
        if identity in components:
            raise ContractValidationError(
                "version constraint kind and name identity must be unique ignoring case"
            )
        components.add(identity)
        if constraint.HasField("observation_policy"):
            _validate_observation_policy(
                constraint.observation_policy, field=f"{field}.observation_policy"
            )
        elif not legacy or identity[0] != execution_pb2.COMPONENT_KIND_PROTOCOL:
            raise ContractValidationError(
                f"{field}.observation_policy must be present for typed or non-protocol constraints"
            )
        if identity[0] != execution_pb2.COMPONENT_KIND_PROTOCOL:
            continue
        protocol_found = True
        if constraint.operator == execution_pb2.VERSION_OPERATOR_COMPATIBLE:
            compatible = versions_compatible(SCHEDULER_PROTOCOL_VERSION, required_version)
        else:
            compatible = required_version == SCHEDULER_PROTOCOL_VERSION
        if not compatible:
            raise ContractValidationError(
                f"protocol {constraint.version} is incompatible with scheduler protocol "
                f"{SCHEDULER_PROTOCOL_VERSION}"
            )
    if not protocol_found:
        raise ContractValidationError("protocol version constraint is required")

    critical_fact_paths: list[str] = []
    for index, critical in enumerate(contract.critical_fact_policies):
        field = f"critical_fact_policies[{index}]"
        if (
            not _canonical_nonempty(critical.fact_path)
            or _FACT_PATH_PATTERN.fullmatch(critical.fact_path) is None
        ):
            raise ContractValidationError(f"{field}.fact_path must use canonical dotted syntax")
        critical_fact_paths.append(critical.fact_path)
        if not critical.HasField("observation_policy"):
            raise ContractValidationError(f"{field}.observation_policy must be present")
        _validate_observation_policy(
            critical.observation_policy, field=f"{field}.observation_policy"
        )
    _validate_unique_ids(critical_fact_paths, "critical fact policy paths")

    commit = contract.commit_policy
    if (
        commit.mode == execution_pb2.COMMIT_MODE_UNKNOWN
        or commit.mode not in execution_pb2.CommitMode.values()
    ):
        raise ContractValidationError("commit mode must be recognized")
    if commit.minimum_successful_units <= 0:
        raise ContractValidationError("minimum_successful_units must be positive")
    if not commit.HasField("commit_timeout"):
        raise ContractValidationError("commit_timeout is required")
    _validate_positive_duration(commit.commit_timeout, "commit_timeout")

    backpressure = contract.backpressure_policy
    if (
        backpressure.mode == execution_pb2.BACKPRESSURE_MODE_UNKNOWN
        or backpressure.mode not in execution_pb2.BackpressureMode.values()
    ):
        raise ContractValidationError("backpressure mode must be recognized")
    if backpressure.maximum_buffer_level <= 0:
        raise ContractValidationError("maximum_buffer_level must be positive")
    if not (
        backpressure.low_watermark
        <= backpressure.high_watermark
        <= backpressure.maximum_buffer_level
    ):
        raise ContractValidationError("backpressure watermarks must be ordered")
    if not backpressure.HasField("stall_timeout"):
        raise ContractValidationError("stall_timeout is required")
    _validate_positive_duration(backpressure.stall_timeout, "stall_timeout")

    safe_point = contract.safe_point_policy
    interval_nanoseconds = (
        _duration_nanoseconds(safe_point.interval, "safe-point interval")
        if safe_point.HasField("interval")
        else None
    )
    if commit.require_safe_point and not safe_point.enabled:
        raise ContractValidationError("commit policy requires enabled safe points")
    if safe_point.enabled:
        if (
            safe_point.trigger == execution_pb2.SAFE_POINT_TRIGGER_UNKNOWN
            or safe_point.trigger not in execution_pb2.SafePointTrigger.values()
        ):
            raise ContractValidationError("safe-point trigger must be recognized")
        if not safe_point.HasField("maximum_wait"):
            raise ContractValidationError("safe-point maximum_wait is required")
        _validate_positive_duration(safe_point.maximum_wait, "safe-point maximum_wait")
        required_phases = list(safe_point.required_phase_ids)
        _validate_unique_ids(required_phases, "safe-point required_phase_ids")
        if any(phase_id not in phases for phase_id in required_phases):
            raise ContractValidationError("safe-point required phase must exist")
        if safe_point.trigger == execution_pb2.SAFE_POINT_TRIGGER_INTERVAL:
            if interval_nanoseconds is None:
                raise ContractValidationError("interval trigger requires an interval")
            if interval_nanoseconds <= 0:
                raise ContractValidationError("safe-point interval must be positive")
        elif interval_nanoseconds not in (None, 0):
            raise ContractValidationError("only interval trigger may set an interval")
    elif (
        safe_point.trigger != execution_pb2.SAFE_POINT_TRIGGER_UNKNOWN
        or safe_point.required_phase_ids
        or safe_point.HasField("interval")
        or safe_point.HasField("maximum_wait")
    ):
        raise ContractValidationError("disabled safe-point policy must not carry active fields")

    _validate_unique_ids(list(contract.capabilities.extensions), "capability extensions")
    for index, extension in enumerate(contract.capabilities.extensions):
        if not _canonical_nonempty(extension):
            raise ContractValidationError(f"capability extension {index} must be nonblank")
    if contract.contract_id != canonical_contract_id(contract):
        raise ContractValidationError("contract_id does not match canonical contract content")


def validate_contract_observation(observation: execution_pb2.ContractObservation) -> None:
    """Validate typed observation payloads before they are attached to intents."""
    required_strings = {
        "source": observation.source,
        "event_id": observation.event_id,
        "phase_id": observation.phase_id,
        "policy_version": observation.policy_version,
    }
    missing = [field for field, value in required_strings.items() if not _canonical_nonempty(value)]
    if missing:
        raise ContractValidationError(
            f"contract observation fields must be nonblank: {', '.join(sorted(missing))}"
        )
    if not observation.HasField("observed_at"):
        raise ContractValidationError("contract observation observed_at is required")
    for field_name in ("observed_at", "oldest_sample_at", "backpressure_started_at"):
        if not observation.HasField(field_name):
            continue
        _validate_timestamp_field(
            getattr(observation, field_name), f"contract observation {field_name}"
        )
    for field_name in ("effective_sample_size", "effective_sample_size_ratio"):
        if observation.HasField(field_name) and not math.isfinite(getattr(observation, field_name)):
            raise ContractValidationError(
                f"contract observation {field_name} must be finite when present"
            )
    typed_fact_keys: list[str] = []
    for index, typed_fact in enumerate(observation.typed_facts):
        _validate_semantic_field(
            typed_fact,
            field=f"contract_observation.typed_facts[{index}]",
        )
        if typed_fact.key.startswith("contract."):
            raise ContractValidationError(
                "contract observation typed_facts must use sample.*, batch.*, or group.* paths"
            )
        typed_fact_keys.append(typed_fact.key)
    _validate_unique_ids(typed_fact_keys, "contract observation typed_facts")
    typed_fact_map = {typed_fact.key: typed_fact for typed_fact in observation.typed_facts}
    observed_fact_map: dict[str, execution_pb2.ObservedFact] = {}
    for index, observed_fact in enumerate(observation.fact_observations):
        field = f"contract_observation.fact_observations[{index}]"
        if not observed_fact.HasField("fact"):
            raise ContractValidationError(f"{field}.fact is required")
        _validate_semantic_field(observed_fact.fact, field=f"{field}.fact")
        if not observed_fact.HasField("observed_at"):
            raise ContractValidationError(f"{field}.observed_at is required")
        _validate_timestamp_field(observed_fact.observed_at, f"{field}.observed_at")
        if not _canonical_nonempty(observed_fact.source):
            raise ContractValidationError(f"{field}.source must be nonblank and canonical")
        key = observed_fact.fact.key
        if key in observed_fact_map:
            raise ContractValidationError(
                f"contract observation fact_observations contains duplicate key {key!r}"
            )
        legacy = typed_fact_map.get(key)
        if legacy is not None and legacy.value != observed_fact.fact.value:
            raise ContractValidationError(
                f"contract observation fact_observations {key} conflicts with typed_facts"
            )
        observed_fact_map[key] = observed_fact

    component_versions: dict[tuple[int, str], execution_pb2.ComponentVersion] = {}
    for index, version in enumerate(observation.component_versions):
        field = f"contract_observation.component_versions[{index}]"
        identity = component_version_identity(version, field=field)
        existing = component_versions.get(identity)
        if existing is not None:
            raise ContractValidationError(
                f"{field} duplicates component version identity for {version.name!r}"
            )
        component_versions[identity] = version

    registered_fact_map = dict(typed_fact_map)
    registered_fact_map.update((key, observed.fact) for key, observed in observed_fact_map.items())
    scalar_bindings = (
        ("policy_lag", "sample.policy_lag", "uint64_value"),
        ("sample_stale", "sample.stale", "bool_value"),
        ("buffer_level", "buffer.level.current", "uint64_value"),
        ("effective_sample_size", "batch.effective_sample_size", "double_value"),
        ("safe_point", "runtime.safe_point", "bool_value"),
        ("sample_count", "sample.sample_count", "uint64_value"),
        (
            "effective_sample_size_ratio",
            "batch.effective_sample_size_ratio",
            "double_value",
        ),
    )
    for field_name, fact_key, semantic_kind in scalar_bindings:
        if not observation.HasField(field_name):
            continue
        typed_fact = registered_fact_map.get(fact_key)
        if typed_fact is None:
            raise ContractValidationError(
                f"contract observation typed_facts must include {fact_key} when {field_name} is set"
            )
        if typed_fact.value.WhichOneof("kind") != semantic_kind:
            raise ContractValidationError(
                f"contract observation typed_facts {fact_key} must use {semantic_kind}"
            )
        actual = getattr(observation, field_name)
        expected = getattr(typed_fact.value, semantic_kind)
        if actual != expected:
            raise ContractValidationError(
                f"contract observation typed_facts {fact_key} must match {field_name}"
            )
    accepted_fact_keys = ("batch.accepted_samples", "group.accepted_samples")
    if observation.HasField("accepted_samples"):
        present = [key for key in accepted_fact_keys if key in registered_fact_map]
        if len(present) != 1:
            raise ContractValidationError(
                "contract observation accepted_samples requires exactly one typed fact of "
                "batch.accepted_samples or group.accepted_samples"
            )
        typed_fact = registered_fact_map[present[0]]
        if typed_fact.value.WhichOneof("kind") != "uint64_value":
            raise ContractValidationError(
                f"contract observation typed_facts {present[0]} must use uint64_value"
            )
        if observation.accepted_samples != typed_fact.value.uint64_value:
            raise ContractValidationError(
                f"contract observation typed_facts {present[0]} must match accepted_samples"
            )
    if observation.HasField("expected_samples"):
        typed_fact = registered_fact_map.get("group.expected_samples")
        if typed_fact is None:
            raise ContractValidationError(
                "contract observation expected_samples requires typed fact group.expected_samples"
            )
        if typed_fact.value.WhichOneof("kind") != "uint64_value":
            raise ContractValidationError(
                "contract observation typed_facts group.expected_samples must use uint64_value"
            )
        if observation.expected_samples != typed_fact.value.uint64_value:
            raise ContractValidationError(
                "contract observation typed_facts group.expected_samples must match "
                "expected_samples"
            )
