"""Deterministic composition and validation of execution contracts."""

import hashlib
import re
from itertools import pairwise

from google.protobuf import duration_pb2
from tgsrl.v1 import execution_pb2

from adapters.algorithms import AlgorithmAdapter
from adapters.rollout_modes import RolloutModeAdapter

_CONTRACT_VERSION = re.compile(r"^1\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
_VERSION = re.compile(r"^[vV]?[0-9]+(?:\.[0-9]+){1,2}(?:-[0-9A-Za-z.-]+)?$")
_MAX_DURATION_SECONDS = 315_576_000_000
_MAX_UINT32 = (1 << 32) - 1
_CONTRACT_ID_PREFIX = "contract-sha256-"
SCHEDULER_PROTOCOL_VERSION = "0.3.0"


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
        validity_rules=[*algorithm.validity_rules(), *rollout_mode.validity_rules()],
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


def _validate_unique_ids(values: list[str], field: str, *, case_insensitive: bool = False) -> None:
    if any(not _canonical_nonempty(value) for value in values):
        raise ContractValidationError(
            f"{field} values must be nonempty and have no surrounding whitespace"
        )
    comparison_values = [value.lower() if case_insensitive else value for value in values]
    if len(set(comparison_values)) != len(comparison_values):
        raise ContractValidationError(f"{field} values must be unique")


def normalize_version(value: str) -> str:
    """Normalize the scheduler's accepted two/three-component version syntax."""
    if value != value.strip() or not _VERSION.fullmatch(value):
        raise ContractValidationError(
            "version must be numeric major.minor or major.minor.patch with an optional "
            "v prefix and prerelease suffix"
        )
    without_prefix = value[1:] if value.startswith(("v", "V")) else value
    base, separator, prerelease = without_prefix.partition("-")
    normalized: list[str] = []
    try:
        for component in base.split("."):
            number = int(component, 10)
            if number > _MAX_UINT32:
                raise ValueError
            normalized.append(str(number))
    except ValueError as error:
        raise ContractValidationError(
            "version components must be uint32 decimal numbers"
        ) from error
    if len(normalized) == 2:
        normalized.append("0")
    result = ".".join(normalized)
    if separator:
        result += f"-{prerelease}"
    return result


def versions_compatible(current: str, required: str) -> bool:
    """Apply Go scheduler compatibility: pre-1.0 minor, post-1.0 major."""
    current_parts = normalize_version(current).split(".")
    required_parts = normalize_version(required).split(".")
    if current_parts[0] != required_parts[0]:
        return False
    if current_parts[0] == "0":
        return current_parts[1] == required_parts[1]
    return True


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
        if not _canonical_nonempty(rule.rule_id) or not all(
            (rule.description.strip(), rule.expression.strip())
        ):
            raise ContractValidationError("validity rules require ID, description, and expression")
        if (
            rule.failure_mode == execution_pb2.VALIDITY_FAILURE_MODE_UNKNOWN
            or rule.failure_mode not in execution_pb2.ValidityFailureMode.values()
        ):
            raise ContractValidationError("validity rule failure mode must be recognized")
        rule_ids.append(rule.rule_id)
    _validate_unique_ids(rule_ids, "validity rule IDs")

    if not contract.version_constraints:
        raise ContractValidationError("at least one version constraint is required")
    components: set[str] = set()
    protocol_found = False
    for constraint in contract.version_constraints:
        if not _canonical_nonempty(constraint.component):
            raise ContractValidationError(
                "version constraint component must have no surrounding whitespace"
            )
        required_version = normalize_version(constraint.version)
        if (
            constraint.operator == execution_pb2.VERSION_OPERATOR_UNKNOWN
            or constraint.operator not in execution_pb2.VersionOperator.values()
        ):
            raise ContractValidationError("version constraint operator must be recognized")
        component = constraint.component.lower()
        if component in components:
            raise ContractValidationError(
                "version constraint components must be unique ignoring case"
            )
        components.add(component)
        if constraint.component.casefold() != "protocol":
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
    if contract.contract_id != canonical_contract_id(contract):
        raise ContractValidationError("contract_id does not match canonical contract content")
