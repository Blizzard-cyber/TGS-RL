"""Versioned device-independent SchedulingIntent construction."""

import hashlib
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from google.protobuf import duration_pb2, timestamp_pb2
from tgsrl.v1 import execution_pb2, resource_pb2, scheduling_pb2, trace_pb2

from adapters.contracts import ContractValidationError, validate_execution_contract
from tgsrl_runtime.proto_utils import timestamp_from_datetime

_MAX_DURATION_SECONDS = 315_576_000_000


class IntentValidationError(ValueError):
    """Raised before a malformed intent can cross the gRPC boundary."""


def _duration(value: timedelta) -> duration_pb2.Duration:
    result = duration_pb2.Duration()
    result.FromTimedelta(value)
    return result


def intent_idempotency_key(execution_id: str, stage_id: str, version: int) -> str:
    """Derive the sole valid retry key for an immutable intent version."""
    material = f"{execution_id}\0{stage_id}\0{version}".encode()
    return "intent-sha256-" + hashlib.sha256(material).hexdigest()


def _validate_timestamp(value: timestamp_pb2.Timestamp, field: str) -> None:
    try:
        value.ToDatetime(tzinfo=UTC)
    except (OverflowError, ValueError) as error:
        raise IntentValidationError(f"{field} is not a valid protobuf timestamp") from error


def _validate_duration(value: duration_pb2.Duration) -> int:
    seconds = value.seconds
    nanos = value.nanos
    valid_shape = (
        -_MAX_DURATION_SECONDS <= seconds <= _MAX_DURATION_SECONDS
        and -999_999_999 <= nanos <= 999_999_999
        and not (seconds > 0 and nanos < 0)
        and not (seconds < 0 and nanos > 0)
    )
    if not valid_shape:
        raise IntentValidationError("ttl is not a valid protobuf duration")
    nanoseconds = value.ToNanoseconds()
    if nanoseconds <= 0:
        raise IntentValidationError("ttl must be positive")
    return nanoseconds


def _validate_unique_nonblank(values: Sequence[str], field: str) -> None:
    normalized = [value.strip() for value in values]
    if any(not value for value in normalized):
        raise IntentValidationError(f"{field} values must be nonblank")
    if len(set(normalized)) != len(normalized):
        raise IntentValidationError(f"{field} values must be unique")


def _validate_resources(resources: resource_pb2.ResourceVector) -> None:
    accelerator_units = resources.accelerator_units
    if not math.isfinite(accelerator_units) or accelerator_units < 0:
        raise IntentValidationError(
            "resources_per_unit.accelerator_units must be finite and non-negative"
        )
    if not any(
        (
            resources.cpu_millis,
            resources.memory_bytes,
            accelerator_units,
            resources.ephemeral_storage_bytes,
            resources.network_bandwidth_bps,
        )
    ):
        raise IntentValidationError("resources_per_unit must have at least one positive dimension")


def _validate_capabilities(capabilities: resource_pb2.CapabilitySet) -> None:
    for field in ("names", "algorithms", "rollout_modes", "supported_actions"):
        _validate_unique_nonblank(list(getattr(capabilities, field)), f"capabilities.{field}")
    if any(not key.strip() or not value.strip() for key, value in capabilities.attributes.items()):
        raise IntentValidationError("capability attribute keys and values must be nonblank")
    if any(
        not key.strip() or not math.isfinite(value) or value < 0
        for key, value in capabilities.limits.items()
    ):
        raise IntentValidationError(
            "capability limit keys must be nonblank and values finite and non-negative"
        )
    if capabilities.HasField("measured_at"):
        _validate_timestamp(capabilities.measured_at, "required_capabilities.measured_at")


class IntentBuilder:
    """Issue monotonically versioned generated Proto intents per stage."""

    _forbidden_keys: ClassVar[frozenset[str]] = frozenset(
        {"device_id", "device_ids", "physical_device_id"}
    )

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._versions: dict[tuple[str, str], int] = {}

    def last_version(self, execution_id: str, stage_id: str) -> int:
        return self._versions.get((execution_id, stage_id), 0)

    def restore_version(self, execution_id: str, stage_id: str, version: int) -> None:
        if version <= 0:
            return
        key = (execution_id, stage_id)
        self._versions[key] = max(self._versions.get(key, 0), version)

    def build(
        self,
        *,
        execution_id: str,
        stage_id: str,
        job_id: str,
        contract: execution_pb2.ExecutionContract,
        rollout_mode: int,
        phase_kind: int,
        policy_version: str,
        ttl: timedelta,
        resources_per_unit: resource_pb2.ResourceVector | None = None,
        required_capabilities: resource_pb2.CapabilitySet | None = None,
        unit_count: int = 1,
        priority: int = 0,
        queue: str = "default",
        labels: Mapping[str, str] | None = None,
        preferences: Mapping[str, float] | None = None,
        deterministic_seed: int = 0,
        version: int | None = None,
    ) -> scheduling_pb2.SchedulingIntent:
        key = (execution_id, stage_id)
        previous = self._versions.get(key, 0)
        selected_version = previous + 1 if version is None else version
        if selected_version <= previous:
            raise IntentValidationError(
                f"intent version must increase monotonically (previous={previous})"
            )
        submitted_at = self._clock()
        if submitted_at.tzinfo is None:
            raise IntentValidationError("clock must return a timezone-aware datetime")
        submitted_at = submitted_at.astimezone(UTC)
        valid_until = submitted_at + ttl
        intent = scheduling_pb2.SchedulingIntent(
            execution_id=execution_id,
            stage_id=stage_id,
            version=selected_version,
            valid_until=timestamp_from_datetime(valid_until),
            ttl=_duration(ttl),
            idempotency_key=intent_idempotency_key(execution_id, stage_id, selected_version),
            submitted_at=timestamp_from_datetime(submitted_at),
            job_id=job_id,
            resources_per_unit=resources_per_unit or resource_pb2.ResourceVector(),
            unit_count=unit_count,
            required_capabilities=required_capabilities or resource_pb2.CapabilitySet(),
            execution_contract=contract,
            priority=priority,
            queue=queue,
            labels=dict(labels or {}),
            rollout_mode=rollout_mode,
            phase_kind=phase_kind,
            policy_version=policy_version,
            deterministic_seed=deterministic_seed,
            preferences=dict(preferences or {}),
        )
        self.validate(intent)
        self._versions[key] = selected_version
        return intent

    @classmethod
    def validate(cls, intent: scheduling_pb2.SchedulingIntent) -> None:
        required_strings = {
            "execution_id": intent.execution_id,
            "stage_id": intent.stage_id,
            "job_id": intent.job_id,
            "idempotency_key": intent.idempotency_key,
            "policy_version": intent.policy_version,
            "queue": intent.queue,
        }
        missing = [field for field, value in required_strings.items() if not value.strip()]
        if missing:
            raise IntentValidationError(
                f"intent fields must be nonblank: {', '.join(sorted(missing))}"
            )
        if intent.version <= 0:
            raise IntentValidationError("intent version must be positive")
        if intent.unit_count <= 0:
            raise IntentValidationError("unit_count must be positive")
        if (
            intent.rollout_mode == trace_pb2.ROLLOUT_MODE_UNKNOWN
            or intent.rollout_mode not in trace_pb2.RolloutMode.values()
        ):
            raise IntentValidationError("rollout_mode must be recognized and non-UNKNOWN")
        if (
            intent.phase_kind == execution_pb2.PHASE_KIND_UNKNOWN
            or intent.phase_kind not in execution_pb2.PhaseKind.values()
        ):
            raise IntentValidationError("phase_kind must be recognized and non-UNKNOWN")
        if not intent.HasField("submitted_at") or not intent.HasField("valid_until"):
            raise IntentValidationError("submitted_at and valid_until are required")
        if not intent.HasField("ttl"):
            raise IntentValidationError("ttl is required")
        _validate_timestamp(intent.submitted_at, "submitted_at")
        _validate_timestamp(intent.valid_until, "valid_until")
        ttl_nanoseconds = _validate_duration(intent.ttl)
        actual_lifetime = intent.valid_until.ToNanoseconds() - intent.submitted_at.ToNanoseconds()
        if actual_lifetime != ttl_nanoseconds:
            raise IntentValidationError("valid_until must equal submitted_at + ttl")
        expected_key = intent_idempotency_key(intent.execution_id, intent.stage_id, intent.version)
        if intent.idempotency_key != expected_key:
            raise IntentValidationError("idempotency_key does not match intent identity")
        if not intent.HasField("execution_contract"):
            raise IntentValidationError("execution_contract is required")
        try:
            validate_execution_contract(intent.execution_contract)
        except ContractValidationError as error:
            raise IntentValidationError(f"invalid execution_contract: {error}") from error
        contract_phases = {
            phase.phase_id: phase for phase in intent.execution_contract.phase_graph.phases
        }
        if intent.stage_id not in contract_phases:
            raise IntentValidationError("stage_id must reference an execution-contract phase")
        if contract_phases[intent.stage_id].kind != intent.phase_kind:
            raise IntentValidationError("phase_kind must match the execution-contract stage")
        if not intent.HasField("resources_per_unit"):
            raise IntentValidationError("resources_per_unit is required")
        _validate_resources(intent.resources_per_unit)
        if intent.HasField("required_capabilities"):
            _validate_capabilities(intent.required_capabilities)
        normalized_keys = {key.strip().casefold() for key in (*intent.labels, *intent.preferences)}
        if normalized_keys & cls._forbidden_keys:
            raise IntentValidationError("intent must not contain physical device selectors")
        if any(not key.strip() for key in intent.labels):
            raise IntentValidationError("label keys must be nonblank")
        if any(
            not key.strip() or not math.isfinite(value) for key, value in intent.preferences.items()
        ):
            raise IntentValidationError("preference keys must be nonblank and values finite")
