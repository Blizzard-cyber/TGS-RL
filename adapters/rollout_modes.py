"""Rollout concurrency semantics expressed as generated Proto policies."""

from abc import ABC, abstractmethod
from datetime import timedelta

from google.protobuf import duration_pb2
from tgsrl.v1 import execution_pb2, trace_pb2


def _duration(seconds: float) -> duration_pb2.Duration:
    value = duration_pb2.Duration()
    value.FromTimedelta(timedelta(seconds=seconds))
    return value


class RolloutModeAdapter(ABC):
    """Extension point for barriers, commits, backpressure, and safe points."""

    name: str
    proto_value: int

    def phases(self) -> tuple[execution_pb2.Phase, ...]:
        return (
            execution_pb2.Phase(
                phase_id="prefill",
                display_name="Rollout prefill",
                kind=execution_pb2.PHASE_KIND_PREFILL,
                parallelism=1,
                max_attempts=3,
                labels={"component": "rollout"},
            ),
            execution_pb2.Phase(
                phase_id="decode",
                display_name="Rollout decode",
                kind=execution_pb2.PHASE_KIND_DECODE,
                parallelism=1,
                max_attempts=3,
                labels={"component": "rollout"},
            ),
        )

    @abstractmethod
    def commit_policy(self) -> execution_pb2.CommitPolicy:
        """Return the visibility and retry policy."""

    @abstractmethod
    def backpressure_policy(self) -> execution_pb2.BackpressurePolicy:
        """Return bounded-buffer behavior."""

    @abstractmethod
    def safe_point_policy(self) -> execution_pb2.SafePointPolicy:
        """Return mutation-boundary behavior."""

    @abstractmethod
    def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
        """Return mode-specific barrier and staleness rules."""

    @abstractmethod
    def capabilities(self) -> execution_pb2.Capabilities:
        """Return required execution semantics."""


class SyncRolloutAdapter(RolloutModeAdapter):
    name = "sync"
    proto_value = trace_pb2.ROLLOUT_MODE_SYNC

    def commit_policy(self) -> execution_pb2.CommitPolicy:
        return execution_pb2.CommitPolicy(
            mode=execution_pb2.COMMIT_MODE_ALL_OR_NOTHING,
            minimum_successful_units=1,
            max_retries=2,
            commit_timeout=_duration(60),
            require_safe_point=True,
        )

    def backpressure_policy(self) -> execution_pb2.BackpressurePolicy:
        return execution_pb2.BackpressurePolicy(
            mode=execution_pb2.BACKPRESSURE_MODE_BLOCK_PRODUCER,
            low_watermark=0,
            high_watermark=1,
            maximum_buffer_level=1,
            stall_timeout=_duration(30),
        )

    def safe_point_policy(self) -> execution_pb2.SafePointPolicy:
        return execution_pb2.SafePointPolicy(
            enabled=True,
            trigger=execution_pb2.SAFE_POINT_TRIGGER_PHASE_BOUNDARY,
            maximum_wait=_duration(60),
            required_phase_ids=["optimizer", "weight-sync"],
        )

    def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
        return (
            execution_pb2.ValidityRule(
                rule_id="sync-policy-exact",
                description="Synchronous rollout consumes the current policy only",
                expression="sample.policy_lag == 0",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_REJECT,
            ),
        )

    def capabilities(self) -> execution_pb2.Capabilities:
        return execution_pb2.Capabilities(
            deterministic_replay=True,
            transactional_commits=True,
            checkpoint_restore=True,
            extensions=["barrier:global", "policy-window:0"],
        )


class PartialAsyncRolloutAdapter(RolloutModeAdapter):
    name = "partial-async"
    proto_value = trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC

    def commit_policy(self) -> execution_pb2.CommitPolicy:
        return execution_pb2.CommitPolicy(
            mode=execution_pb2.COMMIT_MODE_QUORUM,
            minimum_successful_units=1,
            max_retries=3,
            commit_timeout=_duration(30),
            require_safe_point=True,
        )

    def backpressure_policy(self) -> execution_pb2.BackpressurePolicy:
        return execution_pb2.BackpressurePolicy(
            mode=execution_pb2.BACKPRESSURE_MODE_REQUEST_SCALE_OUT,
            low_watermark=8,
            high_watermark=32,
            maximum_buffer_level=64,
            stall_timeout=_duration(15),
        )

    def safe_point_policy(self) -> execution_pb2.SafePointPolicy:
        return execution_pb2.SafePointPolicy(
            enabled=True,
            trigger=execution_pb2.SAFE_POINT_TRIGGER_BUFFER_THRESHOLD,
            maximum_wait=_duration(30),
            required_phase_ids=["optimizer", "weight-sync"],
        )

    def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
        return (
            execution_pb2.ValidityRule(
                rule_id="partial-async-policy-window",
                description="Partially asynchronous samples use a bounded policy window",
                expression="sample.policy_lag <= 1",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_REJECT,
            ),
        )

    def capabilities(self) -> execution_pb2.Capabilities:
        return execution_pb2.Capabilities(
            elastic_parallelism=True,
            deterministic_replay=True,
            transactional_commits=True,
            checkpoint_restore=True,
            policy_hot_swap=True,
            extensions=["barrier:quorum", "policy-window:1"],
        )


class FullAsyncRolloutAdapter(RolloutModeAdapter):
    name = "full-async"
    proto_value = trace_pb2.ROLLOUT_MODE_FULLY_ASYNC

    def commit_policy(self) -> execution_pb2.CommitPolicy:
        return execution_pb2.CommitPolicy(
            mode=execution_pb2.COMMIT_MODE_BEST_EFFORT,
            minimum_successful_units=1,
            max_retries=3,
            commit_timeout=_duration(10),
            require_safe_point=False,
        )

    def backpressure_policy(self) -> execution_pb2.BackpressurePolicy:
        return execution_pb2.BackpressurePolicy(
            mode=execution_pb2.BACKPRESSURE_MODE_SHED_OLDEST,
            low_watermark=16,
            high_watermark=64,
            maximum_buffer_level=128,
            stall_timeout=_duration(5),
        )

    def safe_point_policy(self) -> execution_pb2.SafePointPolicy:
        return execution_pb2.SafePointPolicy(
            enabled=True,
            trigger=execution_pb2.SAFE_POINT_TRIGGER_INTERVAL,
            interval=_duration(5),
            maximum_wait=_duration(15),
            required_phase_ids=["weight-sync"],
        )

    def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
        return (
            execution_pb2.ValidityRule(
                rule_id="full-async-policy-window",
                description="Fully asynchronous samples stay inside a bounded window",
                expression="sample.policy_lag <= 3",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_REJECT,
            ),
        )

    def capabilities(self) -> execution_pb2.Capabilities:
        return execution_pb2.Capabilities(
            elastic_parallelism=True,
            deterministic_replay=True,
            checkpoint_restore=True,
            policy_hot_swap=True,
            extensions=["barrier:none", "policy-window:3"],
        )
