"""Algorithm semantics expressed only through generated contract messages."""

from abc import ABC, abstractmethod

from tgsrl.v1 import execution_pb2


def _phase(
    phase_id: str,
    display_name: str,
    kind: int,
    *,
    component: str,
    parallelism: int = 1,
) -> execution_pb2.Phase:
    return execution_pb2.Phase(
        phase_id=phase_id,
        display_name=display_name,
        kind=kind,
        parallelism=parallelism,
        max_attempts=3,
        labels={"component": component},
    )


class AlgorithmAdapter(ABC):
    """Extension point for algorithm acceptance and update semantics."""

    name: str

    @abstractmethod
    def phases(self) -> tuple[execution_pb2.Phase, ...]:
        """Return algorithm-owned phases in dependency order."""

    @abstractmethod
    def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
        """Return scheduler-readable, deterministic acceptance rules."""

    def version_constraints(self) -> tuple[execution_pb2.VersionConstraint, ...]:
        return (
            execution_pb2.VersionConstraint(
                component="protocol",
                operator=execution_pb2.VERSION_OPERATOR_COMPATIBLE,
                version="0.3",
            ),
        )


class PPOAdapter(AlgorithmAdapter):
    """PPO contract semantics without implementing or changing its objective."""

    name = "ppo"

    def phases(self) -> tuple[execution_pb2.Phase, ...]:
        return (
            _phase(
                "reference",
                "Reference log probabilities",
                execution_pb2.PHASE_KIND_REFERENCE,
                component="trainer",
            ),
            _phase(
                "reward",
                "Reward evaluation",
                execution_pb2.PHASE_KIND_REWARD,
                component="trainer",
            ),
            _phase(
                "actor",
                "PPO actor update",
                execution_pb2.PHASE_KIND_ACTOR,
                component="trainer",
            ),
            _phase(
                "optimizer",
                "Optimizer step",
                execution_pb2.PHASE_KIND_OPTIMIZER,
                component="trainer",
            ),
            _phase(
                "weight-sync",
                "Publish policy weights",
                execution_pb2.PHASE_KIND_WEIGHT_SYNC,
                component="policy",
            ),
        )

    def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
        return (
            execution_pb2.ValidityRule(
                rule_id="ppo-policy-window",
                description="PPO samples must remain inside the configured policy window",
                expression="sample.policy_lag <= contract.max_policy_lag",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_REJECT,
            ),
            execution_pb2.ValidityRule(
                rule_id="ppo-complete-batch",
                description="The committed PPO batch must contain accepted samples",
                expression="batch.accepted_samples > 0",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_PAUSE,
            ),
        )


class GRPOAdapter(AlgorithmAdapter):
    """GRPO grouping semantics without interpreting reward values."""

    name = "grpo"

    def phases(self) -> tuple[execution_pb2.Phase, ...]:
        return (
            _phase(
                "reference",
                "Reference log probabilities",
                execution_pb2.PHASE_KIND_REFERENCE,
                component="trainer",
            ),
            _phase(
                "reward",
                "Group reward evaluation",
                execution_pb2.PHASE_KIND_REWARD,
                component="trainer",
            ),
            _phase(
                "actor",
                "GRPO actor update",
                execution_pb2.PHASE_KIND_ACTOR,
                component="trainer",
            ),
            _phase(
                "optimizer",
                "Optimizer step",
                execution_pb2.PHASE_KIND_OPTIMIZER,
                component="trainer",
            ),
            _phase(
                "weight-sync",
                "Publish policy weights",
                execution_pb2.PHASE_KIND_WEIGHT_SYNC,
                component="policy",
            ),
        )

    def validity_rules(self) -> tuple[execution_pb2.ValidityRule, ...]:
        return (
            execution_pb2.ValidityRule(
                rule_id="grpo-policy-window",
                description="Every sample in a GRPO group must remain in the policy window",
                expression="group.max_policy_lag <= contract.max_policy_lag",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_REJECT,
            ),
            execution_pb2.ValidityRule(
                rule_id="grpo-group-complete",
                description="A GRPO group is committed only when its membership is complete",
                expression="group.accepted_samples == group.expected_samples",
                failure_mode=execution_pb2.VALIDITY_FAILURE_MODE_PAUSE,
            ),
        )
