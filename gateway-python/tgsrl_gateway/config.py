"""Gateway backend configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from math import isfinite


@dataclass(slots=True)
class GatewayConfig:
    """Configuration for backend selection and gRPC dependency targets."""

    backend_mode: str = "grpc"
    job_control_target: str = "127.0.0.1:50061"
    scheduler_target: str = "127.0.0.1:50051"
    runtime_target: str = "127.0.0.1:50071"
    experiment_target: str = "127.0.0.1:50071"
    grpc_timeout_seconds: float = 2.0
    command_timeout_seconds: float = 30.0
    health_timeout_seconds: float = 0.5
    decisions_timeout_seconds: float = 0.25

    def __post_init__(self) -> None:
        for name in (
            "grpc_timeout_seconds",
            "command_timeout_seconds",
            "health_timeout_seconds",
            "decisions_timeout_seconds",
        ):
            value = getattr(self, name)
            if not isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")

    @classmethod
    def from_env(cls) -> GatewayConfig:
        """Build configuration from environment variables."""
        return cls(
            backend_mode=os.getenv("TGSRL_GATEWAY_BACKEND_MODE", "grpc").strip() or "grpc",
            job_control_target=os.getenv(
                "TGSRL_GATEWAY_JOB_CONTROL_TARGET", "127.0.0.1:50061"
            ).strip(),
            scheduler_target=os.getenv("TGSRL_GATEWAY_SCHEDULER_TARGET", "127.0.0.1:50051").strip(),
            runtime_target=os.getenv("TGSRL_GATEWAY_RUNTIME_TARGET", "127.0.0.1:50071").strip(),
            experiment_target=os.getenv(
                "TGSRL_GATEWAY_EXPERIMENT_TARGET", "127.0.0.1:50071"
            ).strip(),
            grpc_timeout_seconds=float(
                os.getenv("TGSRL_GATEWAY_GRPC_TIMEOUT_SECONDS", "2.0").strip() or "2.0"
            ),
            command_timeout_seconds=float(
                os.getenv("TGSRL_GATEWAY_COMMAND_TIMEOUT_SECONDS", "30.0").strip() or "30.0"
            ),
            health_timeout_seconds=float(
                os.getenv("TGSRL_GATEWAY_HEALTH_TIMEOUT_SECONDS", "0.5").strip() or "0.5"
            ),
            decisions_timeout_seconds=float(
                os.getenv("TGSRL_GATEWAY_DECISIONS_TIMEOUT_SECONDS", "0.25").strip() or "0.25"
            ),
        )

    def with_overrides(
        self,
        *,
        backend_mode: str | None = None,
        job_control_target: str | None = None,
        scheduler_target: str | None = None,
        runtime_target: str | None = None,
        experiment_target: str | None = None,
        grpc_timeout_seconds: float | None = None,
        command_timeout_seconds: float | None = None,
        health_timeout_seconds: float | None = None,
        decisions_timeout_seconds: float | None = None,
    ) -> GatewayConfig:
        """Return a copy with selected fields overridden."""
        return GatewayConfig(
            backend_mode=backend_mode or self.backend_mode,
            job_control_target=job_control_target or self.job_control_target,
            scheduler_target=scheduler_target or self.scheduler_target,
            runtime_target=runtime_target or self.runtime_target,
            experiment_target=experiment_target or self.experiment_target,
            grpc_timeout_seconds=(
                self.grpc_timeout_seconds if grpc_timeout_seconds is None else grpc_timeout_seconds
            ),
            command_timeout_seconds=(
                self.command_timeout_seconds
                if command_timeout_seconds is None
                else command_timeout_seconds
            ),
            health_timeout_seconds=(
                self.health_timeout_seconds
                if health_timeout_seconds is None
                else health_timeout_seconds
            ),
            decisions_timeout_seconds=(
                self.decisions_timeout_seconds
                if decisions_timeout_seconds is None
                else decisions_timeout_seconds
            ),
        )
