#!/usr/bin/env python3
"""CPU managed workload used by the full-stack Gate integration path.

The process embeds the veRL 0.9 callback adapter around structural trainer
doubles. It proves bootstrap, registry, socket, safe-point, checkpoint, pause,
resume, and stop wiring without claiming a real veRL package or GPU run.
"""

from __future__ import annotations

import argparse
import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from adapters.frameworks.verl_runtime import install_verl_control_from_environment


@dataclass
class _WorkerGroup:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = field(default_factory=list)

    def save_checkpoint(self, path: str, remote: object, step: int) -> None:
        self.calls.append(("save_checkpoint", (path, remote, step), {}))
        Path(path).mkdir(parents=True, exist_ok=True)

    def load_checkpoint(self, path: str, **kwargs: Any) -> None:
        self.calls.append(("load_checkpoint", (path,), kwargs))

    def to(self, device: str, **kwargs: Any) -> None:
        self.calls.append(("to", (device,), kwargs))


@dataclass
class _CheckpointManager:
    calls: list[str] = field(default_factory=list)

    def abort_replicas(self) -> None:
        self.calls.append("abort_replicas")

    def sleep_replicas(self) -> None:
        self.calls.append("sleep_replicas")

    def wake_up_replicas(self) -> None:
        self.calls.append("wake_up_replicas")

    def resume_generation_replicas(self) -> None:
        self.calls.append("resume_generation_replicas")

    def update_weights(self, global_steps: int) -> None:
        self.calls.append(f"update_weights:{global_steps}")


@dataclass
class _Trainer:
    global_steps: int
    actor_rollout_wg: _WorkerGroup = field(default_factory=_WorkerGroup)
    critic_wg: _WorkerGroup = field(default_factory=_WorkerGroup)
    checkpoint_manager: _CheckpointManager = field(default_factory=_CheckpointManager)
    use_critic: bool = False
    replay_buffer: object = field(
        default_factory=lambda: type("ReplayBuffer", (), {"partitions": {}})()
    )


def _cpu_step(seed: int, item: int) -> float:
    payload = f"{seed}:{item}".encode()
    started = time.perf_counter_ns()
    for _ in range(128):
        payload = hashlib.sha256(payload).digest()
    return (time.perf_counter_ns() - started) / 1_000_000


def execute(args: argparse.Namespace) -> None:
    trainer = _Trainer(global_steps=args.iteration)
    hook = install_verl_control_from_environment(
        trainer,
        checkpoint_root=Path(args.checkpoint_root),
        safe_point_timeout_seconds=10.0,
        verify_version=False,
    )
    hook.start()
    started = time.perf_counter_ns()
    try:
        time.sleep(args.startup_delay_seconds)
        for item in range(args.items):
            duration_ms = _cpu_step(args.seed, item)
            hook.safe_point(
                event_type="sample_consumed",
                duration_ms=duration_ms,
                items=1,
                policy_lag=item % 2,
                sample_stale=False,
                effective_sample_size=1.0,
                accepted_samples=1,
                expected_samples=1,
            )
            time.sleep(args.item_delay_seconds)
            if hook.should_stop():
                break
        hook.bridge.record_workload_completed(
            item_count=args.items,
            elapsed_ms=(time.perf_counter_ns() - started) / 1_000_000,
            queue_depth=0,
        )
        while not hook.should_stop():
            hook.safe_point(event_type="safe_point_reached", items=0)
            time.sleep(0.05)
    finally:
        hook.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--items", type=int, required=True)
    parser.add_argument("--iteration", type=int, required=True)
    parser.add_argument("--checkpoint-root", required=True)
    parser.add_argument("--startup-delay-seconds", type=float, default=0.5)
    parser.add_argument("--item-delay-seconds", type=float, default=0.005)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if (
        args.items <= 0
        or args.iteration <= 0
        or args.startup_delay_seconds < 0
        or args.item_delay_seconds < 0
    ):
        raise SystemExit("items and iteration must be positive; delays must be non-negative")
    execute(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
