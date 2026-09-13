#!/usr/bin/env python3
"""Minimal real-CUDA workload for the first TGS-RL hardware smoke."""

from __future__ import annotations

import argparse
import importlib.metadata
import math
import time

import torch

from adapters.frameworks.verl_runtime import install_verl_control_from_environment


class SmokeTrainer:
    """Small callback surface that exercises the real veRL control adapter."""

    def __init__(self) -> None:
        self.global_steps = 1
        self.actor_rollout_wg = self
        self.critic_wg = self
        self.checkpoint_manager = self
        self.use_critic = False
        self.replay_buffer = type("ReplayBuffer", (), {"partitions": {}})()

    def save_checkpoint(self, path: str, _remote: object, _step: int) -> None:
        from pathlib import Path

        Path(path).mkdir(parents=True, exist_ok=True)
        (Path(path) / "smoke.checkpoint").write_text("ready\n", encoding="utf-8")

    def get_queue_size_sync(self) -> int:
        return 0

    def load_checkpoint(self, _path: str, **_kwargs: object) -> None:
        return None

    def to(self, _device: str, **_kwargs: object) -> None:
        return None

    def abort_replicas(self) -> None:
        return None

    def sleep_replicas(self) -> None:
        return None

    def wake_up_replicas(self) -> None:
        return None

    def resume_generation_replicas(self) -> None:
        return None

    def update_weights(self, _global_steps: int) -> None:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--matrix-size", type=int, default=512)
    parser.add_argument("--iteration-delay-seconds", type=float, default=0.0)
    parser.add_argument("--checkpoint-root", default="/tmp/tgsrl/checkpoints")
    args = parser.parse_args()
    if args.iterations <= 0 or args.matrix_size <= 0:
        raise SystemExit("iterations and matrix-size must be positive")
    if not math.isfinite(args.iteration_delay_seconds) or args.iteration_delay_seconds < 0:
        raise SystemExit("iteration-delay-seconds must be finite and non-negative")
    if not torch.cuda.is_available():
        raise SystemExit("CUDA is unavailable in the managed workload")
    expected_versions = {
        "verl": "0.9.0",
        "ray": "2.58.0",
        "torch": "2.13.0",
        "vllm": "0.28.0",
    }
    actual_versions = {name: importlib.metadata.version(name) for name in expected_versions}
    mismatches = {
        name: (expected, actual_versions[name])
        for name, expected in expected_versions.items()
        if actual_versions[name].split("+", 1)[0] != expected
    }
    if mismatches:
        detail = ", ".join(
            f"{name}={actual} (expected {expected})"
            for name, (expected, actual) in sorted(mismatches.items())
        )
        raise SystemExit("workload dependency versions do not match the locked profile: " + detail)

    trainer = SmokeTrainer()
    trainer.message_queue_client = trainer
    hook = install_verl_control_from_environment(
        trainer, checkpoint_root=args.checkpoint_root, verify_version=True
    )
    hook.start()
    started = time.perf_counter_ns()
    try:
        for _iteration in range(args.iterations):
            step_started = time.perf_counter_ns()
            left = torch.randn((args.matrix_size, args.matrix_size), device="cuda")
            right = torch.randn((args.matrix_size, args.matrix_size), device="cuda")
            result = left @ right
            torch.cuda.synchronize()
            duration_ms = (time.perf_counter_ns() - step_started) / 1_000_000
            hook.safe_point(
                event_type="sample_consumed",
                duration_ms=duration_ms,
                gpu_active_ms=duration_ms,
                items=1,
                policy_lag=0,
                sample_stale=False,
                effective_sample_size=1.0,
                accepted_samples=1,
                expected_samples=1,
            )
            del left, right, result
            if args.iteration_delay_seconds > 0:
                time.sleep(args.iteration_delay_seconds)
            if hook.should_stop():
                break
        hook.bridge.record_workload_completed(
            item_count=args.iterations,
            elapsed_ms=(time.perf_counter_ns() - started) / 1_000_000,
            queue_depth=0,
        )
        while not hook.should_stop():
            hook.safe_point(event_type="safe_point_reached", items=0)
            time.sleep(0.1)
    finally:
        hook.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
