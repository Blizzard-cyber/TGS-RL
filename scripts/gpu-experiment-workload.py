#!/usr/bin/env python3
"""Representative single-node CUDA workload for the E5 and E6 experiments."""

from __future__ import annotations

import argparse
import importlib.metadata
import math
import os
import time
from pathlib import Path

import torch

from adapters.frameworks.verl_runtime import install_verl_control_from_environment


class ExperimentTrainer:
    def __init__(self, *, matrix_size: int, resident_memory_mib: int) -> None:
        self.global_steps = 1
        self.actor_rollout_wg = self
        self.critic_wg = self
        self.checkpoint_manager = self
        self.use_critic = False
        self.replay_buffer = type("ReplayBuffer", (), {"partitions": {}})()
        self._matrix_size = matrix_size
        resident_elements = resident_memory_mib * 1024 * 1024 // 4
        self._resident_tensor = torch.randn(resident_elements, dtype=torch.float32, device="cuda")
        self._weight = torch.randn((matrix_size, matrix_size), dtype=torch.float32, device="cuda")
        torch.cuda.synchronize()

    def save_checkpoint(self, path: str, _remote: object, step: int) -> None:
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        torch.save(
            {
                "global_steps": step,
                "resident_tensor": self._resident_tensor.detach().cpu(),
                "weight": self._weight.detach().cpu(),
            },
            target / "experiment-state.pt",
        )

    def load_checkpoint(self, path: str, **_kwargs: object) -> None:
        checkpoint = Path(path) / "experiment-state.pt"
        if not checkpoint.is_file():
            raise RuntimeError(f"experiment checkpoint is missing: {checkpoint}")
        payload = torch.load(checkpoint, map_location="cpu", weights_only=True)
        self.global_steps = int(payload["global_steps"])
        self._resident_tensor = payload["resident_tensor"].to("cuda")
        self._weight = payload["weight"].to("cuda")
        torch.cuda.synchronize()

    def to(self, device: str, **_kwargs: object) -> None:
        if device != "cpu":
            raise RuntimeError(f"experiment trainer only supports offload to CPU, got {device}")
        torch.cuda.synchronize()
        self._resident_tensor = self._resident_tensor.to("cpu")
        self._weight = self._weight.to("cpu")
        torch.cuda.empty_cache()

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

    def tgsrl_resource_metrics(self) -> dict[str, int]:
        return {
            "gpu_memory_allocated_bytes": int(torch.cuda.memory_allocated()),
            "gpu_memory_reserved_bytes": int(torch.cuda.memory_reserved()),
        }

    def step(self) -> float:
        started = time.perf_counter_ns()
        activation = torch.randn(
            (self._matrix_size, self._matrix_size), dtype=torch.float32, device="cuda"
        )
        output = activation @ self._weight
        torch.cuda.synchronize()
        duration_ms = (time.perf_counter_ns() - started) / 1_000_000
        del activation, output
        return duration_ms


def _verify_versions() -> None:
    expected = {
        "verl": "0.9.0",
        "ray": "2.58.0",
        "torch": "2.13.0",
        "vllm": "0.28.0",
    }
    actual = {name: importlib.metadata.version(name) for name in expected}
    mismatches = {
        name: (version, actual[name])
        for name, version in expected.items()
        if actual[name].split("+", 1)[0] != version
    }
    if mismatches:
        detail = ", ".join(
            f"{name}={observed} (expected {wanted})"
            for name, (wanted, observed) in sorted(mismatches.items())
        )
        raise SystemExit("workload dependency versions do not match the locked profile: " + detail)


def _initial_delay(schedule: str, serial_slot_seconds: float) -> float:
    if schedule == "variant":
        return 0.0
    replica_index = int(os.environ.get("TGSRL_REPLICA_INDEX", "0"))
    replica_count = int(os.environ.get("TGSRL_REPLICA_COUNT", "1"))
    if replica_index < 0 or replica_count <= 0 or replica_index >= replica_count:
        raise SystemExit("TGSRL_REPLICA_INDEX and TGSRL_REPLICA_COUNT are invalid")
    return replica_index * serial_slot_seconds


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("interference", "lifecycle"), required=True)
    parser.add_argument("--schedule", choices=("baseline", "variant"), default="variant")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--matrix-size", type=int, default=1024)
    parser.add_argument("--minimum-run-seconds", type=float, default=10.0)
    parser.add_argument("--iteration-delay-seconds", type=float, default=0.0)
    parser.add_argument("--serial-slot-seconds", type=float, default=20.0)
    parser.add_argument("--resident-memory-mib", type=int, default=256)
    parser.add_argument("--checkpoint-root", default="/tmp/tgsrl/checkpoints")
    args = parser.parse_args()
    positive = (
        args.iterations,
        args.matrix_size,
        args.resident_memory_mib,
    )
    if any(value <= 0 for value in positive):
        raise SystemExit("iterations, matrix-size, and resident-memory-mib must be positive")
    for value in (
        args.minimum_run_seconds,
        args.iteration_delay_seconds,
        args.serial_slot_seconds,
    ):
        if not math.isfinite(value) or value < 0:
            raise SystemExit("workload durations must be finite and non-negative")
    if args.mode == "interference" and args.serial_slot_seconds <= args.minimum_run_seconds:
        raise SystemExit("serial-slot-seconds must exceed minimum-run-seconds")
    if not torch.cuda.is_available():
        raise SystemExit("CUDA is unavailable in the managed workload")
    _verify_versions()
    replica_index = int(os.environ.get("TGSRL_REPLICA_INDEX", "0"))
    torch.manual_seed(args.seed + replica_index)
    torch.cuda.manual_seed_all(args.seed + replica_index)

    trainer = ExperimentTrainer(
        matrix_size=args.matrix_size,
        resident_memory_mib=args.resident_memory_mib,
    )
    trainer.message_queue_client = trainer
    hook = install_verl_control_from_environment(
        trainer,
        checkpoint_root=args.checkpoint_root,
        verify_version=True,
    )
    hook.start()
    if args.mode == "interference":
        time.sleep(_initial_delay(args.schedule, args.serial_slot_seconds))
    started = time.perf_counter_ns()
    completed = 0
    try:
        while (
            completed < args.iterations
            or (time.perf_counter_ns() - started) / 1_000_000_000 < args.minimum_run_seconds
        ):
            duration_ms = trainer.step()
            completed += 1
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
            if args.iteration_delay_seconds > 0:
                time.sleep(args.iteration_delay_seconds)
            if hook.should_stop():
                break
        hook.bridge.record_workload_completed(
            item_count=completed,
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
