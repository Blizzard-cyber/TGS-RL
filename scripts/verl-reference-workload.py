#!/usr/bin/env python3
"""Deterministic process-level workload for the first-party veRL bridge.

This is a conformance workload, not a model-quality or GPU-performance claim.
It executes real queue, checkpoint, lifecycle, and optional CUDA operations and
emits raw NDJSON events. Gate metrics are computed by ``gate-tools.py`` from
those events instead of being supplied by this process.
"""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import hashlib
import json
import random
import socket
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

from adapters.frameworks.verl_bridge import (
    ReferenceCallbacks,
    VerlWorkerBridge,
    WorkerIdentity,
    request_worker,
)


def _cpu_step(seed: int, item: int) -> None:
    payload = f"{seed}:{item}".encode()
    for _ in range(128):
        payload = hashlib.sha256(payload).digest()


def _cuda_step(seed: int, item: int) -> float:
    del seed
    library = ctypes.util.find_library("cudart") or "libcudart.so"
    try:
        cudart = ctypes.CDLL(library)
    except OSError as error:
        raise RuntimeError("CUDA workload requires a loadable CUDA runtime") from error
    cudart.cudaGetDeviceCount.argtypes = [ctypes.POINTER(ctypes.c_int)]
    cudart.cudaGetDeviceCount.restype = ctypes.c_int
    cudart.cudaMalloc.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t]
    cudart.cudaMalloc.restype = ctypes.c_int
    cudart.cudaMemset.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_size_t]
    cudart.cudaMemset.restype = ctypes.c_int
    cudart.cudaDeviceSynchronize.argtypes = []
    cudart.cudaDeviceSynchronize.restype = ctypes.c_int
    cudart.cudaFree.argtypes = [ctypes.c_void_p]
    cudart.cudaFree.restype = ctypes.c_int
    device_count = ctypes.c_int()
    if cudart.cudaGetDeviceCount(ctypes.byref(device_count)) != 0 or device_count.value <= 0:
        raise RuntimeError("CUDA workload requires at least one visible CUDA device")
    allocation = ctypes.c_void_p()
    byte_count = 64 * 1024 * 1024
    if cudart.cudaMalloc(ctypes.byref(allocation), byte_count) != 0:
        raise RuntimeError("CUDA reference allocation failed")
    started = time.perf_counter_ns()
    try:
        if cudart.cudaMemset(allocation, item % 251, byte_count) != 0:
            raise RuntimeError("CUDA reference operation failed")
        if cudart.cudaDeviceSynchronize() != 0:
            raise RuntimeError("CUDA reference synchronization failed")
        return (time.perf_counter_ns() - started) / 1_000_000
    finally:
        cudart.cudaFree(allocation)


def _request(bridge: VerlWorkerBridge, action: str, index: int) -> None:
    started = time.perf_counter_ns()
    response = request_worker(
        bridge.socket_path,
        {
            "action": action,
            "sandbox_id": bridge.identity.sandbox_id,
            "generation": bridge.identity.generation,
            "idempotency_key": f"reference:{action}:{index}",
        },
    )
    bridge.record_action(
        action,
        duration_ms=(time.perf_counter_ns() - started) / 1_000_000,
        succeeded=bool(response.get("accepted")),
    )
    if not response.get("accepted"):
        raise RuntimeError(str(response.get("error", f"{action} failed")))


def execute(args: argparse.Namespace) -> list[dict[str, Any]]:
    randomizer = random.Random(args.seed + args.iteration)
    with tempfile.TemporaryDirectory(prefix="tgsrl-verl-reference-", dir="/tmp") as directory:
        root = Path(directory)
        trace_path = root / "events.ndjson"
        bridge = VerlWorkerBridge(
            socket_path=root / "worker.sock",
            trace_path=trace_path,
            identity=WorkerIdentity(
                run_id=f"gate-{args.label}",
                job_id="gate-g-i",
                trace_id=f"gate-{args.label}-{args.phase}-{args.iteration}",
                sandbox_id=f"{args.label}-worker",
                role="rollout-learner",
                generation=1,
                policy_version="policy-1",
            ),
            callbacks=ReferenceCallbacks(root / "checkpoints"),
        )
        server = threading.Thread(target=bridge.serve_forever)
        server.start()
        deadline = time.monotonic() + 2
        while not bridge.socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not bridge.socket_path.exists():
            bridge.shutdown()
            server.join(timeout=2)
            raise RuntimeError("veRL reference worker did not create its control socket")
        queue: deque[tuple[int, int]] = deque()
        policy_revision = 1
        workload_started = time.perf_counter_ns()
        for item in range(args.items):
            produced_version = max(1, policy_revision - randomizer.randint(0, 2))
            queue.append((item, produced_version))
            bridge.observe(
                event_type="sample_produced",
                buffer_level=len(queue),
                policy_lag=policy_revision - produced_version,
                sample_stale=False,
                effective_sample_size=1.0,
                accepted_samples=1,
                expected_samples=1,
            )
            sample, sample_version = queue.popleft()
            started = time.perf_counter_ns()
            gpu_active_ms = 0.0
            if args.device == "cuda":
                gpu_active_ms = _cuda_step(args.seed, sample)
            else:
                _cpu_step(args.seed, sample)
            duration_ms = (time.perf_counter_ns() - started) / 1_000_000
            lag = policy_revision - sample_version
            bridge.observe(
                event_type="sample_consumed",
                buffer_level=len(queue),
                policy_lag=lag,
                sample_stale=lag > 2,
                effective_sample_size=max(0.1, 1.0 - lag * 0.1),
                accepted_samples=1,
                expected_samples=1,
                duration_ms=duration_ms,
                gpu_active_ms=gpu_active_ms,
            )
            if (item + 1) % 8 == 0:
                policy_revision += 1
                bridge.identity.policy_version = f"policy-{policy_revision}"
        if args.control_mode == "tgsrl":
            for index, action in enumerate(
                ("prepare_pause", "checkpoint", "offload", "reload", "resume")
            ):
                _request(bridge, action, index)
        try:
            bridge.record_workload_completed(
                item_count=args.items,
                elapsed_ms=(time.perf_counter_ns() - workload_started) / 1_000_000,
                queue_depth=len(queue),
            )
            events = [json.loads(line) for line in trace_path.read_text().splitlines()]
        finally:
            bridge.shutdown()
            server.join(timeout=2)
        if server.is_alive():
            raise RuntimeError("veRL reference worker did not stop")
    for event in events:
        event["label"] = args.label
        event["phase"] = args.phase
        event["iteration"] = args.iteration
        event["device"] = args.device
        event["node_id"] = hashlib.sha256(socket.gethostname().encode()).hexdigest()[:16]
    return events


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", choices=("baseline", "variant"), required=True)
    parser.add_argument("--phase", choices=("warmup", "measurement"), required=True)
    parser.add_argument("--iteration", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--items", type=int, default=64)
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    parser.add_argument("--control-mode", choices=("static", "tgsrl"), required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.iteration <= 0 or args.items <= 0:
        raise SystemExit("iteration and items must be positive")
    for event in execute(args):
        print(json.dumps(event, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
