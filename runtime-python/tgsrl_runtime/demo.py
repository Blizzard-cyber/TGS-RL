"""Deterministic fixture output and a minimal live scheduler round trip."""

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta

from google.protobuf import json_format
from google.protobuf.message import Message
from tgsrl.v1 import execution_pb2, resource_pb2, scheduling_pb2, trace_pb2

from adapters import GRPOAdapter, PartialAsyncRolloutAdapter, build_execution_contract
from tgsrl_runtime.intent import IntentBuilder
from tgsrl_runtime.scheduler_client import SchedulerClient
from tgsrl_runtime.synthetic import SyntheticScenario, SyntheticWorkload

_DEFAULT_SEED = 2025
_DEFAULT_TIMEOUT_SECONDS = 10.0


def _rollout_mode_name(mode: int) -> str:
    return {
        trace_pb2.ROLLOUT_MODE_SYNC: "sync",
        trace_pb2.ROLLOUT_MODE_PARTIALLY_ASYNC: "partially_async",
        trace_pb2.ROLLOUT_MODE_FULLY_ASYNC: "fully_async",
    }[mode]


def _capabilities(algorithm: str, mode: int) -> resource_pb2.CapabilitySet:
    return resource_pb2.CapabilitySet(
        names=["logical-cpu"],
        algorithms=[algorithm],
        rollout_modes=[_rollout_mode_name(mode)],
        source="mock",
        revision=1,
        supported_actions=["bind"],
    )


def _build_fixture(
    seed: int, now: datetime, *, trace_base_time: datetime | None = None
) -> dict[str, Message]:
    if now.tzinfo is None:
        raise ValueError("fixture time must be timezone-aware")
    now = now.astimezone(UTC)
    mode = PartialAsyncRolloutAdapter()
    algorithm = GRPOAdapter()
    contract = build_execution_contract(algorithm, mode)
    workload = SyntheticWorkload(
        seed,
        algorithm=algorithm.name,
        rollout_mode=mode.proto_value,
        base_time=trace_base_time or datetime(2025, 1, 1, tzinfo=UTC),
    )
    batch = workload.generate_batch(SyntheticScenario.TOOL_WAIT)
    intent = IntentBuilder(clock=lambda: now).build(
        execution_id=batch.execution_id,
        stage_id="decode",
        job_id=batch.events[0].job_id,
        contract=contract,
        rollout_mode=mode.proto_value,
        phase_kind=execution_pb2.PHASE_KIND_DECODE,
        policy_version="policy-1",
        ttl=timedelta(seconds=60),
        resources_per_unit=resource_pb2.ResourceVector(cpu_millis=1000, memory_bytes=1_073_741_824),
        required_capabilities=_capabilities(algorithm.name, mode.proto_value),
        deterministic_seed=seed,
        labels={"data_kind": "synthetic"},
    )
    return {"execution_contract": contract, "scheduling_intent": intent, "trace": batch}


def build_demo_fixture(seed: int = _DEFAULT_SEED) -> dict[str, Message]:
    """Build the stable offline fixture with a deliberately fixed timestamp."""
    return _build_fixture(seed, datetime(2025, 1, 1, 1, tzinfo=UTC))


def build_live_fixture(
    seed: int = _DEFAULT_SEED, *, now: datetime | None = None
) -> dict[str, Message]:
    """Build an unexpired fixture using one captured current UTC instant."""
    captured_now = now or datetime.now(tz=UTC)
    return _build_fixture(seed, captured_now, trace_base_time=captured_now)


def _render_messages(messages: dict[str, Message]) -> str:
    rendered = {
        key: json_format.MessageToDict(
            messages[key],
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        for key in sorted(messages)
    }
    return json.dumps(rendered, indent=2, sort_keys=True) + "\n"


def render_demo_fixture(seed: int = _DEFAULT_SEED) -> str:
    """Render stable, human-readable Proto JSON suitable for a golden fixture."""
    return _render_messages(build_demo_fixture(seed))


async def run_live_demo(
    target: str,
    *,
    seed: int = _DEFAULT_SEED,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Message]:
    """Publish one current intent and await its first scheduler decision."""
    if not target.strip():
        raise ValueError("target is required")
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    fixture = build_live_fixture(seed)
    intent = fixture["scheduling_intent"]
    if not isinstance(intent, scheduling_pb2.SchedulingIntent):  # pragma: no cover
        raise TypeError("live fixture contains an invalid intent")
    async with SchedulerClient(target, timeout=timeout) as client:
        publish_response = await client.publish_intent(intent)
        async with asyncio.timeout(timeout):
            decision = await anext(
                client.watch_decisions(job_ids=(intent.job_id,), heartbeat_seconds=1.0)
            )
    return {"publish_response": publish_response, "decision": decision}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", help="scheduler gRPC target, for example localhost:50051")
    parser.add_argument("--seed", type=int, default=_DEFAULT_SEED)
    parser.add_argument("--timeout", type=float, default=_DEFAULT_TIMEOUT_SECONDS)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.target:
        print(
            _render_messages(
                asyncio.run(run_live_demo(args.target, seed=args.seed, timeout=args.timeout))
            ),
            end="",
        )
    else:
        print(render_demo_fixture(args.seed), end="")


if __name__ == "__main__":
    main()
