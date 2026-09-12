"""CLI for serving and calling the northbound gateway."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from wsgiref.types import WSGIApplication

from tgsrl_gateway.app import create_app
from tgsrl_gateway.config import GatewayConfig
from tgsrl_gateway.contracts import ROUTES_BY_CLI_COMMAND
from tgsrl_gateway.openapi import build_openapi_spec
from tgsrl_gateway.sdk import GatewayClient


def _load_json_argument(value: str) -> dict[str, object]:
    candidate = Path(value)
    if candidate.exists():
        return json.loads(candidate.read_text(encoding="utf-8"))
    return json.loads(value)


def _print(data: dict[str, object]) -> int:
    print(json.dumps(data, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tgsrl-gateway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--backend-mode", choices=["grpc", "memory"], default=None)
    serve.add_argument("--job-control-target", default=None)
    serve.add_argument("--scheduler-target", default=None)
    serve.add_argument("--runtime-target", default=None)
    serve.add_argument("--experiment-target", default=None)

    openapi = subparsers.add_parser("openapi")
    openapi.add_argument("--output", default="-")

    client_parent = argparse.ArgumentParser(add_help=False)
    client_parent.add_argument("--base-url", default="http://127.0.0.1:8080")

    subparsers.add_parser("health", parents=[client_parent])
    subparsers.add_parser("capabilities", parents=[client_parent])
    subparsers.add_parser("resources", parents=[client_parent])
    list_jobs = subparsers.add_parser("list-jobs", parents=[client_parent])
    list_jobs.add_argument("--limit", type=int)
    list_jobs.add_argument("--page-token")
    list_jobs.add_argument("--after-job-id")
    list_jobs.add_argument("--data-kind")

    create_job = subparsers.add_parser("create-job", parents=[client_parent])
    create_job.add_argument("--job", required=True)
    create_job.add_argument("--idempotency-key")
    create_job.add_argument("--request-id")

    validate_job = subparsers.add_parser("validate-job", parents=[client_parent])
    validate_job.add_argument("--job", required=True)
    validate_job.add_argument("--idempotency-key")
    validate_job.add_argument("--request-id")

    get_job = subparsers.add_parser("get-job", parents=[client_parent])
    get_job.add_argument("job_id")

    admit_job = subparsers.add_parser("admit-job", parents=[client_parent])
    admit_job.add_argument("job_id")
    admit_job.add_argument("--reason", default="job admitted")
    admit_job.add_argument("--idempotency-key")
    admit_job.add_argument("--request-id")

    create_run = subparsers.add_parser("create-run", parents=[client_parent])
    create_run.add_argument("job_id")
    create_run.add_argument("--idempotency-key")
    create_run.add_argument("--request-id")

    list_runs = subparsers.add_parser("list-runs", parents=[client_parent])
    list_runs.add_argument("job_id")
    list_runs.add_argument("--limit", type=int)
    list_runs.add_argument("--page-token")
    list_runs.add_argument("--after-run-id")

    get_run = subparsers.add_parser("get-run", parents=[client_parent])
    get_run.add_argument("job_id")
    get_run.add_argument("run_id")

    command = subparsers.add_parser("job-command", parents=[client_parent])
    command.add_argument("job_id")
    command.add_argument("run_id")
    command.add_argument("job_command")
    command.add_argument("--actor", default="cli")
    command.add_argument("--reason")
    command.add_argument("--idempotency-key")
    command.add_argument("--request-id")

    timeline = subparsers.add_parser("timeline", parents=[client_parent])
    timeline.add_argument("job_id")
    timeline.add_argument("--run-id")
    timeline.add_argument("--after-event-id")
    timeline.add_argument("--page-token")
    timeline.add_argument("--limit", type=int)

    traces = subparsers.add_parser("traces", parents=[client_parent])
    traces.add_argument("job_id")
    traces.add_argument("--run-id")
    traces.add_argument("--trace-id")
    traces.add_argument("--data-kind")
    traces.add_argument("--page-token")
    traces.add_argument("--limit", type=int)

    dag = subparsers.add_parser("dag", parents=[client_parent])
    dag.add_argument("job_id")
    dag.add_argument("--run-id")

    topology = subparsers.add_parser("topology", parents=[client_parent])
    topology.add_argument("job_id")
    topology.add_argument("--run-id")

    sandboxes = subparsers.add_parser("sandboxes", parents=[client_parent])
    sandboxes.add_argument("job_id")
    sandboxes.add_argument("--run-id")
    sandboxes.add_argument("--limit", type=int)
    sandboxes.add_argument("--page-token")

    list_decisions = subparsers.add_parser("list-decisions", parents=[client_parent])
    list_decisions.add_argument("job_id")
    list_decisions.add_argument("--run-id")
    list_decisions.add_argument("--limit", type=int)
    list_decisions.add_argument("--page-token")

    get_decision = subparsers.add_parser("get-decision", parents=[client_parent])
    get_decision.add_argument("job_id")
    get_decision.add_argument("decision_id")

    list_operations = subparsers.add_parser("list-operations", parents=[client_parent])
    list_operations.add_argument("--job-id")
    list_operations.add_argument("--run-id")
    list_operations.add_argument("--type")
    list_operations.add_argument("--state")
    list_operations.add_argument("--limit", type=int)
    list_operations.add_argument("--page-token")

    get_operation = subparsers.add_parser("get-operation", parents=[client_parent])
    get_operation.add_argument("operation_id")

    list_replays = subparsers.add_parser("list-replays", parents=[client_parent])
    list_replays.add_argument("--limit", type=int)
    list_replays.add_argument("--page-token")
    list_replays.add_argument("--after-replay-id")

    create_replay = subparsers.add_parser("create-replay", parents=[client_parent])
    create_replay.add_argument("--replay", required=True)
    create_replay.add_argument("--idempotency-key")
    create_replay.add_argument("--request-id")

    get_replay = subparsers.add_parser("get-replay", parents=[client_parent])
    get_replay.add_argument("replay_id")

    replay_command = subparsers.add_parser("replay-command", parents=[client_parent])
    replay_command.add_argument("replay_id")
    replay_command.add_argument("replay_command_name")
    replay_command.add_argument("--idempotency-key")
    replay_command.add_argument("--request-id")

    list_experiments = subparsers.add_parser("list-experiments", parents=[client_parent])
    list_experiments.add_argument("--limit", type=int)
    list_experiments.add_argument("--page-token")
    list_experiments.add_argument("--after-experiment-id")

    create_experiment = subparsers.add_parser("create-experiment", parents=[client_parent])
    create_experiment.add_argument("--experiment", required=True)
    create_experiment.add_argument("--idempotency-key")
    create_experiment.add_argument("--request-id")

    get_experiment = subparsers.add_parser("get-experiment", parents=[client_parent])
    get_experiment.add_argument("experiment_id")

    unsupported = sorted(set(ROUTES_BY_CLI_COMMAND) - {"openapi"})
    missing = [command for command in unsupported if command not in subparsers.choices]
    if missing:
        raise RuntimeError(f"CLI parser is missing contract commands: {', '.join(missing)}")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "serve":
        from wsgiref.simple_server import make_server

        config = GatewayConfig.from_env().with_overrides(
            backend_mode=args.backend_mode,
            job_control_target=args.job_control_target,
            scheduler_target=args.scheduler_target,
            runtime_target=args.runtime_target,
            experiment_target=args.experiment_target,
        )
        app: WSGIApplication = create_app(config=config).__call__
        with make_server(args.host, args.port, app) as server:
            print(f"gateway listening on http://{args.host}:{args.port}")
            server.serve_forever()
        return 0

    if args.command == "openapi":
        payload = json.dumps(build_openapi_spec(), indent=2, sort_keys=True)
        if args.output == "-":
            print(payload)
        else:
            Path(args.output).write_text(payload + "\n", encoding="utf-8")
        return 0

    client = GatewayClient(args.base_url)
    if args.command == "health":
        return _print(client.health())
    if args.command == "capabilities":
        return _print(client.capabilities())
    if args.command == "resources":
        return _print(client.get_resources())
    if args.command == "list-jobs":
        return _print(
            client.list_jobs(
                limit=args.limit,
                page_token=args.page_token,
                after_job_id=args.after_job_id,
                data_kind=args.data_kind,
            )
        )
    if args.command == "create-job":
        return _print(
            client.create_job(
                _load_json_argument(args.job),
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "validate-job":
        return _print(
            client.validate_job(
                _load_json_argument(args.job),
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "get-job":
        return _print(client.get_job(args.job_id))
    if args.command == "admit-job":
        return _print(
            client.admit_job(
                args.job_id,
                reason=args.reason,
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "create-run":
        return _print(
            client.create_run(
                args.job_id,
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "list-runs":
        return _print(
            client.list_runs(
                args.job_id,
                limit=args.limit,
                page_token=args.page_token,
                after_run_id=args.after_run_id,
            )
        )
    if args.command == "get-run":
        return _print(client.get_run(args.job_id, args.run_id))
    if args.command == "job-command":
        return _print(
            client.apply_job_command(
                args.job_id,
                args.run_id,
                args.job_command,
                actor=args.actor,
                reason=args.reason,
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "timeline":
        return _print(
            client.list_timeline(
                args.job_id,
                run_id=args.run_id,
                after_event_id=args.after_event_id,
                page_token=args.page_token,
                limit=args.limit,
            )
        )
    if args.command == "traces":
        return _print(
            client.list_traces(
                args.job_id,
                run_id=args.run_id,
                trace_id=args.trace_id,
                data_kind=args.data_kind,
                page_token=args.page_token,
                limit=args.limit,
            )
        )
    if args.command == "dag":
        return _print(client.get_dag(args.job_id, run_id=args.run_id))
    if args.command == "topology":
        return _print(client.get_topology(args.job_id, run_id=args.run_id))
    if args.command == "sandboxes":
        return _print(
            client.list_sandboxes(
                args.job_id,
                run_id=args.run_id,
                limit=args.limit,
                page_token=args.page_token,
            )
        )
    if args.command == "list-decisions":
        return _print(
            client.list_decisions(
                args.job_id,
                run_id=args.run_id,
                limit=args.limit,
                page_token=args.page_token,
            )
        )
    if args.command == "get-decision":
        return _print(client.get_decision(args.job_id, args.decision_id))
    if args.command == "list-operations":
        return _print(
            client.list_operations(
                job_id=args.job_id,
                run_id=args.run_id,
                operation_type=args.type,
                state=args.state,
                limit=args.limit,
                page_token=args.page_token,
            )
        )
    if args.command == "get-operation":
        return _print(client.get_operation(args.operation_id))
    if args.command == "list-replays":
        return _print(
            client.list_replays(
                limit=args.limit,
                page_token=args.page_token,
                after_replay_id=args.after_replay_id,
            )
        )
    if args.command == "create-replay":
        return _print(
            client.create_replay(
                _load_json_argument(args.replay),
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "get-replay":
        return _print(client.get_replay(args.replay_id))
    if args.command == "replay-command":
        return _print(
            client.apply_replay_command(
                args.replay_id,
                args.replay_command_name,
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "list-experiments":
        return _print(
            client.list_experiments(
                limit=args.limit,
                page_token=args.page_token,
                after_experiment_id=args.after_experiment_id,
            )
        )
    if args.command == "create-experiment":
        return _print(
            client.create_experiment(
                _load_json_argument(args.experiment),
                idempotency_key=args.idempotency_key,
                request_id=args.request_id,
            )
        )
    if args.command == "get-experiment":
        return _print(client.get_experiment(args.experiment_id))
    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
