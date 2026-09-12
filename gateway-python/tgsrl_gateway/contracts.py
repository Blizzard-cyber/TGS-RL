"""Single-source route contracts for the northbound gateway surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

HttpMethod = Literal["GET", "POST"]
ParameterLocation = Literal["path", "query", "header"]


@dataclass(frozen=True, slots=True)
class ContractParameter:
    name: str
    location: ParameterLocation
    schema: dict[str, object]
    required: bool = False


@dataclass(frozen=True, slots=True)
class RouteContract:
    name: str
    method: HttpMethod
    path_template: str
    tag: str
    summary: str
    operation_id: str
    success_status: int
    success_description: str
    cli_command: str | None = None
    sdk_method: str | None = None
    parameters: tuple[ContractParameter, ...] = ()
    has_request_body: bool = False
    request_body_required: bool = False
    error_statuses: tuple[int, ...] = ()

    @property
    def path_parameters(self) -> tuple[ContractParameter, ...]:
        return tuple(parameter for parameter in self.parameters if parameter.location == "path")

    @property
    def query_parameters(self) -> tuple[ContractParameter, ...]:
        return tuple(parameter for parameter in self.parameters if parameter.location == "query")

    @property
    def header_parameters(self) -> tuple[ContractParameter, ...]:
        return tuple(parameter for parameter in self.parameters if parameter.location == "header")


def _path_parameter(name: str) -> ContractParameter:
    return ContractParameter(
        name=name,
        location="path",
        required=True,
        schema={"type": "string"},
    )


def _query_parameter(
    name: str, *, type_name: str = "string", minimum: int | None = None
) -> ContractParameter:
    schema: dict[str, object] = {"type": type_name}
    if minimum is not None:
        schema["minimum"] = minimum
    return ContractParameter(name=name, location="query", schema=schema)


IDEMPOTENCY_HEADER = ContractParameter(
    name="Idempotency-Key",
    location="header",
    schema={"type": "string"},
)

REQUEST_ID_HEADER = ContractParameter(
    name="X-Request-Id",
    location="header",
    schema={"type": "string"},
)


ROUTE_CONTRACTS: tuple[RouteContract, ...] = (
    RouteContract(
        name="health",
        method="GET",
        path_template="/health",
        tag="system",
        summary="Health",
        operation_id="getHealth",
        success_status=200,
        success_description="OK",
        cli_command="health",
        sdk_method="health",
    ),
    RouteContract(
        name="openapi",
        method="GET",
        path_template="/openapi.json",
        tag="system",
        summary="OpenAPI",
        operation_id="getOpenapi",
        success_status=200,
        success_description="OpenAPI document",
        sdk_method="openapi",
    ),
    RouteContract(
        name="capabilities",
        method="GET",
        path_template="/v1/capabilities",
        tag="system",
        summary="Capabilities",
        operation_id="getCapabilities",
        success_status=200,
        success_description="Capabilities",
        cli_command="capabilities",
        sdk_method="capabilities",
    ),
    RouteContract(
        name="get_resources",
        method="GET",
        path_template="/v1/resources",
        tag="resources",
        summary="Get accelerator resources",
        operation_id="getResources",
        success_status=200,
        success_description="Scheduler resource snapshot",
        cli_command="resources",
        sdk_method="get_resources",
    ),
    RouteContract(
        name="list_jobs",
        method="GET",
        path_template="/v1/jobs",
        tag="jobs",
        summary="List jobs",
        operation_id="listJobs",
        success_status=200,
        success_description="Jobs",
        cli_command="list-jobs",
        sdk_method="list_jobs",
        parameters=(
            _query_parameter("limit", type_name="integer", minimum=1),
            _query_parameter("page_token"),
            _query_parameter("after_job_id"),
            _query_parameter("data_kind"),
        ),
    ),
    RouteContract(
        name="create_job",
        method="POST",
        path_template="/v1/jobs",
        tag="jobs",
        summary="Create job",
        operation_id="createJob",
        success_status=201,
        success_description="Created",
        cli_command="create-job",
        sdk_method="create_job",
        parameters=(IDEMPOTENCY_HEADER, REQUEST_ID_HEADER),
        has_request_body=True,
        request_body_required=True,
        error_statuses=(400, 409),
    ),
    RouteContract(
        name="validate_job",
        method="POST",
        path_template="/v1/jobs/validate",
        tag="jobs",
        summary="Validate job",
        operation_id="validateJob",
        success_status=200,
        success_description="Validation",
        cli_command="validate-job",
        sdk_method="validate_job",
        parameters=(IDEMPOTENCY_HEADER, REQUEST_ID_HEADER),
        has_request_body=True,
        request_body_required=True,
        error_statuses=(400,),
    ),
    RouteContract(
        name="get_job",
        method="GET",
        path_template="/v1/jobs/{job_id}",
        tag="jobs",
        summary="Get job",
        operation_id="getJob",
        success_status=200,
        success_description="Job",
        cli_command="get-job",
        sdk_method="get_job",
        parameters=(_path_parameter("job_id"),),
        error_statuses=(404,),
    ),
    RouteContract(
        name="admit_job",
        method="POST",
        path_template="/v1/jobs/{job_id}/admit",
        tag="jobs",
        summary="Admit job",
        operation_id="admitJob",
        success_status=200,
        success_description="Admitted",
        cli_command="admit-job",
        sdk_method="admit_job",
        parameters=(_path_parameter("job_id"), IDEMPOTENCY_HEADER, REQUEST_ID_HEADER),
        has_request_body=True,
        error_statuses=(404,),
    ),
    RouteContract(
        name="list_runs",
        method="GET",
        path_template="/v1/jobs/{job_id}/runs",
        tag="jobs",
        summary="List runs",
        operation_id="listRuns",
        success_status=200,
        success_description="Runs",
        cli_command="list-runs",
        sdk_method="list_runs",
        parameters=(
            _path_parameter("job_id"),
            _query_parameter("limit", type_name="integer", minimum=1),
            _query_parameter("page_token"),
            _query_parameter("after_run_id"),
        ),
        error_statuses=(404,),
    ),
    RouteContract(
        name="create_run",
        method="POST",
        path_template="/v1/jobs/{job_id}/runs",
        tag="jobs",
        summary="Create run",
        operation_id="createRun",
        success_status=201,
        success_description="Run",
        cli_command="create-run",
        sdk_method="create_run",
        parameters=(_path_parameter("job_id"), IDEMPOTENCY_HEADER, REQUEST_ID_HEADER),
        error_statuses=(404,),
    ),
    RouteContract(
        name="get_run",
        method="GET",
        path_template="/v1/jobs/{job_id}/runs/{run_id}",
        tag="jobs",
        summary="Get run",
        operation_id="getRun",
        success_status=200,
        success_description="Run",
        cli_command="get-run",
        sdk_method="get_run",
        parameters=(_path_parameter("job_id"), _path_parameter("run_id")),
        error_statuses=(404,),
    ),
    RouteContract(
        name="apply_job_command",
        method="POST",
        path_template="/v1/jobs/{job_id}/runs/{run_id}/commands/{command}",
        tag="jobs",
        summary="Apply job command",
        operation_id="applyJobCommand",
        success_status=200,
        success_description="Run mutation",
        cli_command="job-command",
        sdk_method="apply_job_command",
        parameters=(
            _path_parameter("job_id"),
            _path_parameter("run_id"),
            _path_parameter("command"),
            IDEMPOTENCY_HEADER,
            REQUEST_ID_HEADER,
        ),
        has_request_body=True,
        error_statuses=(400, 404),
    ),
    RouteContract(
        name="list_timeline",
        method="GET",
        path_template="/v1/jobs/{job_id}/timeline",
        tag="jobs",
        summary="List timeline",
        operation_id="listTimeline",
        success_status=200,
        success_description="Timeline",
        cli_command="timeline",
        sdk_method="list_timeline",
        parameters=(
            _path_parameter("job_id"),
            _query_parameter("run_id"),
            _query_parameter("after_event_id"),
            _query_parameter("page_token"),
            _query_parameter("limit", type_name="integer", minimum=1),
        ),
        error_statuses=(404,),
    ),
    RouteContract(
        name="list_traces",
        method="GET",
        path_template="/v1/jobs/{job_id}/traces",
        tag="traces",
        summary="List trace events",
        operation_id="listTraceEvents",
        success_status=200,
        success_description="Trace events",
        cli_command="traces",
        sdk_method="list_traces",
        parameters=(
            _path_parameter("job_id"),
            _query_parameter("run_id"),
            _query_parameter("trace_id"),
            _query_parameter("data_kind"),
            _query_parameter("page_token"),
            _query_parameter("limit", type_name="integer", minimum=1),
        ),
        error_statuses=(400, 404),
    ),
    RouteContract(
        name="get_dag",
        method="GET",
        path_template="/v1/jobs/{job_id}/dag",
        tag="jobs",
        summary="Get DAG",
        operation_id="getDag",
        success_status=200,
        success_description="DAG",
        cli_command="dag",
        sdk_method="get_dag",
        parameters=(_path_parameter("job_id"), _query_parameter("run_id")),
        error_statuses=(404,),
    ),
    RouteContract(
        name="get_topology",
        method="GET",
        path_template="/v1/jobs/{job_id}/topology",
        tag="runtime",
        summary="Get topology",
        operation_id="getTopology",
        success_status=200,
        success_description="Topology",
        cli_command="topology",
        sdk_method="get_topology",
        parameters=(_path_parameter("job_id"), _query_parameter("run_id")),
        error_statuses=(404,),
    ),
    RouteContract(
        name="list_sandboxes",
        method="GET",
        path_template="/v1/jobs/{job_id}/sandboxes",
        tag="runtime",
        summary="List sandboxes",
        operation_id="listSandboxes",
        success_status=200,
        success_description="Sandboxes",
        cli_command="sandboxes",
        sdk_method="list_sandboxes",
        parameters=(
            _path_parameter("job_id"),
            _query_parameter("run_id"),
            _query_parameter("limit", type_name="integer", minimum=1),
            _query_parameter("page_token"),
        ),
        error_statuses=(404,),
    ),
    RouteContract(
        name="list_decisions",
        method="GET",
        path_template="/v1/jobs/{job_id}/decisions",
        tag="decisions",
        summary="List decisions",
        operation_id="listDecisions",
        success_status=200,
        success_description="Decisions",
        cli_command="list-decisions",
        sdk_method="list_decisions",
        parameters=(
            _path_parameter("job_id"),
            _query_parameter("run_id"),
            _query_parameter("limit", type_name="integer", minimum=1),
            _query_parameter("page_token"),
        ),
        error_statuses=(404,),
    ),
    RouteContract(
        name="get_decision",
        method="GET",
        path_template="/v1/jobs/{job_id}/decisions/{decision_id}",
        tag="decisions",
        summary="Get decision",
        operation_id="getDecision",
        success_status=200,
        success_description="Decision",
        cli_command="get-decision",
        sdk_method="get_decision",
        parameters=(_path_parameter("job_id"), _path_parameter("decision_id")),
        error_statuses=(404,),
    ),
    RouteContract(
        name="list_operations",
        method="GET",
        path_template="/v1/operations",
        tag="operations",
        summary="List operations",
        operation_id="listOperations",
        success_status=200,
        success_description="Operations",
        cli_command="list-operations",
        sdk_method="list_operations",
        parameters=(
            _query_parameter("job_id"),
            _query_parameter("run_id"),
            _query_parameter("type"),
            _query_parameter("state"),
            _query_parameter("limit", type_name="integer", minimum=1),
            _query_parameter("page_token"),
        ),
    ),
    RouteContract(
        name="get_operation",
        method="GET",
        path_template="/v1/operations/{operation_id}",
        tag="operations",
        summary="Get operation",
        operation_id="getOperation",
        success_status=200,
        success_description="Operation",
        cli_command="get-operation",
        sdk_method="get_operation",
        parameters=(_path_parameter("operation_id"),),
        error_statuses=(404,),
    ),
    RouteContract(
        name="list_replays",
        method="GET",
        path_template="/v1/replays",
        tag="replays",
        summary="List replays",
        operation_id="listReplays",
        success_status=200,
        success_description="Replays",
        cli_command="list-replays",
        sdk_method="list_replays",
        parameters=(
            _query_parameter("limit", type_name="integer", minimum=1),
            _query_parameter("page_token"),
            _query_parameter("after_replay_id"),
        ),
    ),
    RouteContract(
        name="create_replay",
        method="POST",
        path_template="/v1/replays",
        tag="replays",
        summary="Create replay",
        operation_id="createReplay",
        success_status=201,
        success_description="Replay",
        cli_command="create-replay",
        sdk_method="create_replay",
        parameters=(IDEMPOTENCY_HEADER, REQUEST_ID_HEADER),
        has_request_body=True,
        request_body_required=True,
        error_statuses=(409,),
    ),
    RouteContract(
        name="get_replay",
        method="GET",
        path_template="/v1/replays/{replay_id}",
        tag="replays",
        summary="Get replay",
        operation_id="getReplay",
        success_status=200,
        success_description="Replay",
        cli_command="get-replay",
        sdk_method="get_replay",
        parameters=(_path_parameter("replay_id"),),
        error_statuses=(404,),
    ),
    RouteContract(
        name="apply_replay_command",
        method="POST",
        path_template="/v1/replays/{replay_id}/commands/{command}",
        tag="replays",
        summary="Apply replay command",
        operation_id="applyReplayCommand",
        success_status=200,
        success_description="Replay mutation",
        cli_command="replay-command",
        sdk_method="apply_replay_command",
        parameters=(
            _path_parameter("replay_id"),
            _path_parameter("command"),
            IDEMPOTENCY_HEADER,
            REQUEST_ID_HEADER,
        ),
        error_statuses=(404,),
    ),
    RouteContract(
        name="list_experiments",
        method="GET",
        path_template="/v1/experiments",
        tag="experiments",
        summary="List experiments",
        operation_id="listExperiments",
        success_status=200,
        success_description="Experiments",
        cli_command="list-experiments",
        sdk_method="list_experiments",
        parameters=(
            _query_parameter("limit", type_name="integer", minimum=1),
            _query_parameter("page_token"),
            _query_parameter("after_experiment_id"),
        ),
    ),
    RouteContract(
        name="create_experiment",
        method="POST",
        path_template="/v1/experiments",
        tag="experiments",
        summary="Create experiment",
        operation_id="createExperiment",
        success_status=201,
        success_description="Experiment",
        cli_command="create-experiment",
        sdk_method="create_experiment",
        parameters=(IDEMPOTENCY_HEADER, REQUEST_ID_HEADER),
        has_request_body=True,
        request_body_required=True,
        error_statuses=(409,),
    ),
    RouteContract(
        name="get_experiment",
        method="GET",
        path_template="/v1/experiments/{experiment_id}",
        tag="experiments",
        summary="Get experiment",
        operation_id="getExperiment",
        success_status=200,
        success_description="Experiment",
        cli_command="get-experiment",
        sdk_method="get_experiment",
        parameters=(_path_parameter("experiment_id"),),
        error_statuses=(404,),
    ),
)

ROUTES_BY_NAME = {route.name: route for route in ROUTE_CONTRACTS}
ROUTES_BY_SDK_METHOD = {
    route.sdk_method: route for route in ROUTE_CONTRACTS if route.sdk_method is not None
}
ROUTES_BY_CLI_COMMAND = {
    route.cli_command: route for route in ROUTE_CONTRACTS if route.cli_command is not None
}


def route_path(name: str, /, **params: str) -> str:
    return ROUTES_BY_NAME[name].path_template.format(**params)


def all_route_paths() -> list[str]:
    return [route.path_template for route in ROUTE_CONTRACTS]


def sdk_method_names() -> list[str]:
    return [route.sdk_method for route in ROUTE_CONTRACTS if route.sdk_method is not None]


def cli_command_names() -> list[str]:
    return [route.cli_command for route in ROUTE_CONTRACTS if route.cli_command is not None]
