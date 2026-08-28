"""Python SDK for the northbound gateway."""

from __future__ import annotations

import json
from collections.abc import Mapping
from urllib import parse, request

from tgsrl_gateway.contracts import route_path


class GatewayClient:
    """Minimal dependency-free client for the gateway API."""

    def __init__(self, base_url: str, *, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, object] | None = None,
        json_body: Mapping[str, object] | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        url = f"{self.base_url}{path}"
        if query:
            filtered = {
                key: value for key, value in query.items() if value is not None and value != ""
            }
            if filtered:
                url = f"{url}?{parse.urlencode(filtered)}"
        headers = {"Accept": "application/json"}
        data: bytes | None = None
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        if request_id:
            headers["X-Request-Id"] = request_id
        req = request.Request(url, data=data, headers=headers, method=method)
        with request.urlopen(req, timeout=self.timeout) as response:
            payload = response.read()
        return json.loads(payload.decode("utf-8"))

    def health(self) -> dict[str, object]:
        return self._request("GET", route_path("health"))

    def openapi(self) -> dict[str, object]:
        return self._request("GET", route_path("openapi"))

    def capabilities(self) -> dict[str, object]:
        return self._request("GET", route_path("capabilities"))

    def list_jobs(
        self,
        *,
        limit: int | None = None,
        page_token: str | None = None,
        after_job_id: str | None = None,
        data_kind: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_jobs"),
            query={
                "limit": limit,
                "page_token": page_token,
                "after_job_id": after_job_id,
                "data_kind": data_kind,
            },
        )

    def create_job(
        self,
        job: dict[str, object],
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            route_path("create_job"),
            json_body=job,
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def validate_job(
        self,
        job: dict[str, object],
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            route_path("validate_job"),
            json_body=job,
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def get_job(self, job_id: str) -> dict[str, object]:
        return self._request("GET", route_path("get_job", job_id=job_id))

    def admit_job(
        self,
        job_id: str,
        *,
        reason: str = "job admitted",
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            route_path("admit_job", job_id=job_id),
            json_body={"reason": reason},
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def create_run(
        self,
        job_id: str,
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            route_path("create_run", job_id=job_id),
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def list_runs(
        self,
        job_id: str,
        *,
        limit: int | None = None,
        page_token: str | None = None,
        after_run_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_runs", job_id=job_id),
            query={"limit": limit, "page_token": page_token, "after_run_id": after_run_id},
        )

    def get_run(self, job_id: str, run_id: str) -> dict[str, object]:
        return self._request("GET", route_path("get_run", job_id=job_id, run_id=run_id))

    def apply_job_command(
        self,
        job_id: str,
        run_id: str,
        command: str,
        *,
        actor: str = "sdk",
        reason: str | None = None,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        body = {"actor": actor, "reason": reason or command}
        return self._request(
            "POST",
            route_path("apply_job_command", job_id=job_id, run_id=run_id, command=command),
            json_body=body,
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def list_timeline(
        self,
        job_id: str,
        *,
        run_id: str | None = None,
        after_event_id: str | None = None,
        page_token: str | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_timeline", job_id=job_id),
            query={
                "run_id": run_id,
                "after_event_id": after_event_id,
                "page_token": page_token,
                "limit": limit,
            },
        )

    def get_dag(self, job_id: str, *, run_id: str | None = None) -> dict[str, object]:
        return self._request("GET", route_path("get_dag", job_id=job_id), query={"run_id": run_id})

    def get_topology(self, job_id: str, *, run_id: str | None = None) -> dict[str, object]:
        return self._request(
            "GET", route_path("get_topology", job_id=job_id), query={"run_id": run_id}
        )

    def list_sandboxes(
        self,
        job_id: str,
        *,
        run_id: str | None = None,
        limit: int | None = None,
        page_token: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_sandboxes", job_id=job_id),
            query={"run_id": run_id, "limit": limit, "page_token": page_token},
        )

    def list_decisions(
        self,
        job_id: str,
        *,
        run_id: str | None = None,
        limit: int | None = None,
        page_token: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_decisions", job_id=job_id),
            query={"run_id": run_id, "limit": limit, "page_token": page_token},
        )

    def get_decision(self, job_id: str, decision_id: str) -> dict[str, object]:
        return self._request(
            "GET", route_path("get_decision", job_id=job_id, decision_id=decision_id)
        )

    def list_operations(
        self,
        *,
        job_id: str | None = None,
        run_id: str | None = None,
        operation_type: str | None = None,
        state: str | None = None,
        limit: int | None = None,
        page_token: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_operations"),
            query={
                "job_id": job_id,
                "run_id": run_id,
                "type": operation_type,
                "state": state,
                "limit": limit,
                "page_token": page_token,
            },
        )

    def get_operation(self, operation_id: str) -> dict[str, object]:
        return self._request("GET", route_path("get_operation", operation_id=operation_id))

    def list_replays(
        self,
        *,
        limit: int | None = None,
        page_token: str | None = None,
        after_replay_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_replays"),
            query={
                "limit": limit,
                "page_token": page_token,
                "after_replay_id": after_replay_id,
            },
        )

    def create_replay(
        self,
        replay: dict[str, object],
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            route_path("create_replay"),
            json_body=replay,
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def get_replay(self, replay_id: str) -> dict[str, object]:
        return self._request("GET", route_path("get_replay", replay_id=replay_id))

    def apply_replay_command(
        self,
        replay_id: str,
        command: str,
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            route_path("apply_replay_command", replay_id=replay_id, command=command),
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def list_experiments(
        self,
        *,
        limit: int | None = None,
        page_token: str | None = None,
        after_experiment_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "GET",
            route_path("list_experiments"),
            query={
                "limit": limit,
                "page_token": page_token,
                "after_experiment_id": after_experiment_id,
            },
        )

    def create_experiment(
        self,
        experiment: dict[str, object],
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, object]:
        return self._request(
            "POST",
            route_path("create_experiment"),
            json_body=experiment,
            idempotency_key=idempotency_key,
            request_id=request_id,
        )

    def get_experiment(self, experiment_id: str) -> dict[str, object]:
        return self._request("GET", route_path("get_experiment", experiment_id=experiment_id))
