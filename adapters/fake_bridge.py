"""Deterministic lifecycle bridge for the local fake runtime."""

from adapters.control import CommandResult, ControlRequest


def handle_lifecycle(request: ControlRequest) -> CommandResult:
    """Return a stable success record without external side effects."""
    return CommandResult(
        exit_code=0,
        stdout=(
            f"{request.component}:{request.adapter}:{request.action.value}:"
            f"{request.run_id}:{request.job_id}:{request.trace_id}"
        ),
    )
