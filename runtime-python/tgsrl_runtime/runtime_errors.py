"""Shared runtime error types used across supervisor transport and app modules."""


class RuntimeLifecycleError(ValueError):
    """Raised when a runtime command violates lifecycle rules."""
