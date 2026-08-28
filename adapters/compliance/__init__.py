"""Shared adapter capability and launch-contract validation."""

from adapters.compliance.runtime import (
    AdapterSupport,
    AdapterUnavailableError,
    LaunchSpec,
    ManifestValidationError,
    RuntimeAdapterError,
    SupportReport,
)

__all__ = [
    "AdapterSupport",
    "AdapterUnavailableError",
    "LaunchSpec",
    "ManifestValidationError",
    "RuntimeAdapterError",
    "SupportReport",
]
