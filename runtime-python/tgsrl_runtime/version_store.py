"""Monotonic revision store for runtime-local products."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class VersionStore:
    """Track independent monotonic counters by namespace."""

    _counters: dict[str, int] = field(default_factory=dict)

    def next(self, key: str) -> int:
        """Advance and return the next revision for one namespace."""
        value = self._counters.get(key, 0) + 1
        self._counters[key] = value
        return value

    def peek(self, key: str) -> int:
        """Return the current revision without incrementing."""
        return self._counters.get(key, 0)

    def snapshot(self) -> dict[str, int]:
        """Return an immutable copy of the revision map."""
        return dict(self._counters)

    def items(self) -> tuple[tuple[str, int], ...]:
        """Return all counters in stable key order."""
        return tuple(sorted(self._counters.items()))

    def restore(
        self,
        counters: dict[str, int] | tuple[tuple[str, int], ...] | list[tuple[str, int]],
        *,
        replace: bool = False,
    ) -> None:
        """Restore counters from public serialized state."""
        restored = dict(counters)
        if any(value < 0 for value in restored.values()):
            raise ValueError("version counters must be non-negative")
        if replace:
            self._counters = restored
            return
        for key, value in restored.items():
            self._counters[key] = value
