"""Trainer runtime adapters."""

from adapters.trainers.base import BaseTrainerAdapter, FakeTrainerAdapter
from adapters.trainers.pytorch import PyTorchTrainerAdapter

__all__ = [
    "BaseTrainerAdapter",
    "FakeTrainerAdapter",
    "PyTorchTrainerAdapter",
]
