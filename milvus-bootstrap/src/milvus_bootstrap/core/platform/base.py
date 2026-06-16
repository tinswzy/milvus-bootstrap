"""PlatformAdapter interface — per-platform polymorphism (L4).

Engines/drivers are platform-agnostic: a driver expresses *platform-neutral
intent* ("ensure this workload exists", "wait until ready per this check") and
the adapter realises it on the concrete platform (k8s helm/CR, docker, ...).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class PlatformAdapter(ABC):
    name: str = "platform"

    @abstractmethod
    def discover_native(self) -> list[dict[str, Any]]:
        """Return raw evidence dicts for every candidate workload on this target."""

    @abstractmethod
    def plan_apply(self, *, kind: str, name: str, namespace: str,
                   method: str, chart: str | None, params: dict[str, Any]) -> str:
        """Human-readable description of what apply_workload *would* do (dry-run)."""

    @abstractmethod
    def apply_workload(self, *, kind: str, name: str, namespace: str,
                       method: str, chart: str | None, params: dict[str, Any]) -> str:
        """Actually create/update the workload. Returns a short detail string."""

    @abstractmethod
    def wait_ready(self, *, kind: str, name: str, namespace: str, check: str) -> str:
        """Block until ready per the given check. Returns a short detail string."""

    @abstractmethod
    def delete_workload(self, *, kind: str, name: str, namespace: str) -> str: ...
