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
    def plan_apply(self, *, kind: str, name: str, namespace: str, method: str,
                   method_kind: str, chart: str | None, params: dict[str, Any]) -> str:
        """Human-readable description of what apply_workload *would* do (dry-run).

        ``method_kind`` is how the install is realised on this platform:
        helm | operator-cr | manifest | compose | external.
        """

    @abstractmethod
    def apply_workload(self, *, kind: str, name: str, namespace: str, method: str,
                       method_kind: str, chart: str | None, params: dict[str, Any]) -> str:
        """Actually create/update the workload. Returns a short detail string."""

    @abstractmethod
    def wait_ready(self, *, kind: str, name: str, namespace: str, check: str) -> str:
        """Block until ready per the given check. Returns a short detail string."""

    @abstractmethod
    def delete_workload(self, *, kind: str, name: str, namespace: str) -> str: ...

    # ---- operator-cr install mechanism (minio Tenant, woodpecker WoodpeckerCluster, ...) ----
    @abstractmethod
    def crd_exists(self, *, group: str, plural: str) -> bool:
        """Is the CRD registered? (i.e. is the operator installed)"""

    @abstractmethod
    def apply_objects(self, *, manifests: list[dict[str, Any]]) -> str:
        """Create/patch arbitrary objects (a config Secret + a CR, etc.)."""

    @abstractmethod
    def wait_cr(self, *, group: str, version: str, plural: str, namespace: str,
                name: str, status_path: str, status_equals: str) -> str:
        """Poll a CR until status_path == status_equals."""

    @abstractmethod
    def delete_cr(self, *, group: str, version: str, plural: str,
                  namespace: str, name: str) -> str: ...

    @abstractmethod
    def exec(self, *, namespace: str, label_selector: str, command: list[str]) -> str:
        """Run a command inside a pod matched by label_selector (e.g. kubectl exec).

        Used by switch-mq to call Milvus's management API from inside the cluster.
        """

    @abstractmethod
    def get_configmap(self, *, namespace: str, name: str) -> dict[str, str]:
        """Return a ConfigMap's data."""

    @abstractmethod
    def restart(self, *, namespace: str, label_selector: str) -> str:
        """Rolling-restart the workloads matched by label_selector."""
