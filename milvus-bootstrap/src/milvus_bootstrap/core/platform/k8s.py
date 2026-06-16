"""K8sAdapter — real Kubernetes target. STUB for the vertical slice.

Mutating ops raise NotImplementedError until wired to the kubernetes client +
a helm subprocess bridge (next increment). Importable without the optional
``kubernetes`` dependency installed.
"""
from __future__ import annotations

from typing import Any

from .base import PlatformAdapter


class K8sAdapter(PlatformAdapter):
    name = "k8s"

    def __init__(self) -> None:
        # Lazy import so the package works without the [k8s] extra.
        try:
            import kubernetes  # noqa: F401
            self._available = True
        except Exception:
            self._available = False

    def discover_native(self) -> list[dict[str, Any]]:
        raise NotImplementedError("K8sAdapter.discover_native: 待实现（扫 STS/Deploy/CR + label）")

    def plan_apply(self, *, kind, name, namespace, method, chart, params):
        return f"[k8s] 经 {method} 安装 {kind}/{name}（chart={chart}）— 待接 helm/kubectl"

    def apply_workload(self, *, kind, name, namespace, method, chart, params):
        raise NotImplementedError("K8sAdapter.apply_workload: 待接 helm/动态客户端")

    def wait_ready(self, *, kind, name, namespace, check):
        raise NotImplementedError("K8sAdapter.wait_ready: 待接 rollout status + 集群级体检")

    def delete_workload(self, *, kind, name, namespace):
        raise NotImplementedError("K8sAdapter.delete_workload: 待实现")
