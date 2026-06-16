"""In-memory FakeAdapter — lets the whole chain run end-to-end without a cluster.

It ships a small fake "cluster" so `discover` returns realistic evidence
(including a control-plane etcd that must be excluded), and records applies so
`install` works in both dry-run and real mode.
"""
from __future__ import annotations

from typing import Any

from .base import PlatformAdapter

_FAKE_CLUSTER: list[dict[str, Any]] = [
    {
        "platform": "k8s",
        "name": "milvus-etcd",
        "namespace": "default",
        "image": "milvusdb/etcd:3.5.18-r1",
        "ports": [2379, 2380],
        "labels": {
            "app.kubernetes.io/managed-by": "Helm",
            "helm.sh/chart": "etcd-6.3.3",
            "app.kubernetes.io/instance": "milvus",
        },
    },
    {
        "platform": "k8s",
        "name": "etcd",                       # control-plane — MUST be excluded
        "namespace": "kube-system",
        "image": "registry.k8s.io/etcd:3.5.12-0",
        "ports": [2379, 2380],
        "labels": {"component": "etcd", "tier": "control-plane"},
    },
]


class FakeAdapter(PlatformAdapter):
    name = "fake"

    def __init__(self) -> None:
        self.applied: list[dict[str, Any]] = []

    def discover_native(self) -> list[dict[str, Any]]:
        return [dict(w) for w in _FAKE_CLUSTER]

    def plan_apply(self, *, kind, name, namespace, method, chart, params):
        where = chart or method
        return f"[fake] 经 {method} 安装 {kind}/{name} 到 ns={namespace}（{where}, params={params}）"

    def apply_workload(self, *, kind, name, namespace, method, chart, params):
        self.applied.append({"kind": kind, "name": name, "namespace": namespace, "method": method})
        return f"[fake] applied {kind}/{name} via {method}"

    def wait_ready(self, *, kind, name, namespace, check):
        return f"[fake] {kind}/{name} ready ({check})"

    def delete_workload(self, *, kind, name, namespace):
        return f"[fake] deleted {kind}/{name}"
