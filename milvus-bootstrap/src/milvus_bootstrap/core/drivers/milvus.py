"""MilvusDriver — install Milvus via milvus-operator (operator-cr), with all
dependencies (etcd / minio / woodpecker) EXTERNAL.

This is the 连 side of 装/连分离: Milvus installs NO dependency pods; it points
at the already-provisioned etcd + object storage + woodpecker LogStore service.
Requires the milvus-operator `feat/external-woodpecker` change (the
`dependencies.woodpecker.external` field). Reuses the operator-cr path
(Milvus is itself a CR: milvus.io/v1beta1 Milvus).
"""
from __future__ import annotations

import json

from ..tasks.engine import Step
from .base import BaseServiceDriver

# woodpecker service port (gRPC) — see WoodpeckerCluster / smoke-test.sh
WOODPECKER_SERVICE_PORT = 18080


def woodpecker_seeds(name: str, replicas: int, namespace: str,
                     port: int = WOODPECKER_SERVICE_PORT) -> list[str]:
    """LogStore quorum seeds, deterministic from a WoodpeckerCluster name.

    <name>-server-<i>.<name>-server-headless.<ns>.svc:18080
    """
    return [
        f"{name}-server-{i}.{name}-server-headless.{namespace}.svc:{port}"
        for i in range(int(replicas))
    ]


def _as_list(v, default: list[str]) -> list[str]:
    if not v:
        return default
    if isinstance(v, str):
        return [x.strip() for x in v.split(",") if x.strip()]
    return list(v)


class MilvusDriver(BaseServiceDriver):
    def build_install_manifests(self, spec, m, params) -> list[dict]:
        ns, name, cr = spec.namespace, spec.name, m.cr

        # --- connection info for the three external deps (the 连 side) ---
        etcd_eps = _as_list(params.get("etcdEndpoints"), [f"etcd.{ns}.svc:2379"])
        storage_endpoint = params.get("storageEndpoint", f"minio.{ns}.svc:9000")

        wp_replicas = int(params.get("woodpeckerReplicas", 3))
        wp_eps = params.get("woodpeckerEndpoints")
        if wp_eps:
            wp_eps = _as_list(wp_eps, [])
        else:  # compute seeds from the deployed WoodpeckerCluster name
            wp_name = params.get("woodpeckerName", "woodpecker")
            wp_eps = woodpecker_seeds(wp_name, wp_replicas, ns)

        manifests: list[dict] = []

        # storage secret (accesskey/secretkey) referenced by storage.secretRef
        secret_ref = params.get("storageSecretRef")
        if not secret_ref:
            secret_ref = f"{name}-minio"
            manifests.append({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": secret_ref, "namespace": ns},
                "type": "Opaque",
                "stringData": {
                    "accesskey": params.get("storageAccessKey", "minioadmin"),
                    "secretkey": params.get("storageSecretKey", "minioadmin"),
                },
            })

        cr_spec = {
            "mode": params.get("mode", "standalone"),
            "components": {"image": params.get("image", "milvusdb/milvus:v2.6.0")},
            "dependencies": {
                "msgStreamType": "woodpecker",
                "etcd": {"external": True, "endpoints": etcd_eps},
                "storage": {
                    "external": True, "type": "MinIO",
                    "endpoint": storage_endpoint, "secretRef": secret_ref,
                },
                "woodpecker": {
                    "external": {"endpoints": wp_eps, "replicaCount": wp_replicas},
                },
            },
        }
        # config overrides (from `config set`) go into the Milvus CR spec.conf.data,
        # which milvus-operator merges into milvus.yaml.
        conf = params.get("_conf")
        if conf:
            cr_spec["conf"] = {"data": conf}

        manifests.append({
            "apiVersion": f"{cr.group}/{cr.version}",
            "kind": cr.kind,
            "metadata": {"name": name, "namespace": ns},
            "spec": cr_spec,
        })
        return manifests

    def config_apply_params(self, params: dict, kv: dict) -> dict:
        # milvus config lives in spec.conf.data, not install params
        merged_conf = {**params.get("_conf", {}), **kv}
        return {**params, "_conf": merged_conf}

    def plan_switch_mq_steps(self, spec, adapter, target_wal: str) -> list[Step]:
        """Switch Milvus's WAL/MQ at runtime via the management API (the ★ flow).

        Calls POST :9091/management/wal/alter inside a milvus pod.
        """
        ns, name = spec.namespace, spec.name
        selector = f"app.kubernetes.io/instance={name}"
        payload = json.dumps({"target_wal_name": target_wal})
        curl = ["curl", "-s", "-X", "POST",
                "http://localhost:9091/management/wal/alter", "-d", payload]
        return [
            Step(name="precheck-target",
                 plan=f"确认目标 MQ（{target_wal}）服务已就位、milvus 可连"),
            Step(name="wal-alter",
                 plan="在 milvus pod 内执行：" + " ".join(curl),
                 action=lambda: adapter.exec(namespace=ns, label_selector=selector, command=curl)),
            Step(name="verify",
                 plan=f"校验 WAL 已切到 {target_wal}、旧 MQ 写入已排空"),
            Step(name="decommission-old",
                 plan="下线旧 MQ（确认无残留写入后删除其资源）"),
        ]
