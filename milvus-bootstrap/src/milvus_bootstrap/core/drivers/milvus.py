"""MilvusDriver — install Milvus via milvus-operator (operator-cr), with all
dependencies (etcd / minio / woodpecker) EXTERNAL.

This is the 连 side of 装/连分离: Milvus installs NO dependency pods; it points
at the already-provisioned etcd + object storage + woodpecker LogStore service.
Requires the milvus-operator `feat/external-woodpecker` change (the
`dependencies.woodpecker.external` field). Reuses the operator-cr path
(Milvus is itself a CR: milvus.io/v1beta1 Milvus).
"""
from __future__ import annotations

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

        manifests.append({
            "apiVersion": f"{cr.group}/{cr.version}",
            "kind": cr.kind,
            "metadata": {"name": name, "namespace": ns},
            "spec": {
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
            },
        })
        return manifests
