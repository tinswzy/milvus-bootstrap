"""WoodpeckerDriver — operator-cr install via the Woodpecker Operator.

Builds the WoodpeckerCluster CR + a configRef ConfigMap carrying the etcd /
object-storage endpoints (the operator does NOT manage those — they must be
supplied). Reuses the base operator-cr install/apply/wait path.
"""
from __future__ import annotations

import yaml

from .base import BaseServiceDriver


class WoodpeckerDriver(BaseServiceDriver):
    def build_install_manifests(self, spec, m, params) -> list[dict]:
        ns, name, cr = spec.namespace, spec.name, m.cr
        cfg_name = f"{name}-config"

        etcd_eps = params.get("etcdEndpoints", ["etcd.default.svc:2379"])
        if isinstance(etcd_eps, str):
            etcd_eps = [etcd_eps]

        wp_conf = {
            "woodpecker": {
                "meta": {"type": "etcd"},
                "storage": {"type": "service", "rootPath": "/woodpecker/data"},
            },
            "log": {"level": "info", "format": "json", "stdout": True},
            "etcd": {"endpoints": etcd_eps, "rootPath": params.get("etcdRootPath", "by-dev")},
            "minio": {
                "address": params.get("minioAddress", "minio.default.svc"),
                "port": int(params.get("minioPort", 9000)),
                "accessKeyID": params.get("minioAccessKey", "minioadmin"),
                "secretAccessKey": params.get("minioSecretKey", "minioadmin"),
                "bucketName": params.get("minioBucket", "woodpecker"),
                "rootPath": params.get("minioRootPath", "files"),
            },
        }
        configmap = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": cfg_name, "namespace": ns},
            "data": {"woodpecker.yaml": yaml.safe_dump(wp_conf, allow_unicode=True, sort_keys=False)},
        }
        cluster = {
            "apiVersion": f"{cr.group}/{cr.version}",
            "kind": cr.kind,
            "metadata": {"name": name, "namespace": ns},
            "spec": {
                "image": params.get("image", "zilliztech/woodpecker:v0.1.26"),
                "replicas": int(params.get("replicas", 3)),
                "storageSize": str(params.get("storageSize", "10Gi")),
                "servicePort": int(params.get("servicePort", 18080)),
                "gossipPort": int(params.get("gossipPort", 17946)),
                "metricsPort": int(params.get("metricsPort", 9091)),
                "configRef": {"name": cfg_name},
            },
        }
        return [configmap, cluster]

    def scale_plan(self, current: int, target: int) -> str:
        if target < current:
            return f"woodpecker {current}→{target}：缩容前 operator 自动 decommission（flush 段到对象存储）"
        return f"woodpecker {current}→{target}：扩容，新节点经 gossip 自动入群"
