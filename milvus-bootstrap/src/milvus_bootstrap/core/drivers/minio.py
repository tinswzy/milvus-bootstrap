"""MinioDriver — operator-cr install via the MinIO Operator (Tenant CRD).

Builds the Tenant CR (+ its config Secret); the base driver's operator-cr path
applies them and waits on the Tenant status. Overrides the component-specific
bits (pool-based scaling).
"""
from __future__ import annotations

from .base import BaseServiceDriver

_CONFIG_ENV = 'export MINIO_ROOT_USER="minioadmin"\nexport MINIO_ROOT_PASSWORD="minioadmin"\n'


class MinioDriver(BaseServiceDriver):
    def build_install_manifests(self, spec, m, params) -> list[dict]:
        ns, name, cr = spec.namespace, spec.name, m.cr
        cfg_name = f"{name}-env"
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": cfg_name, "namespace": ns},
            "type": "Opaque",
            "stringData": {"config.env": _CONFIG_ENV},
        }
        tenant = {
            "apiVersion": f"{cr.group}/{cr.version}",
            "kind": cr.kind,
            "metadata": {"name": name, "namespace": ns},
            "spec": {
                "image": params.get("image", "quay.io/minio/minio:latest"),
                "configuration": {"name": cfg_name},
                "requestAutoCert": False,
                "pools": [{
                    "name": "pool-0",
                    "servers": int(params.get("servers", 4)),
                    "volumesPerServer": int(params.get("volumesPerServer", 1)),
                    "volumeClaimTemplate": {
                        "metadata": {"name": "data"},
                        "spec": {
                            "accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": str(params.get("volumeSize", "500Gi"))}},
                        },
                    },
                }],
            },
        }
        return [secret, tenant]

    def replicas_param(self) -> str:
        return "servers"

    def scale_plan(self, current: int, target: int) -> str:
        return ("MinIO 不能原地缩；扩容=往 spec.pools[] 追加新 pool；"
                "下线需先 mc admin decommission 迁移数据")
