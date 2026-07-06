"""MilvusDriver — install Milvus via milvus-operator (operator-cr), all deps external.

The 连 side of 装/连分离: Milvus installs NO dependency pods; it points at
already-provisioned etcd + object storage + an MQ. The MQ is selectable
(woodpecker-service / woodpecker-embedded / kafka / pulsar / rocksmq) and gated
by the milvus version compatibility matrix (core/compat.py). Reuses operator-cr
(Milvus is itself a CR: milvus.io/v1beta1 Milvus).
"""
from __future__ import annotations

import json

from .. import compat
from ..tasks.engine import Step
from .base import BaseServiceDriver

WOODPECKER_SERVICE_PORT = 18080  # woodpecker LogStore gRPC port


def woodpecker_seeds(name: str, replicas: int, namespace: str,
                     port: int = WOODPECKER_SERVICE_PORT) -> list[str]:
    """LogStore quorum seeds, deterministic from a WoodpeckerCluster name:
    <name>-server-<i>.<name>-server-headless.<ns>.svc:18080"""
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
        image = params.get("image", "milvusdb/milvus:v3.0.0")
        mode = params.get("mode", "standalone")
        mq = params.get("mq", "woodpecker-service")
        # gate the MQ choice on the milvus version — raises if not selectable
        compat.check(mq, image, mode)

        etcd_eps = _as_list(params.get("etcdEndpoints"), [f"etcd.{ns}.svc:2379"])
        storage_endpoint = params.get("storageEndpoint", f"minio.{ns}.svc:9000")

        manifests: list[dict] = []
        secret_ref = params.get("storageSecretRef")
        if not secret_ref:
            secret_ref = f"{name}-minio"
            manifests.append({
                "apiVersion": "v1", "kind": "Secret",
                "metadata": {"name": secret_ref, "namespace": ns}, "type": "Opaque",
                "stringData": {
                    "accesskey": params.get("storageAccessKey", "minioadmin"),
                    "secretkey": params.get("storageSecretKey", "minioadmin"),
                },
            })

        deps = {
            "etcd": {"external": True, "endpoints": etcd_eps},
            "storage": {"external": True, "type": "MinIO",
                        "endpoint": storage_endpoint, "secretRef": secret_ref},
        }
        deps.update(self._mq_deps(mq, params, ns))

        components: dict = {"image": image}
        # ★ External MinIO endpoint MUST reach Milvus as a single host:port string.
        # The operator splits `storage.endpoint` into separate minio.address + minio.port
        # fields; Milvus's segcore "init vector storage" then appends bucketName onto the
        # endpoint (host:port/bucket), which minio-go rejects — "Endpoint url cannot have
        # fully qualified paths." → CrashLoopBackOff. Milvus reads MINIO_ADDRESS with
        # precedence and keeps it verbatim (colon-form), sidestepping the split. The
        # operator does not manage this env, so the override sticks. Only inject when the
        # endpoint carries an explicit port (real AWS S3 endpoints are portless & fine).
        if storage_endpoint and ":" in storage_endpoint:
            components["env"] = [{"name": "MINIO_ADDRESS", "value": storage_endpoint}]

        cr_spec = {"mode": mode, "components": components, "dependencies": deps}
        conf = params.get("_conf")
        if conf:
            cr_spec["conf"] = {"data": conf}

        manifests.append({
            "apiVersion": f"{cr.group}/{cr.version}", "kind": cr.kind,
            "metadata": {"name": name, "namespace": ns}, "spec": cr_spec,
        })
        return manifests

    def _mq_deps(self, mq: str, params: dict, ns: str) -> dict:
        """The dependencies fragment that wires the chosen MQ into the Milvus CR."""
        if mq == "woodpecker-service":
            reps = int(params.get("woodpeckerReplicas", 3))
            eps = params.get("woodpeckerEndpoints")
            eps = _as_list(eps, []) if eps else woodpecker_seeds(
                params.get("woodpeckerName", "woodpecker"), reps, ns)
            return {"msgStreamType": "woodpecker",
                    "woodpecker": {"external": {"endpoints": eps, "replicaCount": reps}}}
        if mq == "woodpecker-embedded":
            return {"msgStreamType": "woodpecker"}  # embedded over external etcd+minio
        if mq == "kafka":
            brokers = _as_list(params.get("kafkaBrokers"), [f"kafka.{ns}.svc:9092"])
            return {"msgStreamType": "kafka", "kafka": {"external": True, "brokerList": brokers}}
        if mq == "pulsar":
            ep = params.get("pulsarEndpoint", f"pulsar.{ns}.svc:6650")
            return {"msgStreamType": "pulsar", "pulsar": {"external": True, "endpoint": ep}}
        if mq == "rocksmq":
            return {"msgStreamType": "rocksmq"}  # embedded, standalone
        raise ValueError(f"未知 MQ 选项：{mq}")

    def plan_switch_mq_steps(self, spec, adapter, target_wal: str) -> list[Step]:
        """Switch Milvus's WAL/MQ at runtime via the management API (the ★ flow)."""
        ns, name = spec.namespace, spec.name
        selector = f"app.kubernetes.io/instance={name}"
        payload = json.dumps({"target_wal_name": target_wal})
        curl = ["curl", "-s", "-X", "POST",
                "http://localhost:9091/management/wal/alter", "-d", payload]
        return [
            Step(name="precheck-target", plan=f"确认目标 MQ（{target_wal}）服务已就位、milvus 可连"),
            Step(name="wal-alter", plan="在 milvus pod 内执行：" + " ".join(curl),
                 action=lambda: adapter.exec(namespace=ns, label_selector=selector, command=curl)),
            Step(name="verify", plan=f"校验 WAL 已切到 {target_wal}、旧 MQ 写入已排空"),
            Step(name="decommission-old", plan="下线旧 MQ（确认无残留写入后删除其资源）"),
        ]

    def config_apply_params(self, params: dict, kv: dict) -> dict:
        # milvus config lives in spec.conf.data, not install params
        merged_conf = {**params.get("_conf", {}), **kv}
        return {**params, "_conf": merged_conf}
