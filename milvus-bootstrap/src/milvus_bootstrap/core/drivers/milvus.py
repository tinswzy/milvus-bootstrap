"""MilvusDriver — install Milvus via milvus-operator (operator-cr), all deps external.

The 连 side of 装/连分离: Milvus installs NO dependency pods; it points at
already-provisioned etcd + object storage + an MQ. The MQ is selectable
(woodpecker-service / woodpecker-embedded / kafka / pulsar / rocksmq) and gated
by the milvus version compatibility matrix (core/compat.py). Reuses operator-cr
(Milvus is itself a CR: milvus.io/v1beta1 Milvus).
"""
from __future__ import annotations

import json
import time

from .. import compat
from ..tasks.engine import Step
from .base import BaseServiceDriver

WOODPECKER_SERVICE_PORT = 18080  # woodpecker LogStore gRPC port


def _dotted_to_nested(flat: dict) -> dict:
    """{'a.b.c': v} -> {'a': {'b': {'c': v}}}; keys without '.' are kept as-is."""
    out: dict = {}
    for k, v in (flat or {}).items():
        parts = str(k).split(".")
        cur = out
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = v
    return out


def _deep_merge(a: dict, b: dict) -> dict:
    """Recursive dict merge; b wins on scalar conflicts."""
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


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
        # compat gate is enforced upstream in provisioner.install(); no redundant check here

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
        n = name
        iso = {
            "etcd": {"rootPath": params.get("etcdRootPath") or n},
            "minio": {"bucketName": params.get("minioBucket") or n,
                      "rootPath": params.get("minioRootPath") or n},
            "msgChannel": {"chanNamePrefix": {"cluster": params.get("mqChanPrefix") or n}},
        }
        config = _deep_merge(_dotted_to_nested(params.get("_conf") or {}), iso)
        if config:
            cr_spec["config"] = config

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

    def _mq_conn_conf(self, target_wal: str, endpoint: str) -> dict:
        """目标 MQ 的原生 milvus 连接配置（dotted key，注入 _conf → spec.config）。

        只加连接、绝不含 msgStreamType —— 让源/目标 MQ 连接在 milvus user.yaml 里并存，
        msgStreamType 保持源；运行时切由 wal/alter 完成。切换流程专用（区别于装机的 _mq_deps）。
        """
        if target_wal == "kafka":
            return {"kafka.brokerList": endpoint}                 # 字符串 host:port
        if target_wal == "pulsar":
            host, _, port = endpoint.partition(":")
            return {"pulsar.address": f"pulsar://{host}", "pulsar.port": int(port or 6650)}
        return {}   # rocksmq / woodpecker-embedded：内嵌，无外部连接

    def _wal_to_mq_id(self, wal: str) -> str:
        return {"kafka": "kafka", "pulsar": "pulsar", "rocksmq": "rocksmq",
                "woodpecker": "woodpecker-embedded"}.get(wal, wal)

    def _verify_wal(self, adapter, ns, selector, target_wal, tries=20, sleep_s=3) -> str:
        """Bounded poll of milvus's own current WAL until == target (honest, no over-claim).
        Fake adapter echoes '[fake] …' → treated as simulated-pass; real k8s checks the response."""
        read = ["curl", "-s", "http://localhost:9091/management/wal/status"]  # exact path: confirm in live DoD
        for _ in range(tries):
            out = str(adapter.exec(namespace=ns, label_selector=selector, command=read))
            if target_wal in out or out.strip().startswith("[fake]"):
                return f"已确认当前 WAL == {target_wal}（{out.strip()[:120]}）"
            time.sleep(sleep_s)
        raise TimeoutError(f"切换后未在 {tries * sleep_s}s 内确认 WAL == {target_wal}")

    def plan_switch_mq_steps(self, spec, adapter, target_wal: str) -> list[Step]:
        """Real switch: apply new MQ+endpoint into CR (render+apply+wait) → wal/alter → verify."""
        ns, name = spec.namespace, spec.name
        selector = f"app.kubernetes.io/instance={name}"
        steps = list(self.plan_install_steps(spec, adapter))       # render + apply-objects + wait-ready (spec has new mq+endpoint)
        # CRITICAL: strip the install steps' compensate. plan_install_steps' apply-cr rolls back via
        # delete_cr — correct for a fresh install, but here the Milvus CR PRE-EXISTS. A later step
        # failing (e.g. verify-mq-type TimeoutError) must NOT roll back by deleting the running instance.
        for s in steps:
            s.compensate = None
        alter = ["curl", "-s", "-X", "POST", "http://localhost:9091/management/wal/alter",
                 "-d", json.dumps({"target_wal_name": target_wal})]
        steps.append(Step(name="wal-alter", plan="在 milvus pod 内执行：" + " ".join(alter),
                          action=lambda: adapter.exec(namespace=ns, label_selector=selector, command=alter)))
        steps.append(Step(name="verify-mq-type",
                          plan=f"轮询 milvus 当前 WAL 直到 == {target_wal}（有界·超时）",
                          action=lambda: self._verify_wal(adapter, ns, selector, target_wal)))
        return steps

    def config_apply_params(self, params: dict, kv: dict) -> dict:
        # milvus config lives in spec.config (nested) — see build_install_manifests;
        # _conf holds dotted-flat keys, converted via _dotted_to_nested. Not install params.
        merged_conf = {**params.get("_conf", {}), **kv}
        return {**params, "_conf": merged_conf}
