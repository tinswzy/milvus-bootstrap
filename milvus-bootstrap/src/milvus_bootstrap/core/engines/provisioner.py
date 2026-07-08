"""Provisioner (generic) — install a component via its driver + the Task engine."""
from __future__ import annotations

from ..models import DependencyBinding, InstallSpec, Instance, Ownership, Task
from ..platform.base import PlatformAdapter
from ..registry import DriverRegistry
from ..state.base import StateStore
from ..tasks.engine import Step, TaskEngine


def _dep_ep_sets(params: dict) -> dict:
    """Per-dependency endpoint sets the milvus install binds to."""
    def _as_set(v):
        if isinstance(v, (list, tuple)):
            return {str(e) for e in v}
        return {str(v)} if v else set()
    return {
        "etcd": _as_set(params.get("etcdEndpoints")),
        "minio": _as_set(params.get("storageEndpoint")),
        "mq": _as_set(params.get("kafkaBrokers")) | _as_set(params.get("pulsarEndpoint")),
    }


def _iso_of(params: dict, name: str) -> dict:
    """Effective per-dependency isolation values (default = instance name)."""
    return {
        "etcd": params.get("etcdRootPath") or name,
        "minio": (params.get("minioBucket") or name, params.get("minioRootPath") or name),
        "mq": params.get("mqChanPrefix") or name,
    }


_DEP_LABELS = {"etcd": "etcd", "minio": "对象存储", "mq": "MQ"}


def check_milvus_install(instances: list, spec) -> None:
    """Reject a milvus install that duplicates a name or collides per-dependency
    (shares a dep endpoint AND uses the same isolation value(s) for that dep)."""
    if any(i.name == spec.name for i in instances):
        raise ValueError(f"实例名 {spec.name} 已存在，请换名")
    new_eps, new_iso = _dep_ep_sets(spec.params), _iso_of(spec.params, spec.name)
    for i in instances:
        snap = i.spec_snapshot or {}
        if snap.get("kind") != "milvus":
            continue
        p = snap.get("params", {}) or {}
        eps, iso = _dep_ep_sets(p), _iso_of(p, i.name)
        for dep in ("etcd", "minio", "mq"):
            if (new_eps[dep] & eps[dep]) and new_iso[dep] == iso[dep]:
                raise ValueError(
                    f"{_DEP_LABELS[dep]} 隔离与 milvus {i.name} 冲突"
                    f"（共享同一 {_DEP_LABELS[dep]} 且隔离值相同），请改{_DEP_LABELS[dep]}的隔离配置")


class Provisioner:
    def __init__(self, registry: DriverRegistry, adapter: PlatformAdapter,
                 state: StateStore, engine: TaskEngine) -> None:
        self.registry = registry
        self.adapter = adapter
        self.state = state
        self.engine = engine

    def install(self, spec: InstallSpec, dry_run: bool = True, force: bool = False) -> Task:
        if spec.kind == "milvus":
            check_milvus_install(self.state.list_instances(), spec)
            from .. import compat
            versions: dict = {}
            try:
                if getattr(self.adapter, "name", "") == "k8s":
                    from .. import probe
                    versions = probe.detect_versions().as_compat_dict()
            except Exception:
                versions = {}
            compat.gate("install", {
                "mq": spec.params.get("mq"),
                "image": spec.params.get("image", ""),
                "mode": spec.params.get("mode", "standalone"),
                "versions": versions,
            }, force=force)
        driver = self.registry.get(spec.kind)
        steps = driver.plan_install_steps(spec, self.adapter)

        method = self.registry.get(spec.kind).profile.method(spec.method, spec.platform)
        method_id = method.id if method else None

        def register() -> str:
            inst = Instance(
                id=spec.name, name=spec.name, platform=spec.platform, namespace=spec.namespace,
                ownership=Ownership.managed,
                deps=[DependencyBinding(
                    kind=spec.kind, install_method=method_id,
                    ownership=Ownership.managed, state_class=driver.state_class(),
                )],
                spec_snapshot=spec.model_dump(mode="json"),
            )
            self.state.put_instance(inst)
            return f"已登记实例 {spec.name} 为 Managed"

        steps.append(Step(name="register", plan="写管理元数据 + 快照，标记 Managed", action=register))

        task = self.engine.run(type="install", target=spec.name, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        return task
