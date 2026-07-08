"""Provisioner (generic) — install a component via its driver + the Task engine."""
from __future__ import annotations

from ..models import DependencyBinding, InstallSpec, Instance, Ownership, Task
from ..platform.base import PlatformAdapter
from ..registry import DriverRegistry
from ..state.base import StateStore
from ..tasks.engine import Step, TaskEngine


def _dep_eps(params: dict) -> set:
    """The dependency endpoint strings a milvus install binds to, as a set."""
    eps: set = set()
    etcd = params.get("etcdEndpoints")
    if isinstance(etcd, (list, tuple)):
        eps.update(str(e) for e in etcd)
    elif etcd:
        eps.add(str(etcd))
    for key in ("storageEndpoint", "pulsarEndpoint"):
        if params.get(key):
            eps.add(str(params[key]))
    kb = params.get("kafkaBrokers")
    if isinstance(kb, (list, tuple)):
        eps.update(str(e) for e in kb)
    elif kb:
        eps.add(str(kb))
    return eps


def check_milvus_install(instances: list, spec) -> None:
    """Reject a milvus install that duplicates a name or collides on (prefix, shared dep)."""
    if any(i.name == spec.name for i in instances):
        raise ValueError(f"实例名 {spec.name} 已存在，请换名")
    new_prefix = spec.params.get("isolationPrefix") or spec.name
    new_eps = _dep_eps(spec.params)
    for i in instances:
        snap = i.spec_snapshot or {}
        if snap.get("kind") != "milvus":
            continue
        p = snap.get("params", {}) or {}
        eff = p.get("isolationPrefix") or i.name
        if eff == new_prefix and (_dep_eps(p) & new_eps):
            raise ValueError(
                f"隔离前缀 {new_prefix} 已被 milvus {i.name} 在共享依赖上占用，请改前缀")


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
