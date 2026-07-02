"""Provisioner (generic) — install a component via its driver + the Task engine."""
from __future__ import annotations

from ..models import DependencyBinding, InstallSpec, Instance, Ownership, Task
from ..platform.base import PlatformAdapter
from ..registry import DriverRegistry
from ..state.base import StateStore
from ..tasks.engine import Step, TaskEngine


class Provisioner:
    def __init__(self, registry: DriverRegistry, adapter: PlatformAdapter,
                 state: StateStore, engine: TaskEngine) -> None:
        self.registry = registry
        self.adapter = adapter
        self.state = state
        self.engine = engine

    def install(self, spec: InstallSpec, dry_run: bool = True, force: bool = False) -> Task:
        if spec.kind == "milvus":
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
