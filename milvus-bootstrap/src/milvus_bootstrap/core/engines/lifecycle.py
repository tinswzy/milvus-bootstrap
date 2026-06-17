"""Lifecycle engine — upgrade / scale / delete on managed instances.

Re-renders from the install snapshot (re-apply = upgrade for helm / patch for
operator-cr), reusing the driver's install steps with overridden params.
"""
from __future__ import annotations

from ..models import InstallSpec, StateClass, Task, TaskStatus
from ..platform.base import PlatformAdapter
from ..registry import DriverRegistry
from ..state.base import StateStore
from ..tasks.engine import Step, TaskEngine


class LifecycleEngine:
    def __init__(self, registry: DriverRegistry, adapter: PlatformAdapter,
                 state: StateStore, engine: TaskEngine) -> None:
        self.registry = registry
        self.adapter = adapter
        self.state = state
        self.engine = engine

    def _load(self, instance_id: str):
        inst = self.state.get_instance(instance_id)
        if inst is None:
            raise KeyError(f"未找到实例 {instance_id}")
        if not inst.spec_snapshot:
            raise ValueError(f"{instance_id} 无安装快照，无法运维（外部实例或非本工具安装）")
        spec = InstallSpec.model_validate(inst.spec_snapshot)
        return inst, spec, self.registry.get(spec.kind)

    def delete(self, instance_id: str, dry_run: bool = True) -> Task:
        _, spec, driver = self._load(instance_id)
        steps = driver.plan_delete_steps(spec, self.adapter)
        task = self.engine.run(type="delete", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        if not dry_run and task.status == TaskStatus.succeeded:
            self.state.delete_instance(instance_id)
        return task

    def scale(self, instance_id: str, replicas: int, dry_run: bool = True) -> Task:
        inst, spec, driver = self._load(instance_id)
        key = driver.replicas_param()
        if key is None:
            raise ValueError(f"{spec.kind} 不支持副本扩缩")
        m = driver.profile.method(spec.method, spec.platform)
        merged = {**(m.params or {}), **spec.params} if m else dict(spec.params)
        current = int(merged.get(key) or 0)
        guard = Step(name="scale-guard", plan=driver.scale_plan(current, replicas))
        spec2 = spec.model_copy(deep=True)
        spec2.params = {**spec.params, key: replicas}
        steps = [guard, *driver.plan_install_steps(spec2, self.adapter)]
        task = self.engine.run(type="scale", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        if not dry_run and task.status == TaskStatus.succeeded:
            inst.spec_snapshot = spec2.model_dump(mode="json")
            self.state.put_instance(inst)
        return task

    def upgrade(self, instance_id: str, image: str, dry_run: bool = True) -> Task:
        inst, spec, driver = self._load(instance_id)
        steps: list[Step] = []
        if driver.state_class() == StateClass.authoritative:
            steps.append(Step(
                name="backup-note",
                plan="权威态：升级前应先备份；命中 immutable 字段（如 PVC）需 orphan 删+重建",
            ))
        spec2 = spec.model_copy(deep=True)
        spec2.params = {**spec.params, "image": image}
        steps.extend(driver.plan_install_steps(spec2, self.adapter))
        task = self.engine.run(type="upgrade", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        if not dry_run and task.status == TaskStatus.succeeded:
            inst.spec_snapshot = spec2.model_dump(mode="json")
            self.state.put_instance(inst)
        return task
