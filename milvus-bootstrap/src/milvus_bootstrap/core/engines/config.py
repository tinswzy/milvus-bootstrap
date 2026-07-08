"""Config engine — get / set / restart the effective component config.

`set` folds overrides via the driver (milvus → CR spec.config nested; others →
install params) and re-applies; operator/helm then roll the pods. `restart`
does an explicit rolling restart.
"""
from __future__ import annotations

from ..models import InstallSpec, Task, TaskStatus
from ..platform.base import PlatformAdapter
from ..registry import DriverRegistry
from ..state.base import StateStore
from ..tasks.engine import Step, TaskEngine


class ConfigEngine:
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
            raise ValueError(f"{instance_id} 无安装快照")
        spec = InstallSpec.model_validate(inst.spec_snapshot)
        return inst, spec, self.registry.get(spec.kind)

    def get(self, instance_id: str) -> dict[str, str]:
        _, spec, driver = self._load(instance_id)
        return self.adapter.get_configmap(namespace=spec.namespace, name=driver.config_cm_name(spec))

    def set(self, instance_id: str, kv: dict, dry_run: bool = True) -> Task:
        inst, spec, driver = self._load(instance_id)
        spec2 = spec.model_copy(deep=True)
        spec2.params = driver.config_apply_params(spec.params, kv)
        steps = [Step(name="config-diff", plan=f"设置 {spec.kind} 配置：{kv}")]
        steps.extend(driver.plan_install_steps(spec2, self.adapter))
        steps.append(Step(name="restart-note", plan="operator/helm 在配置变更后自动滚动重启相关 pod"))
        task = self.engine.run(type="config", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        if not dry_run and task.status == TaskStatus.succeeded:
            inst.spec_snapshot = spec2.model_dump(mode="json")
            self.state.put_instance(inst)
        return task

    def restart(self, instance_id: str, dry_run: bool = True) -> Task:
        _, spec, _ = self._load(instance_id)
        selector = f"app.kubernetes.io/instance={spec.name}"
        step = Step(
            name="rollout-restart",
            plan=f"rolling restart @{selector}（ns={spec.namespace}）",
            action=lambda: self.adapter.restart(namespace=spec.namespace, label_selector=selector),
        )
        task = self.engine.run(type="restart", target=instance_id, steps=[step], dry_run=dry_run)
        self.state.put_task(task)
        return task
