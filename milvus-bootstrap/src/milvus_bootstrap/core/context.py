"""Core assembly — wires profiles, drivers, platform adapter, state, engines.

This is the single object the daemon exposes. The CLI never imports it; it
talks to the daemon over the socket.
"""
from __future__ import annotations

import os
from typing import Any

from .. import __version__
from .engines import (
    ConfigEngine,
    DiscoveryEngine,
    LifecycleEngine,
    OwnershipEngine,
    Provisioner,
)
from .models import Candidate, InstallSpec, Task
from .platform.base import PlatformAdapter
from .profile import load_profiles
from .registry import build_registry
from .state import FileStateStore
from .tasks import TaskEngine


def make_adapter() -> PlatformAdapter:
    name = os.environ.get("MB_ADAPTER", "fake").lower()
    if name == "k8s":
        from .platform.k8s import K8sAdapter
        return K8sAdapter()
    from .platform.fake import FakeAdapter
    return FakeAdapter()


class Core:
    def __init__(self) -> None:
        self.profiles = load_profiles()
        self.registry = build_registry(self.profiles)
        self.adapter = make_adapter()
        self.state = FileStateStore()
        self.engine = TaskEngine()
        self.discovery = DiscoveryEngine(self.adapter, self.registry)
        self.provisioner = Provisioner(self.registry, self.adapter, self.state, self.engine)
        self.lifecycle = LifecycleEngine(self.registry, self.adapter, self.state, self.engine)
        self.ownership = OwnershipEngine(self.registry, self.state, self.engine)
        self.config = ConfigEngine(self.registry, self.adapter, self.state, self.engine)

    def status(self) -> dict[str, Any]:
        return {
            "version": __version__,
            "adapter": self.adapter.name,
            "state": self.state.name,
            "profiles": self.registry.kinds(),
            "instances": [i.name for i in self.state.list_instances()],
        }

    def discover(self) -> list[Candidate]:
        return self.discovery.discover()

    def install(self, spec: InstallSpec, dry_run: bool = True) -> Task:
        return self.provisioner.install(spec, dry_run=dry_run)

    def delete(self, instance_id: str, dry_run: bool = True) -> Task:
        return self.lifecycle.delete(instance_id, dry_run=dry_run)

    def scale(self, instance_id: str, replicas: int, dry_run: bool = True) -> Task:
        return self.lifecycle.scale(instance_id, replicas, dry_run=dry_run)

    def upgrade(self, instance_id: str, image: str, dry_run: bool = True) -> Task:
        return self.lifecycle.upgrade(instance_id, image, dry_run=dry_run)

    def adopt(self, kind: str, name: str, dry_run: bool = True) -> Task:
        cands = [c for c in self.discover() if c.kind == kind and c.name == name]
        if not cands:
            raise KeyError(f"未发现可接管的 {kind}/{name}")
        return self.ownership.adopt(cands[0], dry_run=dry_run)

    def switch_mq(self, instance_id: str, target_wal: str, dry_run: bool = True) -> Task:
        inst = self.state.get_instance(instance_id)
        if inst is None:
            raise KeyError(f"未找到实例 {instance_id}")
        if not inst.spec_snapshot:
            raise ValueError(f"{instance_id} 无安装快照")
        spec = InstallSpec.model_validate(inst.spec_snapshot)
        if spec.kind != "milvus":
            raise ValueError("switch-mq 仅适用于 milvus 实例")
        steps = self.registry.get("milvus").plan_switch_mq_steps(spec, self.adapter, target_wal)
        task = self.engine.run(type="switch-mq", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        return task

    def config_get(self, instance_id: str) -> dict[str, str]:
        return self.config.get(instance_id)

    def config_set(self, instance_id: str, kv: dict, dry_run: bool = True) -> Task:
        return self.config.set(instance_id, kv, dry_run=dry_run)

    def config_restart(self, instance_id: str, dry_run: bool = True) -> Task:
        return self.config.restart(instance_id, dry_run=dry_run)
