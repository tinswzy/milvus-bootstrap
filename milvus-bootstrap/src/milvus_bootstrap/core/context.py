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
from .models import Candidate, InstallSpec, Task, TaskStatus
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

    def install(self, spec: InstallSpec, dry_run: bool = True, force: bool = False) -> Task:
        return self.provisioner.install(spec, dry_run=dry_run, force=force)

    def delete(self, instance_id: str, dry_run: bool = True) -> Task:
        return self.lifecycle.delete(instance_id, dry_run=dry_run)

    def scale(self, instance_id: str, replicas: int, dry_run: bool = True) -> Task:
        return self.lifecycle.scale(instance_id, replicas, dry_run=dry_run)

    def upgrade(self, instance_id: str, image: str, dry_run: bool = True, force: bool = False) -> Task:
        return self.lifecycle.upgrade(instance_id, image, dry_run=dry_run, force=force)

    def adopt(self, kind: str, name: str, dry_run: bool = True) -> Task:
        cands = [c for c in self.discover() if c.kind == kind and c.name == name]
        if not cands:
            raise KeyError(f"未发现可接管的 {kind}/{name}")
        return self.ownership.adopt(cands[0], dry_run=dry_run)

    def switch_mq(self, instance_id: str, target_wal: str, target_name: str = "",
                  target_ns: str = "", dry_run: bool = True, force: bool = False) -> Task:
        inst = self.state.get_instance(instance_id)
        if inst is None:
            raise KeyError(f"未找到实例 {instance_id}")
        if not inst.spec_snapshot:
            raise ValueError(f"{instance_id} 无安装快照")
        spec = InstallSpec.model_validate(inst.spec_snapshot)
        if spec.kind != "milvus":
            raise ValueError("switch-mq 仅适用于 milvus 实例")
        from . import compat
        cur_mq = spec.params.get("mq", "")
        cur_opt = compat.get_option(cur_mq)
        current_wal = cur_opt.wal if cur_opt else cur_mq
        compat.gate("switch-mq", {"current_wal": current_wal, "target_wal": target_wal,
                                  "milvus_version": spec.params.get("image", "")}, force=force)
        driver = self.registry.get("milvus")
        tns = target_ns or spec.namespace

        def _endpoint(wal: str) -> str:
            if wal == "kafka":
                return f"{target_name}.{tns}.svc:9092"
            if wal == "pulsar":
                return f"{target_name}-broker.{tns}.svc:6650"
            return ""

        # 应用态：保持当前 mq（不翻 msgStreamType），只把目标 MQ 连接注入 spec.config（_conf）。
        # 源/目标连接并存 → 重启后两 MQ client 都可建 → wal/alter 运行时切，milvus 不会读旧 checkpoint 崩。
        spec2 = spec.model_copy(deep=True)
        spec2.params = dict(spec2.params)
        endpoint = _endpoint(target_wal)
        if target_wal in ("kafka", "pulsar") and target_name:
            spec2.params["_conf"] = {**spec2.params.get("_conf", {}),
                                     **driver._mq_conn_conf(target_wal, endpoint)}
        steps = driver.plan_switch_mq_steps(spec2, self.adapter, target_wal)
        task = self.engine.run(type="switch-mq", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        if not dry_run and task.status == TaskStatus.succeeded:
            # 成功后 snapshot 记「目标态」作为 UI/未来渲染真相（活 CR 的 msgStreamType 不二次 patch——
            # milvus 重启以 etcd WAL 元数据为准）。用目标态 spec，而非并存态 spec2。
            snap = spec.model_copy(deep=True)
            snap.params = dict(snap.params)
            snap.params["mq"] = driver._wal_to_mq_id(target_wal)
            if target_wal == "kafka" and target_name:
                snap.params["kafkaBrokers"] = endpoint
            elif target_wal == "pulsar" and target_name:
                snap.params["pulsarEndpoint"] = endpoint
            inst.spec_snapshot = snap.model_dump(mode="json")
            self.state.put_instance(inst)
        return task

    def mq_options(self, milvus_version: str, mode: str = "standalone") -> list[dict]:
        from . import compat
        return compat.mq_options(milvus_version, mode)

    def config_get(self, instance_id: str) -> dict[str, str]:
        return self.config.get(instance_id)

    def config_set(self, instance_id: str, kv: dict, dry_run: bool = True) -> Task:
        return self.config.set(instance_id, kv, dry_run=dry_run)

    def config_restart(self, instance_id: str, dry_run: bool = True) -> Task:
        return self.config.restart(instance_id, dry_run=dry_run)
