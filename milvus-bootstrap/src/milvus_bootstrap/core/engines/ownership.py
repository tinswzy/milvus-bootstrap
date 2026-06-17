"""Ownership engine — adopt an Adoptable candidate into a Managed instance.

Writes management metadata + registers the instance. Control-plane / excluded
candidates are refused (hard guardrail).
"""
from __future__ import annotations

from ..models import Candidate, DependencyBinding, InstallSpec, Instance, Ownership, Task
from ..registry import DriverRegistry
from ..state.base import StateStore
from ..tasks.engine import Step, TaskEngine


class OwnershipEngine:
    def __init__(self, registry: DriverRegistry, state: StateStore, engine: TaskEngine) -> None:
        self.registry = registry
        self.state = state
        self.engine = engine

    def adopt(self, candidate: Candidate, dry_run: bool = True) -> Task:
        if candidate.excluded:
            raise ValueError(f"{candidate.kind}/{candidate.name} 命中排除（{candidate.reason}），永不接管")
        if candidate.ownership != Ownership.adoptable:
            raise ValueError(f"{candidate.kind}/{candidate.name} 归属={candidate.ownership.value}，不可接管")

        kind, name = candidate.kind, candidate.name
        ns = candidate.evidence.get("namespace", "default")

        def _register() -> str:
            snap = InstallSpec(
                kind=kind, name=name, platform=candidate.platform,
                namespace=ns, method=candidate.install_method,
            ).model_dump(mode="json")
            inst = Instance(
                id=name, name=name, platform=candidate.platform, namespace=ns,
                ownership=Ownership.managed,
                deps=[DependencyBinding(
                    kind=kind, install_method=candidate.install_method,
                    ownership=Ownership.managed, state_class=candidate.state_class,
                )],
                spec_snapshot=snap,
            )
            self.state.put_instance(inst)
            return f"已接管 {name}（now Managed）"

        steps = [
            Step(name="precheck",
                 plan=f"确认 {kind}/{name} 可接管（归属={candidate.ownership.value}，未命中排除）"),
            Step(name="write-metadata",
                 plan=f"写管理元数据：annotation milvus-bootstrap/managed=true 到 {kind}/{name}"
                      f"（install_method={candidate.install_method}）"),
            Step(name="register", plan=f"登记 {name} 为 Managed", action=_register),
        ]
        task = self.engine.run(type="adopt", target=name, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        return task
