"""Orchestration (L2) — a Task is a list of Steps with the four-part anatomy.

dry_run collects each step's plan without executing. Real runs execute
precheck/do/verify and, on failure, run compensations in reverse (rollback).
"""
from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from ..models import StepResult, StepStatus, Task, TaskStatus


@dataclass
class Step:
    name: str
    plan: str
    action: Callable[[], str] | None = None      # do; returns a detail string
    precheck: Callable[[], bool] | None = None   # True -> already done, skip
    compensate: Callable[[], str] | None = None  # rollback


class TaskEngine:
    def new_id(self) -> str:
        return "task-" + uuid.uuid4().hex[:8]

    def run(self, *, type: str, target: str, steps: list[Step],
            dry_run: bool = True, task_id: str | None = None) -> Task:
        task = Task(id=task_id or self.new_id(), type=type, target=target,
                    dry_run=dry_run, status=TaskStatus.running)

        if dry_run:
            for s in steps:
                task.steps.append(StepResult(name=s.name, plan=s.plan, status=StepStatus.planned))
            task.status = TaskStatus.succeeded
            task.audit.append("dry-run：仅产出计划，未执行任何动作")
            return task

        done: list[Step] = []
        for s in steps:
            res = StepResult(name=s.name, plan=s.plan, status=StepStatus.running)
            task.steps.append(res)
            try:
                if s.precheck and s.precheck():
                    res.status = StepStatus.skipped
                    res.detail = "precheck: 已是目标态，跳过"
                    continue
                res.detail = s.action() if s.action else ""
                res.status = StepStatus.ok
                done.append(s)
            except Exception as exc:  # noqa: BLE001
                res.status = StepStatus.failed
                res.detail = str(exc)
                task.audit.append(f"step '{s.name}' failed: {exc}")
                for d in reversed(done):
                    if d.compensate:
                        task.audit.append(f"compensate '{d.name}': {d.compensate()}")
                task.status = TaskStatus.rolled_back
                return task

        task.status = TaskStatus.succeeded
        return task
