"""Local file-backed StateStore (for docker / standalone / dev).

Plain JSON under the state dir. It's just a cache: delete it and a re-discover
rebuilds it.
"""
from __future__ import annotations

from pathlib import Path

from ... import paths
from ..models import Instance, Task
from .base import StateStore


class FileStateStore(StateStore):
    name = "file"

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or paths.state_dir()
        self.inst_dir = self.root / "instances"
        self.task_dir = self.root / "tasks"
        self.inst_dir.mkdir(parents=True, exist_ok=True)
        self.task_dir.mkdir(parents=True, exist_ok=True)

    def put_instance(self, inst: Instance) -> None:
        (self.inst_dir / f"{inst.id}.json").write_text(inst.model_dump_json(indent=2))

    def get_instance(self, instance_id: str) -> Instance | None:
        f = self.inst_dir / f"{instance_id}.json"
        if not f.exists():
            return None
        return Instance.model_validate_json(f.read_text())

    def list_instances(self) -> list[Instance]:
        return [Instance.model_validate_json(f.read_text()) for f in sorted(self.inst_dir.glob("*.json"))]

    def delete_instance(self, instance_id: str) -> None:
        (self.inst_dir / f"{instance_id}.json").unlink(missing_ok=True)

    def put_task(self, task: Task) -> None:
        (self.task_dir / f"{task.id}.json").write_text(task.model_dump_json(indent=2))

    def list_tasks(self) -> list[Task]:
        return [Task.model_validate_json(f.read_text()) for f in sorted(self.task_dir.glob("*.json"))]
