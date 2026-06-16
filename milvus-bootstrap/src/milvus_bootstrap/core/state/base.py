"""StateStore interface.

Location follows the platform (k8s -> Secret/ConfigMap; docker/standalone ->
local file). ALL stored state is a re-derivable, deletable, self-correcting
cache used for fast update-time checks — never authoritative.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import Instance, Task


class StateStore(ABC):
    name: str = "state"

    @abstractmethod
    def put_instance(self, inst: Instance) -> None: ...

    @abstractmethod
    def get_instance(self, instance_id: str) -> Instance | None: ...

    @abstractmethod
    def list_instances(self) -> list[Instance]: ...

    @abstractmethod
    def delete_instance(self, instance_id: str) -> None: ...

    @abstractmethod
    def put_task(self, task: Task) -> None: ...

    @abstractmethod
    def list_tasks(self) -> list[Task]: ...
