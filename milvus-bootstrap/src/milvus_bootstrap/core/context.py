"""Core assembly — wires profiles, drivers, platform adapter, state, engines.

This is the single object the daemon exposes. The CLI never imports it; it
talks to the daemon over the socket.
"""
from __future__ import annotations

import os
from typing import Any

from .. import __version__
from .engines import DiscoveryEngine, Provisioner
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
