"""Discovery + Identification (generic) — finds candidates, dispatches to drivers."""
from __future__ import annotations

from ..models import Candidate
from ..platform.base import PlatformAdapter
from ..registry import DriverRegistry


class DiscoveryEngine:
    def __init__(self, adapter: PlatformAdapter, registry: DriverRegistry) -> None:
        self.adapter = adapter
        self.registry = registry

    def discover(self) -> list[Candidate]:
        out: list[Candidate] = []
        for evidence in self.adapter.discover_native():
            driver = self.registry.find_for(evidence)
            if driver is not None:
                out.append(driver.identify(evidence))
        return out
