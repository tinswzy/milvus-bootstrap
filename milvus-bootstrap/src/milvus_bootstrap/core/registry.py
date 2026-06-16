"""Driver registry — dispatch by ``kind``; callers never branch on component type."""
from __future__ import annotations

from .drivers import DRIVER_CLASSES, BaseServiceDriver, ServiceDriver
from .profile import Profile


class DriverRegistry:
    def __init__(self) -> None:
        self._drivers: dict[str, ServiceDriver] = {}

    def register(self, driver: ServiceDriver) -> None:
        self._drivers[driver.kind] = driver

    def get(self, kind: str) -> ServiceDriver:
        if kind not in self._drivers:
            raise KeyError(f"未知组件类型：{kind}（已知：{', '.join(sorted(self._drivers))}）")
        return self._drivers[kind]

    def kinds(self) -> list[str]:
        return sorted(self._drivers)

    def all(self) -> list[ServiceDriver]:
        return list(self._drivers.values())

    def find_for(self, evidence: dict) -> ServiceDriver | None:
        for drv in self._drivers.values():
            if drv.detect(evidence):
                return drv
        return None


def build_registry(profiles: dict[str, Profile]) -> DriverRegistry:
    reg = DriverRegistry()
    for kind, prof in profiles.items():
        cls = DRIVER_CLASSES.get(kind, BaseServiceDriver)
        reg.register(cls(prof))
    return reg
