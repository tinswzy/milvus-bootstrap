"""ServiceDriver (L3) — per-component polymorphism.

The caller invokes one uniform interface; dispatch is by ``kind``. The base
class implements the generic 80% from the profile; component drivers subclass
and override only the special 20% (quorum scale, decommission, wal/alter, ...).
"""
from __future__ import annotations

import fnmatch
from abc import ABC, abstractmethod

from ..models import Candidate, InstallSpec, Ownership, Platform, StateClass
from ..platform.base import PlatformAdapter
from ..profile import Profile
from ..tasks.engine import Step


class ServiceDriver(ABC):
    kind: str = ""

    def __init__(self, profile: Profile) -> None:
        self.profile = profile
        self.kind = profile.kind

    @abstractmethod
    def detect(self, evidence: dict) -> bool: ...

    @abstractmethod
    def identify(self, evidence: dict) -> Candidate: ...

    @abstractmethod
    def plan_install_steps(self, spec: InstallSpec, adapter: PlatformAdapter) -> list[Step]: ...

    def state_class(self) -> StateClass:
        return self.profile.state_class

    def connect_info(self) -> str | None:
        return self.profile.connect.milvus_values

    def scale_plan(self, current: int, target: int) -> str:
        return f"将 {self.kind} 副本从 {current} 调整到 {target}"


class BaseServiceDriver(ServiceDriver):
    """Profile-driven generic driver — covers most components as-is."""

    def detect(self, evidence: dict) -> bool:
        img = str(evidence.get("image", "")).lower()
        if any(m.lower() in img for m in self.profile.detect.image_match):
            return True
        chart = str(evidence.get("labels", {}).get("helm.sh/chart", ""))
        if self.profile.detect.helm_chart and fnmatch.fnmatch(chart, self.profile.detect.helm_chart):
            return True
        if self.profile.detect.crd and evidence.get("crd") == self.profile.detect.crd:
            return True
        return False

    def identify(self, evidence: dict) -> Candidate:
        labels = evidence.get("labels", {})
        name = evidence.get("name", "?")
        platform = Platform(evidence.get("platform", "k8s"))
        sc = self.profile.state_class

        # Hard guardrail: excluded labels (e.g. control-plane) -> readonly, never adopt.
        excl = self.profile.detect.exclude_labels
        if excl and all(labels.get(k) == v for k, v in excl.items()):
            return Candidate(
                kind=self.kind, platform=platform, name=name,
                ownership=Ownership.readonly, state_class=sc, excluded=True,
                reason="命中排除标签（控制面组件），永不接管", evidence=evidence,
            )

        if labels.get("app.kubernetes.io/managed-by") == "Helm":
            chart = labels.get("helm.sh/chart", "")
            m = self.profile.default_method(platform)
            return Candidate(
                kind=self.kind, platform=platform, name=name,
                install_method=(m.id if m else "helm"),
                ownership=Ownership.adoptable, state_class=sc,
                reason=f"helm 安装（{chart}），可接管", evidence=evidence,
            )

        return Candidate(
            kind=self.kind, platform=platform, name=name,
            ownership=Ownership.adoptable, state_class=sc,
            reason="裸部署，可写管理元数据纳管", evidence=evidence,
        )

    def plan_install_steps(self, spec: InstallSpec, adapter: PlatformAdapter) -> list[Step]:
        m = self.profile.method(spec.method, spec.platform)
        if m is None:
            raise ValueError(f"{self.kind} 在 {spec.platform.value} 上没有可用的安装方式")
        params = {**(m.params or {}), **(spec.params or {})}
        ns, name, kind = spec.namespace, spec.name, self.kind

        check = self.profile.health.workload_ready
        if self.profile.health.cluster_check:
            check += f" + {self.profile.health.cluster_check}"

        return [
            Step(
                name="render",
                plan=f"按 {m.id} 渲染 {kind}/{name} 安装清单（kind={m.kind}, chart={m.chart}, params={params}）",
            ),
            Step(
                name="apply",
                plan=adapter.plan_apply(kind=kind, name=name, namespace=ns, method=m.id, chart=m.chart, params=params),
                action=lambda: adapter.apply_workload(kind=kind, name=name, namespace=ns, method=m.id, chart=m.chart, params=params),
                compensate=lambda: adapter.delete_workload(kind=kind, name=name, namespace=ns),
            ),
            Step(
                name="wait-ready",
                plan=f"等待就绪：{check}",
                action=lambda: adapter.wait_ready(kind=kind, name=name, namespace=ns, check=check),
            ),
        ]
