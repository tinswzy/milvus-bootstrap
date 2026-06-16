"""ServiceDriver (L3) — per-component polymorphism.

The caller invokes one uniform interface; dispatch is by ``kind``. The base
class implements the generic 80% from the profile; component drivers subclass
and override only the special 20% (quorum scale, decommission, wal/alter, ...).
"""
from __future__ import annotations

import fnmatch
from abc import ABC, abstractmethod

import yaml

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
            helm_methods = [mm for mm in self.profile.install_methods
                            if mm.platform == platform and mm.kind == "helm"]
            method_id = helm_methods[0].id if helm_methods else "helm"
            return Candidate(
                kind=self.kind, platform=platform, name=name,
                install_method=method_id,
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
        if m.kind == "helm":
            return self._helm_steps(spec, adapter, m, params)
        if m.kind == "operator-cr":
            return self._operator_cr_steps(spec, adapter, m, params)
        if m.kind == "external":
            return self._external_steps(spec, m, params)
        raise ValueError(f"{self.kind}: 暂不支持安装方式 kind={m.kind}")

    def _helm_steps(self, spec, adapter, m, params) -> list[Step]:
        ns, name, kind = spec.namespace, spec.name, self.kind
        chart = spec.chart_override or m.chart
        check = self.profile.health.workload_ready
        if self.profile.health.cluster_check:
            check += f" + {self.profile.health.cluster_check}"
        return [
            Step(name="render",
                 plan=f"按 {m.id} 渲染 {kind}/{name} 安装清单（helm, chart={chart}, params={params}）"),
            Step(name="apply",
                 plan=adapter.plan_apply(kind=kind, name=name, namespace=ns, method=m.id,
                                         method_kind=m.kind, chart=chart, params=params),
                 action=lambda: adapter.apply_workload(kind=kind, name=name, namespace=ns, method=m.id,
                                                       method_kind=m.kind, chart=chart, params=params),
                 compensate=lambda: adapter.delete_workload(kind=kind, name=name, namespace=ns)),
            Step(name="wait-ready", plan=f"等待就绪：{check}",
                 action=lambda: adapter.wait_ready(kind=kind, name=name, namespace=ns, check=check)),
        ]

    def _operator_cr_steps(self, spec, adapter, m, params) -> list[Step]:
        if m.cr is None:
            raise ValueError(f"{self.kind} operator-cr 方式缺少 cr 定义")
        cr, ns, name = m.cr, spec.namespace, spec.name
        manifests = self.build_install_manifests(spec, m, params)

        def _check_op() -> str:
            if not adapter.crd_exists(group=cr.group, plural=cr.plural):
                raise RuntimeError(f"未发现 {cr.kind} CRD（{cr.plural}.{cr.group}）——请先安装 {self.kind} operator")
            return f"{cr.kind} CRD 已就位"

        steps = [
            Step(name="precheck-operator",
                 plan=f"确认 {cr.kind} CRD（{cr.plural}.{cr.group}）已注册（operator 就位）",
                 action=_check_op),
            Step(name="apply-cr",
                 plan="将 apply：\n" + yaml.safe_dump_all(manifests, allow_unicode=True, sort_keys=False).strip(),
                 action=lambda: adapter.apply_objects(manifests=manifests),
                 compensate=lambda: adapter.delete_cr(group=cr.group, version=cr.version,
                                                      plural=cr.plural, namespace=ns, name=name)),
        ]
        if m.ready is not None:
            r = m.ready
            steps.append(Step(
                name="wait-status",
                plan=f"等待 {cr.kind}.{r.status_path} == {r.status_equals}",
                action=lambda: adapter.wait_cr(group=cr.group, version=cr.version, plural=cr.plural,
                                               namespace=ns, name=name,
                                               status_path=r.status_path, status_equals=r.status_equals)))
        return steps

    def _external_steps(self, spec, m, params) -> list[Step]:
        ep = params.get("endpoints", "<待填>")
        return [Step(name="record-endpoints",
                     plan=f"external：不安装 {self.kind}，仅在 Milvus 填 {self.connect_info()} = {ep}")]

    # ---- CR manifest builders (operator drivers override) ----
    def build_install_manifests(self, spec, m, params) -> list[dict]:
        cr = m.cr
        return [{
            "apiVersion": f"{cr.group}/{cr.version}",
            "kind": cr.kind,
            "metadata": {"name": spec.name, "namespace": spec.namespace},
            "spec": self.build_cr_spec(spec, m, params),
        }]

    def build_cr_spec(self, spec, m, params) -> dict:
        return dict(params)
