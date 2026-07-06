"""Service knowledge base — declarative per-component descriptor.

A profile drives the generic 80% of every engine (detection, default install,
health gate, connect mapping, state-class). Component-specific behaviour lives
in the ServiceDriver subclass. Adding a component ≈ adding one profile (+ a
thin driver override).
"""
from __future__ import annotations

from importlib import resources
from typing import Any

import yaml
from pydantic import BaseModel, Field

from .models import Platform, StateClass


class DetectRules(BaseModel):
    image_match: list[str] = Field(default_factory=list)
    ports: list[int] = Field(default_factory=list)
    helm_chart: str | None = None
    crd: str | None = None                       # e.g. WoodpeckerCluster
    exclude_labels: dict[str, str] = Field(default_factory=dict)  # hard guardrail


class CRInfo(BaseModel):
    """Custom Resource coordinates for an operator-cr install method."""
    group: str                                   # e.g. minio.min.io
    version: str                                 # e.g. v2
    plural: str                                  # e.g. tenants
    kind: str                                    # e.g. Tenant


class ReadyInfo(BaseModel):
    """How to know an operator-managed CR is ready (status field == value)."""
    status_path: str                             # e.g. status.currentState
    status_equals: str                           # e.g. Initialized


class InstallMethod(BaseModel):
    id: str                                      # bitnami-helm / minio-operator / ...
    platform: Platform = Platform.k8s
    default: bool = False
    kind: str = "helm"                           # helm | operator-cr | compose | external
    chart: str | None = None
    cr: CRInfo | None = None                     # for kind == operator-cr
    ready: ReadyInfo | None = None               # for kind == operator-cr
    params: dict[str, Any] = Field(default_factory=dict)
    wait_timeout_s: int | None = None            # override readiness wait (slow stacks, e.g. pulsar)


class HealthRules(BaseModel):
    workload_ready: str = "readyReplicas == replicas"
    cluster_check: str | None = None             # e.g. etcdctl endpoint health


class ScaleRules(BaseModel):
    replicas: str | None = None                  # e.g. odd-only
    step: str | None = None                      # e.g. one-at-a-time
    scale_down: str | None = None                # e.g. member remove / decommission
    immutable_fields: list[str] = Field(default_factory=list)


class ConnectRules(BaseModel):
    milvus_values: str | None = None             # externalEtcd.endpoints ...


class Profile(BaseModel):
    kind: str
    state_class: StateClass
    detect: DetectRules = Field(default_factory=DetectRules)
    install_methods: list[InstallMethod] = Field(default_factory=list)
    health: HealthRules = Field(default_factory=HealthRules)
    scale_rules: ScaleRules = Field(default_factory=ScaleRules)
    connect: ConnectRules = Field(default_factory=ConnectRules)

    def default_method(self, platform: Platform) -> InstallMethod | None:
        cands = [m for m in self.install_methods if m.platform == platform]
        for m in cands:
            if m.default:
                return m
        return cands[0] if cands else None

    def method(self, method_id: str | None, platform: Platform) -> InstallMethod | None:
        if method_id is None:
            return self.default_method(platform)
        for m in self.install_methods:
            if m.id == method_id:
                return m
        return None


def load_profiles() -> dict[str, Profile]:
    """Load all bundled ``profiles/*.yaml`` into {kind: Profile}."""
    out: dict[str, Profile] = {}
    pkg = resources.files("milvus_bootstrap.profiles")
    for entry in pkg.iterdir():
        if entry.name.endswith((".yaml", ".yml")):
            data = yaml.safe_load(entry.read_text())
            prof = Profile.model_validate(data)
            out[prof.kind] = prof
    return out
