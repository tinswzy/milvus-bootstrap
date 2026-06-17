"""Domain models shared across the core.

These mirror the logical state model from the design docs. Everything stored
is a *re-derivable cache* — truth is always the live target.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Platform(str, Enum):
    k8s = "k8s"
    docker = "docker"
    standalone = "standalone"


class Ownership(str, Enum):
    managed = "managed"        # we installed it
    adoptable = "adoptable"    # installed elsewhere, safe to take over
    readonly = "readonly"      # observe only (e.g. control-plane)
    external = "external"      # off-cluster endpoint, can't manage pods


class StateClass(str, Enum):
    authoritative = "authoritative"  # disk = source of truth (etcd, minio)
    cache_backed = "cache_backed"    # local PVC is cache, truth external (woodpecker)
    stateless = "stateless"


class Candidate(BaseModel):
    """A discovered + identified workload, awaiting a management decision."""
    kind: str
    platform: Platform = Platform.k8s
    name: str
    install_method: str | None = None
    ownership: Ownership = Ownership.adoptable
    state_class: StateClass | None = None
    excluded: bool = False           # hard guardrail hit (e.g. control-plane)
    reason: str = ""
    evidence: dict[str, Any] = Field(default_factory=dict)


class InstallSpec(BaseModel):
    """Request to provision one component."""
    kind: str
    name: str
    platform: Platform = Platform.k8s
    method: str | None = None        # which install_method id; None -> profile default
    namespace: str = "default"
    params: dict[str, Any] = Field(default_factory=dict)
    chart_override: str | None = None  # override the profile's chart (e.g. a local .tgz)


class StepStatus(str, Enum):
    pending = "pending"
    planned = "planned"      # dry-run: action not executed
    running = "running"
    ok = "ok"
    failed = "failed"
    compensated = "compensated"
    skipped = "skipped"


class TaskStatus(str, Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    rolled_back = "rolled_back"


class StepResult(BaseModel):
    name: str
    status: StepStatus = StepStatus.pending
    plan: str = ""           # human-readable "what this step would do"
    detail: str = ""         # output / error after running


class Task(BaseModel):
    id: str
    type: str                # install / switch-mq / upgrade ...
    target: str              # instance / component name
    dry_run: bool = True
    status: TaskStatus = TaskStatus.pending
    steps: list[StepResult] = Field(default_factory=list)
    audit: list[str] = Field(default_factory=list)


class DependencyBinding(BaseModel):
    kind: str
    install_method: str | None = None
    ownership: Ownership = Ownership.managed
    state_class: StateClass | None = None
    endpoints: list[str] = Field(default_factory=list)
    ref: dict[str, Any] = Field(default_factory=dict)


class Instance(BaseModel):
    """A Milvus + its dependency bindings. Re-derivable cache, not authoritative."""
    id: str
    name: str
    platform: Platform = Platform.k8s
    namespace: str = "default"
    ownership: Ownership = Ownership.managed
    deps: list[DependencyBinding] = Field(default_factory=list)
    # last-applied InstallSpec (the "snapshot") — lets lifecycle ops re-render.
    spec_snapshot: dict[str, Any] = Field(default_factory=dict)
