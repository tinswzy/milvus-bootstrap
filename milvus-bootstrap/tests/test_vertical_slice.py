"""End-to-end vertical slice: profile -> registry -> driver -> engine -> state.

Runs the core directly (no daemon, no cluster) via the FakeAdapter.
"""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.models import InstallSpec, Ownership, StateClass, StepStatus, TaskStatus


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def test_profiles_and_registry(core: Core) -> None:
    assert "etcd" in core.registry.kinds()
    assert core.status()["adapter"] == "fake"


def test_discover_excludes_control_plane(core: Core) -> None:
    etcds = [c for c in core.discover() if c.kind == "etcd"]
    assert len(etcds) == 2
    by_name = {c.name: c for c in etcds}

    helm = by_name["milvus-etcd"]
    assert helm.ownership == Ownership.adoptable
    assert helm.excluded is False
    assert helm.install_method == "bitnami-helm"
    assert helm.state_class == StateClass.authoritative

    cp = [c for c in etcds if c.excluded]
    assert len(cp) == 1
    assert cp[0].ownership == Ownership.readonly  # control-plane never adopted


def test_install_dry_run_plans_but_does_nothing(core: Core) -> None:
    task = core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=True)
    assert task.dry_run is True
    assert task.status == TaskStatus.succeeded
    assert [s.name for s in task.steps] == ["render", "apply", "wait-ready", "register"]
    assert all(s.status == StepStatus.planned for s in task.steps)
    # dry-run must not register an instance
    assert core.state.get_instance("etcd-dev") is None


def test_install_apply_registers_managed_instance(core: Core) -> None:
    task = core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    assert task.status == TaskStatus.succeeded
    assert all(s.status == StepStatus.ok for s in task.steps)

    inst = core.state.get_instance("etcd-dev")
    assert inst is not None
    assert inst.ownership == Ownership.managed
    assert inst.deps[0].kind == "etcd"
    assert inst.deps[0].install_method == "bitnami-helm"
    assert inst.deps[0].state_class == StateClass.authoritative


def test_etcd_driver_overrides_scale_plan(core: Core) -> None:
    drv = core.registry.get("etcd")
    plan = drv.scale_plan(3, 5)
    assert "奇数" in plan and "member remove" not in plan  # scale-up note
    assert "member remove" in drv.scale_plan(5, 3)         # scale-down override


def test_unknown_kind_raises(core: Core) -> None:
    with pytest.raises(KeyError):
        core.install(InstallSpec(kind="redis", name="x"), dry_run=True)
