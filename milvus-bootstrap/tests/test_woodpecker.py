"""Woodpecker onboarding — reuses the operator-cr path (WoodpeckerCluster CR + ConfigMap)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.drivers.woodpecker import WoodpeckerDriver
from milvus_bootstrap.core.models import InstallSpec, Platform, StateClass, TaskStatus
from milvus_bootstrap.core.profile import load_profiles


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def test_woodpecker_profile_loaded(core: Core) -> None:
    assert "woodpecker" in core.registry.kinds()
    assert isinstance(core.registry.get("woodpecker"), WoodpeckerDriver)
    assert core.registry.get("woodpecker").state_class() == StateClass.cache_backed


def test_install_woodpecker_dry_run_builds_cr(core: Core) -> None:
    task = core.install(InstallSpec(kind="woodpecker", name="wp-dev"), dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert [s.name for s in task.steps] == ["precheck-operator", "apply-cr", "wait-status", "register"]
    apply = next(s for s in task.steps if s.name == "apply-cr")
    assert "kind: WoodpeckerCluster" in apply.plan
    assert "configRef" in apply.plan
    assert "woodpecker.yaml" in apply.plan       # the ConfigMap


def test_woodpecker_build_manifests_shape() -> None:
    prof = load_profiles()["woodpecker"]
    drv = WoodpeckerDriver(prof)
    method = prof.method("woodpecker-operator", Platform.k8s)
    mans = drv.build_install_manifests(
        InstallSpec(kind="woodpecker", name="wp1"), method, dict(method.params)
    )
    assert [m["kind"] for m in mans] == ["ConfigMap", "WoodpeckerCluster"]
    cm, cluster = mans
    assert cluster["apiVersion"] == "woodpecker.zilliz.io/v1alpha1"
    assert cluster["spec"]["replicas"] == 3
    assert cluster["spec"]["configRef"]["name"] == "wp1-config"
    body = cm["data"]["woodpecker.yaml"]
    assert "etcd" in body and "minio" in body    # endpoints supplied to the operator


def test_install_woodpecker_apply_registers(core: Core) -> None:
    task = core.install(InstallSpec(kind="woodpecker", name="wp-dev"), dry_run=False)
    assert task.status == TaskStatus.succeeded
    inst = core.state.get_instance("wp-dev")
    assert inst is not None
    assert inst.deps[0].install_method == "woodpecker-operator"
    assert inst.deps[0].state_class == StateClass.cache_backed
