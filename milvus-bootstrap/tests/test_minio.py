"""MinIO onboarding: operator-cr path (Tenant CR build + apply/wait via fake)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.drivers.minio import MinioDriver
from milvus_bootstrap.core.models import InstallSpec, Ownership, Platform, StateClass, TaskStatus
from milvus_bootstrap.core.profile import load_profiles


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def test_minio_profile_loaded(core: Core) -> None:
    assert "minio" in core.registry.kinds()
    assert isinstance(core.registry.get("minio"), MinioDriver)


def test_discover_minio_as_helm(core: Core) -> None:
    minios = [c for c in core.discover() if c.kind == "minio"]
    assert len(minios) == 1
    assert minios[0].ownership == Ownership.adoptable
    assert minios[0].install_method == "helm-subchart"      # discovered via helm labels
    assert minios[0].state_class == StateClass.authoritative


def test_install_minio_dry_run_builds_tenant(core: Core) -> None:
    task = core.install(InstallSpec(kind="minio", name="minio-dev"), dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert [s.name for s in task.steps] == ["precheck-operator", "apply-cr", "wait-status", "register"]
    apply = next(s for s in task.steps if s.name == "apply-cr")
    assert "kind: Tenant" in apply.plan
    assert "pools" in apply.plan


def test_install_minio_apply_registers_operator(core: Core) -> None:
    task = core.install(InstallSpec(kind="minio", name="minio-dev"), dry_run=False)
    assert task.status == TaskStatus.succeeded
    inst = core.state.get_instance("minio-dev")
    assert inst is not None
    assert inst.deps[0].install_method == "minio-operator"   # we install via operator
    assert inst.deps[0].state_class == StateClass.authoritative


def test_minio_build_manifests_shape() -> None:
    prof = load_profiles()["minio"]
    drv = MinioDriver(prof)
    method = prof.method("minio-operator", Platform.k8s)
    mans = drv.build_install_manifests(
        InstallSpec(kind="minio", name="m1"), method, dict(method.params)
    )
    kinds = [m["kind"] for m in mans]
    assert kinds == ["Secret", "Tenant"]
    tenant = mans[1]
    assert tenant["apiVersion"] == "minio.min.io/v2"
    assert tenant["spec"]["pools"][0]["servers"] == 4
    assert tenant["spec"]["configuration"]["name"] == "m1-env"
