"""Milvus onboarding — operator-cr install with all deps external (etcd/minio/woodpecker)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.drivers.milvus import MilvusDriver, woodpecker_seeds
from milvus_bootstrap.core.models import InstallSpec, Platform, StateClass, TaskStatus
from milvus_bootstrap.core.profile import load_profiles


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def test_milvus_profile_loaded(core: Core) -> None:
    assert "milvus" in core.registry.kinds()
    assert isinstance(core.registry.get("milvus"), MilvusDriver)
    assert core.registry.get("milvus").state_class() == StateClass.stateless


def test_woodpecker_seed_computation() -> None:
    assert woodpecker_seeds("wp-dev", 3, "default") == [
        "wp-dev-server-0.wp-dev-server-headless.default.svc:18080",
        "wp-dev-server-1.wp-dev-server-headless.default.svc:18080",
        "wp-dev-server-2.wp-dev-server-headless.default.svc:18080",
    ]


def test_install_milvus_dry_run_all_external(core: Core) -> None:
    task = core.install(InstallSpec(kind="milvus", name="milvus-dev", params={
        "etcdEndpoints": "etcd-dev:2379",
        "storageEndpoint": "minio-dev:9000",
        "woodpeckerName": "wp-dev",
        "woodpeckerReplicas": "3",
    }), dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert [s.name for s in task.steps] == ["precheck-operator", "apply-cr", "wait-status", "register"]
    apply = next(s for s in task.steps if s.name == "apply-cr")
    assert "kind: Milvus" in apply.plan
    assert "msgStreamType: woodpecker" in apply.plan
    assert "external: true" in apply.plan                 # etcd + storage external
    assert "wp-dev-server-0.wp-dev-server-headless" in apply.plan  # computed seeds
    # ready gate is the Milvus CR health status
    wait = next(s for s in task.steps if s.name == "wait-status")
    assert "status.status" in wait.plan and "Healthy" in wait.plan


def test_milvus_build_manifests_all_external() -> None:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    params = {**method.params, "woodpeckerName": "wp1"}
    mans = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)

    kinds = [m["kind"] for m in mans]
    assert kinds == ["Secret", "Milvus"]
    secret = mans[0]
    assert "accesskey" in secret["stringData"] and "secretkey" in secret["stringData"]

    cr = mans[1]
    assert cr["apiVersion"] == "milvus.io/v1beta1"
    deps = cr["spec"]["dependencies"]
    assert deps["msgStreamType"] == "woodpecker"
    assert deps["etcd"]["external"] is True
    assert deps["storage"]["external"] is True
    assert deps["storage"]["secretRef"] == "m1-minio"
    wp = deps["woodpecker"]["external"]
    assert wp["replicaCount"] == 3
    assert wp["endpoints"][0] == "wp1-server-0.wp1-server-headless.default.svc:18080"


def test_install_milvus_apply_registers(core: Core) -> None:
    task = core.install(InstallSpec(kind="milvus", name="milvus-dev",
                                    params={"woodpeckerName": "wp-dev"}), dry_run=False)
    assert task.status == TaskStatus.succeeded
    inst = core.state.get_instance("milvus-dev")
    assert inst is not None
    assert inst.deps[0].install_method == "milvus-operator"
    assert inst.deps[0].state_class == StateClass.stateless
