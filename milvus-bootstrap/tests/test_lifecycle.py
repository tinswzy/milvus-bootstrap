"""Lifecycle engine — scale / upgrade / delete on managed instances (fake adapter)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.models import InstallSpec, TaskStatus


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    c = Core()
    c.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    return c


def test_scale_etcd_dry_run_has_guard(core: Core) -> None:
    task = core.scale("etcd-dev", 5, dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert task.steps[0].name == "scale-guard"
    assert "奇数" in task.steps[0].plan          # etcd quorum guard (current=3 -> 5)
    names = [s.name for s in task.steps]
    assert "apply" in names and "wait-ready" in names


def test_scale_etcd_apply_updates_snapshot(core: Core) -> None:
    task = core.scale("etcd-dev", 5, dry_run=False)
    assert task.status == TaskStatus.succeeded
    inst = core.state.get_instance("etcd-dev")
    assert inst.spec_snapshot["params"]["replicaCount"] == 5


def test_delete_etcd_authoritative_guard_then_remove(core: Core) -> None:
    task = core.delete("etcd-dev", dry_run=True)
    assert task.status == TaskStatus.succeeded
    names = [s.name for s in task.steps]
    assert "backup-note" in names and "uninstall" in names   # authoritative + helm
    assert core.state.get_instance("etcd-dev") is not None     # dry-run keeps it
    task2 = core.delete("etcd-dev", dry_run=False)
    assert task2.status == TaskStatus.succeeded
    assert core.state.get_instance("etcd-dev") is None         # apply removes it


def test_upgrade_minio_operator_cr(core: Core) -> None:
    core.install(InstallSpec(kind="minio", name="minio-dev"), dry_run=False)
    task = core.upgrade("minio-dev", "quay.io/minio/minio:RELEASE.2025-01-01", dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert task.steps[0].name == "backup-note"                 # minio authoritative
    apply = next(s for s in task.steps if s.name == "apply-cr")
    assert "RELEASE.2025-01-01" in apply.plan                  # new image in the Tenant CR


def test_delete_minio_operator_cr(core: Core) -> None:
    core.install(InstallSpec(kind="minio", name="minio-dev"), dry_run=False)
    task = core.delete("minio-dev", dry_run=True)
    assert any(s.name == "delete-cr" for s in task.steps)      # operator-cr path


def test_scale_milvus_unsupported(core: Core) -> None:
    core.install(InstallSpec(kind="milvus", name="m1", params={"woodpeckerName": "wp1"}), dry_run=False)
    with pytest.raises(ValueError):
        core.scale("m1", 3, dry_run=True)                      # milvus has no replicas_param


def test_lifecycle_on_unknown_instance(core: Core) -> None:
    with pytest.raises(KeyError):
        core.delete("does-not-exist", dry_run=True)


@pytest.fixture()
def core_milvus_2_5_10(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    c = Core()
    c.install(InstallSpec(kind="milvus", name="milvus-dev",
                          params={"mq": "kafka", "image": "milvusdb/milvus:v2.5.10"}),
              dry_run=False)
    return c


def test_upgrade_blocked_by_upgrade_path(core_milvus_2_5_10: Core) -> None:
    from milvus_bootstrap.core.compat import CompatError
    with pytest.raises(CompatError):
        core_milvus_2_5_10.upgrade("milvus-dev", "milvusdb/milvus:v2.6.18", dry_run=True)


def test_upgrade_path_force_proceeds(core_milvus_2_5_10: Core) -> None:
    task = core_milvus_2_5_10.upgrade("milvus-dev", "milvusdb/milvus:v2.6.18",
                                      dry_run=True, force=True)
    assert task.type == "upgrade"
