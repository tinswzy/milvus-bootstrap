"""Ownership engine — adopt Adoptable candidates; refuse excluded ones."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.models import Ownership, TaskStatus


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def test_adopt_minio_dry_run(core: Core) -> None:
    task = core.adopt("minio", "milvus-minio", dry_run=True)   # fake cluster has a helm minio
    assert task.status == TaskStatus.succeeded
    assert [s.name for s in task.steps] == ["precheck", "write-metadata", "register"]
    assert core.state.get_instance("milvus-minio") is None     # dry-run doesn't register


def test_adopt_minio_apply_registers_managed(core: Core) -> None:
    task = core.adopt("minio", "milvus-minio", dry_run=False)
    assert task.status == TaskStatus.succeeded
    inst = core.state.get_instance("milvus-minio")
    assert inst is not None
    assert inst.ownership == Ownership.managed
    assert inst.deps[0].install_method == "helm-subchart"       # discovered as helm


def test_adopt_control_plane_etcd_refused(core: Core) -> None:
    # the control-plane etcd "etcd" in kube-system is excluded -> never adopted
    with pytest.raises(ValueError):
        core.adopt("etcd", "etcd", dry_run=True)


def test_adopt_then_delete_works(core: Core) -> None:
    core.adopt("minio", "milvus-minio", dry_run=False)
    # adopted instance gets a minimal snapshot, so delete is possible (helm uninstall)
    task = core.delete("milvus-minio", dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert any(s.name == "uninstall" for s in task.steps)
