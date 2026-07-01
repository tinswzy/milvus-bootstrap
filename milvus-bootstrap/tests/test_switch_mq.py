"""switch-mq — runtime MQ/WAL switch via Milvus management API (the ★ flow)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.models import InstallSpec, TaskStatus


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    c = Core()
    c.install(InstallSpec(kind="milvus", name="milvus-dev", params={"woodpeckerName": "wp-dev"}), dry_run=False)
    return c


def test_switch_mq_dry_run_steps(core: Core) -> None:
    task = core.switch_mq("milvus-dev", "kafka", dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert [s.name for s in task.steps] == ["precheck-target", "wal-alter", "verify", "decommission-old"]
    wal = next(s for s in task.steps if s.name == "wal-alter")
    assert "wal/alter" in wal.plan
    assert "target_wal_name" in wal.plan and "kafka" in wal.plan


def test_switch_mq_apply_execs_wal_alter(core: Core) -> None:
    task = core.switch_mq("milvus-dev", "woodpecker", dry_run=False)
    assert task.status == TaskStatus.succeeded
    wal = next(s for s in task.steps if s.name == "wal-alter")
    assert "exec" in wal.detail            # fake adapter recorded the exec


def test_switch_mq_only_for_milvus(core: Core) -> None:
    core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    with pytest.raises(ValueError):
        core.switch_mq("etcd-dev", "kafka", dry_run=True)


@pytest.fixture()
def core_with_milvus_kafka(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    c = Core()
    c.install(InstallSpec(kind="milvus", name="milvus-dev", params={"mq": "kafka"}), dry_run=False)
    return c


def test_same_type_same_mq_raises_compat_error(core_with_milvus_kafka: Core) -> None:
    """kafka→kafka should raise CompatError (same-type gate)."""
    from milvus_bootstrap.core.compat import CompatError
    with pytest.raises(CompatError):
        core_with_milvus_kafka.switch_mq("milvus-dev", "kafka", dry_run=True)


def test_same_type_same_mq_force_returns_task(core_with_milvus_kafka: Core) -> None:
    """kafka→kafka with force=True should bypass the gate and return a task."""
    task = core_with_milvus_kafka.switch_mq("milvus-dev", "kafka", dry_run=True, force=True)
    assert task.status == TaskStatus.succeeded


def test_different_mq_cross_type_returns_task(core_with_milvus_kafka: Core) -> None:
    """kafka→pulsar (different types) should pass the gate and return a task."""
    task = core_with_milvus_kafka.switch_mq("milvus-dev", "pulsar", dry_run=True)
    assert task.status == TaskStatus.succeeded
