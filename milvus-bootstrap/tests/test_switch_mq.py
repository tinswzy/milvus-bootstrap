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
