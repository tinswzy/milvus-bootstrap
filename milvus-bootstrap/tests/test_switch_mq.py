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
    names = [s.name for s in task.steps]
    # milvus uses the operator-cr install method: apply-cr + wait-status come from plan_install_steps
    assert "apply-cr" in names and "wait-status" in names      # config apply + wait first
    assert "wal-alter" in names and "verify-mq-type" in names
    assert "decommission-old" not in names                     # decommission is now optional/manual
    assert names[-1] == "verify-mq-type"                       # workflow completes at verify
    wal = next(s for s in task.steps if s.name == "wal-alter")
    assert "wal/alter" in wal.plan and "target_wal_name" in wal.plan and "kafka" in wal.plan


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


def test_switch_mq_injects_endpoint_and_updates_snapshot(core_with_milvus_kafka) -> None:
    # milvus-dev is on kafka; switch it to pulsar targeting a specific instance
    c = core_with_milvus_kafka
    task = c.switch_mq("milvus-dev", "pulsar", target_name="pulsar-dev", target_ns="default", dry_run=False)
    assert task.status == TaskStatus.succeeded
    snap = c.state.get_instance("milvus-dev").spec_snapshot
    assert snap["params"]["mq"] == "pulsar"
    assert "pulsar-dev" in snap["params"]["pulsarEndpoint"] and ":6650" in snap["params"]["pulsarEndpoint"]
    assert [s.name for s in task.steps][-1] == "verify-mq-type"


def test_switch_mq_embedded_no_endpoint(core_with_milvus_kafka) -> None:
    c = core_with_milvus_kafka
    task = c.switch_mq("milvus-dev", "rocksmq", dry_run=False)      # embedded, no target instance
    assert task.status == TaskStatus.succeeded
    snap = c.state.get_instance("milvus-dev").spec_snapshot
    assert snap["params"]["mq"] == "rocksmq"
    assert "pulsarEndpoint" not in snap["params"]                  # embedded switch injects no endpoint
