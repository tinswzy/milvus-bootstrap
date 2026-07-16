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


def test_switch_mq_steps_have_no_destructive_compensate(core_with_milvus_kafka) -> None:
    """CRITICAL: the switch reuses install steps, but a failed step (e.g. verify-mq-type timeout)
    must NOT roll back by deleting the PRE-EXISTING Milvus CR. So no switch step may carry a compensate."""
    from milvus_bootstrap.core.models import InstallSpec
    c = core_with_milvus_kafka
    spec = InstallSpec.model_validate(c.state.get_instance("milvus-dev").spec_snapshot)
    steps = c.registry.get("milvus").plan_switch_mq_steps(spec, c.adapter, "pulsar")
    assert steps and all(s.compensate is None for s in steps)


def test_switch_apply_cr_injects_target_conn_keeps_source_msgstreamtype(core_with_milvus_kafka) -> None:
    """kafka(源)→pulsar(目标)：apply-cr 渲染的 CR 必须注入 pulsar 连接、且 msgStreamType 仍是 kafka。
    这是本次纠正的核心——绝不能翻成 pulsar（③a 的翻类型 bug 会让 milvus 读旧 checkpoint 崩）。"""
    c = core_with_milvus_kafka
    task = c.switch_mq("milvus-dev", "pulsar", target_name="pulsar-dev", target_ns="default", dry_run=True)
    apply_plan = next(s.plan for s in task.steps if s.name == "apply-cr")
    assert "msgStreamType: kafka" in apply_plan          # 源类型保持，未翻
    assert "msgStreamType: pulsar" not in apply_plan     # 绝未翻成目标
    assert "pulsar://pulsar-dev-broker.default.svc" in apply_plan   # 目标连接已注入
    assert "6650" in apply_plan


def test_switch_apply_cr_kafka_target_injects_brokerlist(core: Core) -> None:
    """源 woodpecker→目标 kafka：注入 kafka.brokerList，msgStreamType 不变成 kafka。"""
    task = core.switch_mq("milvus-dev", "kafka", target_name="kafka-dev", target_ns="default", dry_run=True)
    apply_plan = next(s.plan for s in task.steps if s.name == "apply-cr")
    assert "brokerList: kafka-dev.default.svc:9092" in apply_plan
    assert "msgStreamType: kafka" not in apply_plan      # 源是 woodpecker，未翻成 kafka


def test_mq_conn_conf_kafka(core: Core) -> None:
    d = core.registry.get("milvus")
    assert d._mq_conn_conf("kafka", "kafka-dev.default.svc:9092") == {
        "kafka.brokerList": "kafka-dev.default.svc:9092"}


def test_mq_conn_conf_pulsar_splits_host_port(core: Core) -> None:
    d = core.registry.get("milvus")
    conf = d._mq_conn_conf("pulsar", "pulsar-dev-broker.default.svc:6650")
    assert conf == {"pulsar.address": "pulsar://pulsar-dev-broker.default.svc",
                    "pulsar.port": 6650}


def test_mq_conn_conf_embedded_empty_and_no_msgstreamtype(core: Core) -> None:
    d = core.registry.get("milvus")
    assert d._mq_conn_conf("rocksmq", "") == {}
    assert d._mq_conn_conf("woodpecker", "") == {}
    # 关键不变式：连接配置绝不含 msgStreamType（那是 wal/alter 运行时的事）
    assert "msgStreamType" not in d._mq_conn_conf("kafka", "k.default.svc:9092")
