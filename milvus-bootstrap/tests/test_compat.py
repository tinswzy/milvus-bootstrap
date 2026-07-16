"""Milvus↔MQ version compatibility matrix — unsupported options are non-selectable."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core import compat


def test_2_6_excludes_woodpecker_service():
    opts = {o["id"]: o for o in compat.mq_options("v2.6.3")}
    assert opts["woodpecker-service"]["supported"] is False
    assert "3.0" in opts["woodpecker-service"]["reason"]
    for supported in ("woodpecker-embedded", "kafka", "pulsar", "rocksmq"):
        assert opts[supported]["supported"] is True


def test_3_0_supports_all():
    opts = {o["id"]: o for o in compat.mq_options("v3.0.0")}
    for i in ("woodpecker-service", "woodpecker-embedded", "kafka", "pulsar", "rocksmq"):
        assert opts[i]["supported"] is True


def test_master_or_dev_image_treated_as_newest():
    opts = {o["id"]: o for o in compat.mq_options("milvusdb/milvus:master")}
    assert opts["woodpecker-service"]["supported"] is True


def test_rocksmq_standalone_only():
    opts = {o["id"]: o for o in compat.mq_options("v2.6.3", mode="cluster")}
    assert opts["rocksmq"]["supported"] is False
    assert "standalone" in opts["rocksmq"]["reason"]


def test_check_gates_unsupported():
    with pytest.raises(ValueError):
        compat.check("woodpecker-service", "v2.6.3")          # not supported < 3.0
    assert compat.check("kafka", "v2.6.3").wal == "kafka"
    assert compat.check("woodpecker-service", "v3.0.0").wal == "woodpecker"


def test_check_unknown_mq():
    with pytest.raises(ValueError):
        compat.check("rabbitmq", "v3.0.0")


def test_parse_version():
    assert compat.parse_version("milvusdb/milvus:v2.6.3") == (2, 6, 3)
    assert compat.parse_version("v3.0.0") == (3, 0, 0)
    assert compat.parse_version("master") is None


from milvus_bootstrap.core import compat as _c


def test_version_in_range_basic():
    assert _c.version_in_range("2.6.3", ">=2.6.0,<3.0.0") is True
    assert _c.version_in_range("3.0.0", ">=2.6.0,<3.0.0") is False
    assert _c.version_in_range("2.5.9", ">=2.6.0,<3.0.0") is False
    assert _c.version_in_range("2.6.3", "") is True          # empty range = any


def test_version_in_range_master_is_newest():
    assert _c.version_in_range("milvusdb/milvus:master", ">=3.0.0") is True
    assert _c.version_in_range("master", "<3.0.0") is False   # newest not below 3.0


def test_version_ok_unknown_when_no_bounds():
    assert _c.version_ok("1.3.6", "", "") is None             # unknown
    assert _c.version_ok("1.3.6", "1.0.0", "") is True
    assert _c.version_ok("0.9.0", "1.0.0", "") is False
    assert _c.version_ok("2.0.0", "1.0.0", "1.5.0") is False


def test_load_constraints_reads_yaml():
    cons = _c.load_constraints()
    assert isinstance(cons, list) and cons
    comps = {c.component for c in cons}
    assert {"milvus-operator", "milvus-helm", "k8s"} <= comps
    for c in cons:
        assert c.severity in ("hard", "soft")
        assert c.source in ("confident", "best-effort", "user-table", "docs-tested")


def test_evaluate_unknown_bounds_is_warn_not_fail():
    cons = [_c.Constraint("milvus-operator", "milvus", "r", ">=2.6.0,<3.0.0",
                          "", "", "soft", "user-table", "")]
    out = _c.evaluate({"milvus": "2.6.3", "milvus-operator": "1.3.6"}, cons)
    assert [f.level for f in out] == ["WARN"]


def test_evaluate_hard_fail_when_below_min():
    cons = [_c.Constraint("milvus-operator", "milvus", "r", ">=2.6.0,<3.0.0",
                          "1.4.0", "", "hard", "user-table", "")]
    out = _c.evaluate({"milvus": "2.6.3", "milvus-operator": "1.3.6"}, cons)
    assert out and out[0].level == "FAIL" and out[0].component == "milvus-operator"


def test_evaluate_pass_and_skip():
    cons = [_c.Constraint("milvus-operator", "milvus", "r", ">=2.6.0,<3.0.0",
                          "1.0.0", "", "hard", "user-table", "")]
    assert _c.evaluate({"milvus": "2.6.3", "milvus-operator": "1.3.6"}, cons)[0].level == "PASS"
    assert _c.evaluate({"milvus": "2.6.3"}, cons)[0].level == "SKIP"   # operator absent


def test_evaluate_constraint_out_of_milvus_range_is_dropped():
    cons = [_c.Constraint("milvus-operator", "milvus", "r", ">=3.0.0",
                          "1.4.0", "", "hard", "user-table", "")]
    assert _c.evaluate({"milvus": "2.6.3", "milvus-operator": "1.3.6"}, cons) == []


def test_gate_switch_mq_same_type_blocks():
    with pytest.raises(_c.CompatError):
        _c.gate("switch-mq", {"current_wal": "kafka", "target_wal": "kafka"})


def test_gate_switch_mq_same_type_force_warns():
    out = _c.gate("switch-mq", {"current_wal": "kafka", "target_wal": "kafka"}, force=True)
    assert out and out[0].level == "WARN"


def test_gate_switch_mq_different_ok():
    assert _c.gate("switch-mq", {"current_wal": "kafka", "target_wal": "pulsar"}) == []


def test_gate_switch_mq_kafka_below_2619_blocks():
    """切换到 kafka 在 milvus <2.6.19 上会 CrashLoop（kafka 负数点位 uint64 解码 bug）→ 拦下。"""
    with pytest.raises(_c.CompatError):
        _c.gate("switch-mq", {"current_wal": "pulsar", "target_wal": "kafka",
                              "milvus_version": "milvusdb/milvus:v2.6.18"})


def test_gate_switch_mq_kafka_2619_ok():
    """2.6.19 已修 → 允许。"""
    assert _c.gate("switch-mq", {"current_wal": "pulsar", "target_wal": "kafka",
                                 "milvus_version": "milvusdb/milvus:v2.6.19"}) == []


def test_gate_switch_mq_kafka_below_2619_force_warns():
    out = _c.gate("switch-mq", {"current_wal": "pulsar", "target_wal": "kafka",
                                "milvus_version": "v2.6.18"}, force=True)
    assert out and out[0].level == "WARN"


def test_gate_switch_mq_pulsar_target_below_2619_ok():
    """bug 是 kafka 目标特有的；切到 pulsar 不受版本门限制。"""
    assert _c.gate("switch-mq", {"current_wal": "kafka", "target_wal": "pulsar",
                                 "milvus_version": "v2.6.18"}) == []


def test_gate_switch_mq_kafka_unknown_version_ok():
    """无版本信息（未知/master/dev）→ 不拦（视为最新）。"""
    assert _c.gate("switch-mq", {"current_wal": "pulsar", "target_wal": "kafka"}) == []


def test_gate_install_incompatible_mq_blocks():
    with pytest.raises(_c.CompatError):
        _c.gate("install", {"mq": "woodpecker-service", "image": "v2.6.3", "mode": "standalone"})


def test_gate_install_force_bypasses_mq():
    out = _c.gate("install", {"mq": "woodpecker-service", "image": "v2.6.3",
                              "mode": "standalone"}, force=True)
    assert any(f.level == "WARN" for f in out)


def test_yaml_has_soft_semver_floors():
    cons = {(c.component, c.min): c for c in _c.load_constraints()}
    etcd = next(c for (comp, _), c in cons.items() if comp == "etcd")
    assert etcd.min == "3.5.0" and etcd.severity == "soft"
    pulsar = next(c for (comp, _), c in cons.items() if comp == "pulsar")
    assert pulsar.min == "2.8.2" and pulsar.severity == "soft"
    k8s = next(c for (comp, _), c in cons.items() if comp == "k8s")
    assert k8s.min == "1.16.0" and k8s.severity == "soft"
    helm = next(c for (comp, _), c in cons.items() if comp == "helm")
    assert helm.min == "3.0.0" and helm.severity == "soft"


def test_evaluate_soft_floor_warns_when_below():
    cons = _c.load_constraints()
    out = _c.evaluate({"milvus": "2.6.18", "etcd": "3.4.0"}, cons)
    etcd = [f for f in out if f.component == "etcd"]
    assert etcd and etcd[0].level == "WARN"   # soft floor, below min -> WARN not FAIL
    out2 = _c.evaluate({"milvus": "2.6.18", "etcd": "3.5.25"}, cons)
    assert [f for f in out2 if f.component == "etcd"][0].level == "PASS"


def test_parse_minio_release():
    assert _c.parse_minio_release("RELEASE.2024-12-18T13-15-44Z") == (2024, 12, 18, 13, 15, 44)
    assert _c.parse_minio_release("3.5.0") is None


def test_minio_release_ok():
    m = "RELEASE.2024-12-18T13-15-44Z"
    assert _c.minio_release_ok("RELEASE.2025-01-01T00-00-00Z", m, "") is True
    assert _c.minio_release_ok("RELEASE.2024-06-01T00-00-00Z", m, "") is False
    assert _c.minio_release_ok("RELEASE.2024-12-18T13-15-44Z", m, "") is True
    assert _c.minio_release_ok("garbage", m, "") is None   # unparseable -> unknown


def test_evaluate_minio_release_kind():
    cons = [_c.Constraint("minio", "milvus", "wp+minio S3 conditional write",
                          ">=2.6.0", "RELEASE.2024-12-18T13-15-44Z", "", "soft",
                          "docs-tested", "", kind="minio-release")]
    below = _c.evaluate({"milvus": "2.6.18", "minio": "RELEASE.2024-06-01T00-00-00Z"}, cons)
    assert below and below[0].level == "WARN"      # soft
    ok = _c.evaluate({"milvus": "2.6.18", "minio": "RELEASE.2025-01-01T00-00-00Z"}, cons)
    assert ok[0].level == "PASS"


def test_gate_upgrade_path_blocks_big_jump():
    with pytest.raises(_c.CompatError):
        _c.gate("upgrade", {"current": "2.5.10", "target": "2.6.18"})   # need >=2.5.16


def test_gate_upgrade_path_ok_when_prereq_met():
    assert _c.gate("upgrade", {"current": "2.5.16", "target": "2.6.18"}) == []


def test_gate_upgrade_path_to_3_0_needs_2_5_16():
    with pytest.raises(_c.CompatError):
        _c.gate("upgrade", {"current": "2.4.9", "target": "3.0.0"})


def test_gate_upgrade_path_force_warns():
    out = _c.gate("upgrade", {"current": "2.5.10", "target": "2.6.18"}, force=True)
    assert any(f.level == "WARN" for f in out)


def test_gate_install_woodpecker_minio_too_old_blocks():
    with pytest.raises(_c.CompatError):
        _c.gate("install", {"mq": "woodpecker-embedded", "image": "v2.6.18",
                            "versions": {"milvus": "2.6.18",
                                         "minio": "RELEASE.2024-06-01T00-00-00Z"}})


def test_gate_install_woodpecker_minio_new_ok():
    out = _c.gate("install", {"mq": "woodpecker-embedded", "image": "v2.6.18",
                              "versions": {"milvus": "2.6.18",
                                           "minio": "RELEASE.2025-01-01T00-00-00Z"}})
    assert all(f.level != "FAIL" for f in out)


def test_gate_install_kafka_minio_old_not_blocked():
    # non-woodpecker mq => the minio date rule does not hard-block
    out = _c.gate("install", {"mq": "kafka", "image": "v2.6.18",
                              "versions": {"milvus": "2.6.18",
                                           "minio": "RELEASE.2024-06-01T00-00-00Z"}})
    assert all(f.level != "FAIL" for f in out)


def test_switch_mq_targets_same_wal_not_selectable():
    from milvus_bootstrap.core import compat
    ts = {t["id"]: t for t in compat.switch_mq_targets("kafka", "2.6.0", "standalone")}
    assert ts["kafka"]["selectable"] is False and "相同" in ts["kafka"]["reason"]
    assert ts["kafka"]["current"] is True
    assert ts["pulsar"]["selectable"] is True and ts["pulsar"]["current"] is False


def test_switch_mq_targets_rocksmq_cluster_blocked():
    from milvus_bootstrap.core import compat
    ts = {t["id"]: t for t in compat.switch_mq_targets("kafka", "2.6.0", "cluster")}
    assert ts["rocksmq"]["selectable"] is False and "standalone" in ts["rocksmq"]["reason"]


def test_switch_mq_targets_woodpecker_service_reserved():
    from milvus_bootstrap.core import compat
    # milvus 2.x: blocked by min_milvus (needs 3.0)
    lo = {t["id"]: t for t in compat.switch_mq_targets("kafka", "2.6.0", "standalone")}
    assert lo["woodpecker-service"]["selectable"] is False
    # milvus 3.0: version ok, but reserved-off (external woodpecker not supported yet)
    hi = {t["id"]: t for t in compat.switch_mq_targets("kafka", "3.0.0", "standalone")}
    assert hi["woodpecker-service"]["selectable"] is False
    assert "规划中" in hi["woodpecker-service"]["reason"] or "external" in hi["woodpecker-service"]["reason"]


def test_operator_supports_ext_woodpecker_reserved_false():
    from milvus_bootstrap.core import compat
    assert compat._operator_supports_ext_woodpecker("1.3.6") is False
    assert compat._operator_supports_ext_woodpecker("") is False


def test_switch_mq_targets_embedded_flag():
    from milvus_bootstrap.core import compat
    ts = {t["id"]: t for t in compat.switch_mq_targets("pulsar", "2.6.0", "standalone")}
    assert ts["kafka"]["embedded"] is False and ts["pulsar"]["embedded"] is False
    assert ts["rocksmq"]["embedded"] is True and ts["woodpecker-embedded"]["embedded"] is True
