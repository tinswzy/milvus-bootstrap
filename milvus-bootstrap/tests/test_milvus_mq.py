"""MilvusDriver MQ wiring (kafka/pulsar/woodpecker) + version gating."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.drivers.milvus import MilvusDriver
from milvus_bootstrap.core.models import InstallSpec, Platform, TaskStatus
from milvus_bootstrap.core.profile import load_profiles


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def _milvus_cr(params: dict) -> dict:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    mans = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)
    return next(x for x in mans if x["kind"] == "Milvus")


def test_kafka_wiring_on_2_6():
    cr = _milvus_cr({"image": "milvusdb/milvus:v2.6.3", "mq": "kafka", "kafkaBrokers": "e2e-kafka:9092"})
    deps = cr["spec"]["dependencies"]
    assert deps["msgStreamType"] == "kafka"
    assert deps["kafka"]["external"] is True
    assert deps["kafka"]["brokerList"] == ["e2e-kafka:9092"]
    assert "woodpecker" not in deps


def test_pulsar_wiring_on_2_6():
    cr = _milvus_cr({"image": "milvusdb/milvus:v2.6.3", "mq": "pulsar", "pulsarEndpoint": "e2e-pulsar:6650"})
    deps = cr["spec"]["dependencies"]
    assert deps["msgStreamType"] == "pulsar"
    assert deps["pulsar"]["endpoint"] == "e2e-pulsar:6650"


def test_woodpecker_service_blocked_on_2_6():
    # Gate check is enforced in provisioner.install via compat.gate (not in build_install_manifests).
    # Verify compat.gate raises CompatError (subclass of ValueError) for this combination.
    from milvus_bootstrap.core.compat import CompatError, gate
    with pytest.raises(CompatError):
        gate("install", {"mq": "woodpecker-service", "image": "milvusdb/milvus:v2.6.3",
                         "mode": "standalone", "versions": {}})


def test_woodpecker_service_ok_on_3_0():
    cr = _milvus_cr({"image": "milvusdb/milvus:v3.0.0", "mq": "woodpecker-service", "woodpeckerName": "wp"})
    deps = cr["spec"]["dependencies"]
    assert deps["msgStreamType"] == "woodpecker"
    assert deps["woodpecker"]["external"]["endpoints"][0].startswith("wp-server-0")


def test_install_kafka_dry_run(core: Core) -> None:
    task = core.install(InstallSpec(kind="milvus", name="m1",
                        params={"image": "milvusdb/milvus:v2.6.3", "mq": "kafka", "kafkaBrokers": "k:9092"}),
                        dry_run=True)
    assert task.status == TaskStatus.succeeded
    apply = next(s for s in task.steps if s.name == "apply-cr")
    assert "msgStreamType: kafka" in apply.plan


def test_install_unsupported_mq_raises(core: Core) -> None:
    with pytest.raises(ValueError):
        core.install(InstallSpec(kind="milvus", name="m1",
                     params={"image": "milvusdb/milvus:v2.6.3", "mq": "woodpecker-service"}),
                     dry_run=True)
