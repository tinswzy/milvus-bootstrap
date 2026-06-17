"""kafka + pulsar as installable external MQ deps (helm path, base driver)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.models import InstallSpec, StateClass, TaskStatus


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def test_kafka_pulsar_profiles_loaded(core: Core) -> None:
    assert "kafka" in core.registry.kinds()
    assert "pulsar" in core.registry.kinds()
    assert core.registry.get("kafka").state_class() == StateClass.authoritative
    assert core.registry.get("pulsar").state_class() == StateClass.authoritative


def test_install_kafka_helm_dry_run(core: Core) -> None:
    task = core.install(InstallSpec(kind="kafka", name="kafka-dev"), dry_run=True)
    assert task.status == TaskStatus.succeeded
    names = [s.name for s in task.steps]
    assert "render" in names and "apply" in names      # helm path
    apply = next(s for s in task.steps if s.name == "apply")
    assert "bitnami/kafka" in apply.plan


def test_install_pulsar_helm_dry_run(core: Core) -> None:
    task = core.install(InstallSpec(kind="pulsar", name="pulsar-dev"), dry_run=True)
    assert task.status == TaskStatus.succeeded
    apply = next(s for s in task.steps if s.name == "apply")
    assert "apache/pulsar" in apply.plan


def test_kafka_pulsar_connect_info(core: Core) -> None:
    assert core.registry.get("kafka").connect_info() == "externalKafka.brokerList"
    assert core.registry.get("pulsar").connect_info() == "externalPulsar.endpoint"
