"""Config engine — get / set / restart (milvus -> spec.config; helm -> values)."""
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


def test_config_get_returns_data(core: Core) -> None:
    cfg = core.config_get("milvus-dev")
    assert isinstance(cfg, dict) and cfg


def test_config_set_milvus_into_spec_config(core: Core) -> None:
    task = core.config_set("milvus-dev", {"log.level": "debug"}, dry_run=True)
    assert task.status == TaskStatus.succeeded
    names = [s.name for s in task.steps]
    assert names[0] == "config-diff" and "restart-note" in names
    apply = next(s for s in task.steps if s.name == "apply-cr")
    assert "config" in apply.plan and "level: debug" in apply.plan   # CR spec.config (dotted→nested)


def test_config_set_milvus_apply_persists(core: Core) -> None:
    core.config_set("milvus-dev", {"log.level": "debug"}, dry_run=False)
    inst = core.state.get_instance("milvus-dev")
    assert inst.spec_snapshot["params"]["_conf"]["log.level"] == "debug"


def test_config_set_helm_component_into_params(core: Core) -> None:
    core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    task = core.config_set("etcd-dev", {"autoCompactionRetention": "2000"}, dry_run=True)
    assert task.status == TaskStatus.succeeded
    apply = next(s for s in task.steps if s.name == "apply")    # helm path
    assert "autoCompactionRetention" in apply.plan


def test_config_restart(core: Core) -> None:
    task = core.config_restart("milvus-dev", dry_run=False)
    assert task.status == TaskStatus.succeeded
    assert task.steps[0].name == "rollout-restart"
    assert "restart" in task.steps[0].detail
