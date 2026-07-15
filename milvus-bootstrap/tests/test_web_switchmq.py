from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def _client_with_kafka_milvus(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    client = TestClient(app)
    client.__enter__()
    _core().install(InstallSpec(kind="milvus", name="mq-mv",
                                params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
    return client


def test_api_mq_options_shape(tmp_path, monkeypatch):
    client = _client_with_kafka_milvus(tmp_path, monkeypatch)
    try:
        r = client.get("/api/mq-options", params={"instance": "mq-mv"})
        assert r.status_code == 200
        body = r.json()
        assert body["current_mq"] == "kafka" and body["current_wal"] == "kafka"
        ids = [o["id"] for o in body["options"]]
        assert "kafka" in ids and "pulsar" in ids
        assert all({"id", "wal", "label", "supported"} <= set(o) for o in body["options"])
    finally:
        client.__exit__(None, None, None)


def test_api_switch_mq_dry_run_compatible(tmp_path, monkeypatch):
    client = _client_with_kafka_milvus(tmp_path, monkeypatch)
    try:
        r = client.post("/api/switch-mq", json={"instance": "mq-mv", "target_wal": "pulsar", "dry_run": True})
        assert r.status_code == 200
        assert len(r.json()["task"]["steps"]) >= 1
    finally:
        client.__exit__(None, None, None)


def test_api_switch_mq_same_wal_gate_409_then_force_202(tmp_path, monkeypatch):
    client = _client_with_kafka_milvus(tmp_path, monkeypatch)
    try:
        # kafka -> kafka blocked by gate (same-type) -> 409
        r = client.post("/api/switch-mq", json={"instance": "mq-mv", "target_wal": "kafka", "dry_run": False})
        assert r.status_code == 409 and r.json()["error"] == "compat"
        # with force -> 202
        r2 = client.post("/api/switch-mq", json={"instance": "mq-mv", "target_wal": "kafka",
                                                 "dry_run": False, "force": True})
        assert r2.status_code == 202 and "task_id" in r2.json()
    finally:
        client.__exit__(None, None, None)


def test_api_switch_mq_unknown_400(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.post("/api/switch-mq", json={"instance": "nope", "target_wal": "kafka"})
        assert r.status_code == 400


def test_api_switch_mq_targets_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="milvus", name="sw-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/switch-mq/targets", params={"instance": "sw-mv"})
        assert r.status_code == 200
        body = r.json()
        assert body["current_mq"] == "kafka" and body["current_wal"] == "kafka"
        ts = {t["id"]: t for t in body["targets"]}
        assert ts["kafka"]["selectable"] is False        # same as current
        assert ts["pulsar"]["selectable"] is True
        assert "current" in ts["kafka"] and "reason" in ts["kafka"]


def test_api_switch_mq_targets_unknown_400(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.get("/api/switch-mq/targets", params={"instance": "nope"})
        assert r.status_code == 400


def test_api_switch_mq_targets_lists_instances(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="kafka", name="kafka-dev"), dry_run=False)
        _core().install(InstallSpec(kind="milvus", name="sw-mv",
                                    params={"mq": "pulsar", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/switch-mq/targets", params={"instance": "sw-mv"})
        assert r.status_code == 200
        ts = {t["id"]: t for t in r.json()["targets"]}
        assert ts["kafka"]["embedded"] is False
        names = [x["name"] for x in ts["kafka"]["instances"]]
        assert "kafka-dev" in names
        ep = [x["endpoint"] for x in ts["kafka"]["instances"] if x["name"] == "kafka-dev"][0]
        assert ep.startswith("kafka-dev.") and ":9092" in ep
        assert ts["rocksmq"]["embedded"] is True and ts["rocksmq"]["instances"] == []
