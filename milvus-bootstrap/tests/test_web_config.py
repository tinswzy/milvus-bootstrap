from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def _client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return TestClient(app)


def test_api_config_get_shape(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with _client(tmp_path, monkeypatch) as client:
        _core().install(InstallSpec(kind="milvus", name="cfg-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/config", params={"instance": "cfg-mv"})
        assert r.status_code == 200
        body = r.json()
        assert body["instance"] == "cfg-mv"
        assert "current" in body                      # may be None under fake
        assert isinstance(body["overrides"], dict)


def test_api_config_get_unknown_400(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        r = client.get("/api/config", params={"instance": "nope"})
        assert r.status_code == 400


def test_api_config_set_dry_run_and_apply(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with _client(tmp_path, monkeypatch) as client:
        _core().install(InstallSpec(kind="milvus", name="cfg-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        # dry-run -> 200 planned task with steps
        r = client.post("/api/config/set", json={"instance": "cfg-mv",
                                                 "kv": {"proxy.maxNameLength": "255"}, "dry_run": True})
        assert r.status_code == 200
        task = r.json()["task"]
        assert task["dry_run"] is True and len(task["steps"]) >= 1
        # apply -> 202 with task_id
        r2 = client.post("/api/config/set", json={"instance": "cfg-mv",
                                                  "kv": {"proxy.maxNameLength": "255"}, "dry_run": False})
        assert r2.status_code == 202 and "task_id" in r2.json()
