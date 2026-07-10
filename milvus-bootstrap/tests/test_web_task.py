from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app, runner


def test_api_task_running_returns_partial_steps():
    # seed a running record with a partial task snapshot (as the sink would)
    with runner._lock:
        runner._recs["t-run"] = {
            "state": "running", "result": None, "error": None,
            "partial": {"status": "running", "steps": [{"name": "render", "status": "ok",
                                                        "plan": "将执行：helm ...", "detail": "ok"}]},
        }
    r = TestClient(app).get("/api/task/t-run")
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "running"
    assert body["task"]["steps"][0]["name"] == "render"


def test_api_task_unknown_404():
    r = TestClient(app).get("/api/task/does-not-exist")
    assert r.status_code == 404


def test_api_delete_dry_run_returns_planned_task(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.server.app import _core
    # pick any managed instance if present; otherwise assert the 200/{task} shape on unknown is a ValueError->400
    with TestClient(app) as client:
        insts = _core().state.list_instances()
        if insts:
            name = insts[0].name
            r = client.post("/api/delete", json={"instance": name, "dry_run": True})
            assert r.status_code == 200
            assert "task" in r.json() and "steps" in r.json()["task"]
        else:
            r = client.post("/api/delete", json={"instance": "nope", "dry_run": True})
            assert r.status_code == 400
