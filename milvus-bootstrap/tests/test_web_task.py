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
