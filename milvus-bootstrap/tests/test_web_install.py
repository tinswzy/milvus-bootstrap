import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.server.app import app
    with TestClient(app) as c:
        yield c


def test_install_dry_run_returns_planned_task(client):
    r = client.post("/api/install", json={"kind": "etcd", "name": "e1", "dry_run": True})
    assert r.status_code == 200
    task = r.json()["task"]
    assert task["dry_run"] is True and task["steps"]


def test_install_apply_async_then_poll(client):
    r = client.post("/api/install", json={"kind": "etcd", "name": "e2", "dry_run": False})
    assert r.status_code == 202
    tid = r.json()["task_id"]
    assert r.json()["state"] == "running"
    # poll to completion (fake adapter installs fast)
    end = time.monotonic() + 5
    state = "running"
    while time.monotonic() < end:
        j = client.get(f"/api/task/{tid}").json()
        state = j["state"]
        if state != "running":
            break
        time.sleep(0.05)
    assert state == "succeeded"
    assert client.get(f"/api/task/{tid}").json()["task"]["target"] == "e2"


def test_task_unknown_id_404(client):
    assert client.get("/api/task/nope").status_code == 404


def test_install_milvus_incompatible_mq_returns_409(client):
    r = client.post("/api/install", json={
        "kind": "milvus", "name": "m1", "dry_run": True,
        "params": {"mq": "woodpecker-service", "image": "milvusdb/milvus:v2.6.3"}})
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "compat" and body["force_hint"] is True
    assert "3.0" in body["reason"]           # woodpecker-service needs milvus >= 3.0


def test_install_milvus_force_bypasses_gate(client):
    r = client.post("/api/install", json={
        "kind": "milvus", "name": "m2", "dry_run": True, "force": True,
        "params": {"mq": "woodpecker-service", "image": "milvusdb/milvus:v2.6.3"}})
    assert r.status_code == 200          # force -> gate downgraded, dry-run proceeds
    assert r.json()["task"]["dry_run"] is True
