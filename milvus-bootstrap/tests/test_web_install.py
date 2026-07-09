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


def test_delete_async_then_gone(client):
    import time
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="etcd", name="etcd-del"), dry_run=False)
    r = client.post("/api/delete", json={"instance": "etcd-del"})
    assert r.status_code == 202
    tid = r.json()["task_id"]
    end = time.monotonic() + 5
    state = "running"
    while time.monotonic() < end:
        state = client.get(f"/api/task/{tid}").json()["state"]
        if state != "running":
            break
        time.sleep(0.05)
    assert state == "succeeded"
    names = [i["name"] for i in client.get("/api/instances").json()["instances"]]
    assert "etcd-del" not in names


def test_delete_unknown_instance_400(client):
    r = client.post("/api/delete", json={"instance": "nope"})
    assert r.status_code == 400 and r.json()["error"] == "bad_request"


def test_api_upgrade_dry_run_and_apply(client):
    import time
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="milvus", name="mv-up", params={
        "mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
    # dry-run → 200 with a task plan
    r = client.post("/api/upgrade", json={"instance": "mv-up", "image": "milvusdb/milvus:v2.6.20", "dry_run": True})
    assert r.status_code == 200 and "task" in r.json()
    # apply → 202 → poll to completion
    r = client.post("/api/upgrade", json={"instance": "mv-up", "image": "milvusdb/milvus:v2.6.20", "dry_run": False})
    assert r.status_code == 202
    tid = r.json()["task_id"]
    end = time.monotonic() + 5
    state = "running"
    while time.monotonic() < end:
        state = client.get(f"/api/task/{tid}").json()["state"]
        if state != "running":
            break
        time.sleep(0.05)
    assert state == "succeeded"


def test_api_upgrade_gate_409_and_unknown_400(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    # a woodpecker-service milvus (needs milvus >=3.0) — upgrading down to 2.6.3 must trip the compat gate
    app_module.core.install(InstallSpec(kind="milvus", name="mv-wp", params={
        "mq": "woodpecker-service", "image": "milvusdb/milvus:v3.0.0"}), dry_run=False)
    r = client.post("/api/upgrade", json={"instance": "mv-wp", "image": "milvusdb/milvus:v2.6.3", "dry_run": True})
    assert r.status_code == 409 and r.json()["error"] == "compat"
    assert client.post("/api/upgrade", json={"instance": "nope", "image": "x", "dry_run": True}).status_code == 400
