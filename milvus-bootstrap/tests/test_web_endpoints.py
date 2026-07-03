import pytest
from fastapi.testclient import TestClient

from milvus_bootstrap.core import doctor
from milvus_bootstrap.core.compat import Finding


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    # Hermetic doctor: never shell real kubectl.
    fake = doctor.DoctorReport(
        env=[Finding("PASS", "cluster", "集群可达", "ok"),
             Finding("PASS", "kubectl", "kubectl 可用", "ok")],
        versions={"k8s": "1.34.0", "milvus-operator": "1.3.6"},
        compat=[], tool={"version": "0.0.1", "commit": "abc", "update": "checked"},
    )
    monkeypatch.setattr(doctor, "run", lambda **k: fake)
    from milvus_bootstrap.server.app import app
    with TestClient(app) as c:
        yield c


def test_api_doctor(client):
    r = client.get("/api/doctor")
    assert r.status_code == 200
    j = r.json()
    assert set(j) >= {"env", "versions", "tool"}
    assert j["versions"]["k8s"] == "1.34.0"
    assert any(f["component"] == "cluster" for f in j["env"])


def test_api_compat_rules(client):
    r = client.get("/api/compat-rules")
    assert r.status_code == 200
    j = r.json()
    assert {"mq_rules", "constraints", "upgrade_paths"} <= set(j)
    assert j["mq_rules"] and j["constraints"]


def test_api_instances_empty(client):
    r = client.get("/api/instances")
    assert r.status_code == 200
    assert r.json() == {"instances": []}   # fresh fake state
