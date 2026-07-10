from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def test_api_resources_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.get("/api/resources")
        assert r.status_code == 200
        body = r.json()
        assert "host" in body and "k8s" in body
        assert body["host"]["hostname"]                 # host always present
        assert body["k8s"] is None                       # fake adapter -> no cluster resources
