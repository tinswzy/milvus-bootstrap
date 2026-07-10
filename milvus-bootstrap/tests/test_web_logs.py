from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def test_api_logs_shape_non_k8s(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.get("/api/logs", params={"pod": "mypod", "namespace": "default"})
        assert r.status_code == 200
        body = r.json()
        assert body["pod"] == "mypod" and body["namespace"] == "default"
        assert "logs" in body and isinstance(body["logs"], str)
        assert "k8s" in body["logs"]                  # non-k8s hint string
