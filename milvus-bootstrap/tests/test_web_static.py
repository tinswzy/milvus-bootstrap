import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.server.app import app
    with TestClient(app) as c:
        yield c


def test_root_serves_overview_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    body = r.text
    assert 'id="rail"' in body and 'id="env-list"' in body and 'id="instances"' in body


def test_assets_served(client):
    assert client.get("/assets/web.css").status_code == 200
    js = client.get("/assets/web.js")
    assert js.status_code == 200 and "renderOverview" in js.text


def test_api_routes_win_over_static(client):
    # /healthz is a real route, must not be shadowed by the static mount at "/"
    assert client.get("/healthz").json() == {"ok": True}
