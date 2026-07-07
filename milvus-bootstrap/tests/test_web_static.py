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
    assert 'id="rail"' in body and 'id="env-list"' in body
    assert 'id="instances-card"' not in body        # instances moved off overview


def test_assets_served(client):
    assert client.get("/assets/web.css").status_code == 200
    js = client.get("/assets/web.js")
    assert js.status_code == 200 and "renderOverview" in js.text


def test_api_routes_win_over_static(client):
    # /healthz is a real route, must not be shadowed by the static mount at "/"
    assert client.get("/healthz").json() == {"ok": True}


def test_compat_page_served(client):
    r = client.get("/compat.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="mq-rules"' in r.text and 'id="constraints"' in r.text and 'id="upgrade-paths"' in r.text
    assert "renderCompat" in client.get("/assets/web.js").text


def test_install_page_served(client):
    r = client.get("/install.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="inst-kind"' in r.text and 'id="inst-params"' in r.text and 'id="inst-result"' in r.text
    js = client.get("/assets/web.js").text
    assert "renderInstall" in js and "postJSON" in js
    assert "安装向导（待做）" not in js       # nav item enabled, not the disabled placeholder


def test_nav_has_instance_pages(client):
    js = client.get("/assets/web.js").text
    assert "milvus.html" in js and "deps.html" in js
    assert "renderOverview" in js                    # overview still exists


def test_deps_page_served(client):
    r = client.get("/deps.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="deps-list"' in r.text
    js = client.get("/assets/web.js").text
    assert "renderDeps" in js and "deleteInstance" in js


def test_milvus_page_served(client):
    r = client.get("/milvus.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="milvus-list"' in r.text
    assert "renderMilvus" in client.get("/assets/web.js").text
