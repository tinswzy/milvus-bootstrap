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


def test_api_pods_includes_resources(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="milvus", name="res-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/pods", params={"instance": "res-mv"})
        assert r.status_code == 200
        res = r.json()["resources"]
        assert "total" in res and "pods" in res
        assert set(res["total"]) >= {"cpu_req_m", "cpu_lim_m", "mem_req_b", "mem_lim_b", "pods"}


def test_api_instances_milvus_has_res_key(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="milvus", name="res-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        rows = client.get("/api/instances").json()["instances"]
        mv = [x for x in rows if x["name"] == "res-mv"][0]
        assert "res" in mv                                   # key present (fake -> None)
