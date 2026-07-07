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


def test_api_instances_with_registered_instance(client):
    # Exercises the loop body: kind comes from spec_snapshot, not an Instance attr.
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    r = client.get("/api/instances")
    assert r.status_code == 200
    inst = r.json()["instances"]
    assert len(inst) == 1
    row = inst[0]
    assert row["name"] == "etcd-dev"
    assert row["kind"] == "etcd"          # from spec_snapshot["kind"]
    assert row["namespace"] == "default"
    assert isinstance(row["ownership"], str)


def test_api_instances_enriched_fields(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    app_module.core.install(InstallSpec(kind="milvus", name="milvus-dev", params={
        "mq": "kafka", "image": "milvusdb/milvus:v2.6.18",
        "storageEndpoint": "minio.default.svc:80",
        "kafkaBrokers": "kafka-dev.default.svc:9092"}), dry_run=False)
    rows = {r["name"]: r for r in client.get("/api/instances").json()["instances"]}
    assert rows["etcd-dev"]["deps"] is None and rows["etcd-dev"]["status"] is None
    m = rows["milvus-dev"]
    assert m["image"] == "milvusdb/milvus:v2.6.18"
    assert m["status"] is None                                  # fake adapter → not queried
    assert m["deps"]["mq"] == "kafka" and m["deps"]["storage"] == "minio.default.svc:80"
    assert m["deps"]["mq_endpoint"] == "kafka-dev.default.svc:9092"
