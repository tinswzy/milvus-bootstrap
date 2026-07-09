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


def test_api_instances_empty_state_shows_externals(client):
    rows = client.get("/api/instances").json()["instances"]
    # no managed instances installed → every row is external, control-plane excluded
    assert rows, "fake cluster should yield discovered externals"
    assert all(r["ownership"] == "external" for r in rows)
    names = {r["name"] for r in rows}
    assert "milvus-etcd" in names            # discoverable dep in default ns
    assert "etcd" not in names               # kube-system control-plane excluded


def test_api_instances_with_registered_instance(client):
    # Exercises the loop body: kind comes from spec_snapshot, not an Instance attr.
    # Discovery now merges externals too, so len > 1 is expected.
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    r = client.get("/api/instances")
    assert r.status_code == 200
    rows = {i["name"]: i for i in r.json()["instances"]}
    assert "etcd-dev" in rows
    row = rows["etcd-dev"]
    assert row["name"] == "etcd-dev"
    assert row["kind"] == "etcd"          # from spec_snapshot["kind"]
    assert row["namespace"] == "default"
    assert row["ownership"] == "managed"


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


def test_api_instances_managed_and_fields(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="milvus", name="milvus-dev", params={
        "mq": "kafka", "image": "milvusdb/milvus:v2.6.18",
        "storageEndpoint": "minio.default.svc:80", "kafkaBrokers": "kafka-dev.default.svc:9092"}), dry_run=False)
    rows = {r["name"]: r for r in client.get("/api/instances").json()["instances"]}
    m = rows["milvus-dev"]
    assert m["ownership"] == "managed"
    assert m["image"] == "milvusdb/milvus:v2.6.18"    # from snapshot (fake → no pod match)
    assert m["image_id"] is None                       # fake adapter → no pod images
    assert m["deps"]["mq"] == "kafka"
    assert "image_id" in rows["milvus-etcd"]           # external rows also carry the key
    assert rows["milvus-etcd"]["ownership"] == "external"


def test_api_instances_collapses_managed_subworkloads(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    # managed etcd named "milvus" → the fake-cluster workload "milvus-etcd" (etcd, default)
    # is a "milvus-" segment child → must be collapsed (not shown as a separate external).
    app_module.core.install(InstallSpec(kind="etcd", name="milvus"), dry_run=False)
    rows = client.get("/api/instances").json()["instances"]
    by = {(r["kind"], r["name"]) for r in rows}
    assert ("etcd", "milvus") in by                    # the managed parent
    assert ("etcd", "milvus-etcd") not in by           # its discovered sub-workload, collapsed
    # an unrelated external of a different kind (no managed prefix) still shows
    assert ("minio", "milvus-minio") in by


def test_api_pods_known_and_unknown(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="milvus", name="mv-pods", params={"mq": "kafka"}), dry_run=False)
    r = client.get("/api/pods", params={"instance": "mv-pods"})
    assert r.status_code == 200
    body = r.json()
    assert body["instance"] == "mv-pods" and body["namespace"] == "default" and body["pods"] == []
    assert client.get("/api/pods", params={"instance": "nope"}).status_code == 400


def test_api_pods_returns_desired_image(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="milvus", name="mv-di", params={
        "mq": "kafka", "image": "milvusdb/milvus:v2.6.20"}), dry_run=False)
    body = client.get("/api/pods", params={"instance": "mv-di"}).json()
    assert body["desired_image"] == "milvusdb/milvus:v2.6.20" and body["pods"] == []
