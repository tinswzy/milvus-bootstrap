"""Milvus onboarding — operator-cr install with all deps external (etcd/minio/woodpecker)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.context import Core
from milvus_bootstrap.core.drivers.milvus import MilvusDriver, woodpecker_seeds
from milvus_bootstrap.core.models import InstallSpec, Platform, StateClass, TaskStatus
from milvus_bootstrap.core.profile import load_profiles


@pytest.fixture()
def core(tmp_path, monkeypatch) -> Core:
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return Core()


def test_milvus_profile_loaded(core: Core) -> None:
    assert "milvus" in core.registry.kinds()
    assert isinstance(core.registry.get("milvus"), MilvusDriver)
    assert core.registry.get("milvus").state_class() == StateClass.stateless


def test_woodpecker_seed_computation() -> None:
    assert woodpecker_seeds("wp-dev", 3, "default") == [
        "wp-dev-server-0.wp-dev-server-headless.default.svc:18080",
        "wp-dev-server-1.wp-dev-server-headless.default.svc:18080",
        "wp-dev-server-2.wp-dev-server-headless.default.svc:18080",
    ]


def test_install_milvus_dry_run_all_external(core: Core) -> None:
    task = core.install(InstallSpec(kind="milvus", name="milvus-dev", params={
        "etcdEndpoints": "etcd-dev:2379",
        "storageEndpoint": "minio-dev:9000",
        "woodpeckerName": "wp-dev",
        "woodpeckerReplicas": "3",
    }), dry_run=True)
    assert task.status == TaskStatus.succeeded
    assert [s.name for s in task.steps] == ["precheck-operator", "apply-cr", "wait-status", "register"]
    apply = next(s for s in task.steps if s.name == "apply-cr")
    assert "kind: Milvus" in apply.plan
    assert "msgStreamType: woodpecker" in apply.plan
    assert "external: true" in apply.plan                 # etcd + storage external
    assert "wp-dev-server-0.wp-dev-server-headless" in apply.plan  # computed seeds
    # ready gate is the Milvus CR health status
    wait = next(s for s in task.steps if s.name == "wait-status")
    assert "status.status" in wait.plan and "Healthy" in wait.plan


def test_milvus_build_manifests_all_external() -> None:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    params = {**method.params, "woodpeckerName": "wp1"}
    mans = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)

    kinds = [m["kind"] for m in mans]
    assert kinds == ["Secret", "Milvus"]
    secret = mans[0]
    assert "accesskey" in secret["stringData"] and "secretkey" in secret["stringData"]

    cr = mans[1]
    assert cr["apiVersion"] == "milvus.io/v1beta1"
    deps = cr["spec"]["dependencies"]
    assert deps["msgStreamType"] == "woodpecker"
    assert deps["etcd"]["external"] is True
    assert deps["storage"]["external"] is True
    assert deps["storage"]["secretRef"] == "m1-minio"
    wp = deps["woodpecker"]["external"]
    assert wp["replicaCount"] == 3
    assert wp["endpoints"][0] == "wp1-server-0.wp1-server-headless.default.svc:18080"


def test_milvus_injects_minio_address_env_for_external_endpoint() -> None:
    """External MinIO endpoint (host:port) must be passed to Milvus as MINIO_ADDRESS,
    else the operator splits it into address+port and segcore appends the bucket onto
    the endpoint → minio-go 'fully qualified paths' crash."""
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    params = {**method.params, "mq": "kafka",
              "storageEndpoint": "minio.default.svc:80"}
    cr = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)[-1]
    env = cr["spec"]["components"].get("env", [])
    assert {"name": "MINIO_ADDRESS", "value": "minio.default.svc:80"} in env


def test_milvus_no_minio_env_for_portless_endpoint() -> None:
    """A portless endpoint (e.g. real AWS S3) needs no colon-form override."""
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    params = {**method.params, "mq": "kafka", "storageEndpoint": "s3.amazonaws.com"}
    cr = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)[-1]
    env = {e["name"] for e in cr["spec"]["components"].get("env", [])}
    assert "MINIO_ADDRESS" not in env


def test_install_milvus_apply_registers(core: Core) -> None:
    task = core.install(InstallSpec(kind="milvus", name="milvus-dev",
                                    params={"woodpeckerName": "wp-dev"}), dry_run=False)
    assert task.status == TaskStatus.succeeded
    inst = core.state.get_instance("milvus-dev")
    assert inst is not None
    assert inst.deps[0].install_method == "milvus-operator"
    assert inst.deps[0].state_class == StateClass.stateless


def test_milvus_per_dep_isolation_defaults_and_override() -> None:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    cfg = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"),
                                      method, {**method.params, "mq": "kafka"})[-1]["spec"]["config"]
    # all four default to the instance name (incl. minio.rootPath)
    assert cfg["etcd"]["rootPath"] == "m1"
    assert cfg["minio"]["bucketName"] == "m1"
    assert cfg["minio"]["rootPath"] == "m1"
    assert cfg["msgChannel"]["chanNamePrefix"]["cluster"] == "m1"
    assert "conf" not in drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, method.params)[-1]["spec"]
    # per-key override, others untouched
    cfg2 = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method,
        {**method.params, "mq": "kafka", "minioRootPath": "custom-rp", "mqChanPrefix": "myprefix"})[-1]["spec"]["config"]
    assert cfg2["minio"]["rootPath"] == "custom-rp"
    assert cfg2["msgChannel"]["chanNamePrefix"]["cluster"] == "myprefix"
    assert cfg2["etcd"]["rootPath"] == "m1" and cfg2["minio"]["bucketName"] == "m1"


def test_milvus_conf_merged_into_spec_config() -> None:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    params = {**method.params, "mq": "kafka", "_conf": {"queryNode.gracefulTime": 5000}}
    cfg = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)[-1]["spec"]["config"]
    assert cfg["queryNode"]["gracefulTime"] == 5000       # _conf routed into spec.config (dotted→nested)
    assert cfg["etcd"]["rootPath"] == "m1"                # isolation still present


def test_milvus_default_prefix_shared_dep_allowed(core: Core) -> None:
    # the NORMAL case: two distinct-named milvus (default prefix = own name) sharing the
    # SAME kafka must be allowed — distinct prefixes → no collision, no false positive.
    core.install(InstallSpec(kind="milvus", name="mv-1", params={
        "mq": "kafka", "kafkaBrokers": "kafka-shared.default.svc:9092"}), dry_run=False)
    core.install(InstallSpec(kind="milvus", name="mv-2", params={
        "mq": "kafka", "kafkaBrokers": "kafka-shared.default.svc:9092"}), dry_run=True)


def test_milvus_dup_name_rejected(core: Core) -> None:
    core.install(InstallSpec(kind="milvus", name="mv", params={
        "mq": "kafka", "kafkaBrokers": "kafka-dev.default.svc:9092"}), dry_run=False)
    with pytest.raises(ValueError, match="已存在"):
        core.install(InstallSpec(kind="milvus", name="mv", params={"mq": "kafka"}), dry_run=True)


def test_milvus_mq_collision_on_shared_broker(core: Core) -> None:
    import pytest
    core.install(InstallSpec(kind="milvus", name="mv-a", params={
        "mq": "kafka", "kafkaBrokers": "kafka-x.default.svc:9092", "mqChanPrefix": "shared"}), dry_run=False)
    with pytest.raises(ValueError, match="MQ"):
        core.install(InstallSpec(kind="milvus", name="mv-b", params={
            "mq": "kafka", "kafkaBrokers": "kafka-x.default.svc:9092", "mqChanPrefix": "shared"}), dry_run=True)
    # same prefix, different broker → allowed
    core.install(InstallSpec(kind="milvus", name="mv-c", params={
        "mq": "kafka", "kafkaBrokers": "kafka-y.default.svc:9092", "mqChanPrefix": "shared"}), dry_run=True)


def test_milvus_minio_pair_collision(core: Core) -> None:
    import pytest
    base = {"mq": "kafka", "kafkaBrokers": "k.default.svc:9092", "storageEndpoint": "minio.default.svc:80"}
    core.install(InstallSpec(kind="milvus", name="mv-a", params={**base, "minioBucket": "shared", "minioRootPath": "rp"}), dry_run=False)
    # same bucket + same rootPath on the shared minio → collision
    with pytest.raises(ValueError, match="对象存储"):
        core.install(InstallSpec(kind="milvus", name="mv-b", params={**base, "minioBucket": "shared", "minioRootPath": "rp"}), dry_run=True)
    # same bucket but DIFFERENT rootPath → allowed (share bucket, isolate by path)
    core.install(InstallSpec(kind="milvus", name="mv-c", params={**base, "minioBucket": "shared", "minioRootPath": "other"}), dry_run=True)
