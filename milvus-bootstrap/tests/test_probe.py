from milvus_bootstrap.core import probe


def _fake_run(script):
    def run(args):
        key = " ".join(args)
        for pat, val in script.items():
            if pat in key:
                return val
        return (1, "", "not found")
    return run


def test_detect_k8s_and_operator_and_milvus():
    run = _fake_run({
        "version": (0, '{"serverVersion":{"gitVersion":"v1.34.0"}}', ""),
        "get deploy": (0, "milvus-operator\tmilvusio/milvus-operator:v1.3.6", ""),
        "get milvus": (0, "milvus-dev\tmilvusdb/milvus:v2.6.18", ""),
    })
    dv = probe.detect_versions(run=run)
    assert dv.k8s == "1.34.0"
    assert dv.operator == "1.3.6"
    assert dv.milvus == {"milvus-dev": "2.6.18"}
    d = dv.as_compat_dict()
    assert d["k8s"] == "1.34.0" and d["milvus-operator"] == "1.3.6" and d["milvus"] == "2.6.18"


def test_detect_missing_is_none():
    dv = probe.detect_versions(run=lambda a: (1, "", "err"))
    assert dv.k8s is None and dv.operator is None and dv.milvus == {}
    assert "k8s" not in dv.as_compat_dict()


def test_detect_dependency_versions_from_pod_images():
    pods = "\n".join([
        "etcd-0\tdocker.io/bitnamilegacy/etcd:3.5.25",
        "minio-pool-0-0\tminio/minio:RELEASE.2024-12-18T13-15-44Z",
        "kafka-dev-controller-0\tbitnamilegacy/kafka:3.9.0",
        "pulsar-dev-broker-0\tapachepulsar/pulsar:3.0.7",
    ])
    run = _fake_run({
        "version": (0, '{"serverVersion":{"gitVersion":"v1.34.0"}}', ""),
        "get pods": (0, pods, ""),
    })
    dv = probe.detect_versions(run=run)
    assert dv.etcd == "3.5.25"
    assert dv.minio == "RELEASE.2024-12-18T13-15-44Z"   # full tag, not semver
    assert dv.kafka == "3.9.0"
    assert dv.pulsar == "3.0.7"
    d = dv.as_compat_dict()
    assert d["etcd"] == "3.5.25" and d["minio"].startswith("RELEASE.")
