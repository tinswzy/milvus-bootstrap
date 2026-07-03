from milvus_bootstrap.core import webapi


def test_compat_rules_shape():
    r = webapi.compat_rules()
    assert set(r) == {"mq_rules", "constraints", "upgrade_paths"}
    # mq_rules from MQ_OPTIONS (woodpecker-embedded/service, kafka, pulsar, rocksmq)
    ids = {m["id"] for m in r["mq_rules"]}
    assert {"woodpecker-embedded", "woodpecker-service", "kafka", "pulsar", "rocksmq"} <= ids
    wp_svc = next(m for m in r["mq_rules"] if m["id"] == "woodpecker-service")
    assert wp_svc["min_milvus"] == "3.0.0" and wp_svc["wal"] == "woodpecker"
    # constraints include the docs-seeded soft floors
    comps = {c["component"] for c in r["constraints"]}
    assert {"etcd", "pulsar", "k8s", "helm", "minio"} <= comps
    etcd = next(c for c in r["constraints"] if c["component"] == "etcd")
    assert etcd["min"] == "3.5.0" and etcd["severity"] == "soft"
    # upgrade paths present
    assert any(u["requires_current_min"] == "2.5.16" for u in r["upgrade_paths"])
