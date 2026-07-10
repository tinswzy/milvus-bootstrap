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


def test_milvus_status_ok_and_missing():
    ok = probe.milvus_status("m1", run=_fake_run({"get milvus m1": (0, "Healthy", "")}))
    assert ok == "Healthy"
    assert probe.milvus_status("m1", run=lambda a: (1, "", "err")) is None
    assert probe.milvus_status("m1", run=lambda a: (0, "", "")) is None   # empty status


def test_sha_of_extracts_digest():
    assert probe._sha_of("docker-pullable://milvusdb/milvus@sha256:abc123") == "sha256:abc123"
    assert probe._sha_of("milvusdb/etcd@sha256:def456") == "sha256:def456"
    assert probe._sha_of("milvusdb/etcd:3.5") == ""          # no digest
    assert probe._sha_of("") == ""


def test_pod_images_parses_and_matches():
    line = ("default\tetcd-0\tmilvusdb/etcd:3.5.18\tmilvusdb/etcd@sha256:aaa\n"
            "default\tmilvus-dev-standalone-1\tmilvusdb/milvus:v2.6.18\tdocker-pullable://milvusdb/milvus@sha256:bbb\n")
    pods = probe.pod_images(run=lambda a: (0, line, ""))
    assert len(pods) == 2 and pods[0].pod == "etcd-0"
    # match by ns + name prefix
    assert probe.match_pod_image(pods, "etcd", "default") == ("milvusdb/etcd:3.5.18", "sha256:aaa")
    assert probe.match_pod_image(pods, "milvus-dev", "default") == ("milvusdb/milvus:v2.6.18", "sha256:bbb")
    assert probe.match_pod_image(pods, "etcd", "other-ns") == ("", "")   # ns mismatch
    assert probe.pod_images(run=lambda a: (1, "", "boom")) == []          # kubectl failure


def test_pods_of_parses_and_filters():
    line = ("milvus-dev-milvus-standalone-abc\tRunning\ttrue,true,\t0,1,\tmilvusdb/milvus:v2.6.20\t2026-07-01T00:00:00Z\n"
            "other-thing-xyz\tRunning\ttrue,\t0,\tbusybox\t2026-07-01T00:00:00Z\n"
            "milvus-dev-milvus-standalone-def\tPending\tfalse,\t3,\tmilvusdb/milvus:v2.6.18\t2026-07-02T00:00:00Z\n")
    pods = probe.pods_of("milvus-dev", "default", run=lambda a: (0, line, ""))
    assert [p["pod"] for p in pods] == ["milvus-dev-milvus-standalone-abc", "milvus-dev-milvus-standalone-def"]
    assert pods[0] == {"pod": "milvus-dev-milvus-standalone-abc", "phase": "Running",
                       "ready": "2/2", "restarts": 1, "image": "milvusdb/milvus:v2.6.20", "created": "2026-07-01T00:00:00Z"}
    assert pods[1]["image"] == "milvusdb/milvus:v2.6.18" and pods[1]["ready"] == "0/1"
    assert probe.pods_of("milvus-dev", "default", run=lambda a: (1, "", "boom")) == []


def test_pod_logs_ok_and_error():
    from milvus_bootstrap.core import probe

    def ok(args):
        assert "logs" in args and "--tail=100" in args and "--all-containers=true" in args
        return (0, "line1\nline2\n", "")
    assert probe.pod_logs("mypod", "default", run=ok) == "line1\nline2\n"

    def fail(args):
        return (1, "", "Error from server (NotFound): pods \"x\" not found")
    out = probe.pod_logs("x", "default", run=fail)
    assert "NotFound" in out


def test_rollout_of_tag_compare():
    from milvus_bootstrap.core.probe import PodImage
    pods = [
        PodImage("default", "mv-a-milvus-standalone-1", "docker.io/milvusdb/milvus:v2.6.20", ""),  # registry prefix, on target
        PodImage("default", "mv-a-milvus-standalone-2", "milvusdb/milvus:v2.6.18", ""),            # old
        PodImage("default", "other-x", "busybox", ""),                                             # not this instance
    ]
    r = probe.rollout_of(pods, "mv-a", "default", "milvusdb/milvus:v2.6.20")
    assert r == {"rolling": True, "pods_upgraded": 1, "pods_total": 2}
    # all on target → not rolling
    pods2 = [PodImage("default", "mv-a-milvus-standalone-1", "milvusdb/milvus:v2.6.20", "")]
    assert probe.rollout_of(pods2, "mv-a", "default", "milvusdb/milvus:v2.6.20")["rolling"] is False
    # no pods → total 0, not rolling
    assert probe.rollout_of([], "mv-a", "default", "x")["pods_total"] == 0
    # unparseable desired (no tag) + real pods → all counted upgraded, not rolling
    pods3 = [PodImage("default", "mv-a-0", "busybox", "")]
    assert probe.rollout_of(pods3, "mv-a", "default", "no-colon-image") == {
        "rolling": False, "pods_upgraded": 1, "pods_total": 1}
