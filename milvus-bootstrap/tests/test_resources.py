import json

from milvus_bootstrap.core import resources


def test_parse_cpu():
    assert resources.parse_cpu("12") == 12000
    assert resources.parse_cpu("500m") == 500
    assert resources.parse_cpu("0.5") == 500
    assert resources.parse_cpu("") == 0
    assert resources.parse_cpu(None) == 0
    assert resources.parse_cpu("garbage") == 0


def test_parse_mem():
    assert resources.parse_mem("32779072Ki") == 32779072 * 1024
    assert resources.parse_mem("512Mi") == 536870912
    assert resources.parse_mem("2Gi") == 2147483648
    assert resources.parse_mem("1000000") == 1000000
    assert resources.parse_mem("") == 0
    assert resources.parse_mem(None) == 0
    assert resources.parse_mem("1G") == 1000 ** 3       # decimal suffix distinct from Gi


def _fake_run(nodes, pods, top=(1, "", "err")):
    def run(args):
        key = " ".join(args)
        if "get nodes" in key:
            return (0, json.dumps(nodes), "")
        if "get pods" in key:
            return (0, json.dumps(pods), "")
        if "top nodes" in key:
            return top
        return (1, "", "no")
    return run


def test_cluster_resources_aggregates_requests():
    nodes = {"items": [{"metadata": {"name": "n1"},
                        "status": {"allocatable": {"cpu": "12", "memory": "32Gi"}}}]}
    pods = {"items": [
        {"spec": {"nodeName": "n1", "containers": [
            {"resources": {"requests": {"cpu": "500m", "memory": "1Gi"},
                           "limits": {"cpu": "1", "memory": "2Gi"}}}]}},
        {"spec": {"nodeName": "n1", "containers": [
            {"resources": {"requests": {"cpu": "250m"}}}]}},          # partial: no mem, no limits
        {"spec": {"nodeName": "other-node", "containers": [{"resources": {}}]}},  # off-node, ignored
    ]}
    r = resources.cluster_resources(run=_fake_run(nodes, pods))
    assert r["metrics_available"] is False
    n1 = r["nodes"][0]
    assert n1["name"] == "n1" and n1["pods"] == 2
    assert n1["cpu_alloc_m"] == 12000 and n1["mem_alloc_b"] == 32 * 1024 ** 3
    assert n1["cpu_req_m"] == 750 and n1["mem_req_b"] == 1024 ** 3       # 500m+250m ; 1Gi+0
    assert n1["cpu_lim_m"] == 1000 and n1["mem_lim_b"] == 2 * 1024 ** 3
    assert n1["cpu_usage_m"] is None
    c = r["cluster"]
    assert c["nodes"] == 1 and c["pods"] == 2 and c["cpu_req_m"] == 750


def test_cluster_resources_top_populates_usage():
    nodes = {"items": [{"metadata": {"name": "n1"},
                        "status": {"allocatable": {"cpu": "12", "memory": "32Gi"}}}]}
    pods = {"items": []}
    top = (0, "n1 1200m 10% 3000Mi 9%\n", "")
    r = resources.cluster_resources(run=_fake_run(nodes, pods, top=top))
    assert r["metrics_available"] is True
    assert r["nodes"][0]["cpu_usage_m"] == 1200 and r["nodes"][0]["mem_usage_b"] == 3000 * 1024 ** 2
    assert r["cluster"]["cpu_usage_m"] == 1200


def test_cluster_resources_nodes_fail_returns_none():
    assert resources.cluster_resources(run=lambda a: (1, "", "boom")) is None
