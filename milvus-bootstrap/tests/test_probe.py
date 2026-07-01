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
