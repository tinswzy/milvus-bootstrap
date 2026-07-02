from milvus_bootstrap.core import doctor
from milvus_bootstrap.core.compat import Finding


def test_report_exit_code_and_fails():
    r = doctor.DoctorReport(
        env=[Finding("PASS", "kubectl", "", ""), Finding("FAIL", "cluster", "", "unreachable")],
        versions={"k8s": "1.34.0"},
        compat=[Finding("WARN", "milvus-operator", "", "")],
        tool={"version": "0.0.1", "commit": None, "update": "unavailable"},
    )
    assert r.exit_code() == 1
    assert [f.component for f in r.fails()] == ["cluster"]
    j = r.to_json()
    assert j["exit_code"] == 1 and j["versions"]["k8s"] == "1.34.0"


def test_check_environment_no_proxy_warns_without_cluster_ip():
    run = lambda a: (1, "", "err")   # cluster unreachable
    env = doctor.check_environment(run, no_proxy="localhost,127.0.0.1", daemon_up=False)
    kinds = {f.component: f.level for f in env}
    assert kinds.get("kubectl") in ("PASS", "FAIL")     # present/absent both valid
    assert kinds.get("daemon") == "WARN"                # not running


def test_run_builds_all_sections(monkeypatch):
    from milvus_bootstrap.core import probe
    monkeypatch.setattr(probe, "detect_versions",
                        lambda run=None: probe.DetectedVersions(k8s="1.34.0", operator="1.3.6",
                                                                milvus={"m": "2.6.18"}))
    r = doctor.run(run=lambda a: (1, "", ""), no_proxy="", daemon_up=False)
    assert r.versions["milvus-operator"] == "1.3.6"
    assert isinstance(r.compat, list) and isinstance(r.env, list)
    assert r.tool["version"]


def test_check_cpu_simd_pass_and_fail_and_skip():
    ok = doctor.check_cpu_simd(read=lambda: "flags : fpu vme sse4_2 avx2 ht\n")
    assert ok.level == "PASS" and ok.component == "cpu-simd"
    bad = doctor.check_cpu_simd(read=lambda: "flags : fpu vme ht\n")
    assert bad.level == "FAIL"
    def _boom():
        raise OSError("no /proc")
    assert doctor.check_cpu_simd(read=_boom).level == "SKIP"


def test_check_environment_includes_cpu_simd():
    env = doctor.check_environment(lambda a: (1, "", ""), no_proxy="", daemon_up=False)
    assert any(f.component == "cpu-simd" for f in env)
