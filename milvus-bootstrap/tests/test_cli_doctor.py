import json

from typer.testing import CliRunner

from milvus_bootstrap.cli.main import app
from milvus_bootstrap.core import doctor
from milvus_bootstrap.core.compat import Finding

runner = CliRunner()


def _fake_report():
    return doctor.DoctorReport(
        env=[Finding("PASS", "kubectl", "kubectl 可用", "ok"),
             Finding("FAIL", "cluster", "集群可达", "unreachable")],
        versions={"k8s": "1.34.0", "milvus-operator": "1.3.6"},
        compat=[Finding("WARN", "milvus-operator", "r", "约束未配置")],
        tool={"version": "0.0.1", "commit": "abc1234", "update": "checked"},
    )


def test_doctor_json(monkeypatch):
    monkeypatch.setattr(doctor, "run", lambda **k: _fake_report())
    res = runner.invoke(app, ["doctor", "--json"])
    assert res.exit_code == 1                       # has a FAIL
    data = json.loads(res.stdout)
    assert data["versions"]["k8s"] == "1.34.0" and data["exit_code"] == 1


def test_doctor_table(monkeypatch):
    monkeypatch.setattr(doctor, "run", lambda **k: _fake_report())
    res = runner.invoke(app, ["doctor"])
    assert res.exit_code == 1
    assert "kubectl" in res.stdout and "1.3.6" in res.stdout
