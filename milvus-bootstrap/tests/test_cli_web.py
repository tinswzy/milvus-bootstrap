from typer.testing import CliRunner

from milvus_bootstrap.cli.main import app

runner = CliRunner()


def test_web_command_calls_run_web(monkeypatch):
    calls = {}
    import milvus_bootstrap.server.__main__ as srv
    monkeypatch.setattr(srv, "run_web", lambda host, port: calls.update(host=host, port=port))
    res = runner.invoke(app, ["web", "--host", "127.0.0.1", "--port", "9001"])
    assert res.exit_code == 0
    assert calls == {"host": "127.0.0.1", "port": 9001}


def test_run_web_warns_on_all_interfaces(monkeypatch, capsys):
    import milvus_bootstrap.server.__main__ as srv
    served = {}
    monkeypatch.setattr(srv.uvicorn, "run", lambda *a, **k: served.update(k))
    srv.run_web("0.0.0.0", 8080)
    out = capsys.readouterr().out
    assert "0.0.0.0" in out and ("警告" in out or "WARN" in out.upper())
    assert served.get("host") == "0.0.0.0" and served.get("port") == 8080
