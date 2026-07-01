"""mb doctor orchestrator — environment preflight + versions + compat + tool info.
Local-first: runs without the daemon; unavailable checks degrade to SKIP/WARN."""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass

from .. import __version__
from . import compat, probe
from .compat import Finding


@dataclass
class DoctorReport:
    env: list[Finding]
    versions: dict
    compat: list[Finding]
    tool: dict

    def fails(self) -> list[Finding]:
        return [f for f in (*self.env, *self.compat) if f.level == "FAIL"]

    def exit_code(self) -> int:
        return 1 if self.fails() else 0

    def to_json(self) -> dict:
        f = lambda xs: [vars(x) for x in xs]
        return {"env": f(self.env), "versions": self.versions,
                "compat": f(self.compat), "tool": self.tool,
                "exit_code": self.exit_code()}


def check_environment(run, no_proxy: str, daemon_up: bool) -> list[Finding]:
    out: list[Finding] = []

    if shutil.which("kubectl"):
        out.append(Finding("PASS", "kubectl", "kubectl 可用", "found on PATH"))
    else:
        out.append(Finding("FAIL", "kubectl", "kubectl 可用", "未找到 kubectl"))

    rc, o, e = run(["version", "-o", "json"])
    out.append(Finding("PASS", "cluster", "集群可达", "apiserver responded")
               if rc == 0 else Finding("FAIL", "cluster", "集群可达", (e or "unreachable").strip()[:120]))

    proxy_set = bool(os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY"))
    if proxy_set and "192.168.49" not in (no_proxy or "") and "10.96." not in (no_proxy or ""):
        out.append(Finding("WARN", "no_proxy", "NO_PROXY 含集群网段",
                           "代理已设置但 NO_PROXY 疑似缺 minikube/service 网段（apiserver 可能 EOF）"))
    else:
        out.append(Finding("PASS", "no_proxy", "NO_PROXY 含集群网段", "ok"))

    out.append(Finding("PASS", "daemon", "core daemon 运行", "running")
               if daemon_up else Finding("WARN", "daemon", "core daemon 运行", "未运行（doctor 仍可预检）"))

    rc, o, _ = run(["get", "crd", "milvuses.milvus.io", "-o", "name"])
    out.append(Finding("PASS", "operator", "Milvus CRD 就位", "milvuses.milvus.io present")
               if rc == 0 and o.strip() else Finding("WARN", "operator", "Milvus CRD 就位", "未探测到 Milvus CRD"))

    return out


def tool_info(run=probe.run_kubectl) -> dict:
    commit = None
    try:
        import subprocess
        p = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                           capture_output=True, text=True, timeout=10)
        commit = p.stdout.strip() if p.returncode == 0 else None
        r = subprocess.run(["git", "ls-remote", "--tags", "origin"],
                           capture_output=True, text=True, timeout=10)
        update = "checked" if r.returncode == 0 else "unavailable"
    except (OSError, subprocess.SubprocessError):
        update = "unavailable"
    return {"version": __version__, "commit": commit, "update": update}


def _daemon_up() -> bool:
    try:
        from ..client import DaemonClient
        return bool(DaemonClient().local_status().get("running"))
    except Exception:
        return False


def run(run=probe.run_kubectl, no_proxy: str | None = None,
        daemon_up: bool | None = None) -> DoctorReport:
    no_proxy = os.environ.get("NO_PROXY", "") if no_proxy is None else no_proxy
    daemon_up = _daemon_up() if daemon_up is None else daemon_up
    env = check_environment(run, no_proxy, daemon_up)
    versions = probe.detect_versions(run=run).as_compat_dict()
    findings = compat.evaluate(versions)
    return DoctorReport(env=env, versions=versions, compat=findings, tool=tool_info(run))
