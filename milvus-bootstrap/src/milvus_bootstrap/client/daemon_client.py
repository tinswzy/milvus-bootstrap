"""Thin client transport — talks to the core daemon over a Unix domain socket.

Also owns the daemon lifecycle the CLI needs: ensure-running (spawn + wait),
stop, and local status. No business logic here.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from typing import Any

import httpx

from .. import paths


class DaemonClient:
    def __init__(self) -> None:
        self.sock = paths.sock_path()

    def _client(self) -> httpx.Client:
        transport = httpx.HTTPTransport(uds=str(self.sock))
        return httpx.Client(transport=transport, base_url="http://daemon", timeout=60)

    def ping(self) -> bool:
        if not self.sock.exists():
            return False
        try:
            with self._client() as c:
                return c.get("/healthz").status_code == 200
        except Exception:
            return False

    def ensure_running(self) -> None:
        if self.ping():
            return
        paths.ensure_base()
        if self.sock.exists():           # stale socket from a dead daemon
            self.sock.unlink(missing_ok=True)
        with open(paths.log_path(), "ab") as logf:
            subprocess.Popen(
                [sys.executable, "-m", "milvus_bootstrap.server", "--uds", str(self.sock)],
                stdout=logf, stderr=logf, start_new_session=True,
            )
        for _ in range(50):              # ~10s
            if self.ping():
                return
            time.sleep(0.2)
        raise RuntimeError(f"core daemon 启动失败，见日志 {paths.log_path()}")

    def stop(self) -> bool:
        pid_file = paths.pid_path()
        if not pid_file.exists():
            return False
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, signal.SIGTERM)
        except (ValueError, ProcessLookupError):
            pid_file.unlink(missing_ok=True)
            return False
        for _ in range(25):
            if not self.ping():
                break
            time.sleep(0.2)
        self.sock.unlink(missing_ok=True)
        return True

    def local_status(self) -> dict[str, Any]:
        pid = paths.pid_path().read_text().strip() if paths.pid_path().exists() else None
        return {"running": self.ping(), "sock": str(self.sock), "pid": pid}

    def request(self, method: str, path: str, json: dict | None = None) -> Any:
        self.ensure_running()
        with self._client() as c:
            resp = c.request(method, path, json=json)
            resp.raise_for_status()
            return resp.json()
