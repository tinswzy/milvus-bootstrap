"""Filesystem locations for the daemon socket, pidfile and local state.

Override the base dir with the ``MB_HOME`` env var (handy for tests).
"""
from __future__ import annotations

import os
from pathlib import Path


def base_dir() -> Path:
    return Path(os.environ.get("MB_HOME", str(Path.home() / ".milvus-bootstrap")))


def sock_path() -> Path:
    return base_dir() / "daemon.sock"


def pid_path() -> Path:
    return base_dir() / "daemon.pid"


def state_dir() -> Path:
    return base_dir() / "state"


def log_path() -> Path:
    return base_dir() / "daemon.log"


def ensure_base() -> Path:
    d = base_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d
