"""Physical-host snapshot for the machine mb runs on. Stdlib only, best-effort."""
from __future__ import annotations

import os
import shutil
import socket


def _meminfo() -> tuple[int | None, int | None]:
    try:
        d = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, rest = line.partition(":")
                d[k.strip()] = int(rest.strip().split()[0]) * 1024   # kB -> bytes
        return d.get("MemTotal"), d.get("MemAvailable")
    except Exception:  # noqa: BLE001
        return None, None


def _disk_path() -> str:
    for p in (os.environ.get("MB_HOME"), os.path.expanduser("~/.milvus-bootstrap"), "/"):
        if p and os.path.exists(p):
            return p
    return "/"


def collect() -> dict:
    out: dict = {"hostname": None, "os": None, "kernel": None, "cpu_count": None,
                 "mem_total_b": None, "mem_available_b": None, "mem_used_pct": None,
                 "load1": None, "load5": None, "load15": None,
                 "disk_path": None, "disk_total_b": None, "disk_used_b": None, "disk_pct": None}
    try:
        out["hostname"] = socket.gethostname()
    except Exception:  # noqa: BLE001
        pass
    try:
        u = os.uname()
        out["os"], out["kernel"] = u.sysname, u.release
    except Exception:  # noqa: BLE001
        pass
    out["cpu_count"] = os.cpu_count()
    mt, ma = _meminfo()
    out["mem_total_b"], out["mem_available_b"] = mt, ma
    if mt and ma is not None:
        out["mem_used_pct"] = round(100 * (mt - ma) / mt, 1)
    try:
        l1, l5, l15 = os.getloadavg()
        out["load1"], out["load5"], out["load15"] = round(l1, 2), round(l5, 2), round(l15, 2)
    except Exception:  # noqa: BLE001
        pass
    try:
        p = _disk_path()
        du = shutil.disk_usage(p)
        out["disk_path"] = p
        out["disk_total_b"], out["disk_used_b"] = du.total, du.used
        out["disk_pct"] = round(100 * du.used / du.total, 1) if du.total else None
    except Exception:  # noqa: BLE001
        pass
    return out
