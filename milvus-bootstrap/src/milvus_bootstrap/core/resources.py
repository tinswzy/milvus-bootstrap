"""On-demand cluster resource watermark (requests/limits vs allocatable). Best-effort."""
from __future__ import annotations

import json

from .probe import run_kubectl


def parse_cpu(s: str | None) -> int:
    """k8s CPU quantity -> millicores. '12'->12000, '500m'->500, '0.5'->500."""
    if not s:
        return 0
    s = str(s).strip()
    if s.endswith("m"):
        try:
            return int(float(s[:-1]))
        except ValueError:
            return 0
    try:
        return int(float(s) * 1000)
    except ValueError:
        return 0


_MEM_UNITS = {"Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4, "Pi": 1024 ** 5,
              "K": 1000, "M": 1000 ** 2, "G": 1000 ** 3, "T": 1000 ** 4, "P": 1000 ** 5}


def parse_mem(s: str | None) -> int:
    """k8s memory quantity -> bytes. '32779072Ki', '512Mi', '2Gi', '1000000'."""
    if not s:
        return 0
    s = str(s).strip()
    for u, mult in _MEM_UNITS.items():          # binary (Ki/Mi/Gi) before decimal (K/M/G)
        if s.endswith(u):
            try:
                return int(float(s[:-len(u)]) * mult)
            except ValueError:
                return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _sum_reqs(containers, field):
    cpu = mem = 0
    for c in containers or []:
        r = (c.get("resources") or {}).get(field) or {}
        cpu += parse_cpu(r.get("cpu"))
        mem += parse_mem(r.get("memory"))
    return cpu, mem


def cluster_resources(run=run_kubectl) -> dict | None:
    rc, out, _ = run(["get", "nodes", "-o", "json"])
    if rc != 0:
        return None
    try:
        nodes_j = json.loads(out)
    except Exception:  # noqa: BLE001
        return None
    nodes: dict[str, dict] = {}
    for n in nodes_j.get("items", []):
        name = n["metadata"]["name"]
        alloc = n.get("status", {}).get("allocatable", {})
        nodes[name] = {"name": name, "cpu_alloc_m": parse_cpu(alloc.get("cpu")),
                       "mem_alloc_b": parse_mem(alloc.get("memory")),
                       "cpu_req_m": 0, "cpu_lim_m": 0, "mem_req_b": 0, "mem_lim_b": 0, "pods": 0,
                       "cpu_usage_m": None, "mem_usage_b": None}
    pod_total = 0
    rc, out, _ = run(["get", "pods", "-A", "-o", "json"])
    if rc == 0:
        try:
            pods_j = json.loads(out)
        except Exception:  # noqa: BLE001
            pods_j = {"items": []}
        for p in pods_j.get("items", []):
            nn = (p.get("spec") or {}).get("nodeName")
            if nn not in nodes:
                continue
            pod_total += 1
            nodes[nn]["pods"] += 1
            conts = (p.get("spec") or {}).get("containers", [])
            rc_, rm_ = _sum_reqs(conts, "requests")
            lc_, lm_ = _sum_reqs(conts, "limits")
            nodes[nn]["cpu_req_m"] += rc_
            nodes[nn]["mem_req_b"] += rm_
            nodes[nn]["cpu_lim_m"] += lc_
            nodes[nn]["mem_lim_b"] += lm_
    metrics = False
    rc, out, _ = run(["top", "nodes", "--no-headers"])
    if rc == 0 and out.strip():
        metrics = True
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0] in nodes:
                nodes[parts[0]]["cpu_usage_m"] = parse_cpu(parts[1])
                nodes[parts[0]]["mem_usage_b"] = parse_mem(parts[3])
    nlist = list(nodes.values())

    def _sum(k):
        return sum(x[k] or 0 for x in nlist)

    cluster = {"nodes": len(nlist), "pods": pod_total,
               "cpu_alloc_m": _sum("cpu_alloc_m"), "mem_alloc_b": _sum("mem_alloc_b"),
               "cpu_req_m": _sum("cpu_req_m"), "cpu_lim_m": _sum("cpu_lim_m"),
               "mem_req_b": _sum("mem_req_b"), "mem_lim_b": _sum("mem_lim_b")}
    if metrics:
        cluster["cpu_usage_m"] = _sum("cpu_usage_m")
        cluster["mem_usage_b"] = _sum("mem_usage_b")
    return {"metrics_available": metrics, "cluster": cluster, "nodes": nlist}
