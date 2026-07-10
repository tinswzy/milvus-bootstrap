# Overview 增强：物理机 + 集群资源与水位 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Overview 页新增「物理机（mb 主机）」与「集群资源与水位」两块，按需快照展示 mb 所在主机资源 + 集群 requests/limits÷allocatable 调度水位。

**Architecture:** 两个纯读后端模块（`core/hostinfo.py` stdlib 读 /proc；`core/resources.py` 解析 kubectl JSON 聚合水位）+ 一个 `GET /api/resources` 端点；前端 `renderOverview` 追加一次 `api/resources` 拉取并渲染两卡，复用已有 `#refresh` 按钮手动重读。**全程按需快照，无 setInterval / 无常驻轮询。**

**Tech Stack:** Python 3 stdlib（os/socket/shutil/json）+ FastAPI + pytest；vanilla JS + CSS；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-10-overview-resources-design.md`（决策 D1–D9）。
- **无轮询**：Overview 进页读一次、点已有 `#refresh` 按钮再读一次；**禁止新增 setInterval / 定时轮询 k8s**。
- **无新 pip 依赖**：物理机信息只用 stdlib（`os`/`socket`/`shutil`/`/proc`）——**不得引入 psutil** 或任何第三方包。
- 水位口径 = **requests/limits ÷ allocatable**（调度水位，永远可读）；真实用量仅 best-effort `kubectl top nodes`，缺则 `metrics_available=false`。
- best-effort：非 k8s adapter / kubectl 失败 → `k8s=null`（前端只显示物理机卡不崩）；物理机某字段读不到 → 该字段 `None`，绝不抛。
- **新 `/api/resources` 路由必须注册在 `server/app.py` 末尾 `app.mount("/", StaticFiles(...))` 之前。**
- 命令在 `milvus-bootstrap/` 下跑：`cd milvus-bootstrap && source .venv/bin/activate`。基线 183 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用仓库 `user.name=tinswzy`。

---

### Task 1: `core/resources.py` — 数量解析 + 集群水位聚合

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/core/resources.py`
- Test: `milvus-bootstrap/tests/test_resources.py`

**Interfaces:**
- Consumes: `probe.run_kubectl(args: list[str]) -> tuple[int,str,str]` (injectable `run` param).
- Produces:
  - `parse_cpu(s: str|None) -> int`（毫核）
  - `parse_mem(s: str|None) -> int`（字节）
  - `cluster_resources(run=run_kubectl) -> dict|None` → `{"metrics_available": bool, "cluster": {...}, "nodes": [{name, cpu_alloc_m, mem_alloc_b, cpu_req_m, cpu_lim_m, mem_req_b, mem_lim_b, pods, cpu_usage_m, mem_usage_b}, ...]}`；`get nodes` 失败 → None。

- [ ] **Step 1: Write the failing tests**

`milvus-bootstrap/tests/test_resources.py`:
```python
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_resources.py -q`
Expected: FAIL — `milvus_bootstrap.core.resources` does not exist.

- [ ] **Step 3: Create `core/resources.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_resources.py -q && python -m pytest -q`
Expected: resources tests PASS; full suite PASS (was 183, +5).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/resources.py tests/test_resources.py
git commit -m "feat(core): resources.py — parse_cpu/parse_mem + cluster_resources watermark aggregation"
```

---

### Task 2: `core/hostinfo.py` — 物理机快照（stdlib）

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/core/hostinfo.py`
- Test: `milvus-bootstrap/tests/test_hostinfo.py`

**Interfaces:**
- Produces: `collect() -> dict` with keys `hostname, os, kernel, cpu_count, mem_total_b, mem_available_b, mem_used_pct, load1, load5, load15, disk_path, disk_total_b, disk_used_b, disk_pct`（读不到的字段为 None，绝不抛）。

- [ ] **Step 1: Write the failing test**

`milvus-bootstrap/tests/test_hostinfo.py`:
```python
from milvus_bootstrap.core import hostinfo


def test_collect_keys_and_types():
    h = hostinfo.collect()
    expected = {"hostname", "os", "kernel", "cpu_count", "mem_total_b", "mem_available_b",
                "mem_used_pct", "load1", "load5", "load15", "disk_path", "disk_total_b",
                "disk_used_b", "disk_pct"}
    assert set(h) == expected
    # On the Linux CI/dev host these must be present and positive.
    assert isinstance(h["cpu_count"], int) and h["cpu_count"] > 0
    assert isinstance(h["mem_total_b"], int) and h["mem_total_b"] > 0
    assert isinstance(h["disk_total_b"], int) and h["disk_total_b"] > 0
    assert 0 <= h["disk_pct"] <= 100
    assert isinstance(h["hostname"], str) and h["hostname"]


def test_collect_never_raises(monkeypatch):
    # Even if /proc/meminfo is unreadable, collect() must not raise; mem fields -> None.
    monkeypatch.setattr(hostinfo, "_meminfo", lambda: (None, None))
    h = hostinfo.collect()
    assert h["mem_total_b"] is None and h["mem_used_pct"] is None
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_hostinfo.py -q`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Create `core/hostinfo.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_hostinfo.py -q && python -m pytest -q`
Expected: hostinfo tests PASS; full suite PASS (+2).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/hostinfo.py tests/test_hostinfo.py
git commit -m "feat(core): hostinfo.py — stdlib physical-host snapshot (no psutil)"
```

---

### Task 3: `GET /api/resources` 端点

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（插在 `app.mount("/")` 之前）
- Test: `milvus-bootstrap/tests/test_web_resources.py`（create）

**Interfaces:**
- Consumes: `hostinfo.collect()`, `resources.cluster_resources()` (Tasks 1-2); `_core().adapter.name`.
- Produces: `GET /api/resources` → `{"host": <dict>, "k8s": <dict or null>}`。

- [ ] **Step 1: Write the failing test**

`milvus-bootstrap/tests/test_web_resources.py`:
```python
from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def test_api_resources_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.get("/api/resources")
        assert r.status_code == 200
        body = r.json()
        assert "host" in body and "k8s" in body
        assert body["host"]["hostname"]                 # host always present
        assert body["k8s"] is None                       # fake adapter -> no cluster resources
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_resources.py -q`
Expected: FAIL — route 404.

- [ ] **Step 3: Add the endpoint**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, insert BEFORE the `# --- WebUI static frontend` comment / `app.mount("/", ...)` line (after the other `/api/*` routes):
```python
@app.get("/api/resources")
def api_resources() -> dict[str, Any]:
    from ..core import hostinfo, resources
    host = hostinfo.collect()
    k8s = None
    if getattr(_core().adapter, "name", "") == "k8s":
        try:
            k8s = resources.cluster_resources()
        except Exception:  # noqa: BLE001
            k8s = None
    return {"host": host, "k8s": k8s}
```

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_resources.py -q && python -m pytest -q`
Expected: PASS (+1).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/server/app.py tests/test_web_resources.py
git commit -m "feat(server): GET /api/resources — host snapshot + cluster watermark (best-effort)"
```

---

### Task 4: 前端 Overview 两卡 + 水位条

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/index.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `GET /api/resources` (Task 3); `getJSON`, `esc` (existing). The existing `#refresh` button already runs `renderOverview` (wired inline in index.html) — reuse it; do NOT add a new refresh button.
- Produces: `renderOverview` renders `#host-info` and `#k8s-res`; helpers `resBar(label, used, total, fmt)`, `fmtCpu(m)`, `fmtGiB(b)`.

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_overview_resources_present(client):
    html = client.get("/index.html").text
    assert 'id="host-info"' in html and 'id="k8s-res"' in html
    js = client.get("/assets/web.js").text
    assert "api/resources" in js and "function resBar" in js
    assert "host-info" in js and "k8s-res" in js
    assert "setInterval" not in js               # no-polling: no timers anywhere
    css = client.get("/assets/web.css").text
    assert ".resbar" in css
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_overview_resources_present -q`
Expected: FAIL — markers absent.

- [ ] **Step 3: Add the two cards to `index.html`**

In `milvus-bootstrap/src/milvus_bootstrap/webui/index.html`, after the existing 运行环境 card (the `<div class="card">...<div id="env-list">...</div></div>` block), add:
```html
      <div class="card"><div class="card-head"><h3>物理机（mb 主机）</h3></div>
        <div class="card-pad"><div id="host-info">加载中…</div></div></div>
      <div class="card"><div class="card-head"><h3>集群资源与水位</h3>
        <span class="muted" style="font-size:12px">按需快照 · 调度水位（requests/limits ÷ allocatable）</span></div>
        <div class="card-pad"><div id="k8s-res">加载中…</div></div></div>
```

- [ ] **Step 4: Add rendering to `renderOverview` + helpers**

In `web.js`, add these helpers just above `renderOverview`:
```javascript
function fmtCpu(m) { return (m == null) ? '—' : (m / 1000).toFixed(1) + ' 核'; }
function fmtGiB(b) { return (b == null) ? '—' : (b / 1073741824).toFixed(1) + ' GiB'; }
function resBar(label, used, total, fmt) {
  const pct = total > 0 ? Math.round(100 * used / total) : 0;
  const cls = pct >= 95 ? ' crit' : (pct >= 85 ? ' hot' : '');
  return `<div class="resrow"><div class="resl">${esc(label)}<span class="muted">${esc(fmt(used))} / ${esc(fmt(total))} · ${pct}%</span></div>` +
         `<div class="resbar${cls}"><i style="width:${Math.min(pct, 100)}%"></i></div></div>`;
}
function renderHostInfo(h) {
  if (!h) return '<div class="muted">无法读取物理机信息</div>';
  const memPct = h.mem_used_pct == null ? '—' : h.mem_used_pct + '%';
  const load = [h.load1, h.load5, h.load15].map(x => x == null ? '—' : x).join(' / ');
  return '<table class="tbl"><tbody>' +
    `<tr><td>主机名</td><td>${esc(h.hostname || '—')}</td></tr>` +
    `<tr><td>系统 / 内核</td><td>${esc((h.os || '—') + ' · ' + (h.kernel || '—'))}</td></tr>` +
    `<tr><td>CPU 逻辑核</td><td>${esc(h.cpu_count == null ? '—' : String(h.cpu_count))}</td></tr>` +
    `<tr><td>内存</td><td>${esc(fmtGiB(h.mem_available_b))} 可用 / ${esc(fmtGiB(h.mem_total_b))} · 已用 ${esc(memPct)}</td></tr>` +
    `<tr><td>负载 1/5/15</td><td>${esc(load)}</td></tr>` +
    `<tr><td>磁盘 ${esc(h.disk_path || '—')}</td><td>${esc(fmtGiB(h.disk_used_b))} / ${esc(fmtGiB(h.disk_total_b))} · ${esc(h.disk_pct == null ? '—' : h.disk_pct + '%')}</td></tr>` +
    '</tbody></table>';
}
function renderK8sRes(k) {
  if (!k) return '<div class="muted">未连接 k8s，无集群资源</div>';
  const c = k.cluster;
  let html = `<div class="muted" style="margin-bottom:8px">${c.nodes} 节点 · ${c.pods} pod · 合计 ${esc(fmtCpu(c.cpu_alloc_m))} / ${esc(fmtGiB(c.mem_alloc_b))} 可分配</div>`;
  html += k.nodes.map(n => {
    let rows = `<div class="resnode"><b>${esc(n.name)}</b> <span class="muted">${esc(fmtCpu(n.cpu_alloc_m))} / ${esc(fmtGiB(n.mem_alloc_b))} · ${n.pods} pod</span></div>`;
    rows += resBar('CPU 请求', n.cpu_req_m, n.cpu_alloc_m, fmtCpu);
    rows += resBar('CPU 上限', n.cpu_lim_m, n.cpu_alloc_m, fmtCpu);
    rows += resBar('内存请求', n.mem_req_b, n.mem_alloc_b, fmtGiB);
    rows += resBar('内存上限', n.mem_lim_b, n.mem_alloc_b, fmtGiB);
    if (k.metrics_available) {
      rows += resBar('CPU 用量', n.cpu_usage_m, n.cpu_alloc_m, fmtCpu);
      rows += resBar('内存用量', n.mem_usage_b, n.mem_alloc_b, fmtGiB);
    }
    return `<div class="rescard">${rows}</div>`;
  }).join('');
  if (!k.metrics_available) html += '<div class="muted" style="margin-top:6px">真实用量：N/A（metrics-server 未装）</div>';
  return html;
}
```
Then, inside `renderOverview`'s `try` block, AFTER the existing `#conn` assignment, add the resources fetch + render:
```javascript
    const rsrc = await getJSON('api/resources');
    document.getElementById('host-info').innerHTML = renderHostInfo(rsrc.host);
    document.getElementById('k8s-res').innerHTML = renderK8sRes(rsrc.k8s);
```
(The existing `#refresh` button — wired inline as `document.getElementById('refresh').onclick=renderOverview` in index.html — already re-runs this. Do NOT add a new button or any setInterval.)

- [ ] **Step 5: Add CSS**

Append to `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`:
```css
/* overview resource watermark bars */
.rescard { border:1px solid var(--border); border-radius:8px; padding:10px 12px; margin-bottom:10px; background:var(--surface-2); }
.resnode { margin-bottom:6px; }
.resrow { margin:4px 0; }
.resl { display:flex; justify-content:space-between; font-size:12px; margin-bottom:2px; }
.resbar { height:9px; background:var(--surface-3); border-radius:6px; overflow:hidden; }
.resbar > i { display:block; height:100%; background:var(--accent); border-radius:6px; }
.resbar.hot > i { background:var(--warn); }
.resbar.crit > i { background:var(--err); }
```

- [ ] **Step 6: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 7: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/index.html src/milvus_bootstrap/webui/assets/web.js src/milvus_bootstrap/webui/assets/web.css tests/test_web_static.py
git commit -m "feat(webui): Overview host-info + cluster resource watermark cards (on-demand, reuses #refresh)"
```

---

## Notes for the executor
- 每个 Task 末尾跑全量 `python -m pytest -q`（基线 183，逐任务递增），前端改动后 `node --check`。
- 关键：不得新增任何 `setInterval` / 定时轮询；物理机信息仅用 stdlib（不引入 psutil 等新依赖）。
- 手动 DoD（合并前真集群一次）：Overview 出「物理机」卡（核数/内存已用%/负载/盘）与「集群资源与水位」卡（minikube 节点 CPU/内存 请求·上限 水位条；真实用量 N/A）；点已有「刷新」按钮数据重取；`MB_ADAPTER=fake` 或断网时物理机卡仍在、集群卡提示未连接。
