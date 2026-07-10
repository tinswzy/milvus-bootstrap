# Overview 增强：物理机 + 集群资源与水位 · 设计

- 日期：2026-07-10
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：Overview 页新增两块——**mb 所在物理机信息**（stdlib 读 `/proc`，无 psutil）与**集群资源与使用水位**（节点 allocatable + 各 pod requests/limits，**调度水位**口径；metrics-server 在则顺带真实用量）。**全部按需一次性快照 + 手动 🔄 刷新，无 setInterval / 无常驻轮询**——守住 mb 无轮询·轻量准则。

## 1. 背景与关键约束（已核实）

- 现状 Overview（`webui/index.html` + `renderOverview`）只有两卡：k8s 连接、运行环境（读 `GET /api/doctor`）。
- **live 实测**：本 minikube **无 metrics-server**（`kubectl top` → "Metrics API not available"）；但 `kubectl get nodes` 的 capacity/allocatable、`kubectl get pods -A` 的 requests/limits + nodeName **永远可读**。物理机 `/proc` 可读（12 核 / 31GiB / load / 盘）；**`psutil` 未装**（mb 保持最小依赖）→ 物理机信息走 **stdlib**（`os`/`/proc`/`shutil`）。
- **准则**（[[feedback-mb-lightweight-principles]]）：mb 绝不后台持续轮询 k8s；本切面所有数据都是**进页读一次 + 点刷新再读一次**，无定时器。虽用户口语称"监测"，实现是**按需快照**。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 水位口径 | **requests/limits ÷ allocatable = 调度水位**（永远可读，不依赖 metrics-server）。同时显示 requests 与 limits 两条 |
| D2 | 真实用量 | best-effort `kubectl top nodes`：成功则附真实 CPU/内存用量条，失败 → `metrics_available=false`，前端显示「N/A（metrics-server 未装）」 |
| D3 | 物理机信息 | stdlib，无 psutil：主机名 / OS·内核 / 逻辑核数 / 内存总·可用·已用% / 负载 1·5·15 / `MB_HOME` 所在盘 容量·已用·% |
| D4 | 无轮询 | 进 Overview 读一次；页头 **🔄 刷新** 手动重读。**无 setInterval / 后台轮询** |
| D5 | 数据获取 | `kubectl get nodes -o json` + `kubectl get pods -A -o json`（各一次），python 解析（对可选的嵌套 requests/limits 比 jsonpath 稳）；数量单位用 `parse_cpu`（→毫核）/`parse_mem`（→字节）解析 |
| D6 | 盘 | 物理机盘 = `MB_HOME` 所在文件系统（不存在则回退 `/`）|
| D7 | 集群摘要 | 节点数、pod 总数、集群 allocatable 合计、requests/limits 合计与水位% |
| D8 | best-effort | 非 k8s adapter / 连不上 → `k8s=null`，前端只显示物理机卡不崩；物理机某字段读不到 → 该字段 null |
| D9 | 非目标 | 不做历史曲线/时序、不做告警、不做 per-pod 明细表（只到 per-node）；不引入 metrics-server 依赖；不引入新 pip 依赖 |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/hostinfo.py` | 新：`collect() -> dict` 物理机快照（stdlib）|
| `core/resources.py` | 新：`parse_cpu(s)->int(毫核)`、`parse_mem(s)->int(字节)`、`cluster_resources(run=run_kubectl)->dict\|None` 节点/集群水位聚合 + best-effort top |
| `server/app.py` | 新：`GET /api/resources`（注册在 `app.mount("/")` 之前）|
| `webui/index.html` | Overview 加两卡 `#host-info` / `#k8s-res` + 页头 `🔄 刷新` 按钮 |
| `webui/assets/web.js` | `renderOverview` 拉 `api/resources` 并渲染两卡（水位条）；刷新按钮重渲染 |
| `webui/assets/web.css` | `.resbar`（水位条，>85% 转黄/红）|

## 4. 后端

### 4.1 `core/hostinfo.py`（stdlib only）
```python
"""Physical-host snapshot for the machine mb runs on. Stdlib only, best-effort."""
from __future__ import annotations
import os, shutil, socket

def _meminfo() -> tuple[int | None, int | None]:
    try:
        d = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, rest = line.partition(":")
                d[k.strip()] = int(rest.strip().split()[0]) * 1024  # kB -> bytes
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
    try: out["hostname"] = socket.gethostname()
    except Exception: pass  # noqa: BLE001,E701
    try:
        u = os.uname(); out["os"] = u.sysname; out["kernel"] = u.release
    except Exception: pass  # noqa: BLE001,E701
    out["cpu_count"] = os.cpu_count()
    mt, ma = _meminfo()
    out["mem_total_b"], out["mem_available_b"] = mt, ma
    if mt and ma is not None:
        out["mem_used_pct"] = round(100 * (mt - ma) / mt, 1)
    try:
        l1, l5, l15 = os.getloadavg(); out["load1"], out["load5"], out["load15"] = round(l1, 2), round(l5, 2), round(l15, 2)
    except Exception: pass  # noqa: BLE001,E701
    try:
        p = _disk_path(); du = shutil.disk_usage(p)
        out["disk_path"] = p; out["disk_total_b"] = du.total; out["disk_used_b"] = du.used
        out["disk_pct"] = round(100 * du.used / du.total, 1) if du.total else None
    except Exception: pass  # noqa: BLE001,E701
    return out
```

### 4.2 `core/resources.py`
```python
"""On-demand cluster resource watermark (requests/limits vs allocatable). Best-effort."""
from __future__ import annotations
import json
from .probe import run_kubectl

def parse_cpu(s: str | None) -> int:
    """k8s CPU quantity -> millicores. '12'->12000, '500m'->500, '0.5'->500."""
    if not s: return 0
    s = str(s).strip()
    if s.endswith("m"):
        try: return int(float(s[:-1]))
        except ValueError: return 0
    try: return int(float(s) * 1000)
    except ValueError: return 0

_MEM_UNITS = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4, "Pi": 1024**5,
              "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4, "P": 1000**5}

def parse_mem(s: str | None) -> int:
    """k8s memory quantity -> bytes. '32779072Ki', '512Mi', '2Gi', '1000000'."""
    if not s: return 0
    s = str(s).strip()
    for u, mult in _MEM_UNITS.items():
        if s.endswith(u):
            try: return int(float(s[:-len(u)]) * mult)
            except ValueError: return 0
    try: return int(float(s))
    except ValueError: return 0

def _sum_reqs(containers, field):
    cpu = mem = 0
    for c in containers or []:
        r = (c.get("resources") or {}).get(field) or {}
        cpu += parse_cpu(r.get("cpu")); mem += parse_mem(r.get("memory"))
    return cpu, mem

def cluster_resources(run=run_kubectl) -> dict | None:
    rc, out, _ = run(["get", "nodes", "-o", "json"])
    if rc != 0: return None
    try: nodes_j = json.loads(out)
    except Exception: return None  # noqa: BLE001
    nodes: dict[str, dict] = {}
    for n in nodes_j.get("items", []):
        name = n["metadata"]["name"]; alloc = n.get("status", {}).get("allocatable", {})
        nodes[name] = {"name": name, "cpu_alloc_m": parse_cpu(alloc.get("cpu")),
                       "mem_alloc_b": parse_mem(alloc.get("memory")),
                       "cpu_req_m": 0, "cpu_lim_m": 0, "mem_req_b": 0, "mem_lim_b": 0, "pods": 0,
                       "cpu_usage_m": None, "mem_usage_b": None}
    pod_total = 0
    rc, out, _ = run(["get", "pods", "-A", "-o", "json"])
    if rc == 0:
        try: pods_j = json.loads(out)
        except Exception: pods_j = {"items": []}  # noqa: BLE001
        for p in pods_j.get("items", []):
            nn = (p.get("spec") or {}).get("nodeName")
            if nn not in nodes: continue
            pod_total += 1; nodes[nn]["pods"] += 1
            conts = (p.get("spec") or {}).get("containers", [])
            rc_, rm_ = _sum_reqs(conts, "requests"); lc_, lm_ = _sum_reqs(conts, "limits")
            nodes[nn]["cpu_req_m"] += rc_; nodes[nn]["mem_req_b"] += rm_
            nodes[nn]["cpu_lim_m"] += lc_; nodes[nn]["mem_lim_b"] += lm_
    # best-effort actual usage
    metrics = False
    rc, out, _ = run(["top", "nodes", "--no-headers"])
    if rc == 0 and out.strip():
        metrics = True
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0] in nodes:
                nodes[parts[0]]["cpu_usage_m"] = parse_cpu(parts[1]); nodes[parts[0]]["mem_usage_b"] = parse_mem(parts[3])
    nlist = list(nodes.values())
    def _sum(k): return sum(x[k] or 0 for x in nlist)
    cluster = {"nodes": len(nlist), "pods": pod_total,
               "cpu_alloc_m": _sum("cpu_alloc_m"), "mem_alloc_b": _sum("mem_alloc_b"),
               "cpu_req_m": _sum("cpu_req_m"), "cpu_lim_m": _sum("cpu_lim_m"),
               "mem_req_b": _sum("mem_req_b"), "mem_lim_b": _sum("mem_lim_b")}
    if metrics:
        cluster["cpu_usage_m"] = _sum("cpu_usage_m"); cluster["mem_usage_b"] = _sum("mem_usage_b")
    return {"metrics_available": metrics, "cluster": cluster, "nodes": nlist}
```
（`top nodes --no-headers` 行形如 `minikube 1200m 10% 3000Mi 9%`——取第 2、4 列。）

### 4.3 `GET /api/resources`（注册在 `app.mount` 之前）
```python
@app.get("/api/resources")
def api_resources() -> dict[str, Any]:
    from ..core import hostinfo, resources
    host = hostinfo.collect()
    k8s = None
    if getattr(_core().adapter, "name", "") == "k8s":
        try: k8s = resources.cluster_resources()
        except Exception: k8s = None  # noqa: BLE001
    return {"host": host, "k8s": k8s}
```

## 5. 前端

### 5.1 `index.html`
页头 `.page-head` 右侧加 `<button id="ov-refresh" class="btn btn-ghost btn-sm">🔄 刷新</button>`；在现有两卡之后加：
```html
<div class="card"><div class="card-head"><h3>物理机（mb 主机）</h3></div>
  <div class="card-pad"><div id="host-info">加载中…</div></div></div>
<div class="card"><div class="card-head"><h3>集群资源与水位</h3><span class="muted" style="font-size:12px">按需快照 · 调度水位（requests/limits ÷ allocatable）</span></div>
  <div class="card-pad"><div id="k8s-res">加载中…</div></div></div>
```

### 5.2 `renderOverview` 增段
doctor 渲染后追加：`getJSON('api/resources')` → 渲染 `#host-info`（字段表：主机名/OS·内核/核数/内存 已用%（gib）/负载/盘 已用%）与 `#k8s-res`：
- `k8s==null` → 「未连接 k8s，无集群资源」。
- 否则集群摘要行（节点数·pod 数·CPU/内存 requests 水位）+ 每节点：`resBar('CPU 请求', cpu_req_m, cpu_alloc_m, 毫核→核)`、`resBar('CPU 上限', cpu_lim_m, cpu_alloc_m)`、`resBar('内存请求', mem_req_b, mem_alloc_b, 字节→GiB)`、`resBar('内存上限', mem_lim_b, mem_alloc_b)`；真实用量：`metrics_available` 则 `resBar('CPU 用量', cpu_usage_m, cpu_alloc_m)` 等，否则「真实用量：N/A（metrics-server 未装）」。
- `resBar(label, used, total, fmt)`：`pct=total>0?round(100*used/total):0`；返回 `label + 条(.resbar，宽 pct%，>85 加 .hot) + "used/total · pct%"`（数值用 `fmt` 人类化：`fmtCpu(m)=（m/1000).toFixed(1)+' 核'`、`fmtGiB(b)=(b/1073741824).toFixed(1)+' GiB'`）。全部 `esc`。
- `#ov-refresh` onclick = `renderOverview`（重读一次，幂等，无定时器）。

### 5.3 CSS
`.resbar{height:9px;background:var(--surface-3);border-radius:6px;overflow:hidden;margin:3px 0} .resbar>i{display:block;height:100%;background:var(--accent);border-radius:6px} .resbar.hot>i{background:var(--warn)} .resbar.crit>i{background:var(--err)}`（>85% hot 黄、>95% crit 红）；水位行 label + 数值排版复用现有 `.tbl`/flex。

## 6. 测试与验收
- **`resources`**（`tests/test_resources.py`）：`parse_cpu`（`"12"→12000`、`"500m"→500`、`"0.5"→500`、`""→0`、`"x"→0`）；`parse_mem`（`"32779072Ki"`、`"512Mi"→536870912`、`"2Gi"→2147483648`、`"1000000"→1000000`、`""→0`）；`cluster_resources` 用 fake `run`（返 nodes/pods JSON）→ 每节点 requests 汇总 + 集群总计 + `metrics_available=false`（top 返 rc!=0）；top 成功分支置 usage。
- **`hostinfo`**（`tests/test_hostinfo.py`）：`collect()` 返回全部键；Linux 下 `cpu_count`/`mem_total_b`/`disk_total_b` 非空且为正；类型正确。
- **端点**（`tests/`）：`GET /api/resources` 返回 `{host:{...}, k8s: null 或 dict}`（fake adapter → k8s null；host 有 hostname）。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js 含 `api/resources`/`resBar`/`host-info`/`k8s-res`/`ov-refresh`；index.html 含 `id="host-info"`/`id="k8s-res"`/`ov-refresh`；css 含 `.resbar`；**无 `setInterval`** 新增（grep 断言 renderOverview 段无定时器）。
- **JS**：`node --check`。
- **手动 DoD**（真集群）：Overview 出「物理机」卡（核数/内存已用%/负载/盘）与「集群资源与水位」卡（minikube 节点 CPU/内存 requests·limits 水位条；真实用量显示 N/A）；点 🔄 刷新数据重取；断网/非 k8s 时物理机卡仍在、集群卡提示未连接。

## 7. 非目标 / 后续
- 历史曲线 / 时序 / 告警（本切面只快照）。
- per-pod 资源明细（只到 per-node）。
- 引导安装 metrics-server（仅 best-effort 消费）。
- 后续可加：Top-N 占用 pod（需 metrics-server）、PVC/存储容量水位。
