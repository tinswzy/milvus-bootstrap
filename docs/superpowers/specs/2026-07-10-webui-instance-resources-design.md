# WebUI 每实例资源占用（requests/limits 汇总 · 按需快照）· 设计

- 日期：2026-07-10
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：让每个 Milvus 实例的资源占用「有地方看」——**实例卡加一行资源汇总**（CPU/内存 requests·limits），**Pods 弹窗加逐 pod 资源列 + 合计**。装完后从该实例的 live pods 读，**按需一次性快照、无轮询**，复用 Overview 的 `resources.py` 解析。

## 1. 背景与口径

- mb 安装用 operator/chart 默认值，「安装那一刻」难精确预估（取决于 operator 渲染的 pod spec）。**可行且有用的是：装完后从该实例的 live pods 读实际 requests/limits 汇总**。
- **口径 = CPU/内存 requests + limits 汇总**（永远可读、不依赖 metrics-server，与 Overview 水位一致）。真实用量（`kubectl top pods`）仅 metrics-server 在时在 Pods 弹窗附带；**卡片只显 requests/limits**。
- **准则**（[[feedback-mb-lightweight-principles]]）：进卡页/开 Pods 弹窗**各读一次**，**无 setInterval / 无后台轮询**。best-effort：读不到→省略不崩；仅 managed。
- 复用 `core/resources.py`（已有 `parse_cpu`→毫核、`parse_mem`→字节、`_sum_reqs(containers, field)`）；前端已有 `fmtCpu`/`fmtGiB`。
- 现状：`/api/pods` 用 `probe.pods_of`（jsonpath，返回 pod/phase/ready/restarts/image/created，**无资源**）；`/api/instances` 每 managed milvus 行已有 image/status/rolling/deps。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 口径 | requests + limits 汇总（毫核 / 字节）。卡片显 requests **与** limits；limit 未设(=0)显「—」|
| D2 | 卡片 | Milvus 卡 MQ 行下加一行：`资源 · CPU 请求 X核/上限 Y核 · 内存 请求 A/上限 B` |
| D3 | Pods 弹窗 | pod 表加列 `CPU请求`/`CPU上限`/`内存请求`/`内存上限`；底部**合计**行；metrics-server 在则再加真实用量列/行 |
| D4 | 数据源 | 卡片：`/api/instances` 加 `res` 合计（**一次** `kubectl get pods -A -o json` 按实例聚合，非 N 次调用）。Pods：`/api/pods` 加 `resources`（该实例 `kubectl get pods -n ns -o json` + best-effort `top pods`）|
| D5 | 无轮询 | 卡页/Pods 弹窗各读一次；无定时器。`/api/instances` 为卡片合计多读一次 `get pods -A -o json`（一次批量、按需、非轮询、仅当有 managed milvus + k8s adapter）|
| D6 | pod 归属 | 单实例查询按名字段匹配（同 `pods_of`：`pod==name or startswith(name+"-")`）；批量聚合按**最长前缀**归属，防名字前缀重叠双计（如 `a` 与 `a-b`）|
| D7 | 非目标 | 不改 pod 状态表来源（`pods_of` 仍供 phase/ready）；不做资源配额/告警；不做安装表单里的预估；卡片不显真实用量（仅 Pods 弹窗） |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/resources.py` | 新 `instance_resources(name, ns, run) -> dict`、`instances_totals(instances, run) -> dict` |
| `server/app.py` | `/api/pods` 响应加 `resources`；`/api/instances` managed milvus 行加 `res` |
| `webui/assets/web.js` | 卡片资源行（`renderMilvus`）；Pods 弹窗资源列+合计（`openPods`）|
| `webui/assets/web.css` | 复用现有 `.tbl` / muted；无新样式或极少 |

## 4. 后端

### 4.1 `resources.instance_resources`
```python
def _pod_matches(pod_name: str, inst: str) -> bool:
    return pod_name == inst or pod_name.startswith(inst + "-")

def instance_resources(name: str, ns: str, run=run_kubectl) -> dict:
    """Per-pod + total requests/limits for one instance; best-effort actual usage."""
    empty = {"metrics_available": False,
             "total": {"cpu_req_m": 0, "cpu_lim_m": 0, "mem_req_b": 0, "mem_lim_b": 0, "pods": 0},
             "pods": []}
    rc, out, _ = run(["get", "pods", "-n", ns, "-o", "json"])
    if rc != 0:
        return empty
    try:
        items = json.loads(out).get("items", [])
    except Exception:  # noqa: BLE001
        return empty
    pods = []
    for p in items:
        pod_name = p.get("metadata", {}).get("name", "")
        if not _pod_matches(pod_name, name):
            continue
        conts = (p.get("spec") or {}).get("containers", [])
        rc_, rm_ = _sum_reqs(conts, "requests")
        lc_, lm_ = _sum_reqs(conts, "limits")
        pods.append({"pod": pod_name, "cpu_req_m": rc_, "cpu_lim_m": lc_,
                     "mem_req_b": rm_, "mem_lim_b": lm_, "cpu_usage_m": None, "mem_usage_b": None})
    total = {"cpu_req_m": sum(x["cpu_req_m"] for x in pods),
             "cpu_lim_m": sum(x["cpu_lim_m"] for x in pods),
             "mem_req_b": sum(x["mem_req_b"] for x in pods),
             "mem_lim_b": sum(x["mem_lim_b"] for x in pods), "pods": len(pods)}
    metrics = False
    rc, out, _ = run(["top", "pods", "-n", ns, "--no-headers"])
    if rc == 0 and out.strip():
        metrics = True
        by = {x["pod"]: x for x in pods}
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 3 and parts[0] in by:            # NAME CPU(cores) MEMORY(bytes)
                by[parts[0]]["cpu_usage_m"] = parse_cpu(parts[1])
                by[parts[0]]["mem_usage_b"] = parse_mem(parts[2])
        total["cpu_usage_m"] = sum((x["cpu_usage_m"] or 0) for x in pods)
        total["mem_usage_b"] = sum((x["mem_usage_b"] or 0) for x in pods)
    return {"metrics_available": metrics, "total": total, "pods": pods}
```

### 4.2 `resources.instances_totals`（批量，一次 get pods -A）
```python
def instances_totals(instances, run=run_kubectl) -> dict:
    """One get-pods-A-json aggregated per instance (longest-prefix ownership). {name: total}."""
    rc, out, _ = run(["get", "pods", "-A", "-o", "json"])
    if rc != 0:
        return {}
    try:
        items = json.loads(out).get("items", [])
    except Exception:  # noqa: BLE001
        return {}
    # instances: list of {"name","namespace"}
    totals = {i["name"]: {"cpu_req_m": 0, "cpu_lim_m": 0, "mem_req_b": 0, "mem_lim_b": 0, "pods": 0}
              for i in instances}
    by_ns: dict[str, list] = {}
    for i in instances:
        by_ns.setdefault(i["namespace"], []).append(i["name"])
    for p in items:
        md = p.get("metadata", {})
        ns = md.get("namespace", "")
        pod_name = md.get("name", "")
        cands = [n for n in by_ns.get(ns, []) if _pod_matches(pod_name, n)]
        if not cands:
            continue
        owner = max(cands, key=len)                           # longest prefix = most specific
        conts = (p.get("spec") or {}).get("containers", [])
        rc_, rm_ = _sum_reqs(conts, "requests")
        lc_, lm_ = _sum_reqs(conts, "limits")
        t = totals[owner]
        t["cpu_req_m"] += rc_; t["cpu_lim_m"] += lc_; t["mem_req_b"] += rm_; t["mem_lim_b"] += lm_; t["pods"] += 1
    return totals
```

### 4.3 `/api/pods` 加 `resources`
在现有 `pods = probe.pods_of(...)` 后（k8s adapter 分支内）：`resources = resources_mod.instance_resources(instance, inst.namespace)`（try/except→ empty）；响应加 `"resources": resources`。非 k8s → `resources` 为 empty 结构。

### 4.4 `/api/instances` 加 `res`
managed milvus 行：先在函数内**一次** `insts_res = resources_mod.instances_totals([{name,namespace} for 每个 managed milvus], run)`（仅当 k8s adapter 且存在 managed milvus；try/except→{}）；每行 `row["res"] = insts_res.get(i.name)`（无则 None）。

## 5. 前端

### 5.1 Milvus 卡资源行（`renderMilvus`）
MQ badge 行（`<div class="mvmeta">…MQ…</div>`）之后加：
```javascript
`${resLine(i.res)}` +
```
```javascript
function resLine(r) {
  if (!r || !r.pods) return '';
  const lim = (m, f) => m > 0 ? f(m) : '—';
  return `<div class="mvmeta muted" style="font-size:12px">资源 · CPU 请求 ${esc(fmtCpu(r.cpu_req_m))}/上限 ${esc(lim(r.cpu_lim_m, fmtCpu))}` +
         ` · 内存 请求 ${esc(fmtGiB(r.mem_req_b))}/上限 ${esc(lim(r.mem_lim_b, fmtGiB))}</div>`;
}
```

### 5.2 Pods 弹窗资源列 + 合计（`openPods`）
`d.resources` = instance_resources 输出。按 pod 名建 map `rmap = {}; (d.resources.pods||[]).forEach(x => rmap[x.pod]=x)`。
- 表头在「龄」后、「日志」前插：`<th>CPU请求</th><th>CPU上限</th><th>内存请求</th><th>内存上限</th>`（metrics 在再加 `<th>CPU用量</th><th>内存用量</th>`）。
- 每行对应插单元格：`const r = rmap[p.pod] || {}`，`fmtCpu(r.cpu_req_m||0)` 等；limit 0→「—」。
- `<tbody>` 末加合计行：`<tr class="restot"><td>合计</td><td colspan=…></td>…<td>fmtCpu(total.cpu_req_m)</td>…</tr>`（对齐列）。
- metrics 不在：不加用量列。全 `esc`。

（保留现有 pod/状态/Ready/重启/龄/日志列结构；仅在「龄」与「日志」之间插资源列。）

### 5.3 CSS
`.restot td { font-weight:600; border-top:2px solid var(--border); }`（合计行加粗上边框）；其余复用 `.tbl`。

## 6. 测试与验收
- **`resources`**（`tests/test_resources.py` 追加）：`instance_resources` 用 fake run（pods JSON：该实例 2 pod〔一个有 requests+limits、一个部分〕+ 一个他实例 pod 被过滤）→ per-pod 汇总 + total 正确、`metrics_available=False`（top rc≠0）；top 成功分支置 usage。`instances_totals`：两实例名前缀重叠（`a`/`a-b`）→ 各 pod 按最长前缀归属、不双计；跨 ns 隔离；get pods 失败→{}。
- **端点**（`tests/`，fake）：`GET /api/pods` 响应含 `resources`（fake→empty 结构，键齐）；`GET /api/instances` 某 managed milvus 行含 `res`（fake→None 或零结构，键存在不崩）。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js 含 `function resLine`/`function` 内 `i.res`/Pods 段含 `CPU请求`/`合计`/`rmap`；**`setInterval` 仍不存在**；css 含 `.restot`。
- **JS**：`node --check`。
- **手动 DoD**（真集群）：某 managed milvus 卡显示「资源 · CPU 请求…/上限… · 内存…」；点「Pods」→ 表格多出资源列 + 底部合计；metrics-server 缺时无用量列、卡片仅 requests/limits；external/未连接不崩。

## 7. 非目标 / 后续
- 安装表单里的资源预估（operator 默认，难准确；YAGNI）。
- 资源配额 / 超卖告警。
- 依赖(etcd/minio/…)的资源（本切面聚焦 milvus 实例；后续同法可接 Dependencies）。
- 卡片真实用量（仅 Pods 弹窗；需 metrics-server）。
