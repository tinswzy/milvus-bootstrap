# WebUI 每实例资源占用 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Milvus 实例卡显示该实例资源汇总（CPU/内存 requests·limits），Pods 弹窗显示逐 pod 资源 + 合计——装完从 live pods 按需读一次。

**Architecture:** 复用 `core/resources.py` 的 `parse_cpu`/`parse_mem`/`_sum_reqs`，加 `instance_resources`（单实例逐 pod）与 `instances_totals`（一次 get-pods-A 批量聚合）；`/api/pods` 加 `resources`、`/api/instances` 加 `res`；前端卡片资源行 + Pods 弹窗资源列/合计。

**Tech Stack:** Python 3 stdlib（json）+ FastAPI + pytest；vanilla JS + CSS；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-10-webui-instance-resources-design.md`（决策 D1–D7）。
- **无轮询**：卡页/Pods 弹窗各读一次；**禁止 setInterval / 定时轮询**。`/api/instances` 为卡片合计多读一次 `get pods -A -o json`（一次批量、按需、仅当 k8s adapter + 有 managed milvus）。
- **口径**：requests + limits 汇总（`parse_cpu`→毫核、`parse_mem`→字节）；卡片显 requests **与** limits（limit=0→「—」）；真实用量仅 Pods 弹窗 + metrics-server 在时。
- best-effort：读不到 / 非 k8s → 空结构或 None，前端省略不崩；仅 managed。
- **新 `/api/*` 无新增路由**（只改现有 `/api/pods`、`/api/instances`）；无新 pip 依赖。
- pod 归属：单实例按 `pod==name or startswith(name+"-")`；批量按**最长前缀**归属防重叠双计。
- 命令在 `milvus-bootstrap/` 下跑：`cd milvus-bootstrap && source .venv/bin/activate`。基线 197 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用仓库 `user.name=tinswzy`。

---

### Task 1: `resources.instance_resources` + `instances_totals`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/resources.py`
- Test: `milvus-bootstrap/tests/test_resources.py`（追加）

**Interfaces:**
- Consumes: existing `resources.parse_cpu`, `parse_mem`, `_sum_reqs(containers, field)`, `run_kubectl`.
- Produces:
  - `instance_resources(name: str, ns: str, run=run_kubectl) -> dict` → `{"metrics_available": bool, "total": {cpu_req_m,cpu_lim_m,mem_req_b,mem_lim_b,pods[,cpu_usage_m,mem_usage_b]}, "pods": [{pod,cpu_req_m,cpu_lim_m,mem_req_b,mem_lim_b,cpu_usage_m,mem_usage_b}]}`.
  - `instances_totals(instances, run=run_kubectl) -> dict` → `{name: {cpu_req_m,cpu_lim_m,mem_req_b,mem_lim_b,pods}}`（`instances` = list of `{"name","namespace"}`）。

- [ ] **Step 1: Write the failing tests**

Add to `milvus-bootstrap/tests/test_resources.py`:
```python
def _pods_json(pods):
    return {"items": [{"metadata": {"name": n, "namespace": ns},
                       "spec": {"containers": conts}} for (n, ns, conts) in pods]}


def test_instance_resources_aggregates_and_filters():
    from milvus_bootstrap.core import resources
    import json as _json
    conts_full = [{"resources": {"requests": {"cpu": "500m", "memory": "1Gi"},
                                 "limits": {"cpu": "1", "memory": "2Gi"}}}]
    conts_partial = [{"resources": {"requests": {"cpu": "250m"}}}]
    payload = _pods_json([
        ("mv-milvus-standalone-a", "default", conts_full),
        ("mv-milvus-standalone-b", "default", conts_partial),
        ("other-x", "default", conts_full),                # different instance -> filtered
    ])

    def run(args):
        if "get" in args and "pods" in args:
            return (0, _json.dumps(payload), "")
        return (1, "", "no")                               # top pods -> not available

    r = resources.instance_resources("mv", "default", run=run)
    assert r["metrics_available"] is False
    assert r["total"]["pods"] == 2
    assert r["total"]["cpu_req_m"] == 750 and r["total"]["mem_req_b"] == 1024 ** 3
    assert r["total"]["cpu_lim_m"] == 1000 and r["total"]["mem_lim_b"] == 2 * 1024 ** 3
    assert [p["pod"] for p in r["pods"]] == ["mv-milvus-standalone-a", "mv-milvus-standalone-b"]
    assert r["pods"][0]["cpu_usage_m"] is None


def test_instance_resources_top_usage():
    from milvus_bootstrap.core import resources
    import json as _json
    payload = _pods_json([("mv-milvus-standalone-a", "default",
                           [{"resources": {"requests": {"cpu": "500m"}}}])])

    def run(args):
        if "top" in args:
            return (0, "mv-milvus-standalone-a 300m 900Mi\n", "")
        return (0, _json.dumps(payload), "")

    r = resources.instance_resources("mv", "default", run=run)
    assert r["metrics_available"] is True
    assert r["pods"][0]["cpu_usage_m"] == 300 and r["pods"][0]["mem_usage_b"] == 900 * 1024 ** 2
    assert r["total"]["cpu_usage_m"] == 300


def test_instance_resources_get_fail_empty():
    from milvus_bootstrap.core import resources
    r = resources.instance_resources("mv", "default", run=lambda a: (1, "", "boom"))
    assert r["total"]["pods"] == 0 and r["pods"] == []


def test_instances_totals_longest_prefix_no_double_count():
    from milvus_bootstrap.core import resources
    import json as _json
    c = [{"resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}}]
    payload = _pods_json([
        ("a-milvus-standalone-1", "default", c),          # -> instance "a"
        ("a-b-milvus-standalone-1", "default", c),        # -> instance "a-b" (longest prefix), NOT "a"
        ("a-x", "other-ns", c),                            # ns mismatch -> ignored
    ])
    insts = [{"name": "a", "namespace": "default"}, {"name": "a-b", "namespace": "default"}]
    t = resources.instances_totals(insts, run=lambda args: (0, _json.dumps(payload), ""))
    assert t["a"]["pods"] == 1 and t["a"]["cpu_req_m"] == 100
    assert t["a-b"]["pods"] == 1 and t["a-b"]["cpu_req_m"] == 100


def test_instances_totals_get_fail_empty():
    from milvus_bootstrap.core import resources
    assert resources.instances_totals([{"name": "a", "namespace": "default"}],
                                      run=lambda a: (1, "", "boom")) == {}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_resources.py -q`
Expected: FAIL — `instance_resources`/`instances_totals` don't exist.

- [ ] **Step 3: Implement in `core/resources.py`**

Append to `milvus-bootstrap/src/milvus_bootstrap/core/resources.py`:
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
        rq, mq = _sum_reqs(conts, "requests")
        lq, lm = _sum_reqs(conts, "limits")
        pods.append({"pod": pod_name, "cpu_req_m": rq, "cpu_lim_m": lq,
                     "mem_req_b": mq, "mem_lim_b": lm, "cpu_usage_m": None, "mem_usage_b": None})
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
            if len(parts) >= 3 and parts[0] in by:          # NAME  CPU(cores)  MEMORY(bytes)
                by[parts[0]]["cpu_usage_m"] = parse_cpu(parts[1])
                by[parts[0]]["mem_usage_b"] = parse_mem(parts[2])
        total["cpu_usage_m"] = sum((x["cpu_usage_m"] or 0) for x in pods)
        total["mem_usage_b"] = sum((x["mem_usage_b"] or 0) for x in pods)
    return {"metrics_available": metrics, "total": total, "pods": pods}


def instances_totals(instances, run=run_kubectl) -> dict:
    """One get-pods-A-json aggregated per instance (longest-prefix ownership). {name: total}."""
    rc, out, _ = run(["get", "pods", "-A", "-o", "json"])
    if rc != 0:
        return {}
    try:
        items = json.loads(out).get("items", [])
    except Exception:  # noqa: BLE001
        return {}
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
        owner = max(cands, key=len)                          # longest prefix = most specific
        conts = (p.get("spec") or {}).get("containers", [])
        rq, mq = _sum_reqs(conts, "requests")
        lq, lm = _sum_reqs(conts, "limits")
        t = totals[owner]
        t["cpu_req_m"] += rq
        t["cpu_lim_m"] += lq
        t["mem_req_b"] += mq
        t["mem_lim_b"] += lm
        t["pods"] += 1
    return totals
```

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_resources.py -q && python -m pytest -q`
Expected: resources tests PASS; full suite PASS (was 197, +5).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/resources.py tests/test_resources.py
git commit -m "feat(core): instance_resources + instances_totals — per-instance requests/limits aggregation"
```

---

### Task 2: `/api/pods` 加 `resources` + `/api/instances` 加 `res`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_web_resources.py`（追加）

**Interfaces:**
- Consumes: `resources.instance_resources`, `resources.instances_totals` (Task 1); `_core()`, `probe`.
- Produces: `GET /api/pods` response gains `"resources"`; `GET /api/instances` managed milvus rows gain `"res"`.

- [ ] **Step 1: Write the failing tests**

Add to `milvus-bootstrap/tests/test_web_resources.py`:
```python
def test_api_pods_includes_resources(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="milvus", name="res-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/pods", params={"instance": "res-mv"})
        assert r.status_code == 200
        res = r.json()["resources"]
        assert "total" in res and "pods" in res
        assert set(res["total"]) >= {"cpu_req_m", "cpu_lim_m", "mem_req_b", "mem_lim_b", "pods"}


def test_api_instances_milvus_has_res_key(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="milvus", name="res-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        rows = client.get("/api/instances").json()["instances"]
        mv = [x for x in rows if x["name"] == "res-mv"][0]
        assert "res" in mv                                   # key present (fake -> None)
```
(`TestClient`, `app` are already imported at the top of `test_web_resources.py`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_resources.py -q`
Expected: FAIL — `resources` key absent from /api/pods; `res` absent from milvus rows.

- [ ] **Step 3: Wire `/api/pods`**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, the current `api_pods` body is:
```python
    pods: list[dict] = []
    if getattr(core.adapter, "name", "") == "k8s":
        try:
            pods = probe.pods_of(instance, inst.namespace)
        except Exception:
            pods = []
    return {"instance": instance, "namespace": inst.namespace, "desired_image": desired, "pods": pods}
```
Replace that whole block (from `pods: list[dict] = []` through the `return`) with:
```python
    pods: list[dict] = []
    resources_out = {"metrics_available": False,
                     "total": {"cpu_req_m": 0, "cpu_lim_m": 0, "mem_req_b": 0, "mem_lim_b": 0, "pods": 0},
                     "pods": []}
    if getattr(core.adapter, "name", "") == "k8s":
        from ..core import resources as resources_mod
        try:
            pods = probe.pods_of(instance, inst.namespace)
        except Exception:  # noqa: BLE001
            pods = []
        try:
            resources_out = resources_mod.instance_resources(instance, inst.namespace)
        except Exception:  # noqa: BLE001
            pass
    return {"instance": instance, "namespace": inst.namespace, "desired_image": desired,
            "pods": pods, "resources": resources_out}
```

- [ ] **Step 4: Wire `/api/instances`**

In `api_instances`, before the `for i in core.state.list_instances():` loop, compute the milvus totals once; inside the loop set `row["res"]` for milvus rows. Add (near where `is_k8s`/`pods` are set, before the loop):
```python
    from ..core import resources as resources_mod
    milvus_list = [{"name": inst.name, "namespace": inst.namespace}
                   for inst in core.state.list_instances()
                   if (inst.spec_snapshot or {}).get("kind") == "milvus"]
    insts_res = {}
    if is_k8s and milvus_list:
        try:
            insts_res = resources_mod.instances_totals(milvus_list)
        except Exception:  # noqa: BLE001
            insts_res = {}
```
Then, inside the loop, in the `if kind == "milvus":` block (right after the existing `row.update(probe.rollout_of(...))`), add:
```python
            row["res"] = insts_res.get(i.name)
```

- [ ] **Step 5: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_resources.py -q && python -m pytest -q`
Expected: PASS (+2).

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/server/app.py tests/test_web_resources.py
git commit -m "feat(server): /api/pods +resources, /api/instances +res (per-instance requests/limits)"
```

---

### Task 3: 前端 — 卡片资源行 + Pods 弹窗资源列/合计

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `/api/instances` row `res` + `/api/pods` `resources` (Task 2); `fmtCpu`, `fmtGiB`, `esc`, `badge`, `ageOf` (existing).
- Produces: `resLine(r)`; `renderMilvus` card gains a resource line; `openPods` table gains resource columns + 合计 row.

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_instance_resources_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function resLine" in js and "resLine(i.res)" in js   # card resource line
    body = js.split("async function openPods", 1)[1].split("\nfunction ", 1)[0]
    assert "CPU请求" in body and "合计" in body and "rmap" in body   # Pods resource columns + total
    assert "setInterval" not in js
    css = client.get("/assets/web.css").text
    assert ".restot" in css
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_instance_resources_ui_present -q`
Expected: FAIL — markers absent.

- [ ] **Step 3: Add `resLine` + card resource line**

In `web.js`, add `resLine` next to `fmtCpu`/`fmtGiB` (they were added in the Overview slice):
```javascript
function resLine(r) {
  if (!r || !r.pods) return '';
  const lim = (m, f) => m > 0 ? f(m) : '—';
  return `<div class="mvmeta muted" style="font-size:12px">资源 · CPU 请求 ${esc(fmtCpu(r.cpu_req_m))}/上限 ${esc(lim(r.cpu_lim_m, fmtCpu))}` +
         ` · 内存 请求 ${esc(fmtGiB(r.mem_req_b))}/上限 ${esc(lim(r.mem_lim_b, fmtGiB))}</div>`;
}
```
In `renderMilvus`, the card currently has the MQ meta line:
```javascript
            `<div class="mvmeta"><span class="badge b-accent"><span class="d"></span>MQ: ${esc(d.mq || '—')}</span></div>` +
```
Add the resource line immediately after it:
```javascript
            `<div class="mvmeta"><span class="badge b-accent"><span class="d"></span>MQ: ${esc(d.mq || '—')}</span></div>` +
            `${resLine(i.res)}` +
```

- [ ] **Step 4: Add resource columns + 合计 to `openPods`**

In `openPods`, after the `const pods = d.pods || [];` line, add a resource map; then extend the table header and rows and append a 合计 row. Replace the `el.innerHTML = pods.length ? ... : ...;` assignment (the pods-table block, which currently ends the row with the 日志 `<td>`) with this version:
```javascript
  const pods = d.pods || [];
  const rd = d.resources || { pods: [], total: {}, metrics_available: false };
  const rmap = {};
  (rd.pods || []).forEach(x => { rmap[x.pod] = x; });
  const mc = rd.metrics_available;
  const fc = m => (m ? esc(fmtCpu(m)) : '—');
  const fg = b => (b ? esc(fmtGiB(b)) : '—');
  el.innerHTML = pods.length
    ? '<table class="tbl"><thead><tr><th>Pod</th><th>状态</th><th>Ready</th><th>重启</th><th>龄</th>' +
      '<th>CPU请求</th><th>CPU上限</th><th>内存请求</th><th>内存上限</th>' +
      (mc ? '<th>CPU用量</th><th>内存用量</th>' : '') + '<th>日志</th></tr></thead><tbody>' +
      pods.map(p => {
        const r = rmap[p.pod] || {};
        const usage = mc ? `<td>${fc(r.cpu_usage_m)}</td><td>${fg(r.mem_usage_b)}</td>` : '';
        return `<tr><td class="mono">${esc(p.pod)}</td>` +
          `<td>${badge(p.phase === 'Running' ? 'PASS' : 'WARN', p.phase)}</td>` +
          `<td>${esc(p.ready)}</td><td>${esc(String(p.restarts))}</td><td>${esc(ageOf(p.created))}</td>` +
          `<td>${fc(r.cpu_req_m)}</td><td>${fc(r.cpu_lim_m)}</td><td>${fg(r.mem_req_b)}</td><td>${fg(r.mem_lim_b)}</td>` +
          usage +
          `<td><button class="btn btn-ghost btn-sm" data-log-pod="${esc(p.pod)}" data-log-ns="${esc(d.namespace)}">日志</button></td></tr>`;
      }).join('') +
      (rd.total && rd.total.pods ? '<tr class="restot"><td colspan="5">合计（' + esc(String(rd.total.pods)) + ' pod）</td>' +
        `<td>${fc(rd.total.cpu_req_m)}</td><td>${fc(rd.total.cpu_lim_m)}</td><td>${fg(rd.total.mem_req_b)}</td><td>${fg(rd.total.mem_lim_b)}</td>` +
        (mc ? `<td>${fc(rd.total.cpu_usage_m)}</td><td>${fg(rd.total.mem_usage_b)}</td>` : '') + '<td></td></tr>' : '') +
      '</tbody></table>'
    : `<div class="muted">ns:${esc(d.namespace)} 下未找到该实例的 pod（或未连接集群）</div>`;
  el.querySelectorAll('[data-log-pod]').forEach(b => {
    b.onclick = () => openLogs(b.getAttribute('data-log-pod'), b.getAttribute('data-log-ns'));
  });
```
(The pre-fetch `let d; try { d = await getJSON('api/pods...') } catch ...` lines above stay unchanged. The `[data-log-pod]` wiring from the pod-logs slice is preserved.)

- [ ] **Step 5: Add CSS**

Append to `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`:
```css
/* per-instance resource total row in Pods modal */
.restot td { font-weight:600; border-top:2px solid var(--border); }
```

- [ ] **Step 6: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 7: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js src/milvus_bootstrap/webui/assets/web.css tests/test_web_static.py
git commit -m "feat(webui): per-instance resource summary on card + per-pod columns/total in Pods modal"
```

---

## Notes for the executor
- 每个 Task 末尾跑全量 `python -m pytest -q`（基线 197，逐任务递增），前端改动后 `node --check`。
- 不得新增 setInterval；资源全按需读一次。
- 手动 DoD（合并前真集群一次）：某 managed milvus 卡显示「资源 · CPU 请求…/上限… · 内存…」；点「Pods」→ 表格多出 CPU/内存 请求·上限列 + 底部合计行；metrics-server 缺→无用量列；external/未连接→卡无资源行、不崩。
