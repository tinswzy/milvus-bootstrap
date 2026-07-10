# WebUI Pod 容器日志 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pods 弹窗每行加「日志」按钮，点一下单次拉该 pod 最后 100 条容器日志（非流式、无后端常驻任务）。

**Architecture:** 后端 `probe.pod_logs`（单次 `kubectl logs --tail=100`）+ `GET /api/logs` 端点；前端 `openPods` 表格加「日志」列 → `openLogs(pod,ns)` 弹窗（textContent 显示、🔄 手动刷新=再发一次单请求）。

**Tech Stack:** Python 3 + FastAPI + pytest（后端）；vanilla JS + CSS（前端）；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-10-webui-pod-logs-design.md`（决策 D1–D7）。
- **非流式 / 无常驻任务**：单次 `kubectl logs ... --tail=100`（**绝不 `-f`/follow**）；重看=再点一次（又一次独立单请求）。**禁止 setInterval / 后台轮询 / 持续连接**。
- **行数硬编码 100**：不接收客户端 tail 参数（既轻量又无注入面）；`--all-containers=true --prefix=true`（多容器前缀，无容器选择器）。
- **新 `/api/logs` 路由必须注册在 `server/app.py` 末尾 `app.mount("/", StaticFiles(...))` 之前。**
- best-effort：`pod_logs` rc≠0 → 返回 stderr 文本不抛；端点非 k8s adapter → 提示串；前端失败/空 → 占位不崩。
- 前端日志文本用 **`textContent`（不是 innerHTML）** 渲染——天然 XSS 安全、保留原文。
- 仅 managed 实例的 pod（Pods 弹窗本就 managed-only 才可开）。
- 命令在 `milvus-bootstrap/` 下跑：`cd milvus-bootstrap && source .venv/bin/activate`。基线 192 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用仓库 `user.name=tinswzy`。

---

### Task 1: 后端 `probe.pod_logs` + `GET /api/logs`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/probe.py`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（插在 `app.mount("/")` 之前）
- Test: `milvus-bootstrap/tests/test_probe.py`（追加）、`milvus-bootstrap/tests/test_web_logs.py`（create）

**Interfaces:**
- Consumes: `probe.run_kubectl(args) -> (rc, out, err)`（已有）；`_core().adapter.name`；`probe`（app.py 已 import）。
- Produces:
  - `probe.pod_logs(pod: str, namespace: str, run=run_kubectl) -> str`
  - `GET /api/logs?pod=&namespace=` → `{"pod","namespace","logs"}`。

- [ ] **Step 1: Write the failing probe test**

Add to `milvus-bootstrap/tests/test_probe.py`:
```python
def test_pod_logs_ok_and_error():
    from milvus_bootstrap.core import probe

    def ok(args):
        assert "logs" in args and "--tail=100" in args and "--all-containers=true" in args
        return (0, "line1\nline2\n", "")
    assert probe.pod_logs("mypod", "default", run=ok) == "line1\nline2\n"

    def fail(args):
        return (1, "", "Error from server (NotFound): pods \"x\" not found")
    out = probe.pod_logs("x", "default", run=fail)
    assert "NotFound" in out
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_probe.py::test_pod_logs_ok_and_error -q`
Expected: FAIL — `probe.pod_logs` does not exist.

- [ ] **Step 3: Add `pod_logs` to `probe.py`**

Append to `milvus-bootstrap/src/milvus_bootstrap/core/probe.py`:
```python
def pod_logs(pod: str, namespace: str, run=run_kubectl) -> str:
    """Last 100 lines of a pod's container logs (all containers, prefixed). One-shot, best-effort."""
    rc, out, err = run(["logs", pod, "-n", namespace, "--tail=100",
                        "--all-containers=true", "--prefix=true"])
    if rc != 0:
        return err.strip() or "（无日志或读取失败）"
    return out
```

- [ ] **Step 4: Run probe test to verify pass**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_probe.py::test_pod_logs_ok_and_error -q`
Expected: PASS.

- [ ] **Step 5: Write the failing endpoint test**

`milvus-bootstrap/tests/test_web_logs.py`:
```python
from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def test_api_logs_shape_non_k8s(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.get("/api/logs", params={"pod": "mypod", "namespace": "default"})
        assert r.status_code == 200
        body = r.json()
        assert body["pod"] == "mypod" and body["namespace"] == "default"
        assert "logs" in body and isinstance(body["logs"], str)
        assert "k8s" in body["logs"]                  # non-k8s hint string
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_logs.py -q`
Expected: FAIL — route 404.

- [ ] **Step 7: Add `GET /api/logs`**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, insert BEFORE the `# --- WebUI static frontend` comment / `app.mount("/", ...)` line (among the other `/api/*` routes):
```python
@app.get("/api/logs")
def api_logs(pod: str, namespace: str = "default") -> dict[str, Any]:
    core = _core()
    if getattr(core.adapter, "name", "") == "k8s":
        try:
            logs = probe.pod_logs(pod, namespace)
        except Exception:  # noqa: BLE001
            logs = "（读取失败）"
    else:
        logs = "（非 k8s 环境，无 pod 日志）"
    return {"pod": pod, "namespace": namespace, "logs": logs}
```

- [ ] **Step 8: Run endpoint test + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_logs.py tests/test_probe.py -q && python -m pytest -q`
Expected: all PASS (full suite was 192, +2).

- [ ] **Step 9: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/probe.py src/milvus_bootstrap/server/app.py tests/test_probe.py tests/test_web_logs.py
git commit -m "feat(server): probe.pod_logs + GET /api/logs — one-shot kubectl logs --tail=100"
```

---

### Task 2: 前端 Pods 日志列 + `openLogs`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（`openPods` + 新 `openLogs`）
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `GET /api/logs` (Task 1); `openModal`, `getJSON`, `esc`, `badge`, `ageOf` (existing).
- Produces: `openLogs(pod, ns)`; `openPods` table gains a 「日志」 column wired to `[data-log-pod]`.

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_pod_logs_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function openLogs" in js
    assert "api/logs" in js and "data-log-pod" in js and "log-view" in js
    assert "最后 100 条" in js
    # openPods table wires a log button; still no timers anywhere (single-shot)
    assert "data-log-pod" in js.split("function openPods", 1)[1].split("function ", 1)[0]
    assert "setInterval" not in js
    css = client.get("/assets/web.css").text
    assert ".logview" in css
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_pod_logs_ui_present -q`
Expected: FAIL — markers absent.

- [ ] **Step 3: Add the 日志 column + wiring to `openPods`**

In `web.js`, the current `openPods` renders the pods table. Replace its `el.innerHTML = pods.length ? ... : ...` block so the table has a 日志 column, and add wiring after it. The full new body of `openPods` after the fetch is:
```javascript
  const pods = d.pods || [];
  el.innerHTML = pods.length
    ? '<table class="tbl"><thead><tr><th>Pod</th><th>状态</th><th>Ready</th><th>重启</th><th>龄</th><th>日志</th></tr></thead><tbody>' +
      pods.map(p => `<tr><td class="mono">${esc(p.pod)}</td>` +
        `<td>${badge(p.phase === 'Running' ? 'PASS' : 'WARN', p.phase)}</td>` +
        `<td>${esc(p.ready)}</td><td>${esc(String(p.restarts))}</td><td>${esc(ageOf(p.created))}</td>` +
        `<td><button class="btn btn-ghost btn-sm" data-log-pod="${esc(p.pod)}" data-log-ns="${esc(d.namespace)}">日志</button></td></tr>`).join('') +
      '</tbody></table>'
    : `<div class="muted">ns:${esc(d.namespace)} 下未找到该实例的 pod（或未连接集群）</div>`;
  el.querySelectorAll('[data-log-pod]').forEach(b => {
    b.onclick = () => openLogs(b.getAttribute('data-log-pod'), b.getAttribute('data-log-ns'));
  });
```
(Only the table string gained a `<th>日志</th>` + a `<td>` button per row, plus the `querySelectorAll` wiring. The `let d; try { d = await getJSON('api/pods?instance='...) } catch ...` lines above stay unchanged.)

- [ ] **Step 4: Add `openLogs`**

Add this function next to `openPods` in `web.js`:
```javascript
async function openLogs(pod, ns) {
  const m = openModal('日志 · ' + pod,
    '<div style="margin-bottom:8px;display:flex;gap:8px">' +
    '<button id="log-refresh" class="btn btn-ghost btn-sm">🔄 刷新</button>' +
    '<button id="log-copy" class="btn btn-ghost btn-sm">复制</button>' +
    '<span class="muted" style="align-self:center;font-size:12px">最后 100 条 · 单次读取</span></div>' +
    '<pre id="log-view" class="logview">读取中…</pre>');
  const view = m.body.querySelector('#log-view');
  const load = async () => {
    view.textContent = '读取中…';
    try {
      const d = await getJSON('api/logs?pod=' + encodeURIComponent(pod) + '&namespace=' + encodeURIComponent(ns));
      view.textContent = d.logs || '（无日志）';
    } catch (e) {
      view.textContent = '读取失败：' + e.message;
    }
  };
  m.body.querySelector('#log-refresh').onclick = load;
  m.body.querySelector('#log-copy').onclick = () => {
    try { navigator.clipboard.writeText(view.textContent); } catch (e) { /* best-effort */ }
  };
  load();
}
```

- [ ] **Step 5: Add CSS**

Append to `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`:
```css
/* pod log view (single-shot tail, scrollable) */
.logview { max-height:420px; overflow:auto; background:var(--surface-2); border:1px solid var(--border);
  border-radius:8px; padding:10px 12px; margin:0; font-size:12px; line-height:1.5;
  white-space:pre-wrap; word-break:break-all; }
```

- [ ] **Step 6: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 7: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js src/milvus_bootstrap/webui/assets/web.css tests/test_web_static.py
git commit -m "feat(webui): pod log button in Pods modal + openLogs (single-shot last 100, textContent)"
```

---

## Notes for the executor
- 每个 Task 末尾跑全量 `python -m pytest -q`（基线 192），前端改动后 `node --check`。
- 绝不引入 setInterval / `-f`follow / 持续连接；日志用 `textContent` 渲染。
- 手动 DoD（合并前真集群一次）：某 managed milvus 卡「Pods」→ 表格每行有「日志」→ 点开见最后 100 条容器日志（多容器带 `[pod/container]` 前缀）→ 点 🔄 再取一次 → 复制可用；`MB_ADAPTER=fake`/断连时显示占位不崩。
