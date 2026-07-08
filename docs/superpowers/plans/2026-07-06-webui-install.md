# WebUI Install slice — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Let the WebUI install base components (etcd/minio/kafka/pulsar/milvus) via a per-component form: dry-run preview → async apply with polling, with compat-gate blocks surfaced as a clean 409 (+ `--force` retry).

**Architecture:** A generic in-process `TaskRunner` (submit fn → task_id, poll status) backs a new async `POST /api/install` (dry-run sync, apply async) + `GET /api/task/{id}`. A global FastAPI exception handler maps `CompatError`→409 and `ValueError`→400 (also fixes the pre-existing 500-on-gate issue). A new `webui/install.html` + `renderInstall()` drives the form, polling, and force-retry.

**Tech Stack:** Python 3.11, FastAPI + uvicorn, pydantic, threading (stdlib), pytest + fastapi.testclient. Frontend: vanilla HTML/CSS/JS.

## Global Constraints

- **First UI write path.** Only `/api/install` mutates; still gated behind `mb web` default `127.0.0.1` binding (unchanged from the overview slice).
- **dry-run is synchronous** (fast, returns the planned Task); **apply is asynchronous** (submit → 202 `{task_id}`, poll `GET /api/task/{id}`).
- **Apply must do a synchronous gate pre-check before submitting** — run `_core().install(spec, dry_run=True, force=force)` first; a raised `CompatError` becomes 409 *before* the 202 is returned (the async thread's exceptions can't reach the HTTP handler).
- **Exception handlers:** `CompatError` → HTTP 409 `{error:"compat", reason, force_hint:true}`; other `ValueError` → HTTP 400 `{error:"bad_request", reason}`. Register `CompatError` handler explicitly (it subclasses `ValueError`).
- **Components:** etcd / minio / kafka / pulsar / milvus (no woodpecker).
- **milvus param defaults** (prefilled in the form): `mq=kafka`, `image=milvusdb/milvus:v2.6.18`, `storageEndpoint=minio.default.svc:80`, `kafkaBrokers=kafka-dev.default.svc:9092`. Other components default to no params.
- **Task poll states:** `running` (task=null) / `succeeded` / `failed` / `rolled_back` / `error`. Finding/step levels unchanged. XSS: all server strings via `esc()`.
- **Frontend has no JS unit tests** (YAGNI) — verified by content-marker tests + manual DoD.
- Tests hermetic (MB_ADAPTER=fake). Run from `milvus-bootstrap/` with `source .venv/bin/activate`.
- Branch `feat/webui-install` off main. End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Verified current shapes:
- `Core.install(spec, dry_run=True, force=False) -> Task`; gate is milvus-only, raised inside for incompatible MQ.
- `Task` → `.model_dump(mode="json")` gives `{id,type,target,dry_run,status,steps:[{name,status,plan,detail}],audit}`.
- `compat.CompatError` at `milvus_bootstrap/core/compat.py` (subclass of `ValueError`).
- `server/app.py` already imports: `from fastapi import FastAPI`, `from ..core.context import Core`, `from ..core.models import InstallSpec, Platform`, `from ..core import doctor, webapi`; has `_core()`, `InstallReq` model, and (registered LAST) a `StaticFiles` mount at `/`.
- `webui/assets/web.js` has `NAV` (install item currently `{id:'install', label:'安装向导（待做）', disabled:true}` at line 5), `esc()`, `shell()`, `getJSON()`, `badge()`. No `postJSON`.

---

### Task 1: `core/taskrunner.py` — in-process async task runner

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/core/taskrunner.py`
- Test: `milvus-bootstrap/tests/test_taskrunner.py`

**Interfaces:**
- Produces: `class TaskRunner` with `submit(fn: Callable[[], Any]) -> str` (runs `fn` in a daemon thread; stores result/exception) and `get(task_id: str) -> dict | None` returning `{"state": "running"|"done"|"error", "result": Any|None, "error": str|None}` (None if unknown id).

- [ ] **Step 1: Write the failing tests** — create `tests/test_taskrunner.py`:

```python
import time

from milvus_bootstrap.core.taskrunner import TaskRunner


def _wait(runner, tid, timeout=3.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        rec = runner.get(tid)
        if rec and rec["state"] != "running":
            return rec
        time.sleep(0.01)
    return runner.get(tid)


def test_runner_success():
    r = TaskRunner()
    tid = r.submit(lambda: "hello")
    assert isinstance(tid, str) and tid
    rec = _wait(r, tid)
    assert rec["state"] == "done" and rec["result"] == "hello" and rec["error"] is None


def test_runner_error():
    r = TaskRunner()
    def boom():
        raise ValueError("nope")
    rec = _wait(r, r.submit(boom))
    assert rec["state"] == "error" and "nope" in rec["error"] and rec["result"] is None


def test_runner_unknown_id():
    assert TaskRunner().get("does-not-exist") is None


def test_runner_running_before_done():
    import threading
    gate = threading.Event()
    r = TaskRunner()
    tid = r.submit(lambda: gate.wait(2) or "ok")
    assert r.get(tid)["state"] == "running"   # still blocked
    gate.set()
    assert _wait(r, tid)["state"] == "done"
```

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_taskrunner.py -v`
Expected: FAIL — `ModuleNotFoundError: ... taskrunner`.

- [ ] **Step 3: Implement `core/taskrunner.py`**

```python
"""Tiny in-process async runner: submit a fn, poll its status. No business logic."""
from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any


class TaskRunner:
    def __init__(self) -> None:
        self._recs: dict[str, dict] = {}
        self._lock = threading.Lock()

    def submit(self, fn: Callable[[], Any]) -> str:
        tid = uuid.uuid4().hex[:12]
        with self._lock:
            self._recs[tid] = {"state": "running", "result": None, "error": None}

        def _run() -> None:
            try:
                res = fn()
                rec = {"state": "done", "result": res, "error": None}
            except Exception as exc:  # noqa: BLE001
                rec = {"state": "error", "result": None, "error": str(exc)}
            with self._lock:
                self._recs[tid] = rec

        threading.Thread(target=_run, daemon=True).start()
        return tid

    def get(self, task_id: str) -> dict | None:
        with self._lock:
            rec = self._recs.get(task_id)
            return dict(rec) if rec is not None else None
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_taskrunner.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/taskrunner.py milvus-bootstrap/tests/test_taskrunner.py
git commit -m "feat(taskrunner): in-process async task runner (submit/get)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `POST /api/install` (dry-run sync + apply async) + `GET /api/task/{id}`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_web_install.py`

**Interfaces:**
- Consumes: `TaskRunner` (Task 1), existing `InstallReq`, `_core()`.
- Produces:
  - `POST /api/install` — `dry_run=true` → 200 `{"task": <Task dump>}`; `dry_run=false` → runs a sync gate pre-check (`install(dry_run=True)`), then 202 `{"task_id", "state":"running"}`.
  - `GET /api/task/{task_id}` → 200 `{"state", "task", "error"}` (state = "running" | task.status | "error"); 404 if unknown.
  - Module-level `runner = TaskRunner()` in app.py.

- [ ] **Step 1: Write the failing tests** — create `tests/test_web_install.py`:

```python
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.server.app import app
    with TestClient(app) as c:
        yield c


def test_install_dry_run_returns_planned_task(client):
    r = client.post("/api/install", json={"kind": "etcd", "name": "e1", "dry_run": True})
    assert r.status_code == 200
    task = r.json()["task"]
    assert task["dry_run"] is True and task["steps"]


def test_install_apply_async_then_poll(client):
    r = client.post("/api/install", json={"kind": "etcd", "name": "e2", "dry_run": False})
    assert r.status_code == 202
    tid = r.json()["task_id"]
    assert r.json()["state"] == "running"
    # poll to completion (fake adapter installs fast)
    end = time.monotonic() + 5
    state = "running"
    while time.monotonic() < end:
        j = client.get(f"/api/task/{tid}").json()
        state = j["state"]
        if state != "running":
            break
        time.sleep(0.05)
    assert state == "succeeded"
    assert client.get(f"/api/task/{tid}").json()["task"]["target"] == "e2"


def test_task_unknown_id_404(client):
    assert client.get("/api/task/nope").status_code == 404
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_install.py -v`
Expected: FAIL — 404 on `/api/install`.

- [ ] **Step 3: Implement in `server/app.py`**

Add imports (with the other imports at the top):
```python
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from ..core.taskrunner import TaskRunner
```
Add a module-level runner (after `app = FastAPI(...)`):
```python
runner = TaskRunner()
```
Add the routes (after the existing `/api/compat-rules` route, and BEFORE the static mount at the end of the file):
```python
@app.post("/api/install")
def api_install(req: InstallReq) -> Any:
    spec = InstallSpec(
        kind=req.kind, name=req.name, platform=req.platform,
        method=req.method, namespace=req.namespace, params=req.params,
        chart_override=req.chart_override,
    )
    if req.dry_run:
        task = _core().install(spec, dry_run=True, force=req.force)
        return {"task": task.model_dump(mode="json")}
    # apply: synchronous gate pre-check (raises CompatError -> 409 via handler), then submit
    _core().install(spec, dry_run=True, force=req.force)
    tid = runner.submit(lambda: _core().install(spec, dry_run=False, force=req.force))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)


@app.get("/api/task/{task_id}")
def api_task(task_id: str) -> dict[str, Any]:
    rec = runner.get(task_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="unknown task")
    if rec["state"] == "running":
        return {"state": "running", "task": None, "error": None}
    if rec["state"] == "error":
        return {"state": "error", "task": None, "error": rec["error"]}
    dump = rec["result"].model_dump(mode="json")   # a Task
    return {"state": dump["status"], "task": dump, "error": None}
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_install.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/tests/test_web_install.py
git commit -m "feat(server): async POST /api/install + GET /api/task/{id}

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Global exception handlers (CompatError→409, ValueError→400)

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_web_install.py` (append)

**Interfaces:**
- Consumes: `compat.CompatError`.
- Produces: FastAPI exception handlers so any route raising `CompatError` → 409 `{error:"compat", reason, force_hint:true}`, other `ValueError` → 400 `{error:"bad_request", reason}`.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_web_install.py`:

```python
def test_install_milvus_incompatible_mq_returns_409(client):
    r = client.post("/api/install", json={
        "kind": "milvus", "name": "m1", "dry_run": True,
        "params": {"mq": "woodpecker-service", "image": "milvusdb/milvus:v2.6.3"}})
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "compat" and body["force_hint"] is True
    assert "3.0" in body["reason"]           # woodpecker-service needs milvus >= 3.0


def test_install_milvus_force_bypasses_gate(client):
    r = client.post("/api/install", json={
        "kind": "milvus", "name": "m2", "dry_run": True, "force": True,
        "params": {"mq": "woodpecker-service", "image": "milvusdb/milvus:v2.6.3"}})
    assert r.status_code == 200          # force -> gate downgraded, dry-run proceeds
    assert r.json()["task"]["dry_run"] is True
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_install.py -k "incompatible_mq or force_bypasses" -v`
Expected: FAIL — currently the `CompatError` surfaces as 500 (no handler).

- [ ] **Step 3: Add exception handlers to `server/app.py`** (add import + handlers after `app = FastAPI(...)`; register `CompatError` before `ValueError` since it subclasses it):

```python
from fastapi import Request
from ..core.compat import CompatError


@app.exception_handler(CompatError)
def _compat_handler(request: Request, exc: CompatError) -> JSONResponse:
    return JSONResponse(status_code=409,
                        content={"error": "compat", "reason": str(exc), "force_hint": True})


@app.exception_handler(ValueError)
def _value_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(status_code=400,
                        content={"error": "bad_request", "reason": str(exc)})
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_install.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/tests/test_web_install.py
git commit -m "feat(server): map CompatError->409 / ValueError->400 (clean gate errors)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Install form frontend (`install.html` + `renderInstall()`)

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/install.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (enable nav item, add `postJSON()` + `renderInstall()`)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `/api/install`, `/api/task/{id}` (Tasks 2-3); existing `shell()`, `esc()`, `getJSON()`, `badge()`.
- Produces: `GET /install.html` served; `renderInstall()` in web.js; `postJSON(url, body) -> {status, data}`; nav "安装向导" enabled.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:

```python
def test_install_page_served(client):
    r = client.get("/install.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="inst-kind"' in r.text and 'id="inst-params"' in r.text and 'id="inst-result"' in r.text
    js = client.get("/assets/web.js").text
    assert "renderInstall" in js and "postJSON" in js
    assert "安装向导（待做）" not in js       # nav item enabled, not the disabled placeholder
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k install_page -v`
Expected: FAIL — `/install.html` 404, no `renderInstall`.

- [ ] **Step 3: Enable the nav item in `webui/assets/web.js`** — replace line 5:

```javascript
  { id: 'install',  label: '安装向导', href: 'install.html' },
```

- [ ] **Step 4: Add `postJSON()` to `webui/assets/web.js`** (after `getJSON`):

```javascript
async function postJSON(url, body) {
  const r = await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  let data = null;
  try { data = await r.json(); } catch (e) { /* empty body */ }
  return { status: r.status, data };
}
```

- [ ] **Step 5: Create `webui/install.html`**

```html
<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>安装向导 · Milvus Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/web.css">
</head><body>
<div class="app">
  <aside class="rail" id="rail"></aside>
  <div class="main">
    <header class="topbar" id="topbar"></header>
    <div class="content doc">
      <div class="page-head"><div class="h-l"><h1>安装向导</h1>
        <p>逐个安装基础组件：先 dry-run 预览，确认后 apply（异步）</p></div></div>
      <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
      <div class="card"><div class="card-head"><h3>新建实例</h3></div><div class="card-pad">
        <div class="frow"><label>组件</label>
          <select id="inst-kind"></select>
          <label>实例名</label><input id="inst-name" placeholder="如 etcd-dev">
          <label>命名空间</label><input id="inst-ns" value="default">
        </div>
        <div style="margin:12px 0 6px"><b>参数</b> <span class="muted">(key = value)</span>
          <button class="btn btn-ghost btn-sm" id="inst-addparam">+ 加一行</button></div>
        <div id="inst-params"></div>
        <div style="margin-top:14px">
          <button class="btn btn-ghost" id="inst-dryrun">dry-run 预览</button>
          <button class="btn btn-primary" id="inst-apply">确认安装</button>
        </div>
      </div></div>
      <div class="card"><div class="card-head"><h3>结果</h3></div>
        <div class="card-pad"><div id="inst-result" class="muted">填好后点 dry-run 预览</div></div></div>
    </div>
  </div>
</div>
<script src="assets/web.js"></script>
<script>renderInstall();</script>
</body></html>
```

- [ ] **Step 6: Add `renderInstall()` to `webui/assets/web.js`** (append at end):

```javascript
const INSTALL_KINDS = ['etcd', 'minio', 'kafka', 'pulsar', 'milvus'];
const INSTALL_DEFAULTS = {
  etcd: {}, minio: {}, kafka: {}, pulsar: {},
  milvus: { mq: 'kafka', image: 'milvusdb/milvus:v2.6.18',
            storageEndpoint: 'minio.default.svc:80', kafkaBrokers: 'kafka-dev.default.svc:9092' },
};

function paramRow(k, v) {
  const row = document.createElement('div');
  row.className = 'prow';
  row.innerHTML = `<input class="pk" placeholder="key"><span>=</span><input class="pv" placeholder="value"><button class="btn btn-ghost btn-sm pdel">删</button>`;
  row.querySelector('.pk').value = k || '';
  row.querySelector('.pv').value = v || '';
  row.querySelector('.pdel').onclick = () => row.remove();
  return row;
}

function fillParams(kind) {
  const box = document.getElementById('inst-params');
  box.innerHTML = '';
  const d = INSTALL_DEFAULTS[kind] || {};
  const entries = Object.entries(d);
  if (!entries.length) box.appendChild(paramRow('', ''));
  else entries.forEach(([k, v]) => box.appendChild(paramRow(k, v)));
}

function collectParams() {
  const out = {};
  document.querySelectorAll('#inst-params .prow').forEach(r => {
    const k = r.querySelector('.pk').value.trim();
    if (k) out[k] = r.querySelector('.pv').value.trim();
  });
  return out;
}

function renderTaskResult(task) {
  const st = { succeeded: 'PASS', failed: 'FAIL', rolled_back: 'FAIL' }[task.status] || 'WARN';
  return `<div style="margin-bottom:8px">总状态：${badge(st, task.status)}${task.dry_run ? ' <span class="muted">(dry-run)</span>' : ''}</div>` +
    '<table class="tbl"><thead><tr><th>步骤</th><th>状态</th><th>详情/计划</th></tr></thead><tbody>' +
    task.steps.map(s => {
      const lvl = { ok: 'PASS', failed: 'FAIL', skipped: 'SKIP', planned: 'WARN', running: 'WARN' }[s.status] || 'WARN';
      return `<tr><td>${esc(s.name)}</td><td>${badge(lvl, s.status)}</td><td class="muted">${esc(s.detail || s.plan)}</td></tr>`;
    }).join('') + '</tbody></table>';
}

function installBody(dryRun, force) {
  return {
    kind: document.getElementById('inst-kind').value,
    name: document.getElementById('inst-name').value.trim(),
    namespace: document.getElementById('inst-ns').value.trim() || 'default',
    params: collectParams(), dry_run: dryRun, force: !!force,
  };
}

async function pollInstall(taskId, resultEl) {
  const started = Date.now();
  while (true) {
    let j;
    try { j = await getJSON('api/task/' + taskId); }
    catch (e) { resultEl.innerHTML = '<span class="conn bad">轮询失败：' + esc(e.message) + '</span>'; return; }
    if (j.state === 'running') {
      resultEl.innerHTML = `<span class="muted">安装中… ${Math.round((Date.now() - started) / 1000)}s</span>`;
      await new Promise(r => setTimeout(r, 1500));
      continue;
    }
    if (j.state === 'error') { resultEl.innerHTML = '<span class="conn bad">执行出错：' + esc(j.error) + '</span>'; return; }
    resultEl.innerHTML = renderTaskResult(j.task);
    return;
  }
}

async function submitInstall(dryRun, force) {
  const err = document.getElementById('err'); err.style.display = 'none';
  const resultEl = document.getElementById('inst-result');
  const body = installBody(dryRun, force);
  if (!body.name) { err.style.display = 'block'; err.textContent = '请填实例名'; return; }
  resultEl.innerHTML = '<span class="muted">提交中…</span>';
  const { status, data } = await postJSON('api/install', body);
  if (status === 200) { resultEl.innerHTML = renderTaskResult(data.task); return; }
  if (status === 202) { await pollInstall(data.task_id, resultEl); return; }
  if (status === 409) {
    resultEl.innerHTML = `<div class="conn bad">被兼容门禁拦截：${esc(data.reason)}</div>` +
      `<button class="btn btn-primary btn-sm" id="inst-force" style="margin-top:8px">强制安装 --force</button>`;
    document.getElementById('inst-force').onclick = () => {
      if (confirm('确认跳过兼容门禁强制安装？')) submitInstall(dryRun, true);
    };
    return;
  }
  resultEl.innerHTML = '';
  err.style.display = 'block';
  err.textContent = '失败（HTTP ' + status + '）：' + esc((data && data.reason) || '未知错误');
}

function renderInstall() {
  shell('install');
  const sel = document.getElementById('inst-kind');
  sel.innerHTML = INSTALL_KINDS.map(k => `<option value="${k}">${k}</option>`).join('');
  sel.onchange = () => fillParams(sel.value);
  fillParams(sel.value);
  document.getElementById('inst-addparam').onclick = () =>
    document.getElementById('inst-params').appendChild(paramRow('', ''));
  document.getElementById('inst-dryrun').onclick = () => submitInstall(true, false);
  document.getElementById('inst-apply').onclick = () => submitInstall(false, false);
}
```

- [ ] **Step 7: Append form styles to `webui/assets/web.css`** (at end):

```css
/* --- install form --- */
.frow{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.frow label{font-size:13px;color:var(--fg-2)}
.frow select,.frow input,.prow input{padding:7px 10px;border:1px solid var(--line);border-radius:7px;background:var(--surface);color:var(--fg-1);font-size:13px}
.prow{display:flex;align-items:center;gap:8px;margin:6px 0}
.prow .pk{width:220px} .prow .pv{flex:1}
.btn-sm{padding:4px 10px;font-size:12px}
```

- [ ] **Step 8: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_static.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/install.html milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): install form — dry-run preview, async apply+poll, force retry

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`mb web --port 8090` (live minikube + NO_PROXY), open `http://127.0.0.1:8090/install.html`:
- Pick `etcd`, name `etcd-web`, dry-run → step plan shows; 确认安装 → spinner → succeeded; Overview lists `etcd-web`.
- Pick `milvus`, set `mq=woodpecker-service` + `image=milvusdb/milvus:v2.6.3`, dry-run → 409 reason shown + 强制安装 button.

## Self-Review

- **Spec coverage:** D1 form flow → Task 4; D2 async apply+poll → Tasks 1,2,4; D3 key=value + defaults → Task 4 (`INSTALL_DEFAULTS`); D4 gate 409 + force → Tasks 3,4; D5 components → Task 4 (`INSTALL_KINDS`); D6 binding unchanged. §4 endpoints → Tasks 2,3. §5 TaskRunner + pre-check + handlers → Tasks 1,2,3. §6 frontend → Task 4. §7 tests → each task + manual DoD.
- **Placeholder scan:** every code step is complete; frontend verified by content-marker test + manual DoD (no JS test harness — stated). No TBD/TODO.
- **Type consistency:** `TaskRunner.submit/get` + record shape `{state,result,error}` consistent Tasks 1↔2; `/api/task` response `{state,task,error}` consistent Tasks 2↔4; `InstallReq` reused (not redefined); `postJSON` returns `{status,data}` consistent Task 4; `esc/shell/getJSON/badge` from prior slice reused, only `postJSON`/`renderInstall` added.
- **Pre-check note:** Task 2 implements the apply gate pre-check (`install(dry_run=True)` before submit); Task 3 adds the handler that turns its `CompatError` into 409 — so the milvus-409 test lives in Task 3 (after the handler exists), while Task 2 tests the etcd happy path (no gate).
