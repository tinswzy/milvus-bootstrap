# mb WebUI Overview slice — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A read-only WebUI first slice — an Overview page (environment / k8s connection / detected versions / instances) plus a static version-dependency rules page — served by the existing FastAPI app over TCP via a new `mb web` command.

**Architecture:** Extend the existing FastAPI `app` (currently uds-only) with read JSON endpoints (`/api/doctor`, `/api/instances`, `/api/compat-rules`) and a static mount of a new in-package `webui/` dir (vanilla HTML/CSS/JS, aesthetic copied from the prototype `hub.css`). A new `mb web [--host --port]` runs uvicorn on TCP serving the same app. No frontend framework, no build step.

**Tech Stack:** Python 3.11, FastAPI + uvicorn (already deps), typer, pytest + fastapi.testclient. Frontend: vanilla HTML/CSS/JS, `fetch`.

## Global Constraints

- **Read-only slice.** No install/delete/scale/upgrade/switch actions in the UI or new write endpoints. Only GET endpoints are added.
- **`mb web` default binds `127.0.0.1`.** `--host 0.0.0.0` must print a one-line warning that mutating API routes get exposed to the network.
- **Frontend lives in-package:** `milvus-bootstrap/src/milvus_bootstrap/webui/` (`index.html`, `compat.html`, `assets/`). `prototype/` is NOT modified.
- **Static mount is registered LAST** in `app.py` (after all API routes) so `/api/*`, `/status`, `/healthz` win over the catch-all static mount at `/`.
- **Version-dependency page is STATIC rules only** — no live PASS/WARN/FAIL evaluation.
- **Overview gates versions + instances on k8s connection:** show them only when the doctor env finding `component=="cluster"` has `level=="PASS"`; otherwise show a placeholder, never crash.
- **Tests are hermetic (MB_ADAPTER=fake).** The `/api/doctor` test MUST monkeypatch `milvus_bootstrap.core.doctor.run` to a canned `DoctorReport` (otherwise the endpoint shells real `kubectl` against the live cluster — slow + non-hermetic).
- Run tests from `milvus-bootstrap/` with `source .venv/bin/activate`.
- Work on branch `feat/webui-overview` (created off main before Task 1). End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Current shapes (verified):
- `doctor.run() -> DoctorReport`; `.to_json() == {"env":[{level,component,rule,reason}…], "versions":{comp:ver…}, "compat":[…], "tool":{version,commit,update}, "exit_code":int}`.
- `compat.MQ_OPTIONS: list[MqOption]`; `MqOption` fields: `id, wal, label, min_milvus, dep_kind, standalone_only, note`.
- `compat.load_constraints() -> list[Constraint]`; `Constraint` fields: `component, requires, rule, milvus_range, min, max, severity, source, reason, kind`.
- `compat.load_upgrade_paths() -> list[dict]` (each `{target_min, requires_current_min, reason}`).
- `Core.state.list_instances() -> list[Instance]`; `Instance` fields: `kind, name, namespace, ownership (enum), deps, spec_snapshot`.
- `server/app.py`: `app = FastAPI(...)`, `_core() -> Core`, existing `@app.get("/status")` returns `_core().status()` (names only). Import site for new code: `from ..core.context import Core` already present.

---

### Task 1: `core/webapi.py::compat_rules()` — assemble static rules JSON

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/core/webapi.py`
- Test: `milvus-bootstrap/tests/test_webapi.py`

**Interfaces:**
- Produces: `compat_rules() -> dict` with keys `mq_rules`, `constraints`, `upgrade_paths`:
  - `mq_rules`: `[{id, label, wal, min_milvus, dep_kind, standalone_only, note}]` from `MQ_OPTIONS`.
  - `constraints`: `[{component, requires, rule, milvus_range, min, max, severity, source, kind, reason}]` from `load_constraints()`.
  - `upgrade_paths`: `load_upgrade_paths()` verbatim.

- [ ] **Step 1: Write the failing test** — create `tests/test_webapi.py`:

```python
from milvus_bootstrap.core import webapi


def test_compat_rules_shape():
    r = webapi.compat_rules()
    assert set(r) == {"mq_rules", "constraints", "upgrade_paths"}
    # mq_rules from MQ_OPTIONS (woodpecker-embedded/service, kafka, pulsar, rocksmq)
    ids = {m["id"] for m in r["mq_rules"]}
    assert {"woodpecker-embedded", "woodpecker-service", "kafka", "pulsar", "rocksmq"} <= ids
    wp_svc = next(m for m in r["mq_rules"] if m["id"] == "woodpecker-service")
    assert wp_svc["min_milvus"] == "3.0.0" and wp_svc["wal"] == "woodpecker"
    # constraints include the docs-seeded soft floors
    comps = {c["component"] for c in r["constraints"]}
    assert {"etcd", "pulsar", "k8s", "helm", "minio"} <= comps
    etcd = next(c for c in r["constraints"] if c["component"] == "etcd")
    assert etcd["min"] == "3.5.0" and etcd["severity"] == "soft"
    # upgrade paths present
    assert any(u["requires_current_min"] == "2.5.16" for u in r["upgrade_paths"])
```

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_webapi.py -v`
Expected: FAIL — `ModuleNotFoundError: ... webapi`.

- [ ] **Step 3: Implement `core/webapi.py`**

```python
"""Assemble compatibility rules into frontend-friendly JSON (read-only, pure)."""
from __future__ import annotations

from dataclasses import asdict

from . import compat


def compat_rules() -> dict:
    mq_rules = [
        {"id": o.id, "label": o.label, "wal": o.wal, "min_milvus": o.min_milvus,
         "dep_kind": o.dep_kind, "standalone_only": o.standalone_only, "note": o.note}
        for o in compat.MQ_OPTIONS
    ]
    constraints = [asdict(c) for c in compat.load_constraints()]
    upgrade_paths = list(compat.load_upgrade_paths())
    return {"mq_rules": mq_rules, "constraints": constraints, "upgrade_paths": upgrade_paths}
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_webapi.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/webapi.py milvus-bootstrap/tests/test_webapi.py
git commit -m "feat(webapi): assemble compat rules into frontend JSON

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Read API endpoints — `/api/doctor`, `/api/instances`, `/api/compat-rules`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_web_endpoints.py`

**Interfaces:**
- Consumes: `doctor.run()` (Task-independent, existing), `webapi.compat_rules()` (Task 1), `_core().state.list_instances()`.
- Produces routes:
  - `GET /api/doctor` → `doctor.run().to_json()`.
  - `GET /api/instances` → `{"instances": [{name, kind, namespace, ownership}]}` from `state.list_instances()` (ownership serialized to its `.value`).
  - `GET /api/compat-rules` → `webapi.compat_rules()`.

- [ ] **Step 1: Write the failing tests** — create `tests/test_web_endpoints.py`:

```python
import pytest
from fastapi.testclient import TestClient

from milvus_bootstrap.core import doctor
from milvus_bootstrap.core.compat import Finding


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    # Hermetic doctor: never shell real kubectl.
    fake = doctor.DoctorReport(
        env=[Finding("PASS", "cluster", "集群可达", "ok"),
             Finding("PASS", "kubectl", "kubectl 可用", "ok")],
        versions={"k8s": "1.34.0", "milvus-operator": "1.3.6"},
        compat=[], tool={"version": "0.0.1", "commit": "abc", "update": "checked"},
    )
    monkeypatch.setattr(doctor, "run", lambda **k: fake)
    from milvus_bootstrap.server.app import app
    with TestClient(app) as c:
        yield c


def test_api_doctor(client):
    r = client.get("/api/doctor")
    assert r.status_code == 200
    j = r.json()
    assert set(j) >= {"env", "versions", "tool"}
    assert j["versions"]["k8s"] == "1.34.0"
    assert any(f["component"] == "cluster" for f in j["env"])


def test_api_compat_rules(client):
    r = client.get("/api/compat-rules")
    assert r.status_code == 200
    j = r.json()
    assert {"mq_rules", "constraints", "upgrade_paths"} <= set(j)
    assert j["mq_rules"] and j["constraints"]


def test_api_instances_empty(client):
    r = client.get("/api/instances")
    assert r.status_code == 200
    assert r.json() == {"instances": []}   # fresh fake state
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_endpoints.py -v`
Expected: FAIL — 404 on the new routes.

- [ ] **Step 3: Add the endpoints to `server/app.py`** — add imports near the top (after existing imports):

```python
from ..core import doctor
from ..core import webapi
```
Add these routes (place them AFTER the existing `@app.get("/status")` and BEFORE any static mount — the static mount is added in Task 3 at the very end of the file):

```python
@app.get("/api/doctor")
def api_doctor() -> dict[str, Any]:
    return doctor.run().to_json()


@app.get("/api/instances")
def api_instances() -> dict[str, Any]:
    out = []
    for i in _core().state.list_instances():
        out.append({"name": i.name, "kind": i.kind, "namespace": i.namespace,
                    "ownership": i.ownership.value})
    return {"instances": out}


@app.get("/api/compat-rules")
def api_compat_rules() -> dict[str, Any]:
    return webapi.compat_rules()
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_web_endpoints.py -v`
Expected: PASS.

- [ ] **Step 5: Full suite regression**

Run: `python -m pytest -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/tests/test_web_endpoints.py
git commit -m "feat(server): read endpoints /api/doctor /api/instances /api/compat-rules

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Static mount + `webui/` shell + Overview page (end-to-end)

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py` (static mount at end)
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/index.html`
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css` (copied from prototype hub.css)
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `/api/doctor`, `/api/instances` (Task 2).
- Produces: `GET /` serves `index.html`; `GET /assets/web.js` and `/assets/web.css` served. `web.js` exposes `renderOverview()` (called by index.html) and a shared `shell(active)` for rail/topbar; later reused by Task 4.

- [ ] **Step 1: Write the failing test** — create `tests/test_web_static.py`:

```python
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.server.app import app
    with TestClient(app) as c:
        yield c


def test_root_serves_overview_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    body = r.text
    assert 'id="rail"' in body and 'id="env-list"' in body and 'id="instances"' in body


def test_assets_served(client):
    assert client.get("/assets/web.css").status_code == 200
    js = client.get("/assets/web.js")
    assert js.status_code == 200 and "renderOverview" in js.text


def test_api_routes_win_over_static(client):
    # /healthz is a real route, must not be shadowed by the static mount at "/"
    assert client.get("/healthz").json() == {"ok": True}
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -v`
Expected: FAIL — `/` 404 (no static mount / no files).

- [ ] **Step 3: Copy the prototype CSS as the web stylesheet**

```bash
mkdir -p milvus-bootstrap/src/milvus_bootstrap/webui/assets
cp prototype/assets/hub.css milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css
```

- [ ] **Step 4: Create `webui/index.html`**

```html
<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Overview · Milvus Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/web.css">
</head><body>
<div class="app">
  <aside class="rail" id="rail"></aside>
  <div class="main">
    <header class="topbar" id="topbar"></header>
    <div class="content doc">
      <div class="page-head"><div class="h-l"><h1>Overview</h1>
        <p>运行环境 · k8s 连接 · 探测到的版本 · 集群内实例</p></div>
        <div class="h-r"><button class="btn btn-primary" id="refresh">刷新</button></div></div>
      <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
      <div class="card"><div class="card-head"><h3>k8s 连接</h3></div>
        <div class="card-pad"><div id="conn">加载中…</div></div></div>
      <div class="card"><div class="card-head"><h3>运行环境</h3></div>
        <div class="card-pad"><div id="env-list">加载中…</div></div></div>
      <div class="card" id="versions-card"><div class="card-head"><h3>探测到的版本</h3></div>
        <div class="card-pad"><div id="versions">连接集群后展示</div></div></div>
      <div class="card" id="instances-card"><div class="card-head"><h3>集群内实例</h3></div>
        <div class="card-pad"><div id="instances">连接集群后展示</div></div></div>
    </div>
  </div>
</div>
<script src="assets/web.js"></script>
<script>renderOverview();document.getElementById('refresh').onclick=renderOverview;</script>
</body></html>
```

- [ ] **Step 5: Create `webui/assets/web.js`**

```javascript
// Minimal vanilla renderer for the Milvus Admin WebUI (read-only overview).
const NAV = [
  { id: 'overview', label: 'Overview', href: 'index.html' },
  { id: 'compat',   label: '版本依赖', href: 'compat.html' },
  { id: 'install',  label: '安装向导（待做）', disabled: true },
];
const LVL = { PASS: 'ok', WARN: 'warn', FAIL: 'err', SKIP: 'idle' };

function esc(s) { return String(s == null ? '' : s).replace(/[&<>]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;' }[c])); }

function shell(active) {
  const rail = document.getElementById('rail');
  if (rail) rail.innerHTML = '<div class="brand">Milvus Admin</div><nav class="nav">' +
    NAV.map(n => n.disabled
      ? `<span class="navitem disabled">${esc(n.label)}</span>`
      : `<a class="navitem${n.id === active ? ' active' : ''}" href="${n.href}">${esc(n.label)}</a>`
    ).join('') + '</nav>';
  const top = document.getElementById('topbar');
  if (top) top.innerHTML = `<div class="crumbs">Milvus Admin <span class="sep">/</span> <b>${esc(active === 'compat' ? '版本依赖' : 'Overview')}</b></div>`;
}

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(url + ' -> HTTP ' + r.status);
  return r.json();
}

function badge(level, text) {
  return `<span class="badge b-${LVL[level] || 'idle'}"><span class="d"></span>${esc(text || level)}</span>`;
}

async function renderOverview() {
  shell('overview');
  const err = document.getElementById('err');
  err.style.display = 'none';
  try {
    const doc = await getJSON('api/doctor');
    // environment rows
    document.getElementById('env-list').innerHTML =
      '<table class="tbl"><tbody>' + doc.env.map(f =>
        `<tr><td>${esc(f.rule || f.component)}</td><td>${badge(f.level)}</td><td class="muted">${esc(f.reason)}</td></tr>`
      ).join('') + '</tbody></table>';
    // k8s connection
    const cluster = doc.env.find(f => f.component === 'cluster');
    const connected = cluster && cluster.level === 'PASS';
    document.getElementById('conn').innerHTML = connected
      ? `<div class="conn ok">✅ 已连接　<span class="muted">${esc(cluster.reason)}</span></div>`
      : `<div class="conn bad">❌ 未连接　<span class="muted">${esc(cluster ? cluster.reason : '未探测')}</span></div>`;
    // versions (only if connected)
    document.getElementById('versions').innerHTML = connected
      ? '<table class="tbl"><tbody>' + Object.entries(doc.versions).map(([k, v]) =>
          `<tr><td>${esc(k)}</td><td class="mono">${esc(v)}</td></tr>`).join('') +
        (Object.keys(doc.versions).length ? '' : '<tr><td class="muted" colspan="2">未探测到组件版本</td></tr>') +
        '</tbody></table>'
      : '<div class="muted">连接集群后展示</div>';
    // instances (only if connected)
    if (connected) {
      const inst = (await getJSON('api/instances')).instances;
      document.getElementById('instances').innerHTML = inst.length
        ? '<table class="tbl"><thead><tr><th>名称</th><th>类型</th><th>命名空间</th><th>Ownership</th></tr></thead><tbody>' +
          inst.map(i => `<tr><td>${esc(i.name)}</td><td>${esc(i.kind)}</td><td>${esc(i.namespace)}</td><td>${esc(i.ownership)}</td></tr>`).join('') +
          '</tbody></table>'
        : '<div class="muted">该集群下暂无本工具登记的实例</div>';
    } else {
      document.getElementById('instances').innerHTML = '<div class="muted">连接集群后展示</div>';
    }
  } catch (e) {
    err.style.display = 'block';
    err.textContent = '加载失败：' + e.message;
  }
}
```

- [ ] **Step 6: Append a few helper styles to `webui/assets/web.css`** (so the new class names render; append at the end of the copied file):

```css
/* --- webui additions --- */
.rail .brand{font-weight:800;font-size:15px;padding:16px 18px;color:var(--fg-1)}
.nav .navitem{display:block;padding:9px 18px;color:var(--fg-2);text-decoration:none;font-size:14px}
.nav .navitem.active{color:var(--fg-1);background:var(--surface-2);font-weight:600}
.nav .navitem.disabled{color:var(--fg-3);opacity:.5}
.muted{color:var(--fg-3)} .mono{font-family:'IBM Plex Mono',monospace}
.conn.ok{color:#2f9e6f;font-weight:600} .conn.bad{color:#b42318;font-weight:600}
.tbl td,.tbl th{padding:8px 12px}
```

- [ ] **Step 7: Add the static mount at the VERY END of `server/app.py`**

```python
# --- WebUI static frontend (registered LAST so /api/* and /status win) ---
import pathlib
from fastapi.staticfiles import StaticFiles

_WEBUI_DIR = pathlib.Path(__file__).resolve().parent.parent / "webui"
app.mount("/", StaticFiles(directory=str(_WEBUI_DIR), html=True), name="webui")
```

- [ ] **Step 8: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_static.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/src/milvus_bootstrap/webui milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): static mount + Overview page (env/conn/versions/instances)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: `compat.html` — static version-dependency rules page

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/compat.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (add `renderCompat()`)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `/api/compat-rules` (Task 2), `shell()` + `esc()` + `getJSON()` (Task 3).
- Produces: `GET /compat.html` serves the page; `renderCompat()` renders three tables (mq_rules / constraints / upgrade_paths).

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:

```python
def test_compat_page_served(client):
    r = client.get("/compat.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="mq-rules"' in r.text and 'id="constraints"' in r.text and 'id="upgrade-paths"' in r.text
    assert "renderCompat" in client.get("/assets/web.js").text
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k compat_page -v`
Expected: FAIL — `/compat.html` 404, no `renderCompat`.

- [ ] **Step 3: Create `webui/compat.html`**

```html
<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>版本依赖 · Milvus Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/web.css">
</head><body>
<div class="app">
  <aside class="rail" id="rail"></aside>
  <div class="main">
    <header class="topbar" id="topbar"></header>
    <div class="content doc">
      <div class="page-head"><div class="h-l"><h1>版本依赖关系限制</h1>
        <p>静态规则参考（来自 compat 矩阵 + MQ 规则 + 升级路径）</p></div></div>
      <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
      <div class="card"><div class="card-head"><h3>MQ ↔ milvus</h3></div>
        <div class="card-pad"><div id="mq-rules">加载中…</div></div></div>
      <div class="card"><div class="card-head"><h3>组件版本约束</h3></div>
        <div class="card-pad"><div id="constraints">加载中…</div></div></div>
      <div class="card"><div class="card-head"><h3>升级路径</h3></div>
        <div class="card-pad"><div id="upgrade-paths">加载中…</div></div></div>
    </div>
  </div>
</div>
<script src="assets/web.js"></script>
<script>renderCompat();</script>
</body></html>
```

- [ ] **Step 4: Add `renderCompat()` to `webui/assets/web.js`** (append at end):

```javascript
async function renderCompat() {
  shell('compat');
  const err = document.getElementById('err');
  err.style.display = 'none';
  try {
    const r = await getJSON('api/compat-rules');
    document.getElementById('mq-rules').innerHTML =
      '<table class="tbl"><thead><tr><th>MQ</th><th>WAL</th><th>最低 milvus</th><th>依赖</th><th>说明</th></tr></thead><tbody>' +
      r.mq_rules.map(m => `<tr><td>${esc(m.label)}</td><td>${esc(m.wal)}</td><td class="mono">${esc(m.min_milvus)}</td>` +
        `<td>${esc(m.dep_kind || '嵌入')}${m.standalone_only ? ' · 仅standalone' : ''}</td><td class="muted">${esc(m.note)}</td></tr>`).join('') +
      '</tbody></table>';
    document.getElementById('constraints').innerHTML =
      '<table class="tbl"><thead><tr><th>组件</th><th>规则</th><th>下限</th><th>milvus 区间</th><th>强度</th><th>来源</th></tr></thead><tbody>' +
      r.constraints.map(c => `<tr><td>${esc(c.component)}</td><td>${esc(c.rule)}</td><td class="mono">${esc(c.min || '—')}</td>` +
        `<td class="mono">${esc(c.milvus_range || '任意')}</td><td>${badge(c.severity === 'hard' ? 'FAIL' : 'WARN', c.severity)}</td><td class="muted">${esc(c.source)}</td></tr>`).join('') +
      '</tbody></table>';
    document.getElementById('upgrade-paths').innerHTML =
      '<table class="tbl"><thead><tr><th>目标 ≥</th><th>需当前 ≥</th><th>说明</th></tr></thead><tbody>' +
      r.upgrade_paths.map(u => `<tr><td class="mono">${esc(u.target_min)}</td><td class="mono">${esc(u.requires_current_min)}</td><td class="muted">${esc(u.reason)}</td></tr>`).join('') +
      '</tbody></table>';
  } catch (e) {
    err.style.display = 'block';
    err.textContent = '加载失败：' + e.message;
  }
}
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_static.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/compat.html milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): static version-dependency rules page

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: `mb web` command + `run_web()`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/__main__.py` (add `run_web`)
- Modify: `milvus-bootstrap/src/milvus_bootstrap/cli/main.py` (add `web` command)
- Test: `milvus-bootstrap/tests/test_cli_web.py`

**Interfaces:**
- Produces: `run_web(host: str, port: int) -> None` in `server/__main__.py` (prints an exposure warning when `host == "0.0.0.0"`, then `uvicorn.run("milvus_bootstrap.server.app:app", host=host, port=port, log_level="warning")`). CLI `mb web --host 127.0.0.1 --port 8080` calls it.

- [ ] **Step 1: Write the failing test** — create `tests/test_cli_web.py`:

```python
from typer.testing import CliRunner

from milvus_bootstrap.cli.main import app

runner = CliRunner()


def test_web_command_calls_run_web(monkeypatch):
    calls = {}
    import milvus_bootstrap.server.__main__ as srv
    monkeypatch.setattr(srv, "run_web", lambda host, port: calls.update(host=host, port=port))
    res = runner.invoke(app, ["web", "--host", "127.0.0.1", "--port", "9001"])
    assert res.exit_code == 0
    assert calls == {"host": "127.0.0.1", "port": 9001}


def test_run_web_warns_on_all_interfaces(monkeypatch, capsys):
    import milvus_bootstrap.server.__main__ as srv
    served = {}
    monkeypatch.setattr(srv.uvicorn, "run", lambda *a, **k: served.update(k))
    srv.run_web("0.0.0.0", 8080)
    out = capsys.readouterr().out
    assert "0.0.0.0" in out and ("警告" in out or "WARN" in out.upper())
    assert served.get("host") == "0.0.0.0" and served.get("port") == 8080
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_cli_web.py -v`
Expected: FAIL — no `run_web`, no `web` command.

- [ ] **Step 3: Add `run_web` to `server/__main__.py`** (the module already imports `uvicorn`; add):

```python
def run_web(host: str, port: int) -> None:
    if host == "0.0.0.0":
        print(f"[警告] 绑定 {host}:{port} 会把包含 install/delete 等可变更操作的 API 暴露到网络。")
    print(f"WebUI: http://{host}:{port}/")
    uvicorn.run("milvus_bootstrap.server.app:app", host=host, port=port, log_level="warning")
```

- [ ] **Step 4: Add the `web` command to `cli/main.py`** — add after the `status()` command:

```python
@app.command()
def web(
    host: str = typer.Option("127.0.0.1", "--host", help="绑定地址；0.0.0.0 = 局域网可访（会暴露可变更 API）"),
    port: int = typer.Option(8080, "--port", help="端口"),
) -> None:
    """启动 WebUI（TCP 服务，浏览器打开 http://<host>:<port>/）。"""
    from ..server.__main__ import run_web
    run_web(host, port)
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `python -m pytest tests/test_cli_web.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/__main__.py milvus-bootstrap/src/milvus_bootstrap/cli/main.py milvus-bootstrap/tests/test_cli_web.py
git commit -m "feat(cli): mb web — serve the WebUI over TCP

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`cd milvus-bootstrap && source .venv/bin/activate && mb web --port 8080` (with the live minikube reachable + NO_PROXY set), then open `http://127.0.0.1:8080/`:
- Overview shows 环境 rows, a green 已连接 card, the detected versions table, and the instances table (etcd/kafka-dev/minio/pulsar-dev/milvus-dev/milvus-pulsar).
- `/compat.html` shows the three static rule tables.
- Stop the live cluster reachability (or run without NO_PROXY) → Overview degrades: red 未连接, versions/instances show placeholders, no crash.
- `mb web --host 0.0.0.0 --port 8080` prints the exposure warning.

## Self-Review

- **Spec coverage:** D1 TCP+static → Tasks 2,3,5; D2 static rules page → Tasks 1,4; D3 `mb web` localhost default + warning → Task 5; D4 vanilla + hub.css → Task 3; D5 refresh button, no poll → Task 3 (index.html `#refresh`); D6 in-package webui/ → Tasks 3,4. §4 endpoints → Tasks 1,2. §5 pages → Tasks 3,4. §6 degradation → Task 3 (`connected` gating + `#err`). §7 tests → every task + manual DoD.
- **Deviation from spec (noted):** spec §4 said "reuse GET /status for instances", but `/status` returns names only; the plan adds a richer `GET /api/instances` (name/kind/namespace/ownership) instead. Live "Healthy" status per instance is deferred (needs per-instance cluster queries) — the instances table shows mb-known fields only. This is an intentional, called-out refinement, not a silent drop.
- **Placeholder scan:** frontend rendering is verified by (a) static-serving + content-marker tests and (b) the manual DoD — there is no JS unit-test harness (no framework, YAGNI); this is stated, not hidden. No TBD/TODO; all code steps are complete.
- **Type consistency:** `Finding` dict keys `{level,component,rule,reason}` used identically in web.js and the doctor test; `compat_rules()` keys `{mq_rules,constraints,upgrade_paths}` consistent across Tasks 1/2/4; `/api/instances` fields `{name,kind,namespace,ownership}` consistent between Task 2 endpoint and Task 3 web.js; `shell()`/`esc()`/`getJSON()`/`badge()` defined in Task 3, reused in Task 4.
