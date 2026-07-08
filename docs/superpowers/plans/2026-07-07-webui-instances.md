# WebUI instances-pages + delete — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Split instance display off the Overview into two prototype-styled pages — a Dependencies page (etcd/minio/kafka/pulsar, grouped by kind) and a Milvus page (per-milvus card with live CR health + dep bindings) — and add a delete action (async, reusing the install slice's TaskRunner/poll).

**Architecture:** Backend extends `GET /api/instances` (adds image + milvus status/deps) via a new `probe.milvus_status`, and adds async `POST /api/delete` (submit `Core.delete` to the existing TaskRunner, poll via `GET /api/task/{id}`). Frontend removes the Overview instances section, adds `deps.html`/`milvus.html` + `renderDeps()`/`renderMilvus()` + a shared `deleteInstance()`, and two nav items.

**Tech Stack:** Python 3.11, FastAPI, pydantic, pytest+TestClient. Frontend: vanilla HTML/CSS/JS.

## Global Constraints

- **Manage action = delete only** this slice (async: `POST /api/delete` → 202 {task_id} → poll `GET /api/task/{id}`). No upgrade/scale/switch-mq UI.
- **Delete takes NO force** (`Core.delete(instance_id, dry_run)` has no force param; delete has no compat gate). `POST /api/delete` body is `{instance}`.
- **Unknown instance:** `Core.delete` raises `KeyError` → NOT caught by the ValueError handler. `POST /api/delete` MUST pre-check `state.get_instance(instance) is None` and raise `ValueError` (→400 via the existing handler) instead of submitting.
- **milvus health:** `probe.milvus_status(name)` queries CR `.status.status`, best-effort (fake adapter / unreachable → None). Called from `/api/instances` only for milvus kind AND when `adapter.name == "k8s"`, wrapped in try/except → None.
- **deps versions** come from `/api/doctor` `versions[kind]` (frontend cross-reference) — do NOT query each dep instance's workload.
- **Components:** deps = etcd/minio/kafka/pulsar; milvus separate.
- **XSS:** all server strings via `esc()`. Frontend has no JS unit tests (content-marker tests + manual DoD).
- Tests hermetic (MB_ADAPTER=fake). Run from `milvus-bootstrap/` with `source .venv/bin/activate`.
- Branch `feat/webui-instances` off main. End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Verified shapes:
- `Instance`: `id,name,platform,namespace,ownership,deps,spec_snapshot`. `spec_snapshot` is a dict with `kind`, `name`, `params` (params holds milvus mq/image/storageEndpoint/kafkaBrokers/etc).
- Current `GET /api/instances` returns `[{name,kind,namespace,ownership}]` (kind from `spec_snapshot.get("kind","")`). Existing tests `test_api_instances_empty` / `test_api_instances_with_registered_instance` check individual keys (not whole-dict `==`), so ADDING keys won't break them.
- `Core.delete(instance_id, dry_run=True) -> Task`; `Core.adapter.name` is `"fake"`/`"k8s"`.
- `server/app.py` already has: `runner = TaskRunner()`, `_core()`, `InstallReq`, `DeleteReq{instance,dry_run}`, `JSONResponse`, `HTTPException`, exception handlers (CompatError→409/ValueError→400), `from ..core import doctor` (probe NOT yet imported), `GET /api/instances`, `POST /api/install`, `GET /api/task/{id}`, and a `StaticFiles` mount registered LAST.
- `webui/assets/web.js` has `NAV` (overview/compat/install), `NAV_ICON`, `svgIco`, `shell()` (breadcrumb map `{compat,install}`), `esc/getJSON/postJSON/badge`, `renderOverview` (with an instances fetch/render block), `renderCompat`, `renderInstall`.
- `webui/index.html` has an instances card `id="instances-card"` / `id="instances"`. `test_web_static.py::test_root_serves_overview_html` asserts `'id="instances"' in body` — must be updated when the card is removed.

---

### Task 1: `probe.milvus_status` + extend `GET /api/instances`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/probe.py`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_probe.py` (append), `milvus-bootstrap/tests/test_web_endpoints.py` (append)

**Interfaces:**
- Produces: `probe.milvus_status(name: str, run=run_kubectl) -> str | None`; `GET /api/instances` rows gain `image` (milvus: `params.image`; else `""`), `status` (milvus+k8s: CR status else None), `deps` (milvus: `{etcd,storage,mq,mq_endpoint}` else None).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_probe.py`:
```python
def test_milvus_status_ok_and_missing():
    ok = probe.milvus_status("m1", run=_fake_run({"get milvus m1": (0, "Healthy", "")}))
    assert ok == "Healthy"
    assert probe.milvus_status("m1", run=lambda a: (1, "", "err")) is None
    assert probe.milvus_status("m1", run=lambda a: (0, "", "")) is None   # empty status
```

Append to `tests/test_web_endpoints.py`:
```python
def test_api_instances_enriched_fields(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="etcd", name="etcd-dev"), dry_run=False)
    app_module.core.install(InstallSpec(kind="milvus", name="milvus-dev", params={
        "mq": "kafka", "image": "milvusdb/milvus:v2.6.18",
        "storageEndpoint": "minio.default.svc:80",
        "kafkaBrokers": "kafka-dev.default.svc:9092"}), dry_run=False)
    rows = {r["name"]: r for r in client.get("/api/instances").json()["instances"]}
    assert rows["etcd-dev"]["deps"] is None and rows["etcd-dev"]["status"] is None
    m = rows["milvus-dev"]
    assert m["image"] == "milvusdb/milvus:v2.6.18"
    assert m["status"] is None                                  # fake adapter → not queried
    assert m["deps"]["mq"] == "kafka" and m["deps"]["storage"] == "minio.default.svc:80"
    assert m["deps"]["mq_endpoint"] == "kafka-dev.default.svc:9092"
```
(The `client` fixture in `test_web_endpoints.py` uses MB_ADAPTER=fake.)

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_probe.py::test_milvus_status_ok_and_missing tests/test_web_endpoints.py::test_api_instances_enriched_fields -v`
Expected: FAIL — no `milvus_status`; rows lack `deps`/`status`/`image`.

- [ ] **Step 3: Add `milvus_status` to `core/probe.py`** (near `detect_versions`):
```python
def milvus_status(name: str, run=run_kubectl) -> str | None:
    rc, out, _ = run(["get", "milvus", name, "-o", "jsonpath={.status.status}"])
    if rc != 0:
        return None
    return out.strip() or None
```

- [ ] **Step 4: Extend `GET /api/instances` in `server/app.py`** — add `from ..core import probe` with the other imports, and replace the `api_instances` body:
```python
@app.get("/api/instances")
def api_instances() -> dict[str, Any]:
    is_k8s = getattr(_core().adapter, "name", "") == "k8s"
    out = []
    for i in _core().state.list_instances():
        snap = i.spec_snapshot or {}
        kind = snap.get("kind", "")
        params = snap.get("params", {}) or {}
        row = {"name": i.name, "kind": kind, "namespace": i.namespace,
               "ownership": i.ownership.value, "image": "", "status": None, "deps": None}
        if kind == "milvus":
            row["image"] = params.get("image", "")
            row["deps"] = {
                "etcd": params.get("etcdEndpoints", ""),
                "storage": params.get("storageEndpoint", ""),
                "mq": params.get("mq", ""),
                "mq_endpoint": params.get("kafkaBrokers") or params.get("pulsarEndpoint") or "",
            }
            if is_k8s:
                try:
                    row["status"] = probe.milvus_status(i.name)
                except Exception:
                    row["status"] = None
        out.append(row)
    return {"instances": out}
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `python -m pytest tests/test_probe.py tests/test_web_endpoints.py -v` then `python -m pytest -q`
Expected: PASS (incl. the pre-existing instances tests — new keys don't break their per-key asserts).

- [ ] **Step 6: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/probe.py milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/tests/test_probe.py milvus-bootstrap/tests/test_web_endpoints.py
git commit -m "feat(server): enrich /api/instances (image + milvus status/deps) via probe.milvus_status

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Async `POST /api/delete`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_web_install.py` (append)

**Interfaces:**
- Consumes: `runner` (existing), `DeleteReq{instance,dry_run}`, `Core.delete`.
- Produces: `POST /api/delete {instance}` → pre-check existence (missing → `ValueError`→400), else `runner.submit(Core.delete apply)` → 202 `{task_id,state:"running"}`.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_web_install.py`:
```python
def test_delete_async_then_gone(client):
    import time
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="etcd", name="etcd-del"), dry_run=False)
    r = client.post("/api/delete", json={"instance": "etcd-del"})
    assert r.status_code == 202
    tid = r.json()["task_id"]
    end = time.monotonic() + 5
    state = "running"
    while time.monotonic() < end:
        state = client.get(f"/api/task/{tid}").json()["state"]
        if state != "running":
            break
        time.sleep(0.05)
    assert state == "succeeded"
    names = [i["name"] for i in client.get("/api/instances").json()["instances"]]
    assert "etcd-del" not in names


def test_delete_unknown_instance_400(client):
    r = client.post("/api/delete", json={"instance": "nope"})
    assert r.status_code == 400 and r.json()["error"] == "bad_request"
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_install.py -k "delete" -v`
Expected: FAIL — 404 on `/api/delete`.

- [ ] **Step 3: Add the route to `server/app.py`** (after `POST /api/install`, before the static mount):
```python
@app.post("/api/delete")
def api_delete(req: DeleteReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    tid = runner.submit(lambda: _core().delete(req.instance, dry_run=False))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_install.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/tests/test_web_install.py
git commit -m "feat(server): async POST /api/delete (submit Core.delete, 400 on unknown)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Overview cleanup + nav additions

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/index.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Test: `milvus-bootstrap/tests/test_web_static.py` (modify existing + append)

**Interfaces:**
- Produces: overview no longer shows instances; NAV gains `milvus`/`deps` items (with icons) + shell breadcrumb map entries. `renderDeps`/`renderMilvus` are added in later tasks — the nav links point at `deps.html`/`milvus.html` (served once those files exist in Tasks 4-5).

- [ ] **Step 1: Update the failing test** — in `tests/test_web_static.py`, change `test_root_serves_overview_html` to drop the instances assertion and add a nav check; and append a nav test:
```python
# in test_root_serves_overview_html: replace the body asserts line with:
    assert 'id="rail"' in body and 'id="env-list"' in body
    assert 'id="instances-card"' not in body        # instances moved off overview


def test_nav_has_instance_pages(client):
    js = client.get("/assets/web.js").text
    assert "milvus.html" in js and "deps.html" in js
    assert "renderOverview" in js                    # overview still exists
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k "overview_html or nav_has_instance" -v`
Expected: `test_nav_has_instance_pages` FAILs (no milvus.html/deps.html in nav); `test_root_serves_overview_html` still passes with the updated assertion once the card is removed (fails now because card present).

- [ ] **Step 3: Remove the instances card from `webui/index.html`** — delete this block:
```html
      <div class="card" id="instances-card"><div class="card-head"><h3>集群内实例</h3></div>
        <div class="card-pad"><div id="instances">连接集群后展示</div></div></div>
```

- [ ] **Step 4: Update `webui/assets/web.js`** — set the NAV, add icons, extend the breadcrumb map, and remove the overview instances block.

Replace the `NAV` array:
```javascript
const NAV = [
  { id: 'overview', label: 'Overview', href: 'index.html' },
  { id: 'milvus',   label: 'Milvus 实例', href: 'milvus.html' },
  { id: 'deps',     label: 'Dependencies', href: 'deps.html' },
  { id: 'compat',   label: '版本依赖', href: 'compat.html' },
  { id: 'install',  label: '安装向导', href: 'install.html' },
];
```
Add two entries to `NAV_ICON`:
```javascript
  milvus: '<path d="M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  deps: '<path d="M6 3h12v6H6zM6 15h12v6H6zM12 9v6"/>',
```
Extend the `shell()` breadcrumb map to include the new pages:
```javascript
  if (top) top.innerHTML = `<div class="crumbs">Milvus Admin <span class="sep">/</span> <b>${esc({ compat: '版本依赖', install: '安装向导', milvus: 'Milvus 实例', deps: 'Dependencies' }[active] || 'Overview')}</b></div>`;
```
In `renderOverview`, remove the instances block — delete the `if (connected) { ... instances fetch/render ... } else { ... }` section that targets `document.getElementById('instances')` (the versions block stays). The final `renderOverview` no longer references `api/instances`.

- [ ] **Step 5: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_static.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 6: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/index.html milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): move instances off Overview; add Milvus/Dependencies nav

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Dependencies page + shared `deleteInstance`

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/deps.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (add `deleteInstance` + `renderDeps` + DEP consts)
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css` (chips)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `/api/instances`, `/api/doctor`, `/api/delete`, `/api/task/{id}`; `shell/esc/getJSON/postJSON/badge`.
- Produces: `GET /deps.html`; `renderDeps()`; `deleteInstance(name, onDone)` (reused by Task 5).

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_deps_page_served(client):
    r = client.get("/deps.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="deps-list"' in r.text
    js = client.get("/assets/web.js").text
    assert "renderDeps" in js and "deleteInstance" in js
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k deps_page -v`
Expected: FAIL — `/deps.html` 404.

- [ ] **Step 3: Create `webui/deps.html`**
```html
<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dependencies · Milvus Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/web.css">
</head><body>
<div class="app">
  <aside class="rail" id="rail"></aside>
  <div class="main">
    <header class="topbar" id="topbar"></header>
    <div class="content doc">
      <div class="page-head"><div class="h-l"><h1>Dependencies</h1>
        <p>Milvus 依赖的组件实例，按类型分组。Milvus 实例本身在「Milvus 实例」页管理。</p></div>
        <div class="h-r"><button class="btn btn-primary" id="refresh">刷新</button></div></div>
      <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
      <div id="deps-list">加载中…</div>
    </div>
  </div>
</div>
<script src="assets/web.js"></script>
<script>renderDeps();document.getElementById('refresh').onclick=renderDeps;</script>
</body></html>
```

- [ ] **Step 4: Add `deleteInstance` + `renderDeps` to `webui/assets/web.js`** (append at end):
```javascript
async function deleteInstance(name, onDone) {
  if (!confirm(`确认删除实例 ${name}？（依赖 / PVC 默认保留）`)) return;
  const err = document.getElementById('err');
  if (err) err.style.display = 'none';
  let resp;
  try { resp = await postJSON('api/delete', { instance: name }); }
  catch (e) { if (err) { err.style.display = 'block'; err.textContent = '删除失败：' + esc(e.message); } return; }
  if (resp.status !== 202) {
    if (err) { err.style.display = 'block'; err.textContent = '删除失败：' + esc((resp.data && resp.data.reason) || ('HTTP ' + resp.status)); }
    return;
  }
  const tid = resp.data.task_id;
  while (true) {
    let j;
    try { j = await getJSON('api/task/' + tid); } catch (e) { break; }
    if (j.state === 'running') { await new Promise(r => setTimeout(r, 1200)); continue; }
    break;
  }
  onDone();
}

const DEP_KINDS = ['etcd', 'minio', 'kafka', 'pulsar'];
const DEP_LABEL = { etcd: 'etcd · 元数据', minio: 'MinIO · 对象存储', kafka: 'Kafka · 消息队列', pulsar: 'Pulsar · 消息队列' };

async function renderDeps() {
  shell('deps');
  const box = document.getElementById('deps-list');
  try {
    const inst = await getJSON('api/instances');
    const doc = await getJSON('api/doctor').catch(() => ({ versions: {} }));
    const versions = doc.versions || {};
    box.innerHTML = DEP_KINDS.map(kind => {
      const rows = inst.instances.filter(i => i.kind === kind);
      const head = `<div class="card-head"><h3>${esc(DEP_LABEL[kind] || kind)} <span class="muted mono">v${esc(versions[kind] || '—')}</span></h3>` +
        `<a class="btn btn-ghost btn-sm" href="install.html">+ 新建</a></div>`;
      const body = rows.length
        ? '<table class="tbl"><tbody>' + rows.map(i =>
            `<tr><td>${esc(i.name)}</td><td class="muted">ns:${esc(i.namespace)}</td>` +
            `<td style="text-align:right"><button class="btn btn-ghost btn-sm" data-del="${esc(i.name)}">删除</button></td></tr>`).join('') + '</tbody></table>'
        : '<div class="muted">无实例</div>';
      return `<div class="card">${head}<div class="card-pad">${body}</div></div>`;
    }).join('');
    box.querySelectorAll('[data-del]').forEach(b => { b.onclick = () => deleteInstance(b.getAttribute('data-del'), renderDeps); });
  } catch (e) {
    box.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>';
  }
}
```

- [ ] **Step 5: Append chip styles to `webui/assets/web.css`** (at end):
```css
/* --- instance chips --- */
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{font-size:12px;font-family:'IBM Plex Mono',monospace;color:var(--fg-2);background:var(--surface-2);border:1px solid var(--line);border-radius:20px;padding:2px 10px}
```

- [ ] **Step 6: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_static.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 7: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/deps.html milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): Dependencies page (grouped by kind) + delete action

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Milvus instances page

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/milvus.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (add `renderMilvus`)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `/api/instances`, `deleteInstance` (Task 4), `shell/esc/getJSON/badge`.
- Produces: `GET /milvus.html`; `renderMilvus()`.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_milvus_page_served(client):
    r = client.get("/milvus.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="milvus-list"' in r.text
    assert "renderMilvus" in client.get("/assets/web.js").text
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k milvus_page -v`
Expected: FAIL — `/milvus.html` 404.

- [ ] **Step 3: Create `webui/milvus.html`**
```html
<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Milvus 实例 · Milvus Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/web.css">
</head><body>
<div class="app">
  <aside class="rail" id="rail"></aside>
  <div class="main">
    <header class="topbar" id="topbar"></header>
    <div class="content doc">
      <div class="page-head"><div class="h-l"><h1>Milvus 实例</h1>
        <p>以 Milvus 为中心，一卡一个实例：实时健康 + 依赖绑定。</p></div>
        <div class="h-r"><button class="btn btn-primary" id="refresh">刷新</button></div></div>
      <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
      <div id="milvus-list">加载中…</div>
    </div>
  </div>
</div>
<script src="assets/web.js"></script>
<script>renderMilvus();document.getElementById('refresh').onclick=renderMilvus;</script>
</body></html>
```

- [ ] **Step 4: Add `renderMilvus` to `webui/assets/web.js`** (append at end):
```javascript
async function renderMilvus() {
  shell('milvus');
  const box = document.getElementById('milvus-list');
  try {
    const inst = await getJSON('api/instances');
    const rows = inst.instances.filter(i => i.kind === 'milvus');
    const chip = (label, v) => v ? `<span class="chip">${esc(label)}: ${esc(v)}</span>` : '';
    const head = `<div style="margin-bottom:12px"><a class="btn btn-primary btn-sm" href="install.html">+ 新建 Milvus</a></div>`;
    box.innerHTML = head + (rows.length ? rows.map(i => {
      const st = i.status ? badge(i.status === 'Healthy' ? 'PASS' : 'WARN', i.status) : '<span class="muted">健康 —</span>';
      const d = i.deps || {};
      return `<div class="card"><div class="card-head"><h3>${esc(i.name)} ${st}</h3>` +
        `<button class="btn btn-ghost btn-sm" data-del="${esc(i.name)}">删除</button></div>` +
        `<div class="card-pad"><div class="muted mono" style="margin-bottom:8px">ns:${esc(i.namespace)} · ${esc(i.image || '—')}</div>` +
        `<div class="chips">${chip('etcd', d.etcd)}${chip('存储', d.storage)}${chip('MQ', d.mq)}${chip('端点', d.mq_endpoint)}</div></div></div>`;
    }).join('') : '<div class="card"><div class="card-pad muted">暂无 Milvus 实例</div></div>');
    box.querySelectorAll('[data-del]').forEach(b => { b.onclick = () => deleteInstance(b.getAttribute('data-del'), renderMilvus); });
  } catch (e) {
    box.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>';
  }
}
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_static.py -v` then `python -m pytest -q`
Expected: PASS.

- [ ] **Step 6: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/milvus.html milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): Milvus instances page (health + dep chips + delete)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`mb web --port 8090` (live cluster), open `http://127.0.0.1:8090/`:
- Overview no longer has an instances section.
- `/deps.html` groups etcd/minio/kafka/pulsar with versions; `/milvus.html` shows milvus-dev/milvus-pulsar with a green Healthy badge + etcd/存储/MQ chips.
- Delete a throwaway instance (install `etcd-web` first) → confirm → poll → it disappears from the list.

## Self-Review

- **Spec coverage:** D1 split → Tasks 3,4,5; D2 delete async → Task 2 + Tasks 4/5 (deleteInstance); D3 milvus live health / deps no-health → Tasks 1,5,4; D4 prototype card + chips → Tasks 4,5 (+CSS); D5 components → DEP_KINDS/milvus filter. §4 endpoints → Tasks 1,2. §5 probe.milvus_status + delete-pre-check → Tasks 1,2. §6 pages → Tasks 3,4,5.
- **Placeholder scan:** every code step complete; frontend verified via content-marker tests + manual DoD (no JS harness — stated). No TBD/TODO.
- **Type consistency:** `/api/instances` row keys `{name,kind,namespace,ownership,image,status,deps}` consistent Tasks 1↔4↔5; `deps` sub-keys `{etcd,storage,mq,mq_endpoint}` consistent Task 1↔5; `deleteInstance(name,onDone)` + `POST /api/delete {instance}` consistent Tasks 2↔4↔5; `probe.milvus_status(name,run)` Task 1; reuses `shell/esc/getJSON/postJSON/badge` from prior slices; nav ids `milvus`/`deps` match NAV_ICON + breadcrumb map + page filenames.
- **Existing-test note:** Task 3 updates `test_root_serves_overview_html` (drops the `id="instances"` assertion) since the instances card is removed — an intentional, called-out change.
