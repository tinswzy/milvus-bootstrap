# WebUI 配置(get/set) + 切换 MQ Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 接通 Milvus 卡片仅剩的两个灰占位——配置查看/修改 + 切换 MQ——复用已建成的流式日志基建（modal + logPanel + pollTask + 预演/dry-run + 门禁 409/force）。

**Architecture:** 后端加 4 个 async `/api/*` 端点（config GET / config-set / mq-options GET / switch-mq），复用既有 `config_get/config_set/switch_mq/mq_options` 核心逻辑 + `runner` 流式；前端加 `openConfig` / `openSwitchMq` 两个弹窗，接卡片按钮。仅 managed milvus 可用。

**Tech Stack:** Python 3 / FastAPI / pydantic / pytest（后端）；vanilla JS + CSS（前端）；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-10-webui-config-switchmq-design.md`（决策 D1–D8）。
- **新 `/api/*` 路由必须注册在 `app.py` 末尾的 `app.mount("/", StaticFiles(...))` 之前**（该 mount 吃掉 `/`；注册在其后不会生效）。把新路由加在既有 `/config/restart` 路由之后、`# --- WebUI static frontend` 注释之前。
- **流式/无轮询**：apply 走 `runner.submit` + 前端 `pollTask`（只轮 `GET /api/task` 的 mb 内存、有界、完成即停）。不引入 k8s 轮询；不改 `wait_cr`/`wait_ready`/`switch_mq`/`config_set` 核心逻辑；不动 sync CLI 路由（`/config/*`、`/switch-mq`、`/mq-options`）。
- **不谎报成功**：apply 后只说「已提交…· operator 处理中/滚动重启 · 🔄刷新」，绝不说「成功」。
- **门禁**：`switch-mq` 镜像 `/api/upgrade`——CompatError→409（既有全局 handler，返回 `{error,reason,force_hint}`）；未知实例先 `get_instance` 前置成 `ValueError`→400（既有 handler），避免 500。switch-mq 门禁只在 `target_wal == current_wal` 时触发（同类切换无意义）。
- **权限**：仅 `i.ownership==='managed'` 出真按钮；external 保持灰占位 `ph(...)`。
- **切 MQ 二次确认**：真实切换前必弹 `confirm(...)`（独立于门禁 force）。
- **配置只读视图默认折叠**（`<details>` 不带 `open`）。
- 命令在 `milvus-bootstrap/` 下跑：`cd milvus-bootstrap && source .venv/bin/activate`。基线 174 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用仓库 `user.name=tinswzy`。

---

### Task 1: 后端 config 端点（GET /api/config + POST /api/config/set）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（在 `/config/restart` 路由之后、`app.mount` 之前插入）
- Test: `milvus-bootstrap/tests/test_web_config.py`（create）

**Interfaces:**
- Consumes: `_core().state.get_instance`, `_core().config_get(instance)`, `_core().config_set(instance, kv, dry_run)`, `runner.submit`；既有 `ValueError`→400 handler。
- Produces:
  - `GET /api/config?instance=` → `{"instance","current": <configmap dict 或 None>, "overrides": <dict>}`.
  - `POST /api/config/set`（body `ConfigSetApiReq{instance:str, kv:dict={}, dry_run:bool=True}`）→ dry-run:200 `{"task": <dump>}`；apply:202 `{"task_id","state"}`.

- [ ] **Step 1: Write the failing tests**

`milvus-bootstrap/tests/test_web_config.py`:
```python
from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def _client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    return TestClient(app)


def test_api_config_get_shape(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with _client(tmp_path, monkeypatch) as client:
        _core().install(InstallSpec(kind="milvus", name="cfg-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/config", params={"instance": "cfg-mv"})
        assert r.status_code == 200
        body = r.json()
        assert body["instance"] == "cfg-mv"
        assert "current" in body                      # may be None under fake
        assert isinstance(body["overrides"], dict)


def test_api_config_get_unknown_400(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        r = client.get("/api/config", params={"instance": "nope"})
        assert r.status_code == 400


def test_api_config_set_dry_run_and_apply(tmp_path, monkeypatch):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with _client(tmp_path, monkeypatch) as client:
        _core().install(InstallSpec(kind="milvus", name="cfg-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        # dry-run -> 200 planned task with steps
        r = client.post("/api/config/set", json={"instance": "cfg-mv",
                                                 "kv": {"proxy.maxNameLength": "255"}, "dry_run": True})
        assert r.status_code == 200
        task = r.json()["task"]
        assert task["dry_run"] is True and len(task["steps"]) >= 1
        # apply -> 202 with task_id
        r2 = client.post("/api/config/set", json={"instance": "cfg-mv",
                                                  "kv": {"proxy.maxNameLength": "255"}, "dry_run": False})
        assert r2.status_code == 202 and "task_id" in r2.json()
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_config.py -q`
Expected: FAIL — `/api/config` routes don't exist (404).

- [ ] **Step 3: Add the endpoints**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, immediately AFTER the `config_restart` route (the `def config_restart(...)` block) and BEFORE the `# --- WebUI static frontend` comment, insert:
```python
@app.get("/api/config")
def api_config(instance: str) -> dict[str, Any]:
    inst = _core().state.get_instance(instance)
    if inst is None:
        raise ValueError(f"未找到实例：{instance}")
    snap = inst.spec_snapshot or {}
    overrides = (snap.get("params", {}) or {}).get("_conf", {}) or {}
    try:
        current = _core().config_get(instance)
    except Exception:  # noqa: BLE001  — CM may not exist yet; best-effort
        current = None
    return {"instance": instance, "current": current, "overrides": overrides}


class ConfigSetApiReq(BaseModel):
    instance: str
    kv: dict[str, Any] = {}
    dry_run: bool = True


@app.post("/api/config/set")
def api_config_set(req: ConfigSetApiReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().config_set(req.instance, req.kv, dry_run=True)
        return {"task": task.model_dump(mode="json")}
    tid = runner.submit(lambda: _core().config_set(req.instance, req.kv, dry_run=False))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)
```
(`BaseModel`, `Any`, `JSONResponse`, `runner`, `_core` are all already imported/defined in this file.)

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_config.py -q && python -m pytest -q`
Expected: config tests PASS; full suite PASS (was 174, +3).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/server/app.py tests/test_web_config.py
git commit -m "feat(server): async /api/config (GET current+overrides) + /api/config/set (dry-run 200 / apply 202)"
```

---

### Task 2: 后端 switch-mq 端点（GET /api/mq-options + POST /api/switch-mq）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（同 Task 1，插在 `app.mount` 之前）
- Test: `milvus-bootstrap/tests/test_web_switchmq.py`（create）

**Interfaces:**
- Consumes: `_core().state.get_instance`, `_core().mq_options(version, mode)`, `_core().switch_mq(instance, target_wal, dry_run, force)`, `probe._tag`, `compat.get_option`, `runner.submit`；既有 `CompatError`→409 / `ValueError`→400 handlers。
- Produces:
  - `GET /api/mq-options?instance=` → `{"instance","current_mq","current_wal","options": [{id,wal,label,dep_kind,supported,reason,note}]}`.
  - `POST /api/switch-mq`（body `SwitchMqApiReq{instance:str, target_wal:str, dry_run:bool=True, force:bool=False}`）→ dry-run:200 `{"task"}`；apply:202 `{"task_id"}`；门禁：409（`target_wal==current_wal` 且非 force）。

- [ ] **Step 1: Write the failing tests**

`milvus-bootstrap/tests/test_web_switchmq.py`:
```python
from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app


def _client_with_kafka_milvus(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    client = TestClient(app)
    client.__enter__()
    _core().install(InstallSpec(kind="milvus", name="mq-mv",
                                params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
    return client


def test_api_mq_options_shape(tmp_path, monkeypatch):
    client = _client_with_kafka_milvus(tmp_path, monkeypatch)
    try:
        r = client.get("/api/mq-options", params={"instance": "mq-mv"})
        assert r.status_code == 200
        body = r.json()
        assert body["current_mq"] == "kafka" and body["current_wal"] == "kafka"
        ids = [o["id"] for o in body["options"]]
        assert "kafka" in ids and "pulsar" in ids
        assert all({"id", "wal", "label", "supported"} <= set(o) for o in body["options"])
    finally:
        client.__exit__(None, None, None)


def test_api_switch_mq_dry_run_compatible(tmp_path, monkeypatch):
    client = _client_with_kafka_milvus(tmp_path, monkeypatch)
    try:
        r = client.post("/api/switch-mq", json={"instance": "mq-mv", "target_wal": "pulsar", "dry_run": True})
        assert r.status_code == 200
        assert len(r.json()["task"]["steps"]) >= 1
    finally:
        client.__exit__(None, None, None)


def test_api_switch_mq_same_wal_gate_409_then_force_202(tmp_path, monkeypatch):
    client = _client_with_kafka_milvus(tmp_path, monkeypatch)
    try:
        # kafka -> kafka blocked by gate (same-type) -> 409
        r = client.post("/api/switch-mq", json={"instance": "mq-mv", "target_wal": "kafka", "dry_run": False})
        assert r.status_code == 409 and r.json()["error"] == "compat"
        # with force -> 202
        r2 = client.post("/api/switch-mq", json={"instance": "mq-mv", "target_wal": "kafka",
                                                 "dry_run": False, "force": True})
        assert r2.status_code == 202 and "task_id" in r2.json()
    finally:
        client.__exit__(None, None, None)


def test_api_switch_mq_unknown_400(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.post("/api/switch-mq", json={"instance": "nope", "target_wal": "kafka"})
        assert r.status_code == 400
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py -q`
Expected: FAIL — `/api/mq-options` and `/api/switch-mq` don't exist (404).

- [ ] **Step 3: Add the endpoints**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, right after the Task-1 config endpoints (still before `app.mount`), insert:
```python
@app.get("/api/mq-options")
def api_mq_options(instance: str) -> dict[str, Any]:
    inst = _core().state.get_instance(instance)
    if inst is None:
        raise ValueError(f"未找到实例：{instance}")
    params = (inst.spec_snapshot or {}).get("params", {}) or {}
    from ..core import compat, probe
    version = probe._tag(params.get("image", "")) or ""
    mode = params.get("mode", "standalone")
    cur_mq = params.get("mq", "")
    cur_opt = compat.get_option(cur_mq)
    current_wal = cur_opt.wal if cur_opt else cur_mq
    return {"instance": instance, "current_mq": cur_mq, "current_wal": current_wal,
            "options": _core().mq_options(version, mode)}


class SwitchMqApiReq(BaseModel):
    instance: str
    target_wal: str
    dry_run: bool = True
    force: bool = False


@app.post("/api/switch-mq")
def api_switch_mq(req: SwitchMqApiReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().switch_mq(req.instance, req.target_wal, dry_run=True, force=req.force)
        return {"task": task.model_dump(mode="json")}
    # apply: sync gate pre-check (CompatError -> 409 via handler), then submit
    _core().switch_mq(req.instance, req.target_wal, dry_run=True, force=req.force)
    tid = runner.submit(lambda: _core().switch_mq(req.instance, req.target_wal, dry_run=False, force=req.force))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)
```

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py -q && python -m pytest -q`
Expected: switch-mq tests PASS; full suite PASS (+4).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/server/app.py tests/test_web_switchmq.py
git commit -m "feat(server): async /api/mq-options + /api/switch-mq (dry-run 200 / gate 409 / apply 202)"
```

---

### Task 3: 前端 配置 UI（openConfig + collectKv + configButton）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `GET /api/config`, `POST /api/config/set` (Task 1); `logPanel`, `pollTask`, `openModal`, `closeModal`, `postJSON`, `getJSON`, `esc`, `renderMilvus` (existing).
- Produces: `openConfig(name)`, `collectKv()`, `configButton(i)`; card action row wires `[data-config]`.

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_config_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function openConfig" in js and "function collectKv" in js and "function configButton" in js
    assert "api/config" in js and "data-config" in js
    assert "cfg-view" in js                       # collapsed current-config view
    css = client.get("/assets/web.css").text
    assert ".cfg-view" in css
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_config_ui_present -q`
Expected: FAIL — functions/markers absent.

- [ ] **Step 3: Add `configButton`, wire card, add `openConfig` + `collectKv`**

In `web.js`, add `configButton` next to `upgradeButton` (after the `upgradeButton` function, ~line 518):
```javascript
function configButton(i) {
  return i.ownership === 'managed'
    ? `<button class="btn btn-ghost btn-sm" data-config="${esc(i.name)}">配置</button>`
    : `<button class="btn btn-ghost btn-sm" disabled title="external：mb 未安装，不可改配置">配置</button>`;
}
```
In `renderMilvus`, change the action row (currently
`` `<div class="mv-actions">${upgradeButton(i)}${ph('配置')}${podsButton(i)}${ph('切换 MQ')}${delButton(i)}</div>` ``)
so the 配置 placeholder becomes the real button:
```javascript
            `<div class="mv-actions">${upgradeButton(i)}${configButton(i)}${podsButton(i)}${ph('切换 MQ')}${delButton(i)}</div>` +
```
Add the wiring next to the other `box.querySelectorAll('[data-...]')` lines in `renderMilvus`:
```javascript
    box.querySelectorAll('[data-config]').forEach(b => { b.onclick = () => openConfig(b.getAttribute('data-config')); });
```
Add these two functions (place them near `openUpgrade`):
```javascript
function collectKv() {
  const kv = {};
  document.querySelectorAll('#cfg-rows .crow').forEach(row => {
    const k = row.querySelector('.ck').value.trim();
    const v = row.querySelector('.cv').value.trim();
    if (k) kv[k] = v;
  });
  return kv;
}

function cfgRow(k, v) {
  return `<div class="crow prow"><input class="ck pk" placeholder="key（如 proxy.maxNameLength）" value="${esc(k || '')}">` +
         `<span>=</span><input class="cv pv" placeholder="value" value="${esc(v || '')}">` +
         `<button class="btn btn-ghost btn-sm cdel">删</button></div>`;
}

async function openConfig(name) {
  const m = openModal('配置 · ' + name,
    '<div id="cfg-top" class="muted">加载中…</div>' +
    '<div style="margin:10px 0 4px;font-weight:600">覆盖配置（点状键 = 值）</div>' +
    '<div id="cfg-rows"></div>' +
    '<div style="margin-top:8px"><button class="btn btn-ghost btn-sm" id="cfg-add">+ 添加</button></div>' +
    '<div style="margin-top:12px;display:flex;gap:8px"><button class="btn btn-ghost btn-sm" id="cfg-dry">预演</button>' +
    '<button class="btn btn-primary btn-sm" id="cfg-go">应用</button></div>' +
    '<div id="cfg-result" style="margin-top:12px"></div>');
  const rows = m.body.querySelector('#cfg-rows');
  const res = m.body.querySelector('#cfg-result');
  const addRow = (k, v) => {
    rows.insertAdjacentHTML('beforeend', cfgRow(k, v));
    const row = rows.lastElementChild;
    row.querySelector('.cdel').onclick = () => row.remove();
  };
  m.body.querySelector('#cfg-add').onclick = () => addRow('', '');

  let data;
  try { data = await getJSON('api/config?instance=' + encodeURIComponent(name)); }
  catch (e) { m.body.querySelector('#cfg-top').innerHTML = '<span class="conn bad">读取失败：' + esc(e.message) + '</span>'; return; }
  const cur = data.current;
  m.body.querySelector('#cfg-top').innerHTML = cur
    ? '<details class="cfg-view"><summary>当前生效配置（只读）</summary><pre>' +
      esc(typeof cur === 'string' ? cur : JSON.stringify(cur, null, 2)) + '</pre></details>'
    : '<div class="muted">无法读取当前配置（可能尚未生成）</div>';
  const ov = data.overrides || {};
  Object.keys(ov).forEach(k => addRow(k, ov[k]));
  if (!Object.keys(ov).length) addRow('', '');

  const submit = async (dryRun) => {
    res.innerHTML = '<span class="muted">提交中…</span>';
    let resp;
    try { resp = await postJSON('api/config/set', { instance: name, kv: collectKv(), dry_run: dryRun }); }
    catch (e) { res.innerHTML = '<span class="conn bad">提交失败：' + esc(e.message) + '</span>'; return; }
    const { status, data: d } = resp;
    if (status === 200) { res.innerHTML = logPanel(d.task, false); return; }
    if (status === 202) {
      await pollTask(d.task_id, res, () => {
        res.innerHTML += '<div class="conn ok" style="margin-top:8px">已提交配置变更 · operator 滚动重启相关 pod</div>' +
          '<button class="btn btn-ghost btn-sm" id="cfg-refresh" style="margin-top:6px">🔄 刷新</button>';
        const b = document.getElementById('cfg-refresh');
        if (b) b.onclick = () => { closeModal(); renderMilvus(); };
      });
      return;
    }
    res.innerHTML = '<span class="conn bad">失败（HTTP ' + status + '）：' + esc((d && d.reason) || '未知错误') + '</span>';
  };
  m.body.querySelector('#cfg-dry').onclick = () => submit(true);
  m.body.querySelector('#cfg-go').onclick = () => submit(false);
}
```

- [ ] **Step 4: Add CSS**

Append to `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`:
```css
/* config view (collapsed current config) */
.cfg-view { border:1px solid var(--border); border-radius:8px; background:var(--surface-2); }
.cfg-view > summary { cursor:pointer; padding:8px 10px; font-weight:600; }
.cfg-view pre { max-height:220px; overflow:auto; margin:0; padding:8px 10px; font-size:12px;
  white-space:pre-wrap; word-break:break-all; }
```

- [ ] **Step 5: Verify JS parses + tests pass**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q`
Expected: JS OK; tests PASS.

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js src/milvus_bootstrap/webui/assets/web.css tests/test_web_static.py
git commit -m "feat(webui): config UI — collapsed current config + dotted-key overrides, streams apply"
```

---

### Task 4: 前端 切换 MQ UI（openSwitchMq + submitSwitchMq + switchMqButton）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `GET /api/mq-options`, `POST /api/switch-mq` (Task 2); `logPanel`, `pollTask`, `openModal`, `closeModal`, `postJSON`, `getJSON`, `esc`, `renderMilvus` (existing).
- Produces: `openSwitchMq(name)`, `submitSwitchMq(name, targetWal, dryRun, force, el)`, `switchMqButton(i)`; card action row wires `[data-switch]`.

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_switch_mq_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function openSwitchMq" in js and "function submitSwitchMq" in js and "function switchMqButton" in js
    assert "api/switch-mq" in js and "api/mq-options" in js and "data-switch" in js
    body = js.split("function submitSwitchMq", 1)[1].split("\nfunction ", 1)[0]
    assert "pollTask(" in body and "409" in body and "已提交 MQ 切换" in body   # stream + gate + honest handoff
    ob = js.split("function openSwitchMq", 1)[1].split("\nfunction ", 1)[0]
    assert "确认切换 MQ" in ob                     # D4 second confirmation
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_ui_present -q`
Expected: FAIL — functions/markers absent.

- [ ] **Step 3: Add `switchMqButton`, wire card, add `openSwitchMq` + `submitSwitchMq`**

In `web.js`, add `switchMqButton` next to `configButton`:
```javascript
function switchMqButton(i) {
  return i.ownership === 'managed'
    ? `<button class="btn btn-ghost btn-sm" data-switch="${esc(i.name)}">切换 MQ</button>`
    : `<button class="btn btn-ghost btn-sm" disabled title="external：mb 未安装，不可切 MQ">切换 MQ</button>`;
}
```
In `renderMilvus`, change the action row so the 切换 MQ placeholder becomes the real button (the row was updated in Task 3 to
`` `...${configButton(i)}${podsButton(i)}${ph('切换 MQ')}${delButton(i)}...` ``); make it:
```javascript
            `<div class="mv-actions">${upgradeButton(i)}${configButton(i)}${podsButton(i)}${switchMqButton(i)}${delButton(i)}</div>` +
```
Add the wiring next to the other `[data-...]` lines in `renderMilvus`:
```javascript
    box.querySelectorAll('[data-switch]').forEach(b => { b.onclick = () => openSwitchMq(b.getAttribute('data-switch')); });
```
Add these two functions (place them near `openUpgrade`/`submitUpgrade`):
```javascript
async function submitSwitchMq(name, targetWal, dryRun, force, el) {
  el.innerHTML = '<span class="muted">提交中…</span>';
  let resp;
  try { resp = await postJSON('api/switch-mq', { instance: name, target_wal: targetWal, dry_run: dryRun, force: !!force }); }
  catch (e) { el.innerHTML = '<span class="conn bad">提交失败：' + esc(e.message) + '</span>'; return; }
  const { status, data } = resp;
  if (status === 200) { el.innerHTML = logPanel(data.task, false); return; }
  if (status === 202) {
    await pollTask(data.task_id, el, () => {
      el.innerHTML += '<div class="conn ok" style="margin-top:8px">已提交 MQ 切换 · operator 处理中</div>' +
        '<button class="btn btn-ghost btn-sm" id="mq-refresh" style="margin-top:6px">🔄 刷新</button>';
      const b = document.getElementById('mq-refresh');
      if (b) b.onclick = () => { closeModal(); renderMilvus(); };
    });
    return;
  }
  if (status === 409) {
    el.innerHTML = `<div class="conn bad">被兼容门禁拦截：${esc((data && data.reason) || '兼容门禁')}</div>` +
      `<button class="btn btn-primary btn-sm" id="mq-force" style="margin-top:8px">强制切换 --force</button>`;
    const b = document.getElementById('mq-force');
    if (b) b.onclick = () => { if (confirm('确认跳过兼容门禁强制切换 MQ？')) submitSwitchMq(name, targetWal, dryRun, true, el); };
    return;
  }
  el.innerHTML = '<span class="conn bad">失败（HTTP ' + status + '）：' + esc((data && data.reason) || '未知错误') + '</span>';
}

async function openSwitchMq(name) {
  const m = openModal('切换 MQ · ' + name,
    '<div id="mq-top" class="muted">加载中…</div>' +
    '<div style="margin-top:12px;display:flex;gap:8px"><button class="btn btn-ghost btn-sm" id="mq-dry">预演</button>' +
    '<button class="btn btn-primary btn-sm" id="mq-go">切换</button></div>' +
    '<div id="mq-result" style="margin-top:12px"></div>');
  const res = m.body.querySelector('#mq-result');
  let data;
  try { data = await getJSON('api/mq-options?instance=' + encodeURIComponent(name)); }
  catch (e) { m.body.querySelector('#mq-top').innerHTML = '<span class="conn bad">读取失败：' + esc(e.message) + '</span>'; return; }
  const curWal = data.current_wal || '';
  const opts = (data.options || []).map(o => {
    const dis = (!o.supported || o.wal === curWal) ? ' disabled' : '';
    const tag = o.wal === curWal ? '（当前）' : (o.supported ? '' : '（不兼容：' + esc(o.reason || '') + '）');
    return `<option value="${esc(o.wal)}"${dis}>${esc(o.label)} · ${esc(o.dep_kind || '嵌入')}${tag}</option>`;
  }).join('');
  m.body.querySelector('#mq-top').innerHTML =
    `<div>当前 MQ：<b>${esc(data.current_mq || '—')}</b> <span class="muted">(wal=${esc(curWal || '—')})</span></div>` +
    `<label class="mvl" style="margin-top:8px">目标 MQ</label><select id="mq-target" class="f-in">${opts}</select>`;
  const target = () => { const s = m.body.querySelector('#mq-target'); return s ? s.value : ''; };
  m.body.querySelector('#mq-dry').onclick = () => { if (target()) submitSwitchMq(name, target(), true, false, res); };
  m.body.querySelector('#mq-go').onclick = () => {
    const t = target();
    if (!t) return;
    if (confirm('确认切换 MQ 到 ' + t + '？这会更改消息队列/WAL 并在 pod 内执行变更，可能影响写入。'))
      submitSwitchMq(name, t, false, false, res);
  };
}
```

- [ ] **Step 4: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js tests/test_web_static.py
git commit -m "feat(webui): switch-MQ UI — options dropdown, 2-step confirm, gate 409/force, streams apply"
```

---

## Notes for the executor
- 每个 Task 末尾跑一次全量 `python -m pytest -q`（基线 174，逐任务递增），前端改动后 `node --check`。
- Task 3 与 Task 4 都改 `renderMilvus` 的同一行动作行（先换 `ph('配置')`→`configButton(i)`，再换 `ph('切换 MQ')`→`switchMqButton(i)`）——顺序执行无冲突；Task 4 基于 Task 3 后的行内容修改。
- 手动 DoD（合并前真集群一次）：某 managed milvus 卡「配置」→ 折叠展开看当前 configmap；加 `proxy.maxNameLength=255`→预演看计划步骤+命令→应用看流式日志→完成提示刷新。「切换 MQ」→ 选目标→预演看步骤；点「切换」先弹二次确认→（kafka→kafka 会 409+强制）→流式→完成提示。external 实例这俩仍灰。
