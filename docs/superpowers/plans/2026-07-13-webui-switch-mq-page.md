# WebUI Switch-MQ 独立页面 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把切换 MQ 从卡片模态升级为独立页面（prototype 风格），目标 MQ 用兼容表驱动——不可选项灰置并说明原因；先支持已有 MQ、woodpecker-service 判定预留。

**Architecture:** 后端 `compat.switch_mq_targets`（在 `mq_options` 上叠加 switch 可选性）+ `GET /api/switch-mq/targets`；前端新 `switch-mq.html` + `renderSwitchMq()`（复用 `submitSwitchMq`/`logPanel`/`pollTask`），卡「切换 MQ」按钮改跳转、退休模态 `openSwitchMq`。

**Tech Stack:** Python 3 + FastAPI + pytest；vanilla HTML/JS/CSS；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-13-webui-switch-mq-page-design.md`（决策 D1–D8）。
- **兼容规则**（`switch_mq_targets`）：① `wal==current_wal`→不可选「与当前 MQ 相同」；② `standalone_only`+`mode!=standalone`→不可选（rocksmq/cluster）；③ `min_milvus>当前`→不可选（2.x→woodpecker-service）；④ `woodpecker-service`→**预留关闭**（`_operator_supports_ext_woodpecker` 恒 False），reason 提「规划中」。
- **woodpecker-service 不实现切换**，仅判定预留；`operator_version` 参数预留（端点传 ""，不做重探测）。
- **护栏**：目标不可选灰置带因；切换需勾「WAL 不可迁移」确认框 + `confirm` 二次确认。
- **无轮询**：进页/换实例读一次 targets；apply 流式（pollTask）；**禁止 setInterval**。
- **新 `/api/switch-mq/targets` 注册在 `app.py` 末尾 `app.mount("/")` 之前**；不改现有 `/api/switch-mq`。
- best-effort：读不到/未知实例→400 或占位不崩；仅 managed milvus。
- 命令在 `milvus-bootstrap/` 下：`cd milvus-bootstrap && source .venv/bin/activate`。基线 205 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用 `user.name=tinswzy`。

---

### Task 1: `compat.switch_mq_targets`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.py`
- Test: `milvus-bootstrap/tests/test_compat.py`（追加）

**Interfaces:**
- Consumes: existing `compat.mq_options(milvus_version, mode) -> list[dict]`（每项 `{id,wal,label,dep_kind,supported,reason,note}`）。
- Produces:
  - `_operator_supports_ext_woodpecker(operator_version: str) -> bool`（现恒 False）
  - `switch_mq_targets(current_wal, milvus_version, mode="standalone", operator_version="") -> list[dict]`（每项 `{id,wal,label,dep_kind,note,current,selectable,reason}`）

- [ ] **Step 1: Write the failing tests**

Add to `milvus-bootstrap/tests/test_compat.py`:
```python
def test_switch_mq_targets_same_wal_not_selectable():
    from milvus_bootstrap.core import compat
    ts = {t["id"]: t for t in compat.switch_mq_targets("kafka", "2.6.0", "standalone")}
    assert ts["kafka"]["selectable"] is False and "相同" in ts["kafka"]["reason"]
    assert ts["kafka"]["current"] is True
    assert ts["pulsar"]["selectable"] is True and ts["pulsar"]["current"] is False


def test_switch_mq_targets_rocksmq_cluster_blocked():
    from milvus_bootstrap.core import compat
    ts = {t["id"]: t for t in compat.switch_mq_targets("kafka", "2.6.0", "cluster")}
    assert ts["rocksmq"]["selectable"] is False and "standalone" in ts["rocksmq"]["reason"]


def test_switch_mq_targets_woodpecker_service_reserved():
    from milvus_bootstrap.core import compat
    # milvus 2.x: blocked by min_milvus (needs 3.0)
    lo = {t["id"]: t for t in compat.switch_mq_targets("kafka", "2.6.0", "standalone")}
    assert lo["woodpecker-service"]["selectable"] is False
    # milvus 3.0: version ok, but reserved-off (external woodpecker not supported yet)
    hi = {t["id"]: t for t in compat.switch_mq_targets("kafka", "3.0.0", "standalone")}
    assert hi["woodpecker-service"]["selectable"] is False
    assert "规划中" in hi["woodpecker-service"]["reason"] or "external" in hi["woodpecker-service"]["reason"]


def test_operator_supports_ext_woodpecker_reserved_false():
    from milvus_bootstrap.core import compat
    assert compat._operator_supports_ext_woodpecker("1.3.6") is False
    assert compat._operator_supports_ext_woodpecker("") is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_compat.py -q`
Expected: FAIL — `switch_mq_targets`/`_operator_supports_ext_woodpecker` don't exist.

- [ ] **Step 3: Implement in `core/compat.py`**

Add (after the `mq_options` function):
```python
def _operator_supports_ext_woodpecker(operator_version: str) -> bool:
    # Reserved: external woodpecker-service switching needs a milvus-operator version that
    # supports it. mb does not support this switch yet -> always False for now.
    return False


def switch_mq_targets(current_wal: str, milvus_version: str, mode: str = "standalone",
                      operator_version: str = "") -> list[dict]:
    """Per-MQ selectability for switching THIS instance's MQ. Builds on mq_options()."""
    out = []
    for o in mq_options(milvus_version, mode):        # supported/reason from min_milvus + standalone_only
        selectable, reason = o["supported"], o["reason"]
        if o["wal"] == current_wal:
            selectable, reason = False, "与当前 MQ 相同，无需切换"
        elif o["id"] == "woodpecker-service" and not _operator_supports_ext_woodpecker(operator_version):
            selectable = False
            reason = reason or ("暂不支持切换到 Woodpecker 独立服务"
                                "（需 milvus≥3.0 且 milvus-operator 支持 external woodpecker，规划中）")
        out.append({"id": o["id"], "wal": o["wal"], "label": o["label"], "dep_kind": o["dep_kind"],
                    "note": o["note"], "current": o["wal"] == current_wal,
                    "selectable": selectable, "reason": reason})
    return out
```

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_compat.py -q && python -m pytest -q`
Expected: compat tests PASS; full suite PASS (was 205, +4).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/compat.py tests/test_compat.py
git commit -m "feat(compat): switch_mq_targets — compat-driven MQ target selectability (reserve woodpecker-service)"
```

---

### Task 2: `GET /api/switch-mq/targets`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（插在 `app.mount("/")` 之前）
- Test: `milvus-bootstrap/tests/test_web_switchmq.py`（追加）

**Interfaces:**
- Consumes: `compat.switch_mq_targets`, `compat.get_option`, `probe._tag` (Task 1 + existing).
- Produces: `GET /api/switch-mq/targets?instance=` → `{instance, current_mq, current_wal, milvus_version, mode, targets:[...]}`。

- [ ] **Step 1: Write the failing test**

Add to `milvus-bootstrap/tests/test_web_switchmq.py`:
```python
def test_api_switch_mq_targets_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="milvus", name="sw-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/switch-mq/targets", params={"instance": "sw-mv"})
        assert r.status_code == 200
        body = r.json()
        assert body["current_mq"] == "kafka" and body["current_wal"] == "kafka"
        ts = {t["id"]: t for t in body["targets"]}
        assert ts["kafka"]["selectable"] is False        # same as current
        assert ts["pulsar"]["selectable"] is True
        assert "current" in ts["kafka"] and "reason" in ts["kafka"]


def test_api_switch_mq_targets_unknown_400(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    with TestClient(app) as client:
        r = client.get("/api/switch-mq/targets", params={"instance": "nope"})
        assert r.status_code == 400
```
(`TestClient`, `app` already imported at the top of `test_web_switchmq.py`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py -q`
Expected: FAIL — route 404.

- [ ] **Step 3: Add the endpoint**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, insert BEFORE the `# --- WebUI static frontend` comment / `app.mount("/", ...)` line (among the other `/api/*` routes):
```python
@app.get("/api/switch-mq/targets")
def api_switch_mq_targets(instance: str) -> dict[str, Any]:
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
    targets = compat.switch_mq_targets(current_wal, version, mode, operator_version="")  # op_ver reserved
    return {"instance": instance, "current_mq": cur_mq, "current_wal": current_wal,
            "milvus_version": version, "mode": mode, "targets": targets}
```

- [ ] **Step 4: Run tests to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py -q && python -m pytest -q`
Expected: PASS (+2).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/server/app.py tests/test_web_switchmq.py
git commit -m "feat(server): GET /api/switch-mq/targets — instance-aware MQ target selectability"
```

---

### Task 3: `switch-mq.html` 页面 + `renderSwitchMq`

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/webui/switch-mq.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（新 `renderSwitchMq`；`shell` crumb 加 switch-mq；`submitSwitchMq` 的刷新钮改 `location.reload()`）
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Test: `milvus-bootstrap/tests/test_web_static.py`（追加）

**Interfaces:**
- Consumes: `GET /api/switch-mq/targets` (Task 2); `loadInstances`, `submitSwitchMq`, `getJSON`, `esc`, `shell` (existing).
- Produces: `renderSwitchMq()`; page served at `/switch-mq.html`.

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_switch_mq_page_present(client):
    html = client.get("/switch-mq.html").text
    assert 'id="sw-targets"' in html and 'id="sw-ack"' in html and "renderSwitchMq()" in html
    js = client.get("/assets/web.js").text
    assert "function renderSwitchMq" in js
    assert "api/switch-mq/targets" in js and "sw-opt" in js and "data-wal" in js
    assert "location.reload()" in js.split("function submitSwitchMq", 1)[1].split("\nfunction ", 1)[0]
    assert "setInterval" not in js
    css = client.get("/assets/web.css").text
    assert ".sw-opt" in css
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_page_present -q`
Expected: FAIL — page/markers absent.

- [ ] **Step 3: Create `switch-mq.html`**

Create `milvus-bootstrap/src/milvus_bootstrap/webui/switch-mq.html`:
```html
<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>切换 MQ · Milvus Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/web.css">
</head><body>
<div class="app">
  <aside class="rail" id="rail"></aside>
  <div class="main">
    <header class="topbar" id="topbar"></header>
    <div class="content doc">
      <div class="page-head"><div class="h-l"><h1>切换消息队列</h1>
        <p>把某个 Milvus 实例切换到另一种 MQ。不可选的目标会灰置并说明原因（版本 / 模式 / 同类 / 规划中）。</p></div></div>
      <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
      <div class="card"><div class="card-head"><h3>实例与当前 MQ</h3></div><div class="card-pad">
        <div class="f-row"><label>实例</label><select id="sw-inst" class="f-in"></select></div>
        <div id="sw-current" class="muted" style="margin-top:8px">—</div></div></div>
      <div class="card"><div class="card-head"><h3>目标 MQ</h3><span class="sub">灰置 = 不可选（下方看原因）</span></div>
        <div class="card-pad"><div id="sw-targets">加载中…</div></div></div>
      <div class="card co-warn" style="margin:0 0 14px"><div class="card-pad">
        <div><b>WAL 数据不可跨 MQ 迁移。</b>切换等价于在空集群上更换后端，存量流式数据无法原地迁移。</div>
        <label style="display:flex;gap:8px;margin-top:10px;cursor:pointer"><input type="checkbox" id="sw-ack">
          <span>我已知悉：切换后需重建 collection 并重新导入</span></label></div></div>
      <div class="card"><div class="card-head"><h3>执行</h3></div><div class="card-pad">
        <button class="btn btn-ghost" id="sw-dry" disabled>预演（dry-run）</button>
        <button class="btn btn-primary" id="sw-go" disabled>切换</button>
        <div id="sw-result" style="margin-top:12px"></div></div></div>
    </div>
  </div>
</div>
<script src="assets/web.js"></script>
<script>renderSwitchMq();</script>
</body></html>
```

- [ ] **Step 4: Add `renderSwitchMq` + crumb + submitSwitchMq reload**

In `web.js`, add the `switch-mq` crumb to the `shell` topbar map (line ~34): change
`{ compat: '版本依赖', install: '安装向导', milvus: 'Milvus 实例', deps: 'Dependencies' }` to
`{ compat: '版本依赖', install: '安装向导', milvus: 'Milvus 实例', deps: 'Dependencies', 'switch-mq': '切换 MQ' }`.

In `submitSwitchMq`, change the 202 handoff refresh button handler from
`if (b) b.onclick = () => { closeModal(); renderMilvus(); };`
to
`if (b) b.onclick = () => { location.reload(); };`

Add the new page function (place it near `renderInstall`):
```javascript
async function renderSwitchMq() {
  shell('switch-mq');
  const params = new URLSearchParams(location.search);
  let selInst = params.get('instance') || '';
  let selectedWal = null;
  const sel = document.getElementById('sw-inst');
  const cur = document.getElementById('sw-current');
  const tgt = document.getElementById('sw-targets');
  const ack = document.getElementById('sw-ack');
  const dry = document.getElementById('sw-dry');
  const go = document.getElementById('sw-go');
  const res = document.getElementById('sw-result');

  const insts = await loadInstances();
  const milvus = insts.filter(i => i.kind === 'milvus' && i.ownership === 'managed');
  if (!milvus.length) { cur.innerHTML = '<span class="muted">无 managed milvus 实例</span>'; tgt.innerHTML = ''; return; }
  sel.innerHTML = milvus.map(i => `<option value="${esc(i.name)}">${esc(i.name)} (${esc(i.namespace)})</option>`).join('');
  if (!selInst || !milvus.some(i => i.name === selInst)) selInst = milvus[0].name;
  sel.value = selInst;

  const syncButtons = () => {
    dry.disabled = !selectedWal;
    go.disabled = !(selectedWal && ack.checked);
  };

  const load = async (name) => {
    selectedWal = null; res.innerHTML = ''; ack.checked = false; syncButtons();
    tgt.innerHTML = '<span class="muted">加载中…</span>';
    let d;
    try { d = await getJSON('api/switch-mq/targets?instance=' + encodeURIComponent(name)); }
    catch (e) { tgt.innerHTML = '<span class="conn bad">加载失败：' + esc(e.message) + '</span>'; return; }
    cur.innerHTML = `当前 MQ：<b>${esc(d.current_mq || '—')}</b> ` +
      `<span class="muted">wal=${esc(d.current_wal || '—')} · milvus ${esc(d.milvus_version || '—')} · ${esc(d.mode || '—')}</span>`;
    tgt.innerHTML = (d.targets || []).map(t => {
      const cls = 'sw-opt' + (t.current ? ' cur' : '') + (t.selectable ? '' : ' dis');
      const note = t.note ? `<div class="r">${esc(t.note)}</div>` : '';
      const reason = t.selectable ? '' : `<div class="r">${esc(t.reason || '不可选')}</div>`;
      return `<div class="${cls}" data-wal="${esc(t.wal)}" data-ok="${t.selectable ? 1 : 0}">` +
        `<div class="t">${esc(t.label)}${t.current ? ' · 当前' : ''}</div>${note}${reason}</div>`;
    }).join('') || '<span class="muted">无可用 MQ 选项</span>';
    tgt.querySelectorAll('.sw-opt').forEach(elm => {
      if (elm.getAttribute('data-ok') !== '1') return;
      elm.onclick = () => {
        tgt.querySelectorAll('.sw-opt').forEach(x => x.classList.remove('sel'));
        elm.classList.add('sel'); selectedWal = elm.getAttribute('data-wal'); syncButtons();
      };
    });
  };

  sel.onchange = () => { selInst = sel.value; load(selInst); };
  ack.onchange = syncButtons;
  dry.onclick = () => { if (selectedWal) submitSwitchMq(selInst, selectedWal, true, false, res); };
  go.onclick = () => {
    if (!selectedWal) return;
    if (confirm('确认切换 ' + selInst + ' 的 MQ 到 ' + selectedWal +
                '？这会更改 WAL 并在 pod 内执行变更，存量流式数据将无法保留。'))
      submitSwitchMq(selInst, selectedWal, false, false, res);
  };
  load(selInst);
}
```

- [ ] **Step 5: Add CSS**

Append to `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`:
```css
/* switch-mq target options */
.sw-opt { display:inline-flex; flex-direction:column; gap:2px; min-width:190px; vertical-align:top;
  border:1px solid var(--border); border-radius:10px; padding:10px 12px; margin:0 8px 8px 0; cursor:pointer; }
.sw-opt .t { font-weight:600; }
.sw-opt .r { font-size:11.5px; color:var(--muted); margin-top:2px; }
.sw-opt.sel { border-color:var(--accent); box-shadow:0 0 0 2px var(--accent-soft); }
.sw-opt.dis { opacity:.5; cursor:not-allowed; background:var(--surface-2); }
.sw-opt.cur { border-style:dashed; }
```

- [ ] **Step 6: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 7: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/switch-mq.html src/milvus_bootstrap/webui/assets/web.js src/milvus_bootstrap/webui/assets/web.css tests/test_web_static.py
git commit -m "feat(webui): switch-mq.html page + renderSwitchMq — compat-driven target selection (grayed+reason)"
```

---

### Task 4: 卡按钮改跳转 + 退休模态 `openSwitchMq`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（`renderMilvus` 卡 `[data-switch]` 改跳转；删除 `openSwitchMq`）
- Test: `milvus-bootstrap/tests/test_web_static.py`（更新 `test_switch_mq_ui_present`）

**Interfaces:**
- Consumes: `switch-mq.html` page (Task 3). `submitSwitchMq`, `switchMqButton` stay.
- Produces: 卡「切换 MQ」按钮跳转到 `switch-mq.html?instance=<name>`；`openSwitchMq` 模态移除。

- [ ] **Step 1: Update the failing test**

Replace the existing `test_switch_mq_ui_present` in `milvus-bootstrap/tests/test_web_static.py` with:
```python
def test_switch_mq_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function submitSwitchMq" in js and "function switchMqButton" in js
    assert "api/switch-mq" in js and "data-switch" in js
    # modal retired: openSwitchMq gone; card navigates to the dedicated page
    assert "function openSwitchMq" not in js
    assert "switch-mq.html?instance=" in js
    body = js.split("function submitSwitchMq", 1)[1].split("\nfunction ", 1)[0]
    assert "pollTask(" in body and "409" in body and "已提交 MQ 切换" in body   # stream + gate + honest handoff
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_ui_present -q`
Expected: FAIL — `openSwitchMq` still present; card wiring still calls it (no `switch-mq.html?instance=`).

- [ ] **Step 3: Change the card button wiring to navigate**

In `web.js` `renderMilvus`, the current switch wiring is:
```javascript
    box.querySelectorAll('[data-switch]').forEach(b => { b.onclick = () => openSwitchMq(b.getAttribute('data-switch')); });
```
Replace it with:
```javascript
    box.querySelectorAll('[data-switch]').forEach(b => { b.onclick = () => { location.href = 'switch-mq.html?instance=' + encodeURIComponent(b.getAttribute('data-switch')); }; });
```

- [ ] **Step 4: Delete the `openSwitchMq` modal function**

In `web.js`, delete the entire `async function openSwitchMq(name) { ... }` function (the modal — now replaced by the dedicated page). Do NOT touch `submitSwitchMq` or `switchMqButton`.

- [ ] **Step 5: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js tests/test_web_static.py
git commit -m "feat(webui): Milvus card 切换 MQ navigates to switch-mq page; retire openSwitchMq modal"
```

---

## Notes for the executor
- 每个 Task 末尾跑全量 `python -m pytest -q`（基线 205，逐任务递增），前端改动后 `node --check`。
- 不得新增 setInterval；不改 `/api/switch-mq` 逻辑；woodpecker-service 仅判定预留、不实现切换。
- 手动 DoD（合并前真集群一次）：Milvus 卡「切换 MQ」→ 跳 `switch-mq.html?instance=milvus007`；当前 MQ=pulsar；目标卡 kafka 可选、pulsar 灰(相同)、rocksmq 灰(视 mode)、woodpecker 独立服务 灰(规划中)；勾护栏 → 预演出计划步骤 → 切换二次确认 → 流式；换实例下拉刷新 targets。