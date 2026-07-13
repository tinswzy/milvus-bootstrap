# WebUI Switch-MQ 拓扑化 + 步骤引导 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 switch-mq 页从卡片列表改成 prototype 风格的拓扑链路（Milvus→当前MQ⟶目标MQ，目标用下拉）+ 3 步引导 stepper。

**Architecture:** 纯前端。复用 web.css 已有的 `.topo`/`.box`/`.box-mv`/`.flow-*`/`.stepper`；只加 switch 专属 CSS（`.box-dark`/`.flow-switch`/`.mq-zone`）。重写 `switch-mq.html` 正文 + `renderSwitchMq`（下拉灰置带因、目标框联动、stepper 推进），后端与 `submitSwitchMq` 不动。

**Tech Stack:** vanilla HTML/CSS/JS；pytest content-marker；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-13-switch-mq-topology-design.md`（决策 D1–D8）。
- **纯前端**：不改后端（`switch_mq_targets`/`GET /api/switch-mq/targets`/`POST /api/switch-mq`）、不改 `submitSwitchMq`。
- **目标下拉**：`<select id="sw-target">`，不可选项 `disabled` 且标签内嵌原因；`value=wal`；每个 target 各一项（不去重）。
- **三层护栏保留**：disabled 不可选 / 切换钮需选中+勾 `#sw-ack` / `confirm` 二次确认。
- **无 setInterval**（flow 是 CSS 动画，带 `prefers-reduced-motion` 守卫）。
- 复用现有 `mqLogo(wal)`、`loadInstances()`、`submitSwitchMq(name,wal,dryRun,force,el)`、`shell`、`esc`、`getJSON`。
- 命令在 `milvus-bootstrap/` 下：`cd milvus-bootstrap && source .venv/bin/activate`。基线 212 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用 `user.name=tinswzy`。

---

### Task 1: Switch 专属 CSS（拓扑深框 + 流动箭头）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Test: `milvus-bootstrap/tests/test_web_static.py`（追加）

**Interfaces:**
- Produces: CSS classes `.mq-topo`, `.mq-zone`, `.box.box-dark`, `.box-dark .r`, `.flow-switch`(+`::after`,`.lbl`), `@keyframes swflow`, and a `prefers-reduced-motion` guard. Consumed by Task 2's markup.

- [ ] **Step 1: Write the failing test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_switch_mq_topology_css(client):
    css = client.get("/assets/web.css").text
    assert ".flow-switch" in css and ".box-dark" in css and ".mq-topo" in css
    assert "@keyframes swflow" in css
    assert "prefers-reduced-motion" in css        # animation guarded
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_topology_css -q`
Expected: FAIL — classes absent.

- [ ] **Step 3: Append the CSS**

Append to `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`:
```css
/* switch-mq topology (prototype-style MQ link) */
.mq-topo { display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
.mq-topo > .flow-h { width:34px; flex:none; }   /* .flow-h is grid-sized elsewhere; give it width in this flex row */
.mq-zone { display:flex; align-items:center; gap:0; flex-wrap:wrap; }
.box.box-dark { background:var(--surface-3); border:1.5px solid var(--accent); min-width:220px; cursor:default; }
.box.box-dark:hover { transform:none; box-shadow:none; }
.box-dark .r { font-size:11.5px; color:var(--muted); margin-top:6px; }
.flow-switch { position:relative; width:70px; height:2px; margin:0 20px; flex:none;
  background-image:linear-gradient(90deg,var(--warn) 55%,transparent 0); background-size:11px 2px; background-repeat:repeat-x;
  animation:swflow .8s linear infinite; }
.flow-switch::after { content:''; position:absolute; right:-2px; top:-4px;
  border-left:7px solid var(--warn); border-top:5px solid transparent; border-bottom:5px solid transparent; }
.flow-switch .lbl { position:absolute; top:-20px; left:50%; transform:translateX(-50%); font-size:10px;
  color:var(--warn); white-space:nowrap; background:var(--surface); padding:0 7px; font-weight:600; }
@keyframes swflow { to { background-position:11px 0; } }
@media (prefers-reduced-motion: reduce) { .flow-switch, .flow-h, .flow-v { animation:none; } }
```

- [ ] **Step 4: Run test to verify pass + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: PASS (full suite was 212, +1).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.css tests/test_web_static.py
git commit -m "feat(webui): switch-mq topology CSS — dark target box + animated flow arrow (reduced-motion guarded)"
```

---

### Task 2: `switch-mq.html` 拓扑正文 + `renderSwitchMq` 重写

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/switch-mq.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（`renderSwitchMq` 整体替换）
- Test: `milvus-bootstrap/tests/test_web_static.py`（更新 `test_switch_mq_page_present`）

**Interfaces:**
- Consumes: `GET /api/switch-mq/targets` (unchanged); Task 1 CSS; `mqLogo`, `loadInstances`, `submitSwitchMq`, `shell`, `esc`, `getJSON` (existing).
- Produces: new topology+stepper page; `renderSwitchMq()` renders topology, builds `#sw-target` dropdown (disabled+reason), advances `#sw-stepper` via `setStep`.

- [ ] **Step 1: Update the failing test**

Replace `test_switch_mq_page_present` in `milvus-bootstrap/tests/test_web_static.py` with:
```python
def test_switch_mq_page_present(client):
    html = client.get("/switch-mq.html").text
    assert 'class="mq-topo"' in html and 'id="sw-target"' in html and 'id="sw-stepper"' in html
    assert 'id="sw-ack"' in html and "renderSwitchMq()" in html
    assert 'id="sw-targets"' not in html          # old .sw-opt card list removed
    js = client.get("/assets/web.js").text
    assert "function renderSwitchMq" in js
    assert "api/switch-mq/targets" in js and "setStep" in js and "getElementById('sw-target')" in js
    assert "location.reload()" in js.split("function submitSwitchMq", 1)[1].split("\nfunction ", 1)[0]
    assert "setInterval" not in js
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_page_present -q`
Expected: FAIL — new markers (`mq-topo`/`sw-target`/`sw-stepper`/`setStep`) absent; old `sw-targets` still present.

- [ ] **Step 3: Rewrite `switch-mq.html` body**

Replace the content between `<div class="content doc">` and its closing (the `page-head` + cards) in `milvus-bootstrap/src/milvus_bootstrap/webui/switch-mq.html` with:
```html
      <div class="page-head"><div class="h-l"><h1>切换消息队列</h1>
        <p>把某个 Milvus 实例的 MQ 切换到另一种类型。目标下拉里不可选项已灰置并注明原因。带护栏三步：选目标 → 预演 → 切换。</p></div></div>
      <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
      <div class="card card-pad" style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:16px">
        <span class="muted">切换实例</span>
        <select id="sw-inst" class="f-in" style="min-width:220px"></select>
        <span class="muted" style="margin-left:auto;font-size:12px">目标 MQ 类型不可与当前相同</span>
      </div>
      <div class="card" style="margin-bottom:16px"><div class="card-head"><h3>实例拓扑与切换目标</h3>
        <span class="sub">浅框 = 当前数据流 · 深框 = 切换目标</span></div>
        <div class="card-pad"><div class="mq-topo">
          <div class="box box-mv"><div class="bt"><span class="lo">M</span><div>
            <div class="nm" id="sw-mv-name">—</div><div class="role">向量数据库内核</div></div></div>
            <div class="mvmeta"><span class="badge b-accent" id="sw-mv-mode">—</span></div></div>
          <div class="flow-h"></div>
          <div class="mq-zone">
            <div class="box"><div class="bt"><span class="lo" id="sw-cur-logo">📨</span><div>
              <div class="nm" id="sw-cur-name">—</div><div class="role">当前 MQ · ACTIVE</div></div></div></div>
            <div class="flow-switch"><span class="lbl">切换 ⟶</span></div>
            <div class="box box-dark"><div class="bt"><span class="lo" id="sw-tgt-logo">🎯</span><div>
              <div class="nm" id="sw-tgt-name">选择目标</div><div class="role">目标 MQ</div></div></div>
              <select id="sw-target" class="f-in" style="margin-top:10px;width:100%"></select>
              <div class="r" id="sw-tgt-reason"></div></div>
          </div>
        </div></div></div>
      <div class="callout co-err" style="margin-bottom:16px"><b>WAL 数据不可跨 MQ 迁移。</b>
        切换等价于在空集群上更换后端，存量流式数据无法原地迁移；执行后 MQ 后端固定，不可热回退。
        <label style="display:flex;gap:8px;margin-top:10px;cursor:pointer"><input type="checkbox" id="sw-ack">
          <span>我已知悉：切换后需重建 collection 并重新导入</span></label></div>
      <div class="card card-pad" style="margin-bottom:16px"><div class="stepper" id="sw-stepper">
        <div class="st active" data-s="1"><div class="dot">1</div><div class="tx"><b>选目标 &amp; 确认</b><span>选可切换的目标 MQ 并勾选护栏</span></div><div class="bar"></div></div>
        <div class="st" data-s="2"><div class="dot">2</div><div class="tx"><b>预演</b><span>dry-run 查看切换计划步骤</span></div><div class="bar"></div></div>
        <div class="st" data-s="3"><div class="dot">3</div><div class="tx"><b>切换执行</b><span>改配置 → 切流 → 下线旧 MQ</span></div></div>
      </div></div>
      <div class="card"><div class="card-head"><h3>执行</h3></div><div class="card-pad">
        <button class="btn btn-ghost" id="sw-dry" disabled>预演（dry-run）</button>
        <button class="btn btn-primary" id="sw-go" disabled>切换</button>
        <div id="sw-result" style="margin-top:12px"></div></div></div>
```
(Keep the surrounding shell — `<aside class="rail">`, `<header class="topbar">`, the `<script src="assets/web.js">` and `<script>renderSwitchMq();</script>` — unchanged. Only the inner page body between the `topbar` header and the closing `</div></div></div>` is replaced.)

- [ ] **Step 4: Replace `renderSwitchMq` in `web.js`**

Replace the entire existing `async function renderSwitchMq() { ... }` with:
```javascript
async function renderSwitchMq() {
  shell('switch-mq');
  const params = new URLSearchParams(location.search);
  let selInst = params.get('instance') || '';
  let selectedWal = null;
  const sel = document.getElementById('sw-inst');
  const tgtSel = document.getElementById('sw-target');
  const ack = document.getElementById('sw-ack');
  const dry = document.getElementById('sw-dry');
  const go = document.getElementById('sw-go');
  const res = document.getElementById('sw-result');
  const curLogo = document.getElementById('sw-cur-logo');
  const curName = document.getElementById('sw-cur-name');
  const tgtLogo = document.getElementById('sw-tgt-logo');
  const tgtName = document.getElementById('sw-tgt-name');
  const tgtReason = document.getElementById('sw-tgt-reason');
  const mvName = document.getElementById('sw-mv-name');
  const mvMode = document.getElementById('sw-mv-mode');

  const insts = await loadInstances();
  const milvus = insts.filter(i => i.kind === 'milvus' && i.ownership === 'managed');
  if (!milvus.length) { mvName.textContent = '无 managed milvus 实例'; return; }
  sel.innerHTML = milvus.map(i => `<option value="${esc(i.name)}">${esc(i.name)} (${esc(i.namespace)})</option>`).join('');
  if (!selInst || !milvus.some(i => i.name === selInst)) selInst = milvus[0].name;
  sel.value = selInst;

  const setStep = (n) => {
    document.querySelectorAll('#sw-stepper .st').forEach(st => {
      const s = Number(st.getAttribute('data-s'));
      st.classList.toggle('active', s === n);
      st.classList.toggle('done', s < n);
    });
  };
  const syncButtons = () => {
    dry.disabled = !selectedWal;
    go.disabled = !(selectedWal && ack.checked);
  };
  const advance = () => { setStep(selectedWal && ack.checked ? 2 : 1); syncButtons(); };

  let noteByWal = {};
  const load = async (name) => {
    selectedWal = null; res.innerHTML = ''; ack.checked = false;
    tgtLogo.textContent = '🎯'; tgtName.textContent = '选择目标'; tgtReason.textContent = '';
    setStep(1); syncButtons();
    let d;
    try { d = await getJSON('api/switch-mq/targets?instance=' + encodeURIComponent(name)); }
    catch (e) { mvName.textContent = '加载失败：' + e.message; return; }
    mvName.textContent = name;
    mvMode.textContent = `${d.mode || '—'} · milvus ${d.milvus_version || '—'}`;
    curLogo.textContent = mqLogo(d.current_wal);
    curName.textContent = d.current_mq || '—';
    noteByWal = {};
    const opts = ['<option value="">选择目标…</option>'];
    (d.targets || []).forEach(t => {
      noteByWal[t.wal] = t.note || '';
      const dis = t.selectable ? '' : ' disabled';
      const tail = t.current ? '（当前）' : (t.selectable ? '' : ' · ' + (t.reason || '不可选'));
      opts.push(`<option value="${esc(t.wal)}"${dis}>${esc(t.label)}${esc(tail)}</option>`);
    });
    tgtSel.innerHTML = opts.join('');
  };

  tgtSel.onchange = () => {
    const wal = tgtSel.value;
    selectedWal = wal || null;
    if (wal) {
      tgtLogo.textContent = mqLogo(wal);
      tgtName.textContent = tgtSel.options[tgtSel.selectedIndex].textContent;
      tgtReason.textContent = noteByWal[wal] || '';
    } else {
      tgtLogo.textContent = '🎯'; tgtName.textContent = '选择目标'; tgtReason.textContent = '';
    }
    advance();
  };
  ack.onchange = advance;
  sel.onchange = () => { selInst = sel.value; load(selInst); };
  dry.onclick = () => { if (selectedWal) submitSwitchMq(selInst, selectedWal, true, false, res); };
  go.onclick = () => {
    if (!selectedWal) return;
    if (confirm('确认切换 ' + selInst + ' 的 MQ 到 ' + selectedWal +
                '？这会更改 WAL 并在 pod 内执行变更，存量流式数据将无法保留。')) {
      setStep(3);
      submitSwitchMq(selInst, selectedWal, false, false, res);
    }
  };
  load(selInst);
}
```

- [ ] **Step 5: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS (no count change — updated existing test).

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/switch-mq.html src/milvus_bootstrap/webui/assets/web.js tests/test_web_static.py
git commit -m "feat(webui): switch-mq topology page — MQ link + dropdown target + 3-step stepper"
```

---

## Notes for the executor
- 每个 Task 末尾跑全量 `python -m pytest -q`（基线 212），前端改动后 `node --check`。
- 不改后端；不新增 setInterval（flow 是 CSS 动画）。
- 手动 DoD（合并前真集群一次）：卡「切换 MQ」→ 拓扑页；`Milvus → 当前 MQ ⟶ 目标 MQ` 一眼可见；下拉里 pulsar(当前)/woodpecker 独立服务(需3.0) 灰置带因、kafka 可选；选 kafka → 目标框变 Kafka、箭头流动、stepper 到「② 预演」；勾护栏 → 切换可点；预演出计划；切换二次确认 → stepper「③」+ 流式；`prefers-reduced-motion` 时箭头静止。
