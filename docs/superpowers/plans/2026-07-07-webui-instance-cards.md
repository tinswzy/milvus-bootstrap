# WebUI instance cards (match prototype) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Re-render the Milvus and Dependencies pages as the prototype's rich cards — a per-milvus instance card with a dependency topology (etcd▸core▸store▸MQ + flow connectors), and a per-kind expandable accordion for dependencies — using real data and (for Milvus) the CSS already present in web.css.

**Architecture:** Frontend-only. Rewrite `renderMilvus()` to emit the prototype `.card.inst`/`.topo`/`.box` markup (classes already in web.css). Rewrite `renderDeps()` to emit a `.acc` accordion + a collapse toggle, and port the accordion CSS (`.acc/.acc-head/.img` …) from the prototype into web.css. No backend change.

**Tech Stack:** Vanilla HTML/CSS/JS; pytest content-marker tests.

## Global Constraints

- **Frontend-only.** No backend/endpoint change. Reuse `/api/instances` (has `image`,`status`,`deps{etcd,storage,mq,mq_endpoint}`) + `/api/doctor` (`versions[kind]`).
- **Milvus card = zero new CSS** — the classes `.inst/.inst-head/.topo/.box/.box-mv/.flow-h/.flow-v/.mvdot/.cell-etcd/.cell-store/.cell-mq/.mvmeta/.mv-actions/.b-accent` are ALL already in `web.css`. Emit matching markup.
- **Deps accordion CSS is ported** from prototype `upgrade.html` inline styles into `web.css`.
- **Fidelity A**: full topology row with flow connectors.
- **Deferred actions** (切换MQ/配置/Pods) render as `disabled` placeholder buttons (`title="下一切面"`); **delete** is the only real action (`data-del` → `deleteInstance`).
- **Deps endpoint** is a derived convention string: etcd→`{name}.{ns}.svc:2379`, minio→`{name}.{ns}.svc:80`, kafka→`{name}.{ns}.svc:9092`, pulsar→`{name}-broker.{ns}.svc:6650`.
- **XSS:** every server string via `esc()`. Reuse `shell/esc/getJSON/badge/deleteInstance` — do NOT redefine.
- Tests hermetic. Run from `milvus-bootstrap/` with `source .venv/bin/activate`. `node --check` the JS.
- Branch `feat/webui-instance-cards` off main. End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Verified: `renderMilvus` at web.js:246, `renderDeps` at web.js:271, `DEP_KINDS`/`DEP_LABEL` at web.js:268-269. `deleteInstance(name,onDone)` + `badge(level,text)` + `esc`/`getJSON`/`shell` exist. `--font-display`/`--font-mono`/`--accent`/`--indigo`/`--accent-ink`/`--ok` CSS vars are in web.css (copied hub.css). Existing tests `test_milvus_page_served`/`test_deps_page_served` assert page ids + `renderMilvus`/`renderDeps`/`deleteInstance` in js — the rewrites keep those names.

---

### Task 1: Milvus instance card (prototype topology)

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (rewrite `renderMilvus`; add `depBox`/`mqLogo` helpers)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `/api/instances` rows (`name,namespace,image,status,deps`), `shell/esc/getJSON/badge/deleteInstance`.
- Produces: `renderMilvus()` emitting `.card.inst` topology; `depBox(cls,logo,name,role,id)`, `mqLogo(mq)`.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_milvus_card_topology_markup(client):
    js = client.get("/assets/web.js").text
    assert "renderMilvus" in js
    for marker in ['class="card inst"', 'inst-head', 'class="topo"', 'box box-mv', 'flow-h', 'mv-actions', 'function depBox']:
        assert marker in js, marker
    assert 'disabled title="下一切面"' in js       # deferred action placeholders
```

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_web_static.py -k milvus_card_topology -v`
Expected: FAIL — current `renderMilvus` is the simple chip version (no `.topo`/`depBox`).

- [ ] **Step 3: Replace `renderMilvus` in `web.js`** — replace the entire existing `async function renderMilvus() { ... }` block (web.js:246-266) with:
```javascript
function mqLogo(mq) { return ({ kafka: '🌊', pulsar: '📡', woodpecker: '🪶', rocksmq: '🪨' })[mq] || '📨'; }

function depBox(cls, logo, name, role, id) {
  return `<div class="box ${cls}">` +
    `<div class="bt"><span class="lo">${esc(logo)}</span><div><div class="nm">${esc(name)}</div><div class="role">${esc(role)}</div></div></div>` +
    `<div class="id"><span class="d" style="background:#3fb950"></span>${esc(id || '—')}</div></div>`;
}

async function renderMilvus() {
  shell('milvus');
  const box = document.getElementById('milvus-list');
  try {
    const inst = await getJSON('api/instances');
    const rows = inst.instances.filter(i => i.kind === 'milvus');
    const head = `<div style="margin-bottom:14px"><a class="btn btn-primary btn-sm" href="install.html">+ 新建 Milvus</a></div>`;
    const ph = t => `<button class="btn btn-ghost btn-sm" disabled title="下一切面">${t}</button>`;
    box.innerHTML = head + (rows.length ? rows.map(i => {
      const d = i.deps || {};
      const st = i.status ? badge(i.status === 'Healthy' ? 'PASS' : 'WARN', i.status) : '<span class="muted">健康 —</span>';
      return `<div class="card inst">` +
        `<div class="inst-head"><span class="mvdot">M</span>` +
        `<div><div class="nm">${esc(i.name)}</div><div class="ns">ns: ${esc(i.namespace)} · ${esc(i.image || '—')}</div></div>` +
        `<div class="right">${st}</div></div>` +
        `<div class="topo">` +
          depBox('cell-etcd', '🗄️', 'etcd', '元数据', d.etcd || ('etcd.' + i.namespace + '.svc:2379')) +
          `<div class="flow-h col2"></div>` +
          `<div class="box box-mv">` +
            `<div class="bt"><span class="lo">M</span><div><div class="nm">${esc(i.name)}</div><div class="role">向量数据库内核 · MixCoord</div></div></div>` +
            `<div class="id"><span class="d" style="background:#3fb950"></span>${esc(i.name)} · ${esc(i.image || '—')}</div>` +
            `<div class="mvmeta"><span class="badge b-accent"><span class="d"></span>MQ: ${esc(d.mq || '—')}</span></div>` +
            `<div class="mv-actions">${ph('切换 MQ')}${ph('配置')}${ph('Pods')}<button class="btn btn-ghost btn-sm" data-del="${esc(i.name)}">删除</button></div>` +
          `</div>` +
          `<div class="flow-h col4"></div>` +
          depBox('cell-store', '🪣', '对象存储', 'Object Storage', d.storage) +
          `<div class="flow-v"></div>` +
          depBox('cell-mq', mqLogo(d.mq), d.mq || 'MQ', '消息队列 · WAL', d.mq_endpoint) +
        `</div></div>`;
    }).join('') : '<div class="card"><div class="card-pad muted">暂无 Milvus 实例</div></div>');
    box.querySelectorAll('[data-del]').forEach(b => { b.onclick = () => deleteInstance(b.getAttribute('data-del'), renderMilvus); });
  } catch (e) {
    box.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>';
  }
}
```

- [ ] **Step 4: Verify JS + run test + full suite**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; PASS.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): Milvus instance card with dependency topology (prototype-style)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Dependencies accordion + ported CSS

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (rewrite `renderDeps`; add `DEP_META`/`depEndpoint`)
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css` (append accordion CSS)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `/api/instances`, `/api/doctor`, `shell/esc/getJSON/deleteInstance`.
- Produces: `renderDeps()` emitting `.card.acc` accordions + a collapse toggle; `DEP_META`, `depEndpoint(kind,name,ns)`.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_deps_accordion_markup_and_css(client):
    js = client.get("/assets/web.js").text
    assert "renderDeps" in js
    for marker in ['class="card acc open"', 'acc-head', 'acc-body', 'class="img"', 'function depEndpoint']:
        assert marker in js, marker
    css = client.get("/assets/web.css").text
    for c in ['.acc-head', '.acc-body', '.img', '.acc.open']:
        assert c in css, c
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k deps_accordion -v`
Expected: FAIL — current `renderDeps` uses `.tbl`, and `.acc` CSS not present.

- [ ] **Step 3: Replace `renderDeps` (and its `DEP_KINDS`/`DEP_LABEL` consts) in `web.js`** — replace web.js:268-289 (the `DEP_KINDS`/`DEP_LABEL` consts + the whole `async function renderDeps(){...}`) with:
```javascript
const DEP_KINDS = ['etcd', 'minio', 'kafka', 'pulsar'];
const DEP_META = {
  etcd: { logo: '🗄️', name: 'etcd', role: '元数据' },
  minio: { logo: '🪣', name: 'MinIO', role: '对象存储' },
  kafka: { logo: '🌊', name: 'Kafka', role: '消息队列' },
  pulsar: { logo: '📡', name: 'Pulsar', role: '消息队列' },
};
function depEndpoint(kind, name, ns) {
  return ({
    etcd: `${name}.${ns}.svc:2379`,
    minio: `${name}.${ns}.svc:80`,
    kafka: `${name}.${ns}.svc:9092`,
    pulsar: `${name}-broker.${ns}.svc:6650`,
  })[kind] || `${name}.${ns}.svc`;
}

async function renderDeps() {
  shell('deps');
  const box = document.getElementById('deps-list');
  try {
    const inst = await getJSON('api/instances');
    const doc = await getJSON('api/doctor').catch(() => ({ versions: {} }));
    const versions = doc.versions || {};
    box.innerHTML = DEP_KINDS.map(kind => {
      const meta = DEP_META[kind];
      const rows = inst.instances.filter(i => i.kind === kind);
      const body = rows.length ? rows.map(i =>
        `<div class="dep-row"><span class="nm">${esc(i.name)}</span>` +
        `<span class="muted">ns:${esc(i.namespace)}</span>` +
        `<span class="mono muted">${esc(depEndpoint(kind, i.name, i.namespace))}</span>` +
        `<button class="btn btn-ghost btn-sm" data-del="${esc(i.name)}">删除</button></div>`).join('')
        : '<div class="muted">无实例</div>';
      return `<div class="card acc open">` +
        `<div class="acc-head"><span class="lo">${esc(meta.logo)}</span>` +
        `<div><div class="nm">${esc(meta.name)}</div><div class="sub">${rows.length} 个实例</div></div>` +
        `<div class="right"><span class="img">image: <span class="t">v${esc(versions[kind] || '—')}</span></span>` +
        `<a class="btn btn-ghost btn-sm" href="install.html">+ 新建</a><span class="chev">▾</span></div></div>` +
        `<div class="acc-body">${body}</div></div>`;
    }).join('');
    box.querySelectorAll('.acc-head').forEach(h => {
      h.onclick = e => { if (e.target.closest('a,button')) return; h.parentElement.classList.toggle('open'); };
    });
    box.querySelectorAll('[data-del]').forEach(b => { b.onclick = () => deleteInstance(b.getAttribute('data-del'), renderDeps); });
  } catch (e) {
    box.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>';
  }
}
```

- [ ] **Step 4: Append the accordion CSS to `web.css`** (at end):
```css
/* --- dependencies accordion (ported from prototype upgrade.html) --- */
.acc { margin-bottom:14px; overflow:hidden; border-color:var(--line-2); transition:box-shadow .18s, border-color .18s; }
.acc:hover { box-shadow:var(--shadow); border-color:var(--line-strong); }
.acc-head { display:flex; align-items:center; gap:14px; padding:15px 20px 15px 22px; cursor:pointer; transition:background .14s; user-select:none; position:relative; }
.acc-head:hover { background:var(--surface-2); }
.acc-head .lo { width:40px;height:40px;border-radius:11px; display:grid;place-items:center; font-size:19px; background:var(--surface-3); border:1px solid var(--line); flex:none; box-shadow:var(--shadow-sm); transition:.16s; }
.acc-head .nm { font-family:var(--font-display); font-weight:700; font-size:15.5px; letter-spacing:-.1px; }
.acc-head .sub { font-size:11px; color:var(--fg-3); margin-top:2px; font-family:var(--font-mono); }
.acc-head .right { margin-left:auto; display:flex; align-items:center; gap:10px; }
.acc-head .chev { transition:transform .22s; color:var(--fg-3); }
.acc.open .acc-head .chev { transform:rotate(180deg); }
.acc.open .acc-head { border-bottom:1px solid var(--line); background:linear-gradient(90deg, rgba(8,180,200,.09), rgba(8,180,200,0) 46%), var(--surface); }
.acc.open .acc-head::before { content:""; position:absolute; left:0; top:0; bottom:0; width:3px; background:linear-gradient(180deg,var(--accent),var(--indigo)); }
.acc.open .acc-head .lo { border-color:#bfeef4; box-shadow:0 5px 14px rgba(8,180,200,.2); }
.img { font-family:var(--font-mono); font-size:11.5px; color:var(--fg-2); background:var(--surface-2); border:1px solid var(--line); border-radius:6px; padding:3px 8px; display:inline-block; }
.img .t { color:var(--accent-ink); font-weight:600; }
.acc-body { padding:8px 20px 16px; }
.acc:not(.open) .acc-body { display:none; }
.dep-row { display:flex; align-items:center; gap:14px; padding:8px 2px; border-bottom:1px solid var(--line); }
.dep-row:last-child { border-bottom:none; }
.dep-row .nm { font-weight:600; min-width:120px; }
.dep-row .mono { margin-left:auto; }
```

- [ ] **Step 5: Verify JS + run test + full suite**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; PASS.

- [ ] **Step 6: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): Dependencies accordion (prototype-style) + ported CSS

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`mb web --port 8090` (live cluster), open `http://127.0.0.1:8090/`:
- **Milvus 页**: each instance is a topology card — M dot + name + ns/image + Healthy badge; a row of etcd▸[Milvus core box: name/kernel/image/MQ chip/actions]▸store▸MQ boxes with connector arrows; 切换MQ/配置/Pods are greyed (disabled), 删除 works.
- **Dependencies 页**: each of etcd/minio/kafka/pulsar is an accordion — logo + name + `image: v<version>` chip + chevron; click header toggles collapse; body lists instances with derived endpoint + 删除; 新建 links to install.
- Responsive: at narrow width the topo collapses to a single column (web.css media query already handles `.topo`/`.flow-*`).

## Self-Review

- **Spec coverage:** D1 topology fidelity A → Task 1 (`.topo` + flow connectors); D2 deferred-action placeholders + real delete → Task 1 (`ph()` disabled buttons, `data-del`); D3 accordion + toggle → Task 2; D4 reuse endpoints → both (no backend); D5 milvus reuses CSS / deps ports CSS → Task 1 (no CSS) + Task 2 (CSS block). §4 card structure → Task 1. §5 accordion + endpoint derivation → Task 2. §6 CSS port → Task 2. §7 content-marker tests → both.
- **Placeholder scan:** every step has complete code; frontend verified by content-marker + manual DoD (no JS harness — stated). No TBD/TODO.
- **Type consistency:** `depBox(cls,logo,name,role,id)` defined+used in Task 1; `depEndpoint(kind,name,ns)`/`DEP_META` defined+used in Task 2; reuses `shell/esc/getJSON/badge/deleteInstance`; markup classes (`.inst/.topo/.box-mv/.acc/.acc-body/.img/.dep-row`) match the CSS (existing for milvus, added in Task 2 for deps). `renderMilvus`/`renderDeps` names preserved so existing page tests still pass.
