# WebUI Switch-MQ 三角布局 + 分组下拉 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** switch-mq 页拓扑改成三角分叉（Milvus 上、当前 MQ 左下、目标 MQ 右下），目标下拉按 MQ 类型 optgroup 分组、组下列已部署实例名、整类不可选注明原因。

**Architecture:** UI/只读增强。后端 `switch_mq_targets` 加 `embedded`、`/api/switch-mq/targets` 加 `instances`（含 endpoint，备 ③ 用）；前端三角布局 CSS/标记 + `renderSwitchMq` 分组下拉。切换仍按类型（`submitSwitchMq` 不动）。

**Tech Stack:** Python + FastAPI + pytest；vanilla HTML/CSS/JS；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-15-switch-mq-triangle-grouped-design.md`（决策 D1–D8）。
- **仅 UI/只读**：切换仍 `submitSwitchMq(inst, selectedWal, …)` 按类型；**不改** `submitSwitchMq`/`POST /api/switch-mq`/`plan_switch_mq_steps`。「真重指端点注入」是 ③（下一切面），本切面只把 `instances[].endpoint` + `data-inst/data-ns` 备好。
- **分组下拉**：`<optgroup>` 按 MQ 类型；外部型（有 dep_kind：kafka/pulsar/woodpecker）列 state 中该 kind 的实例；嵌入型（dep_kind None：rocksmq/woodpecker-embedded）一条「（嵌入，无独立实例）」；整类 `!selectable` → `<optgroup disabled>` + label 带 reason；外部型无实例 → 组内一条 disabled「（无可复用实例，需先安装）」。
- **护栏/门禁/流式全保留**；**无 setInterval**（动画是 CSS，reduced-motion 守 dashed）。
- 新增/改的 `/api/*` 无新增路由（只改 `api_switch_mq_targets`）；无新 pip 依赖。
- 命令在 `milvus-bootstrap/` 下：`cd milvus-bootstrap && source .venv/bin/activate`。基线 213 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用 `user.name=tinswzy`。

---

### Task 1: 后端 — `switch_mq_targets` 加 `embedded` + 端点加 `instances`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.py`（`switch_mq_targets` 输出加 `embedded`）
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（`api_switch_mq_targets` 加 `_dep_endpoint` + `instances`）
- Test: `milvus-bootstrap/tests/test_compat.py`、`milvus-bootstrap/tests/test_web_switchmq.py`（各追加）

**Interfaces:**
- Consumes: `compat.switch_mq_targets` (existing), `core.state.list_instances()`.
- Produces: each target dict gains `embedded: bool`; the endpoint response's each target gains `instances: [{name, namespace, endpoint}]`.

- [ ] **Step 1: Write the failing compat test**

Add to `milvus-bootstrap/tests/test_compat.py`:
```python
def test_switch_mq_targets_embedded_flag():
    from milvus_bootstrap.core import compat
    ts = {t["id"]: t for t in compat.switch_mq_targets("pulsar", "2.6.0", "standalone")}
    assert ts["kafka"]["embedded"] is False and ts["pulsar"]["embedded"] is False
    assert ts["rocksmq"]["embedded"] is True and ts["woodpecker-embedded"]["embedded"] is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_compat.py::test_switch_mq_targets_embedded_flag -q`
Expected: FAIL — `embedded` key absent.

- [ ] **Step 3: Add `embedded` to `switch_mq_targets`**

In `milvus-bootstrap/src/milvus_bootstrap/core/compat.py`, the `out.append({...})` in `switch_mq_targets` currently is:
```python
        out.append({"id": o["id"], "wal": o["wal"], "label": o["label"], "dep_kind": o["dep_kind"],
                    "note": o["note"], "current": o["wal"] == current_wal,
                    "selectable": selectable, "reason": reason})
```
Add the `embedded` field:
```python
        out.append({"id": o["id"], "wal": o["wal"], "label": o["label"], "dep_kind": o["dep_kind"],
                    "note": o["note"], "current": o["wal"] == current_wal,
                    "embedded": o["dep_kind"] is None,
                    "selectable": selectable, "reason": reason})
```

- [ ] **Step 4: Run compat test to verify pass**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_compat.py::test_switch_mq_targets_embedded_flag -q`
Expected: PASS.

- [ ] **Step 5: Write the failing endpoint test**

Add to `milvus-bootstrap/tests/test_web_switchmq.py`:
```python
def test_api_switch_mq_targets_lists_instances(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="kafka", name="kafka-dev"), dry_run=False)
        _core().install(InstallSpec(kind="milvus", name="sw-mv",
                                    params={"mq": "pulsar", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.get("/api/switch-mq/targets", params={"instance": "sw-mv"})
        assert r.status_code == 200
        ts = {t["id"]: t for t in r.json()["targets"]}
        assert ts["kafka"]["embedded"] is False
        names = [x["name"] for x in ts["kafka"]["instances"]]
        assert "kafka-dev" in names
        ep = [x["endpoint"] for x in ts["kafka"]["instances"] if x["name"] == "kafka-dev"][0]
        assert ep.startswith("kafka-dev.") and ":9092" in ep
        assert ts["rocksmq"]["embedded"] is True and ts["rocksmq"]["instances"] == []
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py::test_api_switch_mq_targets_lists_instances -q`
Expected: FAIL — `instances` key absent from targets.

- [ ] **Step 7: Add `instances` enrichment to `api_switch_mq_targets`**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, the `api_switch_mq_targets` currently ends with:
```python
    targets = compat.switch_mq_targets(current_wal, version, mode, operator_version="")  # op_ver reserved
    return {"instance": instance, "current_mq": cur_mq, "current_wal": current_wal,
            "milvus_version": version, "mode": mode, "targets": targets}
```
Replace those lines with (enrich each target with deployed instances of its dep_kind):
```python
    targets = compat.switch_mq_targets(current_wal, version, mode, operator_version="")  # op_ver reserved

    def _dep_endpoint(kind, dep_name, dep_ns):
        return {"kafka": f"{dep_name}.{dep_ns}.svc:9092",
                "pulsar": f"{dep_name}-broker.{dep_ns}.svc:6650",
                "woodpecker": f"{dep_name}.{dep_ns}.svc:9000"}.get(kind, f"{dep_name}.{dep_ns}.svc")

    by_kind: dict[str, list] = {}
    for si in _core().state.list_instances():
        k = (si.spec_snapshot or {}).get("kind", "")
        by_kind.setdefault(k, []).append(si)
    for t in targets:
        dep = t.get("dep_kind")
        t["instances"] = ([] if not dep else
                          [{"name": si.name, "namespace": si.namespace,
                            "endpoint": _dep_endpoint(dep, si.name, si.namespace)}
                           for si in by_kind.get(dep, [])])
    return {"instance": instance, "current_mq": cur_mq, "current_wal": current_wal,
            "milvus_version": version, "mode": mode, "targets": targets}
```

- [ ] **Step 8: Run tests + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py tests/test_compat.py -q && python -m pytest -q`
Expected: all PASS (full suite was 213, +2).

- [ ] **Step 9: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/compat.py src/milvus_bootstrap/server/app.py tests/test_compat.py tests/test_web_switchmq.py
git commit -m "feat(server): switch-mq targets — embedded flag + per-category deployed instances (with endpoint for ③)"
```

---

### Task 2: 前端 — 三角布局（Milvus 上 / 当前左下 / 目标右下）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/switch-mq.html`（拓扑区改三角标记）
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`（`.sw-fork*` 换成 `.sw-tri*`）
- Test: `milvus-bootstrap/tests/test_web_static.py`（更新 `test_switch_mq_topology_css`、`test_switch_mq_page_present`）

**Interfaces:**
- Consumes: existing element ids (`sw-mv-name`/`sw-mv-mode`/`sw-cur-logo`/`sw-cur-name`/`sw-tgt-logo`/`sw-tgt-name`/`sw-target`/`sw-tgt-reason`) — all preserved so `renderSwitchMq` keeps working.
- Produces: triangle layout markup (`.sw-tri`) + CSS; retires `.sw-fork`/`.sw-conn`/`.sw-block`/`.sw-line*`.

- [ ] **Step 1: Update the failing tests**

In `milvus-bootstrap/tests/test_web_static.py`, replace `test_switch_mq_topology_css` with:
```python
def test_switch_mq_topology_css(client):
    css = client.get("/assets/web.css").text
    assert ".sw-tri" in css and ".box-dark" in css and ".tri-hub" in css
    assert ".tri-arm.l" in css and ".tri-arm.r" in css   # solid=current(left), dashed=target(right)
    assert "prefers-reduced-motion" in css        # animation guarded
```
And in `test_switch_mq_page_present`, change the topology assertion line from
`    assert 'class="sw-fork"' in html and 'id="sw-target"' in html and 'id="sw-stepper"' in html`
to
```python
    assert 'class="sw-tri"' in html and 'id="sw-target"' in html and 'id="sw-stepper"' in html
```
and change `    assert 'sw-block' in html and 'id="sw-ack"' in html and "renderSwitchMq()" in html` to
```python
    assert 'tri-hub' in html and 'id="sw-ack"' in html and "renderSwitchMq()" in html
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_topology_css tests/test_web_static.py::test_switch_mq_page_present -q`
Expected: FAIL — `.sw-tri`/`.tri-hub` absent; html still `.sw-fork`.

- [ ] **Step 3: Rewrite the topology markup in `switch-mq.html`**

Replace the existing `.sw-fork` block (from `<div class="card-pad"><div class="sw-fork">` through its matching close before the WAL warning callout) with:
```html
        <div class="card-pad"><div class="sw-tri">
          <div class="box box-mv sw-mv"><div class="bt"><span class="lo">M</span><div>
            <div class="nm" id="sw-mv-name">—</div><div class="role">向量数据库内核</div></div></div>
            <div class="mvmeta"><span class="badge b-accent" id="sw-mv-mode">—</span></div></div>
          <div class="tri-v"></div>
          <div class="tri-fork"><span class="tri-hub">⇄ 切换</span>
            <i class="tri-bus"></i><i class="tri-arm l" data-lbl="当前"></i><i class="tri-arm r" data-lbl="目标"></i></div>
          <div class="box sw-cur"><div class="bt"><span class="lo" id="sw-cur-logo">📨</span><div>
            <div class="nm" id="sw-cur-name">—</div><div class="role">当前 MQ · ACTIVE</div></div></div></div>
          <div class="box box-dark sw-tgt"><div class="bt"><span class="lo" id="sw-tgt-logo">🎯</span><div>
            <div class="nm" id="sw-tgt-name">选择目标</div><div class="role">目标 MQ</div></div></div>
            <select id="sw-target" class="f-in" style="margin-top:10px;width:100%"></select>
            <div class="r" id="sw-tgt-reason"></div></div>
        </div></div></div>
```

- [ ] **Step 4: Replace the switch CSS in `web.css`**

Replace the entire `.sw-fork` … block (the switch topology CSS added last slice — from the `/* switch-mq fork topology ... */` comment through its `@media (prefers-reduced-motion...)` line) with:
```css
/* switch-mq triangle topology: Milvus(top) → [⇄切换] → solid↙当前 / dashed↘目标 */
.sw-tri { display:grid; grid-template-columns:1fr 1fr; justify-items:center; align-items:start; }
.sw-tri .sw-mv { grid-row:1; grid-column:1 / 3; }
.tri-v { grid-row:2; grid-column:1 / 3; width:2px; height:22px; background:var(--accent); }
.tri-fork { grid-row:3; grid-column:1 / 3; position:relative; width:100%; height:36px; }
.tri-hub { position:absolute; top:6px; left:50%; transform:translateX(-50%); z-index:2; white-space:nowrap;
  font-size:11px; font-weight:600; color:var(--warn); background:var(--surface-2);
  border:1.5px solid var(--warn); border-radius:8px; padding:2px 9px; }
.tri-bus { position:absolute; top:18px; left:25%; right:25%; height:2px; background:var(--line-2); }
.tri-arm { position:absolute; top:18px; width:2px; height:18px; }
.tri-arm.l { left:25%; background:var(--accent); }
.tri-arm.r { left:75%; background-image:linear-gradient(180deg,var(--warn) 55%,transparent 0);
  background-size:2px 9px; background-repeat:repeat-y; animation:swflowv .8s linear infinite; }
.tri-arm::after { content:''; position:absolute; bottom:-2px; left:-4px;
  border-left:5px solid transparent; border-right:5px solid transparent; }
.tri-arm.l::after { border-top:7px solid var(--accent); } .tri-arm.r::after { border-top:7px solid var(--warn); }
.tri-arm::before { content:attr(data-lbl); position:absolute; top:-1px; left:8px; font-size:10px; font-weight:600; white-space:nowrap; }
.tri-arm.l::before { color:var(--accent); } .tri-arm.r::before { color:var(--warn); }
.box.box-dark { background:var(--surface-3); border:1.5px solid var(--accent); min-width:220px; cursor:default; }
.box.box-dark:hover { transform:none; box-shadow:none; }
.box-dark .r { font-size:11.5px; color:var(--muted); margin-top:6px; }
.sw-cur { grid-row:4; grid-column:1; align-self:start; justify-self:center; }
.sw-tgt { grid-row:4; grid-column:2; align-self:start; justify-self:center; }
@keyframes swflowv { to { background-position:0 9px; } }
@media (prefers-reduced-motion: reduce) { .tri-arm.r, .flow-h, .flow-v { animation:none; } }
```

- [ ] **Step 5: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/switch-mq.html src/milvus_bootstrap/webui/assets/web.css tests/test_web_static.py
git commit -m "feat(webui): switch-mq triangle layout — Milvus top, current bottom-left, target bottom-right"
```

---

### Task 3: 前端 — 分组下拉（optgroup 列实例 / 嵌入 / 整类灰置）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（`renderSwitchMq` 的下拉构建 + onchange）
- Test: `milvus-bootstrap/tests/test_web_static.py`（追加）

**Interfaces:**
- Consumes: `/api/switch-mq/targets` targets now carrying `embedded` + `instances` (Task 1).
- Produces: `renderSwitchMq` builds `#sw-target` as `<optgroup>`s; onchange reads `value`(wal) + `data-inst`/`data-ns`. `submitSwitchMq` calls unchanged (by-type).

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_switch_mq_grouped_dropdown(client):
    js = client.get("/assets/web.js").text
    body = js.split("async function renderSwitchMq", 1)[1].split("\nasync function ", 1)[0]
    assert "optgroup" in body and "data-inst" in body           # grouped by type, instances carried
    assert "无可复用实例" in body and "嵌入，无独立实例" in body   # empty-external + embedded copy
    assert "setInterval" not in js
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_grouped_dropdown -q`
Expected: FAIL — no `optgroup`/`data-inst` in renderSwitchMq.

- [ ] **Step 3: Replace the dropdown build + onchange in `renderSwitchMq`**

In `web.js`, the current dropdown build in `load()` is:
```javascript
    const opts = ['<option value="">选择目标…</option>'];
    (d.targets || []).forEach(t => {
      noteByWal[t.wal] = t.note || '';
      const dis = t.selectable ? '' : ' disabled';
      const tail = t.current ? '（当前）' : (t.selectable ? '' : ' · ' + (t.reason || '不可选'));
      opts.push(`<option value="${esc(t.wal)}"${dis}>${esc(t.label)}${esc(tail)}</option>`);
    });
    tgtSel.innerHTML = opts.join('');
```
Replace it with an `<optgroup>` build:
```javascript
    const opts = ['<option value="">选择目标…</option>'];
    (d.targets || []).forEach(t => {
      noteByWal[t.wal] = t.note || '';
      const glabel = t.selectable ? esc(t.label) : `${esc(t.label)} · ${esc(t.reason || '不可选')}`;
      const gdis = t.selectable ? '' : ' disabled';
      let inner;
      if (t.embedded) {
        inner = `<option value="${esc(t.wal)}" data-inst="" data-ns="">（嵌入，无独立实例）</option>`;
      } else if ((t.instances || []).length) {
        inner = t.instances.map(x =>
          `<option value="${esc(t.wal)}" data-inst="${esc(x.name)}" data-ns="${esc(x.namespace)}">` +
          `${esc(x.name)} (${esc(x.namespace)})</option>`).join('');
      } else {
        inner = '<option disabled>（无可复用实例，需先安装）</option>';
      }
      opts.push(`<optgroup label="${glabel}"${gdis}>${inner}</optgroup>`);
    });
    tgtSel.innerHTML = opts.join('');
```
Then update `tgtSel.onchange` — the current version is:
```javascript
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
```
Replace it with a version that also captures the chosen instance (for display now, for ③ later):
```javascript
  tgtSel.onchange = () => {
    const wal = tgtSel.value;
    selectedWal = wal || null;
    if (wal) {
      const opt = tgtSel.options[tgtSel.selectedIndex];
      const inst = opt.getAttribute('data-inst') || '';
      tgtLogo.textContent = mqLogo(wal);
      tgtName.textContent = inst ? `${wal} · ${inst}` : wal;
      tgtReason.textContent = noteByWal[wal] || '';
    } else {
      tgtLogo.textContent = '🎯'; tgtName.textContent = '选择目标'; tgtReason.textContent = '';
    }
    advance();
  };
```
(`submitSwitchMq(selInst, selectedWal, …)` calls in `#sw-dry`/`#sw-go` stay unchanged — still by-type; the captured instance is display-only until ③.)

- [ ] **Step 4: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js tests/test_web_static.py
git commit -m "feat(webui): switch-mq grouped dropdown — optgroup per MQ type, lists deployed instances, whole-category disabled+reason"
```

---

## Notes for the executor
- 每个 Task 末尾跑全量 `python -m pytest -q`（基线 213），前端改动后 `node --check`。
- 切换仍按类型（不改 submitSwitchMq）；无 setInterval。
- 手动 DoD（合并前真集群一次）：卡「切换 MQ」→ 三角图（Milvus 上、当前 pulsar 左下、目标右下）；目标下拉分组：`Kafka` 组下 `kafka-dev (default)`；`Pulsar · 与当前相同` 组灰；`Woodpecker（独立服务）· 需 milvus≥3.0` 组灰；`RocksMQ（嵌入）` 组下「（嵌入…）」；选 kafka-dev → 目标框显示 `kafka · kafka-dev`；勾护栏 → 预演/切换（按类型）。
