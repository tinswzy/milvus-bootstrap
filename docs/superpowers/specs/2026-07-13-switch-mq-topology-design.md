# WebUI Switch-MQ 页面拓扑化 + 步骤引导 · 设计

- 日期：2026-07-13
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：把已上线的 `switch-mq.html` 从「下拉+卡片列表」改成 **prototype 风格的拓扑交互**——一条 `Milvus → 当前 MQ ⟶ 目标 MQ` 的链路图（目标框内嵌**下拉**选择），加一个 **3 步引导 stepper** + 各步提示/提醒。**纯前端**：后端 `switch_mq_targets`/`/api/switch-mq`/`submitSwitchMq` 及三层护栏全部不动。

## 1. 背景与现状

- 现状 `switch-mq.html` + `renderSwitchMq`（上一切面）：实例下拉 + 当前 MQ 文本 + `.sw-opt` 卡片列表（灰置带因）+ 护栏勾选 + 预演/切换（`submitSwitchMq` 流式）。功能全，但不够直观。
- 用户诉求：像 `prototype/switch-mq.html` 那样——**拓扑一眼看清在操作哪条 MQ 链路**；目标用**下拉**（简单、不多填空）；**不可选灰置并提示**；切换时有**步骤提示/提醒**。
- **复用（已在 web.css）**：`.topo`/`.box`/`.box-mv`/`.flow-h`/`.flow-v`/`.bt`/`.lo`/`.nm`/`.role`/`.id`/`.mvmeta`（拓扑，386-417）；`.stepper`/`.st`/`.dot`/`.bar`/`.st.active`/`.st.done`（步骤条，273-282）。只缺 switch 专属：`.box-dark`（目标深框）、`.flow-switch`（动画虚线箭头）、`.mq-zone`（当前⟶目标 布局）。
- **数据源不变**：`GET /api/switch-mq/targets?instance=` 已返回 `{current_mq,current_wal,milvus_version,mode,targets:[{id,wal,label,dep_kind,note,current,selectable,reason}]}`。`submitSwitchMq(name,wal,dryRun,force,el)` 复用（200→logPanel / 202→pollTask 流式 / 409→门禁 force / 刷新钮 `location.reload()`）。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 拓扑 | 聚焦 MQ 链路：`[Milvus 实例 .box-mv] —flow→ [当前 MQ · ACTIVE 浅框] ⟶ [目标 MQ 深框 .box-dark]`。简单明了，不带 etcd/存储 |
| D2 | 目标选择 | **下拉** `<select id="sw-target">` 内嵌在目标深框里；可选项正常，**不可选项 `disabled` 且标签内嵌原因**（`Pulsar · 与当前相同`、`Woodpecker 独立服务 · 需 milvus≥3.0` 等）。选中→目标框 logo/名字实时更新 |
| D3 | 流动箭头 | `.flow-switch`：当前↔目标间**动画虚线箭头**（CSS `@keyframes` 流动 + `切换 ⟶` 标签）；**`prefers-reduced-motion` 守卫**关动效。非 JS 定时器 |
| D4 | 步骤引导 | 复用 `.stepper` 3 步：① 选目标 & 确认（选可切 MQ + 勾护栏）② 预演（dry-run 计划）③ 切换执行（apply 流式）。随操作点亮 `active`/`done` |
| D5 | 提示/提醒 | 每步副标题提示；「WAL 不可迁移」红 callout + 「执行后不可热回退」保留；执行时 logPanel 即真实步骤（precheck/wal-alter/verify/decommission）|
| D6 | 护栏不变 | 三层：不可选项 disabled 不能选 / 切换钮需选中+勾 `#sw-ack` / `confirm` 二次确认 |
| D7 | 无轮询 | 进页/换实例/换目标读一次；flow 是 CSS 动画；**无 setInterval** |
| D8 | 非目标 | 不改后端；不做「装新目标 MQ」多步编排/PVC；不引入新依赖；下拉不做自定义填空 |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `webui/switch-mq.html` | 重写正文：实例选择 + 拓扑卡（milvus/当前MQ/目标MQ下拉）+ stepper + 护栏 + 执行区。移除旧 `#sw-targets` 卡片列表 |
| `webui/assets/web.js` `renderSwitchMq` | 重写：渲染拓扑、构建下拉（disabled+reason）、目标框联动、stepper 推进、护栏、复用 submitSwitchMq |
| `webui/assets/web.css` | 加 `.box-dark`/`.flow-switch`/`@keyframes swflow`/`.mq-zone`/`.box-dark select` |

## 4. 前端

### 4.1 `switch-mq.html` 正文（复用 shell）
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
        <select id="sw-target" style="margin-top:10px;width:100%"></select>
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
`<script>renderSwitchMq();</script>` 不变。

### 4.2 `renderSwitchMq` 重写
- `shell('switch-mq')`；`?instance=` 预选；`loadInstances()` 过滤 managed milvus 填 `#sw-inst`（无→提示）。
- `mqLogo(wal)`：复用现有 `mqLogo`（kafka🌊/pulsar📡/woodpecker🪶/rocksmq🪨/default📨）给当前/目标框 logo。
- `setStep(n)`：给 `#sw-stepper .st` 按 `data-s` 设 `active`（==n）/`done`（<n）/无（>n）。
- `load(name)`：`getJSON('api/switch-mq/targets?instance=')` →
  - `#sw-mv-name`=name，`#sw-mv-mode`=`d.mode·milvus d.milvus_version`；`#sw-cur-logo`=mqLogo(current_wal)、`#sw-cur-name`=current_mq。
  - 构建 `#sw-target` 下拉：首项 `<option value="">选择目标…</option>`；**每个 target 各一** `<option value="<wal>" [disabled if !selectable]>label[ · reason if !selectable][（当前）if current]</option>`（全 `esc`；不去重——woodpecker 嵌入/独立服务同 wal 但一可选一预留，都展示更清晰；`value=wal` 提交无歧义）。
  - 重置：`selectedWal=null`、`#sw-ack.checked=false`、目标框回「选择目标」、`#sw-tgt-reason`空、`setStep(1)`、`syncButtons()`、`#sw-result`空。
- `#sw-target` onchange：取选中 option；`selectedWal=value||null`；更新目标框 `#sw-tgt-logo`=mqLogo(wal)、`#sw-tgt-name`=label；`#sw-tgt-reason` 显示该 wal 的 note（可选）；`advance()`。
- `#sw-ack` onchange → `advance()`。
- `advance()`：`selectedWal && ack.checked` ? `setStep(2)` : `setStep(1)`；`syncButtons()`。
- `syncButtons()`：`#sw-dry.disabled=!selectedWal`；`#sw-go.disabled=!(selectedWal && ack.checked)`。
- `#sw-dry` → `if(selectedWal) submitSwitchMq(inst, selectedWal, true, false, #sw-result)`（预演，不改 step）。
- `#sw-go` → `confirm('确认切换 '+inst+' 的 MQ 到 '+selectedWal+'？…存量流式数据将无法保留。')` → `setStep(3); submitSwitchMq(inst, selectedWal, false, false, #sw-result)`。
- `#sw-inst` onchange → `load(sel.value)`。
- 全部 task 派生串 `esc`；无 setInterval。

### 4.3 CSS（switch 专属）
```css
.mq-topo { display:flex; align-items:center; gap:14px; flex-wrap:wrap; }
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
@media (prefers-reduced-motion: reduce) { .flow-switch { animation:none; } }
```

## 5. 测试与验收
- **前端 content-marker**（`tests/test_web_static.py`）：`switch-mq.html` 含 `class="mq-topo"`/`id="sw-target"`/`id="sw-stepper"`/`id="sw-ack"`/`box-dark`/`flow-switch`；不再有 `id="sw-targets"`（旧卡片列表移除）。web.js `renderSwitchMq` 段含 `sw-target`/`setStep`/`disabled`（构建 disabled option）/`api/switch-mq/targets`；`function renderSwitchMq` 存在；`setInterval` 不存在。css 含 `.flow-switch`/`.box-dark`/`@keyframes swflow`。更新旧 `test_switch_mq_page_present`（`sw-targets`→`sw-target`/拓扑标记）。
- **JS**：`node --check`。
- **手动 DoD**（真集群）：卡「切换 MQ」→ 拓扑页；`Milvus → 当前 MQ ⟶ 目标 MQ` 一眼可见；下拉里 pulsar(当前)/woodpecker 独立服务(需3.0) 灰置带因、kafka 可选；选 kafka → 目标框变 Kafka、箭头流动、stepper 到「② 预演」；勾护栏 → 「切换」可点；预演出计划；切换二次确认 → stepper「③ 切换执行」+ 流式；`prefers-reduced-motion` 时箭头静止。

## 6. 非目标 / 后续
- 「装新目标 MQ」编排 / PVC 清理（仍预留）。
- woodpecker-service 真正切换（仍预留）。
- 完整拓扑（etcd/存储）——本切面聚焦 MQ 链路。
