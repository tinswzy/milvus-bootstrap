# WebUI Switch-MQ 独立页面（兼容表驱动的目标选择）· 设计

- 日期：2026-07-13
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：把「切换 MQ」从卡片模态升级为**独立页面**（prototype 风格），核心是**兼容表驱动的目标 MQ 选择**——不可选项**灰置并显示原因**（同类/standalone-only/版本/预留）。**先把已有 MQ（kafka/pulsar/woodpecker-embedded/rocksmq）切换支持好；woodpecker 独立服务的判定逻辑预留、暂不实现切换。**

## 1. 背景与现状

- 现状：Milvus 卡「切换 MQ」按钮开模态 `openSwitchMq`（下拉 + 预演/切换 + 门禁/force + 二次确认）。功能对，但对这类**较重、不可逆**的操作，模态太简。
- prototype `prototype/switch-mq.html` 是三步向导（校验→准备目标→切流下线旧 MQ）+ 拓扑图 + 「WAL 数据不可迁移」护栏。其中「准备目标/切流/清 PVC」多步是**未来**编排；本切面**先做能用的单页**，把多步**逻辑预留**。
- 复用：`compat.mq_options(version, mode)`（返回每 MQ `{id,wal,label,dep_kind,supported,reason,note}`，已判 min_milvus + standalone_only）；`/api/switch-mq`（预演 200 / 门禁 409 / apply 202 流式）；前端 `submitSwitchMq`/`logPanel`/`pollTask`；`probe.detect_versions().operator`（operator 版本，可探测）。
- 现有 MQ 语义（`compat.MQ_OPTIONS`）：`woodpecker-embedded`(wal=woodpecker,min2.6)、`woodpecker-service`(wal=woodpecker,min3.0,dep=woodpecker)、`kafka`(min2.0)、`pulsar`(min2.0)、`rocksmq`(min2.0,standalone_only)。switch gate（`compat.gate("switch-mq")`）仅拦「同 wal」。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 页面 | 独立页 `switch-mq.html` + `renderSwitchMq()`；卡「切换 MQ」按钮**跳转** `switch-mq.html?instance=X`；退休模态 `openSwitchMq`（保留 `submitSwitchMq`）|
| D2 | 目标可选性（核心）| 新 `compat.switch_mq_targets(current_wal, milvus_version, mode, operator_version)`：每项 `{id,wal,label,dep_kind,note,current,selectable,reason}`。不可选→前端灰置 + 显示 reason |
| D3 | 规则 | ① `wal==current_wal`→不可选「与当前 MQ 相同」(kafka→kafka)；② `standalone_only`(rocksmq)+`mode!=standalone`→「仅 standalone，cluster 不可切 rocksmq」；③ `min_milvus>当前`→「需 milvus≥X」(2.x→woodpecker-service〔min3.0〕不可)；④ `woodpecker-service`→**预留关闭**「暂不支持切到 Woodpecker 独立服务（需 milvus≥3.0 且 milvus-operator 支持 external woodpecker，规划中）」|
| D4 | 预留钩子 | `_operator_supports_ext_woodpecker(operator_version)->bool` 现恒 False；`operator_version` 参数**预留**（端点暂传 ""，不为它做重探测）|
| D5 | 护栏 | prototype 那条「WAL 数据不可迁移」警示 + **必勾确认框** + 切换时 `confirm` 二次确认（三重护栏）|
| D6 | 执行 | 复用 `/api/switch-mq`：[预演]→dry-run 200→`logPanel`；[切换]→勾选+确认→`submitSwitchMq`（202 流式 + 门禁 409/force + 诚实收口）|
| D7 | 无轮询 | 进页/换实例读一次 targets；apply 流式（pollTask 有界）；无 setInterval |
| D8 | 非目标 | 不做「安装/准备新目标 MQ」多步编排（假定目标 MQ 服务已就位，同现 `switch_mq`）；不做 PVC 清理；**不实现** woodpecker-service 切换（仅判定预留）；不改 `/api/switch-mq` 逻辑 |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/compat.py` | 新 `_operator_supports_ext_woodpecker`、`switch_mq_targets` |
| `server/app.py` | 新 `GET /api/switch-mq/targets`（注册在 `app.mount` 之前）|
| `webui/switch-mq.html` | 新页面 |
| `webui/assets/web.js` | 新 `renderSwitchMq()`；卡 `[data-switch]` 改跳转；退休 `openSwitchMq`（`submitSwitchMq` 保留）；`shell` crumb 加 switch-mq |
| `webui/assets/web.css` | `.sw-opt`（目标卡：`.sel`/`.dis`/`.cur`）|

## 4. 后端

### 4.1 `compat.switch_mq_targets`
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

### 4.2 `GET /api/switch-mq/targets?instance=`（注册在 `app.mount` 之前）
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
（`operator_version` 现传 ""——woodpecker-service 无论如何预留关闭，避免为其做重量级 `detect_versions` 探测；参数在签名里预留。）

## 5. 前端

### 5.1 `switch-mq.html`（标准 shell，不进 NAV，卡片驱动）
```html
<!doctype html><html lang="zh-CN"><head>…同 install.html 的 head，title「切换 MQ · Milvus Admin」…</head><body>
<div class="app"><aside class="rail" id="rail"></aside><div class="main">
  <header class="topbar" id="topbar"></header>
  <div class="content doc">
    <div class="page-head"><div class="h-l"><h1>切换消息队列</h1>
      <p>把某个 Milvus 实例切换到另一种 MQ。不可选的目标会灰置并说明原因（版本 / 模式 / 同类 / 规划中）。</p></div></div>
    <div id="err" class="callout co-err" style="display:none;margin-bottom:14px"></div>
    <div class="card"><div class="card-head"><h3>实例与当前 MQ</h3></div><div class="card-pad">
      <div class="f-row"><label>实例</label><select id="sw-inst" class="f-in"></select></div>
      <div id="sw-current" class="muted" style="margin-top:8px">—</div></div></div>
    <div class="card"><div class="card-head"><h3>目标 MQ</h3><span class="sub">灰置=不可选（悬停/下方看原因）</span></div>
      <div class="card-pad"><div id="sw-targets">加载中…</div></div></div>
    <div class="card co-warn" style="margin:0 0 14px"><div class="card-pad">
      <div><b>WAL 数据不可跨 MQ 迁移。</b>切换等价于在空集群上更换后端，存量流式数据无法原地迁移。</div>
      <label style="display:flex;gap:8px;margin-top:10px;cursor:pointer"><input type="checkbox" id="sw-ack">
        <span>我已知悉：切换后需重建 collection 并重新导入</span></label></div></div>
    <div class="card"><div class="card-head"><h3>执行</h3></div><div class="card-pad">
      <button class="btn btn-ghost" id="sw-dry" disabled>预演（dry-run）</button>
      <button class="btn btn-primary" id="sw-go" disabled>切换</button>
      <div id="sw-result" style="margin-top:12px"></div></div></div>
  </div></div></div>
<script src="assets/web.js"></script><script>renderSwitchMq();</script></body></html>
```

### 5.2 `renderSwitchMq()`
- `shell('switch-mq')`（`shell` 的 crumb 映射加 `switch-mq:'切换 MQ'`；rail 无 active 无妨）。
- URL `?instance=` 预选；`loadInstances()` 过滤 managed milvus 填 `#sw-inst`（无实例→提示）。
- `load(inst)`：`getJSON('api/switch-mq/targets?instance='+enc(inst))` →
  - `#sw-current` = `当前 MQ：<b>current_mq</b>（wal=current_wal · milvus current_version · mode）`。
  - `#sw-targets` = 每 target 一张 `.sw-opt`：`selectable`→可点（点击置 `selectedWal=wal`、加 `.sel`）；`!selectable`→`.dis` 灰置 + 显示 `reason`；`current`→`.cur` 标「当前」。`selectable` 用 `esc`。
  - 重置 `selectedWal=null`，`syncButtons()`。
- `#sw-inst` onchange → 改 URL(可选) + `load`。`#sw-ack` onchange → `syncButtons()`。
- `syncButtons()`：`#sw-dry.disabled = !selectedWal`；`#sw-go.disabled = !(selectedWal && #sw-ack.checked)`。
- `#sw-dry` → `submitSwitchMq(inst, selectedWal, true, false, #sw-result)`。
- `#sw-go` → `confirm('确认切换 '+inst+' 的 MQ 到 '+selectedWal+'？这会更改 WAL 并在 pod 内执行变更，存量流式数据将无法保留。')` → `submitSwitchMq(inst, selectedWal, false, false, #sw-result)`。
- 复用现有 `submitSwitchMq`（200→logPanel / 202→pollTask+「已提交 MQ 切换·operator 处理中」/ 409→门禁+强制 / 保持 dryRun）。**改造使其页面无关**：模态退休后 `submitSwitchMq` 只被本页调用，把 202 onDone 里那枚 🔄刷新按钮的 `closeModal(); renderMilvus()` 改为 **`location.reload()`**（本页 reload 会重跑 `renderSwitchMq` 重读 targets）——去掉对 `closeModal`/`renderMilvus` 的跨页依赖，避免 ReferenceError。

### 5.3 卡片按钮改跳转 + 退休模态
`renderMilvus` 的 `[data-switch]` wiring：`b.onclick = () => { location.href = 'switch-mq.html?instance=' + encodeURIComponent(b.getAttribute('data-switch')); }`。删除 `openSwitchMq` 函数（模态）。`submitSwitchMq` 保留。

### 5.4 CSS
`.sw-opt{display:inline-flex;flex-direction:column;gap:2px;min-width:180px;border:1px solid var(--border);border-radius:10px;padding:10px 12px;margin:0 8px 8px 0;cursor:pointer;vertical-align:top}` `.sw-opt .t{font-weight:600}` `.sw-opt .r{font-size:11.5px;color:var(--muted)}` `.sw-opt.sel{border-color:var(--accent);box-shadow:0 0 0 2px var(--accent-soft)}` `.sw-opt.dis{opacity:.5;cursor:not-allowed;background:var(--surface-2)}` `.sw-opt.cur{border-style:dashed}`。

## 6. 测试与验收
- **`compat.switch_mq_targets`**（`tests/test_compat.py` 追加）：current_wal=kafka → kafka 项 `selectable False reason 含"相同"`、pulsar `selectable True`；mode=cluster → rocksmq `selectable False reason 含"standalone"`；milvus 2.6 → woodpecker-service `selectable False`（reason 含"3.0" 或"规划中"）；milvus 3.0 → woodpecker-service `selectable False reason 含"规划中/external woodpecker"`（预留关闭）；每项含 `current` 布尔。
- **端点**（`tests/`，fake）：`GET /api/switch-mq/targets?instance=`（装 kafka milvus）→ `{current_mq:"kafka",current_wal:"kafka",targets:[…]}`，kafka 项 selectable False；未知实例→400。
- **前端 content-marker**（`tests/test_web_static.py`）：`switch-mq.html` 含 `id="sw-targets"`/`id="sw-ack"`/`renderSwitchMq`；web.js 含 `function renderSwitchMq`/`sw-opt`/`switch-mq.html?instance=`（卡跳转）/护栏 `sw-ack`；**`function openSwitchMq` 已移除**（更新旧断言）；`setInterval` 仍不存在。
- **JS**：`node --check`。
- **手动 DoD**（真集群）：Milvus 卡「切换 MQ」→ 跳转 `switch-mq.html?instance=milvus007`；当前 MQ=pulsar；目标卡：kafka 可选、pulsar 灰(「相同」)、rocksmq 灰(视 mode)、woodpecker 独立服务 灰(「规划中」)；勾护栏 → 预演出计划步骤 → 切换二次确认 → 流式；换实例下拉刷新 targets。

## 7. 非目标 / 后续
- 「安装/准备新目标 MQ」多步编排 + 切流进度 + PVC 清理（prototype 后续步骤；本切面预留）。
- woodpecker-service 切换的真正实现（operator external woodpecker 支持后，改 `_operator_supports_ext_woodpecker` + 接编排）。
- 依赖实例的 MQ 概念（仅 milvus 实例）。
