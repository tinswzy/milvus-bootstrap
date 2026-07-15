# WebUI Switch-MQ 三角布局 + 分组下拉（列实例）· 设计

- 日期：2026-07-15
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：**仅 UI/只读增强**——(1) 拓扑改成**三角分叉**（Milvus 在上，当前 MQ 左下、目标 MQ 右下）；(2) 目标下拉**按 MQ 类型 optgroup 分组**，外部型组下列**已部署实例名**，整类不可选时在分类处注明原因。**切换仍按类型（target_wal）**；「真重指某实例端点」是**下一切面（③）**。

## 1. 背景与边界

- 现状 switch-mq 页（拓扑分叉左右布局 + 平铺下拉）：目标下拉平铺列 MQ 类型（Kafka/Pulsar/Woodpecker…），不可选项 `<option disabled>` 带因。
- 用户诉求：Milvus 在上、当前 MQ 左下、目标 MQ 右下（更像"从 Milvus 分叉出去"）；下拉**按分类**、分类下列**该类型的实例名**，整类不可选在分类处提示。
- **已核实**：`core.state.list_instances()` 有已部署 MQ 实例（真集群：`kafka-dev`(kind=kafka)、`pulsar-dev`(kind=pulsar)，均 managed）。`compat.MQ_OPTIONS` 每型有 `dep_kind`（外部型=kafka/pulsar/woodpecker；嵌入型 rocksmq/woodpecker-embedded=None）。`compat.switch_mq_targets` 已返回 `{id,wal,label,dep_kind,note,current,selectable,reason}`。
- **明确非目标（③，下一切面）**：选中具体实例时把其端点注入 milvus 配置（`kafka.brokerList`/`pulsar.endpoint`）、**apply CR + 可能滚动重启**让配置生效、再 `wal/alter` 切过去。本切面**不做**——`instances[].endpoint` 先返回好（前端展示 + ③ 备用），但 `submitSwitchMq` 仍只传 `target_wal`。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 布局 | 三角分叉：Milvus 顶部居中 → `⇄切换` 块 → 分叉：**实线向左下**接「当前 MQ」、**虚线向右下**接「目标 MQ」。当前左下、目标右下 |
| D2 | 分组下拉 | `#sw-target` 用 `<optgroup>` 按 MQ 类型分组。首项「选择目标…」|
| D3 | 外部型（有 dep_kind）| optgroup 下列 `core.state` 里该 `kind` 的**每个实例**一 `<option>`（`name (ns)`）；无实例→组内一条 disabled「（无可复用实例，需先安装）」|
| D4 | 嵌入型（dep_kind None）| optgroup 下一条 `<option>`「（嵌入，无独立实例）」（value=wal）|
| D5 | 整类不可选 | 该型 `!selectable`（同类/版本/模式/预留）→ **`<optgroup label="Kafka · <reason>" disabled>`**（原生 disabled 灰置全组）|
| D6 | 选中语义（本切面）| option `value="<wal>"` + `data-inst`/`data-ns`（外部型带实例、嵌入型空）。选中→`selectedWal=value`、目标框显示 `label`（+实例名）。**切换仍 `submitSwitchMq(inst, selectedWal, …)` 按类型**（③ 再接实例端点）|
| D7 | 后端 | `compat.switch_mq_targets` 每项加 `embedded:bool`(=dep_kind is None)；`GET /api/switch-mq/targets` 每 target 加 `instances:[{name,namespace,endpoint}]`（从 state 按 dep_kind 过滤，endpoint 由 kind+name+ns 推）|
| D8 | 护栏/流程不变 | 三层护栏、门禁 409/force、流式、无 setInterval 全保留；`submitSwitchMq`/`/api/switch-mq` 不动 |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/compat.py` `switch_mq_targets` | 每项加 `embedded`(dep_kind is None) |
| `server/app.py` `api_switch_mq_targets` | 每 target 加 `instances`（state 按 dep_kind 过滤 + `_dep_endpoint` 推端点）|
| `webui/switch-mq.html` | 拓扑区改三角布局标记 |
| `webui/assets/web.js` `renderSwitchMq` | 目标下拉改 optgroup 分组（列实例 / 嵌入 / 整类 disabled）；选中读 value+data-inst |
| `webui/assets/web.css` | 三角布局 CSS（`.sw-tri` + 连接线），退休上一版左右 `.sw-fork` 相关 |

## 4. 后端

### 4.1 `compat.switch_mq_targets` 加 `embedded`
在现有每项 dict 加 `"embedded": o["dep_kind"] is None`。（其余不变。）

### 4.2 `api_switch_mq_targets` 加 `instances`
端点内（拿到 `targets` 后、`core.state.list_instances()` 已可用）：
```python
def _dep_endpoint(kind: str, name: str, ns: str) -> str:
    return {"kafka": f"{name}.{ns}.svc:9092",
            "pulsar": f"{name}-broker.{ns}.svc:6650",
            "woodpecker": f"{name}.{ns}.svc:9000"}.get(kind, f"{name}.{ns}.svc")

# 建索引：kind -> [instance]
by_kind = {}
for inst in core.state.list_instances():
    k = (inst.spec_snapshot or {}).get("kind", "")
    by_kind.setdefault(k, []).append(inst)
for t in targets:
    dep = t.get("dep_kind")
    t["instances"] = ([] if not dep else
        [{"name": i.name, "namespace": i.namespace,
          "endpoint": _dep_endpoint(dep, i.name, i.namespace)} for i in by_kind.get(dep, [])])
```
（`instances` 仅外部型非空；嵌入型 `dep_kind None` → `[]`。响应形状：`targets:[{…, embedded, instances:[…]}]`。）

## 5. 前端

### 5.1 三角布局（`switch-mq.html` + CSS）
拓扑区标记（替换现左右 `.sw-fork`）：
```
        [ Milvus 实例 ]        ← 顶部居中
             │  (实线)
        [ ⇄ 切换 ]
      ┌──────┴──────┐          ← 分叉横线
   实线│           ┊虚线
 [当前 MQ·ACTIVE]  [目标 MQ ▼]  ← 左下 / 右下
```
用 CSS grid：Milvus 顶行居中；下接竖实线到 `⇄切换` 块；再一条横"分叉总线"，左臂**实线**下探到当前 MQ、右臂**虚线**（动画）下探到目标 MQ；当前 MQ 左下、目标 MQ 右下。元素 id 全部沿用（`sw-mv-name`/`sw-cur-logo`/`sw-cur-name`/`sw-tgt-logo`/`sw-tgt-name`/`sw-target`/`sw-tgt-reason`）。CSS 新 `.sw-tri` 系列 + 连接线（`.tri-v` 竖实线 / `.tri-bus` 横线 / `.tri-l` 实线左臂 / `.tri-r` 虚线右臂），reduced-motion 守 dashed。

### 5.2 分组下拉（`renderSwitchMq`）
`load()` 构建 `#sw-target`：
```javascript
const opts = ['<option value="">选择目标…</option>'];
(d.targets || []).forEach(t => {
  const glabel = t.selectable ? esc(t.label) : `${esc(t.label)} · ${esc(t.reason || '不可选')}`;
  const gdis = t.selectable ? '' : ' disabled';
  let inner;
  if (t.embedded) {
    inner = `<option value="${esc(t.wal)}" data-inst="" data-ns="">（嵌入，无独立实例）</option>`;
  } else if ((t.instances || []).length) {
    inner = t.instances.map(x =>
      `<option value="${esc(t.wal)}" data-inst="${esc(x.name)}" data-ns="${esc(x.namespace)}">${esc(x.name)} (${esc(x.namespace)})</option>`).join('');
  } else {
    inner = `<option disabled>（无可复用实例，需先安装）</option>`;
  }
  opts.push(`<optgroup label="${glabel}"${gdis}>${inner}</optgroup>`);
});
tgtSel.innerHTML = opts.join('');
```
`onchange`：`const opt = tgtSel.options[tgtSel.selectedIndex]; selectedWal = tgtSel.value || null; selectedInst = opt ? (opt.getAttribute('data-inst') || '') : '';`。目标框 `sw-tgt-name` 显示 `optgroup 的类型 label + 实例名`（用选中 option 的 optgroup label 或 t.label；简化：显示 option 文本 + 类型），`sw-tgt-logo`=`mqLogo(selectedWal)`。`advance()`/`syncButtons()`/预演/切换、护栏、confirm 全不变——**submitSwitchMq 仍只传 selectedWal**（③ 再加 selectedInst/端点）。

（注：`<optgroup disabled>` 原生使整组不可选，满足 D5「整类灰置带因」。）

## 6. 测试与验收
- **`compat.switch_mq_targets`**（`tests/test_compat.py`）：每项含 `embedded`；kafka/pulsar `embedded False`、rocksmq/woodpecker-embedded `embedded True`。
- **端点**（`tests/test_web_switchmq.py`，fake）：装 kafka milvus + 装一个 kafka dep 实例后 `GET /api/switch-mq/targets` 的 kafka target `instances` 含该实例 `{name,namespace,endpoint}`（endpoint 形如 `<name>.<ns>.svc:9092`）；嵌入型 `instances==[]`。
- **前端 content-marker**（`tests/test_web_static.py`）：`switch-mq.html` 含 `.sw-tri`/三角连接线类；web.js `renderSwitchMq` 段含 `optgroup`/`data-inst`/`embedded`；`sw-target` 仍在；`setInterval` 不存在；css 含 `.sw-tri`/`.tri-r`。更新旧 `test_switch_mq_topology_css`/`test_switch_mq_page_present`（`.sw-fork`→`.sw-tri`）。
- **JS**：`node --check`。
- **手动 DoD**（真集群）：卡「切换 MQ」→ 三角图：Milvus 上、当前 MQ(pulsar) 左下、目标 MQ 右下；目标下拉分组：`Kafka` 组下 `kafka-dev (default)`、`Pulsar · 与当前相同` 组 disabled、`Woodpecker（独立服务）· 需 milvus≥3.0` 组 disabled、`RocksMQ（嵌入）` 组下「（嵌入…）」；选 kafka-dev → 目标框显示 Kafka/kafka-dev；勾护栏 → 预演/切换（按类型切，同现状）。

## 7. 非目标 / 后续（③ 下一切面）
- **③ 真重指端点注入**：选中具体实例 → 把其 `endpoint` 注入 milvus 配置（`kafka.brokerList`/`pulsar.endpoint` via spec.config）→ **apply CR + 可能滚动重启**（用户指出：配置需重启才渲染生效）→ 再 `wal/alter` 切换 → verify → decommission。破坏性，单独设计 + 真集群谨慎验证。本切面已把 `instances[].endpoint` + `data-inst/data-ns` 备好。
- 「装新目标 MQ」编排、PVC 清理（仍预留）。
