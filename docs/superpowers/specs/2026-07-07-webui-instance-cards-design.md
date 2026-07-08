# WebUI 实例页富卡片（对齐原型）· 设计

- 日期：2026-07-07
- 状态：设计已确认，待写实现计划
- 范围：把刚做的 Milvus 实例页 / Dependencies 页从「朴素表格/chip」升级成原型那样的富卡片（实例拓扑卡 + 依赖手风琴），只改前端渲染 + 少量 CSS。

## 1. 背景与目标

第三切面做出了 Milvus 页 / Dependencies 页，但渲染太朴素（一个简单卡 + chip / 一个表格），"没什么内容可看"。用户要对齐原型：
- **Milvus 页**：每实例一张原型式「实例卡」——头部（logo + 名 + ns/镜像 + 健康 badge）+ 依赖拓扑行（etcd 盒子 ▸ Milvus 核心盒子 ▸ 存储盒子 ▸ MQ 盒子，带连接线）。**保真度 A（完整拓扑）**已选定。
- **Dependencies 页**：每类依赖一个原型式手风琴（logo + 类型名 + 镜像 tag chip + 展开箭头；展开列出该类实例 + endpoint + 删除）。

**关键发现（省大量工作）**：原型 Milvus 卡的样式类（`.inst / .inst-head / .topo / .box / .box-mv / .flow-h / .flow-v / .mvdot / .cell-etcd / .cell-store / .cell-mq / .box .bt/.lo/.nm/.role/.id`）**已经在 `web.css`**（当初从 hub.css 整份拷来）。所以 Milvus 卡**只需产出对应 markup、零新 CSS**。只有 Dependencies 的手风琴样式（`.acc / .acc-head / .img / .tag-up` 等）是原型 upgrade.html 内联的、**需移植进 web.css**。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | Milvus 卡保真度 | **A：完整拓扑**（etcd▸核心▸存储▸MQ + 连接线），用真实 deps 数据 |
| D2 | 未实现的操作 | 核心盒子里 **删除**是真的；**切换MQ / 配置 / Pods** 做成**灰置位占位**（点了 toast/alert「下一切面」）|
| D3 | Dependencies 呈现 | 每类一个手风琴（默认展开、可折叠），头部带镜像 tag chip |
| D4 | 数据 | 复用现有 `/api/instances`（含 image/status/deps）+ `/api/doctor`（deps 版本）；不新增后端 |
| D5 | 样式 | Milvus 卡复用 web.css 已有类；deps 手风琴样式从原型移植进 web.css |

## 3. 架构与模块

| 模块 | 职责 | 变更 |
|---|---|---|
| `webui/assets/web.js` | 重写 `renderMilvus()`（原型实例卡 markup）+ `renderDeps()`（手风琴 markup + 折叠 toggle）；小工具 `depLogo(kind)` 等 | 改 |
| `webui/assets/web.css` | 追加 deps 手风琴样式（`.acc/.acc-head/.img/.tag-up` 等，从原型移植） | 改 |

**仅前端**。后端端点不动。所有服务端字符串经 `esc()`。

## 4. Milvus 实例卡（`renderMilvus`）

每个 `kind==='milvus'` 实例产出：
```
<div class="card inst">
  <div class="inst-head">
    <span class="mvdot">M</span>
    <div><div class="nm">{name}</div><div class="ns">ns: {namespace} · {image}</div></div>
    <div class="right">{健康 badge}</div>
  </div>
  <div class="topo">
    {etcd 盒子}         <div class="flow-h col2"></div>
    <div class="box box-mv">
      <div class="bt"><span class="lo">M</span><div><div class="nm">{name}</div><div class="role">向量数据库内核 · MixCoord</div></div></div>
      <div class="id"><span class="d"></span>{name} · {image}</div>
      <div class="mvmeta"><span class="badge b-accent"><span class="d"></span>MQ: {deps.mq}</span></div>
      <div class="mv-actions">
        {切换MQ 灰置位} {配置 灰置位} {Pods 灰置位} <button 删除(real)>
      </div>
    </div>
    <div class="flow-h col4"></div>
    {存储盒子}          <div class="flow-v"></div>   {MQ 盒子}
  </div>
</div>
```
- **健康 badge**：`status==='Healthy'`→PASS、其它非空→WARN、null→`<span class="muted">健康 —</span>`。
- **etcd 盒子**：`depBox('cell-etcd','🗄️','etcd','元数据', deps.etcd || 'etcd.{ns}.svc:2379')`。
- **存储盒子**：`depBox('cell-store', '🪣','对象存储','Object Storage', deps.storage)`。
- **MQ 盒子**：`depBox('cell-mq', mqLogo(deps.mq), deps.mq||'MQ','消息队列 · WAL', deps.mq_endpoint)`。
- `depBox(cls,logo,name,role,id)` 产出原型 `.box` 结构（`.bt`>`.lo`+`.nm`+`.role`，`.id`）。
- 占位按钮：`<button class="btn btn-ghost" disabled title="下一切面">切换MQ</button>` 等（disabled 灰显）；删除按钮 real（`data-del`）。
- 顶部「+ 新建 Milvus」。
- 空列表：占位卡「暂无 Milvus 实例」。

## 5. Dependencies 手风琴（`renderDeps`）

每类（etcd/minio/kafka/pulsar）产出一个 `.acc`（默认 `.acc.open`）：
```
<div class="acc card open">
  <div class="acc-head" onclick=toggle>
    <span class="lo">{depLogo(kind)}</span>
    <div><div class="nm">{Kafka…}</div><div class="sub">{n} 个实例</div></div>
    <div class="right"><span class="img">image: <span class="t">v{versions[kind]}</span></span>
      <span class="chev">▾</span></div>
  </div>
  <div class="acc-body">   // 展开区
    每实例: <div>{name} · ns:{ns} · <span class="mono">{endpoint}</span> · <button 删除></div>
    <a href="install.html">+ 新建</a>
  </div>
</div>
```
- `depLogo`: etcd🗄️ / minio🪣 / kafka🌊 / pulsar📡；名：etcd/MinIO/Kafka/Pulsar。
- **endpoint 派生**（best-effort，约定 svc）：etcd→`{name}.{ns}.svc:2379`、minio→`{name}.{ns}.svc:80`、kafka→`{name}.{ns}.svc:9092`、pulsar→`{name}-broker.{ns}.svc:6650`。
- 折叠 toggle：`.acc-head` onclick 切换所在 `.acc` 的 `open` class（纯 DOM，无 fetch）。删除按钮 real。
- 空类：仍显示手风琴头 + 「无实例」。

## 6. CSS 移植（web.css 追加）

从原型 `upgrade.html` 内联 `<style>` 移植 `.acc / .acc-head / .acc-head .lo(.mv) / .nm / .sub / .right / .chev / .acc.open* / .img / .img .t / .tag-up`，并加 `.acc-body`（展开区 padding；`.acc:not(.open) .acc-body{display:none}`）。Milvus 卡类已在 web.css，不动。

## 7. 测试与验收

- **前端 content-marker**（`tests/test_web_static.py` 追加/改）：
  - milvus.html 渲染后含拓扑 markup —— 因是 JS 运行时渲染，测断言 **web.js 里** `renderMilvus` 含关键类名字符串（`class="topo"`、`box-mv`、`inst-head`）+ `depBox`。
  - web.js `renderDeps` 含 `acc-head` / `acc-body` 字符串；web.css 含 `.acc` / `.img`。
  - 复用现有 milvus.html/deps.html 页服务测试（id 不变）。
- **JS 合法性**：`node --check web.js`。
- **手动 DoD**：`mb web` → Milvus 页每实例是原型式拓扑卡（etcd▸核心▸存储▸MQ + 连接线 + 健康 badge + MQ chip + 删除 real + 切换MQ/配置/Pods 灰置位）；Dependencies 页每类是手风琴（logo + 镜像 tag + 可折叠 + 实例含 endpoint + 删除）；删除仍可用。后端/单测无回归。

## 8. 非目标 / 后续
- 不实现 切换MQ/配置/Pods（仅占位）；下一切面接。
- endpoint 是约定派生显示，非后端实查。
- 不做原型的 SVG 动画数据流特效（用已有 `.flow-h/.flow-v` 静态连接线即可）。
