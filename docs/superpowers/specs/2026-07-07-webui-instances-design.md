# WebUI 实例页重构（Dependencies + Milvus）+ 删除 · 设计

- 日期：2026-07-07
- 状态：设计已确认，待写实现计划
- 范围：WebUI 第三切面 —— 把实例展示从 Overview 拆到两张按原型组织的页面（Dependencies / Milvus），并加「删除」这一管理动作，凑齐 装-看-管 MVP。

## 1. 背景与目标

前两切面：Overview（只读环境/版本/实例）+ 安装向导（异步 install）。当前 Overview 把所有实例混在一张表里。用户要按原型把它拆成两块并加基础管理：
1. Overview 去掉实例区（保留 环境 / k8s 连接 / 版本）。
2. **Dependencies 页**：依赖组件实例（etcd/minio/kafka/pulsar），按 kind 分组，显示 image/版本/接入点，可新建、可删除。
3. **Milvus 实例页**：以 milvus 为中心，一卡一个，显示实时 CR 健康 + 依赖绑定，可删除、可新建。
4. 加「删除」动作（异步任务 + 轮询，复用安装切面的 TaskRunner）。

现状（已核实）：
- `GET /api/instances` 现返回 `[{name,kind,namespace,ownership}]`（来自 `state.list_instances()`）。
- `Instance` 字段：`id,name,platform,namespace,ownership,deps,spec_snapshot`（无 `kind`；kind 在 `spec_snapshot["kind"]`）。milvus 的 mq/image/endpoints 在 `spec_snapshot["params"]`。
- `Core.delete(instance_id, dry_run, force?)` 存在（`lifecycle.delete`，state-class 护栏，PVC 默认保留）；`POST /delete` 同步端点存在。
- `TaskRunner`（`submit/get`）+ `GET /api/task/{id}` 轮询 + 异常处理器（CompatError→409 / ValueError→400）已在（安装切面）。
- `probe.run_kubectl` + `probe.detect_versions()`（按 kind 出 etcd/minio/kafka/pulsar/milvus 版本）已在。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 页面拆分 | Overview 去实例区；新 Dependencies 页 + Milvus 实例页 |
| D2 | 管理动作 | 本切面只做**删除**（异步，复用 TaskRunner + /api/task 轮询）；升级/扩缩/switch-mq 留下一切面 |
| D3 | 健康展示 | milvus 查实时 CR `.status.status`；deps 不逐实例查健康（只显 image/版本/接入点） |
| D4 | 布局保真度 | 沿用原型观感（卡片/badge/accent），MVP 用简化卡片 + 依赖 chip；不做动画数据流拓扑 |
| D5 | 组件范围 | deps: etcd/minio/kafka/pulsar；milvus 单列 |

## 3. 架构与模块

| 模块 | 职责 | 变更 |
|---|---|---|
| `core/probe.py` | 加 `milvus_status(name, run=run_kubectl) -> str | None`（查 Milvus CR `.status.status`，best-effort） | 改 |
| `server/app.py` | 扩展 `GET /api/instances`（加 image + milvus status/deps）；新增异步 `POST /api/delete` | 改 |
| `webui/index.html` + web.js `renderOverview` | 去掉实例区 | 改 |
| `webui/deps.html`（新）+ web.js `renderDeps()` | Dependencies 页 | 新/改 |
| `webui/milvus.html`（新）+ web.js `renderMilvus()` | Milvus 实例页 | 新/改 |
| `webui/assets/web.js` | 导航加 Milvus 实例/Dependencies；delete 动作（confirm→POST→轮询） | 改 |

**边界原则**：后端只提供数据 + 复用已有 delete/gate/runner；前端只渲染 + 触发。milvus 健康查询是 best-effort（fake/连不上→null，页面显示「—」不崩）。deps 版本复用 `/api/doctor` 的按-kind 探测，不为每个 dep 实例单独查集群。

## 4. 数据端点

```
GET /api/instances  (扩展)
  -> { "instances": [ {
        name, kind, namespace, ownership,
        image,                       # milvus: spec_snapshot.params.image；其它: ""
        status,                      # milvus: CR .status.status（如 "Healthy"）；非 milvus 或查不到: null
        deps                         # milvus: {etcd, storage, mq, mq_endpoint}（从快照 params 提）；非 milvus: null
      } ] }

POST /api/delete  { instance, force=false }
  -> 202 { task_id, state:"running" }      # runner.submit(Core.delete(instance, dry_run=False, force))
  异常(护栏/未知实例) -> 400/409（异常处理器）
GET /api/task/{id}                          # 复用（安装切面）
```

`deps` 提取规则（从 milvus 的 `spec_snapshot["params"]`）：
- `etcd` = `params.etcdEndpoints`（或默认 `etcd.{ns}.svc:2379`）。
- `storage` = `params.storageEndpoint`。
- `mq` = `params.mq`（kafka/pulsar/woodpecker…）。
- `mq_endpoint` = `params.kafkaBrokers` 或 `params.pulsarEndpoint`（按 mq）。
缺失字段用空串，前端显「默认/—」。

## 5. 后端细节

### `probe.milvus_status(name, run=run_kubectl) -> str | None`
```
rc, out, _ = run(["get", "milvus", name, "-o", "jsonpath={.status.status}"])
return out.strip() or None if rc == 0 else None
```
`GET /api/instances` 里：仅当 `spec_snapshot["kind"]=="milvus"` 且 `adapter.name=="k8s"` 时调用，try/except→None（fake/连不上不崩、不拖慢）。

### `POST /api/delete`
```
runner.submit(lambda: _core().delete(req.instance, dry_run=False, force=req.force))
-> 202 {task_id, state:"running"}
```
delete 的 state-class 护栏在 `lifecycle.delete` 内（如权威态 backup-note 步骤）；若 delete 抛（未知实例 KeyError→需转 ValueError 或让其 500？）—— 未知实例 `Core.delete` 抛 `KeyError`。**处理**：`POST /api/delete` 先同步查实例存在性（`_core().state.get_instance(req.instance)`），不存在→抛 `ValueError`（→400 via handler），存在才 submit。

## 6. 前端

### 导航（web.js NAV）
`Overview / Milvus 实例(milvus.html) / Dependencies(deps.html) / 版本依赖(compat.html) / 安装向导(install.html)`，各带图标；shell() 面包屑加两项。

### `index.html` / `renderOverview`
删除「集群内实例」card + `renderOverview` 里 instances 的 fetch/渲染段（保留 环境/连接/版本 + 刷新）。

### `deps.html` / `renderDeps()`
- `fetch('/api/instances')` + `fetch('/api/doctor')`（取 versions[kind]）。
- 过滤 kind ∈ {etcd,minio,kafka,pulsar}，**按 kind 分组**；每组标题 + 「新建」按钮（→ `install.html`）。
- 每实例一行/卡：名 · ns · image/版本（`versions[kind]`）· 「删除」按钮。
- 空组不显示或显「无」。

### `milvus.html` / `renderMilvus()`
- `fetch('/api/instances')`，过滤 kind=="milvus"。
- 每实例一卡：名 · ns · `image` · 健康 badge（`status` → PASS/WARN/未知）· 依赖 chip（etcd / storage / mq(+endpoint)）· 「删除」按钮。
- 顶部「新建 Milvus」按钮（→ install.html）。

### delete 动作（共用）
```
删除按钮 onclick -> confirm(`确认删除实例 <name>？(依赖/PVC 默认保留)`)
  -> POST /api/delete {instance:name} -> 202 -> 轮询 /api/task/{id}
  -> 成功后重新 render 当前页（列表刷新）
  -> 400/409/error -> 顶部错误条
```
XSS：所有服务端字符串经 `esc()`。

## 7. 测试与验收

- **后端单测**（TestClient, MB_ADAPTER=fake, hermetic）：
  - `probe.milvus_status`：fake run 返回 `"Healthy"` → 得 `"Healthy"`；rc!=0 → None。
  - `GET /api/instances` 扩展：装一个 etcd + 一个 milvus（fake），断言 milvus 行有 `image`/`status`(fake→null)/`deps` 键、etcd 行 `deps` 为 null。
  - `POST /api/delete`：装个 etcd → delete → 202 task_id → 轮询到 succeeded → `/api/instances` 不再含它；delete 不存在实例 → 400。
- **前端**：content-marker（deps.html/milvus.html 关键 id + web.js 有 `renderDeps`/`renderMilvus`；index.html 不再有实例区标记）+ 手动 DoD。
- **DoD**：`mb web` 起服务 → Dependencies 页按 kind 分组显 etcd/minio/kafka/pulsar + 版本；Milvus 页显 milvus-dev/milvus-pulsar + Healthy + 依赖 chip；某实例点删除 → 确认 → 轮询成功 → 列表刷新消失；Overview 不再有实例区。后端单测全过。

## 8. 待办依赖 / 后续切面
- 后续：升级/扩缩/switch-mq 的 UI；原型的动画数据流拓扑；deps 逐实例 live 健康；pollInstall/task 轮询的 wall-clock 超时（沿用安装切面遗留）。
