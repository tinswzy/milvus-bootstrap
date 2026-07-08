# WebUI 安装交互切面 · 设计

- 日期：2026-07-06
- 状态：设计已确认，待写实现计划
- 范围：WebUI 的第二个切面 —— 逐组件安装（dry-run 预览 → 异步 apply），引入 UI 第一个写路径 + 门禁清晰化。

## 1. 背景与目标

第一切面（Overview）是只读的。本切面让用户能**从 WebUI 逐个安装基础组件**（etcd/minio/kafka/pulsar/milvus），走「选组件 + 填参数 → dry-run 预览 → 确认 → apply」的流程，并把安装做成**异步任务 + 轮询**（apply 可能几分钟，不能阻塞请求）。同时把兼容门禁的拦截在 UI 里表达清楚（清晰原因 + 可选 `--force`）。

现状（已核实）：
- `POST /install` 端点存在（`InstallReq{kind,name,platform,method,namespace,params,chart_override,dry_run,force}` → `Task`），但**同步**执行 saga（apply 会阻塞到 `wait_ready` 完成）。
- 任务引擎 `tasks/engine.py:run()` **就地**更新 `task.steps`（每步 running→ok/failed/skipped）。
- daemon **无全局异常处理器** → `compat.gate` 抛的 `CompatError`（`ValueError` 子类）当前变成 HTTP 500。
- `Task` 结构：`{id,type,target,dry_run,status,steps:[{name,status,plan,detail}],audit}`。

### 非目标
- 不做原型 `install.html` 那套多组件「搭积木」Milvus 编排向导（后续切面）。
- 不做逐步**实时流式**进度（用 spinner+计时 + 完成后步骤明细）；真流式需给引擎穿透回调，作后续增强。
- 不做 delete/scale/switch-mq 的 UI（后续切面）。
- woodpecker 组件暂缓（只列 etcd/minio/kafka/pulsar/milvus）。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 流程形态 | 逐组件表单 + dry-run 预览 → apply |
| D2 | apply 长任务 | 异步提交 + 轮询（提交即返回 task_id；轮询完成；期间 spinner+计时；完成展示步骤明细） |
| D3 | 参数 UX | 通用 key=value 参数行 + 按组件预填默认 |
| D4 | 门禁表达 | 全局异常处理器：`CompatError`→HTTP 409 + 结构化原因 + `force_hint`；UI 显示原因 + `--force` 重提 |
| D5 | 组件范围 | etcd / minio / kafka / pulsar / milvus |
| D6 | 绑定 | 沿用第一切面：`mb web` 默认 localhost（此切面新增 UI 写路径，靠绑定控制暴露面） |

## 3. 架构与模块

| 模块 | 职责 | 变更 |
|---|---|---|
| `core/taskrunner.py`（新） | 进程内异步任务运行器：`submit(fn)->task_id` 后台线程执行、`get(task_id)->dict` 查状态 | 新 |
| `server/app.py` | 新增 `POST /api/install`、`GET /api/task/{id}`；注册全局 exception handler | 改 |
| `webui/install.html`（新） | 安装表单页 | 新 |
| `webui/assets/web.js` | 加 `renderInstall()`；导航启用「安装向导」入口 | 改 |

**边界原则**：`TaskRunner` 只管「跑一个返回 `Task` 的函数并暴露状态」，不含业务逻辑（业务仍是 `Core.install`）。dry-run 走同步快路径；apply 走 runner。异常处理器把领域错误（`CompatError`/`ValueError`）翻成干净 HTTP，前端据 status code 分支。

### 单元职责（可独立测试）
- `TaskRunner.submit/get`：纯并发原语，`submit` 起线程跑 `fn()`，捕获返回的 `Task` 或异常，`get` 返回 `{state, task, error}`。用一个快速 `fn` 测（不依赖集群）。
- `POST /api/install` / `GET /api/task/{id}`：TestClient 测（fake adapter）。
- 异常处理器：TestClient 断言 `CompatError`→409、reason 在 body。

## 4. 数据端点

```
POST /api/install
  body: { kind, name, namespace="default", params={}, dry_run=true, force=false }
  dry_run=true  -> 200 { "task": <Task> }                 # 同步，planned 步骤
  dry_run=false -> 202 { "task_id": "...", "state": "running" }   # 异步提交
  门禁拦截      -> 409 { "error": "compat", "reason": "<清晰原因>", "force_hint": true }
  其它 ValueError-> 400 { "error": "bad_request", "reason": "..." }

GET /api/task/{task_id}
  -> 200 { "state": "running"|"succeeded"|"failed"|"error",
           "task": <Task|null>, "error": <str|null> }
  未知 id -> 404
```

`state` 语义：`running`（还在跑，`task`=null）；`succeeded`/`failed`/`rolled_back`（跑完，`task`=完整 Task，据 `task.status`）；`error`（runner 捕获到异常，如意外崩溃，`error`=消息）。为简化，前端把 `task.status` 直接当最终态展示；runner 的 `state` 主要区分「还在跑 vs 已结束 vs runner 层异常」。

## 5. 后端细节

### `TaskRunner`
```
class TaskRunner:
    submit(fn: Callable[[], Task]) -> str           # 起线程跑 fn；存 {state,task,error}
    get(task_id: str) -> dict | None                # {state, task(dict|None), error}
```
- 进程内 `dict[task_id -> record]`，线程写 record 的 state/task/error。
- daemon 持有一个模块级 `TaskRunner` 实例（lifespan 或模块级）。

### `POST /api/install`
- 组 `InstallSpec`（同现有 `/install`）。
- `dry_run=true` → 同步 `_core().install(spec, dry_run=True, force=force)`，返回 `{task:...}`。
- `dry_run=false`（apply）→ **两步**：
  1. **同步门禁预检**：先跑 `_core().install(spec, dry_run=True, force=force)`（`provisioner.install` 在 plan 前调 `compat.gate`，所以 dry-run 也会触发门禁；且不产生任何真实副作用）。若抛 `CompatError`，异常处理器直接返回 409 —— 后台任务不提交。
  2. 预检通过 → `runner.submit(lambda: _core().install(spec, dry_run=False, force=force))`，返回 `{task_id, state:"running"}`（202）。
- **为什么要预检**：apply 的真正执行在**后台线程**里，线程内抛的 `CompatError` 到不了 HTTP 异常处理器（响应早已返回 202）。先同步 dry-run 一次，能在返回响应前把门禁拦截变成 409 呈现给前端。

### 全局异常处理器（`server/app.py`）
```
@app.exception_handler(CompatError)  -> 409 {error:"compat", reason:str(exc), force_hint:True}
@app.exception_handler(ValueError)   -> 400 {error:"bad_request", reason:str(exc)}
```
（`CompatError` 是 `ValueError` 子类，须先注册 `CompatError` 处理器使其优先。）这同时修好之前 deferred 的「switch-mq/install 被拦时 CLI/UI 收到 500」问题。

## 6. 前端（`webui/install.html` + `renderInstall()`）

- 启用左侧导航「安装向导」（第一切面里是 `disabled` 占位 → 改为可点 `install.html`）。
- 表单：
  - 组件下拉：etcd / minio / kafka / pulsar / milvus。
  - 实例名输入。
  - namespace 输入（默认 default）。
  - 参数 key=value 行编辑器（[+ 加一行] / [删]）。**选组件变化时预填该组件默认**：milvus → `mq=kafka`、`image=milvusdb/milvus:v2.6.18`、`storageEndpoint=minio.default.svc:80`、`kafkaBrokers=kafka-dev.default.svc:9092`；其它组件默认空。
- 按钮：
  - **[dry-run 预览]** → `POST /api/install {dry_run:true}` → 渲染返回 Task 的步骤（name + plan）。
  - **[确认安装]** → `POST /api/install {dry_run:false}`：
    - 202 → 拿 `task_id`，进入轮询 `GET /api/task/{id}`（每 ~1.5s），期间显示 spinner + 已用时；完成后渲染 `task.steps`（每步 status 配色 + detail）+ 总状态（succeeded/failed/rolled_back）。
    - 409（compat）→ 显示 `reason` + **[强制安装 --force]** 按钮（点后二次确认，带 `force:true` 重提）。
    - 400/其它 → 红色错误条显示 reason。
- XSS：所有服务端字符串经 `esc()`（沿用第一切面）。

## 7. 测试与验收

- **后端单测**（pytest + TestClient，MB_ADAPTER=fake，hermetic）：
  - `TaskRunner`：`submit` 一个立即返回 Task 的 fn，轮询 `get` 到 succeeded；一个抛异常的 fn → `get` 到 `error`。
  - `POST /api/install {dry_run:true, kind:"etcd", name:"e1"}` → 200，`task.steps` 非空、`task.dry_run` true。
  - apply：`POST {dry_run:false, kind:"etcd"}` → 202 + task_id；轮询 `GET /api/task/{id}` 到 `succeeded`（fake 安装快）。
  - 门禁：`POST /api/install {dry_run:true, kind:"milvus", params:{mq:"woodpecker-service", image:"milvusdb/milvus:v2.6.3"}}` → **409** + reason 含版本原因；同参 `force:true` → 200（放行）。
  - `GET /api/task/unknown` → 404。
- **前端**：content-marker 测试（`/install.html` 含表单 id、web.js 含 `renderInstall`）；手动 DoD。
- **DoD**：`mb web` 起服务 → 打开安装向导 → 选 etcd 填名 → dry-run 出步骤 → 确认安装 → 轮询到成功、Overview 里能看到新实例；选 milvus 填不兼容 MQ → 看到 409 原因 + 强制按钮。后端单测全过。

## 8. 待办依赖 / 后续切面
- 后续：多组件搭积木 Milvus 向导；delete/scale/switch-mq 的 UI；逐步实时流式进度（引擎回调）。
- 安全：远程访问仍靠默认 localhost 规避；写路径上线后如需远程，考虑最小鉴权。
