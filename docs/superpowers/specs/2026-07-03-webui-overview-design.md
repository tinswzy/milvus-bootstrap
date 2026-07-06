# mb WebUI — Overview 切面 · 设计

- 日期：2026-07-03
- 状态：设计已确认，待写实现计划
- 范围：WebUI 的第一个切面 —— 只读的 Overview 页 + 静态版本依赖规则页。安装/变更类交互留到下一切面。

## 1. 背景与目标

`milvus-bootstrap`（`mb`）已有 CLI + FastAPI daemon（over Unix socket）+ `mb doctor`（环境自检/版本探测/兼容矩阵）+ 兼容门禁。现在开始做**产品 WebUI**，参考 `prototype/` 的 vanilla 观感，但接**实时数据**。

第一个切面聚焦「打开就能看清现状」：
1. 打开首页 = Overview。
2. 展示**运行环境**（doctor 的环境检查逐项）。
3. **k8s 是否连接成功**的状态醒目展示。
4. 连接成功 → 罗列**能探测到的各组件版本**。
5. 连接成功 → 展示该 k8s 下的**各类实例**。
6. 另有一个**版本依赖关系限制**页（静态规则参考表）。

### 非目标（本切面明确不做）
- UI 里不做 install / delete / scale / upgrade / switch-mq 等**变更**动作（下一切面）。
- 版本依赖页**只列静态规则**，不结合当前集群做逐条 PASS/WARN/FAIL 评估。
- 不引入前端框架/构建链；不做认证鉴权（内部工具，靠绑定地址控制暴露面）。
- 不自动轮询刷新（提供手动「刷新」按钮）。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | WebUI 服务方式 | 扩展现有 FastAPI app 走 TCP，同一个 app 同时服务 vanilla 静态前端 + JSON API；不引入独立前端工程 |
| D2 | 版本依赖页形态 | **静态规则参考表**（不结合集群评估） |
| D3 | 启动与绑定 | 新命令 `mb web [--host 127.0.0.1] [--port 8080]`，**默认 localhost**；`--host 0.0.0.0` 开远程时打印「暴露可变更 API」警告 |
| D4 | 前端技术 | vanilla HTML/CSS/JS，沿用原型 `hub.css`/`hub.js` 观感，`fetch` 实时数据 |
| D5 | 刷新模型 | 加载即拉 + 手动「刷新」按钮，无自动轮询 |
| D6 | 前端代码位置 | 打进包内 `milvus-bootstrap/src/milvus_bootstrap/webui/`（随工具发布，FastAPI 挂载）；`prototype/` 保留作设计参考、不改 |

## 3. 架构与模块

| 模块 | 职责 | 变更 |
|---|---|---|
| `server/app.py` | 挂载静态 `webui/`；新增 `GET /api/doctor`、`GET /api/compat-rules`；复用 `GET /status` | 改 |
| `core/webapi.py`（新） | 组装 compat-rules 数据（把 `MQ_OPTIONS` + `load_constraints` + `load_upgrade_paths` 转成前端友好的 JSON 结构）；供 app 调用 | 新 |
| `server/__main__.py` | 加 `run_web(host, port)`：uvicorn 起 TCP 服务同一个 `app`（与既有 uds `main()` 并列） | 改 |
| `cli/main.py` | 新增 `mb web --host --port` 命令，转 `run_web`；`0.0.0.0` 打印警告 | 改 |
| `webui/`（新静态目录） | `index.html`(overview) + `compat.html`(规则) + `assets/`(css/js，改造自 hub.css/js) | 新 |

**边界原则**：JSON 端点复用 core 已有能力（`doctor.run()`、`Core.status()`、`compat` 数据），app 层只做 HTTP 包装；前端只做渲染，逻辑（探测/评估）全在后端。`mb web` 是 `mb core start`（uds）之外的一个 TCP 服务入口，跑同一个 app、同一套 Core（读为主，本切面只读）。

### 单元职责（可独立测试）
- `webapi.compat_rules() -> dict`：纯函数，把 compat 三处规则汇成 `{mq_rules, constraints, upgrade_paths}`。易测。
- `GET /api/doctor`：调 `doctor.run().to_json()`，去掉/保留 `exit_code`。经 TestClient 测。
- `GET /api/compat-rules`：调 `webapi.compat_rules()`。经 TestClient 测。
- `run_web(host, port)`：薄封装 uvicorn，仅确保参数正确（不在单测里真起服务）。

## 4. 数据端点

```
GET /api/doctor        -> { "env": [Finding…], "versions": {comp: ver…}, "tool": {...} }
                          （doctor.run().to_json()；env 里的 "cluster" 项即 k8s 连接状态）
GET /status            -> { "instances": [ {name, kind, managed/state_class, status…} ] }  （已有）
GET /api/compat-rules  -> {
    "mq_rules":    [ {id, label, wal, min_milvus, dep_kind, standalone_only, note} … ],  # 来自 MQ_OPTIONS
    "constraints": [ {component, requires, rule, milvus_range, min, max, severity, source, kind, reason} … ],  # load_constraints
    "upgrade_paths":[ {target_min, requires_current_min, reason} … ]                    # load_upgrade_paths
  }
```

## 5. 前端页面

### `index.html`（Overview）
- 复用原型壳：顶栏 crumbs + 左侧 rail。rail 项：**Overview（高亮）**、**版本依赖**（→ compat.html）、未来入口（安装向导等）**灰置位不可点**。
- 加载时 `fetch('/api/doctor')`；`fetch('/status')`。
- **① 运行环境**：`env` 逐项渲染成状态行（PASS/WARN/FAIL/SKIP 配色，复用 hub 的 badge/term 样式）。
- **k8s 连接状态**：从 `env` 里 `component=="cluster"` 的 Finding 取，醒目卡片（绿=已连接 / 红=未连接 + 原因）。
- **② 版本**：仅当 cluster=PASS 时渲染 `versions` 为表（组件→版本）。否则显示「连接集群后展示」。
- **③ 实例**：仅当 cluster=PASS 时 `fetch('/status')` 渲染实例表（名/类型/Managed/状态）。否则隐藏。
- 右上「刷新」按钮：重跑上述 fetch。

### `compat.html`（版本依赖）
- 同壳。加载 `fetch('/api/compat-rules')`，分三组渲染静态表：
  - **MQ ↔ milvus**（embed 需 ≥2.6、service 需 ≥3.0、kafka/pulsar/rocksmq ≥2.0、rocksmq 仅 standalone）。
  - **组件版本下限/约束**（etcd≥3.5、pulsar≥2.8.2、k8s≥1.16、helm≥3.0 soft；woodpecker+minio ≥2024-12-18 硬）。
  - **升级路径**（→2.6.10 需 ≥2.5.16；→3.0 需 ≥2.5.16）。
- 每条标 severity（hard/soft）与来源。

### `assets/`
- `web.css`：从 `prototype/assets/hub.css` 裁出 overview/compat 用到的部分（app 壳、rail、topbar、card、tbl、badge、term、callout 配色）。
- `web.js`：轻量渲染 + fetch 帮手（无框架）。可参考 `hub.js` 的 rail/mount 结构，但去掉假数据、改成真实 fetch。

## 6. 错误 / 降级
- `/api/doctor` 后端不抛：doctor 本地优先、缺项 SKIP。前端 fetch 失败 → 顶部错误条 + 「重试」。
- cluster 未连接：环境段照常显示（含红色 cluster 行），版本/实例段显示占位提示，不报错崩。
- `mb web` 绑 0.0.0.0：启动打印一行警告（会把可变更 API 暴露到网络）。

## 7. 测试与验收
- **后端单测**（pytest + FastAPI TestClient，MB_ADAPTER=fake，hermetic）：
  - `GET /` 返回 200 + HTML（静态挂载生效）。
  - `GET /api/doctor` 返回含 `env`/`versions`/`tool` 键的 JSON。
  - `GET /api/compat-rules` 返回含 `mq_rules`/`constraints`/`upgrade_paths` 三键，且各非空、字段齐全。
  - `GET /status` 出实例（已有覆盖，必要时补断言）。
  - `webapi.compat_rules()` 纯函数单测：断言三组规则数量/关键字段。
- **前端**：人工核对（`mb web` 起服务 → 浏览器开 overview/compat，连集群与不连两种态）。逻辑在后端、已单测覆盖。
- **DoD**：`mb web` 起 TCP；浏览器 `/` 出 Overview 四段（环境/连接/版本/实例）；未连集群时优雅降级；`/compat.html` 出三组静态规则表；后端单测全过；`--host 0.0.0.0` 打印警告。

## 8. 待办依赖 / 后续切面
- 下一切面：逐个基础组件的**安装交互**（install 向导），需要 UI→变更 API 的写路径 + 门禁/`--force` 的前端表达。
- 若要远程且安全，后续可考虑给 TCP app 加最小鉴权（本切面靠默认 localhost 规避）。
