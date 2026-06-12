# 方案三：Milvus Enterprise Admin WebUI

> 面向读者：研发 / 产品。
>
> 目标：从根上解耦"安装编排"与"运行连线"。做一个**轻量**的 Python + Web 管理控制台，它**自己不代管任何中间件**，而是编排各组件**官方的** operator / helm / kubectl，把组件间的依赖规则在**界面层**显式表达。所见即所得，像搭积木。
>
> 配套可点击原型：[`../prototype/index.html`](../prototype/index.html)（全假数据、可互相跳转）。

---

## 1. 为什么不叫 "Hub"

"Hub"（如 Cloudera Data Hub、Transwarp Data Hub）暗示一个**大集合**——把所有组件的管理都吞进自己。这恰恰是我们要避免的：现有 milvus-helm/operator 的病根，就是"一个工具越界代管 etcd/minio/pulsar/kafka"。

我们要的相反：**每个组件由它自己的官方 operator 管**，admin webui 只是薄薄一层"编排 + 配置面板 + 依赖可视化"。所以叫 **Enterprise Admin WebUI**，强调它的**轻量**与**不越界**。

| | "Hub" 心智（要避免） | "Admin WebUI" 心智（我们要的） |
| --- | --- | --- |
| 谁装 kafka | Hub 自带一套 kafka 子 chart | Strimzi（kafka 官方 operator）装，webui 只下发 CR |
| 谁管生命周期 | Hub 越界代管 | 各组件 operator 自管，webui 只读状态 + 触发 |
| 组件依赖 | 隐含在代码 if-else 里 | 在 webui 界面**显式**表达成规则 |
| 体量 | 重，绑死所有组件 | 轻，可插拔，组件各归各 |

---

## 2. toB 厂商最佳实践对标

我们不是第一个做这件事的。看几个成熟 toB 产品怎么处理"多组件平台的安装与编排"，提炼可借鉴的模式：

### 场景对标

| 厂商 / 产品 | 它解决的场景 | 我们能借鉴什么 |
| --- | --- | --- |
| **Cloudera Data Hub** | 在云上按"工作负载模板"一键拉起 Hadoop/Spark/Kafka 等集群 | **模板化安装**：把"etcd+minio+woodpecker+milvus"存成一个可复用的"集群蓝图" |
| **Transwarp Data Hub (TDH) / Manager** | 私有化场景下，图形化安装/扩容/巡检一整套大数据组件 | **私有化 + 所见即所得**：on-prem 客户用界面点选组件、看健康、做巡检 |
| **Confluent Control Center / Cloud** | 专注 Kafka 一个组件，但把 topic/连接/监控做到极致 | **单组件深度管理**：每个组件详情页要能管到 ConfigMap 级别 |
| **Rancher** | 不自己实现 K8s，而是**纳管**多个 K8s 集群与其上的 app | **纳管而非取代**：webui 纳管已有的官方 operator，而不是重写它们 |
| **Strimzi / Pulsar / etcd Operator** | 各自把一个中间件的安装运维做成 CR | **底层执行者**：webui 的"安装 kafka"最终就是下发一个 `Kafka` CR 给 Strimzi |

### 提炼出的 4 条设计原则

1. **纳管，不取代**（学 Rancher）：底层永远是组件官方 operator/helm 在干活，webui 只编排和可视化。
2. **模板化蓝图**（学 Cloudera）：把"一套可用的 Milvus 部署"沉淀成蓝图，一键拉起 / 复制 / 灰度。
3. **所见即所得 + 私有化友好**（学 Transwarp）：每个动作有预览、有校验、有回滚提示；能离线/私有化部署。
4. **管到 ConfigMap 级**（学 Confluent）：组件详情页能直接 diff/编辑配置并触发重启——这正是 switch MQ 的核心动作。

---

## 3. 几个关键场景：admin webui 怎么做

> 下面每个场景都对应原型里的一个页面，可点开 [`../prototype/`](../prototype/) 实际走一遍。

### 场景 1：搭积木式安装一套 Milvus（对应 `install.html`）

用户在向导里像搭积木一样勾选：
```
[元数据]  etcd                        ← 由 etcd-operator / bitnami chart 装
[对象存储] ○ 内置 MinIO  ● 外部 S3     ← MinIO operator,或只填外部 S3 endpoint
[消息队列] ○ Pulsar ○ Kafka ● Woodpecker  ← 互斥单选(运行期),依赖规则在界面提示
[内核]    Milvus(operator)            ← milvus-operator 下发 Milvus CR
[工具]    □ Attu  □ Birdwatcher  □ Log Export
```
- **依赖规则在界面显式表达**：选了 Woodpecker → 界面自动高亮"必须有对象存储"，未配置则禁用"下一步"。
- 点"安装" → 后台按拓扑顺序，分别调用**各组件自己的** operator/helm 完成安装，并实时回显进度。

### 场景 2：★ 切换 MQ（对应 `switch-mq.html`，核心场景）

把方案二那套手工命令，变成一个**有护栏的三步向导**：

```
Step 1 校验      → 检查集群是否为空 / 是否已备份;红字提示"WAL 数据不可跨 MQ 迁移"
Step 2 准备新 MQ → 调 Strimzi 装好 kafka,等待就绪(此时旧 MQ 仍在跑)
Step 3 切流+下线 → 改 milvus ConfigMap 的 mq.type=kafka → 重建 milvus → 校验 → 下线旧 MQ
                   (删 PVC 需单独二次确认)
```
- 每一步都有**前置校验**和**不可逆操作的二次确认**，把方案二里"靠人记得"的前提变成"系统强制确认"。
- 失败可回退到上一步（旧 MQ 还在，未删 PVC 前都可回滚）。

### 场景 3：改某个组件的 ConfigMap 并重启（对应 `configmap.html`）

```
选组件(milvus / kafka / etcd / ...) → 编辑器载入当前 ConfigMap
 → 改完点"预览 diff" → 确认 → apply → 选择重启策略(滚动/重建) → 回显结果
```
这是"组件之间怎么互相使用"的统一手法：**变更各自的 ConfigMap，然后重启**。webui 把这套动作标准化、可视化、可审计。

### 场景 4：升级与版本检查（对应 `upgrade.html`）

```
表格列出每个组件:当前版本 | 最新可用版本 | 来源(operator/chart) | [检查更新] [升级]
 → 点"检查更新"拉取各 operator/chart repo 的最新 tag
 → 点"升级"走各组件官方的升级路径(operator 改 image / helm upgrade)
```

### 场景 5：组件详情与工具（对应 `component.html` / `tools.html`）

- 每个组件一个详情页：状态、副本、资源、ConfigMap、事件、日志入口、操作（重启/扩缩容/卸载）。
- 工具箱：Attu（可视化客户端）、Birdwatcher（元数据诊断）、Log Export（日志导出）——一键打开 / 运行。

---

## 4. 架构

### 4.1 总体分层

```
┌─────────────────────────────────────────────────────────────┐
│  浏览器 (企业控制台 UI)                                         │
│  Vue/React 单页 · 深色侧边栏 · 仪表盘/向导/编辑器               │
└───────────────▲──────────────────────────┬──────────────────┘
                │ REST / WebSocket(进度流)   │
┌───────────────┴──────────────────────────▼──────────────────┐
│  Python 后端 (FastAPI)  —— Milvus Enterprise Admin WebUI       │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐ │
│  │ API 层        │ │ 编排引擎      │ │ 依赖规则引擎          │ │
│  │ /components   │ │ Orchestrator │ │ DependencyRules      │ │
│  │ /install      │ │ (任务/状态机) │ │ (woodpecker⇒storage) │ │
│  │ /switch-mq    │ └──────┬───────┘ └──────────────────────┘ │
│  │ /configmap    │        │                                   │
│  │ /upgrade      │ ┌──────▼───────────────────────────────┐ │
│  └──────────────┘ │ 执行器 Executors(适配各组件原生方式)    │ │
│                   │  ├ KubeClient   (kubectl/client-go API) │ │
│                   │  ├ HelmRunner   (helm upgrade/uninstall)│ │
│                   │  └ OperatorCR   (下发各 operator 的 CR) │ │
│                   └──────┬───────────────────────────────┘   │
└──────────────────────────┼───────────────────────────────────┘
                           │ K8s API
┌──────────────────────────▼───────────────────────────────────┐
│  Kubernetes 集群                                               │
│  各组件【官方】operator / chart 各管各的:                      │
│   etcd-op │ MinIO-op │ Strimzi(kafka) │ Pulsar-op │ milvus-op │
│   ……admin webui 不代管它们,只下发 CR / values 并读状态         │
└───────────────────────────────────────────────────────────────┘
```

### 4.2 关键设计点

| 模块 | 职责 | 实现要点 |
| --- | --- | --- |
| **执行器 Executors** | 把"安装/升级/卸载/改配置"翻译成对应组件的原生动作 | 三种适配器：`KubeClient`（client-go/kubectl）、`HelmRunner`（subprocess 调 helm）、`OperatorCR`（apply CR）。**关键：webui 自己不实现任何中间件逻辑，只调原生工具** |
| **编排引擎 Orchestrator** | 把多步操作（如 switch MQ 三步）做成可观测的状态机 | 每个任务一个状态机 + 进度事件流（WebSocket 推给前端）；失败可重试/回退 |
| **依赖规则引擎** | 把组件间依赖在**界面层**显式表达 | 声明式规则，如 `woodpecker requires objectStorage`、`milvus requires etcd & objectStorage & one-of(mq)`；安装向导据此启用/禁用选项 |
| **状态聚合** | 把各 operator/CR/pod 的状态汇成统一视图 | watch 各 CR status + pod readiness，缓存后供仪表盘读取 |
| **审计 / RBAC** | 控制台天然可加操作日志与权限 | 每个变更落审计日志；接 K8s RBAC / 企业 SSO |

### 4.3 一次"安装 Kafka"的调用链（举例）

```
前端点击"安装 Kafka"
  → POST /install {component: kafka, version: 3.x, blueprint: ...}
  → Orchestrator 起任务,查依赖规则(kafka 无强依赖,放行)
  → Executor 选择 OperatorCR 适配器
  → 检查 Strimzi operator 是否就位(没有则提示先装 operator)
  → apply 一个 Kafka CR 给 Strimzi
  → watch Kafka CR.status,进度经 WebSocket 实时回显
  → 完成,仪表盘组件矩阵里 kafka 变绿
```

**注意**：webui 没有写一行 kafka 的部署逻辑——它只是把 CR 交给 Strimzi。这就是"纯净丝滑"。

---

## 5. 原型说明

可点击原型在 [`../prototype/`](../prototype/)，纯静态 HTML + 一点 JS，全假数据，所有"执行"只弹提示，可互相跳转。

| 页面 | 文件 | 演示什么 |
| --- | --- | --- |
| 总览仪表盘 | `index.html` | 集群健康、组件状态矩阵、版本更新提醒、快捷入口 |
| 安装向导 | `install.html` | 搭积木选组件、依赖规则高亮、拓扑预览 |
| **MQ 切换向导** | `switch-mq.html` | 三步有护栏的切换流程（核心） |
| ConfigMap 编辑器 | `configmap.html` | 选组件 → diff → apply → 重启 |
| 升级 & 版本检查 | `upgrade.html` | 各组件版本对比、检查更新、一键升级 |
| 组件详情 | `component.html?c=woodpecker` | 单组件状态/配置/操作（按参数渲染不同组件） |
| 工具箱 | `tools.html` | Attu / Birdwatcher / Log Export |

> 原型目的：把"轻量 admin webui + 各组件原生 operator"的产品形态**可视化**，供评审对齐方向，不含真实后端对接。

---

## 6. 落地路线（MVP → 完整）

| 阶段 | 范围 | 价值 |
| --- | --- | --- |
| **MVP-0 只读纳管** | 接 K8s API，把现有 milvus + 依赖的状态聚合成仪表盘（只读） | 零风险，先有"统一视图" |
| **MVP-1 核心 switch MQ** | `switch-mq.html` 后端落地：改 ConfigMap + 重建 + 护栏 | 直接解决 3.0 的痛点 |
| **MVP-2 ConfigMap 管理** | 任意组件的 configmap 编辑 + diff + 重启 | 把"改配置重启"标准化 |
| **MVP-3 安装向导** | 搭积木安装，对接 etcd/MinIO/Strimzi/Pulsar/milvus 官方 operator | 真正的"组件各管各" |
| **MVP-4 升级 / 模板 / 审计** | 版本检查、蓝图模板、操作审计、RBAC | 产品化、可商用 |

---

## 7. 与方案一/二的关系

- 方案二的手册 = 方案三 `switch-mq.html` 向导背后的"人肉版"，webui 把每一步、每个前提校验自动化。
- 方案一对 helm/operator 的改造 = 方案三**底层执行器**可复用的能力（operator 支持删单个 MQ，webui 调它即可）。
- **三者不冲突**：方案二止血、方案一补工具、方案三把这些能力收进一个所见即所得的控制台，并把"安装"彻底从 milvus 内核工具里剥离出去。这才是终态。
