# Milvus Admin WebUI — 资源解耦方案与原型

> 围绕 **Milvus 3.0 "switch MQ"** 诉求，论证如何把"底层资源的安装编排"与"Milvus 运行时连线"两件事解耦，并给出三种落地方案的对比、各自的实施细则，以及一套轻量管理控制台（Milvus Admin WebUI）的可点击原型。

## 背景一句话

Milvus 3.0 内部已经把消息队列（MQ）抽象在统一的 **WAL 接口**（`WALImpls`）之后，理论上 `pulsar / kafka / woodpecker / rocksmq` 是可插拔的；但**安装期**（谁来 helm install pulsar/kafka）和**运行期**（milvus 该连哪个 MQ）这两件本应独立的事，被现有的 milvus-helm 和 milvus-operator **耦合**进了同一套 if-else 互斥假设里。结果就是"换 MQ"这种本该轻量的操作，变成了牵一发动全身的重活。

## 设计原则（核心思想 · 轻量之本）

mb 的一切设计都必须守住这几条——**这是它保持轻量的关键，后续每个新功能都要对照它**：

- **简单（Simple）**：最少的活动部件。瘦 CLI + 瘦 daemon；能不加组件就不加，能复用就复用。
- **幂等（Idempotent）**：所有操作可安全重复。读全是纯 GET、无副作用；重复 apply 同一 spec 对底层是 no-op。用户随便点、随便重试都安全。
- **无状态（Stateless）**：**集群（operator / CR）才是唯一真相**；mb 自己的 state 只是可随时重建的缓存，丢了能从集群重推。mb 不做权威账本。
- **无轮询（No polling）**：mb **绝不后台持续轮询 k8s**。所有状态都是**按需一次性读**，由用户动作触发（CLI 命令 / UI 点刷新 / 切页面）。没有常驻循环制造无谓请求。
- **用户 / CLI 驱动（User-driven）**：动作只在用户发起时发生。mb **不跑自主后台 reconcile**——那是 operator 的职责。mb 只负责「把改动 apply 下去、交给 operator」，然后把控制权交还用户；用户想看进度时自己拉。
- **透明 / 可观测（Transparent，不是黑盒）**：mb 不是黑盒。每次 install / upgrade / delete / dry-run 都暴露**分步骤日志**——每一步做了什么、执行的**实际 k8s/helm 命令**、结果 / 错误都可查，便于核对步骤是否正确、准确。日志**来自 mb 自身的执行**、**按需、有界**（操作完即停），不是对集群的持续监控——与「无轮询」一致：唯一的短时轮询针对 mb 自己的内存任务记录，而非 k8s。

**一条推论（很重要）**：mb apply 完就返回，**真正的收敛由 operator 驱动、mb 不盯**。所以：
- **不谎报成功**：mb 只能说「已提交 / 已 apply」，不能把「升级瞬间仍是旧状态的 Healthy」当成升级成功。真·成败由用户**按需查集群**（CR 状态 + pod 镜像 + 日志）得知。
- 进度 / 日志一律**按需拉取 + 手动刷新**，不做实时监控。

> 一句话：**mb 是一把「幂等、按需、把活交给 operator」的瘦扳手，不是常驻监控 / 调度器。** 谁想加「实时监控 / 自动轮询 / 后台 reconcile / 权威状态机」，先问这是不是违背了轻量之本。

## 仓库结构导览

三类内容一眼分清：**① 产品代码** / **② 设计·讨论文档** / **③ 原型·沟通页**。

```
milvus-admin-webui/
├── milvus-bootstrap/        ★① 真正的实现代码（mb —— CLI 工具 + FastAPI daemon）
│   ├── src/milvus_bootstrap/
│   │   ├── cli/main.py          瘦 CLI（install / upgrade / switch-mq / doctor …）
│   │   ├── server/              FastAPI daemon（跑真正逻辑的常驻进程）
│   │   ├── client/              CLI ↔ daemon 通信 transport
│   │   ├── core/                ★ 业务核心
│   │   │   ├── engines/             provisioner / lifecycle / discovery / ownership / config
│   │   │   ├── drivers/             etcd / minio / kafka / milvus / woodpecker 组件驱动
│   │   │   ├── platform/            k8s / fake 适配器
│   │   │   ├── state/               文件状态存储
│   │   │   ├── compat.py+compat.yaml  兼容矩阵 / 操作门禁
│   │   │   ├── probe.py doctor.py     版本探测 / mb doctor 环境预检
│   │   │   └── context.py models.py registry.py profile.py
│   │   └── profiles/            各组件 YAML profile（安装拓扑 / 版本 / 连线规则）
│   ├── tests/                   pytest 单测
│   └── scripts/                 辅助脚本（mb-live-env.sh …）
├── docs/                    ② 设计 / 方案讨论文档（见下「文档导航」）
│   └── superpowers/             spec + 实现 plan（mb doctor 等特性）
├── prototype/               ③ 可点击 HTML 原型 + phase1 逐步验收页（纯展示，见下「原型」）
├── serve-docs.py            零依赖文档服务器（本地起 http 看 html/md，开发沟通用）
└── README.md
```

- **要读 / 改产品，只进 `milvus-bootstrap/src/milvus_bootstrap/` 与 `milvus-bootstrap/tests/`。** 其余（`docs/`、`prototype/`、`serve-docs.py`）都是围绕它的设计、讨论、进度沟通，不参与运行。
- 本地看这些文档：`python3 serve-docs.py 8899`，浏览器开 `http://<本机IP>:8899/`（HTML 直接渲染、`.md` 自动渲染）。
- `mb` 工具用法与安装：见 [milvus-bootstrap/README.md](milvus-bootstrap/README.md)。

## 文档导航

| 文档 | 面向读者 | 内容 |
| --- | --- | --- |
| [docs/00-方案对比报告.md](docs/00-方案对比报告.md) | 架构 / 管理决策层 | 问题本质拆解、三方案多维对比表、推荐路线与风险 |
| [docs/01-方案一_helm-operator扩展.md](docs/01-方案一_helm-operator扩展.md) | 研发 | 改造 helm/operator 的 MQ 互斥 if-else，让 upgrade 支持删除其中某个 MQ 服务 |
| [docs/02-方案二_k8s原生操作.md](docs/02-方案二_k8s原生操作.md) | 研发 / SRE | 不改 helm/operator，安装后用 kubectl/helm 原生操作拆掉其中一个服务（以 kafka 为例）的逐步手册 |
| [docs/03-方案三_enterprise-admin-webui.md](docs/03-方案三_enterprise-admin-webui.md) | 研发 / 产品 | toB 厂商最佳实践对标 + Milvus Admin WebUI 原型说明 + Python/Web 架构 |

## 原型（全假数据、可互相跳转的静态 HTML）

入口：[prototype/index.html](prototype/index.html)

| 页面 | 作用 |
| --- | --- |
| `index.html` | 总览仪表盘：集群健康、组件状态矩阵、版本更新提醒、快捷入口 |
| `install.html` | 安装向导：像搭积木一样勾选 etcd / 对象存储 / MQ / Milvus / 工具 |
| `switch-mq.html` | **★ MQ 切换向导**（核心场景）：校验 → 改 ConfigMap → 重建 |
| `configmap.html` | ConfigMap 编辑器：选组件 → diff → apply → 重启 |
| `upgrade.html` | 升级 & 检查最新版本：各 operator / 组件版本对比 |
| `component.html` | 通用组件详情（`?c=woodpecker/kafka/etcd/...`） |
| `tools.html` | 工具箱：Attu / Birdwatcher / Log Export |

> 原型只演示交互与信息架构，所有数据为假、所有"执行"只弹 toast，不真实对接后端。

## 一句话结论

- **短期止血**：方案一或方案二，先满足 3.0 的 switch MQ；
- **战略解耦**：方案三，用一个**轻量** admin webui 编排各组件**自己的**原生 operator/helm，彻底把"安装"与"运行"拆开——每个组件像积木，依赖关系只在 webui 层显式表达，底层互不越界。

> 命名说明：刻意不叫 "Hub"。Hub 会让人误以为它把所有组件的管理都吞进自己，恰恰违背"各组件由自己 operator 管"的初衷。叫 **Admin WebUI**，强调它只是薄薄一层编排与配置面板。
