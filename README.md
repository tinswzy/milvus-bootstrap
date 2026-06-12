# Milvus Admin WebUI — 资源解耦方案与原型

> 围绕 **Milvus 3.0 "switch MQ"** 诉求，论证如何把"底层资源的安装编排"与"Milvus 运行时连线"两件事解耦，并给出三种落地方案的对比、各自的实施细则，以及一套轻量管理控制台（Milvus Admin WebUI）的可点击原型。

## 背景一句话

Milvus 3.0 内部已经把消息队列（MQ）抽象在统一的 **WAL 接口**（`WALImpls`）之后，理论上 `pulsar / kafka / woodpecker / rocksmq` 是可插拔的；但**安装期**（谁来 helm install pulsar/kafka）和**运行期**（milvus 该连哪个 MQ）这两件本应独立的事，被现有的 milvus-helm 和 milvus-operator **耦合**进了同一套 if-else 互斥假设里。结果就是"换 MQ"这种本该轻量的操作，变成了牵一发动全身的重活。

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
