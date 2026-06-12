# 方案一：扩展 helm / operator，支持多 MQ 与删除单个 MQ 服务

> 面向读者：milvus-helm / milvus-operator 研发。
>
> 目标：在**不另起炉灶**的前提下，改造现有工具，让它 ① 安装期不再假设"MQ 只能有一种且互斥"，② `upgrade` 能安全地**删除其中某一个** MQ 服务。
>
> 立场：这是**过渡方案**。每多支持一种组合，模板/reconcile 的 if-else 就更复杂一分，纯粹度持续下降——文末"为什么越改越不纯粹"会量化这一点。

---

## 1. 现状：耦合点在哪

### 1.1 Helm：一条带优先级的互斥 if-else 链

`charts/milvus/templates/config.tpl`（约 82–170 行）按固定优先级选 MQ，并把地址注入渲染后的 `milvus.yaml`：

```
externalPulsar.enabled → pulsar(外部)
  └ else pulsar.enabled       → pulsar(内置 v2)
      └ else woodpecker.enabled → woodpecker
          └ else pulsarv3.enabled → pulsar(内置 v3, 默认)
（之后独立判断）externalKafka.enabled → kafka(外部) else kafka.enabled → kafka(内置)
```

同时 `charts/milvus/requirements.yaml` 把 etcd / minio / pulsarv2 / pulsarv3 / kafka 作为**条件子 chart**（`condition: pulsar.enabled` 等）。**"装不装 pulsar" 与 "milvus 连不连 pulsar" 共用一个开关。**

### 1.2 Operator：一个互斥枚举 `switch`

```go
// pkg/controllers/dependencies.go
func (r *MilvusReconciler) ReconcileMsgStream(ctx, mc) error {
    switch mc.Spec.Dep.MsgStreamType {        // ← 互斥枚举
    case MsgStreamTypeKafka:  return r.ReconcileKafka(ctx, mc)   // helm 装 kafka
    case MsgStreamTypePulsar: return r.ReconcilePulsar(ctx, mc)  // helm 装 pulsar
    default:                  return nil                          // 内置 MQ 不装
    }
}
```

- 默认值：`milvus_webhook.go:483-496` → cluster 默认 pulsar，standalone(≥2.6) 默认 woodpecker。
- 切换无拦截：`ValidateUpdate`（`milvus_webhook.go:131-151`）**不校验 `MsgStreamType` 是否被改**，但内核 `mq.type` 实质不可热切，所以改了也不会无损迁移。
- 旧 MQ 清理：靠 finalizer + `InCluster.DeletionPolicy`（`pkg/controllers/milvus.go:103-130`），默认 `Retain`。

---

## 2. 要改什么（两个独立改造）

### 改造 A：放开"安装期"的 MQ 互斥假设

**核心思想**：把"装哪些中间件"（安装意图，可多选）与"milvus 连哪个 MQ"（运行意图，单选）**拆成两个语义**。

#### A-1. Helm 侧

引入一个**显式的运行时选择字段**，把"装"和"连"解耦：

```yaml
# values.yaml 新增（示意）
messageQueue:
  # 运行时：milvus 实际连哪个（单选）。不再从 *.enabled 反推
  active: woodpecker         # [pulsar|kafka|woodpecker|rocksmq]
  # 安装期:允许多个 MQ 子系统并存（多选），用于灰度/切换窗口
  provision:
    pulsar: false
    kafka:  true             # 例如:切换窗口里 kafka 和 woodpecker 同时在
    woodpecker: true
```

- `requirements.yaml` 的 `condition` 从 `pulsar.enabled` 改为 `messageQueue.provision.pulsar`（**装**由 provision 决定）。
- `config.tpl` 的 MQ 注入**不再走 if-else-if 优先级链**，改为直接读 `messageQueue.active`（**连**由 active 决定）：

```gotemplate
{{- $mq := .Values.messageQueue.active }}
mq:
  type: {{ $mq }}
{{- if eq $mq "pulsar" }}
pulsar:
  address: {{ include "milvus.pulsar.address" . }}
  port:    {{ .Values.pulsar.proxy.ports.pulsar }}
{{- else if eq $mq "kafka" }}
kafka:
  brokerList: {{ include "milvus.kafka.brokerList" . }}
{{- else if eq $mq "woodpecker" }}
# woodpecker 走对象存储,地址由 minio/externalS3 推导
{{- end }}
```

> 关键差异：**provision（装）是集合，active（连）是单值**。二者独立后，"装了 kafka 但还连 woodpecker"（切换准备期）才表达得出来。保留旧 `*.enabled` 字段做兼容映射（`*.enabled` 同时置 provision 与 active），老 values 无感。

#### A-2. Operator 侧

`MilvusDependencies` 增加并存表达。当前 `MsgStreamType` 是单选枚举且 `Pulsar`/`Kafka`/`WoodPecker` 字段已并列存在，可这样演进：

```go
// dependencies_types.go 演进(示意)
type MilvusDependencies struct {
    // 运行时:milvus 实际连的 MQ(单选,保持兼容)
    MsgStreamType MsgStreamType `json:"msgStreamType,omitempty"`

    // 新增:安装期希望保留在集群里的 MQ 集合(切换窗口用)
    // 缺省 = {MsgStreamType},即老行为
    // +optional
    ProvisionMsgStreams []MsgStreamType `json:"provisionMsgStreams,omitempty"`

    Pulsar     MilvusPulsar    `json:"pulsar,omitempty"`
    Kafka      MilvusKafka     `json:"kafka,omitempty"`
    WoodPecker MilvusBuiltInMQ `json:"woodpecker,omitempty"`
    // ...
}
```

`ReconcileMsgStream` 从"单 switch"改为"对 provision 集合逐个 reconcile，连接配置只按 `MsgStreamType` 写"：

```go
func (r *MilvusReconciler) ReconcileMsgStream(ctx, mc) error {
    provision := mc.Spec.Dep.ProvisionMsgStreams
    if len(provision) == 0 {
        provision = []MsgStreamType{mc.Spec.Dep.MsgStreamType} // 兼容老行为
    }
    for _, mq := range provision {           // ← 安装期:可多个
        switch mq {
        case MsgStreamTypeKafka:  if err := r.ReconcileKafka(ctx, mc);  err != nil { return err }
        case MsgStreamTypePulsar: if err := r.ReconcilePulsar(ctx, mc); err != nil { return err }
        }
    }
    return nil // milvus 容器的 mq.type 仍只由 MsgStreamType 决定(运行期:单选)
}
```

### 改造 B：让 upgrade 支持"删除其中一个 MQ 服务"

这是 switch MQ 的下半场：切到新 MQ 后，要能**干净下线旧 MQ**。

#### B-1. Operator：把"删除某个 provision MQ"做成受控操作

当 `ProvisionMsgStreams` 从 `{woodpecker, kafka}` 收敛回 `{woodpecker}` 时，operator 应当卸载 kafka 的 helm release。复用现有 finalizer 清理逻辑（`pkg/controllers/milvus.go:103-130`），但从"删实例才触发"扩展为"**provision 集合收缩时也触发**"：

```go
// 伪代码:对比 desired/current provision,卸载被移除的 MQ
removed := diff(currentProvision, desiredProvision) // 被踢出集合的 MQ
for _, mq := range removed {
    switch mq {
    case MsgStreamTypeKafka:
        if !mc.Spec.Dep.Kafka.External &&
           mc.Spec.Dep.Kafka.InCluster.DeletionPolicy == DeletionPolicyDelete {
            helmUninstall(mc.Name + "-kafka")
            if mc.Spec.Dep.Kafka.InCluster.PVCDeletion { deletePVCs(...) }
        }
    case MsgStreamTypePulsar:
        // 同理卸载 mc.Name + "-pulsar"
    }
}
```

**护栏（必须做）：**
1. **禁止删除 active MQ**：若被删 MQ == `MsgStreamType`，webhook 直接拒绝（见 B-3）。
2. **`DeletionPolicy` 默认 `Retain`**：删除 release 不自动删 PVC，除非显式 `PVCDeletion: true`。
3. **删除前置校验**：active 已切到新 MQ 且新 MQ 就绪，才允许下线旧 MQ。

#### B-2. Helm：`upgrade` 卸载被关掉的子 chart

helm 本身在 `helm upgrade` 时，把 `condition` 变 false 的子 chart 资源删掉。所以只要 `messageQueue.provision.kafka: false`，`helm upgrade` 即会移除 kafka 子 chart 的 Deployment/STS/SVC。**但 helm 不会删 PVC**（PVC 默认保留）——这正是我们要的安全默认；删 PVC 作为单独的、显式的运维动作。

> 注意 woodpecker 是手写 STS（非子 chart），其下线要在模板里按 `messageQueue.provision.woodpecker` 加 `{{- if }}` 包裹，否则 `helm upgrade` 不会移除它。

#### B-3. Webhook：把"切换"变成受控状态机，而非裸改字段

现状 `ValidateUpdate` 不拦 `MsgStreamType`。改造后应加入**有限的状态校验**（不是完全锁死，而是给护栏）：

```go
func (r *Milvus) validateMsgStreamSwitch(old *Milvus) field.ErrorList {
    var errs field.ErrorList
    // 1) 新 active 必须在 provision 集合里(不能连一个没装的 MQ)
    if !contains(r.Spec.Dep.ProvisionMsgStreams, r.Spec.Dep.MsgStreamType) {
        errs = append(errs, field.Invalid(..., "active MQ must be provisioned first"))
    }
    // 2) 不能在同一次变更里既切 active 又删掉旧 active 的 provision(必须分两步:先切,再下线)
    // 3) 改 MsgStreamType 时给 Warning:数据不可迁移,等价于空集群换后端
    return errs
}
```

> 这一步把"switch MQ"从"随手改个枚举"升级为"**先 provision 新 MQ → 切 active → 校验 → 再下线旧 MQ**"的两阶段安全流程。

---

## 3. 完整改动清单

| # | 仓库 | 文件 | 改动 |
| --- | --- | --- | --- |
| 1 | helm | `charts/milvus/values.yaml` | 新增 `messageQueue.active` / `messageQueue.provision.*`；保留旧 `*.enabled` 做兼容映射 |
| 2 | helm | `charts/milvus/requirements.yaml` | `condition` 指向 `messageQueue.provision.*` |
| 3 | helm | `charts/milvus/templates/config.tpl` | MQ 注入改为读 `messageQueue.active`，去掉优先级 if-else-if 链 |
| 4 | helm | `templates/woodpecker-statefulset.yaml` 等 | 用 `if messageQueue.provision.woodpecker` 包裹，支持 upgrade 下线 |
| 5 | helm | `templates/_helpers.tpl` | 新增 `milvus.pulsar.address` / `milvus.kafka.brokerList` 等 helper，兼容映射逻辑 |
| 6 | operator | `apis/.../dependencies_types.go` | 新增 `ProvisionMsgStreams []MsgStreamType` |
| 7 | operator | `pkg/controllers/dependencies.go` | `ReconcileMsgStream` 改为遍历 provision 集合 |
| 8 | operator | `pkg/controllers/milvus.go` | finalizer 清理逻辑扩展为"provision 收缩即卸载被移除 MQ" |
| 9 | operator | `apis/.../milvus_webhook.go` | 新增 `validateMsgStreamSwitch`：active 必须已 provision、两阶段切换、改 MQ 给 Warning |
| 10 | operator | `apis/.../milvus_webhook.go` | `setDefault*`：provision 缺省 = `{MsgStreamType}`，保证老 CR 行为不变 |
| 11 | 两者 | e2e / chart 测试 | 增加"切换矩阵"用例：woodpecker↔kafka↔pulsar、删除单 MQ、删 PVC 护栏 |

---

## 4. 操作示例：从 Woodpecker 切到 Kafka（operator）

```yaml
# 第 1 步:provision kafka(此时仍连 woodpecker),helm 装好 kafka
spec:
  dependencies:
    msgStreamType: woodpecker          # active 仍是 woodpecker
    provisionMsgStreams: [woodpecker, kafka]   # 安装期并存
    kafka:
      inCluster:
        deletionPolicy: Retain
```

```yaml
# 第 2 步:校验 kafka 就绪、集群数据已清空后,切 active
spec:
  dependencies:
    msgStreamType: kafka               # ← active 切到 kafka(触发 milvus 重建)
    provisionMsgStreams: [woodpecker, kafka]
```

```yaml
# 第 3 步:下线旧 woodpecker
spec:
  dependencies:
    msgStreamType: kafka
    provisionMsgStreams: [kafka]       # ← 移除 woodpecker,operator 卸载其 STS/PVC(按策略)
```

> 每一步都是一次 `kubectl apply` + operator reconcile，webhook 在第 2 步会给出"数据不可迁移"Warning。

---

## 5. 为什么"越改越不纯粹"

| 症状 | 说明 |
| --- | --- |
| **组合爆炸** | `active ∈ {4 种 MQ}` × `provision ⊆ {4 种}` × `standalone/cluster` × `internal/external`，测试矩阵指数级膨胀 |
| **越界依旧** | 我们还是在用自己维护的 pulsar/kafka 子 chart 代管别人的中间件，版本滞后、能力缺失（如 kafka 的 KRaft、分层存储）依旧 |
| **语义漂移** | `*.enabled` / `active` / `provision` 三套字段并存，新老用户都要理解"装"和"连"的区别，心智负担转嫁给用户 |
| **生命周期半吊子** | helm 不删 PVC、operator finalizer 删，删除语义分裂在两处，用户难以形成稳定心智 |
| **治标不治本** | 真正的问题——"一个工具越界管所有组件"——没有被解决，只是被推迟 |

**结论**：方案一能在数周内让 3.0 的 switch MQ "工具化、可控化"，是合理的过渡。但它本质是在一个"该拆"的架构上继续叠加 if-else，**纯粹度持续走低**。它买的是时间，不是终局。终局见 [方案三](03-方案三_enterprise-admin-webui.md)。
