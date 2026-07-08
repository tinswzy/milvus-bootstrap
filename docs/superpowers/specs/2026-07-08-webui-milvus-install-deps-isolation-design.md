# WebUI Milvus 安装：依赖下拉 + 数据隔离前缀 + dry-run 查重 · 设计

- 日期：2026-07-08
- 状态：设计已确认（含验证性安装实测），待写实现计划
- 范围：Milvus 安装表单专项——依赖 etcd/存储/MQ 改下拉、新增「数据隔离前缀」字段（默认=实例名、可改、注入 spec.config）、dry-run 查重（实例名重复 + 前缀在共享依赖上撞车）。顺带修 `spec.conf`→`spec.config` 注入 bug。

## 1. 背景与实测结论

Milvus 复用同一套 kafka/pulsar/etcd/minio 时，靠 `msgChannel.chanNamePrefix.cluster` / `etcd.rootPath` / `minio.bucketName` = 实例名做数据隔离（operator 自动按 CR 名设）。用户要：(A) 依赖改下拉更好选；(B) 把隔离前缀显式暴露为可改字段（默认=名）；(C) dry-run 查重防撞。

**验证性安装实测（2026-07-08，已清理）确认**：
- CRD `milvus.io/v1beta1` 只有 **`spec.config`**（嵌套 map）；**`spec.conf` 不存在**（`kubectl explain milvus.spec.conf`→报错）。
- **★现有 bug**：`drivers/milvus.py` 写 `cr_spec["conf"]={"data":conf}`（=死字段 `spec.conf.data`）→ 被 k8s 剪掉、从不生效（mb `config set` 目前空操作）。**注入必须改用 `spec.config`（嵌套）**。
- **★operator 尊重覆盖**：在 `spec.config.{msgChannel.chanNamePrefix.cluster, etcd.rootPath, minio.bucketName}` 显式设 `custompfx` → operator 渲染的 configmap `user.yaml` 三键全 `custompfx`（不是 CR 名），且与 operator 默认 merge。→ **可编辑前缀真能生效**。

现状（已核实）：
- 前端 `webui/install.html` + web.js `renderInstall`：kind 下拉 + 实例名 + k8s 命名空间(`inst-ns`) + 通用 key=value 参数编辑器（`#inst-params`）。milvus 预填 mq/image/storageEndpoint/kafkaBrokers 为文本参数。
- 后端 `InstallReq{kind,name,method,namespace,params}` → `InstallSpec`；dry-run 走 `_core().install(spec,dry_run=True)` → `provisioner.install`（`provisioner` 持有 `self.state`）。
- milvus driver `build_install_manifests`：组 `cr_spec{mode,components,dependencies}`，MQ 经 `_mq_deps`。deps endpoints 从 `params.etcdEndpoints`/`storageEndpoint`/`kafkaBrokers`/`pulsarEndpoint`。
- `/api/instances` 已能按 kind 列 etcd/minio/kafka/pulsar 实例（managed+external）；`depEndpoint(kind,name,ns)` 前端已有。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 一个切面 | 依赖下拉 + 隔离前缀 + 查重 合并做 |
| D2 | 隔离前缀语义 | 新字段（≠k8s 命名空间），默认=实例名（mirror-until-edited），控制三键 |
| D3 | 注入字段 | **`spec.config`（嵌套）**：`msgChannel.chanNamePrefix.cluster` + `etcd.rootPath` + `minio.bucketName` = 前缀。始终注入（WYSIWYG，=名时与 operator 默认一致）|
| D4 | 顺带修 | `_conf`（config set 那条路）也改走 `spec.config`（去掉死的 `spec.conf`）；dotted-key→nested + deep-merge。isolation 键优先 |
| D5 | 依赖下拉 | etcd/存储/MQ 从 `/api/instances` 按 kind 列；每个保留「自定义」→ 文本框回退 |
| D6 | dry-run 查重（milvus） | (1) 实例名不得与任一现有 mb 实例重名；(2) 隔离前缀不得与另一 milvus 在**共享任一依赖端点**时相同 |
| D7 | 查重深度 | 只按 mb state 查（不实探 etcd/minio 里既有 rootPath/bucket）；非-milvus 表单不动 |

## 3. 架构与模块

| 模块 | 职责 | 变更 |
|---|---|---|
| `core/drivers/milvus.py` | 注入隔离前缀到 `spec.config`；`_conf` 也进 `spec.config`（去 `spec.conf`）；`dotted_to_nested`/`deep_merge` helper | 改 |
| `core/engines/provisioner.py`（或校验 helper） | milvus dry-run/install 前置校验：名重复 + 前缀共享依赖撞车（读 state） | 改 |
| `webui/assets/web.js` `renderInstall` | 隔离前缀字段（mirror）+ etcd/存储/MQ 下拉（+自定义回退）+ params 带 `isolationPrefix` | 改 |
| `webui/install.html` | milvus 专区容器（下拉 + 前缀字段的挂点） | 改 |

**边界**：注入是驱动纯函数；校验读 state（provisioner 层，CLI+UI 共享）；前端只渲染+组 params。非-milvus 安装路径不变。config-get 是否读旧 `spec.conf` 不在本切面范围（若读则本就返回空——预先存在，标注为后续）。

## 4. Part A — 依赖下拉（前端）

`renderInstall` 里，当 kind=milvus 时渲染结构化区（替掉通用 key=value 对 milvus 依赖的手填）：
- **etcd 依赖** `<select>`：选项 = `/api/instances` 里 kind=etcd 的实例（value=`depEndpoint('etcd',name,ns)`，label=`name (ns)`）+ 末尾「自定义…」。选自定义 → 显示文本框，值进 `etcdEndpoints`。
- **存储依赖** `<select>`：kind=minio 实例（value=`depEndpoint('minio',...)`）+ 自定义 → `storageEndpoint`。
- **MQ 类型** `<select>`：kafka/pulsar/woodpecker-service/woodpecker-embedded/rocksmq。若选 kafka/pulsar → **MQ 实例** `<select>`（对应 kind 实例 + 自定义）→ `kafkaBrokers`/`pulsarEndpoint`。woodpecker-embedded/rocksmq 无需端点。
- 镜像仍文本框。
- 组 install body 时把这些拼进 `params`（键名与现有一致：etcdEndpoints/storageEndpoint/kafkaBrokers/pulsarEndpoint/mq/image）。
- 下拉数据在进入 milvus 表单时 `fetch('/api/instances')` 拉一次。

## 5. Part B — 隔离前缀字段 + 注入

**前端**：milvus 表单加「数据隔离前缀」输入，`id=inst-iso`。实例名输入 `oninput` 时，若前缀未被手动改过则同步填同值（mirror-until-edited：前缀框自己被编辑后置 `dirty` 标记停止同步）。组 body 时 `params.isolationPrefix = <前缀值 || 实例名>`。

**后端**（`drivers/milvus.py`）：
```python
prefix = (params.get("isolationPrefix") or name)
iso = {"msgChannel": {"chanNamePrefix": {"cluster": prefix}},
       "etcd": {"rootPath": prefix}, "minio": {"bucketName": prefix}}
config = deep_merge(dotted_to_nested(params.get("_conf") or {}), iso)   # iso 后合并→优先
if config:
    cr_spec["config"] = config
# 删除旧的 cr_spec["conf"] = {"data": conf}
```
`dotted_to_nested({"a.b.c": v})` → `{"a":{"b":{"c":v}}}`（键无 "." 原样）；`deep_merge(a,b)` 递归合并、b 覆盖。

## 6. Part C — dry-run 查重（后端，milvus）

校验 helper（在 `provisioner.install` 前置调用，仅 `spec.kind=="milvus"`；读 `self.state`）：
```
def check_milvus_install(state, spec):   # raise ValueError on conflict
    insts = state.list_instances()
    # (1) 名重复：任一现有实例同名 → 报错
    if any(i.name == spec.name for i in insts):
        raise ValueError(f"实例名 {spec.name} 已存在")
    # (2) 前缀 × 共享依赖：另一 milvus 前缀相同且共享任一依赖端点 → 报错
    new_prefix = spec.params.get("isolationPrefix") or spec.name
    new_eps = _dep_eps(spec.params)                       # {etcd endpoints, storage, mq endpoint} 归一为 set
    for i in insts:
        if i.spec_snapshot.get("kind") != "milvus": continue
        p = i.spec_snapshot.get("params", {})
        eff = p.get("isolationPrefix") or i.name           # 老实例默认=名
        if eff == new_prefix and (_dep_eps(p) & new_eps):
            raise ValueError(f"隔离前缀 {new_prefix} 已被 milvus {i.name} 在共享依赖上占用，请改前缀")
```
`_dep_eps(params)` = 把 etcdEndpoints(列表/串)、storageEndpoint、kafkaBrokers/pulsarEndpoint 归一成字符串集合。默认前缀=唯一名 → 天然不撞；只有手改成与他人相同、且真共享依赖才报。

## 7. 测试与验收

- **驱动**（`tests/test_milvus.py`）：build_install_manifests 出的 CR 有 `spec.config.msgChannel.chanNamePrefix.cluster == prefix`、`etcd.rootPath == prefix`、`minio.bucketName == prefix`；无 `spec.conf`；`_conf={"a.b":1}` → `spec.config.a.b==1` 且与 iso 共存；无 isolationPrefix 时 prefix=name。
- **校验**（`tests/`，fake state）：装一个 milvus 后再装同名 → ValueError；两个 milvus 同自定义前缀 + 共享 etcd 端点 → ValueError；同前缀但依赖不共享 → 放行；默认前缀（各自名）→ 放行。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js `renderInstall` 含 `inst-iso`、依赖 `<select>`、`自定义`、mirror 逻辑标记、`isolationPrefix`；install.html 有 milvus 专区挂点。
- **JS**：`node --check`。
- **手动 DoD**：milvus 表单——输名→隔离前缀自动跟随、可改；etcd/存储/MQ 是下拉（选实例即填端点，选自定义出文本框）；dry-run 装重名/撞前缀→报错条，正常→步骤预览；真装出的 CR `spec.config` 含三键=前缀（`kubectl get cm <name> -o jsonpath='{.data.user\.yaml}'` 可核）。

## 8. 非目标 / 后续
- config-get 是否需同步改读 `spec.config`（本切面只修注入侧；config-get 若读旧 `spec.conf` 本就返回空——预先存在，留后续）。
- 不实探 etcd/minio 现有 rootPath/bucket；非-milvus 表单不变；adopt UI 不做。
- 前缀被 operator 尊重已实测；不额外做「前缀 ≠ 名时的告警」。
