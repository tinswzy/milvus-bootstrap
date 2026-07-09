# WebUI Milvus 安装：逐依赖数据隔离配置（贴各依赖 + hover + 可单改）· 设计

- 日期：2026-07-08
- 状态：设计已确认，待写实现计划
- 范围：把 milvus 安装表单里单个「数据隔离前缀」拆成**四个逐依赖隔离值**，各默认=实例名、可单独改、显示在各自依赖盒子里、hover 解释它对应 milvus 哪个配置/影响什么。后端注入与 dry-run 查重都改成逐依赖。

## 1. 背景

上一切面用一个 `isolationPrefix` → 三键（etcd.rootPath / minio.bucketName / msgChannel.chanNamePrefix.cluster）一刀切，都=实例名。用户要：每个依赖的隔离配置**贴到它那条流线旁**、**hover 说明对应 milvus 的什么配置 + 影响**、**可逐个自定义**（默认仍=实例名）。minio 再细分 bucket 与 rootPath 两维。

现状（已核实）：
- milvus 驱动 `build_install_manifests`：`prefix = params.get("isolationPrefix") or name`；`iso = {msgChannel.chanNamePrefix.cluster, etcd.rootPath, minio.bucketName} = prefix`；`config = _deep_merge(_dotted_to_nested(_conf), iso)` → `cr_spec["config"]`。（`_dotted_to_nested`/`_deep_merge` 已有。）
- 查重 `check_milvus_install(instances, spec)`（provisioner）：名重复 + 「同 effective prefix 且共享任一依赖端点」。`_dep_eps(params)` 归一依赖端点集。
- 前端 `fillParams('milvus')`：拓扑表单（`.topo-edit`），中心核心盒有 `#inst-image` + `#inst-iso`（隔离前缀）+ `#iso-preview` 预览 chips；依赖盒里是各依赖下拉。`collectParams` milvus 分支发 `isolationPrefix`。
- **operator 尊重 spec.config 覆盖**（实测）；`spec.config` 是正字段（`spec.conf` 不存在）。见 [[project-milvus-operator-isolation]]。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 隔离模型 | 由单个 `isolationPrefix` 换成**四个独立值**，各默认=实例名、可单改、mirror-until-edited |
| D2 | 四个值 | `etcdRootPath`→etcd.rootPath；`minioBucket`→minio.bucketName；`minioRootPath`→minio.rootPath；`mqChanPrefix`→msgChannel.chanNamePrefix.cluster |
| D3 | 默认与注入 | 四个**全部默认=实例名、全部始终注入**（含 minio.rootPath=名，`s3://<名>/<名>/…` 便于识别） |
| D4 | 展示位置 | 各字段显示在**对应依赖盒子里**（etcd 盒:rootPath；存储盒:bucket+rootPath；MQ 盒:cluster），贴各自流线 |
| D5 | hover | 每字段 label 带 `title`，说明对应 milvus 配置键 + 作用 + 共用依赖时的意义 + 默认=名 |
| D6 | 查重 | 改逐依赖：名重复仍查；etcd 键=rootPath、mq 键=cluster（单键）；minio 键=`(bucket, rootPath)` 对；「共享该依赖 endpoint 且键值相同」才撞，分别报 |
| D7 | 非目标 | 公共镜像（下切面，逐 profile 镜像映射）；milvus 镜像本切面仍在核心盒填 |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/drivers/milvus.py` | 注入从单 prefix 改四键：`etcd.rootPath=params.etcdRootPath\|name`、`minio.bucketName=params.minioBucket\|name`、`minio.rootPath=params.minioRootPath\|name`、`msgChannel.chanNamePrefix.cluster=params.mqChanPrefix\|name`；deep-merge 进 spec.config。去掉 `isolationPrefix`。 |
| `core/engines/provisioner.py` | `check_milvus_install` 改逐依赖：名重复；etcd/mq 单键 + 共享 endpoint；minio `(bucket,rootPath)` 对 + 共享 endpoint。effective 值 = 快照对应键 or 那实例名。 |
| `webui/assets/web.js` `fillParams` | 去中心盒 `#inst-iso`/`#iso-preview`；依赖盒里各加隔离字段（`#inst-etcd-root`、`#inst-store-bucket`、`#inst-store-root`、`#inst-mq-prefix`），各 mirror 名、hover title；`collectParams` 发四键。 |
| `webui/assets/web.css` | 盒内隔离字段小样式（label + input） |

**边界**：仅 milvus。四值默认=名，缺省行为与「实例名唯一→天然不撞」一致。非-milvus 表单不动。

## 4. 后端注入（`drivers/milvus.py`）

替换现有 `prefix`/`iso` 段：
```python
n = name
iso = {
    "etcd": {"rootPath": params.get("etcdRootPath") or n},
    "minio": {"bucketName": params.get("minioBucket") or n,
              "rootPath": params.get("minioRootPath") or n},
    "msgChannel": {"chanNamePrefix": {"cluster": params.get("mqChanPrefix") or n}},
}
config = _deep_merge(_dotted_to_nested(params.get("_conf") or {}), iso)
if config:
    cr_spec["config"] = config
```
（去掉 `params.get("isolationPrefix")` 一行。）

## 5. 逐依赖查重（`provisioner.py`）

```python
def _iso_of(params, name):
    return {
        "etcd":  params.get("etcdRootPath")  or name,
        "minio": (params.get("minioBucket") or name, params.get("minioRootPath") or name),
        "mq":    params.get("mqChanPrefix")  or name,
    }
def _dep_ep_sets(params):        # 每类依赖各自的 endpoint 集合
    return {"etcd": <etcdEndpoints 集>, "minio": {storageEndpoint} if set else ∅,
            "mq": {kafkaBrokers/pulsarEndpoint} if set else ∅}

def check_milvus_install(instances, spec):
    if any(i.name == spec.name for i in instances): raise ValueError("实例名 X 已存在…")
    ni, ne = _iso_of(spec.params, spec.name), _dep_ep_sets(spec.params)
    for i in instances(kind==milvus):
        oi, oe = _iso_of(i.snap.params, i.name), _dep_ep_sets(i.snap.params)
        for dep, label, key in [("etcd","etcd","etcd.rootPath"), ("minio","对象存储","minio bucket/rootPath"), ("mq","MQ","chanNamePrefix.cluster")]:
            if (ne[dep] & oe[dep]) and ni[dep] == oi[dep]:
                raise ValueError(f"{label} 隔离与 milvus {i.name} 冲突（共享同一 {label} 且 {key} 相同），请改{label}隔离值")
```
minio 的 `ni["minio"]`/`oi["minio"]` 是 `(bucket, rootPath)` 元组，相等即两维都同。默认四值=各自唯一实例名 → 天然不撞。

## 6. 前端（`fillParams('milvus')`）

拓扑盒结构不变（etcd ▸ 核心 ▸ 存储 + MQ 下挂）。改动：
- **中心核心盒**：去掉 `#inst-iso` 与 `#iso-preview`；保留 M + 实例名(live) + `#inst-image`。
- **etcd 盒**：下拉下方加 `<label title="…">rootPath</label><input id="inst-etcd-root" class="f-in iso-in">`。
- **存储盒**：加两个 `<label>bucket</label><input id="inst-store-bucket">` + `<label>rootPath</label><input id="inst-store-root">`。
- **MQ 盒**：加 `<label>cluster</label><input id="inst-mq-prefix">`。
- 四字段各 mirror `#inst-name`（各自 dirty 标记）：名改则未手改的字段跟着变。
- hover `title` 文案（例）：
  - etcd rootPath：`etcd.rootPath —— Milvus 在 etcd 存元数据的根路径。共用同一 etcd 时用它区分不同 Milvus；默认=实例名。`
  - minio bucket：`minio.bucketName —— Milvus 对象存储的桶名，各 Milvus 一个桶；默认=实例名。`
  - minio rootPath：`minio.rootPath —— 桶内子路径前缀；想多个 Milvus 共用一个桶又互不干扰时改它；默认=实例名。`
  - mq cluster：`msgChannel.chanNamePrefix.cluster —— MQ topic/channel 名前缀，共用同一 kafka/pulsar 时避免撞名；默认=实例名。`
- `collectParams` milvus 分支：去 `isolationPrefix`，加 `etcdRootPath`/`minioBucket`/`minioRootPath`/`mqChanPrefix`（各=字段值 or 空→后端回退名）。

## 7. 测试与验收

- **驱动**（`tests/test_milvus.py`）：默认（无 iso 参数）→ `spec.config` 四键全=name（含 minio.rootPath）；给 `minioRootPath="p"` → minio.rootPath=="p"，其余仍=name；`_conf` 仍并入。
- **查重**（`tests/`，fake state）：装 mv-a（默认）后装 mv-b 同名→ValueError；mv-b 与 mv-a 共享同一 etcd endpoint 且 etcdRootPath 相同→ValueError；共享 minio 同 bucket 但 rootPath 不同→放行；共享 minio 同 bucket+同 rootPath→ValueError；默认四值（各自名）→放行。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js `fillParams` 含 `inst-etcd-root`/`inst-store-bucket`/`inst-store-root`/`inst-mq-prefix`、四键名、hover `title=`；不再含 `inst-iso`/`isolationPrefix`。
- **JS**：`node --check`。
- **手动 DoD**：milvus 表单——etcd 盒有 rootPath、存储盒有 bucket+rootPath、MQ 盒有 cluster，各默认=实例名、随实例名改、可单改；hover 出配置说明；真装某实例后 `kubectl get cm <name> -o jsonpath='{.data.user\.yaml}'` 见四键=各字段值。

## 8. 非目标 / 后续
- 公共镜像字段（下切面，逐 profile 镜像映射，全 kind 正确）。
- config-get 是否同步（预先存在，前切面已 flag）。
