# 实例发现收敛：排除 operator + 归并 managed 子负载 · 设计

- 日期：2026-07-07
- 状态：设计已确认，待写实现计划
- 范围：修掉上一切面 live 实测暴露的 `/api/instances` 虚高问题——把 operator 从实例识别里排除，把 managed 实例的子工作负载归并回父实例。真集群从 17 行收敛到 6 行（0 虚假 external）。

## 1. 背景（live 实测暴露的缺陷）

上一切面（managed/external 合并）在真集群跑出 **17 行 vs 实际 6 个实例**：
- **operator 被误识别为实例**：`detect()` 用镜像子串匹配，milvus profile `image_match:["milvusdb/milvus"]` 命中 `milvusdb/milvus-operator`；minio `["minio"]` 命中 `quay.io/minio/operator` → operator 工作负载被当成 milvus/minio 实例。
- **子工作负载各成一行**：`identify()` 用工作负载名当实例名（`milvus-dev-milvus-standalone`、`pulsar-dev-{bookie,broker,proxy,recovery,zookeeper}`、`kafka-dev-controller`、`minio-pool-0`），精确 `(kind,name,ns)` 去重抓不到它们属于 managed 的 `milvus-dev`/`pulsar-dev`/`kafka-dev`/`minio`。

现状（已核实）：
- `DiscoveryEngine.discover()` → `adapter.discover_native()`(列 STS/Deploy/独立 Pod) → `registry.find_for(evidence)` → `driver.detect()`→`driver.identify()` → `Candidate{kind,name(=工作负载名),ownership,excluded,evidence}`。
- `detect()` 只在 `BaseServiceDriver`（`core/drivers/base.py:49`）一处，**无 driver 覆写**；匹配 `image_match` 子串 / helm chart / crd。
- `/api/instances`（上一切面）：managed 来自 state，external 来自 discovery，按 `(kind,name,ns)` 精确去重（managed 赢），excluded/readonly 已滤。
- 观察到的所有子负载都以 managed 实例名为**段前缀**（`<inst>-<component>`），因 helm/operator 都这么命名子资源。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | operator 排除位置 | 在 `BaseServiceDriver.detect()` 单点（全局：UI + `mb discover`/adopt 一并受益） |
| D2 | operator 判定 | 镜像**名组件**（去 registry/tag/digest 后的最后一段）含 `operator` → 不识别 |
| D3 | 子负载归并位置 | `/api/instances` 端点（managed 名 → 段前缀匹配压掉 external 子负载） |
| D4 | 归并判定 | external 候选与某**同 kind+ns** 的 managed 实例名相等或 `startswith(managed 名 + "-")` → 跳过 |
| D5 | 外部多工作负载分组 | **defer**（当前无 live 用例；label key 各组件不统一） |

## 3. 架构与模块

| 模块 | 职责 | 变更 |
|---|---|---|
| `core/drivers/base.py` `BaseServiceDriver.detect` | 加 operator 护栏：operator 镜像不识别为任何组件实例 | 改 |
| `server/app.py` `api_instances` | external 候选按段前缀归并进同 kind+ns 的 managed 实例 | 改 |

**边界**：Part B 是 discovery 层纯函数式改动（不联网）；Part A 是端点内内存过滤。两者都不新增查询、不改端点契约（行 schema 不变，只是行数收敛）。

## 4. Part B — operator 排除（`detect`）

`BaseServiceDriver.detect(evidence)` 开头加：
```python
img = str(evidence.get("image", "")).lower()
# operator workloads (e.g. milvusdb/milvus-operator, quay.io/minio/operator) are not
# component instances — their image name component contains "operator". Never identify them.
for ref in img.split():                      # evidence.image is space-joined container images
    name_part = ref.split("@")[0].rsplit(":", 1)[0].rsplit("/", 1)[-1]
    if "operator" in name_part:
        return False
```
放在现有 image_match / chart / crd 匹配**之前**。效果：`milvusdb/milvus-operator`→name_part `milvus-operator`→含 operator→False；`quay.io/minio/operator:v7.1.1`→`operator`→False；`milvusdb/milvus:v2.6.18`→`milvus`→放行；`quay.io/minio/minio:latest`→`minio`→放行。

> 说明：组件 server 镜像名从不含 "operator"，故不会误伤；`evidence.image` 是空格连接的多容器镜像，任一容器是 operator 即判为 operator 工作负载。

## 5. Part A — 归并 managed 子负载（`/api/instances`）

在 managed 循环里同时建 `managed_names: dict[(kind,ns), list[str]]`。external 循环里、精确 `seen` 去重之后，加子负载判定：
```python
def _is_subworkload(kind, name, ns) -> bool:
    for mn in managed_names.get((kind, ns), ()):
        if name == mn or name.startswith(mn + "-"):
            return True
    return False
...
# external loop:
if key in seen or _is_subworkload(c.kind, c.name, ns):
    continue
```
效果（本集群）：`milvus-dev-milvus-standalone`←`milvus-dev-`、`pulsar-dev-bookie/...`←`pulsar-dev-`、`kafka-dev-controller`←`kafka-dev-`、`minio-pool-0`←`minio-` 全部并入各自 managed 父 → external 清零。

> 段边界 `mn + "-"`（而非裸 `startswith`）避免 `etcd` 误吞 `etcdkeeper`；`etcd-0` 之类仍以 `-` 段命中。

## 6. 测试与验收

- **Part B**（`tests/`，registry/detect 层，hermetic）：从 `core` 或构建的 registry 取 driver / 用 `registry.find_for`：
  - `find_for({"image":"milvusdb/milvus-operator","labels":{}})` → None（无 driver 认领）。
  - `find_for({"image":"quay.io/minio/operator:v7.1.1","labels":{}})` → None。
  - `find_for({"image":"milvusdb/milvus:v2.6.18",...})` → milvus driver（真组件仍被认领）。
  - `find_for({"image":"quay.io/minio/minio:latest",...})` → minio driver。
- **Part A**（`tests/test_web_endpoints.py`，fake adapter）：装一个 managed 实例，其名是 fake 集群里某同 kind+ns 工作负载的段前缀（或构造），断言该子负载**不**出现在 `/api/instances`（被并入 managed）；一个前缀不匹配的 external 仍出现。
- **回归**：现有 `test_api_instances_*` 仍过（Part A/B 只减行不改 schema；若某断言依赖被排除的 operator/子负载出现，更新之并在报告说明）。
- **手动 DoD**：`mb web` 真集群 → `/api/instances` 只剩 6 个 managed 行、0 虚假 external，无 operator 行；页面每类只显真实例。

## 7. 非目标 / 后续
- 外部（非 mb 装）多工作负载实例的分组归一（defer，等有真实用例 + 统一分组键）。
- 不改 identify 对真实例的归属/接管逻辑；不动 `mb doctor`。
