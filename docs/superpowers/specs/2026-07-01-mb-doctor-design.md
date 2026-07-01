# mb doctor — 环境自检 + 版本/兼容矩阵 + 操作门禁 · 设计

- 日期：2026-07-01
- 状态：设计已确认，待写实现计划
- 范围：打磨现有版本，**不**新增 switch-mq / woodpecker 的功能支持

## 1. 背景与目标

milvus-bootstrap（`mb`）目前能装/连/升级 milvus 及其依赖，但缺少两类能力：

1. **环境预检**——在真正操作前，回答"我这套环境到底能不能跑"：kubectl 是否可用、集群是否可达、代理/NO_PROXY 是否正确、milvus-operator/CRD 是否就位、关键镜像能否拉取。
2. **版本可见性与兼容治理**——把工具自身版本、集群里各组件（milvus / operator / helm / minio / kafka/pulsar / k8s / woodpecker）的实际版本探测出来，对照一张兼容关系表给出提示，并据此**限制**不安全的操作（如 switch-mq 同类切换、版本不满足的安装）。

本设计把这些能力收敛到**一个命令 `mb doctor`**，并把兼容判定沉淀成**数据驱动、可编辑**的唯一真相，供 doctor 展示与操作门禁共用。

### 非目标（明确不做）
- 不新增 switch-mq / woodpecker 的功能支持（仅复用已有兼容规则做展示/门禁）。
- 不做真实镜像 pull 验证（只做 manifest 可达 + 节点缓存检查）。
- 不接 PyPI 更新源（工具更新检查走 git 远端比对）。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 命令形态 | 单命令 `mb doctor` 全包：环境 / 版本 / 兼容 / 工具四段 |
| D2 | 镜像检查力度 | manifest 轻量探测（registry 可达）+ 节点缓存检查；**不真拉** |
| D3 | 兼容矩阵权威来源 | 数据驱动、可编辑；milvus↔MQ/woodpecker = 有把握的**硬约束**；operator/helm/k8s↔milvus = 由用户**权威表**填入，未填=只提示不硬拦 |
| D4 | 工具更新检查 | 显示 `__version__` + git commit；能访问 remote 时比对最新 tag/commit；访不到=不可用 |
| D5 | 门禁行为 | hard 冲突→拦截 + `--force` 逃生口；soft/未知→只 WARN；switch-mq 同类切换=硬拦 |
| D6 | 输出/运行 | rich 彩色表（PASS/WARN/FAIL/SKIP）+ `--json`；**本地优先、优雅降级**，不强依赖 daemon；有任一 FAIL → 退出码非 0 |

## 3. 架构与模块划分

| 模块 | 职责 | 依赖 |
|---|---|---|
| `core/compat.py`（扩展现有） | 兼容判定唯一真相：加载 YAML 矩阵 + 保留现有 MQ 硬规则；暴露 `evaluate(versions) -> [Finding]` 与 `gate(op, ctx)` | `compat.yaml` |
| `core/compat.yaml`（新，可编辑） | 数据驱动的版本约束表（component × milvus 版本区间 × min/max × severity × source） | — |
| `core/probe.py`（新） | 只读版本探测：k8s server / operator / helm / minio / kafka / pulsar / milvus 实例 / woodpecker；每项 best-effort，探不到→`None` | k8s 只读客户端、state |
| `core/doctor.py`（新） | 编排器：环境检查 + 调 probe + 调 `compat.evaluate` + 工具版本，产出结构化 `DoctorReport` | probe、compat、git |
| `cli/main.py`（改） | 新增 `doctor` 命令（渲染 rich / `--json`）；给 install/upgrade/switch-mq 加 `--force` 并接线 `compat.gate` | doctor、compat |

**边界原则**：`doctor` 直接实例化只读探测，**不经 daemon**；daemon 自身是"环境"里的一个被检查项。这样预检在 daemon/集群未就绪时仍可用（相关项标 `SKIP`）。

### 单元职责（可独立测试）
- `compat.evaluate(versions)`：入=各组件版本 dict，出=`Finding` 列表（level=PASS/WARN/FAIL、component、rule、reason）。纯函数，易测。
- `compat.gate(op, ctx)`：入=操作名 + 上下文（含 `--force`），命中 hard 冲突且非 force→raise；否则返回（含 WARN 列表）。纯逻辑，易测。
- `probe.detect_*()`：各组件一个探测函数，返回版本串或 None；对 adapter/kubectl 是薄封装，用 fake 测。
- `doctor.run(opts)`：编排，产出 `DoctorReport`；渲染与逻辑分离（渲染在 CLI 层）。

## 4. 数据模型：`compat.yaml`

```yaml
# 每条约束一条记录。severity: hard=硬拦 / soft=只WARN。
# source: confident=有把握(代码逻辑亦覆盖) / best-effort / user-table(待权威表填)
constraints:
  - component: milvus-operator
    requires: milvus
    rule: "milvus 2.6.x 需要 operator >= ?"
    milvus_range: ">=2.6.0,<3.0.0"
    min: ""          # ← 待用户权威表填；留空 = 未知，只提示不硬拦
    max: ""
    severity: soft
    source: user-table
    reason: ""
  - component: milvus-helm
    requires: milvus
    milvus_range: ">=2.6.0"
    min: ""
    severity: soft
    source: user-table
  - component: k8s
    requires: milvus
    milvus_range: ""
    min: ""
    severity: soft
    source: user-table
# milvus↔kafka/pulsar/woodpecker 的硬规则仍由 compat.py 的 MQ_OPTIONS 承载
# （含 woodpecker-embedded>=2.6.0、woodpecker-service>=3.0.0），YAML 不重复。
```

- **milvus↔MQ/woodpecker**：`source: confident`，硬约束，逻辑保留在 `compat.py`（现有 `MQ_OPTIONS` + `check()`）。含用户点名的 2.6 embed / 3.0 service、kafka/pulsar 版本下限。
- **operator/helm/k8s↔milvus**：在 `compat.yaml` 建好槽位，`source: user-table`；`min/max` 为空时该约束按"未知→只 WARN/信息"处理，绝不误硬拦。用户提供权威数字后，把对应项改 `severity: hard` 即生效。

## 5. 版本探测来源（`probe.py`）

| 组件 | 探测方式 | 探不到时 |
|---|---|---|
| k8s server | `kubectl version` / kubernetes client `/version` | SKIP |
| milvus-operator | 其 ns 里 operator deployment 镜像 tag + `milvuses.milvus.io` CRD 版本 | SKIP |
| milvus-helm | helm release metadata（operator 装则多为 N/A） | N/A |
| minio / kafka / pulsar | 运行 pod 镜像 tag / helm release chart 版本 | SKIP |
| milvus（各实例） | Managed 实例 CR 的 `spec.components.image` | 无实例=SKIP |
| woodpecker | 同上（如有） | SKIP |
| 镜像可拉取性 | ① 节点缓存检查（`minikube ssh docker images` / crictl，可靠）② registry manifest HEAD（best-effort，失败→unknown） | unknown |

## 6. 门禁集成（`compat.gate`）

接到 install / upgrade / switch-mq：
- **hard 冲突** → 抛错拦截，打印清晰原因（哪个组件、要求什么、实际什么）；`--force` 打印警告后放行。
- **soft / 未知** → 只打 WARN，正常放行。
- **switch-mq 同类切换**（`target_wal == 当前 wal`）→ hard 拦（无意义操作）。

`--force` 作为新 flag 加到 install/upgrade/switch-mq，线程化传入 gate。

## 7. `mb doctor` 输出

四段 rich 表：
1. **环境**：kubectl 存在 · 集群可达 · NO_PROXY 含 minikube IP（本项目真实坑）· daemon 运行 · operator 就位 · 必需 CRD 存在 · 关键镜像 registry 可达 + 节点缓存。
2. **版本**：各组件探测到的版本，逐行标 PASS/WARN/FAIL（对照兼容矩阵）。
3. **兼容**：`compat.evaluate` 的 Finding 列表 + 提示。
4. **工具**：`__version__` + git commit + 远端更新比对。

`--json` 输出同一 `DoctorReport` 结构。退出码：有 FAIL → 非 0。

## 8. 测试与验收

纯单测（无集群，pytest）：
- `compat.yaml` 加载 + `evaluate`：给定版本组合，断言 Finding level 正确（含空 min/max 不误判 FAIL）。
- `gate`：switch-mq 同类切换被拒；hard 冲突被拒；`--force` 绕过并带 WARN；soft 只 WARN。
- 版本解析：复用/扩展 `compat.parse_version`（master/latest→最新）。
- `doctor.run`：mock probe，断言 DoctorReport 分段 + 退出码逻辑（有 FAIL→非0）。
- 渲染与逻辑分离，逻辑层不依赖 rich/终端。

**DoD**：`mb doctor` 在真集群跑出四段表；至少一条真实约束（如 milvus 2.5 + woodpecker-service）被 gate 硬拦、`--force` 可放行；switch-mq 同类被拒；全部单测通过；`--json` 可被解析；有 FAIL 时退出码非 0。

## 9. 待办依赖
- **operator/helm/k8s↔milvus 的权威版本数字**由用户后续提供，填入 `compat.yaml` 并把对应项转 `severity: hard`。在此之前这些项按"未知→只提示"运行。
