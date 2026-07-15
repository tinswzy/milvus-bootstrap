# WebUI Switch-MQ ③a：真重指端点注入 + 完整校验 · 设计

- 日期：2026-07-15
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：让「切换 MQ」在选中具体目标实例时**真正生效**——把该实例端点注入 milvus 配置 → apply CR → 等 operator 滚动就绪 → `wal/alter` 运行时切换 → **真校验 milvus 当前 WAL == 目标**（切换真完成才算成）。旧 MQ 清理**不自动做**，仅完成后提示可选人工清理。跑在现有流式任务上。

## 1. 背景与边界

- 前序（②，已合并）：目标下拉分组列已部署实例，option 带 `data-inst`/`data-ns`；`/api/switch-mq/targets` 每 target 带 `instances[].endpoint`。但**切换仍只按类型（target_wal）**，未注入端点——选 kafka-dev 只是显示。
- 现状 `plan_switch_mq_steps(spec, adapter, target_wal)`：仅 `wal-alter`（exec curl）有动作；precheck/verify/decommission 是 plan-only 占位。`context.switch_mq(instance, target_wal, …)` → gate → steps → engine.run（现有流式任务）。
- 机制（已核实）：`_mq_deps(mq, params, ns)` 把 MQ 端点渲染进 CR（kafka→`kafka.brokerList`、pulsar→`pulsar.endpoint`）；`config.set` 既有模式 = 改 params → `plan_install_steps`（渲染+apply+wait）→ **operator 配置变更后自动滚动重启 pod**（mb 只 apply+等，不主动 restart）。`adapter.exec` 可在 milvus pod 内 exec（wal-alter 已用）。
- **verify 机制（已确认）**：exec 进 milvus pod curl 它自己的管理 API 读当前 WAL，有界循环直到 == 目标或超时（无新依赖；反映 milvus 运行时真相，与 etcd 元数据一致）。
- **明确非目标**：③b 任务审计页、③c 人工干预（滚动失败暂停/续跑）是后续切面；本切面**不做**。旧 MQ 自动清理**不做**。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 切换序列 | **apply-config → wait-ready → wal-alter → verify-mq-type**，四步都是真动作。工作流**完成于 verify**（mq-type==目标） |
| D2 | apply-config | 改 spec2.params：`mq=目标选项 id` + 外部型端点；复用 `plan_install_steps(spec2)`（渲染 CR + `apply_objects` + `wait_cr`）——即前两步 |
| D3 | wal-alter | 现有 exec curl `POST /management/wal/alter {target_wal_name}` |
| D4 | verify-mq-type | **新·真校验**：有界循环 exec curl 读 milvus 当前 WAL，`== target_wal` 才成、否则重试至超时 → 超时则该步 failed（工作流不谎报成功）。确切读-WAL API 路径实现时查 milvus（`management/wal/...`）确认 |
| D5 | decommission-old | **移出工作流**。完成后前端提示：旧 MQ 未自动清理，清理是**可选人工操作**；**若仍被其他实例使用或为 external 勿删**；引导去 Dependencies 页手动删除（那里删除本就挡 external）。**不影响"已完成"状态、不建新删除逻辑** |
| D6 | target_wal→mq id | kafka/pulsar/rocksmq：id==wal；woodpecker→`woodpecker-embedded`（service 预留不选）。端点参数：kafka→`kafkaBrokers`、pulsar→`pulsarEndpoint`；嵌入型（rocksmq/woodpecker-embedded）无端点 |
| D7 | 快照 | 非 dry-run 成功后更新 `spec_snapshot`（新 mq+endpoint），使 state 反映新 MQ |
| D8 | 准则 | 跑成**有界异步任务**（现有 TaskRunner + 流式 step 日志）；wait/verify 是**任务内有界循环**（同 wait_cr），非常驻 poller；不谎报（verify 真中目标才完成）；三层护栏 + 门禁 409/force 保留 |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/drivers/milvus.py` `plan_switch_mq_steps` | 重写序列：`plan_install_steps(spec2)` + wal-alter + **verify-mq-type**（去掉 decommission-old 动作步）|
| `core/drivers/milvus.py` | 新 `_endpoint_param(dep_kind, endpoint)` / `_wal_to_mq_id(wal)` 小辅助（端点参数 + wal→id 映射）|
| `core/context.py` `switch_mq` | 签名加 `target_name/target_ns`；构建 spec2（注入 mq+endpoint）；成功后更新快照 |
| `server/app.py` | `POST /api/switch-mq` + `/switch-mq` req 加 `target_name/target_ns` |
| `webui/assets/web.js` `submitSwitchMq`/`renderSwitchMq` | 传 `target_name/target_ns`（②已备 `data-inst/data-ns`）；完成提示加旧 MQ 可选清理引导 |

## 4. 后端

### 4.1 `plan_switch_mq_steps` 重写（milvus.py）
```python
def _wal_to_mq_id(self, wal: str) -> str:
    return {"kafka": "kafka", "pulsar": "pulsar", "rocksmq": "rocksmq",
            "woodpecker": "woodpecker-embedded"}.get(wal, wal)

def plan_switch_mq_steps(self, spec, adapter, target_wal: str) -> list[Step]:
    """Real switch: apply endpoint into CR → wait rolled → wal/alter → verify mq-type."""
    ns, name = spec.namespace, spec.name
    selector = f"app.kubernetes.io/instance={name}"
    # ① + ②：渲染新 CR（spec 已含新 mq+endpoint）+ apply + wait_cr —— 复用安装步骤
    steps = self.plan_install_steps(spec, adapter)          # apply-objects + wait-ready
    # ③ wal-alter（运行时切换）
    alter = ["curl", "-s", "-X", "POST", "http://localhost:9091/management/wal/alter",
             "-d", json.dumps({"target_wal_name": target_wal})]
    steps.append(Step(name="wal-alter", plan="在 milvus pod 内执行：" + " ".join(alter),
                      action=lambda: adapter.exec(namespace=ns, label_selector=selector, command=alter)))
    # ④ verify-mq-type（有界循环真校验；确切读-WAL 命令实现时查 milvus 管理 API）
    steps.append(Step(name="verify-mq-type",
                      plan=f"轮询 milvus 当前 WAL 直到 == {target_wal}（有界·超时）",
                      action=lambda: self._verify_wal(adapter, ns, selector, target_wal)))
    return steps

def _verify_wal(self, adapter, ns, selector, target_wal, tries=20, sleep_s=3) -> str:
    read = ["curl", "-s", "http://localhost:9091/management/wal/status"]   # 确切路径实现时确认
    for _ in range(tries):
        out = adapter.exec(namespace=ns, label_selector=selector, command=read)
        if target_wal in str(out):
            return f"已确认当前 WAL == {target_wal}"
        time.sleep(sleep_s)
    raise TimeoutError(f"切换后未在 {tries*sleep_s}s 内确认 WAL == {target_wal}")
```
（`plan_install_steps(spec, adapter)` 已含 apply-objects + wait_cr；用 spec2 渲染即带新 mq+endpoint。decommission-old 动作步删除——改为前端完成提示。fake adapter 下 `exec` 被 stub 返回含 target_wal 的串使 verify 立即通过；真集群走真 curl。`time` 在 milvus.py import。）

### 4.2 `context.switch_mq` 加 `target_name/target_ns` + 注入
```python
def switch_mq(self, instance_id, target_wal, target_name="", target_ns="", dry_run=True, force=False):
    ... 现有 load + kind 检查 + current_wal + compat.gate ...
    driver = self.registry.get("milvus")
    spec2 = spec.model_copy(deep=True); spec2.params = dict(spec2.params)
    spec2.params["mq"] = driver._wal_to_mq_id(target_wal)
    dep_kind = {"kafka": "kafka", "pulsar": "pulsar"}.get(target_wal)   # 外部型才注端点
    if dep_kind and target_name:
        ep = _dep_endpoint(dep_kind, target_name, target_ns or spec.namespace)   # 同 targets 端点推法
        spec2.params[{"kafka": "kafkaBrokers", "pulsar": "pulsarEndpoint"}[dep_kind]] = ep
    steps = driver.plan_switch_mq_steps(spec2, self.adapter, target_wal)
    task = self.engine.run(type="switch-mq", target=instance_id, steps=steps, dry_run=dry_run)
    self.state.put_task(task)
    if not dry_run and task.status == TaskStatus.succeeded:
        inst.spec_snapshot = spec2.model_dump(mode="json"); self.state.put_instance(inst)
    return task
```
（`_dep_endpoint` 与 ② 端点同法：kafka `<name>.<ns>.svc:9092`、pulsar `<name>-broker.<ns>.svc:6650`。抽成 `core` 小工具或 milvus 驱动内联，两处一致。）

### 4.3 端点：`POST /api/switch-mq` + `/switch-mq`
`SwitchMqApiReq`/`SwitchMqReq` 加 `target_name: str = ""`、`target_ns: str = ""`；`api_switch_mq`/`switch_mq` 路由把二者透传给 `_core().switch_mq(...)`。dry-run/gate 409/apply 202 分支不变。

## 5. 前端

`submitSwitchMq(name, targetWal, dryRun, force, el, targetName="", targetNs="")`：POST body 加 `target_name: targetName, target_ns: targetNs`。`renderSwitchMq`：`onchange` 已取 `data-inst`/`data-ns` → 存 `selectedInst`/`selectedNs`；`#sw-dry`/`#sw-go` 调用改 `submitSwitchMq(selInst, selectedWal, …, res, selectedInst, selectedNs)`。**apply(202) 完成 onDone**：在既有「已提交 MQ 切换」之外，追加**旧 MQ 清理提示**：`旧 MQ <curMq> 未自动清理 · 清理为可选人工操作，若仍被其他实例使用或为 external 请勿删除 · [前往 Dependencies]`（链接 `deps.html`）。不影响流式完成态。

## 6. 测试与验收
- **`plan_switch_mq_steps`**（`tests/test_switch_mq.py`）：kafka（带 target endpoint）dry-run → steps 名序含 apply/wait（来自 plan_install_steps）+ `wal-alter` + `verify-mq-type`，**无 `decommission-old` 动作步**；apply（fake）→ verify 步 detail 含目标（exec stub 命中）；spec2 快照 params `mq==kafka`、`kafkaBrokers` 含目标端点。
- **`context.switch_mq`**（fake）：`switch_mq("mv", "kafka", "kafka-dev", "default", dry_run=False)` → 成功 + 实例快照 `mq=kafka`、`kafkaBrokers` 指向 `kafka-dev...:9092`；嵌入型（rocksmq）无端点参数。
- **端点**（`tests/test_web_switchmq.py`）：`POST /api/switch-mq {instance,target_wal:kafka,target_name:kafka-dev,dry_run:true}` → 200 task 步骤含 verify-mq-type；apply → 202。
- **前端 content-marker**（`test_web_static.py`）：`submitSwitchMq` 签名/调用含 `target_name`；`renderSwitchMq` 传 `selectedInst`；完成提示含「旧 MQ」「Dependencies」；`setInterval` 不存在。
- **JS**：`node --check`。
- **手动 DoD（真集群·throwaway milvus 真切一次）**：装一个 throwaway milvus(pulsar) + 一个 kafka → 切到 kafka-dev → 流式见 apply-config→wait-ready→wal-alter→verify-mq-type 逐步；milvus CR 配置变 kafka+kafka-dev 端点、operator 滚动、WAL 切到 kafka、verify 命中；完成提示旧 MQ(pulsar) 可选清理；快照更新。

## 7. 非目标 / 后续
- **③b 任务审计页**（工作流列表 + 切页找回 + 展开续看）。
- **③c 人工干预**（apply/滚动失败 → 暂停 + 页面手动重滚 + 续跑）。
- 旧 MQ 自动清理 / 共享检测（本切面仅提示 + 引导 Dependencies）。
- verify 的确切读-WAL API 路径若 milvus 无直接读接口，退化为"wal-alter 成功 + CR msgStreamType==目标"弱校验（实现时定，spec DoD 以真集群确认为准）。
