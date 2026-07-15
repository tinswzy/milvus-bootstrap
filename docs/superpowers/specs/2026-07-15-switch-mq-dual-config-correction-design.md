# Switch-MQ 双配置纠正（③a 修正）Design

> 修正已合并 ③a（`2026-07-15-switch-mq-endpoint-inject-design.md`）的**切换步骤根本错误**。
> ③a 直接把 CR 的 `msgStreamType` 翻成目标 → 重启崩（读到旧 MQ 的持久化 WAL checkpoint，panic 在
> `streamingnode/wal/adaptor/opener.go NewWALCheckpointFromProto → pulsar/message_id.go`）。真集群 throwaway
> DoD 已复现（milvus v2.6.18，CrashLoopBackOff）。

## 背景与根因

`wal/alter` 是 milvus 官方运行时切 WAL 的管理 API（③a 已验证真集群返回 `{"msg":"OK"}`）。正确的切换语义是：

1. **只往 milvus 的 user.yaml（`spec.config`）里"增加目标 MQ 的连接地址"**，**不动 `msgStreamType`** —— 让源 MQ 与
   目标 MQ 的连接配置**同时存在**，重启后 milvus 两个 MQ client 都能建、某一时刻都可用。
2. 重启生效后，发 `wal/alter` 让 milvus **自己**完成内部 MQ 日志流切换 + **元数据更新**（写 etcd）。
3. milvus 重启以 **etcd 元数据为准**（不看 `msgStreamType`）→ 切成功后 CR 无需二次收尾。

③a 错在第 1 步把 `msgStreamType` 一起翻了，milvus 以纯目标模式重启、读到旧 MQ 的持久化 checkpoint → 崩。

## 目标 / 非目标

**目标**：让「真重指切换」在 milvus 2.6.x 上真正切通、切后实例 Healthy 不崩；verify 以 etcd 持久化的 WAL 类型为准。

**非目标**：切 woodpecker-service（沿用 ③a 的 gate 预留）；旧 MQ 清理（沿用已完成的可选人工提示）；数据迁移（milvus 层不支持，护栏已明示"切换等价空集群换后端、需重建 collection"）。

## 架构改动（对已合并 ③a 的最小修正）

前端 / 端点（`target_name`/`target_ns` 转发）/ gate / 202 异步 / compensate 剥离（Critical 修复）**全部保留不动**。改动集中在 core：

### 1. `core/drivers/milvus.py`：注入连接、不翻类型

新增 helper —— 把目标 MQ 渲染成**原生 milvus config dotted key**（只连接、无 `msgStreamType`）：

```python
def _mq_conn_conf(self, target_wal: str, endpoint: str) -> dict:
    """目标 MQ 的原生 milvus 连接配置（dotted key，注入 _conf → spec.config）。
    只加连接、绝不含 msgStreamType —— 让源/目标 MQ 连接并存，msgStreamType 留给 wal/alter 运行时切。"""
    if target_wal == "kafka":
        return {"kafka.brokerList": endpoint}                       # e.g. kafka-dev.default.svc:9092
    if target_wal == "pulsar":
        host, _, port = endpoint.partition(":")                     # e.g. pulsar-dev-broker.default.svc:6650
        return {"pulsar.address": f"pulsar://{host}", "pulsar.port": int(port or 6650)}
    return {}   # rocksmq/woodpecker-embedded 无外部连接 → 无注入（内嵌）
```

> **实现期 live DoD 钉死 (a)**：kafka.brokerList / pulsar.address+port 的确切键名与取值格式，以 throwaway 上 milvus 实际读到的 user.yaml + 切通为准。

### 2. `core/context.py switch_mq`：保持当前 mq，注入目标连接

```python
# 现状（错）：spec2.params["mq"] = driver._wal_to_mq_id(target_wal)  → _mq_deps 刷 msgStreamType
# 改为：mq 不动，仅把目标连接注入 _conf
spec2 = spec.model_copy(deep=True)
spec2.params = dict(spec2.params)
if target_wal in ("kafka", "pulsar") and target_name:
    ep = _endpoint_for(target_wal, target_name, tns)               # kafka→:9092 / pulsar→-broker:6650（沿用 ③a 推导）
    conn = driver._mq_conn_conf(target_wal, ep)
    spec2.params["_conf"] = {**spec2.params.get("_conf", {}), **conn}
# spec2.params["mq"] 保持 = 当前（不再设为 target）
```

`plan_switch_mq_steps(spec2, adapter, target_wal)` 形状不变：`plan_install_steps`（剥 compensate）渲染出
**双 MQ 配置 + msgStreamType=旧** 的 CR → apply → wait-ready（不再崩）→ wal-alter → verify。

**成功后 snapshot**（`if not dry_run and task.status == succeeded`）：记 `mq=target` + 目标端点，作为 UI/未来渲染真相；
**不**对活的 CR 二次 patch msgStreamType（etcd 元数据驱动运行时——答案①）。

### 3. `core/drivers/milvus.py _verify_wal`：改 etcd 读

```python
def _verify_wal(self, adapter, ns, selector, target_wal, tries=20, sleep_s=3) -> str:
    """有界轮询 milvus 持久化在 etcd 的 WAL 类型 key 直到 == target（honest，不谎报）。
    fake adapter exec 回显 '[fake] …' → 视为模拟通过；真 k8s 读 etcd 值。"""
    read = [<exec etcdctl 读实例 rootPath 下的 WAL-type key>]        # 见 DoD (b)
    for _ in range(tries):
        out = str(adapter.exec(namespace=ns, label_selector=<etcd selector>, command=read))
        if target_wal in out or out.strip().startswith("[fake]"):
            return f"已确认 etcd WAL 类型 == {target_wal}（{out.strip()[:120]}）"
        time.sleep(sleep_s)
    raise TimeoutError(f"切换后未在 {tries*sleep_s}s 内确认 etcd WAL 类型 == {target_wal}")
```

> **实现期 live DoD 钉死 (b)**：milvus 持久化 WAL 类型的**确切 etcd key 路径**（在实例 `etcdRootPath` 下）、
> 用哪个 pod + etcdctl 命令 + 认证（bitnami etcd 的 root 口令）读到。以 throwaway 上真读到目标值为准。

## 数据流

```
用户在 switch-mq 页选目标实例(kafka-dev) → POST /api/switch-mq{instance,target_wal,target_name,target_ns}
  → gate 兼容校验(不过 409+force)
  → TaskRunner.submit(异步) 执行 plan_switch_mq_steps：
      precheck-operator → apply-cr(双 MQ 配置,msgStreamType=旧) → wait-status(Healthy,不崩)
      → wal-alter(milvus 内部切+写 etcd 元数据) → verify-mq-type(etcd 读 WAL 类型==目标,有界轮询)
  → 成功：snapshot 记 mq=target；页面下方流式日志显示各步；提示可选清理旧 MQ
  → 失败：留实例原样(compensate 已剥,绝不删)、报 failed 供排查
```

## 错误处理

- **apply/wait 失败**：compensate 已剥离（③a Critical 修复）→ 绝不 delete_cr，留实例原样报 failed。
- **wal-alter 失败**：报 failed，实例仍在旧 MQ（双连接配置无害，下次可重试）。
- **verify 超时**：honest TimeoutError → task failed（不谎报成功）；实例存活。
- **verify etcd 读不到/认证失败**：与超时同路径，报 failed 供排查（不静默通过）。

## 测试

**单元/集成（fake，随 CI）**：
- `_mq_conn_conf`：kafka→`{"kafka.brokerList": ep}`；pulsar→`{"pulsar.address":"pulsar://…","pulsar.port":int}`；rocksmq/woodpecker-embedded→`{}`。
- `switch_mq` dry-run 渲染的 CR：**含目标连接 key（如 kafka.brokerList=kafka-dev…:9092）且 msgStreamType 仍是源**（回归：证明不翻类型）。
- 步骤序：apply-cr/wait-status/wal-alter/verify-mq-type 存在、verify 最后、无 decommission-old。
- `all(s.compensate is None for s in steps)`（保留 ③a 回归）。
- 端点转发：目标实例端点出现在 apply-cr plan（保留 ③a 回归）。
- snapshot 成功后 mq==target。

**Live DoD（throwaway 真切，破坏性，人工执行一次）**：
1. 装 throwaway `mv-tw`（pulsar，隔离前缀 mv-tw，镜像同 milvus007）。
2. probe 钉死 (a)：注入 kafka.brokerList 后看 mv-tw user.yaml 实际键；钉死 (b)：找 etcd WAL 类型 key + etcdctl 读法。
3. 真切 pulsar→kafka(kafka-dev)。**断言**：全程无 CrashLoopBackOff（apply 后 mv-tw 持续 Healthy）、wal-alter 返回 OK、verify 读 etcd WAL 类型 == kafka、task succeeded、mv-tw 存活。
4. **milvus007 全程不动**（真实例）。清理 mv-tw + secret。

## 影响文件

- `core/drivers/milvus.py`：+`_mq_conn_conf`；`_verify_wal` 改 etcd 读；`plan_switch_mq_steps` 形状不变（spec2 来源变）。
- `core/context.py`：`switch_mq` 注入逻辑改（保持 mq、注入 _conf）。
- `tests/test_switch_mq.py` / `tests/test_web_switchmq.py`：更新/新增上述断言。
- 前端/端点/gate：不动。
