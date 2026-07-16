# Switch-MQ 双配置纠正 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修正已合并 ③a 的切换步骤——apply-cr 不再翻转 `msgStreamType`，改为「只注入目标 MQ 连接、留旧类型 → wal/alter 运行时切 → 读 etcd 元数据 verify」，使 milvus 2.6.x 切换后不再 CrashLoopBackOff。

**Architecture:** 改动集中在 core 两个文件。`core/drivers/milvus.py` 新增 `_mq_conn_conf`（渲染目标 MQ 原生 config dotted key，无 msgStreamType）并把 `_verify_wal` 从 curl milvus 改为 exec etcdctl 读 etcd WAL 类型；`core/context.py switch_mq` 保持当前 mq、把目标连接注入 `_conf`，成功后记目标态 snapshot。前端/端点转发/gate/202 异步/compensate 剥离全部保留不动。

**Tech Stack:** Python 3 / FastAPI daemon / pytest（fake adapter）；milvus-operator CR（`spec.config` 注入原生 milvus.yaml key）；etcdctl（exec 进 etcd pod 读 WAL 元数据）。

## Global Constraints

- **绝不翻 `msgStreamType`**：切换的 apply-cr 渲染出的 CR 必须保持 `msgStreamType == 源 MQ`；目标 MQ 只以连接配置（`spec.config` 原生 key）注入，与源 MQ 连接并存。
- **compensate 必须全 None**：`plan_switch_mq_steps` 复用 `plan_install_steps` 时已剥离 compensate（CR 预存，失败绝不 `delete_cr`）——保留此不变式，回归断言 `all(s.compensate is None for s in steps)`。
- **honest verify 不谎报**：verify 读不到目标 → 有界超时 `raise TimeoutError` → task failed；绝不静默通过。fake adapter exec 回显以 `[fake]` 开头 → 视为模拟通过。
- **milvus config key 事实来源**：kafka→`kafka.brokerList`（字符串 host:port）；pulsar→`pulsar.address`(`pulsar://host`)+`pulsar.port`(int)。确切键名/格式以 Task 4 live DoD 在 throwaway 上 milvus 实读 user.yaml + 切通为准。
- **git 身份**：本仓库提交作者已 pin，subagent 只 `git add`+`git commit`，**绝不** filter-branch/rebase/reset/push/amend。
- **测试基线**：改动前 `pytest` 全绿（当前 220 测）；每个 Task 结束全绿。

---

### Task 1: `_mq_conn_conf` helper（目标 MQ 原生连接配置）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py`（`MilvusDriver` 内，`_mq_deps` 之后）
- Test: `milvus-bootstrap/tests/test_switch_mq.py`

**Interfaces:**
- Produces: `MilvusDriver._mq_conn_conf(self, target_wal: str, endpoint: str) -> dict[str, object]` —— 返回 dotted-key 字典（注入 `_conf`）。kafka→`{"kafka.brokerList": endpoint}`；pulsar→`{"pulsar.address": "pulsar://<host>", "pulsar.port": <int>}`；其它（rocksmq/woodpecker）→`{}`。**绝不含 `msgStreamType`。**

- [ ] **Step 1: 写失败测试**

在 `tests/test_switch_mq.py` 末尾追加：

```python
def test_mq_conn_conf_kafka(core: Core) -> None:
    d = core.registry.get("milvus")
    assert d._mq_conn_conf("kafka", "kafka-dev.default.svc:9092") == {
        "kafka.brokerList": "kafka-dev.default.svc:9092"}


def test_mq_conn_conf_pulsar_splits_host_port(core: Core) -> None:
    d = core.registry.get("milvus")
    conf = d._mq_conn_conf("pulsar", "pulsar-dev-broker.default.svc:6650")
    assert conf == {"pulsar.address": "pulsar://pulsar-dev-broker.default.svc",
                    "pulsar.port": 6650}


def test_mq_conn_conf_embedded_empty_and_no_msgstreamtype(core: Core) -> None:
    d = core.registry.get("milvus")
    assert d._mq_conn_conf("rocksmq", "") == {}
    assert d._mq_conn_conf("woodpecker", "") == {}
    # 关键不变式：连接配置绝不含 msgStreamType（那是 wal/alter 运行时的事）
    assert "msgStreamType" not in d._mq_conn_conf("kafka", "k.default.svc:9092")
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd milvus-bootstrap && python -m pytest tests/test_switch_mq.py -k mq_conn_conf -q`
Expected: FAIL —— `AttributeError: 'MilvusDriver' object has no attribute '_mq_conn_conf'`

- [ ] **Step 3: 实现 `_mq_conn_conf`**

在 `milvus.py` 的 `_mq_deps` 方法之后、`_wal_to_mq_id` 之前插入：

```python
    def _mq_conn_conf(self, target_wal: str, endpoint: str) -> dict:
        """目标 MQ 的原生 milvus 连接配置（dotted key，注入 _conf → spec.config）。

        只加连接、绝不含 msgStreamType —— 让源/目标 MQ 连接在 milvus user.yaml 里并存，
        msgStreamType 保持源；运行时切由 wal/alter 完成。切换流程专用（区别于装机的 _mq_deps）。
        """
        if target_wal == "kafka":
            return {"kafka.brokerList": endpoint}                 # 字符串 host:port
        if target_wal == "pulsar":
            host, _, port = endpoint.partition(":")
            return {"pulsar.address": f"pulsar://{host}", "pulsar.port": int(port or 6650)}
        return {}   # rocksmq / woodpecker-embedded：内嵌，无外部连接
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd milvus-bootstrap && python -m pytest tests/test_switch_mq.py -k mq_conn_conf -q`
Expected: PASS（3 passed）

- [ ] **Step 5: 提交**

```bash
cd milvus-bootstrap
git add src/milvus_bootstrap/core/drivers/milvus.py tests/test_switch_mq.py
git commit -m "feat(switch-mq): _mq_conn_conf renders target MQ connection (no msgStreamType)"
```

---

### Task 2: `switch_mq` 注入目标连接、保持源类型、成功记目标 snapshot

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/context.py:79-109`（`switch_mq`）
- Test: `milvus-bootstrap/tests/test_switch_mq.py`, `milvus-bootstrap/tests/test_web_switchmq.py`

**Interfaces:**
- Consumes: `MilvusDriver._mq_conn_conf(target_wal, endpoint)`（Task 1）；`driver._wal_to_mq_id(wal)`（已存在）。
- Produces: `Core.switch_mq(...)` 行为变更——渲染的 CR 保持源 `msgStreamType`、目标连接进 `spec.config`；成功后 `spec_snapshot` 记目标态（`mq=target` + 目标端点 param），活 CR 不二次 patch。

- [ ] **Step 1: 写失败测试（核心回归：注入连接但不翻类型）**

在 `tests/test_switch_mq.py` 末尾追加。`apply-cr` 步的 `plan` 含完整渲染 CR 的 YAML（`yaml.safe_dump_all(manifests)`），直接断言：

```python
def test_switch_apply_cr_injects_target_conn_keeps_source_msgstreamtype(core_with_milvus_kafka) -> None:
    """kafka(源)→pulsar(目标)：apply-cr 渲染的 CR 必须注入 pulsar 连接、且 msgStreamType 仍是 kafka。
    这是本次纠正的核心——绝不能翻成 pulsar（③a 的翻类型 bug 会让 milvus 读旧 checkpoint 崩）。"""
    c = core_with_milvus_kafka
    task = c.switch_mq("milvus-dev", "pulsar", target_name="pulsar-dev", target_ns="default", dry_run=True)
    apply_plan = next(s.plan for s in task.steps if s.name == "apply-cr")
    assert "msgStreamType: kafka" in apply_plan          # 源类型保持，未翻
    assert "msgStreamType: pulsar" not in apply_plan     # 绝未翻成目标
    assert "pulsar://pulsar-dev-broker.default.svc" in apply_plan   # 目标连接已注入
    assert "6650" in apply_plan


def test_switch_apply_cr_kafka_target_injects_brokerlist(core: Core) -> None:
    """源 woodpecker→目标 kafka：注入 kafka.brokerList，msgStreamType 不变成 kafka。"""
    task = core.switch_mq("milvus-dev", "kafka", target_name="kafka-dev", target_ns="default", dry_run=True)
    apply_plan = next(s.plan for s in task.steps if s.name == "apply-cr")
    assert "brokerList: kafka-dev.default.svc:9092" in apply_plan
    assert "msgStreamType: kafka" not in apply_plan      # 源是 woodpecker，未翻成 kafka
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd milvus-bootstrap && python -m pytest tests/test_switch_mq.py -k "injects_target_conn or brokerlist" -q`
Expected: FAIL —— 现状 `switch_mq` 设 `spec2.params["mq"]=target` → 渲染出 `msgStreamType: pulsar`（第一个断言 `msgStreamType: kafka` 找不到）。

- [ ] **Step 3: 改 `switch_mq`（context.py:94-108）**

把现有第 94-108 行（从 `driver = self.registry.get("milvus")` 到 `self.state.put_instance(inst)`）替换为：

```python
        driver = self.registry.get("milvus")
        tns = target_ns or spec.namespace

        def _endpoint(wal: str) -> str:
            if wal == "kafka":
                return f"{target_name}.{tns}.svc:9092"
            if wal == "pulsar":
                return f"{target_name}-broker.{tns}.svc:6650"
            return ""

        # 应用态：保持当前 mq（不翻 msgStreamType），只把目标 MQ 连接注入 spec.config（_conf）。
        # 源/目标连接并存 → 重启后两 MQ client 都可建 → wal/alter 运行时切，milvus 不会读旧 checkpoint 崩。
        spec2 = spec.model_copy(deep=True)
        spec2.params = dict(spec2.params)
        endpoint = _endpoint(target_wal)
        if target_wal in ("kafka", "pulsar") and target_name:
            spec2.params["_conf"] = {**spec2.params.get("_conf", {}),
                                     **driver._mq_conn_conf(target_wal, endpoint)}
        steps = driver.plan_switch_mq_steps(spec2, self.adapter, target_wal)
        task = self.engine.run(type="switch-mq", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        if not dry_run and task.status == TaskStatus.succeeded:
            # 成功后 snapshot 记「目标态」作为 UI/未来渲染真相（活 CR 的 msgStreamType 不二次 patch——
            # milvus 重启以 etcd WAL 元数据为准）。用目标态 spec，而非并存态 spec2。
            snap = spec.model_copy(deep=True)
            snap.params = dict(snap.params)
            snap.params["mq"] = driver._wal_to_mq_id(target_wal)
            if target_wal == "kafka" and target_name:
                snap.params["kafkaBrokers"] = endpoint
            elif target_wal == "pulsar" and target_name:
                snap.params["pulsarEndpoint"] = endpoint
            inst.spec_snapshot = snap.model_dump(mode="json")
            self.state.put_instance(inst)
        return task
```

- [ ] **Step 4: 更新受影响的 web 转发测试**

现有 `tests/test_web_switchmq.py::test_api_switch_mq_passes_target_instance`（约第 118-127 行）断言 `":6650" in apply_plan`——新方案 pulsar 端点渲染成 `pulsar.address`+`pulsar.port`，`":6650"` 不再出现。把该断言块（`# forwarding proof:` 起到函数末尾）替换为：

```python
        # forwarding proof: 目标实例端点被注入渲染的 CR，且未翻 msgStreamType（源是 kafka）
        apply_plan = next(s["plan"] for s in steps if s["name"] == "apply-cr")
        assert "pulsar://pulsar-dev-broker.default.svc" in apply_plan and "6650" in apply_plan
        assert "msgStreamType: kafka" in apply_plan          # 源类型保持，未翻成 pulsar
```

- [ ] **Step 5: 跑全部 switch 相关测试确认通过**

Run: `cd milvus-bootstrap && python -m pytest tests/test_switch_mq.py tests/test_web_switchmq.py -q`
Expected: PASS（含既有 `test_switch_mq_injects_endpoint_and_updates_snapshot`、`test_switch_mq_embedded_no_endpoint`——snapshot 仍记目标态，不受影响；`test_switch_mq_steps_have_no_destructive_compensate` 仍绿）

- [ ] **Step 6: 提交**

```bash
cd milvus-bootstrap
git add src/milvus_bootstrap/core/context.py tests/test_switch_mq.py tests/test_web_switchmq.py
git commit -m "fix(switch-mq): inject target MQ connection into config, keep source msgStreamType (no crash on switch)"
```

---

### Task 3: `_verify_wal` 改为读 etcd WAL 元数据

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py`（`_verify_wal` + `plan_switch_mq_steps` 的 verify 步）
- Test: `milvus-bootstrap/tests/test_switch_mq.py`

**Interfaces:**
- Consumes: `adapter.exec(namespace, label_selector, command) -> str`（已存在；fake 回显 `[fake] …`）。
- Produces: `MilvusDriver._verify_wal(self, adapter, etcd_ns, etcd_selector, root, target_wal, tries=20, sleep_s=3) -> str` —— exec etcdctl 读 etcd 里 milvus 的 WAL 元数据前缀，有界轮询直到 `target_wal` 出现；超时 `raise TimeoutError`。

- [ ] **Step 1: 写失败测试**

在 `tests/test_switch_mq.py` 末尾追加：

```python
def test_verify_wal_reads_etcd_prefix(core: Core, monkeypatch) -> None:
    """verify 应 exec 进 etcd pod 跑 etcdctl 读 WAL 元数据前缀，命中 target 即通过。"""
    d = core.registry.get("milvus")
    calls = {}

    class _AD:
        def exec(self, namespace, label_selector, command):
            calls["ns"] = namespace
            calls["sel"] = label_selector
            calls["cmd"] = command
            return "streamingcoord/wal ... walName:kafka ..."   # 命中 target
    out = d._verify_wal(_AD(), "default", "app.kubernetes.io/instance=etcd",
                        "milvus-dev", "kafka", tries=3, sleep_s=0)
    assert "kafka" in out
    assert "etcdctl" in calls["cmd"][0] or "etcdctl" in " ".join(calls["cmd"])
    assert "milvus-dev" in " ".join(calls["cmd"])               # 读实例 rootPath 前缀
    assert calls["sel"] == "app.kubernetes.io/instance=etcd"    # 进 etcd pod，不是 milvus pod


def test_verify_wal_times_out_when_absent(core: Core) -> None:
    d = core.registry.get("milvus")

    class _AD:
        def exec(self, namespace, label_selector, command):
            return "streamingcoord/wal ... walName:pulsar ..."  # 从不出现 kafka
    with pytest.raises(TimeoutError):
        d._verify_wal(_AD(), "default", "app.kubernetes.io/instance=etcd",
                      "milvus-dev", "kafka", tries=2, sleep_s=0)


def test_verify_wal_fake_tolerance(core: Core) -> None:
    d = core.registry.get("milvus")

    class _AD:
        def exec(self, namespace, label_selector, command):
            return "[fake] etcdctl get ..."
    assert "kafka" in d._verify_wal(_AD(), "default", "app.kubernetes.io/instance=etcd",
                                    "milvus-dev", "kafka", tries=1, sleep_s=0)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd milvus-bootstrap && python -m pytest tests/test_switch_mq.py -k verify_wal -q`
Expected: FAIL —— 现 `_verify_wal(self, adapter, ns, selector, target_wal, ...)` 签名不含 `etcd_ns/etcd_selector/root`，`TypeError`。

- [ ] **Step 3: 改 `_verify_wal` + 加模块常量 + 改 `plan_switch_mq_steps` 的 verify 步**

在 `milvus.py` 顶部 `WOODPECKER_SERVICE_PORT = 18080` 之后加常量：

```python
# milvus 把运行时 WAL 类型持久化在 etcd 的 meta rootPath 下（wal/alter 更新）。
# 读此前缀、子串匹配目标 WAL 名即可确认切换完成。确切前缀以 Task 4 live DoD 在 throwaway 上钉死。
MILVUS_WAL_META_PREFIX = "streamingcoord"
```

把现有 `_verify_wal`（约第 148-157 行）整体替换为：

```python
    def _verify_wal(self, adapter, etcd_ns, etcd_selector, root, target_wal,
                    tries=20, sleep_s=3) -> str:
        """有界轮询 milvus 持久化在 etcd 的 WAL 类型直到命中 target（honest，不谎报）。

        exec 进 etcd pod 跑 etcdctl 读实例 rootPath 下的 WAL 元数据前缀，子串匹配目标 WAL 名。
        fake adapter 回显 '[fake] …' → 视为模拟通过；真 k8s 读 etcd 实值。
        """
        key = f"{root}/{MILVUS_WAL_META_PREFIX}"
        read = ["etcdctl", "get", "--prefix", key]   # 认证/端点若需，Task 4 live DoD 补
        for _ in range(tries):
            out = str(adapter.exec(namespace=etcd_ns, label_selector=etcd_selector, command=read))
            if target_wal in out or out.strip().startswith("[fake]"):
                return f"已确认 etcd WAL 类型 == {target_wal}（{out.strip()[:120]}）"
            time.sleep(sleep_s)
        raise TimeoutError(f"切换后未在 {tries * sleep_s}s 内确认 etcd WAL 类型 == {target_wal}")
```

在 `plan_switch_mq_steps` 里，把 verify 步之前的 selector 计算补上 etcd 派生，并改 verify 步的 action。现有（约 161-175 行）：

```python
        ns, name = spec.namespace, spec.name
        selector = f"app.kubernetes.io/instance={name}"
        steps = list(self.plan_install_steps(spec, adapter))
        for s in steps:
            s.compensate = None
        alter = ["curl", "-s", "-X", "POST", "http://localhost:9091/management/wal/alter",
                 "-d", json.dumps({"target_wal_name": target_wal})]
        steps.append(Step(name="wal-alter", plan="在 milvus pod 内执行：" + " ".join(alter),
                          action=lambda: adapter.exec(namespace=ns, label_selector=selector, command=alter)))
        steps.append(Step(name="verify-mq-type",
                          plan=f"轮询 milvus 当前 WAL 直到 == {target_wal}（有界·超时）",
                          action=lambda: self._verify_wal(adapter, ns, selector, target_wal)))
        return steps
```

替换为（新增 etcd 派生 + 改 verify plan/action）：

```python
        ns, name = spec.namespace, spec.name
        selector = f"app.kubernetes.io/instance={name}"
        # verify 读 etcd：从实例的 etcdEndpoints/rootPath 派生 etcd pod selector 与 meta rootPath
        etcd_eps = _as_list(spec.params.get("etcdEndpoints"), [f"etcd.{ns}.svc:2379"])
        etcd_host = etcd_eps[0].split(":")[0]                       # e.g. etcd.default.svc
        parts = etcd_host.split(".")
        etcd_selector = f"app.kubernetes.io/instance={parts[0]}"
        etcd_ns = parts[1] if len(parts) > 1 else ns
        root = spec.params.get("etcdRootPath") or name
        steps = list(self.plan_install_steps(spec, adapter))
        for s in steps:
            s.compensate = None
        alter = ["curl", "-s", "-X", "POST", "http://localhost:9091/management/wal/alter",
                 "-d", json.dumps({"target_wal_name": target_wal})]
        steps.append(Step(name="wal-alter", plan="在 milvus pod 内执行：" + " ".join(alter),
                          action=lambda: adapter.exec(namespace=ns, label_selector=selector, command=alter)))
        steps.append(Step(name="verify-mq-type",
                          plan=f"读 etcd（{etcd_selector}）{root}/{MILVUS_WAL_META_PREFIX} 直到 WAL == {target_wal}（有界·超时）",
                          action=lambda: self._verify_wal(adapter, etcd_ns, etcd_selector, root, target_wal)))
        return steps
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd milvus-bootstrap && python -m pytest tests/test_switch_mq.py tests/test_web_switchmq.py -q`
Expected: PASS（verify 3 测 + 既有全绿；`test_switch_mq_apply_execs_wal_alter` 等 dry/exec 测不受影响）

- [ ] **Step 5: 全量回归**

Run: `cd milvus-bootstrap && python -m pytest -q`
Expected: PASS（全绿，测数 = 原 220 + 本计划新增）

- [ ] **Step 6: 提交**

```bash
cd milvus-bootstrap
git add src/milvus_bootstrap/core/drivers/milvus.py tests/test_switch_mq.py
git commit -m "feat(switch-mq): verify by reading milvus WAL type from etcd (honest, bounded)"
```

---

### Task 4: Live DoD（破坏性真切验证 + 钉死 live 值）— 由控制者执行

> 非 subagent 代码任务：需真集群（含代理 NO_PROXY/venv 环境），由控制者手动执行。可能产出 1-2 个小的常量修正提交（config key 格式 / etcd 前缀 / etcdctl 认证）。

- [ ] **Step 1: 装 throwaway `mv-tw`（pulsar）** —— 镜像/deps 镜像 milvus007，隔离前缀 mv-tw，mq=pulsar。等 Healthy。

- [ ] **Step 2: 钉死 (a) config key** —— dry-run 切 mv-tw→kafka(kafka-dev)，看渲染 CR 是否含 `kafka.brokerList: kafka-dev...:9092` 且 `msgStreamType` 仍 pulsar；apply 后 exec 进 mv-tw pod 看实际 user.yaml 是否读到该 key。若 milvus 实际键名/格式不同 → 修 `_mq_conn_conf` 常量并补提交。

- [ ] **Step 3: 真切 pulsar→kafka** —— `switch_mq('mv-tw','kafka',target_name='kafka-dev',target_ns='default',dry_run=False)`。**断言**：apply 后 mv-tw **全程无 CrashLoopBackOff、持续 Healthy**（对比 ③a 崩溃——核心 DoD）；wal-alter 返回 `{"msg":"OK"}`。

- [ ] **Step 4: 钉死 (b) etcd key + verify 通** —— exec 进 etcd pod `etcdctl get --prefix mv-tw/`（必要时加 `--user root:<pass>`），grep WAL 类型键，确认切后值含 `kafka`。若前缀/认证与代码不符 → 修 `MILVUS_WAL_META_PREFIX` / `_verify_wal` etcdctl 命令并补提交。确认 `verify-mq-type` 步能真通过、task succeeded。

- [ ] **Step 5: 清理 + 保护真实例** —— `mb delete mv-tw` + 删 `mv-tw-minio` secret；确认 **milvus007 全程未动、仍 Healthy**。

- [ ] **Step 6: 全量回归 + whole-branch review + finish-branch** —— `pytest -q` 全绿；派 opus 终审整分支 diff；然后 finishing-a-development-branch（合并 main + push github.com/tinswzy/milvus-bootstrap）。

---

## Self-Review

**Spec coverage:**
- 注入连接不翻类型 → Task 1（helper）+ Task 2（switch_mq 应用 + 核心回归测试 msgStreamType 保持源）✓
- 步骤形状不变 + compensate 剥离 → Task 3 保留 `for s in steps: s.compensate=None` + 既有回归测试 ✓
- verify 读 etcd → Task 3 ✓
- 成功后 snapshot 记目标态、CR 不二次 patch → Task 2 Step 3 ✓
- 前端/端点/gate/202 不动 → 无任务改动，Task 2 Step 4 仅更新一处受影响断言 ✓
- Live DoD（无崩、wal-alter OK、etcd==target、存活、清理、milvus007 不动）→ Task 4 ✓
- 两个 live 未知值 (a)(b) → Task 4 Step 2/4 钉死 ✓

**Placeholder scan:** 无 TBD/TODO；Task 3 的 etcd 前缀/认证为「已知待 live 确认的具体常量」（有默认值可跑，非占位），已在 Task 4 明确验证/修正路径。

**Type consistency:** `_mq_conn_conf(target_wal, endpoint)` 签名 Task 1 定义、Task 2 调用一致；`_verify_wal(adapter, etcd_ns, etcd_selector, root, target_wal, tries, sleep_s)` Task 3 定义与 `plan_switch_mq_steps` 调用一致；`MILVUS_WAL_META_PREFIX` Task 3 定义并在同文件引用。
