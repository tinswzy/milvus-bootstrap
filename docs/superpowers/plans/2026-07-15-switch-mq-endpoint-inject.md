# WebUI Switch-MQ ③a：端点注入 + 真校验 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 选中具体目标 MQ 实例时真正生效——注入端点到 milvus 配置 → apply CR → 等 operator 滚动就绪 → wal/alter → 真校验 milvus 当前 WAL == 目标。

**Architecture:** 后端 `plan_switch_mq_steps` 重写为真步骤序列（复用 `plan_install_steps` 做 render+apply+wait，再 wal-alter + verify-mq-type）；`context.switch_mq` 加 `target_name/target_ns` 注入端点 + 更新快照；端点 req 加两字段；前端 `submitSwitchMq` 透传选中实例 + 完成提示旧 MQ 可选清理。跑现有流式任务。

**Tech Stack:** Python + FastAPI + pytest；vanilla JS；`node --check`。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-15-switch-mq-endpoint-inject-design.md`（决策 D1–D8）。
- **切换序列**：`apply-config`(复用 `plan_install_steps(spec2)`=render+apply+wait_cr) → `wal-alter`(exec) → `verify-mq-type`(有界循环真校验)；**decommission-old 不再是动作步**（改前端可选清理提示）。工作流**完成于 verify**。
- **真校验不谎报**：`verify-mq-type` 有界循环 exec 读 milvus 当前 WAL，`== target_wal` 才成、超时则该步 failed。fake adapter 的 exec 回显 `[fake]…` → 视为"模拟通过"（fake 无法模拟 milvus WAL，真校验以真集群 DoD 为准）；真 k8s 走 `target_wal in 输出` 真判。
- **准则**：跑成有界异步任务（现有 TaskRunner + 流式 step 日志）；wait/verify 是**任务内有界循环**（同 wait_cr），非常驻 poller；三层护栏 + 门禁 409/force 保留；无 setInterval。
- **旧 MQ 清理不自动**：完成后前端仅提示可选人工清理（共享/external 勿删、引导 Dependencies）；不建新删除逻辑。
- `target_wal→mq id`：kafka/pulsar/rocksmq id==wal；woodpecker→`woodpecker-embedded`。端点参数：kafka→`kafkaBrokers`、pulsar→`pulsarEndpoint`（嵌入型无端点）。
- 命令在 `milvus-bootstrap/` 下：`cd milvus-bootstrap && source .venv/bin/activate`。基线 216 passing。
- Git 纪律：只 `git add` 本任务列出的文件 + `git commit` 一次；**禁止** filter-branch/rebase/reset/push/amend/切分支。署名沿用 `user.name=tinswzy`。

---

### Task 1: 后端 — `plan_switch_mq_steps` 重写 + `context.switch_mq` 端点注入

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py`（`plan_switch_mq_steps` 重写；加 `_wal_to_mq_id`、`_verify_wal`；`import time`）
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/context.py`（`switch_mq` 加 `target_name/target_ns` + 注入 + 快照更新）
- Test: `milvus-bootstrap/tests/test_switch_mq.py`（更新旧步骤断言 + 加新）

**Interfaces:**
- Consumes: `self.plan_install_steps(spec, adapter)` (base driver; milvus→render+apply-objects+wait_cr), `adapter.exec`.
- Produces:
  - `MilvusDriver._wal_to_mq_id(wal) -> str`
  - `MilvusDriver._verify_wal(adapter, ns, selector, target_wal, tries=20, sleep_s=3) -> str`
  - `MilvusDriver.plan_switch_mq_steps(spec, adapter, target_wal) -> list[Step]`（新序列）
  - `Core.switch_mq(instance_id, target_wal, target_name="", target_ns="", dry_run=True, force=False) -> Task`

- [ ] **Step 1: Update old + write new failing tests**

In `milvus-bootstrap/tests/test_switch_mq.py`, REPLACE `test_switch_mq_dry_run_steps` with a version matching the new step names, and ADD the endpoint-injection tests:
```python
def test_switch_mq_dry_run_steps(core: Core) -> None:
    task = core.switch_mq("milvus-dev", "kafka", dry_run=True)
    assert task.status == TaskStatus.succeeded
    names = [s.name for s in task.steps]
    # milvus uses the operator-cr install method: apply-cr + wait-status come from plan_install_steps
    assert "apply-cr" in names and "wait-status" in names      # config apply + wait first
    assert "wal-alter" in names and "verify-mq-type" in names
    assert "decommission-old" not in names                     # decommission is now optional/manual
    assert names[-1] == "verify-mq-type"                       # workflow completes at verify
    wal = next(s for s in task.steps if s.name == "wal-alter")
    assert "wal/alter" in wal.plan and "target_wal_name" in wal.plan and "kafka" in wal.plan


def test_switch_mq_injects_endpoint_and_updates_snapshot(core_with_milvus_kafka) -> None:
    # milvus-dev is on kafka; switch it to pulsar targeting a specific instance
    c = core_with_milvus_kafka
    task = c.switch_mq("milvus-dev", "pulsar", target_name="pulsar-dev", target_ns="default", dry_run=False)
    assert task.status == TaskStatus.succeeded
    snap = c.state.get_instance("milvus-dev").spec_snapshot
    assert snap["params"]["mq"] == "pulsar"
    assert "pulsar-dev" in snap["params"]["pulsarEndpoint"] and ":6650" in snap["params"]["pulsarEndpoint"]
    assert [s.name for s in task.steps][-1] == "verify-mq-type"


def test_switch_mq_embedded_no_endpoint(core_with_milvus_kafka) -> None:
    c = core_with_milvus_kafka
    task = c.switch_mq("milvus-dev", "rocksmq", dry_run=False)      # embedded, no target instance
    assert task.status == TaskStatus.succeeded
    snap = c.state.get_instance("milvus-dev").spec_snapshot
    assert snap["params"]["mq"] == "rocksmq"
    assert "pulsarEndpoint" not in snap["params"]                  # embedded switch injects no endpoint
```
(The existing `core_with_milvus_kafka` fixture installs milvus with `params={"mq": "kafka"}`. `test_switch_mq_apply_execs_wal_alter` and the gate tests stay as-is.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_switch_mq.py -q`
Expected: FAIL — old step names gone / `verify-mq-type` not produced / `switch_mq` has no `target_name` kwarg.

- [ ] **Step 3: Rewrite `plan_switch_mq_steps` + add helpers in `milvus.py`**

Add `import time` near the top of `milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py` (with the other imports). Replace the existing `plan_switch_mq_steps` method with:
```python
    def _wal_to_mq_id(self, wal: str) -> str:
        return {"kafka": "kafka", "pulsar": "pulsar", "rocksmq": "rocksmq",
                "woodpecker": "woodpecker-embedded"}.get(wal, wal)

    def _verify_wal(self, adapter, ns, selector, target_wal, tries=20, sleep_s=3) -> str:
        """Bounded poll of milvus's own current WAL until == target (honest, no over-claim).
        Fake adapter echoes '[fake] …' → treated as simulated-pass; real k8s checks the response."""
        read = ["curl", "-s", "http://localhost:9091/management/wal/status"]  # exact path: confirm in live DoD
        for _ in range(tries):
            out = str(adapter.exec(namespace=ns, label_selector=selector, command=read))
            if target_wal in out or out.strip().startswith("[fake]"):
                return f"已确认当前 WAL == {target_wal}（{out.strip()[:120]}）"
            time.sleep(sleep_s)
        raise TimeoutError(f"切换后未在 {tries * sleep_s}s 内确认 WAL == {target_wal}")

    def plan_switch_mq_steps(self, spec, adapter, target_wal: str) -> list[Step]:
        """Real switch: apply new MQ+endpoint into CR (render+apply+wait) → wal/alter → verify."""
        ns, name = spec.namespace, spec.name
        selector = f"app.kubernetes.io/instance={name}"
        steps = list(self.plan_install_steps(spec, adapter))       # render + apply-objects + wait-ready (spec has new mq+endpoint)
        alter = ["curl", "-s", "-X", "POST", "http://localhost:9091/management/wal/alter",
                 "-d", json.dumps({"target_wal_name": target_wal})]
        steps.append(Step(name="wal-alter", plan="在 milvus pod 内执行：" + " ".join(alter),
                          action=lambda: adapter.exec(namespace=ns, label_selector=selector, command=alter)))
        steps.append(Step(name="verify-mq-type",
                          plan=f"轮询 milvus 当前 WAL 直到 == {target_wal}（有界·超时）",
                          action=lambda: self._verify_wal(adapter, ns, selector, target_wal)))
        return steps
```
(`json` and `Step` are already imported in this file — the old `plan_switch_mq_steps` used them. Confirm `time` is imported.)

- [ ] **Step 4: Extend `context.switch_mq` in `context.py`**

Replace the `switch_mq` method in `milvus-bootstrap/src/milvus_bootstrap/core/context.py` with:
```python
    def switch_mq(self, instance_id: str, target_wal: str, target_name: str = "",
                  target_ns: str = "", dry_run: bool = True, force: bool = False) -> Task:
        inst = self.state.get_instance(instance_id)
        if inst is None:
            raise KeyError(f"未找到实例 {instance_id}")
        if not inst.spec_snapshot:
            raise ValueError(f"{instance_id} 无安装快照")
        spec = InstallSpec.model_validate(inst.spec_snapshot)
        if spec.kind != "milvus":
            raise ValueError("switch-mq 仅适用于 milvus 实例")
        from . import compat
        cur_mq = spec.params.get("mq", "")
        cur_opt = compat.get_option(cur_mq)
        current_wal = cur_opt.wal if cur_opt else cur_mq
        compat.gate("switch-mq", {"current_wal": current_wal, "target_wal": target_wal}, force=force)
        driver = self.registry.get("milvus")
        spec2 = spec.model_copy(deep=True)
        spec2.params = dict(spec2.params)
        spec2.params["mq"] = driver._wal_to_mq_id(target_wal)
        _ep = {"kafka": (f"{target_name}.{target_ns or spec.namespace}.svc:9092", "kafkaBrokers"),
               "pulsar": (f"{target_name}-broker.{target_ns or spec.namespace}.svc:6650", "pulsarEndpoint")}
        if target_wal in _ep and target_name:
            endpoint, param = _ep[target_wal]
            spec2.params[param] = endpoint
        steps = driver.plan_switch_mq_steps(spec2, self.adapter, target_wal)
        task = self.engine.run(type="switch-mq", target=instance_id, steps=steps, dry_run=dry_run)
        self.state.put_task(task)
        if not dry_run and task.status == TaskStatus.succeeded:
            inst.spec_snapshot = spec2.model_dump(mode="json")
            self.state.put_instance(inst)
        return task
```

- [ ] **Step 5: Run tests + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_switch_mq.py -q && python -m pytest -q`
Expected: switch_mq tests PASS; full suite PASS (was 216; net: updated 1 + added 2 → ~+2).

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/drivers/milvus.py src/milvus_bootstrap/core/context.py tests/test_switch_mq.py
git commit -m "feat(core): switch-mq真重指 — inject target endpoint into CR (apply→wait→wal-alter→verify), update snapshot"
```

---

### Task 2: 端点 — `/api/switch-mq` + `/switch-mq` 加 `target_name/target_ns`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（`SwitchMqReq`、`SwitchMqApiReq`、`switch_mq`、`api_switch_mq`）
- Test: `milvus-bootstrap/tests/test_web_switchmq.py`（追加）

**Interfaces:**
- Consumes: `Core.switch_mq(instance, target_wal, target_name, target_ns, dry_run, force)` (Task 1).
- Produces: both switch-mq routes accept + forward `target_name`/`target_ns`.

- [ ] **Step 1: Write the failing test**

Add to `milvus-bootstrap/tests/test_web_switchmq.py`:
```python
def test_api_switch_mq_passes_target_instance(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server.app import _core
    with TestClient(app) as client:
        _core().install(InstallSpec(kind="milvus", name="mq-mv",
                                    params={"mq": "kafka", "image": "milvusdb/milvus:v2.6.18"}), dry_run=False)
        r = client.post("/api/switch-mq", json={"instance": "mq-mv", "target_wal": "pulsar",
                                                "target_name": "pulsar-dev", "target_ns": "default", "dry_run": True})
        assert r.status_code == 200
        names = [s["name"] for s in r.json()["task"]["steps"]]
        assert "wal-alter" in names and "verify-mq-type" in names
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py::test_api_switch_mq_passes_target_instance -q`
Expected: FAIL — req models reject the extra fields / target not forwarded.

- [ ] **Step 3: Add the two fields + forward them**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`:
Add to `SwitchMqReq` (after `force: bool = False`):
```python
    target_name: str = ""
    target_ns: str = ""
```
Change the `/switch-mq` sync route body to forward them:
```python
@app.post("/switch-mq")
def switch_mq(req: SwitchMqReq) -> dict[str, Any]:
    return _core().switch_mq(req.instance, req.target_wal, target_name=req.target_name,
                             target_ns=req.target_ns, dry_run=req.dry_run, force=req.force).model_dump()
```
Add to `SwitchMqApiReq` (after `force: bool = False`):
```python
    target_name: str = ""
    target_ns: str = ""
```
Change `api_switch_mq`'s three `_core().switch_mq(...)` calls to pass the target instance:
```python
@app.post("/api/switch-mq")
def api_switch_mq(req: SwitchMqApiReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().switch_mq(req.instance, req.target_wal, target_name=req.target_name,
                                 target_ns=req.target_ns, dry_run=True, force=req.force)
        return {"task": task.model_dump(mode="json")}
    _core().switch_mq(req.instance, req.target_wal, target_name=req.target_name,
                      target_ns=req.target_ns, dry_run=True, force=req.force)   # sync gate pre-check
    tid = runner.submit(lambda: _core().switch_mq(req.instance, req.target_wal, target_name=req.target_name,
                                                  target_ns=req.target_ns, dry_run=False, force=req.force))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)
```

- [ ] **Step 4: Run test + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_switchmq.py -q && python -m pytest -q`
Expected: PASS (+1).

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/server/app.py tests/test_web_switchmq.py
git commit -m "feat(server): /api/switch-mq + /switch-mq accept target_name/target_ns (forward to endpoint injection)"
```

---

### Task 3: 前端 — `submitSwitchMq` 透传实例 + 完成提示旧 MQ 清理

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（`submitSwitchMq` 签名+POST；`renderSwitchMq` 存 selectedInst/Ns + 调用传参；完成提示）
- Test: `milvus-bootstrap/tests/test_web_static.py`（追加）

**Interfaces:**
- Consumes: `/api/switch-mq` now accepting `target_name`/`target_ns` (Task 2); the target `<option>`'s `data-inst`/`data-ns` (② shipped).
- Produces: `submitSwitchMq(name, targetWal, dryRun, force, el, targetName="", targetNs="")` forwards the instance; renderSwitchMq passes selected instance; completion handoff shows old-MQ cleanup hint.

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_switch_mq_passes_instance_and_cleanup_hint(client):
    js = client.get("/assets/web.js").text
    body = js.split("async function submitSwitchMq", 1)[1].split("\nasync function ", 1)[0]
    assert "target_name" in body and "target_ns" in body           # forwarded to POST
    assert "旧 MQ" in body and "deps.html" in body                 # cleanup hint + Dependencies link
    rbody = js.split("async function renderSwitchMq", 1)[1].split("\nasync function ", 1)[0]
    assert "selectedInst" in rbody                                 # captured chosen instance
    assert "setInterval" not in js
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_switch_mq_passes_instance_and_cleanup_hint -q`
Expected: FAIL — markers absent.

- [ ] **Step 3: Extend `submitSwitchMq` (signature + POST + cleanup hint)**

In `web.js`, change `submitSwitchMq`'s signature and POST body, and the 202 onDone handoff. The current function starts:
```javascript
async function submitSwitchMq(name, targetWal, dryRun, force, el) {
  el.innerHTML = '<span class="muted">提交中…</span>';
  let resp;
  try { resp = await postJSON('api/switch-mq', { instance: name, target_wal: targetWal, dry_run: dryRun, force: !!force }); }
```
Change the signature + POST to carry the instance:
```javascript
async function submitSwitchMq(name, targetWal, dryRun, force, el, targetName = '', targetNs = '') {
  el.innerHTML = '<span class="muted">提交中…</span>';
  let resp;
  try { resp = await postJSON('api/switch-mq', { instance: name, target_wal: targetWal, dry_run: dryRun, force: !!force, target_name: targetName, target_ns: targetNs }); }
```
Then in the `status === 202` branch's `pollTask` onDone, after the existing "已提交 MQ 切换" + refresh button, append the old-MQ cleanup hint. The current onDone is:
```javascript
    await pollTask(data.task_id, el, () => {
      el.innerHTML += '<div class="conn ok" style="margin-top:8px">已提交 MQ 切换 · operator 处理中</div>' +
        '<button class="btn btn-ghost btn-sm" id="mq-refresh" style="margin-top:6px">🔄 刷新</button>';
      const b = document.getElementById('mq-refresh');
      if (b) b.onclick = () => { location.reload(); };
    });
```
Replace it with:
```javascript
    await pollTask(data.task_id, el, () => {
      el.innerHTML += '<div class="conn ok" style="margin-top:8px">已提交 MQ 切换 · operator 处理中</div>' +
        '<div class="muted" style="margin:6px 0;font-size:12px">旧 MQ 未自动清理 —— 清理是可选人工操作。' +
        '若旧 MQ 仍被其他实例使用或为 external，请勿删除；确认独占后可去 ' +
        '<a href="deps.html">Dependencies</a> 页手动处理。</div>' +
        '<button class="btn btn-ghost btn-sm" id="mq-refresh" style="margin-top:6px">🔄 刷新</button>';
      const b = document.getElementById('mq-refresh');
      if (b) b.onclick = () => { location.reload(); };
    });
```
(200 dry-run / 409 force branches unchanged. The 409 `[强制]` re-call — update it to preserve the instance args too:)
Change the 409 force click line from
```javascript
    if (b) b.onclick = () => { if (confirm('确认跳过兼容门禁强制切换 MQ？')) submitSwitchMq(name, targetWal, dryRun, true, el); };
```
to
```javascript
    if (b) b.onclick = () => { if (confirm('确认跳过兼容门禁强制切换 MQ？')) submitSwitchMq(name, targetWal, dryRun, true, el, targetName, targetNs); };
```

- [ ] **Step 4: Capture + pass the chosen instance in `renderSwitchMq`**

In `web.js` `renderSwitchMq`, add two closure vars next to `let selectedWal = null;`:
```javascript
  let selectedInst = '';
  let selectedNs = '';
```
In `tgtSel.onchange`, capture them (the `data-inst`/`data-ns` are already read for display). Change the `if (wal) { … }` block so `inst`/`ns` persist:
```javascript
  tgtSel.onchange = () => {
    const wal = tgtSel.value;
    selectedWal = wal || null;
    if (wal) {
      const opt = tgtSel.options[tgtSel.selectedIndex];
      selectedInst = opt.getAttribute('data-inst') || '';
      selectedNs = opt.getAttribute('data-ns') || '';
      tgtLogo.textContent = mqLogo(wal);
      tgtName.textContent = selectedInst ? `${wal} · ${selectedInst}` : wal;
      tgtReason.textContent = noteByWal[wal] || '';
    } else {
      selectedInst = ''; selectedNs = '';
      tgtLogo.textContent = '🎯'; tgtName.textContent = '选择目标'; tgtReason.textContent = '';
    }
    advance();
  };
```
Also reset them in `load()`'s reset block (where `selectedWal = null` is set) — change that line to:
```javascript
    selectedWal = null; selectedInst = ''; selectedNs = ''; res.innerHTML = ''; ack.checked = false;
```
Finally, pass them in the `#sw-dry` / `#sw-go` calls:
```javascript
  dry.onclick = () => { if (selectedWal) submitSwitchMq(selInst, selectedWal, true, false, res, selectedInst, selectedNs); };
  go.onclick = () => {
    if (!selectedWal) return;
    if (confirm('确认切换 ' + selInst + ' 的 MQ 到 ' + selectedWal +
                '？这会更改 WAL 并在 pod 内执行变更，存量流式数据将无法保留。')) {
      setStep(3);
      submitSwitchMq(selInst, selectedWal, false, false, res, selectedInst, selectedNs);
    }
  };
```

- [ ] **Step 5: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js tests/test_web_static.py
git commit -m "feat(webui): switch-mq passes chosen instance (target_name/ns) + old-MQ cleanup hint on completion"
```

---

## Notes for the executor
- 每个 Task 末尾跑全量 `python -m pytest -q`（基线 216），前端改动后 `node --check`。
- 不新增 setInterval；wait/verify 是任务内有界循环；旧 MQ 清理仅提示（不自动删）。
- **手动 DoD（真集群 · throwaway milvus 真切一次）**：装 throwaway milvus(pulsar) + kafka → 卡「切换 MQ」拓扑页选 kafka-dev → 勾护栏 → 切换 → 流式见 render→apply→wait-ready→wal-alter→verify-mq-type 逐步；milvus CR 变 kafka+kafka-dev 端点、operator 滚动、WAL 切 kafka、verify 命中(**确认 `wal/status` 读路径，若 milvus 无此接口按 spec §7 退化为 CR msgStreamType==目标弱校验并回报**)；完成提示旧 MQ(pulsar) 可选清理；快照更新为 kafka。
