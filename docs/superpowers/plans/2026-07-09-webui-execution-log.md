# WebUI 执行步骤流式日志 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 install/upgrade/delete/dry-run 的执行过程从"倒计时黑盒"变成逐步骤流式日志面板（新在上、旧在下，含实际 k8s/helm 命令与结果），并把"透明/可观测"写进 README 设计原则。

**Architecture:** 复用已有 `Task.steps`（name/status/plan/detail）。后端加一个 `core/progress.py` ContextVar sink，`TaskEngine.run` 每完成一步就 publish 当前 task；`TaskRunner.submit` 的 worker 把 publish 的快照写进任务内存记录的 `partial` 字段；`GET /api/task` 运行中返回 `partial`。前端新增 `logPanel(task,running)` 滚动面板 + 泛化 `pollTask`（有界、只轮 mb 内存、完成即停），install/upgrade/delete/dry-run 全接上。

**Tech Stack:** Python 3 / FastAPI / pydantic / pytest（后端）；vanilla JS + CSS（前端）；`node --check` 做 JS 语法校验。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-09-webui-execution-log-design.md`（决策 D1–D9 逐条落实）。
- **无轮询准则调和**：唯一轮询是**短时、有界、针对 mb 自己的内存任务记录（`GET /api/task`，非 k8s）**，任务一完即停。**不得**引入对 k8s 的持续轮询；**不得**改 `wait_cr`/`wait_ready`；**不得**引入 SSE/WebSocket。
- **不谎报成功**：upgrade 完成后仍显示「已提交升级 · operator 正在滚动 · 查看进展」；delete 完成后「已提交删除 · operator 回收 · 🔄刷新列表」——流式只覆盖 mb 自身执行的那几步。
- 复用 `Task.steps`，**不新造**日志格式。命令行取 `s.plan`（helm 步骤已含字面命令；k8s-API 步骤为动作描述，不伪造 shell 命令），结果取 `s.detail`。
- best-effort：fake / 无步骤 / 连不上时面板显示占位不崩。
- 全部命令在仓库根的子目录 `milvus-bootstrap/` 下运行：`cd milvus-bootstrap && source .venv/bin/activate` 后跑 `pytest`。
- Git 纪律（每个实现者必须遵守）：只 `git add` + `git commit`；**禁止** `git filter-branch` / `rebase` / `reset` / `push` / `amend` / 改历史。提交署名沿用仓库当前 `user.name=tinswzy`。

---

### Task 1: `core/progress.py` 进度 sink + engine 发布

**Files:**
- Create: `milvus-bootstrap/src/milvus_bootstrap/core/progress.py`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/tasks/engine.py`
- Test: `milvus-bootstrap/tests/test_progress.py` (create)

**Interfaces:**
- Produces:
  - `progress.set_sink(fn: Callable[[Task], None]) -> Token`
  - `progress.reset_sink(token: Token) -> None`
  - `progress.publish(task: Task) -> None`（无 sink 时 no-op）
  - `TaskEngine.run(...)` 在 dry-run=False 时，每次 append `StepResult` 与每次翻 ok/skipped/failed 后调用 `progress.publish(task)`。

- [ ] **Step 1: Write the failing test**

`milvus-bootstrap/tests/test_progress.py`:
```python
from milvus_bootstrap.core import progress
from milvus_bootstrap.core.tasks.engine import Step, TaskEngine


def test_publish_is_noop_without_sink():
    # No sink registered -> must not raise.
    progress.publish(object())


def test_set_and_reset_sink():
    seen = []
    token = progress.set_sink(lambda t: seen.append(t))
    try:
        progress.publish("x")
        assert seen == ["x"]
    finally:
        progress.reset_sink(token)
    progress.publish("y")          # after reset -> no-op
    assert seen == ["x"]


def test_engine_publishes_intermediate_steps():
    counts = []
    token = progress.set_sink(lambda t: counts.append(len(t.steps)))
    try:
        steps = [Step(name="a", plan="planA", action=lambda: "ra"),
                 Step(name="b", plan="planB", action=lambda: "rb")]
        task = TaskEngine().run(type="x", target="y", steps=steps, dry_run=False)
    finally:
        progress.reset_sink(token)
    # published while the task was still mid-flight: first publish saw only 1 step
    assert counts and counts[0] == 1 and counts[-1] == 2
    assert task.status.value == "succeeded"
    assert [s.status.value for s in task.steps] == ["ok", "ok"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_progress.py -q`
Expected: FAIL — `ModuleNotFoundError: milvus_bootstrap.core.progress` (and engine doesn't publish yet).

- [ ] **Step 3: Create `core/progress.py`**

```python
"""Task-scoped progress sink (ContextVar). The engine publishes each step;
a TaskRunner worker registers a sink to capture snapshots. No-op otherwise."""
from __future__ import annotations

import contextvars
from collections.abc import Callable
from typing import Any

_sink: contextvars.ContextVar = contextvars.ContextVar("mb_task_progress_sink", default=None)


def set_sink(fn: Callable[[Any], None]) -> contextvars.Token:
    return _sink.set(fn)


def reset_sink(token: contextvars.Token) -> None:
    _sink.reset(token)


def publish(task: Any) -> None:
    fn = _sink.get()
    if fn is not None:
        fn(task)
```

- [ ] **Step 4: Wire `engine.py` to publish**

In `milvus-bootstrap/src/milvus_bootstrap/core/tasks/engine.py`, add the import near the top (with the other relative imports):
```python
from .. import progress
```
Then in `run()`'s real-run loop (the `if dry_run:` block stays unchanged), publish after each state change. Replace the loop body so it reads:
```python
        done: list[Step] = []
        for s in steps:
            res = StepResult(name=s.name, plan=s.plan, status=StepStatus.running)
            task.steps.append(res)
            progress.publish(task)
            try:
                if s.precheck and s.precheck():
                    res.status = StepStatus.skipped
                    res.detail = "precheck: 已是目标态，跳过"
                    progress.publish(task)
                    continue
                res.detail = s.action() if s.action else ""
                res.status = StepStatus.ok
                progress.publish(task)
                done.append(s)
            except Exception as exc:  # noqa: BLE001
                res.status = StepStatus.failed
                res.detail = str(exc)
                progress.publish(task)
                task.audit.append(f"step '{s.name}' failed: {exc}")
                for d in reversed(done):
                    if d.compensate:
                        task.audit.append(f"compensate '{d.name}': {d.compensate()}")
                task.status = TaskStatus.rolled_back
                return task
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_progress.py -q`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/progress.py src/milvus_bootstrap/core/tasks/engine.py tests/test_progress.py
git commit -m "feat(core): progress sink + engine publishes each step (live task steps)"
```

---

### Task 2: `TaskRunner` 暴露 `partial` + `GET /api/task` 返回运行中步骤

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/taskrunner.py`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py:208-218`
- Test: `milvus-bootstrap/tests/test_taskrunner.py` (create), `milvus-bootstrap/tests/test_web_task.py` (create)

**Interfaces:**
- Consumes: `progress.set_sink/reset_sink/publish` (Task 1).
- Produces:
  - `TaskRunner.submit(fn)` — worker registers a sink writing each published task's `model_dump(mode="json")` into `rec["partial"]`; record shape becomes `{"state","result","error","partial"}`.
  - `GET /api/task/{id}` — running → `{"state":"running","task":<partial-or-None>,"error":None}`；error → `{"state":"error","task":<partial-or-None>,"error":...}`；done 不变。

- [ ] **Step 1: Write the failing test (runner)**

`milvus-bootstrap/tests/test_taskrunner.py`:
```python
import threading
import time

from milvus_bootstrap.core import progress
from milvus_bootstrap.core.taskrunner import TaskRunner


class _FakeTask:
    def __init__(self, n):
        self.n = n
    def model_dump(self, mode=None):
        return {"steps": [{"name": f"s{i}"} for i in range(self.n)], "status": "running"}


def test_partial_visible_while_running():
    r = TaskRunner()
    gate = threading.Event()

    def fn():
        progress.publish(_FakeTask(2))   # engine would do this
        gate.wait(2)
        return "final"

    tid = r.submit(fn)
    partial = None
    for _ in range(200):
        rec = r.get(tid)
        if rec and rec.get("partial"):
            partial = rec["partial"]
            break
        time.sleep(0.01)
    gate.set()
    assert partial == {"steps": [{"name": "s0"}, {"name": "s1"}], "status": "running"}
    # after completion
    for _ in range(200):
        rec = r.get(tid)
        if rec["state"] != "running":
            break
        time.sleep(0.01)
    assert rec["state"] == "done" and rec["result"] == "final"


def test_get_unknown_is_none():
    assert TaskRunner().get("nope") is None
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_taskrunner.py -q`
Expected: FAIL — `rec.get("partial")` always None (submit doesn't register a sink).

- [ ] **Step 3: Implement runner sink**

Replace `milvus-bootstrap/src/milvus_bootstrap/core/taskrunner.py` body with:
```python
"""Tiny in-process async runner: submit a fn, poll its status + live steps."""
from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any

from . import progress


class TaskRunner:
    def __init__(self) -> None:
        self._recs: dict[str, dict] = {}
        self._lock = threading.Lock()

    def _set_partial(self, tid: str, task: Any) -> None:
        dump = task.model_dump(mode="json")
        with self._lock:
            rec = self._recs.get(tid)
            if rec is not None:
                rec["partial"] = dump

    def submit(self, fn: Callable[[], Any]) -> str:
        tid = uuid.uuid4().hex[:12]
        with self._lock:
            self._recs[tid] = {"state": "running", "result": None, "error": None, "partial": None}

        def _run() -> None:
            token = progress.set_sink(lambda t: self._set_partial(tid, t))
            try:
                res = fn()
                state, result, error = "done", res, None
            except Exception as exc:  # noqa: BLE001
                state, result, error = "error", None, str(exc)
            finally:
                progress.reset_sink(token)
            with self._lock:
                old = self._recs.get(tid) or {}
                self._recs[tid] = {"state": state, "result": result,
                                   "error": error, "partial": old.get("partial")}

        threading.Thread(target=_run, daemon=True).start()
        return tid

    def get(self, task_id: str) -> dict | None:
        with self._lock:
            rec = self._recs.get(task_id)
            return dict(rec) if rec is not None else None
```

- [ ] **Step 4: Run runner test to verify pass**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_taskrunner.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Write the failing endpoint test**

`milvus-bootstrap/tests/test_web_task.py`:
```python
from fastapi.testclient import TestClient

from milvus_bootstrap.server.app import app, runner


def test_api_task_running_returns_partial_steps():
    # seed a running record with a partial task snapshot (as the sink would)
    with runner._lock:
        runner._recs["t-run"] = {
            "state": "running", "result": None, "error": None,
            "partial": {"status": "running", "steps": [{"name": "render", "status": "ok",
                                                        "plan": "将执行：helm ...", "detail": "ok"}]},
        }
    r = TestClient(app).get("/api/task/t-run")
    assert r.status_code == 200
    body = r.json()
    assert body["state"] == "running"
    assert body["task"]["steps"][0]["name"] == "render"


def test_api_task_unknown_404():
    r = TestClient(app).get("/api/task/does-not-exist")
    assert r.status_code == 404
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_task.py -q`
Expected: FAIL — running branch returns `"task": None` (current code ignores partial).

- [ ] **Step 7: Update `GET /api/task`**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, replace the running/error branches of `api_task` (lines ~213-216) so they surface `partial`:
```python
    if rec["state"] == "running":
        return {"state": "running", "task": rec.get("partial"), "error": None}
    if rec["state"] == "error":
        return {"state": "error", "task": rec.get("partial"), "error": rec["error"]}
```
(The `done` branch — `dump = rec["result"].model_dump(...)` — stays unchanged.)

- [ ] **Step 8: Run endpoint + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_task.py tests/test_taskrunner.py tests/test_progress.py -q && python -m pytest -q`
Expected: all PASS.

- [ ] **Step 9: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/core/taskrunner.py src/milvus_bootstrap/server/app.py tests/test_taskrunner.py tests/test_web_task.py
git commit -m "feat(server): task runner exposes partial steps; GET /api/task streams running steps"
```

---

### Task 3: 前端 `logPanel` + `pollTask` + install 接流式

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `GET /api/task` returns `{state, task, error}` with `task.steps` (Task 2); helpers `esc`, `getJSON`, `badge` (existing).
- Produces:
  - `logPanel(task, running)` → HTML string（新在上、定高滚动、命令 mono）。
  - `pollTask(taskId, el, onDone)` → 轮询 mb 内存至完成，实时 `logPanel`，完成调 `onDone(task)`。
  - `renderTaskResult(task)` 改为 `return logPanel(task, false)`（保留调用点）。`pollInstall` 移除；`submitInstall` 202 分支改调 `pollTask`。

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_exec_log_panel_present(client):
    js = client.get("/assets/web.js").text
    assert "function logPanel" in js and "function pollTask" in js
    assert ".slice().reverse()" in js          # newest-on-top
    assert "logcmd" in js                       # command shown mono
    assert "function pollInstall" not in js     # old countdown poller removed
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_exec_log_panel_present -q`
Expected: FAIL (functions not present; `pollInstall` still present).

- [ ] **Step 3: Add `logPanel` + `pollTask`, rewrite `renderTaskResult`, remove `pollInstall`**

In `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`, replace the whole existing `renderTaskResult` function AND the whole existing `pollInstall` function with the following three functions:
```javascript
const STEP_ICON = { ok: '✓', failed: '✗', skipped: '⤼', running: '⏳', planned: '○', pending: '·' };

function logPanel(task, running) {
  const head = running
    ? '<div class="loghead run">⏳ 执行中…</div>'
    : (task && task.status === 'succeeded'
        ? '<div class="loghead ok">✅ 完成</div>'
        : '<div class="loghead bad">❌ 出错</div>');
  const steps = (task && task.steps) ? task.steps.slice().reverse() : [];   // newest on top
  const rows = steps.map(s => {
    const ic = STEP_ICON[s.status] || '·';
    const cmd = s.plan ? `<div class="logcmd">▸ ${esc(s.plan)}</div>` : '';
    const det = s.detail ? `<div class="logdet">${esc(s.detail)}</div>` : '';
    return `<div class="logrow st-${esc(s.status)}"><span class="ic">${ic}</span>` +
           `<div class="logbody"><b>${esc(s.name)}</b>${cmd}${det}</div></div>`;
  }).join('') || '<div class="muted" style="padding:8px">暂无步骤…</div>';
  return head + `<div class="logpanel">${rows}</div>`;
}

function renderTaskResult(task) { return logPanel(task, false); }

async function pollTask(taskId, el, onDone) {
  while (true) {
    let j;
    try { j = await getJSON('api/task/' + taskId); }
    catch (e) { el.innerHTML = '<span class="conn bad">轮询失败：' + esc(e.message) + '</span>'; return; }
    if (j.state === 'running') {
      el.innerHTML = logPanel(j.task, true);
      await new Promise(r => setTimeout(r, 800));
      continue;
    }
    if (j.state === 'error') {
      el.innerHTML = logPanel(j.task, false) +
        '<div class="conn bad" style="margin-top:8px">执行出错：' + esc(j.error) + '</div>';
      return;
    }
    el.innerHTML = logPanel(j.task, false);
    if (onDone) onDone(j.task);
    return;
  }
}
```

- [ ] **Step 4: Rewire `submitInstall` 202 branch**

In `submitInstall`, change the 202 line from `await pollInstall(data.task_id, resultEl);` to:
```javascript
  if (status === 202) { await pollTask(data.task_id, resultEl); return; }
```
(The `status === 200` dry-run branch already calls `renderTaskResult(data.task)`, which now renders `logPanel` — leave it.)

- [ ] **Step 5: Verify JS parses + test passes**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q`
Expected: JS OK; tests PASS.

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js tests/test_web_static.py
git commit -m "feat(webui): logPanel + pollTask — install execution now streams steps (newest on top)"
```

---

### Task 4: upgrade 接流式（apply 步骤 → 完成后收口「查看进展」）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（`submitUpgrade`）
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `pollTask`, `logPanel`, `openProgress(name)`, `renderMilvus()`, `closeModal()`, `esc` (existing).
- Produces: `submitUpgrade` 202 分支流式 apply 步骤，完成 `onDone` 追加「已提交升级 · operator 正在滚动」+「查看进展」按钮。

- [ ] **Step 1: Write the failing content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_upgrade_streams_then_handoff(client):
    js = client.get("/assets/web.js").text
    body = js.split("async function submitUpgrade", 1)[1].split("\nasync function ", 1)[0]
    assert "pollTask(" in body                 # streams the apply steps
    assert "已提交升级" in body and "查看进展" in body   # honest handoff kept
    assert "openProgress(" in body             # progress modal still reachable
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_upgrade_streams_then_handoff -q`
Expected: FAIL — current 202 branch has no `pollTask`.

- [ ] **Step 3: Rewrite `submitUpgrade` 202 branch**

The current 202 branch (approx.):
```javascript
  if (status === 202) {
    resultEl.innerHTML = '<div class="conn ok">已提交升级 · operator 正在滚动</div>' +
      '<button class="btn btn-ghost btn-sm" id="up-prog">查看进展</button>';
    document.getElementById('up-prog').onclick = () => { closeModal(); openProgress(name); };
    renderMilvus();
    return;
  }
```
Replace it with a version that first streams the mb-side apply steps, then appends the handoff:
```javascript
  if (status === 202) {
    await pollTask(data.task_id, resultEl, () => {
      resultEl.innerHTML +=
        '<div class="conn ok" style="margin-top:8px">已提交升级 · operator 正在滚动</div>' +
        '<button class="btn btn-ghost btn-sm" id="up-prog" style="margin-top:6px">查看进展</button>';
      const b = document.getElementById('up-prog');
      if (b) b.onclick = () => { closeModal(); openProgress(name); };
      renderMilvus();
    });
    return;
  }
```
(The `status === 200` dry-run branch keeps calling `renderTaskResult(data.task)` = `logPanel`. Leave the 409/force branch unchanged.)

- [ ] **Step 4: Verify JS parses + test passes**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q`
Expected: JS OK; tests PASS.

- [ ] **Step 5: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.js tests/test_web_static.py
git commit -m "feat(webui): upgrade streams apply steps then hands off to 查看进展"
```

---

### Task 5: delete 接流式 + 「预演」（含后端 dry-run 分支）

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`（`api_delete` 加 dry-run 分支）
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`（`openDelete`）
- Test: `milvus-bootstrap/tests/test_web_task.py`, `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: `pollTask`, `logPanel`, `openModal`, `closeModal`, `postJSON`, `esc` (existing)；`_core().delete(instance, dry_run=True)` 返回 planned `Task`。
- Produces:
  - `POST /api/delete {instance, dry_run:true}` → 200 `{"task": <planned task dump>}`（镜像 `api_install`）。
  - `openDelete(name, onDone)` 弹窗两枚按钮「预演」「确认删除」；预演渲染 planned `logPanel`；确认走 202 → `pollTask` → onDone 追加「已提交删除 · operator 回收 · 🔄刷新列表」。

- [ ] **Step 1: Write the failing endpoint test**

Add to `milvus-bootstrap/tests/test_web_task.py`:
```python
def test_api_delete_dry_run_returns_planned_task():
    from milvus_bootstrap.server.app import _core
    # pick any managed instance if present; otherwise assert the 200/{task} shape on unknown is a ValueError->400
    insts = _core().state.list_instances()
    client = TestClient(app)
    if insts:
        name = insts[0].name
        r = client.post("/api/delete", json={"instance": name, "dry_run": True})
        assert r.status_code == 200
        assert "task" in r.json() and "steps" in r.json()["task"]
    else:
        r = client.post("/api/delete", json={"instance": "nope", "dry_run": True})
        assert r.status_code == 400
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_task.py::test_api_delete_dry_run_returns_planned_task -q`
Expected: FAIL — `api_delete` always submits async (returns 202), ignoring `dry_run`.

- [ ] **Step 3: Add dry-run branch to `api_delete`**

In `milvus-bootstrap/src/milvus_bootstrap/server/app.py`, update `api_delete` so a dry-run returns the planned task synchronously (mirror `api_install`):
```python
@app.post("/api/delete")
def api_delete(req: DeleteReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().delete(req.instance, dry_run=True)
        return {"task": task.model_dump(mode="json")}
    tid = runner.submit(lambda: _core().delete(req.instance, dry_run=False))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)
```

- [ ] **Step 4: Run endpoint test to verify pass**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_task.py -q`
Expected: PASS.

- [ ] **Step 5: Write the failing frontend content-marker test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_delete_has_dryrun_and_streams(client):
    js = client.get("/assets/web.js").text
    body = js.split("function openDelete", 1)[1].split("\nfunction ", 1)[0]
    assert "预演" in body                       # dry-run button
    assert "pollTask(" in body                  # confirm path streams steps
    assert "dry_run: true" in body              # dry-run request
    assert "刷新列表" in body                    # honest handoff kept
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_delete_has_dryrun_and_streams -q`
Expected: FAIL — current `openDelete` has no 预演 / pollTask.

- [ ] **Step 7: Rewrite `openDelete`**

Replace the entire existing `openDelete` function in `web.js` with:
```javascript
// Honest, transparent delete: 预演 shows planned steps; 确认删除 streams the mb-side
// steps, then hands off (card gone on refresh = deleted). No k8s polling.
function openDelete(name, onDone) {
  const m = openModal('删除 · ' + name,
    `<div>删除实例 <b>${esc(name)}</b>？<span class="muted">（依赖 / PVC 默认保留）</span></div>` +
    `<div style="margin-top:12px;display:flex;gap:8px">` +
    `<button class="btn btn-ghost btn-sm" id="del-dry">预演</button>` +
    `<button class="btn btn-primary btn-sm" id="del-go">确认删除</button></div>` +
    `<div id="del-result" style="margin-top:12px"></div>`);
  const res = m.body.querySelector('#del-result');

  m.body.querySelector('#del-dry').onclick = async () => {
    res.innerHTML = '<span class="muted">预演中…</span>';
    let resp;
    try { resp = await postJSON('api/delete', { instance: name, dry_run: true }); }
    catch (e) { res.innerHTML = '<span class="conn bad">预演失败：' + esc(e.message) + '</span>'; return; }
    if (resp.status === 200 && resp.data && resp.data.task) { res.innerHTML = logPanel(resp.data.task, false); return; }
    res.innerHTML = '<span class="conn bad">预演失败：' + esc((resp.data && resp.data.reason) || ('HTTP ' + resp.status)) + '</span>';
  };

  m.body.querySelector('#del-go').onclick = async () => {
    res.innerHTML = '<span class="muted">提交中…</span>';
    let resp;
    try { resp = await postJSON('api/delete', { instance: name, dry_run: false }); }
    catch (e) { res.innerHTML = '<span class="conn bad">提交失败：' + esc(e.message) + '</span>'; return; }
    const { status, data } = resp;
    if (status === 202) {
      await pollTask(data.task_id, res, () => {
        res.innerHTML +=
          '<div class="conn ok" style="margin-top:8px">已提交删除 · operator 正在回收</div>' +
          '<div class="muted" style="margin:6px 0 8px">刷新列表确认：卡片消失 = 删除成功；仍在 = 尚未完成或失败。</div>' +
          '<button class="btn btn-ghost btn-sm" id="del-refresh">🔄 刷新列表</button>';
        const b = document.getElementById('del-refresh');
        if (b) b.onclick = () => { closeModal(); onDone(); };
      });
      return;
    }
    res.innerHTML = '<span class="conn bad">失败（HTTP ' + status + '）：' + esc((data && data.reason) || '未知错误') + '</span>';
  };
}
```

- [ ] **Step 8: Verify JS parses + tests pass + full suite**

Run: `cd milvus-bootstrap && node --check src/milvus_bootstrap/webui/assets/web.js && source .venv/bin/activate && python -m pytest -q`
Expected: JS OK; all PASS.

- [ ] **Step 9: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/server/app.py src/milvus_bootstrap/webui/assets/web.js tests/test_web_task.py tests/test_web_static.py
git commit -m "feat(webui): delete streams steps + 预演 (dry-run); api_delete dry-run branch"
```

---

### Task 6: CSS 日志面板样式 + README「透明/可观测」原则

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Modify: `README.md`
- Test: `milvus-bootstrap/tests/test_web_static.py`

**Interfaces:**
- Consumes: CSS vars `--border`, `--surface-2`, `--surface-3`, `--muted`, `--accent-ink`, `--ok`, `--err`, `--warn`（existing design tokens）；classes emitted by `logPanel` (Task 3): `.logpanel .logrow .ic .logbody .logcmd .logdet .loghead`.

- [ ] **Step 1: Write the failing test**

Add to `milvus-bootstrap/tests/test_web_static.py`:
```python
def test_log_panel_css_and_readme(client):
    css = client.get("/assets/web.css").text
    assert ".logpanel" in css and ".logcmd" in css and ".logrow" in css
    import pathlib
    readme = pathlib.Path(__file__).resolve().parents[2] / "README.md"
    text = readme.read_text(encoding="utf-8")
    assert "透明" in text and "黑盒" in text
```
(Note: `parents[2]` = repo root from `milvus-bootstrap/tests/test_web_static.py`. If the test file lives elsewhere, adjust so it points at the repo-root `README.md`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py::test_log_panel_css_and_readme -q`
Expected: FAIL — CSS classes + README text absent.

- [ ] **Step 3: Add CSS**

Append to `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`:
```css
/* execution log panel (transparent step streaming) */
.loghead { font-size:13px; font-weight:600; margin-bottom:6px; }
.loghead.run { color:var(--warn); } .loghead.ok { color:var(--ok); } .loghead.bad { color:var(--err); }
.logpanel { max-height:260px; overflow:auto; border:1px solid var(--border); border-radius:8px; background:var(--surface-2); }
.logrow { display:flex; gap:8px; padding:7px 10px; border-bottom:1px solid var(--border); }
.logrow:last-child { border-bottom:none; }
.logrow .ic { flex:0 0 16px; text-align:center; }
.logrow.st-ok .ic { color:var(--ok); } .logrow.st-failed .ic { color:var(--err); }
.logrow.st-running .ic { color:var(--warn); } .logrow.st-skipped .ic, .logrow.st-planned .ic { color:var(--muted); }
.logbody { min-width:0; flex:1; }
.logcmd { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; color:var(--accent-ink);
  background:var(--surface-3); padding:2px 6px; border-radius:4px; margin-top:3px; white-space:pre-wrap; word-break:break-all; }
.logdet { font-size:12px; color:var(--muted); margin-top:3px; white-space:pre-wrap; word-break:break-all; }
```

- [ ] **Step 4: Add README principle**

In `README.md`, the section「设计原则（核心思想 · 轻量之本）」is a **dash bullet list** of `- **name（En）**：desc` items (简单 / 幂等 / 无状态 / 无轮询 / 用户驱动). Add a 6th **dash bullet** immediately after the 用户 / CLI 驱动 line (the last bullet, before the blank line and「一条推论」paragraph), matching the exact format:
```markdown
- **透明 / 可观测（Transparent，不是黑盒）**：mb 不是黑盒。每次 install / upgrade / delete / dry-run 都暴露**分步骤日志**——每一步做了什么、执行的**实际 k8s/helm 命令**、结果 / 错误都可查，便于核对步骤是否正确、准确。日志**来自 mb 自身的执行**、**按需、有界**（操作完即停），不是对集群的持续监控——与「无轮询」一致：唯一的短时轮询针对 mb 自己的内存任务记录，而非 k8s。
```
The exact anchor line to insert after is:
```
- **用户 / CLI 驱动（User-driven）**：动作只在用户发起时发生。mb **不跑自主后台 reconcile**——那是 operator 的职责。mb 只负责「把改动 apply 下去、交给 operator」，然后把控制权交还用户；用户想看进度时自己拉。
```

- [ ] **Step 5: Run test + full suite**

Run: `cd milvus-bootstrap && source .venv/bin/activate && python -m pytest tests/test_web_static.py -q && python -m pytest -q`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
cd milvus-bootstrap && git add src/milvus_bootstrap/webui/assets/web.css ../README.md tests/test_web_static.py
git commit -m "feat(webui): log panel CSS + README transparency principle (not a black box)"
```

---

## Notes for the executor
- 每个 Task 结束跑一次全量 `python -m pytest -q`，确保 162+ 基线不回归。
- 前端改动后务必 `node --check`（无 node 报错才算通过）。
- 手动 DoD（合并前一次真集群实测）：`mb web` 起在 8090（`MB_ADAPTER=k8s`），装一个 throwaway milvus：结果区应逐步刷出「渲染 manifests ▸helm…」「apply CR ▸…→created」「等待就绪 ⏳」，新在上；卡在 wait 时看得到停在哪步；完成 ✅。升级/删除同样流式；各自 dry-run/预演 只显示 planned 步骤 + 命令、不执行。
