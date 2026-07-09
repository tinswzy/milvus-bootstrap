# WebUI 执行步骤流式日志（透明化 · 非黑盒）· 设计

- 日期：2026-07-09
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：把 install / upgrade / delete / dry-run 的**执行过程**从「一个倒计时黑盒」变成**逐步骤流式日志面板**——新在上、旧在下滚动，每步显示步骤名 + **实际执行的 k8s/helm 命令** + 结果/错误，让用户能核对"执行到哪一步、命令对不对、结果准不准"。并把「透明/可观测」写成 mb 的一条正式设计原则（README）。

## 1. 背景与目标

现状：`submitInstall`/`pollInstall` 在任务执行期间只显示 `处理中… Ns` 倒计时；步骤表 `renderTaskResult` 只在任务**结束后**一次性出现。upgrade / delete 更是提交后直接「已提交」，中间做了哪几步、跑了什么命令，用户完全看不到 → **黑盒**。

目标（用户原话提炼）：这个工具**不能是黑盒**，要尽可能透明、步骤明确；执行任何操作时都有对应日志可查，方便核对执行步骤是否正确、准确。

**已核实的现状（复用点）**：
- `core/models.py` `Task.steps: list[StepResult]`，每步 `name` / `status`(planned/running/ok/failed/skipped) / `plan`（"将做什么"，helm 步骤里**已含字面命令** `将执行：helm upgrade --install …`）/ `detail`（执行后的输出/错误）。
- `core/tasks/engine.py` `TaskEngine.run`：dry-run 收集每步 `plan`（status=planned）；实跑时**逐步** append `StepResult`(running) → 执行 `action()` → 翻 ok/failed。数据本就是增量产生的，只是没暴露。
- `core/taskrunner.py` `TaskRunner.submit(fn)`：worker 线程跑 `fn()`，只在**结束**时把返回的 `Task` 存进 `rec["result"]`；运行中 `result=None`。
- `server/app.py` `GET /api/task/{id}`：running 时返回 `{state:"running", task:None}`（**无部分步骤**）；结束返回完整 `task`。
- `webui/assets/web.js`：`renderTaskResult(task)`（静态步骤表）、`pollInstall(taskId,el)`（每 1500ms 轮询，running 只显示倒计时）；`submitInstall` 用 pollInstall；`submitUpgrade` 202→「已提交升级·查看进展」（无轮询）；`openDelete` 202→「已提交删除·刷新列表」（无轮询）。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 日志数据源 | **复用 `Task.steps`**，不新造日志格式。每步：图标(status) · 步骤名 · **命令行(mono，取 `plan`)** · 结果/错误(取 `detail`)。helm 步骤 `plan` 已含字面命令；k8s-API 步骤 `plan` 为该动作描述（不伪造 shell 命令）|
| D2 | 流式机制 | engine 每完成/翻转一步就把当前 `task` 发布进 mb **内存任务记录**；`GET /api/task` running 时返回**部分 task**（含已到步骤）。前端轮询 **mb 内存**（非 k8s）实时重渲染 |
| D3 | 轮询边界（调和无轮询准则）| 唯一轮询是**短时、有界、针对 mb 自己的内存任务记录**（非 k8s），用户主动触发、**任务一完即停**。升级滚动 / 删除 GC 这类**不定长的算子侧收敛**仍是**按需**（查看进展 / 刷新列表）。即"展示 mb 自己做了哪几步"属透明化，非对集群的持续监控 |
| D4 | 面板样式 | 定高 ~260px 滚动容器；**新在上、旧在下**；顶部抬头 `执行中… Ns` / `✅ 完成` / `❌ 出错`；命令行 mono、带轻量前缀标记（`▸`）。取代纯倒计时 |
| D5 | 覆盖范围 | install / upgrade / delete / dry-run **全部**。dry-run(200) 直接渲染 planned 步骤（无轮询）；执行(202) 走 `pollTask` 流式 |
| D6 | delete 预演 | 删除弹窗加「预演(dry-run)」按钮 → 渲染 planned 步骤日志（不执行）；「确认删除」保持真删 |
| D7 | 操作后收口不变 | upgrade 完成后仍「已提交升级 · operator 滚动 · 查看进展」；delete 完成后「已提交删除 · operator 回收 · 🔄刷新列表」（流式只覆盖 mb 侧那几步）|
| D8 | README 原则 | 新增第 6 条原则**透明 / 可观测（Transparent / Observable）**：mb 不是黑盒，每次操作都暴露分步骤日志（步骤 + 实际 k8s/helm 命令 + 结果），可核对每步是否正确；日志按需、有界、来自 mb 自身执行 |
| D9 | 非目标 | 不做 pod 容器日志 tail（另切面）；不改 `wait_cr`/`wait_ready` 逻辑；不引入 SSE/WebSocket（保持简单）|

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/taskrunner.py` | 加 `ContextVar` 进度 sink + `submit()` worker 发布"部分 task"进 rec；`get()` 返回含部分 task |
| `core/tasks/engine.py` | `run()` 每步 append / 翻状态后调 sink 发布当前 `task`（~3 处）|
| `server/app.py` `GET /api/task` | running 时返回**部分 task**（有则含 steps），无部分时回退现状 |
| `webui/assets/web.js` | 新 `logPanel(task, running)`；`pollInstall`→泛化 `pollTask(taskId, el, onDone)`；install/upgrade/delete/dry-run 接上；delete 弹窗加「预演」|
| `webui/assets/web.css` | `.logpanel`（定高滚动）/ `.logrow` / `.logcmd`(mono) / 状态图标样式 |
| `README.md` | 「设计原则」加第 6 条 透明/可观测 |

**边界**：全 best-effort——fake/无步骤/连不上时面板显示占位不崩；纯读 `GET /api/task`、幂等；不改算子侧 wait 逻辑。

## 4. 后端

### 4.1 `taskrunner.py` — 流式发布（ContextVar sink）
```python
import contextvars
_PROGRESS: contextvars.ContextVar = contextvars.ContextVar("mb_task_progress", default=None)

def publish(task) -> None:
    """Engine calls this after each step; no-op outside a submit() worker."""
    sink = _PROGRESS.get()
    if sink is not None:
        sink(task)
```
`submit(fn)`：worker `_run` 内，跑 `fn()` 前 `_PROGRESS.set(lambda t: self._publish(tid, t))`；`_publish(tid, task)` 在锁内写 `self._recs[tid]["partial"] = task.model_dump(mode="json")`（不改 state；state 仍由 fn 结束翻 done/error）。`get()` 原样返回 rec（含 `partial`）。
- 说明：`fn()`→provisioner/lifecycle→`engine.run` 全在**同一 worker 线程**同步执行，ContextVar 在该线程内可见；engine 通过 `taskrunner.publish(task)` 发布，无需改任何中间层签名。

### 4.2 `engine.py` — 发布调用
`run()` 内三处后各加 `taskrunner.publish(task)`：①每次 `task.steps.append(res)`（running 态）后；②翻 `ok`/`skipped` 后；③翻 `failed` 后。dry-run 分支结束也 publish 一次（可选，dry-run 是同步返回，前端直接渲染，不依赖它）。
- 避免循环 import：engine 里 `from .. import taskrunner`（taskrunner 不 import engine）——或把 `publish` 放独立小模块 `core/progress.py`，engine 与 taskrunner 都依赖它（**采用 `core/progress.py`**，零循环风险）。

### 4.3 `GET /api/task` — running 返回部分步骤
```python
rec = runner.get(task_id)                     # {state, result, error, partial?}
if rec is None: 404
if rec["state"] == "running":
    return {"state": "running", "task": rec.get("partial"), "error": None}   # 部分 task 或 None
if rec["state"] == "error":
    return {"state": "error", "task": rec.get("partial"), "error": rec["error"]}  # 附最后已知步骤
dump = rec["result"].model_dump(mode="json")
return {"state": dump["status"], "task": dump, "error": None}
```

## 5. 前端

### 5.1 `logPanel(task, running)` — 滚动日志面板
```javascript
const STEP_ICON = { ok:'✓', failed:'✗', skipped:'⤼', running:'⏳', planned:'○', pending:'·' };
function logPanel(task, running) {
  const head = running
    ? `<div class="loghead run">⏳ 执行中…</div>`
    : (task && task.status === 'succeeded' ? '<div class="loghead ok">✅ 完成</div>'
       : '<div class="loghead bad">❌ 出错</div>');
  const steps = (task && task.steps) ? task.steps.slice().reverse() : [];   // 新在上
  const rows = steps.map(s => {
    const ic = STEP_ICON[s.status] || '·';
    const cmd = s.plan ? `<div class="logcmd">▸ ${esc(s.plan)}</div>` : '';
    const det = s.detail ? `<div class="logdet">${esc(s.detail)}</div>` : '';
    return `<div class="logrow st-${esc(s.status)}"><span class="ic">${ic}</span>` +
           `<div class="logbody"><b>${esc(s.name)}</b>${cmd}${det}</div></div>`;
  }).join('') || '<div class="muted" style="padding:8px">暂无步骤…</div>';
  return head + `<div class="logpanel">${rows}</div>`;
}
```

### 5.2 `pollTask(taskId, el, onDone)` — 泛化轮询（有界，只轮 mb 内存）
```javascript
async function pollTask(taskId, el, onDone) {
  while (true) {
    let j;
    try { j = await getJSON('api/task/' + taskId); }
    catch (e) { el.innerHTML = '<span class="conn bad">轮询失败：' + esc(e.message) + '</span>'; return; }
    if (j.state === 'running') { el.innerHTML = logPanel(j.task, true); await new Promise(r => setTimeout(r, 800)); continue; }
    if (j.state === 'error')   { el.innerHTML = logPanel(j.task, false) + '<div class="conn bad" style="margin-top:8px">执行出错：' + esc(j.error) + '</div>'; return; }
    el.innerHTML = logPanel(j.task, false);
    if (onDone) onDone(j.task);
    return;
  }
}
```
`pollInstall` 删除，`submitInstall` 202 分支改调 `pollTask(data.task_id, resultEl)`；dry-run(200) 改 `resultEl.innerHTML = logPanel(data.task, false)`（替换 `renderTaskResult`，或让 `renderTaskResult` 内部调 `logPanel`——**采用后者**：`renderTaskResult(task){ return logPanel(task,false); }`，保留旧调用点）。

### 5.3 upgrade 接上
`submitUpgrade` 202 分支：先 `pollTask(data.task_id, resultEl, () => { resultEl.innerHTML += 已提交升级·operator 滚动·[查看进展] })`——即流完 apply 步骤后，`onDone` 追加「已提交升级 · operator 正在滚动　[查看进展]」按钮（点击 `closeModal(); openProgress(name)`）+ `renderMilvus()`。dry-run(200) → `logPanel(data.task,false)`。

### 5.4 delete 接上（+ 预演）
`openDelete` 弹窗按钮改为两枚：**「预演」**（dry-run）与**「确认删除」**。
- 预演 → POST `api/delete {instance, dry_run:true}` → 200 → `logPanel(data.task,false)`（planned 步骤，不执行）。
- 确认删除 → POST `api/delete {instance}`（异步 202）→ `pollTask(tid, res, () => res.innerHTML += 已提交删除·operator 回收·🔄刷新列表)`；刷新按钮 `closeModal(); onDone()`。
- 注：需确认 `POST /api/delete` 异步分支存在（现 `openDelete` 已用 202）；dry-run 走同步 `_core().delete(dry_run=True)`（`DeleteReq.dry_run` 已有）。

### 5.5 CSS
`.logpanel{max-height:260px;overflow:auto;border:1px solid var(--border);border-radius:8px;background:var(--surface-2)}`；`.logrow{display:flex;gap:8px;padding:7px 10px;border-bottom:1px solid var(--border)}`（新在上，首行即最新）；`.logrow .ic` 按状态着色（ok 绿/failed 红/running 黄/planned 灰）；`.logcmd{font-family:mono;font-size:12px;color:var(--accent-ink);background:var(--surface-3);padding:2px 6px;border-radius:4px;margin-top:3px;white-space:pre-wrap;word-break:break-all}`；`.logdet{font-size:12px;color:var(--muted);margin-top:3px}`；`.loghead`（run 黄 / ok 绿 / bad 红）。

## 6. README 原则（第 6 条）
在「设计原则（核心思想 · 轻量之本）」列表加：
> **6. 透明 / 可观测（Transparent, not a black box）**：mb 不是黑盒。每次 install/upgrade/delete/dry-run 都暴露**分步骤日志**——每一步做了什么、执行的**实际 k8s/helm 命令**、结果/错误都可查，便于核对步骤是否正确、准确。日志**来自 mb 自身的执行**、**按需、有界**（操作完即停），不是对集群的持续监控——与「无轮询」一致：唯一的短时轮询针对 mb 自己的内存任务记录，而非 k8s。

## 7. 测试与验收
- **taskrunner/engine**（`tests/`）：跑一个多步 fake task，注册一个 sink（经 `core/progress.py`），断言 sink 在**中间**被调到（收到步数递增的部分 task），且最终态正确。
- **端点**（`tests/`，fake）：`GET /api/task` 对运行中任务返回 `task` 含部分 `steps`（构造一个卡住的 task 或直接测 rec 结构）；结束返回完整 task。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js 含 `function logPanel`/`function pollTask`；含 `.slice().reverse()`（新在上）/`logcmd`/`预演`；`pollInstall` 已移除；css 含 `.logpanel`/`.logcmd`。
- **JS**：`node --check`。
- **README**：含「透明 / 可观测」与「不是黑盒」。
- **手动 DoD**：真集群装一个 milvus——结果区出现滚动日志，逐步刷出「渲染 manifests ▸helm…」「apply CR ▸…→created」「等待就绪 ⏳」，新在上；卡在 wait 时看得到停在哪步；完成显示 ✅。升级/删除同样流式；各自 dry-run/预演 显示 planned 步骤+命令不执行。

## 8. 非目标 / 后续
- pod 容器日志 tail（`GET /api/logs`）另切面。
- scale / switch-mq / adopt 复用 `logPanel`+`pollTask`（同 runner/engine，天然生效，后续接 UI）。
- 不改 `wait_cr`/`wait_ready`；不引入 SSE/WebSocket。
