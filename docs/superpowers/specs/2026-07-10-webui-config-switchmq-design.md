# WebUI 配置(get/set) + 切换 MQ · 设计

- 日期：2026-07-10
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：接通 Milvus 卡片仅剩的两个灰占位 `ph('配置')` / `ph('切换 MQ')`——**配置查看/修改** + **切换消息队列(MQ)**，两者全面复用已建成的流式日志基建（modal + `logPanel` + `pollTask` + 预演/dry-run + 门禁 409/force）。仅 **managed milvus** 可用。

## 1. 背景（已核实的复用点）

- 卡片动作行（`web.js` renderMilvus，约 line 436）：`${upgradeButton(i)}${ph('配置')}${podsButton(i)}${ph('切换 MQ')}${delButton(i)}`——`ph(...)` 是灰占位；本切面把这两个换成真按钮（`data-config` / `data-switch`），external 实例保持灰占位（和升级/删除一致）。
- 后端逻辑已存在，但只有 **sync CLI 路由**（保留不动）：
  - `config_get(instance)` → `adapter.get_configmap(...)`（返回 configmap dict：`{yaml-filename: yaml-content}`；读不到抛 RuntimeError）。`POST /config/get`。
  - `config_set(instance, kv, dry_run)` → `Task`。`kv` 是**点状键=值**（如 `proxy.maxNameLength=255`）；`driver.config_apply_params` 把 kv 并进 `params["_conf"]`，`build_install_manifests` 经 `_dotted_to_nested` 落到 **`spec.config`（nested，operator 认）**——**不是**死字段 `spec.conf`（已核实 milvus.py:158-162）。**无兼容门禁**。`POST /config/set`。
  - `switch_mq(instance, target_wal, dry_run, force)` → `Task`；内部**先 `compat.gate("switch-mq", …, force)`（不兼容→CompatError）再建步骤**（无论 dry_run）。步骤含真实动作：`wal-alter` 在 pod 内 `adapter.exec` 一条 curl；另有 precheck/verify/decommission-old。`POST /switch-mq`。
  - `mq_options(milvus_version, mode)` → `[{id, wal, label, dep_kind, …}]`（带兼容状态）。`POST /mq-options`。
- WebUI 已有：`logPanel(task,running)`、`pollTask(taskId,el,onDone)`（有界·只轮 mb 内存 `GET /api/task`·完成即停）、`openModal/closeModal`、`postJSON/getJSON/esc`、`submitUpgrade` 的**门禁 409→force→confirm** 范式、`renderTaskResult=logPanel`。全局 `CompatError→409 {reason,force_hint}`、`ValueError→400` 异常处理器已存在。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 配置 UI 形态 | 弹窗：**上半只读折叠**当前生效配置（config_get 的 configmap，`<details>` **默认折叠**）；**下半 点状键=值 覆盖编辑器**（预填快照里已有的 `_conf`，可增/删/改行）。[预演][应用] |
| D2 | 配置写路径 | 复用 `config_set`（→`spec.config` nested，operator 认）。**无门禁**。apply 流式，完成后「已提交配置变更 · operator 滚动重启相关 pod · 🔄刷新」 |
| D3 | 切 MQ UI | 弹窗：显示**当前 MQ** + 目标**下拉**（`mq_options`，带兼容状态，不兼容项标注/禁选）。[预演][切换] |
| D4 | 切 MQ 二次确认 | **[切换] 必先弹二次确认**（`confirm`：「确认切换 MQ 到 X？这会更改消息队列/WAL 并在 pod 内执行变更，可能影响写入。」）——**独立于**门禁 force；确认后才 apply |
| D5 | 切 MQ 门禁 | 镜像 `/api/upgrade`：apply 前 sync 门禁预检（`switch_mq(dry_run=True)`→CompatError→409 `{reason,force_hint}`）；409 时提示 + [强制切换] 再 `confirm` → force。完成后「已提交 MQ 切换 · operator 处理中 · 🔄刷新」 |
| D6 | 流式 & 无轮询 | 两者 apply 都 202 → `pollTask` 流式（复用）。唯一轮询是 `GET /api/task`（mb 内存），有界、完成即停。不碰 k8s 轮询、不改 wait 逻辑 |
| D7 | 权限 | 仅 managed milvus 出真按钮；external 保持灰占位 `ph(...)`（复用 `i.ownership==='managed'` 判断，同 upgradeButton/podsButton） |
| D8 | 非目标 | 不做配置项校验/schema 提示（自由点状键）；不做 MQ 切换的数据迁移编排（沿用 mb 现有 switch_mq 步骤）；不改 sync CLI 路由 |

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `server/app.py` | 加 `GET /api/config`、`POST /api/config/set`、`GET /api/mq-options`、`POST /api/switch-mq`（async/流式变体；req 模型 `dry_run` 默认 True）|
| `webui/assets/web.js` | 新 `openConfig(name)`、`openSwitchMq(name)`、`collectKv()`（点状键=值收集器）、`submitSwitchMq(name,targetWal,dryRun,force,el)`（门禁/force，仿 submitUpgrade）；卡片 `ph('配置')/ph('切换 MQ')`→真按钮 + wiring |
| `webui/assets/web.css` | 配置只读视图 `.cfg-view`（折叠 yaml）；MQ 选项行样式（复用现有 .f-*/.prow/.badge）|

**边界**：全 best-effort——config_get 读不到→UI 显示「无法读取当前配置（可能尚未生成）」占位、覆盖编辑器仍可用；mq_options 空/连不上→提示不崩；纯读端点幂等。

## 4. 后端

### 4.1 `GET /api/config?instance=`
```python
@app.get("/api/config")
def api_config(instance: str) -> dict:
    inst = _core().state.get_instance(instance)      # None -> ValueError -> 400
    if inst is None: raise ValueError(f"未找到实例：{instance}")
    snap = inst.spec_snapshot or {}
    overrides = (snap.get("params", {}) or {}).get("_conf", {}) or {}
    try:
        current = _core().config_get(instance)        # configmap dict
    except Exception as e:                             # 读不到（CM 未生成等）
        current = None
    return {"instance": instance, "current": current, "overrides": overrides}
```

### 4.2 `POST /api/config/set`（无门禁，dry-run 200 / apply 202）
```python
class ConfigSetApiReq(BaseModel):
    instance: str
    kv: dict = {}
    dry_run: bool = True

@app.post("/api/config/set")
def api_config_set(req: ConfigSetApiReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().config_set(req.instance, req.kv, dry_run=True)
        return {"task": task.model_dump(mode="json")}
    tid = runner.submit(lambda: _core().config_set(req.instance, req.kv, dry_run=False))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)
```

### 4.3 `GET /api/mq-options?instance=`
```python
@app.get("/api/mq-options")
def api_mq_options(instance: str) -> dict:
    inst = _core().state.get_instance(instance)
    if inst is None: raise ValueError(f"未找到实例：{instance}")
    snap = inst.spec_snapshot or {}
    params = snap.get("params", {}) or {}
    from ..core import probe
    version = probe._tag(params.get("image", "")) or ""   # milvus 版本 tag
    mode = params.get("mode", "standalone")
    cur_mq = params.get("mq", "")
    from ..core import compat
    cur_opt = compat.get_option(cur_mq)
    current_wal = cur_opt.wal if cur_opt else cur_mq
    return {"instance": instance, "current_mq": cur_mq, "current_wal": current_wal,
            "options": _core().mq_options(version, mode)}
```

### 4.4 `POST /api/switch-mq`（镜像 `/api/upgrade`：dry-run 200 / 门禁 409 / apply 202）
```python
class SwitchMqApiReq(BaseModel):
    instance: str
    target_wal: str
    dry_run: bool = True
    force: bool = False

@app.post("/api/switch-mq")
def api_switch_mq(req: SwitchMqApiReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().switch_mq(req.instance, req.target_wal, dry_run=True, force=req.force)
        return {"task": task.model_dump(mode="json")}
    _core().switch_mq(req.instance, req.target_wal, dry_run=True, force=req.force)  # sync 门禁预检 → CompatError→409
    tid = runner.submit(lambda: _core().switch_mq(req.instance, req.target_wal, dry_run=False, force=req.force))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)
```
（`CompatError`/`ValueError` 由既有全局处理器转 409/400；`switch_mq` 内 `KeyError`（无实例）已被上面的 get_instance 前置成 ValueError→400，避免 500。）

## 5. 前端

### 5.1 卡片按钮（复用 managed 判断）
新增 `configButton(i)` / `switchMqButton(i)`：`i.ownership==='managed'` → 真按钮（`data-config`/`data-switch`），否则 `ph('配置')`/`ph('切换 MQ')`。renderMilvus 动作行改为
`${upgradeButton(i)}${configButton(i)}${podsButton(i)}${switchMqButton(i)}${delButton(i)}`，并加两处 wiring：
`[data-config]`→`openConfig(name)`；`[data-switch]`→`openSwitchMq(name)`。

### 5.2 `openConfig(name)`
```
openModal('配置 · '+name, body); GET /api/config?instance= →
  上：<details class="cfg-view"><summary>当前生效配置（只读）</summary><pre>…yaml…</pre></details>   // 默认折叠（无 open）
  下：覆盖编辑器：#cfg-rows（预填 overrides 的每个 dotted key=value 一行）+ [+ 添加]
  按钮：[预演] [应用] + #cfg-result
```
- `collectKv()`：读 `#cfg-rows` 每行 `.ck`(key)/.cv(value)，非空 key → 组 `{key:value}` 返回。
- 预演：`postJSON('api/config/set',{instance,kv:collectKv(),dry_run:true})`→200→`#cfg-result=logPanel(data.task,false)`。
- 应用：`postJSON(... dry_run:false)`→202→`pollTask(tid,#cfg-result,onDone)`；onDone 追加「已提交配置变更 · operator 滚动重启相关 pod」+`🔄刷新`（`closeModal(); renderMilvus()`）。current 为 null 时上半显示「无法读取当前配置（可能尚未生成）」。

### 5.3 `openSwitchMq(name)`
```
openModal('切换 MQ · '+name, body); GET /api/mq-options?instance= →
  显示：当前 MQ = current_mq（wal=current_wal）
  <select id="mq-target">：options.map(o => <option value=o.wal [disabled if !compatible]>o.label (o.dep_kind) [不兼容?]</option>)（排除等于 current_wal 的项或标注「当前」）
  按钮：[预演] [切换] + #mq-result
```
- **预演** 与 **切换** 都走同一个 `submitSwitchMq`（统一处理 200/202/409/force——因为 `switch_mq(dry_run=True)` 内部也跑门禁，不兼容目标的**预演也会 409**，须一致处理）：
  - [预演] → `submitSwitchMq(name, target, /*dryRun*/true, /*force*/false, #mq-result)`。
  - [切换] → **先 `confirm('确认切换 MQ 到 '+target+'？这会更改消息队列/WAL 并在 pod 内执行变更，可能影响写入。')`**（D4 二次确认）→ 确认后 `submitSwitchMq(name, target, /*dryRun*/false, /*force*/false, #mq-result)`。
- `submitSwitchMq(name,targetWal,dryRun,force,el)`（仿 `submitUpgrade`）：`postJSON('api/switch-mq',{instance:name,target_wal:targetWal,dry_run:dryRun,force})` →
  - **200**（dry-run 计划）→ `el.innerHTML = logPanel(data.task,false)`；
  - **202**（真切换）→ `pollTask(data.task_id, el, onDone)`，onDone 追加「已提交 MQ 切换 · operator 处理中」+`🔄刷新`（`closeModal(); renderMilvus()`）；
  - **409**（门禁）→ `el` 显示「被兼容门禁拦截：reason」+[强制] 按钮，点击 `if(confirm('确认跳过兼容门禁强制切换 MQ？')) submitSwitchMq(name,targetWal,dryRun,true,el)`（**保持同一 dryRun**——强制预演只看计划、强制切换才执行）；
  - 其他 → 错误行。

### 5.4 CSS
`.cfg-view{border:1px solid var(--border);border-radius:8px;background:var(--surface-2);margin-bottom:10px} .cfg-view>summary{cursor:pointer;padding:8px 10px;font-weight:600} .cfg-view pre{max-height:220px;overflow:auto;margin:0;padding:8px 10px;font-size:12px;white-space:pre-wrap;word-break:break-all}`；MQ 行复用 `.f-in/.f-row/.badge`。

## 6. 测试与验收
- **端点**（`tests/`，fake）：`GET /api/config` 返回 `{current, overrides}`（装一个 milvus 后 overrides 反映 `_conf`；current 在 fake 下可能 None——断言键存在）；`POST /api/config/set` dry-run→200 `{task}` 含步骤、apply→202 `{task_id}`；`GET /api/mq-options` 返回 `{current_mq,current_wal,options:[…]}`；`POST /api/switch-mq` dry-run→200、不兼容目标→409（构造触发门禁的 target）、force→202；未知实例→400。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js 含 `function openConfig`/`function openSwitchMq`/`function submitSwitchMq`/`function collectKv`/`data-config`/`data-switch`/`确认切换 MQ`（二次确认）/`cfg-view`；css 含 `.cfg-view`。
- **JS**：`node --check`。
- **手动 DoD**（真集群）：某 managed milvus 卡「配置」→ 折叠可展开看当前 configmap；加一行 `proxy.maxNameLength=255`→[预演]看计划步骤+命令→[应用]看流式日志→完成提示刷新。「切换 MQ」→ 选目标→[预演]看步骤；[切换]先弹二次确认→（若门禁不兼容→409+强制）→流式→完成提示。external 实例这俩仍是灰占位。

## 7. 非目标 / 后续
- 配置项 schema 校验 / 下拉建议（本切面自由点状键）。
- MQ 切换的数据迁移/排空编排（沿用现有步骤）。
- scale / adopt 的 UI（同基建，后续可接）。
