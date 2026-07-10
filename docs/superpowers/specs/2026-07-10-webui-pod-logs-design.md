# WebUI Pod 容器日志（单次 tail=100 · 非流式）· 设计

- 日期：2026-07-10
- 状态：设计已确认（对话逐条敲定），待写实现计划
- 范围：Pods 弹窗每行加「日志」按钮，点一下**单次**拉该 pod 最后 100 条容器日志（`kubectl logs --tail=100`，**不 follow、不流式、无后端常驻任务**）。想再看就再点 🔄，仍是一次独立请求。严格符合 README 轻量准则（无持续网络服务 / 无后端轮询）。

## 1. 背景与准则

- 现状 Pods 弹窗（`web.js` `openPods` + `GET /api/pods`）已列出某 managed 实例的 pod 表（pod/状态/Ready/重启/龄）。本切面在该表每行加「日志」入口。
- **准则**（README「设计原则」+ [[feedback-mb-lightweight-principles]]）：mb 必须轻量——**无持续连接、无流式、无后端常驻/轮询任务**。故日志是**用户点击驱动的单次 `kubectl logs --tail=100`**，非 `-f`；重看=再点一次（又一次独立单请求）。与已落地的「透明 + 无轮询」一脉相承。
- `probe.run_kubectl(args) -> (rc, out, err)` 已有；kubectl 命令经它跑。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | 拉取方式 | **单次** `kubectl logs <pod> -n <ns> --tail=100 --all-containers=true --prefix=true`。**绝不 `-f`/follow、不流式、无后台任务** |
| D2 | 行数 | **硬编码 100**，不接收客户端参数（既轻量又无注入面；无下拉选择器）|
| D3 | 多容器 | `--all-containers --prefix` → 每行前缀 `[pod/container]`，省去容器选择器 |
| D4 | 重看 | 弹窗内一枚 🔄 刷新 = 再发一次单次请求（client 驱动、非定时）|
| D5 | 入口 | Pods 弹窗表格每行「日志」按钮（`data-log-pod`/`data-log-ns`）；点击 `openLogs(pod, ns)` |
| D6 | 权限/边界 | 仅 managed 实例的 pod（Pods 弹窗本就 managed-only 才可开）；非 k8s adapter / kubectl 失败 → `logs=""` 或 stderr 文本，前端占位不崩 |
| D7 | 非目标 | 不做流式/follow、不做行数选择、不做容器下拉、不做日志搜索/下载（仅复制）；不改 `openPods` 的 pod 表结构（只加一列按钮）|

## 3. 架构与模块

| 模块 | 变更 |
|---|---|
| `core/probe.py` | 新 `pod_logs(pod, namespace, run=run_kubectl) -> str` |
| `server/app.py` | 新 `GET /api/logs`（注册在 `app.mount("/")` 之前）|
| `webui/assets/web.js` | `openPods` 表格加「日志」列 + wiring；新 `openLogs(pod, ns)` |
| `webui/assets/web.css` | `.logview`（定高滚动 pre）|

## 4. 后端

### 4.1 `probe.pod_logs`
```python
def pod_logs(pod: str, namespace: str, run=run_kubectl) -> str:
    """Last 100 lines of a pod's container logs (all containers, prefixed). One-shot, best-effort."""
    rc, out, err = run(["logs", pod, "-n", namespace, "--tail=100",
                        "--all-containers=true", "--prefix=true"])
    if rc != 0:
        return err.strip() or "（无日志或读取失败）"
    return out
```

### 4.2 `GET /api/logs`（注册在 `app.mount` 之前）
```python
@app.get("/api/logs")
def api_logs(pod: str, namespace: str = "default") -> dict[str, Any]:
    core = _core()
    logs = ""
    if getattr(core.adapter, "name", "") == "k8s":
        try:
            logs = probe.pod_logs(pod, namespace)
        except Exception:  # noqa: BLE001
            logs = "（读取失败）"
    else:
        logs = "（非 k8s 环境，无 pod 日志）"
    return {"pod": pod, "namespace": namespace, "logs": logs}
```
（`probe` 已在 app.py import。`pod`/`namespace` 是查询参数；只读、幂等。）

## 5. 前端

### 5.1 `openPods` 表格加「日志」列
表头加 `<th>日志</th>`；每行末加 `<td><button class="btn btn-ghost btn-sm" data-log-pod="${esc(p.pod)}" data-log-ns="${esc(d.namespace)}">日志</button></td>`。渲染后 wiring：
```javascript
el.querySelectorAll('[data-log-pod]').forEach(b => {
  b.onclick = () => openLogs(b.getAttribute('data-log-pod'), b.getAttribute('data-log-ns'));
});
```

### 5.2 `openLogs(pod, ns)`
```
openModal('日志 · '+pod, '<div style="margin-bottom:8px;display:flex;gap:8px">'
  + '<button id="log-refresh" class="btn btn-ghost btn-sm">🔄 刷新</button>'
  + '<button id="log-copy" class="btn btn-ghost btn-sm">复制</button>'
  + '<span class="muted" style="align-self:center;font-size:12px">最后 100 条 · 单次读取</span></div>'
  + '<pre id="log-view" class="logview">读取中…</pre>')
```
- `load()`：`getJSON('api/logs?pod='+enc(pod)+'&namespace='+enc(ns))` → `#log-view.textContent = data.logs || '（无日志）'`（**用 textContent 不用 innerHTML**，天然安全 + 保留原文；失败 → `textContent = '读取失败：'+e.message`）。
- `#log-refresh` onclick = `load`（再发一次单次请求，**无定时器**）。
- `#log-copy` onclick = `navigator.clipboard.writeText(#log-view.textContent)`（best-effort）。
- 开弹窗即 `load()` 一次。

### 5.3 CSS
`.logview{max-height:420px;overflow:auto;background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin:0;font-size:12px;line-height:1.5;white-space:pre-wrap;word-break:break-all}`

## 6. 测试与验收
- **`probe.pod_logs`**（`tests/test_probe.py` 追加）：fake `run` → rc0 返回 out（断言命令 args 含 `--tail=100` 与 `--all-containers=true`）；rc≠0 返回 stderr 文本（非空）。
- **端点**（`tests/`）：`GET /api/logs?pod=x&namespace=default` fake adapter → 200 `{pod,namespace,logs}`，logs 为「非 k8s 环境…」提示串。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js 含 `function openLogs`/`api/logs`/`data-log-pod`/`log-view`/`最后 100 条`；**断言 openLogs 段无 `setInterval`、无 `-f`/follow 相关**（`"setInterval" not in js` 已有全局断言可复用）；`openPods` 段含 `data-log-pod`。
- **JS**：`node --check`。
- **手动 DoD**（真集群）：某 managed milvus 卡「Pods」→ 表格每行有「日志」→ 点开某 pod 见最后 100 条容器日志（多容器带前缀）；点 🔄 再取一次；复制可用；非 k8s/断连显示占位不崩。

## 7. 非目标 / 后续
- 流式 / follow / 实时 tail（**永不做**——违背轻量准则）。
- 行数选择、容器下拉、日志搜索、下载文件（YAGNI）。
- 依赖(etcd/minio/…)的 pod 日志（本切面只在 milvus Pods 弹窗；后续同法可接 Dependencies）。
