# WebUI 实例：managed/external 归属 + 每实例 image/sha · 设计

- 日期：2026-07-07
- 状态：设计已确认，待写实现计划
- 范围：给实例页加「managed / external」归属标签（external 禁删改升）；探测版本改为**跟着每个实例走**（去掉 Overview 单独版本表）；每实例显示 image tag，hover 显示完整 image 路径 + sha256 image id。

## 1. 背景与目标

实例页目前只列 `state.list_instances()`（全是 mb 装的 managed 实例），且版本是 Overview 上一张按-kind 探测表。用户要：
1. mb 装的标 **managed**；用户自己在集群里装的（mb 未装）标 **external**，且 external **不可删除/更新/升级**。
2. 探测版本**跟着实例走**，不要单独版本表——在每个实例处显示它的 **image 版本**。
3. hover 实例的 image 时，显示**完整 image 路径 + sha256 image id**（imageID digest），消除 `latest` 之类同 tag 不同版本的歧义。

现状（已核实）：
- `Ownership` 枚举：`managed`(我们装的) / `adoptable`(别处装、可接管) / `readonly`(只观察，如控制面) / `external`(off-cluster endpoint)。
- `Instance`：`id,name,platform,namespace,ownership,deps,spec_snapshot`（kind 在 `spec_snapshot["kind"]`，milvus image 在 `params.image`）。
- `DiscoveryEngine.discover() -> list[Candidate]`：`adapter.discover_native()`(列全集群 StatefulSet/Deployment/独立 Pod) → `registry.find_for`→`driver.identify`→`Candidate{kind,name,ownership(默认 adoptable),excluded,reason,evidence}`。`evidence` 含 `image`(=podspec 容器镜像 tag，空格连接)、`name`、`namespace`、`labels`。控制面（kube-system etcd 等）被 identify 标 `excluded`/`readonly`。
- `evidence.image` 是 **tag ref**（如 `milvusdb/etcd:3.5.18-r1`），**无 sha256 digest**（podspec 没有 digest）。sha256 在**运行 pod** 的 `.status.containerStatuses[].imageID`。
- `probe.run_kubectl` 已有；`probe.milvus_status` 已有（best-effort CR 查询范式）。
- 现 `/api/instances` 只读 state（无 discovery）；Overview 有「探测到的版本」card（来自 `/api/doctor` probe）；deps 手风琴表头用 `/api/doctor` versions[kind] 做 image chip。

## 2. 关键设计决策（已确认）

| # | 决策 | 取值 |
|---|---|---|
| D1 | external 来源 | 接 `DiscoveryEngine` 扫全集群，与 state 的 managed 按 `(kind,name,ns)` 去重合并 |
| D2 | 归属判定 | 命中 state→`managed`；discovery 识别为我们 5 类、未 excluded/readonly、且不在 state→`external`；excluded/控制面**不显示** |
| D3 | external 操作 | UI 禁用删除按钮（title 说明）；升级/配置/Pods 占位对 external 一样灰。managed 才可删 |
| D4 | 版本呈现 | 去掉 Overview「探测到的版本」表；每实例处显 image tag；UI 不再用 `/api/doctor` versions |
| D5 | image + sha | 新 `probe.pod_images()` 一次 `kubectl get pods -A` 读 image+imageID(sha256)，pod 名前缀+ns 匹配到实例；best-effort，取不到→null |
| D6 | image 显示 | 实例处显 tag（`:`后段）；`title` hover = 完整 image ref + `@sha256:...`（image_id）|

## 3. 架构与模块

| 模块 | 职责 | 变更 |
|---|---|---|
| `core/probe.py` | 加 `pod_images(run) -> list[PodImage]`（一次 `get pods -A` 出 {ns,pod,image,image_id}）+ `match_pod_image(pods, name, ns) -> (image, image_id)`（ns 匹配 + pod 名前缀，取首个） | 改 |
| `server/app.py` | `GET /api/instances` 合并 managed(state)+external(discovery)、去重、加 `ownership/image/image_id` | 改 |
| `webui/index.html` + web.js `renderOverview` | 去掉「探测到的版本」card + 其渲染 | 改 |
| `webui/assets/web.js` | `renderMilvus`/`renderDeps`：managed/external 徽标、per-instance image + hover title、external 禁删；deps 表头去按类版本 chip、image 下沉到行；去掉 doctor 拉取 | 改 |

**边界**：discovery/pod_images 都 best-effort（fake/连不上→空，不崩、不拖慢关键路径）。前端只渲染 + 按 ownership 门控按钮；external 的后端删除本就因不在 state → 400（ValueError），UI 禁用是第一道。

## 4. 数据端点

```
GET /api/instances  (重构)
  -> { "instances": [ {
        name, kind, namespace,
        ownership,          # "managed" | "external"
        image,              # 展示用 image ref（匹配 pod 的 image；退回 evidence/snapshot；""）
        image_id,           # sha256 digest（best-effort；取不到 null）
        status,             # milvus: CR .status.status；否则 null
        deps                # milvus: {etcd,storage,mq,mq_endpoint}；否则 null
      } ] }
```

合并逻辑（`server/app.py`）：
1. `managed = { (kind,name,ns): Instance }` from `state.list_instances()`（ownership 一律 managed）。
2. `cands = discovery.discover()`；保留 `not c.excluded and c.ownership != readonly and kind in {etcd,minio,kafka,pulsar,milvus}`。
3. `pods = probe.pod_images()`（best-effort，adapter=k8s 才查；异常→[]）。
4. 输出：
   - 每个 managed instance：`ownership="managed"`，`image/image_id` = `match_pod_image(pods,name,ns)` 优先，退回 evidence(若该 (kind,name,ns) 也被 discover 到)/`snapshot.params.image`；milvus 补 `status`+`deps`（同现逻辑）。
   - 每个 cand 不在 managed：`ownership="external"`，`image` = `match_pod_image` 优先退回 `evidence.image` 首个 token，`image_id` = match 结果；milvus external 也补 `status`（CR 查询）；`deps`：external 无 snapshot → null。
5. 去重键 `(kind,name,namespace)`。

## 5. probe 细节

```python
class PodImage(NamedTuple):
    namespace: str; pod: str; image: str; image_id: str

def pod_images(run=run_kubectl) -> list[PodImage]:
    rc,out,_ = run(["get","pods","-A","-o",
      "jsonpath={range .items[*]}{.metadata.namespace}{'\\t'}{.metadata.name}{'\\t'}"
      "{.status.containerStatuses[0].image}{'\\t'}{.status.containerStatuses[0].imageID}{'\\n'}{end}"])
    if rc != 0: return []
    out_list=[]
    for line in out.splitlines():
        parts = line.split("\\t")
        if len(parts) == 4: out_list.append(PodImage(*[p.strip() for p in parts]))
    return out_list

def match_pod_image(pods, name, ns) -> tuple[str,str]:
    for p in pods:
        if p.namespace == ns and p.pod.startswith(name):
            return p.image, _sha_of(p.image_id)   # imageID like docker-pullable://repo@sha256:.. → keep @sha256:..
    return "", ""
```
`_sha_of`：从 imageID 提取 `sha256:...`（`imageID` 常形如 `<repo>@sha256:...` 或 `docker-pullable://<repo>@sha256:...`）；无 `sha256:` → 原样/空。`server` 里 image_id 为空则输出 null。

## 6. 前端

### Overview（`index.html` + `renderOverview`）
删除 `versions-card`（`id="versions-card"`/`id="versions"`）及 renderOverview 里 versions 段；不再 fetch doctor 的 versions。Overview = k8s 连接 + 运行环境。

### 归属徽标 + image 工具（web.js）
- `ownBadge(o)`：`managed`→`<span class="badge b-accent">managed</span>`；`external`→`<span class="badge b-muted">external</span>`。
- `imageCell(i)`：显示 `esc(tagOf(i.image) || '—')`，外层 `title="${esc(i.image)}${i.image_id ? ' @ '+esc(i.image_id) : ''}"`（hover 出完整 ref + sha256）。`tagOf(ref)` 取 `:`后、`@`前段。

### Milvus 卡（`renderMilvus`）
- `.inst-head .right` 加 `ownBadge`（在健康 badge 旁）。
- `.box-mv .id` 行的 image 用 `imageCell`（hover）。
- `.mv-actions`：`external` 时删除按钮也 `disabled title="external：mb 未安装，不可删除/升级"`；managed 才 `data-del`。

### Deps 手风琴（`renderDeps`）
- 表头去掉 `image: v<version>`（按类版本）chip；保留 logo+名+`n 个实例`。**不再 fetch `/api/doctor`**。
- `.dep-row`：`名 · ownBadge · imageCell(hover) · endpoint · 删除`；external 行删除 `disabled title=...`，managed 才 `data-del`。

### CSS
加 `.badge.b-muted`（灰底中性徽标）。其余复用。

## 7. 测试与验收

- **probe**（`tests/test_probe.py`）：`pod_images` 用 fake run 出多行→解析成 PodImage 列表；`match_pod_image` ns+前缀匹配、`_sha_of` 从 `repo@sha256:xxx`/`docker-pullable://repo@sha256:xxx` 提取。
- **端点**（`tests/test_web_endpoints.py`，fake adapter，hermetic）：装一个 managed（如 etcd-dev）→ `/api/instances` 含它 `ownership=managed`；fake 集群里其它被识别的工作负载出现且 `ownership=external`；控制面(kube-system etcd) 不出现；milvus 行仍有 deps/status 键。若能构造 (kind,name,ns) 撞 state 的用例，断言去重后为 managed。
- **前端 content-marker**（`tests/test_web_static.py`）：web.js 含 `ownBadge`/`imageCell`/`function tagOf`；`external` 分支含 `disabled`；index.html 不再有 `id="versions-card"`；web.css 含 `.b-muted`；renderDeps 不再含 `api/doctor`。
- **JS**：`node --check`。
- **手动 DoD**：`mb web` 真集群 → 每实例带 managed/external 徽标；managed 可删、external 删除按钮灰(带 title)；每实例显 image tag，hover 出完整路径 + sha256；Overview 无版本表；deps 表头无按类版本、image 在每行。

## 8. 非目标 / 后续
- external 的接管(adopt) UI（本切面只读+禁操作）。
- sha 匹配是 best-effort 名前缀启发式，偶尔可能缺/不准（缺则只显 tag+路径）。
- 不动 `mb doctor` / `/api/doctor` 本身（仅 UI 停用其 versions）。
