# WebUI managed/external + per-instance image/sha — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Label each instance managed (mb-installed) vs external (discovered, user-installed), block delete/upgrade on external, drop the Overview versions table in favor of a per-instance image tag with a hover tooltip showing the full image ref + sha256 image id.

**Architecture:** `GET /api/instances` merges state's managed instances with `DiscoveryEngine.discover()` externals (dedup by kind+name+ns, control-plane excluded), enriching each with `ownership`, `image`, `image_id` (sha256 via a new one-shot `probe.pod_images`). Frontend adds a managed/external badge, a per-instance image cell with hover, disables delete for external, and removes the Overview versions card + the deps by-kind version chip.

**Tech Stack:** Python 3.11, FastAPI, pydantic, pytest+TestClient. Frontend: vanilla HTML/CSS/JS.

## Global Constraints

- **Ownership**: `managed` = mb installed (in `state.list_instances()`); `external` = discovered by `DiscoveryEngine`, identified as one of `{etcd,minio,kafka,pulsar,milvus}`, NOT excluded, NOT `readonly`, and NOT in state. Dedup key `(kind, name, namespace)` — a state match wins (managed).
- **Control-plane / excluded / readonly are NOT shown.**
- **external cannot delete/upgrade**: UI disables the delete button (and the milvus placeholder actions stay disabled); only `managed` rows carry `data-del`. (Backend delete of an external already 400s — not in state — but UI is the first gate.)
- **image_id (sha256)** via `probe.pod_images()` — one `kubectl get pods -A -o jsonpath`, matched to an instance by `namespace == ns AND pod.startswith(name)`, best-effort (fake/unreachable/no-match → `image_id: null`, image falls back to evidence/snapshot).
- **image display**: instance shows the tag (`:` segment, before `@`); hover `title` = full image ref + ` @ ` + sha256 image_id.
- **Versions follow instances**: remove Overview `versions-card`; UI stops using `/api/doctor` versions (deps accordion header drops its by-kind version chip and its doctor fetch). `mb doctor` / `/api/doctor` themselves are unchanged (Overview keeps the doctor fetch for env/conn only).
- **best-effort**: discovery and pod_images are wrapped try/except → never crash or slow the endpoint; only queried under the k8s adapter for pod_images.
- **XSS**: every server string via `esc()`. Reuse `shell/esc/getJSON/badge/deleteInstance/depBox/mqLogo/depEndpoint/DEP_META` — do NOT redefine.
- Tests hermetic (MB_ADAPTER=fake). Run from `milvus-bootstrap/` with `source .venv/bin/activate`; `node --check` the JS.
- Branch `feat/webui-managed-external` off main. End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Verified shapes:
- `_core().discovery.discover() -> list[Candidate]`; `Candidate{kind,name,ownership,excluded,reason,evidence}`. `evidence` is a dict with `image` (space-joined tags), `namespace`, `labels`. Candidate has NO `.namespace` — use `evidence["namespace"]`. `c.ownership` is an `Ownership` enum (`.value` in {managed,adoptable,readonly,external}).
- Fake cluster (`core/platform/fake.py _FAKE_CLUSTER`) yields discoverable workloads incl. `milvus-etcd`(default, etcd), `milvus-minio`(default, minio), and a control-plane `etcd`(kube-system) that identify marks excluded. So under fake, `/api/instances` returns these as `external` even with empty state — the existing `test_api_instances_empty` MUST be updated (Task 2).
- `probe.run_kubectl(args) -> (rc, stdout, stderr)`; `probe.milvus_status(name, run=run_kubectl)` exists.
- Current `/api/instances` (server/app.py) reads only `state.list_instances()`; `from ..core import probe` already imported.
- `renderOverview` (web.js) fetches `getJSON('api/doctor')` and uses `doc.env` (conn/env-list) AND `doc.versions` (versions card) — remove only the versions usage. `renderDeps` fetches `api/doctor` for the header version chip — remove entirely. `renderMilvus` renders `.inst-head .right` (health badge) and `.box-mv .id` (name · image) and `.mv-actions` (placeholders + delete). Shared frontend helpers live in web.js.
- web.css has `.badge`, `.badge.b-accent`, `.badge.b-ok` (add `.badge.b-muted`).

---

### Task 1: `probe.pod_images` + `match_pod_image` + `_sha_of`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/probe.py`
- Test: `milvus-bootstrap/tests/test_probe.py` (append)

**Interfaces:**
- Produces: `probe.PodImage` (NamedTuple `namespace,pod,image,image_id`); `probe.pod_images(run=run_kubectl) -> list[PodImage]`; `probe.match_pod_image(pods, name, ns) -> tuple[str,str]` (image, sha256-or-"") ; `probe._sha_of(image_id) -> str`.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_probe.py`:
```python
def test_sha_of_extracts_digest():
    assert probe._sha_of("docker-pullable://milvusdb/milvus@sha256:abc123") == "sha256:abc123"
    assert probe._sha_of("milvusdb/etcd@sha256:def456") == "sha256:def456"
    assert probe._sha_of("milvusdb/etcd:3.5") == ""          # no digest
    assert probe._sha_of("") == ""


def test_pod_images_parses_and_matches():
    line = ("default\tetcd-0\tmilvusdb/etcd:3.5.18\tmilvusdb/etcd@sha256:aaa\n"
            "default\tmilvus-dev-standalone-1\tmilvusdb/milvus:v2.6.18\tdocker-pullable://milvusdb/milvus@sha256:bbb\n")
    pods = probe.pod_images(run=lambda a: (0, line, ""))
    assert len(pods) == 2 and pods[0].pod == "etcd-0"
    # match by ns + name prefix
    assert probe.match_pod_image(pods, "etcd", "default") == ("milvusdb/etcd:3.5.18", "sha256:aaa")
    assert probe.match_pod_image(pods, "milvus-dev", "default") == ("milvusdb/milvus:v2.6.18", "sha256:bbb")
    assert probe.match_pod_image(pods, "etcd", "other-ns") == ("", "")   # ns mismatch
    assert probe.pod_images(run=lambda a: (1, "", "boom")) == []          # kubectl failure
```

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_probe.py -k "sha_of or pod_images" -v`
Expected: FAIL — names not defined.

- [ ] **Step 3: Add to `core/probe.py`** (near `milvus_status`; add `from typing import NamedTuple` if not present):
```python
class PodImage(NamedTuple):
    namespace: str
    pod: str
    image: str
    image_id: str


def _sha_of(image_id: str) -> str:
    """Extract 'sha256:...' from a k8s imageID (repo@sha256:.. / docker-pullable://repo@sha256:..)."""
    if not image_id or "sha256:" not in image_id:
        return ""
    return "sha256:" + image_id.split("sha256:", 1)[1].strip()


def pod_images(run=run_kubectl) -> list[PodImage]:
    """One-shot map of every pod's primary container image + imageID (best-effort)."""
    rc, out, _ = run(["get", "pods", "-A", "-o",
                      "jsonpath={range .items[*]}{.metadata.namespace}{'\\t'}{.metadata.name}{'\\t'}"
                      "{.status.containerStatuses[0].image}{'\\t'}{.status.containerStatuses[0].imageID}{'\\n'}{end}"])
    if rc != 0:
        return []
    pods: list[PodImage] = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) == 4:
            pods.append(PodImage(*[p.strip() for p in parts]))
    return pods


def match_pod_image(pods, name: str, ns: str) -> tuple[str, str]:
    """First pod in ns whose name starts with the instance name → (image, sha256-or-'')."""
    for p in pods:
        if p.namespace == ns and p.pod.startswith(name):
            return p.image, _sha_of(p.image_id)
    return "", ""
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_probe.py -v && python -m pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/probe.py milvus-bootstrap/tests/test_probe.py
git commit -m "feat(probe): pod_images + match_pod_image (per-instance image + sha256)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `/api/instances` merges managed + external

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_web_endpoints.py` (update `test_api_instances_empty`; append)

**Interfaces:**
- Consumes: `probe.pod_images`/`match_pod_image`/`milvus_status`, `_core().state`, `_core().discovery`.
- Produces: `/api/instances` rows `{name,kind,namespace,ownership,image,image_id,status,deps}` where `ownership∈{"managed","external"}`.

- [ ] **Step 1: Update + write tests** in `tests/test_web_endpoints.py`.

Replace `test_api_instances_empty` (state is empty but discovery surfaces fake-cluster externals now):
```python
def test_api_instances_empty_state_shows_externals(client):
    rows = client.get("/api/instances").json()["instances"]
    # no managed instances installed → every row is external, control-plane excluded
    assert rows, "fake cluster should yield discovered externals"
    assert all(r["ownership"] == "external" for r in rows)
    names = {r["name"] for r in rows}
    assert "milvus-etcd" in names            # discoverable dep in default ns
    assert "etcd" not in names               # kube-system control-plane excluded
```

Append:
```python
def test_api_instances_managed_and_fields(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    app_module.core.install(InstallSpec(kind="milvus", name="milvus-dev", params={
        "mq": "kafka", "image": "milvusdb/milvus:v2.6.18",
        "storageEndpoint": "minio.default.svc:80", "kafkaBrokers": "kafka-dev.default.svc:9092"}), dry_run=False)
    rows = {r["name"]: r for r in client.get("/api/instances").json()["instances"]}
    m = rows["milvus-dev"]
    assert m["ownership"] == "managed"
    assert m["image"] == "milvusdb/milvus:v2.6.18"    # from snapshot (fake → no pod match)
    assert m["image_id"] is None                       # fake adapter → no pod images
    assert m["deps"]["mq"] == "kafka"
    assert "image_id" in rows["milvus-etcd"]           # external rows also carry the key
    assert rows["milvus-etcd"]["ownership"] == "external"
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_endpoints.py -k "instances" -v`
Expected: FAIL — current endpoint returns only state (empty), no `ownership=external`, no `image_id`.

- [ ] **Step 3: Rewrite `api_instances` in `server/app.py`** — add the module constant near the top and replace the whole `def api_instances()`:
```python
_INSTANCE_KINDS = {"etcd", "minio", "kafka", "pulsar", "milvus"}


@app.get("/api/instances")
def api_instances() -> dict[str, Any]:
    core = _core()
    is_k8s = getattr(core.adapter, "name", "") == "k8s"
    pods = []
    if is_k8s:
        try:
            pods = probe.pod_images()
        except Exception:
            pods = []

    def milvus_status_safe(name: str):
        if not is_k8s:
            return None
        try:
            return probe.milvus_status(name)
        except Exception:
            return None

    out = []
    seen = set()
    # managed (from state)
    for i in core.state.list_instances():
        snap = i.spec_snapshot or {}
        kind = snap.get("kind", "")
        params = snap.get("params", {}) or {}
        ns = i.namespace
        img, img_id = probe.match_pod_image(pods, i.name, ns)
        image = img or params.get("image", "")
        status, deps = None, None
        if kind == "milvus":
            deps = {"etcd": params.get("etcdEndpoints", ""), "storage": params.get("storageEndpoint", ""),
                    "mq": params.get("mq", ""),
                    "mq_endpoint": params.get("kafkaBrokers") or params.get("pulsarEndpoint") or ""}
            status = milvus_status_safe(i.name)
        seen.add((kind, i.name, ns))
        out.append({"name": i.name, "kind": kind, "namespace": ns, "ownership": "managed",
                    "image": image, "image_id": img_id or None, "status": status, "deps": deps})
    # external (from discovery)
    try:
        cands = core.discovery.discover()
    except Exception:
        cands = []
    for c in cands:
        if c.excluded or c.kind not in _INSTANCE_KINDS or getattr(c.ownership, "value", "") == "readonly":
            continue
        ev = c.evidence if isinstance(c.evidence, dict) else {}
        ns = ev.get("namespace", "")
        key = (c.kind, c.name, ns)
        if key in seen:
            continue
        seen.add(key)
        img, img_id = probe.match_pod_image(pods, c.name, ns)
        image = img or (ev.get("image", "").split(" ")[0])
        status = milvus_status_safe(c.name) if c.kind == "milvus" else None
        out.append({"name": c.name, "kind": c.kind, "namespace": ns, "ownership": "external",
                    "image": image, "image_id": img_id or None, "status": status, "deps": None})
    return {"instances": out}
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_endpoints.py -v && python -m pytest -q`
Expected: PASS. (If a prior test asserted an exact empty list elsewhere, update it to match the new discovery-merged shape.)

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/tests/test_web_endpoints.py
git commit -m "feat(server): /api/instances merges managed(state)+external(discovery) with image/sha

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Overview drops the versions table

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/index.html`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (`renderOverview`)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Produces: Overview without a versions card; `renderOverview` keeps the doctor fetch for env/conn, drops the versions rendering.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_overview_has_no_versions_card(client):
    body = client.get("/").text
    assert 'id="versions-card"' not in body and 'id="versions"' not in body
    js = client.get("/assets/web.js").text
    assert "renderOverview" in js and "env-list" in js     # overview still renders env
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k versions_card -v`
Expected: FAIL — versions card present.

- [ ] **Step 3: Remove the versions card from `webui/index.html`** — delete this block:
```html
      <div class="card" id="versions-card"><div class="card-head"><h3>探测到的版本</h3></div>
        <div class="card-pad"><div id="versions">连接集群后展示</div></div></div>
```

- [ ] **Step 4: Remove the versions rendering from `renderOverview` in `web.js`** — delete the versions block (the `// versions (only if connected)` section that sets `document.getElementById('versions').innerHTML = connected ? ... : ...`). Keep `const doc = await getJSON('api/doctor')` and the conn/env-list rendering that uses `doc.env`. After this, `renderOverview` no longer references `document.getElementById('versions')` or `doc.versions`.

- [ ] **Step 5: Run to verify pass + full suite**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; PASS. (If `test_root_serves_overview_html` asserted the versions element, update it to drop that assertion.)

- [ ] **Step 6: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/index.html milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): drop Overview versions table (versions now per-instance)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Milvus card — ownership badge + image hover + external-disable

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (add `ownBadge`/`imageCell`/`tagOf`; edit `renderMilvus`)
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css` (add `.badge.b-muted`)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `esc`, row fields `ownership/image/image_id`.
- Produces: `tagOf(ref)`, `imageCell(i)`, `ownBadge(o)` (reused by Task 5); milvus card shows badge + image hover + disables delete for external.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_milvus_card_ownership_and_image_hover(client):
    js = client.get("/assets/web.js").text
    for m in ['function tagOf', 'function imageCell', 'function ownBadge', 'ownBadge(i.ownership)', 'imageCell(i)']:
        assert m in js, m
    # external rows get a disabled delete with an explanatory title
    assert "external：mb 未安装" in js
    css = client.get("/assets/web.css").text
    assert ".badge.b-muted" in css or ".b-muted" in css
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k milvus_card_ownership -v`
Expected: FAIL — helpers/CSS absent.

- [ ] **Step 3: Add the three helpers to `web.js`** (place just before `renderMilvus`):
```javascript
function tagOf(ref) {
  if (!ref) return '';
  const noDigest = String(ref).split('@')[0];
  return noDigest.includes(':') ? noDigest.split(':').pop() : noDigest;
}
function imageCell(i) {
  const title = i.image ? (i.image + (i.image_id ? ' @ ' + i.image_id : '')) : '';
  return `<span class="mono" title="${esc(title)}">${esc(tagOf(i.image) || '—')}</span>`;
}
function ownBadge(o) {
  return o === 'managed'
    ? '<span class="badge b-accent">managed</span>'
    : '<span class="badge b-muted">external</span>';
}
function delButton(i) {
  return i.ownership === 'managed'
    ? `<button class="btn btn-ghost btn-sm" data-del="${esc(i.name)}">删除</button>`
    : `<button class="btn btn-ghost btn-sm" disabled title="external：mb 未安装，不可删除/升级">删除</button>`;
}
```

- [ ] **Step 4: Edit `renderMilvus`** in `web.js`:
  - In `.inst-head`, change the right cell to include the ownership badge before the health badge:
    `<div class="right">${ownBadge(i.ownership)} ${st}</div>`
  - In `.box-mv .id`, replace the image text `${esc(i.image || '—')}` with `${imageCell(i)}` (so the line becomes `${esc(i.name)} · ` + `imageCell(i)`).
  - In `.mv-actions`, replace the hard-coded delete button `<button class="btn btn-ghost btn-sm" data-del="${esc(i.name)}">删除</button>` with `${delButton(i)}`.
  (The `[data-del]` wiring already only binds elements that have the attribute, so external rows — whose delete is `disabled` with no `data-del` — are inert.)

- [ ] **Step 5: Add `.badge.b-muted` to `web.css`** (near the other badge rules or at end):
```css
.badge.b-muted { background:var(--surface-2); color:var(--fg-3); border:1px solid var(--line); }
```

- [ ] **Step 6: Verify + run**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; PASS.

- [ ] **Step 7: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): Milvus card ownership badge + image hover + external-disable delete

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Deps accordion — per-row image + badge + external-disable; drop version chip

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js` (`renderDeps`)
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `ownBadge`/`imageCell`/`delButton` (Task 4), `depEndpoint`/`DEP_META`.
- Produces: deps accordion rows with badge + image hover + external-disable; header no longer shows a by-kind version chip; no `/api/doctor` fetch.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_deps_rows_have_image_and_ownership(client):
    js = client.get("/assets/web.js").text
    # renderDeps no longer fetches doctor versions, and rows carry ownership + image
    deps_src = js[js.index("function renderDeps"):]
    deps_src = deps_src[:deps_src.index("async function renderMilvus")] if "async function renderMilvus" in deps_src else deps_src
    assert "api/doctor" not in deps_src
    assert "ownBadge(i.ownership)" in deps_src and "imageCell(i)" in deps_src and "delButton(i)" in deps_src
```
(If `renderMilvus` sits before `renderDeps` in the file, adjust the slice bound; the intent is that the `renderDeps` body contains no `api/doctor`.)

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k deps_rows_have_image -v`
Expected: FAIL — renderDeps still fetches doctor + no per-row image/badge.

- [ ] **Step 3: Edit `renderDeps` in `web.js`:**
  - Remove the doctor fetch + versions lines:
    ```javascript
    const doc = await getJSON('api/doctor').catch(() => ({ versions: {} }));
    const versions = doc.versions || {};
    ```
  - In the accordion header `.right`, remove the by-kind version chip
    `<span class="img">image: <span class="t">v${esc(versions[kind] || '—')}</span></span>` (keep the `+ 新建` link and `.chev`).
  - Rewrite each `.dep-row` to include the ownership badge, image cell, and gated delete:
    ```javascript
    `<div class="dep-row"><span class="nm">${esc(i.name)}</span>` +
    `${ownBadge(i.ownership)}` +
    `<span class="muted">ns:${esc(i.namespace)}</span>` +
    `${imageCell(i)}` +
    `<span class="mono muted">${esc(depEndpoint(kind, i.name, i.namespace))}</span>` +
    `${delButton(i)}</div>`
    ```
  - The `[data-del]` wiring stays (external rows have no `data-del`, so inert).

- [ ] **Step 4: Verify + run**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; PASS.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): deps accordion per-row image + ownership + external-disable; drop version chip

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`mb web --port 8090` (live cluster), open `http://127.0.0.1:8090/`:
- Overview: no versions card (env + k8s conn only).
- Milvus 页: each card shows a `managed`/`external` badge; managed cards' 删除 is active, external cards' 删除 is greyed with a title; the core box image is a tag, hovering shows the full ref + `@sha256:…`.
- Dependencies 页: accordion header has no version chip; each row shows badge + image tag (hover full+sha) + endpoint + gated 删除.
- Any user-installed (non-mb) etcd/minio/kafka/pulsar/milvus in the cluster appears as `external`; control-plane etcd does not.

## Self-Review

- **Spec coverage:** D1 external via discovery merge → Task 2; D2 ownership/exclusion → Task 2; D3 external no-delete → Tasks 4-5 (`delButton`); D4 drop Overview versions + stop doctor-versions → Tasks 3,5; D5 pod_images/sha → Task 1 + Task 2 wiring; D6 image tag + hover → Task 4 (`tagOf`/`imageCell`). §4 endpoint shape → Task 2. §5 probe → Task 1. §6 frontend (overview/milvus/deps/CSS) → Tasks 3,4,5.
- **Placeholder scan:** every step has complete code; frontend verified via content-marker + manual DoD (no JS harness — stated). No TBD/TODO.
- **Type consistency:** row keys `{name,kind,namespace,ownership,image,image_id,status,deps}` consistent Task 2↔4↔5; `PodImage`/`pod_images`/`match_pod_image`/`_sha_of` Task 1↔2; `tagOf/imageCell/ownBadge/delButton` defined Task 4, reused Task 5; reuses `esc/depEndpoint/DEP_META/deleteInstance`. `ownership` values `"managed"`/`"external"` consistent across backend + frontend gating.
- **Existing-test updates (called out):** Task 2 replaces `test_api_instances_empty` (discovery now surfaces externals under fake); Task 3 may adjust `test_root_serves_overview_html` if it asserted the versions element.
