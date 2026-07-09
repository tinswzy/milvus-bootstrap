# WebUI per-dependency isolation config — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the single `isolationPrefix` with four independent per-dependency isolation values (etcd.rootPath, minio.bucketName, minio.rootPath, msgChannel.chanNamePrefix.cluster), each defaulting to the instance name, shown/editable near each dependency box with hover explanations, injected into `spec.config` and checked per-dependency at dry-run.

**Architecture:** Backend — the milvus driver injects four keys into `spec.config` (each `params.<key> or name`); the provisioner's `check_milvus_install` collides per-dependency (etcd/mq single-key, minio (bucket,rootPath) pair, each only when the dep endpoint is shared). Frontend — the topology form drops the central isolation field and puts a small labeled input in each dependency box, each mirroring the instance name until edited, each with a `title` tooltip.

**Tech Stack:** Python 3.11, FastAPI, pydantic, pytest. Frontend: vanilla JS.

## Global Constraints

- **Four isolation params** (milvus): `etcdRootPath`→`spec.config.etcd.rootPath`; `minioBucket`→`spec.config.minio.bucketName`; `minioRootPath`→`spec.config.minio.rootPath`; `mqChanPrefix`→`spec.config.msgChannel.chanNamePrefix.cluster`. Each injected as `params.get(<key>) or name` — all four ALWAYS injected, all default to the instance name (incl. minio.rootPath). `isolationPrefix` is REMOVED.
- **Injection** deep-merges into `spec.config` on top of `_dotted_to_nested(_conf)` (isolation wins) — same mechanism as today; `spec.config` is the honored CRD field (`spec.conf` doesn't exist).
- **Per-dependency dry-run collision** (milvus, in `check_milvus_install`): name-dup still rejected. Then per dep, only when the new install SHARES a dep endpoint with an existing milvus AND the isolation value(s) match: etcd → `etcdRootPath` equal; mq → `mqChanPrefix` equal; minio → the `(minioBucket, minioRootPath)` pair equal. Effective value of an existing instance = `snapshot.params.<key> or its name`. Default (each = unique name) never false-positives.
- **Frontend**: remove central `#inst-iso` + `#iso-preview`. etcd box gets `#inst-etcd-root` (rootPath); store box gets `#inst-store-bucket` + `#inst-store-root`; mq box gets `#inst-mq-prefix` (cluster). Each mirrors `#inst-name` until edited (per-field `dataset.dirty`). Each label has a `title` hover. `collectParams` sends the four keys (value or empty → backend falls back to name), drops `isolationPrefix`.
- **Non-milvus install unchanged.** milvus image stays in the center box (common-image is a later slice). XSS via `esc()`. Reuse existing helpers — no redefinition.
- Tests hermetic (MB_ADAPTER=fake). Run from `milvus-bootstrap/` with `source .venv/bin/activate`; `node --check` the JS.
- Branch `feat/webui-per-dep-isolation` off main. End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Verified current code:
- `drivers/milvus.py:105-110`: `prefix = params.get("isolationPrefix") or name; iso = {"msgChannel":{"chanNamePrefix":{"cluster":prefix}}, "etcd":{"rootPath":prefix}, "minio":{"bucketName":prefix}}; config = _deep_merge(_dotted_to_nested(params.get("_conf") or {}), iso); if config: cr_spec["config"] = config`.
- `engines/provisioner.py`: `_dep_eps(params)->set[str]` (etcdEndpoints list/str + storageEndpoint + pulsarEndpoint + kafkaBrokers list/str) and `check_milvus_install(instances, spec)` (name-dup; `new_prefix = params.isolationPrefix or spec.name`; collide if `eff == new_prefix and (_dep_eps(p) & new_eps)`). Called from `provisioner.install` milvus branch before compat.gate.
- Driver test setup: `prof = load_profiles()["milvus"]; drv = MilvusDriver(prof); method = prof.method("milvus-operator", Platform.k8s); cr = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)[-1]`.
- `web.js` milvus branch: center box has `#inst-image` + `#inst-iso` + `#iso-preview`; `collectParams` milvus sets `p.isolationPrefix`. Existing tests: `test_install_milvus_isolation_prefix` asserts `inst-iso`/`isolationPrefix`/`isoDirty` (must be updated); `test_milvus_injects_isolation_prefix_into_spec_config` + `test_milvus_prefix_collision_on_shared_dep` assert the old model (must be updated).

---

### Task 1: Driver injects four per-dependency isolation keys

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py`
- Test: `milvus-bootstrap/tests/test_milvus.py` (update the isolation test; keep the `_conf` test)

**Interfaces:**
- Produces: milvus CR `spec.config` gains `etcd.rootPath` / `minio.bucketName` / `minio.rootPath` / `msgChannel.chanNamePrefix.cluster`, each `params.<key> or name`.

- [ ] **Step 1: Update/replace the isolation driver test** — in `tests/test_milvus.py`, REPLACE `test_milvus_injects_isolation_prefix_into_spec_config` with:
```python
def test_milvus_per_dep_isolation_defaults_and_override() -> None:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    cfg = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"),
                                      method, {**method.params, "mq": "kafka"})[-1]["spec"]["config"]
    # all four default to the instance name (incl. minio.rootPath)
    assert cfg["etcd"]["rootPath"] == "m1"
    assert cfg["minio"]["bucketName"] == "m1"
    assert cfg["minio"]["rootPath"] == "m1"
    assert cfg["msgChannel"]["chanNamePrefix"]["cluster"] == "m1"
    assert "conf" not in drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, method.params)[-1]["spec"]
    # per-key override, others untouched
    cfg2 = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method,
        {**method.params, "mq": "kafka", "minioRootPath": "custom-rp", "mqChanPrefix": "myprefix"})[-1]["spec"]["config"]
    assert cfg2["minio"]["rootPath"] == "custom-rp"
    assert cfg2["msgChannel"]["chanNamePrefix"]["cluster"] == "myprefix"
    assert cfg2["etcd"]["rootPath"] == "m1" and cfg2["minio"]["bucketName"] == "m1"
```
(Leave `test_milvus_conf_merged_into_spec_config` as-is — it still passes.)

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_milvus.py -k "per_dep_isolation" -v`
Expected: FAIL — `minio.rootPath` absent; `minioRootPath`/`mqChanPrefix` not honored.

- [ ] **Step 3: Replace the injection block in `core/drivers/milvus.py`** (currently lines 105-110):
```python
        n = name
        iso = {
            "etcd": {"rootPath": params.get("etcdRootPath") or n},
            "minio": {"bucketName": params.get("minioBucket") or n,
                      "rootPath": params.get("minioRootPath") or n},
            "msgChannel": {"chanNamePrefix": {"cluster": params.get("mqChanPrefix") or n}},
        }
        config = _deep_merge(_dotted_to_nested(params.get("_conf") or {}), iso)
        if config:
            cr_spec["config"] = config
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_milvus.py -v && python -m pytest -q`
Expected: `test_milvus.py` PASS. Full suite may still FAIL on provisioner/web tests that reference the old `isolationPrefix` — those are updated in Tasks 2-3. Confirm ONLY `test_milvus.py` (driver) is green here; note remaining failures are the isolationPrefix ones for later tasks.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py milvus-bootstrap/tests/test_milvus.py
git commit -m "feat(milvus): inject four per-dependency isolation keys into spec.config

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Per-dependency dry-run collision check

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/engines/provisioner.py`
- Test: `milvus-bootstrap/tests/test_milvus.py` (rewrite the collision tests)

**Interfaces:**
- Produces: `_dep_ep_sets(params) -> dict[str, set]` (keys `etcd`/`minio`/`mq`); `_iso_of(params, name) -> dict` (keys `etcd`:str, `minio`:tuple, `mq`:str); `check_milvus_install(instances, spec)` collides per dependency.

- [ ] **Step 1: Rewrite the collision tests** — in `tests/test_milvus.py`, REPLACE `test_milvus_prefix_collision_on_shared_dep` (keep `test_milvus_dup_name_rejected` and `test_milvus_default_prefix_shared_dep_allowed` as they are) with:
```python
def test_milvus_mq_collision_on_shared_broker(core: Core) -> None:
    import pytest
    core.install(InstallSpec(kind="milvus", name="mv-a", params={
        "mq": "kafka", "kafkaBrokers": "kafka-x.default.svc:9092", "mqChanPrefix": "shared"}), dry_run=False)
    with pytest.raises(ValueError, match="MQ"):
        core.install(InstallSpec(kind="milvus", name="mv-b", params={
            "mq": "kafka", "kafkaBrokers": "kafka-x.default.svc:9092", "mqChanPrefix": "shared"}), dry_run=True)
    # same prefix, different broker → allowed
    core.install(InstallSpec(kind="milvus", name="mv-c", params={
        "mq": "kafka", "kafkaBrokers": "kafka-y.default.svc:9092", "mqChanPrefix": "shared"}), dry_run=True)


def test_milvus_minio_pair_collision(core: Core) -> None:
    import pytest
    base = {"mq": "kafka", "kafkaBrokers": "k.default.svc:9092", "storageEndpoint": "minio.default.svc:80"}
    core.install(InstallSpec(kind="milvus", name="mv-a", params={**base, "minioBucket": "shared", "minioRootPath": "rp"}), dry_run=False)
    # same bucket + same rootPath on the shared minio → collision
    with pytest.raises(ValueError, match="对象存储"):
        core.install(InstallSpec(kind="milvus", name="mv-b", params={**base, "minioBucket": "shared", "minioRootPath": "rp"}), dry_run=True)
    # same bucket but DIFFERENT rootPath → allowed (share bucket, isolate by path)
    core.install(InstallSpec(kind="milvus", name="mv-c", params={**base, "minioBucket": "shared", "minioRootPath": "other"}), dry_run=True)
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_milvus.py -k "mq_collision or minio_pair" -v`
Expected: FAIL — old check uses `isolationPrefix`; new params ignored.

- [ ] **Step 3: Replace `_dep_eps` and `check_milvus_install` in `core/engines/provisioner.py`** with:
```python
def _dep_ep_sets(params: dict) -> dict:
    """Per-dependency endpoint sets the milvus install binds to."""
    def _as_set(v):
        if isinstance(v, (list, tuple)):
            return {str(e) for e in v}
        return {str(v)} if v else set()
    return {
        "etcd": _as_set(params.get("etcdEndpoints")),
        "minio": _as_set(params.get("storageEndpoint")),
        "mq": _as_set(params.get("kafkaBrokers")) | _as_set(params.get("pulsarEndpoint")),
    }


def _iso_of(params: dict, name: str) -> dict:
    """Effective per-dependency isolation values (default = instance name)."""
    return {
        "etcd": params.get("etcdRootPath") or name,
        "minio": (params.get("minioBucket") or name, params.get("minioRootPath") or name),
        "mq": params.get("mqChanPrefix") or name,
    }


_DEP_LABELS = {"etcd": "etcd", "minio": "对象存储", "mq": "MQ"}


def check_milvus_install(instances: list, spec) -> None:
    """Reject a milvus install that duplicates a name or collides per-dependency
    (shares a dep endpoint AND uses the same isolation value(s) for that dep)."""
    if any(i.name == spec.name for i in instances):
        raise ValueError(f"实例名 {spec.name} 已存在，请换名")
    new_eps, new_iso = _dep_ep_sets(spec.params), _iso_of(spec.params, spec.name)
    for i in instances:
        snap = i.spec_snapshot or {}
        if snap.get("kind") != "milvus":
            continue
        p = snap.get("params", {}) or {}
        eps, iso = _dep_ep_sets(p), _iso_of(p, i.name)
        for dep in ("etcd", "minio", "mq"):
            if (new_eps[dep] & eps[dep]) and new_iso[dep] == iso[dep]:
                raise ValueError(
                    f"{_DEP_LABELS[dep]} 隔离与 milvus {i.name} 冲突"
                    f"（共享同一 {_DEP_LABELS[dep]} 且隔离值相同），请改{_DEP_LABELS[dep]}的隔离配置")
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_milvus.py -v && python -m pytest -q`
Expected: `test_milvus.py` PASS. Full suite still fails only on the web static isolation test (Task 3).

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/engines/provisioner.py milvus-bootstrap/tests/test_milvus.py
git commit -m "feat(provisioner): per-dependency milvus isolation collision check

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Frontend — per-dependency isolation fields in the topology boxes

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css`
- Test: `milvus-bootstrap/tests/test_web_static.py` (update the isolation marker test)

**Interfaces:**
- Consumes: the topology form from the prior slice.
- Produces: `#inst-etcd-root` / `#inst-store-bucket` / `#inst-store-root` / `#inst-mq-prefix`; `collectParams` sends `etcdRootPath`/`minioBucket`/`minioRootPath`/`mqChanPrefix`; `#inst-iso`/`#iso-preview`/`isolationPrefix` removed.

- [ ] **Step 1: Update the marker test** — in `tests/test_web_static.py`, REPLACE `test_install_milvus_isolation_prefix` with:
```python
def test_install_milvus_per_dep_isolation(client):
    js = client.get("/assets/web.js").text
    for m in ["inst-etcd-root", "inst-store-bucket", "inst-store-root", "inst-mq-prefix",
              "etcdRootPath", "minioBucket", "minioRootPath", "mqChanPrefix", "title="]:
        assert m in js, m
    assert "isolationPrefix" not in js and 'id="inst-iso"' not in js
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k per_dep_isolation -v`
Expected: FAIL — new ids absent; `isolationPrefix` still present.

- [ ] **Step 3: Edit `fillParams` milvus branch in `web.js`.**

  (a) Add an isolation-field helper (near the top of the milvus branch, after `const insts = await loadInstances();`):
```javascript
  const isoField = (id, label, title) =>
    `<div class="iso-in"><label title="${esc(title)}">${esc(label)} <span class="q">?</span></label>` +
    `<input id="${id}" class="f-in">`;
```

  (b) In the etcd box, after the etcd `-custom` input line, append:
```javascript
        isoField('inst-etcd-root', 'rootPath', 'etcd.rootPath —— Milvus 在 etcd 存元数据的根路径。共用同一 etcd 时用它区分不同 Milvus；默认=实例名。') +
```
  Close the etcd box's `<div>` after it.

  (c) In the store box, after the storage `-custom` input line, append:
```javascript
        isoField('inst-store-bucket', 'bucket', 'minio.bucketName —— Milvus 对象存储用的桶名，各 Milvus 一个桶；默认=实例名。') +
        isoField('inst-store-root', 'rootPath', 'minio.rootPath —— 桶内子路径前缀；想多个 Milvus 共用一个桶又互不干扰时改它；默认=实例名。') +
```

  (d) In the mq box, after the `#inst-mqinst-row` div, append:
```javascript
        isoField('inst-mq-prefix', 'cluster', 'msgChannel.chanNamePrefix.cluster —— MQ topic/channel 名前缀，共用同一 kafka/pulsar 时避免撞名；默认=实例名。') +
```

  (e) In the center box-mv, REMOVE the `#inst-iso` label+input lines and the `#iso-preview` div (keep the image field).

  (f) REPLACE the mirror/preview wiring block (the `const isoEl` / `const prevEl` / `updatePreview` / `isoDirty` section) with the four-field mirror:
```javascript
  const nameEl = document.getElementById('inst-name');
  const mvNameEl = document.getElementById('mv-name');
  const isoFields = ['inst-etcd-root', 'inst-store-bucket', 'inst-store-root', 'inst-mq-prefix']
    .map(id => document.getElementById(id));
  isoFields.forEach(el => {
    el.value = nameEl.value.trim();
    el.oninput = () => { el.dataset.dirty = '1'; };
  });
  nameEl.oninput = () => {
    isoFields.forEach(el => { if (!el.dataset.dirty) el.value = nameEl.value.trim(); });
    mvNameEl.textContent = nameEl.value.trim() || '新 Milvus';
  };
  mvNameEl.textContent = nameEl.value.trim() || '新 Milvus';
```

  (g) In `collectParams` milvus branch, REMOVE the `isolationPrefix` lines and add:
```javascript
  p.etcdRootPath = (document.getElementById('inst-etcd-root') || {}).value || '';
  p.minioBucket = (document.getElementById('inst-store-bucket') || {}).value || '';
  p.minioRootPath = (document.getElementById('inst-store-root') || {}).value || '';
  p.mqChanPrefix = (document.getElementById('inst-mq-prefix') || {}).value || '';
```

- [ ] **Step 4: Append CSS to `web.css`** (at end):
```css
/* --- install: per-dependency isolation fields inside topology boxes --- */
.iso-in { margin-top:9px; display:flex; flex-direction:column; gap:4px; }
.iso-in > label { font-size:10.5px; font-weight:600; color:var(--fg-3); text-transform:uppercase; letter-spacing:.3px; display:flex; align-items:center; gap:5px; }
.iso-in .q { width:13px; height:13px; border-radius:50%; background:var(--surface-3); border:1px solid var(--line-2);
  color:var(--fg-3); font-size:9px; font-weight:700; display:inline-grid; place-items:center; cursor:help; text-transform:none; }
.iso-in .f-in { height:30px; font-size:12px; }
```

- [ ] **Step 5: Verify JS + run test + full suite**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; ALL tests PASS (this task removes the last `isolationPrefix` references, so the full suite is green now).

- [ ] **Step 6: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.css milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): per-dependency isolation fields in topology boxes (+ hover)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`mb web --port 8090` (live cluster), open `/install.html`, pick **milvus**:
- etcd box shows a `rootPath` field; store box shows `bucket` + `rootPath`; mq box shows `cluster`. Each defaults to the instance name and follows it as you type the name; each is independently editable; hovering the `?` shows the config explanation.
- The center box no longer has the isolation-prefix field / preview chips.
- dry-run a second milvus sharing the same kafka with the same `cluster` → error naming MQ; change `cluster` → passes.
- Install a throwaway, then `kubectl get cm <name> -o jsonpath='{.data.user\.yaml}'` → shows `etcd.rootPath` / `bucketName` / `rootPath` / `chanNamePrefix.cluster` = the four field values.

## Self-Review

- **Spec coverage:** D1 four independent values → Tasks 1,3; D2 the four keys → Task 1 (inject) + Task 3 (params); D3 all default=name always-inject → Task 1; D4 fields in each dep box → Task 3; D5 hover title → Task 3; D6 per-dep collision (minio pair) → Task 2; D7 non-goal (common image) untouched. §4 injection → Task 1; §5 collision → Task 2; §6 frontend → Task 3; §7 tests → each task.
- **Placeholder scan:** every step has complete code; frontend via content-marker + manual DoD (no JS harness — stated). No TBD.
- **Type consistency:** param keys `etcdRootPath`/`minioBucket`/`minioRootPath`/`mqChanPrefix` consistent across Task 1 (driver reads), Task 2 (`_iso_of` reads), Task 3 (collectParams writes). `_dep_ep_sets`/`_iso_of`/`check_milvus_install` (Task 2). Field ids `#inst-etcd-root`/`#inst-store-bucket`/`#inst-store-root`/`#inst-mq-prefix` consistent Task 3 markup ↔ collectParams ↔ test. `isolationPrefix`/`#inst-iso`/`#iso-preview` fully removed (Task 3 + updated tests in Tasks 1-2).
- **Cross-task test note:** the full suite is only guaranteed green after Task 3 (Tasks 1-2 remove backend `isolationPrefix` but the frontend/web-static test still references it until Task 3). Each task's own `test_milvus.py`/target tests pass at its own step; the plan calls this out so a mid-plan red suite isn't mistaken for a regression.
