# WebUI Milvus install: dep dropdowns + isolation prefix + dry-run checks

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the WebUI Milvus install form use dependency dropdowns, expose an editable "data isolation prefix" (default = instance name) that mb injects into the CR `spec.config`, and add dry-run checks for duplicate name / prefix-collision-on-shared-deps.

**Architecture:** Backend — the milvus driver injects the prefix into `spec.config.{msgChannel.chanNamePrefix.cluster, etcd.rootPath, minio.bucketName}` (verified honored by the operator) and routes `_conf` there too (dropping the dead `spec.conf`); a `check_milvus_install` precheck in the provisioner rejects name/prefix conflicts. Frontend — the milvus form renders dep `<select>`s (from `/api/instances`, with a custom fallback) plus an isolation-prefix field mirrored from the instance name.

**Tech Stack:** Python 3.11, FastAPI, pydantic, pytest. Frontend: vanilla JS.

## Global Constraints

- **Injection field is `spec.config` (nested), NOT `spec.conf`** — CRD `milvus.io/v1beta1` has no `spec.conf` (verified). Inject `spec.config.msgChannel.chanNamePrefix.cluster` + `spec.config.etcd.rootPath` + `spec.config.minio.bucketName` = prefix. Always inject (prefix defaults to instance name → matches operator default).
- **prefix** = `params.get("isolationPrefix") or spec.name`.
- **Route `_conf` into `spec.config` too** (dotted-key → nested, deep-merged; isolation keys win) and REMOVE the old `cr_spec["conf"] = {"data": conf}` write. (config-get is out of scope — do not touch it.)
- **dry-run checks (milvus only)**: (1) name must not equal any existing mb instance's name; (2) no other **milvus** may have the same effective prefix AND share any dependency endpoint. Effective prefix of an existing instance = `snapshot.params.isolationPrefix or its name`. Checks read mb state; raise `ValueError` (→ 400 in UI / error in CLI). Default prefix = unique name → no false positives.
- **dep dropdowns** populate from `/api/instances` filtered by kind; each has a `自定义…` (`__custom__`) option revealing a text input. Non-milvus install form is unchanged.
- **XSS**: every server string via `esc()`. Reuse `esc/getJSON/depEndpoint/shell/badge` — no redefinition. Frontend has no JS harness (content-marker tests + manual DoD).
- Tests hermetic (MB_ADAPTER=fake). Run from `milvus-bootstrap/` with `source .venv/bin/activate`; `node --check` the JS.
- Branch `feat/webui-milvus-install-ux` off main. End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Verified shapes:
- Driver test pattern (`tests/test_milvus.py`): `prof = load_profiles()["milvus"]; drv = MilvusDriver(prof); method = prof.method("milvus-operator", Platform.k8s); cr = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)[-1]` → assert on `cr["spec"]`.
- `drivers/milvus.py build_install_manifests`: builds `cr_spec = {"mode","components","dependencies"}` then `conf = params.get("_conf"); if conf: cr_spec["conf"] = {"data": conf}` (THIS is the dead write to change), then appends the CR manifest. `name` and `params` are in scope.
- `provisioner.install(self, spec, dry_run=True, force=False)`: begins `if spec.kind == "milvus":` then `compat.gate(...)`. `self.state.list_instances()` available.
- Frontend `web.js`: `INSTALL_KINDS`, `INSTALL_DEFAULTS`, `fillParams(kind)` (renders `#inst-params`), `collectParams()` (reads key=value rows), `installBody()` (builds `{kind,name,namespace,params:collectParams(),...}`), `renderInstall()` (wires kind select + dryrun/apply). `depEndpoint(kind,name,ns)` exists (etcd :2379 / minio :80 / kafka :9092 / pulsar -broker :6650). `install.html` has `#inst-params` container, `#inst-name`, `#inst-kind`, `#inst-ns`, `#err`, `#inst-result`.

---

### Task 1: Driver injects isolation prefix into `spec.config` (+ route `_conf`, drop `spec.conf`)

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py`
- Test: `milvus-bootstrap/tests/test_milvus.py` (append)

**Interfaces:**
- Produces: milvus CR gains `spec.config` with the 3 isolation keys = prefix; module helpers `_dotted_to_nested(flat: dict) -> dict`, `_deep_merge(a: dict, b: dict) -> dict`.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_milvus.py`:
```python
def test_milvus_injects_isolation_prefix_into_spec_config() -> None:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    # default: prefix == instance name
    cr = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, {**method.params, "mq": "kafka"})[-1]
    cfg = cr["spec"]["config"]
    assert cfg["msgChannel"]["chanNamePrefix"]["cluster"] == "m1"
    assert cfg["etcd"]["rootPath"] == "m1"
    assert cfg["minio"]["bucketName"] == "m1"
    assert "conf" not in cr["spec"]                       # dead spec.conf field removed
    # explicit prefix override
    cr2 = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"),
                                      method, {**method.params, "mq": "kafka", "isolationPrefix": "shared-a"})[-1]
    assert cr2["spec"]["config"]["etcd"]["rootPath"] == "shared-a"


def test_milvus_conf_merged_into_spec_config() -> None:
    prof = load_profiles()["milvus"]
    drv = MilvusDriver(prof)
    method = prof.method("milvus-operator", Platform.k8s)
    params = {**method.params, "mq": "kafka", "_conf": {"queryNode.gracefulTime": 5000}}
    cfg = drv.build_install_manifests(InstallSpec(kind="milvus", name="m1"), method, params)[-1]["spec"]["config"]
    assert cfg["queryNode"]["gracefulTime"] == 5000       # _conf routed into spec.config (dotted→nested)
    assert cfg["etcd"]["rootPath"] == "m1"                # isolation still present
```

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_milvus.py -k "isolation_prefix or conf_merged" -v`
Expected: FAIL — no `spec.config`; CR still has `spec.conf`.

- [ ] **Step 3: Add helpers + change the config write in `core/drivers/milvus.py`.** Add these module-level helpers (near the top, after imports):
```python
def _dotted_to_nested(flat: dict) -> dict:
    """{'a.b.c': v} -> {'a': {'b': {'c': v}}}; keys without '.' are kept as-is."""
    out: dict = {}
    for k, v in (flat or {}).items():
        parts = str(k).split(".")
        cur = out
        for p in parts[:-1]:
            cur = cur.setdefault(p, {})
        cur[parts[-1]] = v
    return out


def _deep_merge(a: dict, b: dict) -> dict:
    """Recursive dict merge; b wins on scalar conflicts."""
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out
```
Then in `build_install_manifests`, replace the block:
```python
        cr_spec = {"mode": mode, "components": components, "dependencies": deps}
        conf = params.get("_conf")
        if conf:
            cr_spec["conf"] = {"data": conf}
```
with:
```python
        cr_spec = {"mode": mode, "components": components, "dependencies": deps}
        prefix = params.get("isolationPrefix") or name
        iso = {"msgChannel": {"chanNamePrefix": {"cluster": prefix}},
               "etcd": {"rootPath": prefix}, "minio": {"bucketName": prefix}}
        config = _deep_merge(_dotted_to_nested(params.get("_conf") or {}), iso)
        if config:
            cr_spec["config"] = config
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_milvus.py -v && python -m pytest -q`
Expected: PASS. (The existing `test_milvus_build_manifests_all_external` etc. still pass — they don't assert on `spec.conf`.)

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/drivers/milvus.py milvus-bootstrap/tests/test_milvus.py
git commit -m "fix(milvus): inject isolation prefix + route _conf into spec.config (drop dead spec.conf)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: dry-run checks — duplicate name + prefix-collision-on-shared-deps

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/engines/provisioner.py`
- Test: `milvus-bootstrap/tests/test_milvus.py` (append)

**Interfaces:**
- Produces: `check_milvus_install(instances: list, spec) -> None` (raises `ValueError`); `_dep_eps(params: dict) -> set[str]`. Called from `provisioner.install` for milvus.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_milvus.py`:
```python
def test_milvus_dup_name_rejected(core: Core) -> None:
    import pytest
    core.install(InstallSpec(kind="milvus", name="mv", params={
        "mq": "kafka", "kafkaBrokers": "kafka-dev.default.svc:9092"}), dry_run=False)
    with pytest.raises(ValueError, match="已存在"):
        core.install(InstallSpec(kind="milvus", name="mv", params={"mq": "kafka"}), dry_run=True)


def test_milvus_prefix_collision_on_shared_dep(core: Core) -> None:
    import pytest
    core.install(InstallSpec(kind="milvus", name="mv-a", params={
        "mq": "kafka", "kafkaBrokers": "kafka-x.default.svc:9092", "isolationPrefix": "shared"}), dry_run=False)
    # different name, SAME custom prefix, SAME kafka endpoint → collision
    with pytest.raises(ValueError, match="隔离前缀"):
        core.install(InstallSpec(kind="milvus", name="mv-b", params={
            "mq": "kafka", "kafkaBrokers": "kafka-x.default.svc:9092", "isolationPrefix": "shared"}), dry_run=True)
    # same prefix but a DIFFERENT (non-shared) endpoint → allowed
    core.install(InstallSpec(kind="milvus", name="mv-c", params={
        "mq": "kafka", "kafkaBrokers": "kafka-y.default.svc:9092", "isolationPrefix": "shared"}), dry_run=True)
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_milvus.py -k "dup_name or prefix_collision" -v`
Expected: FAIL — no such checks; installs proceed.

- [ ] **Step 3: Add the check + call in `core/engines/provisioner.py`.** Add module-level functions (near the top, after imports):
```python
def _dep_eps(params: dict) -> set:
    """The dependency endpoint strings a milvus install binds to, as a set."""
    eps: set = set()
    etcd = params.get("etcdEndpoints")
    if isinstance(etcd, (list, tuple)):
        eps.update(str(e) for e in etcd)
    elif etcd:
        eps.add(str(etcd))
    for key in ("storageEndpoint", "pulsarEndpoint"):
        if params.get(key):
            eps.add(str(params[key]))
    kb = params.get("kafkaBrokers")
    if isinstance(kb, (list, tuple)):
        eps.update(str(e) for e in kb)
    elif kb:
        eps.add(str(kb))
    return eps


def check_milvus_install(instances: list, spec) -> None:
    """Reject a milvus install that duplicates a name or collides on (prefix, shared dep)."""
    if any(i.name == spec.name for i in instances):
        raise ValueError(f"实例名 {spec.name} 已存在，请换名")
    new_prefix = spec.params.get("isolationPrefix") or spec.name
    new_eps = _dep_eps(spec.params)
    for i in instances:
        snap = i.spec_snapshot or {}
        if snap.get("kind") != "milvus":
            continue
        p = snap.get("params", {}) or {}
        eff = p.get("isolationPrefix") or i.name
        if eff == new_prefix and (_dep_eps(p) & new_eps):
            raise ValueError(
                f"隔离前缀 {new_prefix} 已被 milvus {i.name} 在共享依赖上占用，请改前缀")
```
Then in `provisioner.install`, at the very top of the `if spec.kind == "milvus":` block (before `compat.gate`), insert:
```python
        if spec.kind == "milvus":
            check_milvus_install(self.state.list_instances(), spec)
            from .. import compat
            ...
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_milvus.py -v && python -m pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/engines/provisioner.py milvus-bootstrap/tests/test_milvus.py
git commit -m "feat(provisioner): dry-run checks for milvus name dup + prefix-on-shared-dep collision

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Milvus install form — dependency dropdowns

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: `/api/instances`, `depEndpoint`, `esc`.
- Produces: `fillParams('milvus')` renders a structured form (image + etcd/storage/mq selects with custom fallback); `collectParams()` gathers milvus params from it; helpers `loadInstances()`, `depOptions(instances,kind)`, `selVal(selId,custId)`.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_install_milvus_form_has_dep_dropdowns(client):
    js = client.get("/assets/web.js").text
    for m in ['function depOptions', '__custom__', 'inst-etcd', 'inst-storage', 'inst-mqtype', 'inst-mq', 'function selVal']:
        assert m in js, m
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k milvus_form_has_dep -v`
Expected: FAIL — form still uses the generic key=value editor for milvus.

- [ ] **Step 3: Add the dropdown machinery to `web.js`.** Add helpers (near the other install helpers):
```javascript
let INSTANCES_CACHE = null;
async function loadInstances() {
  if (INSTANCES_CACHE) return INSTANCES_CACHE;
  try { INSTANCES_CACHE = (await getJSON('api/instances')).instances; }
  catch (e) { INSTANCES_CACHE = []; }
  return INSTANCES_CACHE;
}
function depOptions(instances, kind) {
  const opts = instances.filter(i => i.kind === kind).map(i => {
    const ep = depEndpoint(kind, i.name, i.namespace);
    return `<option value="${esc(ep)}">${esc(i.name)} (${esc(i.namespace)})</option>`;
  }).join('');
  return opts + '<option value="__custom__">自定义…</option>';
}
function selVal(selId, custId) {
  const s = document.getElementById(selId);
  if (!s) return '';
  if (s.value === '__custom__') { const c = document.getElementById(custId); return c ? c.value.trim() : ''; }
  return s.value;
}
function wireCustom(selId, custId) {
  const s = document.getElementById(selId), c = document.getElementById(custId);
  if (!s || !c) return;
  const sync = () => { c.style.display = s.value === '__custom__' ? '' : 'none'; };
  s.onchange = sync; sync();
}
```
Make `fillParams` async and add a milvus branch (keep the existing generic-rows branch for non-milvus). Replace `fillParams`:
```javascript
async function fillParams(kind) {
  const box = document.getElementById('inst-params');
  if (kind !== 'milvus') {
    box.innerHTML = '';
    const d = INSTALL_DEFAULTS[kind] || {};
    Object.entries(d).forEach(([k, v]) => box.appendChild(paramRow(k, v)));
    return;
  }
  const insts = await loadInstances();
  const mqInst = kind => `<select id="inst-mq"><option value="">—</option>${depOptions(insts, kind)}</select>`
    + `<input id="inst-mq-custom" class="f-in" placeholder="host:port" style="display:none">`;
  box.innerHTML =
    `<div class="mv-form">` +
    `<label>镜像</label><input id="inst-image" class="f-in" value="milvusdb/milvus:v2.6.18">` +
    `<label>etcd 依赖</label><select id="inst-etcd">${depOptions(insts, 'etcd')}</select>` +
    `<input id="inst-etcd-custom" class="f-in" placeholder="etcd.default.svc:2379" style="display:none">` +
    `<label>存储依赖</label><select id="inst-storage">${depOptions(insts, 'minio')}</select>` +
    `<input id="inst-storage-custom" class="f-in" placeholder="minio.default.svc:80" style="display:none">` +
    `<label>MQ 类型</label><select id="inst-mqtype">` +
    ['kafka', 'pulsar', 'woodpecker-service', 'woodpecker-embedded', 'rocksmq'].map(o => `<option value="${o}">${o}</option>`).join('') +
    `</select>` +
    `<div id="inst-mqinst-row"><label>MQ 实例</label>${mqInst('kafka')}</div>` +
    `</div>`;
  wireCustom('inst-etcd', 'inst-etcd-custom');
  wireCustom('inst-storage', 'inst-storage-custom');
  const mqtype = document.getElementById('inst-mqtype');
  const row = document.getElementById('inst-mqinst-row');
  const syncMq = () => {
    const t = mqtype.value;
    if (t === 'kafka' || t === 'pulsar') {
      row.style.display = '';
      row.innerHTML = `<label>MQ 实例</label>${mqInst(t)}`;
      wireCustom('inst-mq', 'inst-mq-custom');
    } else { row.style.display = 'none'; }
  };
  mqtype.onchange = syncMq; syncMq();
}
```
Replace `collectParams` to branch on kind:
```javascript
function collectParams() {
  if (document.getElementById('inst-kind').value !== 'milvus') {
    const out = {};
    document.querySelectorAll('#inst-params .prow').forEach(r => {
      const k = r.querySelector('.pk').value.trim();
      if (k) out[k] = r.querySelector('.pv').value.trim();
    });
    return out;
  }
  const p = { image: (document.getElementById('inst-image') || {}).value || '' };
  const etcd = selVal('inst-etcd', 'inst-etcd-custom'); if (etcd) p.etcdEndpoints = etcd;
  const store = selVal('inst-storage', 'inst-storage-custom'); if (store) p.storageEndpoint = store;
  const mq = (document.getElementById('inst-mqtype') || {}).value || 'kafka';
  p.mq = mq;
  if (mq === 'kafka') { const v = selVal('inst-mq', 'inst-mq-custom'); if (v) p.kafkaBrokers = v; }
  if (mq === 'pulsar') { const v = selVal('inst-mq', 'inst-mq-custom'); if (v) p.pulsarEndpoint = v; }
  return p;
}
```
In `renderInstall`, the `sel.onchange` and initial call must await the now-async `fillParams` — change to:
```javascript
  sel.onchange = () => { fillParams(sel.value); };
  fillParams(sel.value);
```
(fillParams returns a promise; fire-and-forget is fine here.)

- [ ] **Step 4: Verify JS + run test + full suite**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; PASS.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): Milvus install form dependency dropdowns (+ custom fallback)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Isolation-prefix field (mirror instance name) + send `isolationPrefix`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js`
- Test: `milvus-bootstrap/tests/test_web_static.py` (append)

**Interfaces:**
- Consumes: the milvus form from Task 3.
- Produces: milvus form gains `#inst-iso`; `collectParams` (milvus) sends `isolationPrefix`; instance-name input mirrors into the prefix until the prefix is edited.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_static.py`:
```python
def test_install_milvus_isolation_prefix(client):
    js = client.get("/assets/web.js").text
    assert "inst-iso" in js and "isolationPrefix" in js
    assert "isoDirty" in js or "dataset.dirty" in js       # mirror-until-edited flag
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_static.py -k isolation_prefix -v`
Expected: FAIL — no isolation field.

- [ ] **Step 3: Add the isolation field to the milvus form in `fillParams` (Task 3's milvus branch).** Insert the field into the `box.innerHTML` template, right after the 镜像 input line:
```javascript
    `<label>数据隔离前缀 <span class="muted" style="font-weight:400">(默认=实例名，共用依赖时用它隔离 topic/rootPath/bucket)</span></label>` +
    `<input id="inst-iso" class="f-in" placeholder="默认=实例名">` +
```
At the END of `fillParams`'s milvus branch (after `syncMq(); ...`), add the mirror wiring:
```javascript
  const nameEl = document.getElementById('inst-name');
  const isoEl = document.getElementById('inst-iso');
  let isoDirty = false;
  isoEl.value = nameEl.value.trim();
  isoEl.oninput = () => { isoDirty = true; };
  nameEl.oninput = () => { if (!isoDirty) isoEl.value = nameEl.value.trim(); };
```
In `collectParams` (milvus branch), add before `return p;`:
```javascript
  const iso = (document.getElementById('inst-iso') || {}).value;
  p.isolationPrefix = (iso && iso.trim()) || (document.getElementById('inst-name').value.trim());
```

- [ ] **Step 4: Verify JS + run test + full suite**

Run: `node --check src/milvus_bootstrap/webui/assets/web.js && python -m pytest tests/test_web_static.py -v && python -m pytest -q`
Expected: JS OK; PASS.

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/webui/assets/web.js milvus-bootstrap/tests/test_web_static.py
git commit -m "feat(webui): Milvus install data-isolation prefix field (mirrors instance name)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`mb web --port 8090` (live cluster), open `/install.html`, pick **milvus**:
- 镜像 + 数据隔离前缀 (auto-fills as you type the name; editable) + etcd/存储/MQ dropdowns listing the real instances (+ 自定义 reveals a text box).
- dry-run a duplicate name (`milvus-dev`) → error "实例名 … 已存在"; dry-run a fresh name → step preview.
- Actually install a throwaway (e.g. name `milvus-t`, kafka `kafka-dev`) then `kubectl get cm milvus-t -o jsonpath='{.data.user\.yaml}'` → shows `cluster/rootPath/bucketName: milvus-t`. Delete it after.

## Self-Review

- **Spec coverage:** D2 prefix semantics/mirror → Task 4; D3 inject spec.config → Task 1; D4 _conf routing + drop spec.conf → Task 1; D5 dep dropdowns + custom → Task 3; D6 dry-run checks → Task 2; D7 state-based, non-milvus unchanged → Tasks 2,3. §4 dropdowns → Task 3; §5 injection → Task 1; §6 checks → Task 2. §7 tests → each task.
- **Placeholder scan:** every step has complete code; frontend via content-marker + manual DoD (stated). No TBD.
- **Type consistency:** `params` keys `isolationPrefix`/`etcdEndpoints`/`storageEndpoint`/`kafkaBrokers`/`pulsarEndpoint`/`mq`/`image` consistent across Task 1 (driver reads), Task 2 (`_dep_eps` reads), Tasks 3-4 (frontend writes). `_dotted_to_nested`/`_deep_merge` (Task 1); `check_milvus_install`/`_dep_eps` (Task 2); `loadInstances`/`depOptions`/`selVal`/`wireCustom` (Task 3); `#inst-iso`/`isoDirty` (Task 4). Injection target `spec.config` consistent with the verified operator behavior.
- **Existing-behavior note:** `_conf` now lands in `spec.config` (was the dead `spec.conf`); config-get untouched (out of scope, flagged). Non-milvus install unaffected (fillParams/collectParams branch on kind).
