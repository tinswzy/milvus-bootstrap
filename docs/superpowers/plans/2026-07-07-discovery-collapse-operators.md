# Discovery collapse — exclude operators + merge managed sub-workloads

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Fix `/api/instances` over-reporting on real clusters: stop identifying operator workloads as component instances, and collapse a managed instance's discovered sub-workloads (e.g. `pulsar-dev-bookie`) back into their managed parent.

**Architecture:** Two small changes. (Part B) `BaseServiceDriver.detect` rejects any workload whose image name component contains "operator" — global, benefits discover/adopt too. (Part A) `/api/instances` suppresses an external candidate whose name equals or is a `<managed-name>-…` segment child of a managed instance of the same kind+namespace.

**Tech Stack:** Python 3.11, FastAPI, pytest+TestClient.

## Global Constraints

- **operator exclusion** in `BaseServiceDriver.detect` (`core/drivers/base.py`) — single point, no driver overrides it. Reject if any space-joined image's **name component** (after stripping registry path, tag, digest) contains `"operator"`: `milvusdb/milvus-operator`→`milvus-operator`→reject; `quay.io/minio/operator:v7.1.1`→`operator`→reject; `milvusdb/milvus:v2.6.18`→`milvus`→keep; `quay.io/minio/minio:latest`→`minio`→keep. Component server images never contain "operator", so no false drops.
- **sub-workload merge** in `/api/instances` (`server/app.py`): an external Candidate is skipped if, for its `(kind, namespace)`, some managed instance name `mn` satisfies `name == mn or name.startswith(mn + "-")` (segment boundary — `etcd` must not swallow `etcdkeeper`; `etcd-0`/`pulsar-dev-bookie` still match). This is IN ADDITION to the existing exact `(kind,name,ns)` dedup.
- **Row schema unchanged** — these changes only reduce rows, never alter `{name,kind,namespace,ownership,image,image_id,status,deps}`.
- **Defer**: grouping genuinely-external multi-workload installs into one logical instance (no live case).
- Tests hermetic (MB_ADAPTER=fake). Run from `milvus-bootstrap/` with `source .venv/bin/activate`.
- Branch `feat/discovery-collapse` off main. End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

Verified shapes:
- `BaseServiceDriver.detect(evidence)` (`core/drivers/base.py:49-58`): computes `img = str(evidence.get("image","")).lower()`, then matches `profile.detect.image_match` substrings / helm chart / crd. milvus profile `image_match: ["milvusdb/milvus"]` (so `milvusdb/milvus-operator` currently matches); minio `["minio"]` (so `quay.io/minio/operator` currently matches).
- Tests reach the registry via a `Core()` fixture: `core.registry.find_for(evidence) -> ServiceDriver | None` (driver has `.kind`). Pattern exists in `tests/test_vertical_slice.py` (has a `core` fixture, tests `core.registry`).
- Fake cluster (`_FAKE_CLUSTER`): `milvus-etcd`(etcd, default, image milvusdb/etcd), `etcd`(kube-system, excluded), `milvus-minio`(minio, default). No operator entries — do NOT modify `_FAKE_CLUSTER`.
- `/api/instances` (`server/app.py`) has a managed loop (populates `seen`) then an external loop over `core.discovery.discover()` with `if key in seen: continue`. `_INSTANCE_KINDS` module constant exists.

---

### Task 1: Part B — exclude operator workloads in `detect`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/drivers/base.py`
- Test: `milvus-bootstrap/tests/test_vertical_slice.py` (append)

**Interfaces:**
- Produces: `BaseServiceDriver.detect` returns False for operator-image workloads → `registry.find_for(operator-evidence)` returns None.

- [ ] **Step 1: Write the failing test** — append to `tests/test_vertical_slice.py` (it already has a `core: Core` fixture):
```python
def test_operators_not_identified_as_instances(core) -> None:
    # operator workloads must NOT be claimed by any component driver
    assert core.registry.find_for({"image": "milvusdb/milvus-operator:v1.3.6", "labels": {}}) is None
    assert core.registry.find_for({"image": "quay.io/minio/operator:v7.1.1", "labels": {}}) is None
    # real component server images are still claimed by the right driver
    assert core.registry.find_for({"image": "milvusdb/milvus:v2.6.18", "labels": {}}).kind == "milvus"
    assert core.registry.find_for({"image": "quay.io/minio/minio:latest", "labels": {}}).kind == "minio"
```

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_vertical_slice.py::test_operators_not_identified_as_instances -v`
Expected: FAIL — `milvusdb/milvus-operator` currently matches the milvus driver (returns a driver, not None).

- [ ] **Step 3: Add the operator guard to `BaseServiceDriver.detect` in `core/drivers/base.py`** — insert right after the `img = ...` line, before the `image_match` check:
```python
    def detect(self, evidence: dict) -> bool:
        img = str(evidence.get("image", "")).lower()
        # operator workloads are not component instances — their image name component
        # contains "operator" (e.g. milvusdb/milvus-operator, quay.io/minio/operator).
        for ref in img.split():
            name_part = ref.split("@")[0].rsplit(":", 1)[0].rsplit("/", 1)[-1]
            if "operator" in name_part:
                return False
        if any(m.lower() in img for m in self.profile.detect.image_match):
            return True
        chart = str(evidence.get("labels", {}).get("helm.sh/chart", ""))
        if self.profile.detect.helm_chart and fnmatch.fnmatch(chart, self.profile.detect.helm_chart):
            return True
        if self.profile.detect.crd and evidence.get("crd") == self.profile.detect.crd:
            return True
        return False
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_vertical_slice.py -v && python -m pytest -q`
Expected: PASS. (Watch for any discovery/ownership test that relied on an operator image being adoptable — none expected, since `_FAKE_CLUSTER` has no operator entries; if one appears, report it.)

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/drivers/base.py milvus-bootstrap/tests/test_vertical_slice.py
git commit -m "fix(discovery): do not identify operator workloads as component instances

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Part A — collapse managed sub-workloads in `/api/instances`

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py`
- Test: `milvus-bootstrap/tests/test_web_endpoints.py` (append)

**Interfaces:**
- Consumes: existing `api_instances` managed/external loops + `seen`.
- Produces: external candidates that are sub-workloads of a managed instance (same kind+ns, name prefixed by `<managed>-`) are omitted.

- [ ] **Step 1: Write the failing test** — append to `tests/test_web_endpoints.py`:
```python
def test_api_instances_collapses_managed_subworkloads(client):
    from milvus_bootstrap.core.models import InstallSpec
    from milvus_bootstrap.server import app as app_module
    # managed etcd named "milvus" → the fake-cluster workload "milvus-etcd" (etcd, default)
    # is a "milvus-" segment child → must be collapsed (not shown as a separate external).
    app_module.core.install(InstallSpec(kind="etcd", name="milvus"), dry_run=False)
    rows = client.get("/api/instances").json()["instances"]
    by = {(r["kind"], r["name"]) for r in rows}
    assert ("etcd", "milvus") in by                    # the managed parent
    assert ("etcd", "milvus-etcd") not in by           # its discovered sub-workload, collapsed
    # an unrelated external of a different kind (no managed prefix) still shows
    assert ("minio", "milvus-minio") in by
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_web_endpoints.py::test_api_instances_collapses_managed_subworkloads -v`
Expected: FAIL — `("etcd","milvus-etcd")` currently appears as a separate external row.

- [ ] **Step 3: Edit `api_instances` in `server/app.py`:**
  - Before the loops, initialize the managed-name index:
    ```python
    managed_names: dict[tuple, list] = {}
    ```
    (place it next to the existing `out = []` / `seen = set()` initialization.)
  - In the MANAGED loop, right where `seen.add((kind, i.name, ns))` is called, also record the name:
    ```python
    managed_names.setdefault((kind, ns), []).append(i.name)
    ```
  - In the EXTERNAL loop, replace the existing `if key in seen: continue` guard with the sub-workload-aware guard:
    ```python
    mnames = managed_names.get((c.kind, ns), ())
    if key in seen or any(c.name == mn or c.name.startswith(mn + "-") for mn in mnames):
        continue
    ```
  (Everything else in the loop — `seen.add(key)`, image/status/deps enrichment — stays.)

- [ ] **Step 4: Run to verify pass + full suite**

Run: `python -m pytest tests/test_web_endpoints.py -v && python -m pytest -q`
Expected: PASS (existing `test_api_instances_empty_state_shows_externals` still passes — with empty state there is no managed name to suppress `milvus-etcd`, so it still appears as external).

- [ ] **Step 5: Commit**
```bash
git add milvus-bootstrap/src/milvus_bootstrap/server/app.py milvus-bootstrap/tests/test_web_endpoints.py
git commit -m "fix(server): collapse managed instance sub-workloads in /api/instances

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Post-plan manual check (DoD)

`mb web --port 8090` (live cluster), open `http://127.0.0.1:8090/api/instances`:
- Exactly the managed instances appear (etcd, kafka-dev, milvus-dev, milvus-pulsar, minio, pulsar-dev) — no `*-standalone`/`*-bookie`/`*-controller`/`*-pool-0` sub-workload rows, no `*-operator` rows.
- Milvus 页 / Dependencies 页 each show one row per real instance.

## Self-Review

- **Spec coverage:** D1/D2 operator exclusion in `detect` (name component, single point) → Task 1; D3/D4 sub-workload merge (endpoint, segment prefix, same kind+ns) → Task 2; D5 defer external grouping → no task (out of scope, stated). §4 → Task 1; §5 → Task 2; §6 tests → both (Task 1 registry.find_for; Task 2 endpoint collapse + regression note).
- **Placeholder scan:** every step has complete code; no TBD/TODO.
- **Type consistency:** `registry.find_for(evidence) -> driver|None` (Task 1); `managed_names: dict[(kind,ns), list[name]]` populated in managed loop, read in external loop (Task 2); `_INSTANCE_KINDS`/`seen`/`c.evidence`/`ns` consistent with the existing endpoint. `detect` signature unchanged.
- **Regression note (called out):** Task 1 — no `_FAKE_CLUSTER` operator entries, so discovery/ownership tests unaffected; Task 2 — `test_api_instances_empty_state_shows_externals` still valid (empty state → nothing to collapse).
