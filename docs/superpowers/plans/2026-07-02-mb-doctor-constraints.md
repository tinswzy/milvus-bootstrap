# mb doctor — populate researched version constraints + wire install/upgrade gating

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Fill the `compat.yaml` matrix with the constraints researched from milvus.io/docs (soft semver floors + the conditional woodpecker-minio date rule + upgrade-path rules), add the logic those need (minio date-versions, upgrade paths, CPU SIMD), expand version detection, and wire `gate()`/`--force` into install and upgrade.

**Architecture:** Extends the existing `core/compat.py` (evaluate/gate), `core/probe.py` (detection), `core/doctor.py` (env checks), and the install/upgrade paths (`context.py`, `engines/lifecycle.py`, `server/app.py`, `cli/main.py`). All gating stays server-side; doctor stays client-side.

**Tech Stack:** Python 3.11, typer, rich, PyYAML, pytest. No new deps.

## Global Constraints

Researched authoritative values (from milvus.io/docs + zilliztech/milvus-operator) — copy verbatim:
- **SOFT semver floors:** k8s ≥ `1.16`; Helm ≥ `3.0.0`; etcd ≥ `3.5.0`; Pulsar ≥ `2.8.2`. Kafka has NO documented floor (do not add one). These are `severity: soft`, `source: docs-tested`.
- **HARD woodpecker+minio:** when milvus ≥ `2.6.0` uses woodpecker with a MinIO backend, MinIO must be ≥ `RELEASE.2024-12-18T13-15-44Z` (S3 Conditional Write). Enforced HARD only in `gate("install")` when the chosen mq is a woodpecker option; shown as SOFT info in the doctor matrix (no per-instance mq context there).
- **HARD upgrade paths:** upgrading a milvus whose target ≥ `2.6.10` requires current ≥ `2.5.16`; target ≥ `3.0.0` requires current ≥ `2.5.16` (mixCoordinator note). `severity: hard`.
- **MQ-during-upgrade freeze (2.6.9):** documented only — do NOT implement as a gate (note in compat.yaml comment).
- **CPU SIMD** (SSE4.2/AVX/AVX2/AVX-512): doctor ENV check on the LOCAL host's `/proc/cpuinfo` (best-effort proxy for nodes) — FAIL if readable and none present, SKIP if unreadable/non-Linux.
- **MQ↔milvus rules** (woodpecker-embedded ≥2.6, woodpecker-service ≥3.0, kafka/pulsar/rocksmq ≥2.0) already live in `compat.py` `MQ_OPTIONS` — do NOT duplicate.
- Finding levels exactly PASS|WARN|FAIL|SKIP; severity hard|soft. Gating: hard→CompatError unless `force`; soft/unknown→WARN.
- Run tests from `milvus-bootstrap/` with `source .venv/bin/activate`.
- Work on branch `feat/mb-doctor-constraints` (already created off main). Each task commits its own files (repo-root-relative git paths). End commit messages with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

The CURRENT `compat.py` already provides: `Finding`, `Constraint(component,requires,rule,milvus_range,min,max,severity,source,reason)`, `parse_version`, `_cmp`, `_NEWEST`, `version_in_range`, `version_ok`, `load_constraints`, `evaluate`, `get_option`, `check`, `CompatError`, `gate(op,ctx,force)`. `probe.py` provides `run_kubectl`, `DetectedVersions(k8s,operator,minio,kafka,pulsar,milvus)`, `detect_versions`. `doctor.py` provides `DoctorReport`, `check_environment(run,no_proxy,daemon_up)`, `tool_info`, `run`.

---

### Task 1: Fill compat.yaml with soft semver floors

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.yaml`
- Test: `milvus-bootstrap/tests/test_compat.py` (append)

**Interfaces:** consumes existing `load_constraints`/`evaluate`. Produces no new code — data only.

- [ ] **Step 1: Write the failing test** — append to `tests/test_compat.py`:

```python
def test_yaml_has_soft_semver_floors():
    cons = {(c.component, c.min): c for c in _c.load_constraints()}
    etcd = next(c for (comp, _), c in cons.items() if comp == "etcd")
    assert etcd.min == "3.5.0" and etcd.severity == "soft"
    pulsar = next(c for (comp, _), c in cons.items() if comp == "pulsar")
    assert pulsar.min == "2.8.2" and pulsar.severity == "soft"
    k8s = next(c for (comp, _), c in cons.items() if comp == "k8s")
    assert k8s.min == "1.16.0" and k8s.severity == "soft"
    helm = next(c for (comp, _), c in cons.items() if comp == "helm")
    assert helm.min == "3.0.0" and helm.severity == "soft"


def test_evaluate_soft_floor_warns_when_below():
    cons = _c.load_constraints()
    out = _c.evaluate({"milvus": "2.6.18", "etcd": "3.4.0"}, cons)
    etcd = [f for f in out if f.component == "etcd"]
    assert etcd and etcd[0].level == "WARN"   # soft floor, below min -> WARN not FAIL
    out2 = _c.evaluate({"milvus": "2.6.18", "etcd": "3.5.25"}, cons)
    assert [f for f in out2 if f.component == "etcd"][0].level == "PASS"
```

- [ ] **Step 2: Run to verify fail**

Run: `source .venv/bin/activate && python -m pytest tests/test_compat.py -k "soft_semver_floors or soft_floor" -v`
Expected: FAIL — no etcd/pulsar/k8s/helm constraints yet.

- [ ] **Step 3: Append to `core/compat.yaml`** (after the existing `constraints:` entries, same indentation):

```yaml
  # --- SOFT semver floors (docs-tested; below => WARN, never block) ---
  - component: etcd
    requires: milvus
    rule: "etcd 版本下限（docs 测试 3.5.0，operator 默认 3.5.25）"
    milvus_range: ""
    min: "3.5.0"
    max: ""
    severity: soft
    source: docs-tested
    reason: "milvus.io/docs prerequisite: etcd 3.5.x"
  - component: pulsar
    requires: milvus
    rule: "pulsar 版本下限（docs 测试 2.8.2，operator 默认 3.0.7，2.x 亦可）"
    milvus_range: ""
    min: "2.8.2"
    max: ""
    severity: soft
    source: docs-tested
    reason: "milvus.io/docs prerequisite: pulsar 2.8.2"
  - component: helm
    requires: milvus
    rule: "Helm 版本下限（≥3.0；用 pulsar-v3 chart 需 ≥3.14）"
    milvus_range: ""
    min: "3.0.0"
    max: ""
    severity: soft
    source: docs-tested
    reason: "prerequisite-helm: Helm 3.0.0+"
```

Then edit the EXISTING `k8s` constraint (the one with empty min) to set its floor:
```yaml
  - component: k8s
    requires: milvus
    rule: "k8s 版本下限（≥1.16）"
    milvus_range: ""
    min: "1.16.0"
    max: ""
    severity: soft
    source: docs-tested
    reason: "prerequisite-helm: Kubernetes 1.16+"
```

Also append a comment documenting the un-implemented rule:
```yaml
# NOTE: "MQ choice is frozen during an upgrade to milvus 2.6.9" is documented but
# intentionally NOT enforced as a gate rule here.
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_compat.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/compat.yaml milvus-bootstrap/tests/test_compat.py
git commit -m "feat(compat): seed soft semver floors (etcd/pulsar/k8s/helm) from milvus docs

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Expand probe to detect etcd/minio/kafka/pulsar versions

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/probe.py`
- Test: `milvus-bootstrap/tests/test_probe.py` (append)

**Interfaces:**
- Consumes: existing `run_kubectl`, `_tag`, `DetectedVersions`.
- Produces: `detect_versions` additionally populates `etcd`, `minio`, `kafka`, `pulsar` by scanning all pod container images and matching known image substrings. Add field `etcd: str | None = None` to `DetectedVersions` and include it in `as_compat_dict()` under key `"etcd"`. For minio keep the FULL tag (e.g. `RELEASE.2024-12-18T13-15-44Z`), not a semver reduction.

- [ ] **Step 1: Write the failing test** — append to `tests/test_probe.py`:

```python
def test_detect_dependency_versions_from_pod_images():
    pods = "\n".join([
        "etcd-0\tdocker.io/bitnamilegacy/etcd:3.5.25",
        "minio-pool-0-0\tminio/minio:RELEASE.2024-12-18T13-15-44Z",
        "kafka-dev-controller-0\tbitnamilegacy/kafka:3.9.0",
        "pulsar-dev-broker-0\tapachepulsar/pulsar:3.0.7",
    ])
    run = _fake_run({
        "version": (0, '{"serverVersion":{"gitVersion":"v1.34.0"}}', ""),
        "get pods": (0, pods, ""),
    })
    dv = probe.detect_versions(run=run)
    assert dv.etcd == "3.5.25"
    assert dv.minio == "RELEASE.2024-12-18T13-15-44Z"   # full tag, not semver
    assert dv.kafka == "3.9.0"
    assert dv.pulsar == "3.0.7"
    d = dv.as_compat_dict()
    assert d["etcd"] == "3.5.25" and d["minio"].startswith("RELEASE.")
```

(Reuse the existing `_fake_run` helper already in the file.)

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_probe.py -k dependency_versions -v`
Expected: FAIL — deps stay None.

- [ ] **Step 3: Implement in `core/probe.py`**

Add `etcd: str | None = None` to the `DetectedVersions` dataclass (next to `k8s`). In `as_compat_dict()` add:
```python
        if self.etcd:
            d["etcd"] = self.etcd
```
Add a helper and a detection pass in `detect_versions` (after the milvus detection):
```python
def _image_tag(image: str) -> str | None:
    """Return the raw tag after ':' (keeps minio RELEASE.* verbatim)."""
    if not image or ":" not in image:
        return None
    return image.rsplit(":", 1)[-1].split("@", 1)[0] or None


_DEP_MATCH = [("etcd", "etcd"), ("minio", "minio"), ("kafka", "kafka"), ("pulsar", "pulsar")]
```
```python
    rc, out, _ = run(["get", "pods", "-A",
                      "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}"
                      "{.spec.containers[0].image}{\"\\n\"}{end}"])
    if rc == 0:
        for line in out.splitlines():
            if "\t" not in line:
                continue
            _name, image = line.split("\t", 1)
            low = image.lower()
            for field, needle in _DEP_MATCH:
                if needle in low and getattr(dv, field) is None:
                    tag = _image_tag(image)
                    if field == "minio":
                        setattr(dv, field, tag)               # keep RELEASE.* verbatim
                    else:
                        setattr(dv, field, _tag(image) or tag)  # semver-reduce others
    return dv
```
(Ensure the `return dv` at the end is the single final return — remove the earlier bare `return dv` if the new block is inserted before it; the function must end with one `return dv`.)

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_probe.py -v`
Expected: PASS (new + existing).

- [ ] **Step 5: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/probe.py milvus-bootstrap/tests/test_probe.py
git commit -m "feat(probe): detect etcd/minio/kafka/pulsar versions from pod images

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: MinIO date-version comparison + woodpecker-minio constraint

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.py`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.yaml`
- Test: `milvus-bootstrap/tests/test_compat.py` (append)

**Interfaces:**
- Produces: `parse_minio_release(s: str) -> tuple[int,...] | None` (parses `RELEASE.YYYY-MM-DDTHH-MM-SSZ`); `minio_release_ok(version, min_v, max_v) -> bool | None`; new `Constraint.kind: str = "semver"` field; `evaluate` uses `minio_release_ok` when `c.kind == "minio-release"`.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_compat.py`:

```python
def test_parse_minio_release():
    assert _c.parse_minio_release("RELEASE.2024-12-18T13-15-44Z") == (2024, 12, 18, 13, 15, 44)
    assert _c.parse_minio_release("3.5.0") is None


def test_minio_release_ok():
    m = "RELEASE.2024-12-18T13-15-44Z"
    assert _c.minio_release_ok("RELEASE.2025-01-01T00-00-00Z", m, "") is True
    assert _c.minio_release_ok("RELEASE.2024-06-01T00-00-00Z", m, "") is False
    assert _c.minio_release_ok("RELEASE.2024-12-18T13-15-44Z", m, "") is True
    assert _c.minio_release_ok("garbage", m, "") is None   # unparseable -> unknown


def test_evaluate_minio_release_kind():
    cons = [_c.Constraint("minio", "milvus", "wp+minio S3 conditional write",
                          ">=2.6.0", "RELEASE.2024-12-18T13-15-44Z", "", "soft",
                          "docs-tested", "", kind="minio-release")]
    below = _c.evaluate({"milvus": "2.6.18", "minio": "RELEASE.2024-06-01T00-00-00Z"}, cons)
    assert below and below[0].level == "WARN"      # soft
    ok = _c.evaluate({"milvus": "2.6.18", "minio": "RELEASE.2025-01-01T00-00-00Z"}, cons)
    assert ok[0].level == "PASS"
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_compat.py -k "minio_release or minio_release_kind" -v`
Expected: FAIL — no `parse_minio_release`, `Constraint.kind`.

- [ ] **Step 3: Implement in `core/compat.py`**

Add `kind: str = "semver"` as the LAST field of the `Constraint` dataclass (default keeps existing constructions valid). Update `load_constraints` to read it: add `kind=c.get("kind", "semver"),` to the `Constraint(...)` call. Add the functions (near `version_ok`):
```python
def parse_minio_release(s: str) -> tuple[int, int, int, int, int, int] | None:
    m = re.match(r"RELEASE\.(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})Z", s or "")
    return tuple(int(m[i]) for i in range(1, 7)) if m else None


def minio_release_ok(version: str, min_v: str, max_v: str) -> bool | None:
    if not min_v and not max_v:
        return None
    cur = parse_minio_release(version)
    if cur is None:
        return None                     # unparseable -> unknown (don't false-fail)
    if min_v:
        mn = parse_minio_release(min_v)
        if mn and cur < mn:
            return False
    if max_v:
        mx = parse_minio_release(max_v)
        if mx and cur > mx:
            return False
    return True
```
In `evaluate`, replace the `ok = version_ok(comp_v, c.min, c.max)` line with:
```python
        checker = minio_release_ok if c.kind == "minio-release" else version_ok
        ok = checker(comp_v, c.min, c.max)
```

- [ ] **Step 4: Add the woodpecker-minio constraint to `core/compat.yaml`** (append):

```yaml
  # SOFT here (matrix has no per-instance mq context); HARD is enforced in
  # gate("install") when the chosen mq is a woodpecker option (see gate).
  - component: minio
    requires: milvus
    rule: "woodpecker+minio 需 MinIO ≥ 2024-12-18（S3 Conditional Write）"
    milvus_range: ">=2.6.0"
    min: "RELEASE.2024-12-18T13-15-44Z"
    max: ""
    severity: soft
    source: docs-tested
    reason: "use-woodpecker: self-hosted MinIO RELEASE.2024-12-18T13-15-44Z+"
    kind: minio-release
```

- [ ] **Step 5: Run to verify pass**

Run: `python -m pytest tests/test_compat.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/compat.py milvus-bootstrap/src/milvus_bootstrap/core/compat.yaml milvus-bootstrap/tests/test_compat.py
git commit -m "feat(compat): minio date-release comparison + woodpecker-minio constraint

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Upgrade-path rules

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.py`
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.yaml`
- Test: `milvus-bootstrap/tests/test_compat.py` (append)

**Interfaces:**
- Produces: `load_upgrade_paths(path=None) -> list[dict]` (each `{target_min, requires_current_min, reason}`); `gate("upgrade", {"current": v, "target": v, ...})` folds in upgrade-path checks (hard).

- [ ] **Step 1: Write the failing tests** — append to `tests/test_compat.py`:

```python
def test_gate_upgrade_path_blocks_big_jump():
    with pytest.raises(_c.CompatError):
        _c.gate("upgrade", {"current": "2.5.10", "target": "2.6.18"})   # need >=2.5.16


def test_gate_upgrade_path_ok_when_prereq_met():
    assert _c.gate("upgrade", {"current": "2.5.16", "target": "2.6.18"}) == []


def test_gate_upgrade_path_to_3_0_needs_2_5_16():
    with pytest.raises(_c.CompatError):
        _c.gate("upgrade", {"current": "2.4.9", "target": "3.0.0"})


def test_gate_upgrade_path_force_warns():
    out = _c.gate("upgrade", {"current": "2.5.10", "target": "2.6.18"}, force=True)
    assert any(f.level == "WARN" for f in out)
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_compat.py -k upgrade_path -v`
Expected: FAIL — gate doesn't check upgrade paths yet.

- [ ] **Step 3: Add `upgrade_paths` to `core/compat.yaml`** (top-level, sibling of `constraints:`):

```yaml
upgrade_paths:
  - target_min: "2.6.10"
    requires_current_min: "2.5.16"
    reason: "升级到 milvus 2.6.10+ 前需先在 2.5.16+"
  - target_min: "3.0.0"
    requires_current_min: "2.5.16"
    reason: "升级到 milvus 3.0+ 前需先在 2.5.16+ 并启用 mixCoordinator"
```

- [ ] **Step 4: Implement in `core/compat.py`**

Add loader (near `load_constraints`):
```python
def load_upgrade_paths(path: pathlib.Path | None = None) -> list[dict]:
    data = yaml.safe_load((path or _compat_yaml_path()).read_text()) or {}
    return list(data.get("upgrade_paths", []))
```
In `gate`, inside the `if op in ("install", "upgrade"):` block, BEFORE the `mq = ctx.get("mq")` line, add the upgrade-path check (only for upgrade):
```python
        if op == "upgrade":
            cur, tgt = ctx.get("current", ""), ctx.get("target", "")
            for rule in load_upgrade_paths():
                hits = version_ok(tgt, rule["target_min"], "") if tgt else None
                prereq = version_ok(cur, rule["requires_current_min"], "") if cur else True
                if hits and prereq is False:
                    _block(Finding("FAIL", "milvus", "升级路径",
                                   f"目标 {tgt} 需先升到 {rule['requires_current_min']}+"
                                   f"（当前 {cur}）：{rule['reason']}"))
```

- [ ] **Step 5: Run to verify pass**

Run: `python -m pytest tests/test_compat.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/compat.py milvus-bootstrap/src/milvus_bootstrap/core/compat.yaml milvus-bootstrap/tests/test_compat.py
git commit -m "feat(compat): upgrade-path gate (2.5.16 prereq for 2.6.10/3.0)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: CPU SIMD environment check in doctor

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/doctor.py`
- Test: `milvus-bootstrap/tests/test_doctor.py` (append)

**Interfaces:**
- Produces: `check_cpu_simd(read=<default reads /proc/cpuinfo>) -> Finding`; called inside `check_environment` and appended to its findings. `read()` returns the cpuinfo text or raises → SKIP.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_doctor.py`:

```python
def test_check_cpu_simd_pass_and_fail_and_skip():
    ok = doctor.check_cpu_simd(read=lambda: "flags : fpu vme sse4_2 avx2 ht\n")
    assert ok.level == "PASS" and ok.component == "cpu-simd"
    bad = doctor.check_cpu_simd(read=lambda: "flags : fpu vme ht\n")
    assert bad.level == "FAIL"
    def _boom():
        raise OSError("no /proc")
    assert doctor.check_cpu_simd(read=_boom).level == "SKIP"


def test_check_environment_includes_cpu_simd():
    env = doctor.check_environment(lambda a: (1, "", ""), no_proxy="", daemon_up=False)
    assert any(f.component == "cpu-simd" for f in env)
```

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_doctor.py -k cpu_simd -v`
Expected: FAIL — no `check_cpu_simd`.

- [ ] **Step 3: Implement in `core/doctor.py`**

Add near `tool_info`:
```python
_SIMD_FLAGS = ("sse4_2", "avx", "avx2", "avx512f")


def _read_cpuinfo() -> str:
    with open("/proc/cpuinfo", encoding="utf-8") as fh:
        return fh.read()


def check_cpu_simd(read=_read_cpuinfo) -> Finding:
    try:
        text = read()
    except Exception:
        return Finding("SKIP", "cpu-simd", "CPU SIMD 指令集",
                       "无法读取 /proc/cpuinfo（非 Linux 或受限）")
    flags = text.lower()
    have = [f for f in _SIMD_FLAGS if f in flags]
    if have:
        return Finding("PASS", "cpu-simd", "CPU SIMD 指令集", "本机: " + ", ".join(have))
    return Finding("FAIL", "cpu-simd", "CPU SIMD 指令集",
                   "本机 CPU 缺 SSE4.2/AVX/AVX2/AVX-512（milvus 需其一；注：检测的是本机非集群节点）")
```
In `check_environment`, append `out.append(check_cpu_simd())` before `return out`.

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/test_doctor.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/doctor.py milvus-bootstrap/tests/test_doctor.py
git commit -m "feat(doctor): CPU SIMD instruction-set env check

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Wire gate + --force into install and upgrade (incl woodpecker-minio hard check)

**Files:**
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/compat.py` (woodpecker-minio hard check inside `gate("install")`)
- Modify: `milvus-bootstrap/src/milvus_bootstrap/core/context.py` (`install`, `upgrade` pass-through) and/or `engines/lifecycle.py` (`upgrade`) and `engines/provisioner.py` (`install`)
- Modify: `milvus-bootstrap/src/milvus_bootstrap/server/app.py` (`InstallReq`/`UpgradeReq` add `force`; routes pass it)
- Modify: `milvus-bootstrap/src/milvus_bootstrap/cli/main.py` (`install`, `upgrade` add `--force`)
- Test: `milvus-bootstrap/tests/test_compat.py` + `milvus-bootstrap/tests/test_lifecycle.py` (append)

**Interfaces:**
- `gate("install", ctx, force)` gains: when `ctx["mq"]` starts with `"woodpecker"` and `ctx["versions"].get("minio")` is set and `minio_release_ok(minio, "RELEASE.2024-12-18T13-15-44Z", "")` is False → hard block.
- `Core.upgrade(instance, image, dry_run, force=False)` → calls `gate("upgrade", {"current": <snapshot image>, "target": image, "versions": <detected>}, force)` before planning.
- `Core.install(spec, dry_run, force=False)` → for milvus specs, calls `gate("install", {"mq":.., "image":.., "mode":.., "versions": <detected>}, force)` before planning.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_compat.py` (unit-level for the woodpecker-minio hard rule — no cluster):
```python
def test_gate_install_woodpecker_minio_too_old_blocks():
    with pytest.raises(_c.CompatError):
        _c.gate("install", {"mq": "woodpecker-embedded", "image": "v2.6.18",
                            "versions": {"milvus": "2.6.18",
                                         "minio": "RELEASE.2024-06-01T00-00-00Z"}})


def test_gate_install_woodpecker_minio_new_ok():
    out = _c.gate("install", {"mq": "woodpecker-embedded", "image": "v2.6.18",
                              "versions": {"milvus": "2.6.18",
                                           "minio": "RELEASE.2025-01-01T00-00-00Z"}})
    assert all(f.level != "FAIL" for f in out)


def test_gate_install_kafka_minio_old_not_blocked():
    # non-woodpecker mq => the minio date rule does not hard-block
    out = _c.gate("install", {"mq": "kafka", "image": "v2.6.18",
                              "versions": {"milvus": "2.6.18",
                                           "minio": "RELEASE.2024-06-01T00-00-00Z"}})
    assert all(f.level != "FAIL" for f in out)
```

Append to `tests/test_lifecycle.py` (core-level upgrade gate — mirror the existing fixture that builds a Core with a registered milvus instance; install milvus with `params={"mq":"kafka","image":"milvusdb/milvus:v2.5.10"}` so the snapshot's current image is 2.5.10):
```python
def test_upgrade_blocked_by_upgrade_path(core_milvus_2_5_10):   # fixture: milvus snapshot image v2.5.10
    import pytest
    from milvus_bootstrap.core.compat import CompatError
    with pytest.raises(CompatError):
        core_milvus_2_5_10.upgrade("milvus-dev", "milvusdb/milvus:v2.6.18", dry_run=True)


def test_upgrade_path_force_proceeds(core_milvus_2_5_10):
    task = core_milvus_2_5_10.upgrade("milvus-dev", "milvusdb/milvus:v2.6.18",
                                      dry_run=True, force=True)
    assert task.type == "upgrade"
```
(If no such fixture exists, add one following `tests/test_switch_mq.py`'s `core` fixture pattern but with the 2.5.10 image param.)

- [ ] **Step 2: Run to verify fail**

Run: `python -m pytest tests/test_compat.py -k "woodpecker_minio" tests/test_lifecycle.py -k "upgrade_path or upgrade_blocked" -v`
Expected: FAIL — no woodpecker-minio branch; `upgrade()` has no `force`/gate.

- [ ] **Step 3: Add the woodpecker-minio hard check to `gate` in `core/compat.py`**

Inside `gate`, in the `if op in ("install", "upgrade"):` block, after the MQ `check(...)` try/except, add:
```python
        if op == "install" and (mq or "").startswith("woodpecker"):
            minio_v = ctx.get("versions", {}).get("minio")
            if minio_v and minio_release_ok(minio_v, "RELEASE.2024-12-18T13-15-44Z", "") is False:
                _block(Finding("FAIL", "minio", "woodpecker+minio 版本门",
                               f"woodpecker 需 MinIO ≥ 2024-12-18（当前 {minio_v}）"))
```

- [ ] **Step 4: Thread `force` + gate through core/server/cli**

In `core/context.py` (or the provisioner/lifecycle methods they delegate to):
- `Core.upgrade`: add `force: bool = False`; load the instance snapshot to get current image (`spec.params.get("image","")`), detect versions best-effort (`from . import probe; versions = probe.detect_versions().as_compat_dict()` guarded by `try/except → {}`), then `compat.gate("upgrade", {"current": <current image>, "target": image, "versions": versions}, force=force)` BEFORE planning steps. Pass `force` down to `lifecycle.upgrade` if that's where planning happens (add `force` param there too; the gate call may live in either — put it in the method that has the snapshot/image, i.e. `lifecycle.upgrade`).
- `Core.install`: add `force: bool = False`; for `spec.kind == "milvus"` call `compat.gate("install", {"mq": spec.params.get("mq"), "image": spec.params.get("image",""), "mode": spec.params.get("mode","standalone"), "versions": <detected, guarded>}, force=force)` before planning. (The existing driver-level `compat.check` stays; gate adds the version/woodpecker-minio layer.)

In `server/app.py`: add `force: bool = False` to `InstallReq` and `UpgradeReq`; pass `force=req.force` in the `/install` and `/upgrade` routes.

In `cli/main.py`: add `force: bool = typer.Option(False, "--force", help="跳过兼容门禁（自担风险）")` to the `install` and `upgrade` commands; include `"force": force` in their request JSON bodies.

- [ ] **Step 5: Run tests + full suite**

Run: `python -m pytest tests/test_compat.py tests/test_lifecycle.py -v` then `python -m pytest -q`
Expected: PASS (all, including pre-existing install/upgrade/switch-mq tests). Note: existing upgrade/install tests use images/params that must NOT trip the new gate — verify; if the existing `test_lifecycle` upgrade test uses an image pair that now violates an upgrade path, update that test's images to a compliant pair (e.g. current ≥2.5.16) and note it in the report.

- [ ] **Step 6: Commit**

```bash
git add milvus-bootstrap/src/milvus_bootstrap/core/compat.py \
        milvus-bootstrap/src/milvus_bootstrap/core/context.py \
        milvus-bootstrap/src/milvus_bootstrap/core/engines/lifecycle.py \
        milvus-bootstrap/src/milvus_bootstrap/server/app.py \
        milvus-bootstrap/src/milvus_bootstrap/cli/main.py \
        milvus-bootstrap/tests/test_compat.py milvus-bootstrap/tests/test_lifecycle.py
git commit -m "feat(gate): wire compat gate + --force into install/upgrade (incl woodpecker-minio)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

- **Spec coverage:** SOFT floors → Task 1; dep detection → Task 2; minio date-version + woodpecker-minio → Tasks 3 & 6; upgrade paths → Task 4; CPU SIMD → Task 5; install/upgrade gate wiring + --force → Task 6. MQ-freeze-2.6.9 → documented-only (compat.yaml comment, Task 1). MQ↔milvus rules → unchanged (MQ_OPTIONS).
- **Placeholder scan:** every code step has complete code; Task 6's method-location note ("gate in lifecycle.upgrade vs context.upgrade") is an explicit cross-check against real signatures, not a placeholder — the implementer reads the current method and places the gate where the snapshot/image is available.
- **Type consistency:** `Finding(level,component,rule,reason)` and `Constraint(...,kind="semver")` consistent; `minio_release_ok`/`version_ok` share the `(version,min,max)->bool|None` shape; `gate(op,ctx,force)` signature unchanged (new logic folded in); `DetectedVersions.as_compat_dict()` adds `"etcd"`/`"minio"` keys matching compat.yaml component names.
- **Risk:** Task 6 is the only multi-file integration task; its tests are core-level (no cluster). The `probe.detect_versions()` call inside install/upgrade is guarded so a probe failure degrades to `versions={}` (soft), never crashing the operation.
