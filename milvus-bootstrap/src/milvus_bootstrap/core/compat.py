"""Milvus ↔ MQ/dependency compatibility matrix.

Which message-queue (WAL) options a given Milvus version supports. Unsupported
options are still returned (so the UI/CLI can SHOW them) but marked
not-selectable with a reason — you can see that this version doesn't support
them, but you can't pick them.

Key fact: Milvus 2.6.x supports woodpecker only in EMBEDDED mode; the external
woodpecker LogStore (service mode) needs Milvus >= 3.0 (the master / switch-fix
build). kafka / pulsar / rocksmq work on 2.x.
"""
from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass

import yaml


@dataclass(frozen=True)
class MqOption:
    id: str               # selection id
    wal: str              # target_wal_name for switch-mq
    label: str
    min_milvus: str       # min milvus version "X.Y.Z" ("" = any)
    dep_kind: str | None  # dependency component to install (None = embedded, reuses etcd+minio)
    standalone_only: bool = False
    note: str = ""


MQ_OPTIONS: list[MqOption] = [
    MqOption("woodpecker-embedded", "woodpecker", "Woodpecker（嵌入）", "2.6.0", None,
             note="跑在 milvus 进程内，复用外部 etcd+对象存储；无独立 woodpecker 服务"),
    MqOption("woodpecker-service", "woodpecker", "Woodpecker（独立服务）", "3.0.0", "woodpecker",
             note="独立 LogStore 集群；service 模式仅 milvus≥3.0 / master 支持"),
    MqOption("kafka", "kafka", "Kafka", "2.0.0", "kafka"),
    MqOption("pulsar", "pulsar", "Pulsar", "2.0.0", "pulsar"),
    MqOption("rocksmq", "rocksmq", "RocksMQ（嵌入）", "2.0.0", None, standalone_only=True,
             note="嵌入式，仅 standalone 模式"),
]


def parse_version(image_or_version: str) -> tuple[int, int, int] | None:
    """Extract (X,Y,Z) from 'milvusdb/milvus:v2.6.3' / 'v2.6.3' / '2.6.3'.

    Non-semver tags (master / latest / a dev build) -> None = treat as newest
    (supports everything)."""
    s = image_or_version.rsplit(":", 1)[-1] if ":" in image_or_version else image_or_version
    m = re.match(r"v?(\d+)\.(\d+)\.(\d+)", s)
    return (int(m[1]), int(m[2]), int(m[3])) if m else None


def _ge(version: str, minimum: str) -> bool:
    mn = parse_version(minimum)
    if mn is None:
        return True
    cur = parse_version(version)
    if cur is None:        # master / dev build -> assume newest
        return True
    return cur >= mn


@dataclass(frozen=True)
class Finding:
    level: str        # PASS | WARN | FAIL | SKIP
    component: str
    rule: str
    reason: str


@dataclass(frozen=True)
class Constraint:
    component: str
    requires: str
    rule: str
    milvus_range: str
    min: str
    max: str
    severity: str     # hard | soft
    source: str       # confident | best-effort | user-table
    reason: str


_NEWEST = (9999, 9999, 9999)


def _cmp(a: tuple[int, int, int], b: tuple[int, int, int]) -> int:
    return (a > b) - (a < b)


def version_in_range(version: str, range_str: str) -> bool:
    """True if version satisfies every comma-separated clause (>=,<=,>,<,==).
    Empty range => any. Non-semver (master/dev) => treated as newest."""
    if not range_str.strip():
        return True
    cur = parse_version(version) or _NEWEST
    for clause in range_str.split(","):
        clause = clause.strip()
        m = re.match(r"(>=|<=|>|<|==)\s*(.+)", clause)
        if not m:
            continue
        bound = parse_version(m[2])
        if bound is None:
            continue
        c = _cmp(cur, bound)
        ok = {">=": c >= 0, "<=": c <= 0, ">": c > 0, "<": c < 0, "==": c == 0}[m[1]]
        if not ok:
            return False
    return True


def version_ok(version: str, min_v: str, max_v: str) -> bool | None:
    """None = unknown (no bounds). Else whether version is within [min_v, max_v]."""
    if not min_v and not max_v:
        return None
    cur = parse_version(version) or _NEWEST
    if min_v:
        mn = parse_version(min_v)
        if mn and _cmp(cur, mn) < 0:
            return False
    if max_v:
        mx = parse_version(max_v)
        if mx and _cmp(cur, mx) > 0:
            return False
    return True


def _compat_yaml_path() -> pathlib.Path:
    return pathlib.Path(__file__).with_name("compat.yaml")


def load_constraints(path: pathlib.Path | None = None) -> list[Constraint]:
    data = yaml.safe_load((path or _compat_yaml_path()).read_text()) or {}
    out: list[Constraint] = []
    for c in data.get("constraints", []):
        out.append(Constraint(
            component=c["component"], requires=c.get("requires", "milvus"),
            rule=c.get("rule", ""), milvus_range=c.get("milvus_range", ""),
            min=str(c.get("min", "") or ""), max=str(c.get("max", "") or ""),
            severity=c.get("severity", "soft"), source=c.get("source", "user-table"),
            reason=c.get("reason", "") or "",
        ))
    return out


def evaluate(versions: dict, constraints: list[Constraint] | None = None) -> list[Finding]:
    constraints = load_constraints() if constraints is None else constraints
    milvus_v = versions.get("milvus", "")
    out: list[Finding] = []
    for c in constraints:
        if c.requires == "milvus" and milvus_v and not version_in_range(milvus_v, c.milvus_range):
            continue
        comp_v = versions.get(c.component)
        if not comp_v:
            out.append(Finding("SKIP", c.component, c.rule, "版本未探测到"))
            continue
        ok = version_ok(comp_v, c.min, c.max)
        if ok is None:
            out.append(Finding("WARN", c.component, c.rule,
                               f"约束未配置（{c.source}），仅提示"))
        elif ok:
            out.append(Finding("PASS", c.component, c.rule, f"{comp_v} 满足"))
        else:
            lvl = "FAIL" if c.severity == "hard" else "WARN"
            out.append(Finding(lvl, c.component, c.rule,
                               f"{comp_v} 不满足 [{c.min or '·'}..{c.max or '∞'}]"))
    return out


def get_option(mq_id: str) -> MqOption | None:
    return next((o for o in MQ_OPTIONS if o.id == mq_id), None)


def mq_options(milvus_version: str, mode: str = "standalone") -> list[dict]:
    """For a milvus version+mode, list every MQ option with selectable + reason."""
    out: list[dict] = []
    for o in MQ_OPTIONS:
        supported, reason = True, ""
        if not _ge(milvus_version, o.min_milvus):
            supported, reason = False, f"需要 milvus ≥ {o.min_milvus}（当前 {milvus_version}）"
        elif o.standalone_only and mode != "standalone":
            supported, reason = False, "仅 standalone 模式可用"
        out.append({
            "id": o.id, "wal": o.wal, "label": o.label, "dep_kind": o.dep_kind,
            "supported": supported, "reason": reason, "note": o.note,
        })
    return out


def check(mq_id: str, milvus_version: str, mode: str = "standalone") -> MqOption:
    """Validate a chosen MQ against the version; raise if not selectable."""
    o = get_option(mq_id)
    if o is None:
        known = ", ".join(x.id for x in MQ_OPTIONS)
        raise ValueError(f"未知 MQ 选项：{mq_id}（可选：{known}）")
    status = {x["id"]: x for x in mq_options(milvus_version, mode)}[mq_id]
    if not status["supported"]:
        raise ValueError(
            f"milvus {milvus_version} 不支持 MQ '{mq_id}'：{status['reason']} —— 该依赖不可选")
    return o
