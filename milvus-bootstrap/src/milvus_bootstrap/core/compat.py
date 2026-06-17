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

import re
from dataclasses import dataclass


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
