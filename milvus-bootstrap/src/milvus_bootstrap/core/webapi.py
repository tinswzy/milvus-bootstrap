"""Assemble compatibility rules into frontend-friendly JSON (read-only, pure)."""
from __future__ import annotations

from dataclasses import asdict

from . import compat


def compat_rules() -> dict:
    mq_rules = [
        {"id": o.id, "label": o.label, "wal": o.wal, "min_milvus": o.min_milvus,
         "dep_kind": o.dep_kind, "standalone_only": o.standalone_only, "note": o.note}
        for o in compat.MQ_OPTIONS
    ]
    constraints = [asdict(c) for c in compat.load_constraints()]
    upgrade_paths = list(compat.load_upgrade_paths())
    return {"mq_rules": mq_rules, "constraints": constraints, "upgrade_paths": upgrade_paths}
