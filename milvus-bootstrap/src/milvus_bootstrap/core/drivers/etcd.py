"""EtcdDriver — overrides the component-specific 20%.

Most of etcd's knowledge lives in profiles/etcd.yaml; the genuinely
code-shaped bits (quorum-aware scaling) are overridden here.
"""
from __future__ import annotations

from .base import BaseServiceDriver


class EtcdDriver(BaseServiceDriver):
    def replicas_param(self) -> str:
        return "replicaCount"

    def scale_plan(self, current: int, target: int) -> str:
        notes = ["副本保持奇数（quorum）", "一次只增/减一个成员"]
        if target < current:
            notes.append("缩容：preStop 触发 etcdctl member remove 注销成员")
        else:
            notes.append("扩容：新成员加入后等待重新同步")
        return f"etcd {current} → {target}：" + "；".join(notes)

    # backup(): etcdctl snapshot save  (authoritative — required before delete/resize)
    # upgrade(): immutable volumeClaimTemplates -> orphan-delete + recreate STS
    # (wired in a later increment)
