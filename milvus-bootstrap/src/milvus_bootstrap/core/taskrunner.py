"""Tiny in-process async runner: submit a fn, poll its status. No business logic."""
from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any


class TaskRunner:
    def __init__(self) -> None:
        self._recs: dict[str, dict] = {}
        self._lock = threading.Lock()

    def submit(self, fn: Callable[[], Any]) -> str:
        tid = uuid.uuid4().hex[:12]
        with self._lock:
            self._recs[tid] = {"state": "running", "result": None, "error": None}

        def _run() -> None:
            try:
                res = fn()
                rec = {"state": "done", "result": res, "error": None}
            except Exception as exc:  # noqa: BLE001
                rec = {"state": "error", "result": None, "error": str(exc)}
            with self._lock:
                self._recs[tid] = rec

        threading.Thread(target=_run, daemon=True).start()
        return tid

    def get(self, task_id: str) -> dict | None:
        with self._lock:
            rec = self._recs.get(task_id)
            return dict(rec) if rec is not None else None
