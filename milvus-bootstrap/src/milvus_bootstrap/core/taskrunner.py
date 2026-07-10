"""Tiny in-process async runner: submit a fn, poll its status + live steps."""
from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any

from . import progress


class TaskRunner:
    def __init__(self) -> None:
        self._recs: dict[str, dict] = {}
        self._lock = threading.Lock()

    def _set_partial(self, tid: str, task: Any) -> None:
        dump = task.model_dump(mode="json")
        with self._lock:
            rec = self._recs.get(tid)
            if rec is not None:
                rec["partial"] = dump

    def submit(self, fn: Callable[[], Any]) -> str:
        tid = uuid.uuid4().hex[:12]
        with self._lock:
            self._recs[tid] = {"state": "running", "result": None, "error": None, "partial": None}

        def _run() -> None:
            token = progress.set_sink(lambda t: self._set_partial(tid, t))
            try:
                res = fn()
                state, result, error = "done", res, None
            except Exception as exc:  # noqa: BLE001
                state, result, error = "error", None, str(exc)
            finally:
                progress.reset_sink(token)
            with self._lock:
                old = self._recs.get(tid) or {}
                self._recs[tid] = {"state": state, "result": result,
                                   "error": error, "partial": old.get("partial")}

        threading.Thread(target=_run, daemon=True).start()
        return tid

    def get(self, task_id: str) -> dict | None:
        with self._lock:
            rec = self._recs.get(task_id)
            return dict(rec) if rec is not None else None
