"""Task-scoped progress sink (ContextVar). The engine publishes each step;
a TaskRunner worker registers a sink to capture snapshots. No-op otherwise."""
from __future__ import annotations

import contextvars
from collections.abc import Callable
from typing import Any

_sink: contextvars.ContextVar = contextvars.ContextVar("mb_task_progress_sink", default=None)


def set_sink(fn: Callable[[Any], None]) -> contextvars.Token:
    return _sink.set(fn)


def reset_sink(token: contextvars.Token) -> None:
    _sink.reset(token)


def publish(task: Any) -> None:
    fn = _sink.get()
    if fn is not None:
        fn(task)
