"""Daemon API. All real work happens here (in the core); the CLI just calls in."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel

from .. import paths
from ..core.context import Core
from ..core.models import InstallSpec, Platform

core: Core | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global core
    core = Core()
    paths.ensure_base()
    paths.pid_path().write_text(str(os.getpid()))
    try:
        yield
    finally:
        paths.pid_path().unlink(missing_ok=True)


app = FastAPI(title="milvus-bootstrap core daemon", lifespan=lifespan)


def _core() -> Core:
    assert core is not None
    return core


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@app.get("/status")
def status() -> dict[str, Any]:
    return _core().status()


@app.post("/discover")
def discover() -> dict[str, Any]:
    return {"candidates": [c.model_dump() for c in _core().discover()]}


class InstallReq(BaseModel):
    kind: str
    name: str
    platform: Platform = Platform.k8s
    method: str | None = None
    namespace: str = "default"
    params: dict[str, Any] = {}
    dry_run: bool = True


@app.post("/install")
def install(req: InstallReq) -> dict[str, Any]:
    spec = InstallSpec(
        kind=req.kind, name=req.name, platform=req.platform,
        method=req.method, namespace=req.namespace, params=req.params,
    )
    return _core().install(spec, dry_run=req.dry_run).model_dump()
