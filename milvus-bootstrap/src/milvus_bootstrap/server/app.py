"""Daemon API. All real work happens here (in the core); the CLI just calls in."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel

from .. import paths
from ..core import doctor
from ..core import webapi
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


@app.get("/api/doctor")
def api_doctor() -> dict[str, Any]:
    return doctor.run().to_json()


@app.get("/api/instances")
def api_instances() -> dict[str, Any]:
    out = []
    for i in _core().state.list_instances():
        out.append({"name": i.name, "kind": i.kind, "namespace": i.namespace,
                    "ownership": i.ownership.value})
    return {"instances": out}


@app.get("/api/compat-rules")
def api_compat_rules() -> dict[str, Any]:
    return webapi.compat_rules()


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
    chart_override: str | None = None
    dry_run: bool = True
    force: bool = False


@app.post("/install")
def install(req: InstallReq) -> dict[str, Any]:
    spec = InstallSpec(
        kind=req.kind, name=req.name, platform=req.platform,
        method=req.method, namespace=req.namespace, params=req.params,
        chart_override=req.chart_override,
    )
    return _core().install(spec, dry_run=req.dry_run, force=req.force).model_dump()


class DeleteReq(BaseModel):
    instance: str
    dry_run: bool = True


@app.post("/delete")
def delete(req: DeleteReq) -> dict[str, Any]:
    return _core().delete(req.instance, dry_run=req.dry_run).model_dump()


class ScaleReq(BaseModel):
    instance: str
    replicas: int
    dry_run: bool = True


@app.post("/scale")
def scale(req: ScaleReq) -> dict[str, Any]:
    return _core().scale(req.instance, req.replicas, dry_run=req.dry_run).model_dump()


class UpgradeReq(BaseModel):
    instance: str
    image: str
    dry_run: bool = True
    force: bool = False


@app.post("/upgrade")
def upgrade(req: UpgradeReq) -> dict[str, Any]:
    return _core().upgrade(req.instance, req.image, dry_run=req.dry_run, force=req.force).model_dump()


class AdoptReq(BaseModel):
    kind: str
    name: str
    dry_run: bool = True


@app.post("/adopt")
def adopt(req: AdoptReq) -> dict[str, Any]:
    return _core().adopt(req.kind, req.name, dry_run=req.dry_run).model_dump()


class SwitchMqReq(BaseModel):
    instance: str
    target_wal: str
    dry_run: bool = True
    force: bool = False


@app.post("/switch-mq")
def switch_mq(req: SwitchMqReq) -> dict[str, Any]:
    return _core().switch_mq(req.instance, req.target_wal, dry_run=req.dry_run, force=req.force).model_dump()


class MqOptionsReq(BaseModel):
    milvus_version: str
    mode: str = "standalone"


@app.post("/mq-options")
def mq_options(req: MqOptionsReq) -> dict[str, Any]:
    return {"options": _core().mq_options(req.milvus_version, req.mode)}


class ConfigGetReq(BaseModel):
    instance: str


@app.post("/config/get")
def config_get(req: ConfigGetReq) -> dict[str, Any]:
    return {"config": _core().config_get(req.instance)}


class ConfigSetReq(BaseModel):
    instance: str
    kv: dict[str, Any] = {}
    dry_run: bool = True


@app.post("/config/set")
def config_set(req: ConfigSetReq) -> dict[str, Any]:
    return _core().config_set(req.instance, req.kv, dry_run=req.dry_run).model_dump()


class ConfigRestartReq(BaseModel):
    instance: str
    dry_run: bool = True


@app.post("/config/restart")
def config_restart(req: ConfigRestartReq) -> dict[str, Any]:
    return _core().config_restart(req.instance, dry_run=req.dry_run).model_dump()
