"""Daemon API. All real work happens here (in the core); the CLI just calls in."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .. import paths
from ..core import doctor
from ..core import probe
from ..core import webapi
from ..core.compat import CompatError
from ..core.context import Core
from ..core.models import InstallSpec, Platform
from ..core.taskrunner import TaskRunner

core: Core | None = None

_INSTANCE_KINDS = {"etcd", "minio", "kafka", "pulsar", "milvus"}


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
runner = TaskRunner()


@app.exception_handler(CompatError)
def _compat_handler(request: Request, exc: CompatError) -> JSONResponse:
    return JSONResponse(status_code=409,
                        content={"error": "compat", "reason": str(exc), "force_hint": True})


@app.exception_handler(ValueError)
def _value_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(status_code=400,
                        content={"error": "bad_request", "reason": str(exc)})


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
    core = _core()
    is_k8s = getattr(core.adapter, "name", "") == "k8s"
    pods = []
    if is_k8s:
        try:
            pods = probe.pod_images()
        except Exception:
            pods = []

    def milvus_status_safe(name: str):
        if not is_k8s:
            return None
        try:
            return probe.milvus_status(name)
        except Exception:
            return None

    from ..core import resources as resources_mod
    milvus_list = [{"name": inst.name, "namespace": inst.namespace}
                   for inst in core.state.list_instances()
                   if (inst.spec_snapshot or {}).get("kind") == "milvus"]
    insts_res = {}
    if is_k8s and milvus_list:
        try:
            insts_res = resources_mod.instances_totals(milvus_list)
        except Exception:  # noqa: BLE001
            insts_res = {}

    out = []
    seen = set()
    managed_names: dict[tuple, list] = {}
    # managed (from state)
    for i in core.state.list_instances():
        snap = i.spec_snapshot or {}
        kind = snap.get("kind", "")
        params = snap.get("params", {}) or {}
        ns = i.namespace
        img, img_id = probe.match_pod_image(pods, i.name, ns)
        # managed: prefer the authoritative last-applied snapshot image (milvus),
        # fall back to the running pod image (deps have no image in their snapshot).
        image = params.get("image", "") or img
        status, deps = None, None
        if kind == "milvus":
            deps = {"etcd": params.get("etcdEndpoints", ""), "storage": params.get("storageEndpoint", ""),
                    "mq": params.get("mq", ""),
                    "mq_endpoint": params.get("kafkaBrokers") or params.get("pulsarEndpoint") or ""}
            status = milvus_status_safe(i.name)
        seen.add((kind, i.name, ns))
        managed_names.setdefault((kind, ns), []).append(i.name)
        row = {"name": i.name, "kind": kind, "namespace": ns, "ownership": "managed",
               "image": image, "image_id": img_id or None, "status": status, "deps": deps}
        if kind == "milvus":
            row.update(probe.rollout_of(pods, i.name, ns, params.get("image", "")))
            row["res"] = insts_res.get(i.name)
        else:
            row.update({"rolling": False, "pods_upgraded": 0, "pods_total": 0})
        out.append(row)
    # external (from discovery)
    try:
        cands = core.discovery.discover()
    except Exception:
        cands = []
    for c in cands:
        if c.excluded or c.kind not in _INSTANCE_KINDS or getattr(c.ownership, "value", "") == "readonly":
            continue
        ev = c.evidence if isinstance(c.evidence, dict) else {}
        ns = ev.get("namespace", "")
        key = (c.kind, c.name, ns)
        mnames = managed_names.get((c.kind, ns), ())
        if key in seen or any(c.name == mn or c.name.startswith(mn + "-") for mn in mnames):
            continue
        seen.add(key)
        img, img_id = probe.match_pod_image(pods, c.name, ns)
        image = img or (ev.get("image", "").split(" ")[0])
        status = milvus_status_safe(c.name) if c.kind == "milvus" else None
        out.append({"name": c.name, "kind": c.kind, "namespace": ns, "ownership": "external",
                    "image": image, "image_id": img_id or None, "status": status, "deps": None})
    return {"instances": out}


@app.get("/api/pods")
def api_pods(instance: str) -> dict[str, Any]:
    core = _core()
    inst = core.state.get_instance(instance)
    if inst is None:
        raise ValueError(f"未找到实例：{instance}")
    desired = ((inst.spec_snapshot or {}).get("params", {}) or {}).get("image", "")
    pods: list[dict] = []
    resources_out = {"metrics_available": False,
                     "total": {"cpu_req_m": 0, "cpu_lim_m": 0, "mem_req_b": 0, "mem_lim_b": 0, "pods": 0},
                     "pods": []}
    if getattr(core.adapter, "name", "") == "k8s":
        from ..core import resources as resources_mod
        try:
            pods = probe.pods_of(instance, inst.namespace)
        except Exception:  # noqa: BLE001
            pods = []
        try:
            resources_out = resources_mod.instance_resources(instance, inst.namespace)
        except Exception:  # noqa: BLE001
            pass
    return {"instance": instance, "namespace": inst.namespace, "desired_image": desired,
            "pods": pods, "resources": resources_out}


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


@app.post("/api/install")
def api_install(req: InstallReq) -> Any:
    spec = InstallSpec(
        kind=req.kind, name=req.name, platform=req.platform,
        method=req.method, namespace=req.namespace, params=req.params,
        chart_override=req.chart_override,
    )
    if req.dry_run:
        task = _core().install(spec, dry_run=True, force=req.force)
        return {"task": task.model_dump(mode="json")}
    # apply: synchronous gate pre-check (raises CompatError -> 409 via handler), then submit
    _core().install(spec, dry_run=True, force=req.force)
    tid = runner.submit(lambda: _core().install(spec, dry_run=False, force=req.force))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)


@app.get("/api/task/{task_id}")
def api_task(task_id: str) -> dict[str, Any]:
    rec = runner.get(task_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="unknown task")
    if rec["state"] == "running":
        return {"state": "running", "task": rec.get("partial"), "error": None}
    if rec["state"] == "error":
        return {"state": "error", "task": rec.get("partial"), "error": rec["error"]}
    dump = rec["result"].model_dump(mode="json")   # a Task
    return {"state": dump["status"], "task": dump, "error": None}


class DeleteReq(BaseModel):
    instance: str
    dry_run: bool = True


@app.post("/delete")
def delete(req: DeleteReq) -> dict[str, Any]:
    return _core().delete(req.instance, dry_run=req.dry_run).model_dump()


@app.post("/api/delete")
def api_delete(req: DeleteReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().delete(req.instance, dry_run=True)
        return {"task": task.model_dump(mode="json")}
    tid = runner.submit(lambda: _core().delete(req.instance, dry_run=False))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)


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


@app.post("/api/upgrade")
def api_upgrade(req: UpgradeReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().upgrade(req.instance, req.image, dry_run=True, force=req.force)
        return {"task": task.model_dump()}
    _core().upgrade(req.instance, req.image, dry_run=True, force=req.force)   # sync gate pre-check
    tid = runner.submit(lambda: _core().upgrade(req.instance, req.image, dry_run=False, force=req.force))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)


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


@app.get("/api/config")
def api_config(instance: str) -> dict[str, Any]:
    inst = _core().state.get_instance(instance)
    if inst is None:
        raise ValueError(f"未找到实例：{instance}")
    snap = inst.spec_snapshot or {}
    overrides = (snap.get("params", {}) or {}).get("_conf", {}) or {}
    try:
        current = _core().config_get(instance)
    except Exception:  # noqa: BLE001  — CM may not exist yet; best-effort
        current = None
    return {"instance": instance, "current": current, "overrides": overrides}


class ConfigSetApiReq(BaseModel):
    instance: str
    kv: dict[str, Any] = {}
    dry_run: bool = True


@app.post("/api/config/set")
def api_config_set(req: ConfigSetApiReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().config_set(req.instance, req.kv, dry_run=True)
        return {"task": task.model_dump(mode="json")}
    tid = runner.submit(lambda: _core().config_set(req.instance, req.kv, dry_run=False))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)


@app.get("/api/mq-options")
def api_mq_options(instance: str) -> dict[str, Any]:
    inst = _core().state.get_instance(instance)
    if inst is None:
        raise ValueError(f"未找到实例：{instance}")
    params = (inst.spec_snapshot or {}).get("params", {}) or {}
    from ..core import compat, probe
    version = probe._tag(params.get("image", "")) or ""
    mode = params.get("mode", "standalone")
    cur_mq = params.get("mq", "")
    cur_opt = compat.get_option(cur_mq)
    current_wal = cur_opt.wal if cur_opt else cur_mq
    return {"instance": instance, "current_mq": cur_mq, "current_wal": current_wal,
            "options": _core().mq_options(version, mode)}


class SwitchMqApiReq(BaseModel):
    instance: str
    target_wal: str
    dry_run: bool = True
    force: bool = False


@app.post("/api/switch-mq")
def api_switch_mq(req: SwitchMqApiReq) -> Any:
    if _core().state.get_instance(req.instance) is None:
        raise ValueError(f"未找到实例：{req.instance}")
    if req.dry_run:
        task = _core().switch_mq(req.instance, req.target_wal, dry_run=True, force=req.force)
        return {"task": task.model_dump(mode="json")}
    # apply: sync gate pre-check (CompatError -> 409 via handler), then submit
    _core().switch_mq(req.instance, req.target_wal, dry_run=True, force=req.force)
    tid = runner.submit(lambda: _core().switch_mq(req.instance, req.target_wal, dry_run=False, force=req.force))
    return JSONResponse({"task_id": tid, "state": "running"}, status_code=202)


@app.get("/api/resources")
def api_resources() -> dict[str, Any]:
    from ..core import hostinfo, resources
    host = hostinfo.collect()
    k8s = None
    if getattr(_core().adapter, "name", "") == "k8s":
        try:
            k8s = resources.cluster_resources()
        except Exception:  # noqa: BLE001
            k8s = None
    return {"host": host, "k8s": k8s}


@app.get("/api/logs")
def api_logs(pod: str, namespace: str = "default") -> dict[str, Any]:
    core = _core()
    if getattr(core.adapter, "name", "") == "k8s":
        try:
            logs = probe.pod_logs(pod, namespace)
        except Exception:  # noqa: BLE001
            logs = "（读取失败）"
    else:
        logs = "（非 k8s 环境，无 pod 日志）"
    return {"pod": pod, "namespace": namespace, "logs": logs}


@app.get("/api/switch-mq/targets")
def api_switch_mq_targets(instance: str) -> dict[str, Any]:
    inst = _core().state.get_instance(instance)
    if inst is None:
        raise ValueError(f"未找到实例：{instance}")
    params = (inst.spec_snapshot or {}).get("params", {}) or {}
    from ..core import compat, probe
    version = probe._tag(params.get("image", "")) or ""
    mode = params.get("mode", "standalone")
    cur_mq = params.get("mq", "")
    cur_opt = compat.get_option(cur_mq)
    current_wal = cur_opt.wal if cur_opt else cur_mq
    targets = compat.switch_mq_targets(current_wal, version, mode, operator_version="")  # op_ver reserved

    def _dep_endpoint(kind, dep_name, dep_ns):
        return {"kafka": f"{dep_name}.{dep_ns}.svc:9092",
                "pulsar": f"{dep_name}-broker.{dep_ns}.svc:6650",
                "woodpecker": f"{dep_name}.{dep_ns}.svc:9000"}.get(kind, f"{dep_name}.{dep_ns}.svc")

    by_kind: dict[str, list] = {}
    for si in _core().state.list_instances():
        k = (si.spec_snapshot or {}).get("kind", "")
        by_kind.setdefault(k, []).append(si)
    for t in targets:
        dep = t.get("dep_kind")
        t["instances"] = ([] if not dep else
                          [{"name": si.name, "namespace": si.namespace,
                            "endpoint": _dep_endpoint(dep, si.name, si.namespace)}
                           for si in by_kind.get(dep, [])])
    return {"instance": instance, "current_mq": cur_mq, "current_wal": current_wal,
            "milvus_version": version, "mode": mode, "targets": targets}


# --- WebUI static frontend (registered LAST so /api/* and /status win) ---
import pathlib
from fastapi.staticfiles import StaticFiles

_WEBUI_DIR = pathlib.Path(__file__).resolve().parent.parent / "webui"
app.mount("/", StaticFiles(directory=str(_WEBUI_DIR), html=True), name="webui")
