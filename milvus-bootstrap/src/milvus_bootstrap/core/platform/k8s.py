"""K8sAdapter — real Kubernetes target.

Realises platform-neutral intent on k8s:
  - discover_native(): read STS / Deploy / standalone+static Pods -> evidence
  - plan_apply():      build the real `helm upgrade --install ...` command (dry-run)
  - apply_workload():  run helm (subprocess)
  - wait_ready():      poll the workload's readyReplicas by instance label

helm/argv construction is pure + unit-tested without a cluster; only the
apply/discover/wait paths need a live cluster (and the optional ``kubernetes``
client + a ``helm`` binary on PATH).
"""
from __future__ import annotations

import shlex
import subprocess
import time
from typing import Any

from .base import PlatformAdapter


# ---- pure helpers (unit-testable, no cluster / no deps) ----
def _helm_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def flatten_set_args(params: dict[str, Any], prefix: str = "") -> list[str]:
    """Nested params -> helm ``--set a.b.c=v`` argv fragments."""
    args: list[str] = []
    for k, v in (params or {}).items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            args += flatten_set_args(v, prefix=key + ".")
        else:
            args += ["--set", f"{key}={_helm_value(v)}"]
    return args


def helm_install_argv(release: str, chart: str | None, namespace: str,
                      params: dict[str, Any]) -> list[str]:
    if not chart:
        raise ValueError("helm 安装需要 chart")
    argv = ["helm", "upgrade", "--install", release, chart,
            "-n", namespace, "--create-namespace"]
    argv += flatten_set_args(params)
    return argv


def dig(d: Any, path: str) -> Any:
    """Navigate a dotted path in a nested dict (e.g. 'status.currentState')."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


class K8sAdapter(PlatformAdapter):
    name = "k8s"

    def __init__(self) -> None:
        self._loaded = False
        self.apps = None
        self.core = None

    # ---- cluster access ----
    def _ensure_client(self) -> None:
        if self._loaded:
            return
        try:
            from kubernetes import client, config
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("缺少 kubernetes 客户端：uv sync --extra k8s") from exc
        try:
            config.load_incluster_config()
        except Exception:
            try:
                config.load_kube_config()
            except Exception as exc:
                raise RuntimeError("无法连接 k8s：请配置 kubeconfig，或在集群内运行 core") from exc
        self.apps = client.AppsV1Api()
        self.core = client.CoreV1Api()
        self._loaded = True

    @staticmethod
    def _pod_bits(podspec) -> tuple[list[str], list[int]]:
        images, ports = [], []
        for c in (podspec.containers or []):
            if c.image:
                images.append(c.image)
            for p in (c.ports or []):
                if p.container_port:
                    ports.append(p.container_port)
        return images, ports

    def _evidence(self, *, workload: str, md, podspec) -> dict[str, Any]:
        images, ports = self._pod_bits(podspec)
        return {
            "platform": "k8s",
            "workload": workload,
            "name": md.name,
            "namespace": md.namespace,
            "image": " ".join(images),
            "ports": ports,
            "labels": dict(md.labels or {}),
            "annotations": dict(md.annotations or {}),
        }

    def discover_native(self) -> list[dict[str, Any]]:
        self._ensure_client()
        out: list[dict[str, Any]] = []
        for sts in self.apps.list_stateful_set_for_all_namespaces().items:
            out.append(self._evidence(workload="StatefulSet", md=sts.metadata,
                                      podspec=sts.spec.template.spec))
        for dep in self.apps.list_deployment_for_all_namespaces().items:
            out.append(self._evidence(workload="Deployment", md=dep.metadata,
                                      podspec=dep.spec.template.spec))
        # standalone / static pods (e.g. control-plane etcd) — not owned by a controller
        for pod in self.core.list_pod_for_all_namespaces().items:
            owners = pod.metadata.owner_references or []
            if not owners or all(o.kind == "Node" for o in owners):
                out.append(self._evidence(workload="Pod", md=pod.metadata, podspec=pod.spec))
        return out

    # ---- provisioning ----
    def plan_apply(self, *, kind, name, namespace, method, method_kind, chart, params):
        if method_kind == "helm":
            argv = helm_install_argv(name, chart, namespace, params)
            return "将执行：" + " ".join(shlex.quote(a) for a in argv)
        if method_kind == "external":
            return f"external：不安装，只在 Milvus 填 {kind} endpoints"
        return f"[k8s] 经 {method}（{method_kind}）安装 {kind}/{name} — 待接（operator-cr / manifest）"

    def apply_workload(self, *, kind, name, namespace, method, method_kind, chart, params):
        if method_kind != "helm":
            raise NotImplementedError(f"K8sAdapter 目前只实现 helm 安装；{method_kind} 待接")
        self._run(helm_install_argv(name, chart, namespace, params))
        return f"[k8s] helm 安装完成：{name}（ns={namespace}）"

    def wait_ready(self, *, kind, name, namespace, check, timeout_s: int = 420):
        self._ensure_client()
        selector = f"app.kubernetes.io/instance={name}"
        deadline = time.monotonic() + timeout_s
        last = "无匹配工作负载"
        while time.monotonic() < deadline:
            objs = (self.apps.list_namespaced_stateful_set(namespace, label_selector=selector).items
                    + self.apps.list_namespaced_deployment(namespace, label_selector=selector).items)
            if objs and all(self._ready(o) for o in objs):
                return f"[k8s] {kind}/{name} ready（{len(objs)} 工作负载就绪 · 门槛：{check}）"
            last = "；".join(f"{o.metadata.name}: {(o.status.ready_replicas or 0)}/{o.spec.replicas or 1}" for o in objs) or last
            time.sleep(2)
        raise TimeoutError(f"{kind}/{name} 未在 {timeout_s}s 内就绪（{last}）")

    @staticmethod
    def _ready(obj) -> bool:
        want = obj.spec.replicas if obj.spec.replicas is not None else 1
        return (obj.status.ready_replicas or 0) >= want

    def delete_workload(self, *, kind, name, namespace):
        # helm uninstall — note: leaves PVCs (authoritative data) on purpose
        self._run(["helm", "uninstall", name, "-n", namespace])
        return f"[k8s] helm uninstall {name}（PVC 保留）"

    # ---- operator-cr mechanism ----
    def crd_exists(self, *, group, plural):
        self._ensure_client()
        from kubernetes import client
        from kubernetes.client.exceptions import ApiException
        try:
            client.ApiextensionsV1Api().read_custom_resource_definition(name=f"{plural}.{group}")
            return True
        except ApiException as exc:
            if exc.status == 404:
                return False
            raise

    def apply_objects(self, *, manifests):
        self._ensure_client()
        from kubernetes import dynamic
        from kubernetes.client import api_client
        from kubernetes.client.exceptions import ApiException
        dyn = dynamic.DynamicClient(api_client.ApiClient())
        done = []
        for man in manifests:
            res = dyn.resources.get(api_version=man["apiVersion"], kind=man["kind"])
            ns = man.get("metadata", {}).get("namespace")
            name = man["metadata"]["name"]
            try:
                dyn.create(res, body=man, namespace=ns)
                done.append(f"created {man['kind']}/{name}")
            except ApiException as exc:
                if exc.status != 409:
                    raise
                dyn.patch(res, body=man, namespace=ns, name=name,
                          content_type="application/merge-patch+json")
                done.append(f"updated {man['kind']}/{name}")
        return "[k8s] " + "; ".join(done)

    def wait_cr(self, *, group, version, plural, namespace, name,
                status_path, status_equals, timeout_s: int = 420):
        self._ensure_client()
        from kubernetes import client
        co = client.CustomObjectsApi()
        deadline = time.monotonic() + timeout_s
        last = "无 status"
        while time.monotonic() < deadline:
            try:
                obj = co.get_namespaced_custom_object(group, version, namespace, plural, name)
            except Exception as exc:  # noqa: BLE001
                last = str(exc)
                time.sleep(3)
                continue
            val = dig(obj, status_path)
            if val is not None and str(val) == status_equals:
                return f"[k8s] {name}.{status_path}={val}"
            last = f"{status_path}={val}"
            time.sleep(3)
        raise TimeoutError(f"{name} CR 未就绪（{last}）")

    def delete_cr(self, *, group, version, plural, namespace, name):
        self._ensure_client()
        from kubernetes import client
        client.CustomObjectsApi().delete_namespaced_custom_object(group, version, namespace, plural, name)
        return f"[k8s] deleted {plural}/{name}"

    def exec(self, *, namespace, label_selector, command):
        self._ensure_client()
        pods = self.core.list_namespaced_pod(namespace, label_selector=label_selector).items
        running = [p for p in pods if p.status and p.status.phase == "Running"]
        if not running:
            raise RuntimeError(f"无可用 Running pod（{label_selector} in {namespace}）")
        pod = running[0].metadata.name
        out = self._run(["kubectl", "exec", pod, "-n", namespace, "--", *command])
        return f"[k8s] exec {pod}: {out.strip()[:200]}"

    @staticmethod
    def _run(argv: list[str]) -> str:
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            raise RuntimeError(f"找不到命令 {argv[0]}（请确认已安装并在 PATH）") from exc
        if proc.returncode != 0:
            raise RuntimeError(f"命令失败：{' '.join(argv[:4])}… → {proc.stderr.strip()[:400]}")
        return proc.stdout
