"""Read-only, best-effort version detection via kubectl. No daemon needed."""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass, field
from typing import NamedTuple


def run_kubectl(args: list[str]) -> tuple[int, str, str]:
    try:
        p = subprocess.run(["kubectl", *args, "--request-timeout=15s"],
                           capture_output=True, text=True, timeout=25)
        return p.returncode, p.stdout, p.stderr
    except (OSError, subprocess.SubprocessError) as e:
        return 1, "", str(e)


def _tag(image: str) -> str | None:
    """milvusdb/milvus:v2.6.18 -> 2.6.18 ; drop leading v, drop digest."""
    if not image or ":" not in image:
        return None
    tag = image.rsplit(":", 1)[-1].split("@", 1)[0]
    m = re.match(r"v?(\d+\.\d+\.\d+)", tag)
    return m[1] if m else tag or None


def _image_tag(image: str) -> str | None:
    """Return the raw tag after ':' (keeps minio RELEASE.* verbatim)."""
    if not image or ":" not in image:
        return None
    return image.rsplit(":", 1)[-1].split("@", 1)[0] or None


_DEP_MATCH = [("etcd", "etcd"), ("minio", "minio"), ("kafka", "kafka"), ("pulsar", "pulsar")]


@dataclass
class DetectedVersions:
    k8s: str | None = None
    etcd: str | None = None
    operator: str | None = None
    minio: str | None = None
    kafka: str | None = None
    pulsar: str | None = None
    milvus: dict = field(default_factory=dict)

    def as_compat_dict(self) -> dict:
        d = {}
        if self.k8s:
            d["k8s"] = self.k8s
        if self.etcd:
            d["etcd"] = self.etcd
        if self.operator:
            d["milvus-operator"] = self.operator
        if self.minio:
            d["minio"] = self.minio
        if self.kafka:
            d["kafka"] = self.kafka
        if self.pulsar:
            d["pulsar"] = self.pulsar
        if self.milvus:
            d["milvus"] = next(iter(self.milvus.values()))
        return d


def milvus_status(name: str, run=run_kubectl) -> str | None:
    rc, out, _ = run(["get", "milvus", name, "-o", "jsonpath={.status.status}"])
    if rc != 0:
        return None
    return out.strip() or None


class PodImage(NamedTuple):
    namespace: str
    pod: str
    image: str
    image_id: str


def _sha_of(image_id: str) -> str:
    """Extract 'sha256:...' from a k8s imageID (repo@sha256:.. / docker-pullable://repo@sha256:..)."""
    if not image_id or "sha256:" not in image_id:
        return ""
    return "sha256:" + image_id.split("sha256:", 1)[1].strip()


def pod_images(run=run_kubectl) -> list[PodImage]:
    """One-shot map of every pod's primary container image + imageID (best-effort)."""
    rc, out, _ = run(["get", "pods", "-A", "-o",
                      "jsonpath={range .items[*]}{.metadata.namespace}{'\\t'}{.metadata.name}{'\\t'}"
                      "{.status.containerStatuses[0].image}{'\\t'}{.status.containerStatuses[0].imageID}{'\\n'}{end}"])
    if rc != 0:
        return []
    pods: list[PodImage] = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) == 4:
            pods.append(PodImage(*[p.strip() for p in parts]))
    return pods


def rollout_of(pods, name: str, ns: str, desired: str) -> dict:
    """How many of an instance's pods are on the desired image tag (best-effort)."""
    dtag = _tag(desired)
    mine = [p for p in pods if p.namespace == ns and (p.pod == name or p.pod.startswith(name + "-"))]
    total = len(mine)
    # short-circuit: if desired has no parseable tag, treat all matched pods as on-target
    upgraded = sum(1 for p in mine if not dtag or _tag(p.image) == dtag)
    return {"rolling": total > 0 and upgraded < total, "pods_upgraded": upgraded, "pods_total": total}


def match_pod_image(pods, name: str, ns: str) -> tuple[str, str]:
    """First pod in ns at a name-segment boundary (name itself or name-…) → (image, sha256-or-'').

    Segment boundary avoids a short instance name matching an unrelated pod
    (e.g. 'etcd' must not match 'etcd-operator-…'); StatefulSet pods 'etcd-0' still match.
    """
    for p in pods:
        if p.namespace == ns and (p.pod == name or p.pod.startswith(name + "-")):
            return p.image, _sha_of(p.image_id)
    return "", ""


def pods_of(name: str, ns: str, run=run_kubectl) -> list[dict]:
    """Pods belonging to an instance (name-segment match), best-effort."""
    rc, out, _ = run(["get", "pods", "-n", ns, "-o",
                      "jsonpath={range .items[*]}{.metadata.name}{'\\t'}{.status.phase}{'\\t'}"
                      "{range .status.containerStatuses[*]}{.ready},{end}{'\\t'}"
                      "{range .status.containerStatuses[*]}{.restartCount},{end}{'\\t'}"
                      "{.status.containerStatuses[0].image}{'\\t'}"
                      "{.metadata.creationTimestamp}{'\\n'}{end}"])
    if rc != 0:
        return []
    out_pods: list[dict] = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 6:
            continue
        pod, phase, ready_csv, restart_csv, image, created = parts
        if not (pod == name or pod.startswith(name + "-")):
            continue
        readies = [x for x in ready_csv.split(",") if x]
        ready = f"{sum(1 for x in readies if x == 'true')}/{len(readies)}" if readies else "0/0"
        restarts = sum(int(x) for x in restart_csv.split(",") if x.strip().isdigit())
        out_pods.append({"pod": pod, "phase": phase, "ready": ready,
                         "restarts": restarts, "image": image, "created": created})
    return out_pods


def detect_versions(run=run_kubectl) -> DetectedVersions:
    dv = DetectedVersions()

    rc, out, _ = run(["version", "-o", "json"])
    if rc == 0 and out.strip():
        try:
            dv.k8s = (json.loads(out).get("serverVersion", {})
                      .get("gitVersion", "") or "").lstrip("v") or None
        except json.JSONDecodeError:
            dv.k8s = None

    rc, out, _ = run(["get", "deploy", "-A",
                      "-l", "app.kubernetes.io/name=milvus-operator",
                      "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}"
                      "{.spec.template.spec.containers[0].image}{\"\\n\"}{end}"])
    if rc == 0:
        for line in out.splitlines():
            if "\t" in line:
                dv.operator = _tag(line.split("\t", 1)[1])
                break

    rc, out, _ = run(["get", "milvus", "-A",
                      "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}"
                      "{.spec.components.image}{\"\\n\"}{end}"])
    if rc == 0:
        for line in out.splitlines():
            if "\t" in line:
                name, image = line.split("\t", 1)
                v = _tag(image)
                if v:
                    dv.milvus[name] = v

    rc, out, _ = run(["get", "pods", "-A",
                      "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}"
                      "{.spec.containers[0].image}{\"\\n\"}{end}"])
    if rc == 0:
        for line in out.splitlines():
            if "\t" not in line:
                continue
            _name, image = line.split("\t", 1)
            low = image.lower()
            for field, needle in _DEP_MATCH:
                if needle in low and getattr(dv, field) is None:
                    tag = _image_tag(image)
                    if field == "minio":
                        setattr(dv, field, tag)               # keep RELEASE.* verbatim
                    else:
                        setattr(dv, field, _tag(image) or tag)  # semver-reduce others

    return dv


def pod_logs(pod: str, namespace: str, run=run_kubectl) -> str:
    """Last 100 lines of a pod's container logs (all containers, prefixed). One-shot, best-effort."""
    rc, out, err = run(["logs", pod, "-n", namespace, "--tail=100",
                        "--all-containers=true", "--prefix=true"])
    if rc != 0:
        return err.strip() or "（无日志或读取失败）"
    return out
