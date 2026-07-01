"""Read-only, best-effort version detection via kubectl. No daemon needed."""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass, field


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


@dataclass
class DetectedVersions:
    k8s: str | None = None
    operator: str | None = None
    minio: str | None = None
    kafka: str | None = None
    pulsar: str | None = None
    milvus: dict = field(default_factory=dict)

    def as_compat_dict(self) -> dict:
        d = {}
        if self.k8s:
            d["k8s"] = self.k8s
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

    return dv
