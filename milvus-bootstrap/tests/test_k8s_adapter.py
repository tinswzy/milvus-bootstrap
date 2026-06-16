"""K8sAdapter unit tests — the pure helm/argv construction (no cluster needed)."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core.platform.k8s import (
    K8sAdapter,
    flatten_set_args,
    helm_install_argv,
)


def test_flatten_nested_and_bool():
    out = flatten_set_args(
        {"replicaCount": 3, "persistence": {"size": "10Gi"}, "auth": {"rbac": {"enabled": False}}}
    )
    assert out == [
        "--set", "replicaCount=3",
        "--set", "persistence.size=10Gi",
        "--set", "auth.rbac.enabled=false",
    ]


def test_helm_install_argv():
    argv = helm_install_argv("etcd-dev", "bitnami/etcd", "default", {"replicaCount": 3})
    assert argv == [
        "helm", "upgrade", "--install", "etcd-dev", "bitnami/etcd",
        "-n", "default", "--create-namespace", "--set", "replicaCount=3",
    ]


def test_helm_requires_chart():
    with pytest.raises(ValueError):
        helm_install_argv("x", None, "default", {})


def test_plan_apply_helm_string():
    s = K8sAdapter().plan_apply(
        kind="etcd", name="etcd-dev", namespace="default", method="bitnami-helm",
        method_kind="helm", chart="bitnami/etcd",
        params={"replicaCount": 3, "persistence": {"size": "10Gi"}},
    )
    assert "helm upgrade --install etcd-dev bitnami/etcd -n default --create-namespace" in s
    assert "replicaCount=3" in s


def test_plan_apply_external():
    s = K8sAdapter().plan_apply(
        kind="etcd", name="x", namespace="default", method="external",
        method_kind="external", chart=None, params={},
    )
    assert "external" in s
