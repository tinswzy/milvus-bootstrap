"""Milvus↔MQ version compatibility matrix — unsupported options are non-selectable."""
from __future__ import annotations

import pytest

from milvus_bootstrap.core import compat


def test_2_6_excludes_woodpecker_service():
    opts = {o["id"]: o for o in compat.mq_options("v2.6.3")}
    assert opts["woodpecker-service"]["supported"] is False
    assert "3.0" in opts["woodpecker-service"]["reason"]
    for supported in ("woodpecker-embedded", "kafka", "pulsar", "rocksmq"):
        assert opts[supported]["supported"] is True


def test_3_0_supports_all():
    opts = {o["id"]: o for o in compat.mq_options("v3.0.0")}
    for i in ("woodpecker-service", "woodpecker-embedded", "kafka", "pulsar", "rocksmq"):
        assert opts[i]["supported"] is True


def test_master_or_dev_image_treated_as_newest():
    opts = {o["id"]: o for o in compat.mq_options("milvusdb/milvus:master")}
    assert opts["woodpecker-service"]["supported"] is True


def test_rocksmq_standalone_only():
    opts = {o["id"]: o for o in compat.mq_options("v2.6.3", mode="cluster")}
    assert opts["rocksmq"]["supported"] is False
    assert "standalone" in opts["rocksmq"]["reason"]


def test_check_gates_unsupported():
    with pytest.raises(ValueError):
        compat.check("woodpecker-service", "v2.6.3")          # not supported < 3.0
    assert compat.check("kafka", "v2.6.3").wal == "kafka"
    assert compat.check("woodpecker-service", "v3.0.0").wal == "woodpecker"


def test_check_unknown_mq():
    with pytest.raises(ValueError):
        compat.check("rabbitmq", "v3.0.0")


def test_parse_version():
    assert compat.parse_version("milvusdb/milvus:v2.6.3") == (2, 6, 3)
    assert compat.parse_version("v3.0.0") == (3, 0, 0)
    assert compat.parse_version("master") is None
