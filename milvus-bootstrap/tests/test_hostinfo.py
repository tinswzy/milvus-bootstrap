from milvus_bootstrap.core import hostinfo


def test_collect_keys_and_types():
    h = hostinfo.collect()
    expected = {"hostname", "os", "kernel", "cpu_count", "mem_total_b", "mem_available_b",
                "mem_used_pct", "load1", "load5", "load15", "disk_path", "disk_total_b",
                "disk_used_b", "disk_pct"}
    assert set(h) == expected
    # On the Linux CI/dev host these must be present and positive.
    assert isinstance(h["cpu_count"], int) and h["cpu_count"] > 0
    assert isinstance(h["mem_total_b"], int) and h["mem_total_b"] > 0
    assert isinstance(h["disk_total_b"], int) and h["disk_total_b"] > 0
    assert 0 <= h["disk_pct"] <= 100
    assert isinstance(h["hostname"], str) and h["hostname"]


def test_collect_never_raises(monkeypatch):
    # Even if /proc/meminfo is unreadable, collect() must not raise; mem fields -> None.
    monkeypatch.setattr(hostinfo, "_meminfo", lambda: (None, None))
    h = hostinfo.collect()
    assert h["mem_total_b"] is None and h["mem_used_pct"] is None
