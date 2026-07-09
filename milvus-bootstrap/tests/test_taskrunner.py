import threading
import time

from milvus_bootstrap.core import progress
from milvus_bootstrap.core.taskrunner import TaskRunner


def _wait(runner, tid, timeout=3.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        rec = runner.get(tid)
        if rec and rec["state"] != "running":
            return rec
        time.sleep(0.01)
    return runner.get(tid)


def test_runner_success():
    r = TaskRunner()
    tid = r.submit(lambda: "hello")
    assert isinstance(tid, str) and tid
    rec = _wait(r, tid)
    assert rec["state"] == "done" and rec["result"] == "hello" and rec["error"] is None


def test_runner_error():
    r = TaskRunner()
    def boom():
        raise ValueError("nope")
    rec = _wait(r, r.submit(boom))
    assert rec["state"] == "error" and "nope" in rec["error"] and rec["result"] is None


def test_runner_unknown_id():
    assert TaskRunner().get("does-not-exist") is None


def test_runner_running_before_done():
    gate = threading.Event()
    r = TaskRunner()
    tid = r.submit(lambda: gate.wait(2) or "ok")
    assert r.get(tid)["state"] == "running"   # still blocked
    gate.set()
    assert _wait(r, tid)["state"] == "done"


class _FakeTask:
    def __init__(self, n):
        self.n = n
    def model_dump(self, mode=None):
        return {"steps": [{"name": f"s{i}"} for i in range(self.n)], "status": "running"}


def test_partial_visible_while_running():
    r = TaskRunner()
    gate = threading.Event()

    def fn():
        progress.publish(_FakeTask(2))   # engine would do this
        gate.wait(2)
        return "final"

    tid = r.submit(fn)
    partial = None
    for _ in range(200):
        rec = r.get(tid)
        if rec and rec.get("partial"):
            partial = rec["partial"]
            break
        time.sleep(0.01)
    gate.set()
    assert partial == {"steps": [{"name": "s0"}, {"name": "s1"}], "status": "running"}
    # after completion
    for _ in range(200):
        rec = r.get(tid)
        if rec["state"] != "running":
            break
        time.sleep(0.01)
    assert rec["state"] == "done" and rec["result"] == "final"


def test_get_unknown_is_none():
    assert TaskRunner().get("nope") is None
