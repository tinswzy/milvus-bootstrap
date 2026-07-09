from milvus_bootstrap.core import progress
from milvus_bootstrap.core.tasks.engine import Step, TaskEngine


def test_publish_is_noop_without_sink():
    # No sink registered -> must not raise.
    progress.publish(object())


def test_set_and_reset_sink():
    seen = []
    token = progress.set_sink(lambda t: seen.append(t))
    try:
        progress.publish("x")
        assert seen == ["x"]
    finally:
        progress.reset_sink(token)
    progress.publish("y")          # after reset -> no-op
    assert seen == ["x"]


def test_engine_publishes_intermediate_steps():
    counts = []
    token = progress.set_sink(lambda t: counts.append(len(t.steps)))
    try:
        steps = [Step(name="a", plan="planA", action=lambda: "ra"),
                 Step(name="b", plan="planB", action=lambda: "rb")]
        task = TaskEngine().run(type="x", target="y", steps=steps, dry_run=False)
    finally:
        progress.reset_sink(token)
    # published while the task was still mid-flight: first publish saw only 1 step
    assert counts and counts[0] == 1 and counts[-1] == 2
    assert task.status.value == "succeeded"
    assert [s.status.value for s in task.steps] == ["ok", "ok"]
