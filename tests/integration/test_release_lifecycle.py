"""End-to-end: assign tickets to a release, transition all to terminal, confirm
the trigger enqueues the release_inspect task and a release_review row lands."""
from datetime import datetime

import pytest

import core.state as state
import core.queue as q
import core.runtime as rt
import features.releases as releases
import pm.release_runner as runner
from core.tasks.registry import TaskContext, run_task
import core.tasks  # noqa: F401  (registers tasks)


@pytest.fixture()
def _INST(tmp_state):
    return state.active_instance_key()


def _ticket(key, status, **extra):
    base = {"status": status, "slug": f"{key}-slug"}
    if status == "merged":
        base.setdefault("merged_external_status", "Done")
    base.update(extra)
    return base


def _stub_runtime(monkeypatch, instance_key, features_cfg):
    class _Reg:
        config = {"features": features_cfg, "workspace": {"root": "/tmp", "tickets_dir": "tickets"}}

    class _Inst:
        def keys(self): return [instance_key]
        def get(self, k): return _Reg()

    monkeypatch.setattr(rt, "instances", lambda: _Inst())


def test_status_change_to_terminal_triggers_inspection_via_task(_INST, monkeypatch, tmp_path):
    """Full lifecycle: link 2 tickets, transition both to terminal via
    save_ticket, confirm release_inspect job enqueued, run the task, confirm
    review row exists with verdict pass."""
    _stub_runtime(monkeypatch, _INST, {"releases": True})

    # Two tickets in same release; first already terminal, second still planning
    state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
    state.save_ticket("T-2", _ticket("T-2", "planning", release_key="v1.0"))
    rel = releases.upsert_release(_INST, "v1.0", title="Auth release")

    # Capture enqueued jobs
    enqueued: list[tuple] = []
    real_enqueue = q.enqueue_job

    def _capture(instance, task, payload=None, **kw):
        enqueued.append((instance, task, payload))
        return real_enqueue(instance, task, payload=payload, **kw)

    monkeypatch.setattr(q, "enqueue_job", _capture)

    # Transition T-2 to terminal — should fire trigger
    state.save_ticket("T-2", _ticket("T-2", "merged", release_key="v1.0"))

    # Note: save_ticket bypasses the trigger (only update_ticket calls it).
    # Use update_ticket to actually exercise the trigger hook.
    def _to_done(cur):
        cur = dict(cur)
        cur["status"] = "done"
        return cur

    state.update_ticket("T-2", _to_done)

    inspect_jobs = [e for e in enqueued if e[1] == "release_inspect"]
    assert len(inspect_jobs) >= 1, \
        f"expected release_inspect to be enqueued, got {enqueued}"
    assert inspect_jobs[0][2]["release_id"] == rel["id"]

    # Now run the task with a mocked LLM
    monkeypatch.setattr(runner, "run_haiku",
                        lambda *a, **k: '```json\n{"verdict":"pass","findings":[]}\n```')
    cfg = {"features": {"releases": True},
           "workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
    ctx = TaskContext(
        instance_key=_INST, ticket_key=None, task="release_inspect",
        payload={"release_id": rel["id"]}, job_id=0, triggering_event_id=None,
        config=cfg, registry=None, now=datetime.now(),
    )
    result = run_task(ctx)
    assert result.status == "ok"
    assert result.artifacts.get("verdict") == "pass"

    # Review row persisted
    review = releases.latest_review(rel["id"])
    assert review is not None, "review row must exist after task runs"
    assert review["verdict"] == "pass"

    # Release status flipped to inspected
    rel_after = releases.get_release_by_id(_INST, rel["id"])
    assert rel_after["status"] == "inspected"


def test_trigger_no_op_when_release_not_terminal(_INST, monkeypatch):
    _stub_runtime(monkeypatch, _INST, {"releases": True})
    state.save_ticket("T-1", _ticket("T-1", "planning", release_key="v1.0"))
    state.save_ticket("T-2", _ticket("T-2", "planning", release_key="v1.0"))
    releases.upsert_release(_INST, "v1.0")

    enqueued: list = []
    monkeypatch.setattr(q, "enqueue_job",
                        lambda *a, **k: enqueued.append((a, k)))

    def _to_merged(cur):
        cur = dict(cur)
        cur["status"] = "merged"
        cur["merged_external_status"] = "Done"
        return cur

    state.update_ticket("T-1", _to_merged)
    # Only one terminal — release not yet "done"
    inspect = [e for e in enqueued if e[0][1] == "release_inspect"]
    assert inspect == [], "must not enqueue when release isn't fully terminal"


def test_trigger_no_op_when_feature_off(_INST, monkeypatch):
    _stub_runtime(monkeypatch, _INST, {"releases": False})
    state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
    releases.upsert_release(_INST, "v1.0")

    enqueued: list = []
    monkeypatch.setattr(q, "enqueue_job",
                        lambda *a, **k: enqueued.append((a, k)))

    def _to_done(cur):
        cur = dict(cur)
        cur["status"] = "done"
        return cur

    state.update_ticket("T-1", _to_done)
    assert enqueued == [], "must not enqueue when feature flag is off"
