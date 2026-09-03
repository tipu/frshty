"""Tests for the proposed state on the work board.

A proposal is a task frshty opened by itself. It sits on the board with no
run behind it until the operator approves it. These tests cover what the
board shows, what approval starts and what declining does. The tmux launch is
patched; nothing here starts an agent."""
from unittest.mock import patch

import pytest

import core.db as db
import core.state as state
from services import work_debrief, work_launch, work_store


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "personal"
    state._instance_key_cv.set("personal")
    yield


def _proposal(objective="Move WB-412 to the PLT board.", **kwargs):
    return work_store.create_proposal(
        objective, note="Proposed from Slack #platform: Erik asked",
        instance_key="atropos", contexts="atropos,slack",
        brief="\n\n## Slack conversation\n\nErik: please move it\n", **kwargs)


def _personal_config(tmp_path):
    root = tmp_path / "ws"
    root.mkdir(exist_ok=True)
    return {"job": {"key": "personal"}, "workspace": {"root": str(root)},
            "llm": {}, "_base_url": "http://localhost:8000"}


def test_a_proposal_lands_in_its_own_group_and_asks_for_attention():
    item_id = _proposal()
    groups = work_store.grouped_items()

    assert [row["id"] for row in groups["proposed"]] == [item_id]
    assert groups["agent_working"] == [], "a proposal has not started"
    assert work_store.attention_count() == 1
    assert db.query_one("SELECT state, launch_brief, current_checkpoint "
                        "FROM work_items WHERE id = ?",
                        (item_id,))["state"] == "proposed"


def test_a_proposal_has_no_run():
    item_id = _proposal()
    assert db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                        (item_id,)) == []


def test_declining_files_the_proposal_without_running_it():
    item_id = _proposal()
    result = work_store.apply_action(item_id, "decline")

    assert result == {"id": item_id, "action": "decline"}
    row = db.query_one("SELECT state, archived_at, stop_reason FROM work_items WHERE id = ?",
                       (item_id,))
    assert row["state"] == "done"
    assert row["archived_at"], "a declined proposal leaves the board"
    assert row["stop_reason"] == "Proposal declined"
    assert work_store.grouped_items()["proposed"] == []
    assert work_store.attention_count() == 0
    assert db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                        (item_id,)) == [], "declining never starts an agent"


def test_only_a_proposal_can_be_declined():
    item_id = work_store.create_item("an ordinary task")
    assert work_store.apply_action(item_id, "decline") == {
        "error": "only a proposed task can be declined"}


def test_approving_starts_the_agent_on_the_stored_objective(tmp_path):
    item_id = _proposal()
    config = _personal_config(tmp_path)
    with patch.object(work_launch, "personal_config", return_value=config), \
         patch.object(work_launch, "project_entries", return_value=[]), \
         patch.object(work_launch.terminal, "launch_agent") as launch_agent, \
         patch.object(work_launch.terminal, "session_healthy", return_value={"alive": True}), \
         patch.object(work_launch.threading, "Thread"), \
         patch.object(work_launch.work_tags, "schedule_implicit_tags"):
        result = work_launch.launch_proposed(item_id)

    assert result["item_id"] == item_id
    assert result["state"] == "agent_working"
    context = launch_agent.call_args[0][3]
    assert "Move WB-412 to the PLT board." in context
    assert "Erik: please move it" in context, "the brief reaches the session"
    row = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
    assert row["state"] == "agent_working"
    assert len(db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                            (item_id,))) == 1


def test_a_second_approval_finds_nothing_to_approve(tmp_path):
    item_id = _proposal()
    config = _personal_config(tmp_path)
    with patch.object(work_launch, "personal_config", return_value=config), \
         patch.object(work_launch, "project_entries", return_value=[]), \
         patch.object(work_launch.terminal, "launch_agent"), \
         patch.object(work_launch.terminal, "session_healthy", return_value={"alive": True}), \
         patch.object(work_launch.threading, "Thread"), \
         patch.object(work_launch.work_tags, "schedule_implicit_tags"):
        work_launch.launch_proposed(item_id)
        second = work_launch.launch_proposed(item_id)

    assert "not awaiting approval" in second["error"]
    assert len(db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                            (item_id,))) == 1, "one approval, one run"


def test_a_refused_launch_leaves_the_proposal_on_the_board():
    item_id = _proposal()
    with patch.object(work_launch, "personal_config", return_value=None):
        result = work_launch.launch_proposed(item_id)

    assert "personal instance" in result["error"]
    assert work_store.grouped_items()["proposed"][0]["id"] == item_id


def test_claiming_a_proposal_twice_succeeds_once():
    item_id = _proposal()
    assert work_store.claim_proposal(item_id) is True
    assert work_store.claim_proposal(item_id) is False


def test_releasing_returns_a_claimed_proposal():
    item_id = _proposal()
    work_store.claim_proposal(item_id)
    work_store.release_proposal(item_id)
    assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                        (item_id,))["state"] == "proposed"


def test_a_declined_proposal_never_enters_the_debrief_queue():
    declined = _proposal()
    work_store.apply_action(declined, "decline")
    ran = work_store.create_item("a task that actually ran")
    work_store.add_run(ran, "sess-debrief", "work-debrief", "/tmp")
    work_store.apply_action(ran, "done")

    assert work_debrief._pending_done_items() == [ran], (
        "a task with no run has no dialogue to debrief")


def test_a_proposal_refuses_every_action_but_decline():
    item_id = _proposal()
    for action in ("done", "snooze", "reopen", "archive", "autocontinue_off"):
        result = work_store.apply_action(item_id, action, until="2000-01-01T00:00:00+00:00")
        assert result == {"error": "a proposal can only be approved or declined"}, action
    assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                        (item_id,))["state"] == "proposed"


def test_a_launch_that_raises_puts_the_proposal_back(tmp_path):
    """The claim happens before the session is built. An artifact directory
    that cannot be written raises after the claim, and the proposal would
    otherwise sit in agent_working with no run, where the stale sweep cannot
    reach it."""
    item_id = _proposal()
    config = _personal_config(tmp_path)
    with patch.object(work_launch, "personal_config", return_value=config), \
         patch.object(work_launch, "project_entries", return_value=[]), \
         patch.object(work_launch.work_artifacts, "item_dir",
                      side_effect=PermissionError("read-only artifact root")):
        with pytest.raises(PermissionError):
            work_launch.launch_proposed(item_id)

    assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                        (item_id,))["state"] == "proposed"
    assert work_store.grouped_items()["proposed"][0]["id"] == item_id


def test_a_launch_that_raises_after_the_agent_started_keeps_the_task(tmp_path):
    """_start can raise after the session is live, when the kickoff thread or
    the tagging call fails. Releasing then would show a running agent as still
    waiting for approval, and the operator could decline it or approve it
    twice."""
    item_id = _proposal()
    config = _personal_config(tmp_path)
    with patch.object(work_launch, "personal_config", return_value=config), \
         patch.object(work_launch, "project_entries", return_value=[]), \
         patch.object(work_launch.terminal, "launch_agent"), \
         patch.object(work_launch.terminal, "session_healthy", return_value={"alive": True}), \
         patch.object(work_launch.threading, "Thread",
                      side_effect=RuntimeError("cannot start thread")):
        with pytest.raises(RuntimeError):
            work_launch.launch_proposed(item_id)

    assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                        (item_id,))["state"] == "agent_working"
    assert len(db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                            (item_id,))) == 1


def test_the_sweep_fails_a_task_whose_launch_never_made_a_run():
    """The launch writes the item, then the run. A process killed between the
    two leaves a row every other sweep pass joins away."""
    stranded = work_store.create_item("killed between the item and the run")
    ran = work_store.create_item("a normal running task")
    work_store.add_run(ran, "sess-sweep", "work-sweep", "/tmp")
    old = "2020-01-01T00:00:00+00:00"
    db.execute("UPDATE work_items SET updated_at = ? WHERE id IN (?, ?)",
               (old, stranded, ran))

    actions = work_store.fail_runless_items("2026-01-01T00:00:00+00:00")

    assert actions == [{"id": stranded, "action": "failed_without_run"}]
    assert db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?",
                        (stranded,))["stop_reason"] == "The launch never started a session"
    assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                        (ran,))["state"] == "agent_working"
