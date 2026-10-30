"""Every headless ticket stage that reports its start also reports its end.

The worker emits nothing for an ok job, so a stage that logged only
`*_started` left the feed showing the ticket mid-stage after the stage had
finished. LSC-248 reached pr_ready while the feed still read
ticket_prove_started as its last stage event. The end is reported only once
the postconditions pass, so a run that wrote no artifact reports no end.
"""
from unittest.mock import MagicMock, patch

import pytest

from core.tasks import tickets as T
from core.tasks.registry import run_task
from tests.core.test_pipeline_task_bodies import _ctx, _seed, _ticket_dir


def _events(emit: MagicMock) -> list[str]:
    return [c.args[0] for c in emit.call_args_list]


STAGES = [
    ("start_reviewing", "reviewing", None, ["change-manifest.md"],
     "tri-review.md", "VERDICT: PASS\n",
     "ticket_review_started", "ticket_review_complete"),
    ("generate_flow_doc", "reviewing", None, ["change-manifest.md"],
     "FLOW.html", "<html></html>\n",
     "ticket_flow_doc_started", "ticket_flow_doc_complete"),
    ("plan_tests", "testing", None, ["change-manifest.md"],
     "test-plan.md", "plan\n",
     "ticket_test_planning_started", "ticket_test_planning_complete"),
    ("write_tests", "testing", None, ["test-plan.md"],
     "test-files-written.txt", "tests/test_x.py\n",
     "ticket_test_writing_started", "ticket_test_writing_complete"),
    ("backfill_artifacts", "pr_ready", {"pr_url": "http://pr/1", "repo": "myrepo"}, [],
     "tri-review.md", "VERDICT: PASS\n",
     "ticket_backfill_started", "ticket_backfill_complete"),
    ("prove", "proving", None, [],
     "proof.md", "PROOF: DEMOED\n",
     "ticket_prove_started", "ticket_prove_complete"),
]


def _run(fake_config, task, status, payload, inputs, output, content, runner_result):
    _seed(status=status)
    docs = _ticket_dir(fake_config) / "docs"
    for name in inputs:
        (docs / name).write_text("input\n")
    (fake_config["workspace"]["root"] / "PROOF.md").write_text("# proof guide\n\ncontent\n")

    def runner(*args, **kwargs):
        if content is not None:
            (docs / output).write_text(content)
        return runner_result

    with patch("core.tasks.tickets.run_claude_code", side_effect=runner), \
         patch("core.tasks.tickets.repos_with_branch_diff", return_value=[]), \
         patch("core.tasks.tickets._commit_workspace_changes", return_value=[]), \
         patch("core.tasks.tickets._claim_session", return_value=(None, False)), \
         patch("core.tasks.tickets._kill_stray_recorders"), \
         patch("core.tasks.tickets.scope_fingerprint", return_value="r:x"), \
         patch("core.tasks.tickets.log.emit") as emit:
        result = run_task(_ctx(fake_config, task, payload=payload))
    return result, _events(emit)


@pytest.mark.parametrize("task,status,payload,inputs,output,content,started,complete", STAGES)
def test_an_ok_stage_reports_its_end(fake_config, tmp_state, task, status, payload,
                                     inputs, output, content, started, complete):
    result, events = _run(fake_config, task, status, payload, inputs, output, content,
                          runner_result="done")
    assert result.status == "ok", result.reason
    assert events.index(started) < events.index(complete)


@pytest.mark.parametrize("task,status,payload,inputs,output,content,started,complete", STAGES)
def test_a_stage_that_wrote_no_artifact_reports_no_end(fake_config, tmp_state, task, status,
                                                       payload, inputs, output, content,
                                                       started, complete):
    result, events = _run(fake_config, task, status, payload, inputs, output, None,
                          runner_result="done")
    assert result.status == "failed"
    assert started in events
    assert complete not in events


@pytest.mark.parametrize("task,status,payload,inputs,output,content,started,complete", STAGES)
def test_a_failed_agent_run_reports_no_end(fake_config, tmp_state, task, status, payload,
                                           inputs, output, content, started, complete):
    result, events = _run(fake_config, task, status, payload, inputs, output, None,
                          runner_result=None)
    assert result.status == "failed"
    assert complete not in events


def test_dev_complete_reaches_the_feed(fake_config, tmp_state):
    _seed(status="proving")
    with patch("core.events.dispatch") as dispatch, \
         patch("core.tasks.tickets.log.emit") as emit:
        T._fire_ticket_dev_complete(_ctx(fake_config, "mark_ready"))
    assert dispatch.call_args.args[0] == "ticket_dev_complete"
    assert "ticket_dev_complete" in _events(emit)
