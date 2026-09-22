"""Every review the /reviews pipeline starts also runs on the /tasks board.

The board task reviews the same staged diff with the ticket pipeline's lenses
and posts its findings back under the `tasks` provider, so the review page can
show either review. The launch is best effort: the pipeline review must finish
whatever the board does.
"""
import json
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from features import reviewer
from services import review_store


def _cfg(tmp_path):
    return {"_state_dir": Path(tmp_path), "_base_url": "http://board",
            "workspace": {"root": Path(tmp_path)},
            "reviewer": {"providers": ["claude"]}}


def _pr():
    return {"repo": "raven", "id": 4536, "branch": "claude/team-checkout-redirect",
            "url": "https://example/pr/4536"}


def _review_dir(tmp_path):
    d = Path(tmp_path) / "reviews" / "raven" / "claude-team-checkout-redirect"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _names(tmp_path):
    d = _review_dir(tmp_path)
    return sorted(p.name for p in d.iterdir())


@pytest.fixture()
def launched(tmp_path):
    """Launch one task review with the board stubbed out, and keep the call."""
    with patch.object(reviewer.work_launch, "launch",
                      return_value={"item_id": 77}) as launch, \
         patch.object(reviewer, "log"):
        item_id = reviewer.launch_task_review(_cfg(tmp_path), _pr(),
                                              _review_dir(tmp_path), None)
    return item_id, launch


def _token(tmp_path):
    """The token the objective of the open task review carries."""
    return json.loads((_review_dir(tmp_path) / "review.tasks.json").read_text())["token"]


class TestEveryPipelineReviewAlsoStartsATaskReview:
    def _run_ticket(self, tmp_path):
        """review_ticket is the path a PR that belongs to a frshty ticket takes
        when the /reviews page reruns it, and it never reaches review_pr."""
        cfg = _cfg(tmp_path)
        with patch.object(reviewer, "make_platform"), \
             patch.object(reviewer, "_ensure_review_worktree", return_value=None), \
             patch.object(reviewer, "_load_conventions", return_value=""), \
             patch.object(reviewer.presentation, "resolve_ticket_goal", return_value="goal"), \
             patch.object(reviewer, "_reviewed_sibling_sections", return_value=[]), \
             patch.object(reviewer, "_run_personas_for_providers", return_value={}), \
             patch.object(reviewer, "launch_task_review") as launch, \
             patch.object(reviewer, "log"):
            reviewer.review_ticket(cfg, "JIRA-1", [_pr()],
                                   diffs={"raven/4536": "diff --git a/a.ts b/a.ts\n"})
        return launch

    def test_a_ticket_grouped_review_launches_one_too(self, tmp_path):
        assert self._run_ticket(tmp_path).call_count == 1

    def test_the_ticket_path_reviews_the_diff_it_staged(self, tmp_path):
        launch = self._run_ticket(tmp_path)
        review_dir = launch.call_args.args[2]
        assert (review_dir / "diff.txt").read_text() == "diff --git a/a.ts b/a.ts\n"

    def _run(self, tmp_path):
        cfg = _cfg(tmp_path)
        with patch.object(reviewer, "_ensure_review_worktree", return_value=None), \
             patch.object(reviewer, "_load_conventions", return_value=""), \
             patch.object(reviewer, "_run_personas_for_providers", return_value={}), \
             patch.object(reviewer, "launch_task_review") as launch, \
             patch.object(reviewer, "log"):
            reviewer.review_pr(cfg, None, _pr(),
                               prefetched_diff="diff --git a/a.ts b/a.ts\n")
        return launch

    def test_review_pr_launches_one(self, tmp_path):
        assert self._run(tmp_path).call_count == 1

    def test_it_is_launched_even_when_no_provider_produced_a_review(self, tmp_path):
        """The personas returned nothing, so review_pr returns None. The board
        task is the second opinion, and it is most wanted exactly then."""
        assert self._run(tmp_path).call_count == 1

    def test_the_task_reviews_the_diff_the_pipeline_staged(self, tmp_path):
        launch = self._run(tmp_path)
        review_dir = launch.call_args.args[2]
        assert (review_dir / "diff.txt").read_text() == "diff --git a/a.ts b/a.ts\n"


class TestTheLaunchIsBestEffort:
    def test_a_board_that_refuses_does_not_fail_the_review(self, tmp_path):
        with patch.object(reviewer.work_launch, "launch",
                          return_value={"error": "personal instance not loaded"}), \
             patch.object(reviewer, "log") as log:
            assert reviewer.launch_task_review(_cfg(tmp_path), _pr(),
                                               _review_dir(tmp_path), None) is None
        assert log.emit.call_args.args[0] == "review_task_launch_failed"

    def test_a_refused_launch_leaves_no_pending_task_review(self, tmp_path):
        """A placeholder left behind shows the page a task review that will
        never arrive, and lets a later POST from an unrelated run land on it."""
        with patch.object(reviewer.work_launch, "launch",
                          return_value={"error": "personal instance not loaded"}), \
             patch.object(reviewer, "log"):
            reviewer.launch_task_review(_cfg(tmp_path), _pr(), _review_dir(tmp_path), None)
        assert _names(tmp_path) == []

    def test_a_failed_launch_leaves_a_later_launch_its_placeholder(self, tmp_path):
        """A rerun can open a new task review while the previous launch is still
        failing. Taking back a placeholder it no longer owns would 404 the task
        that is on its way."""
        review_dir = _review_dir(tmp_path)
        live = {"pr_id": 4536, "repo": "raven", "status": "reviewing", "token": "live"}

        def fail_after_a_rerun(*_args, **_kwargs):
            (review_dir / "review.tasks.json").write_text(json.dumps(live))
            return {"error": "personal instance not loaded"}

        with patch.object(reviewer.work_launch, "launch", fail_after_a_rerun), \
             patch.object(reviewer, "log"):
            reviewer.launch_task_review(_cfg(tmp_path), _pr(), review_dir, None)
        assert json.loads(
            (review_dir / "review.tasks.json").read_text())["token"] == "live"

    def test_nothing_is_written_until_the_board_has_the_task(self, tmp_path):
        """A placeholder written first is a placeholder a launch that then fails
        has to take back, and taking one back is how a later launch's task loses
        the slot it was promised."""
        seen = {}

        def record(*_args, **_kwargs):
            seen["names"] = _names(tmp_path)
            return {"item_id": 9}

        with patch.object(reviewer.work_launch, "launch", record), \
             patch.object(reviewer, "log"):
            reviewer.launch_task_review(_cfg(tmp_path), _pr(), _review_dir(tmp_path), None)
        assert seen["names"] == []
        assert _names(tmp_path) == ["queued_comments.tasks.json", "review.tasks.json"]

    def test_a_board_that_raises_does_not_fail_the_review(self, tmp_path):
        with patch.object(reviewer.work_launch, "launch",
                          side_effect=RuntimeError("tmux is gone")), \
             patch.object(reviewer, "log") as log:
            assert reviewer.launch_task_review(_cfg(tmp_path), _pr(),
                                               _review_dir(tmp_path), None) is None
        assert "RuntimeError: tmux is gone" in log.emit.call_args.args[1]
        assert _names(tmp_path) == []


class TestAPendingTaskReviewIsFindable:
    def test_the_launch_writes_a_pending_placeholder(self, tmp_path, launched):
        item_id, _ = launched
        assert item_id == 77
        assert _names(tmp_path) == ["queued_comments.tasks.json", "review.tasks.json"]

    def test_the_store_can_find_it_before_the_findings_arrive(self, tmp_path, launched):
        """store_task_review resolves the branch directory through this lookup,
        so a POST that beats the pipeline's own review still lands."""
        found = review_store.find_review(Path(tmp_path), "raven", 4536, provider="tasks")
        assert found is not None
        assert found[0] == _review_dir(tmp_path)

    def test_the_pending_review_is_not_reported_as_a_verdict(self, tmp_path, launched):
        data = json.loads((_review_dir(tmp_path) / "review.tasks.json").read_text())
        assert data["status"] == "reviewing"
        assert data["verdict"] == ""
        assert data["issues"] == []

    def test_it_does_not_touch_the_pipeline_review(self, tmp_path, launched):
        assert not (_review_dir(tmp_path) / "review.json").exists()
        assert not (_review_dir(tmp_path) / "queued_comments.json").exists()

    def test_the_task_runs_where_the_review_does_and_makes_no_worktree(
            self, tmp_path, launched):
        _item_id, launch = launched
        assert launch.call_args.kwargs["cwd"] == str(_review_dir(tmp_path))
        assert launch.call_args.kwargs["no_worktree"] is True


class TestTheObjectiveCarriesTheArm:
    def _objective(self, tmp_path, worktree=None):
        return reviewer._task_review_objective(_cfg(tmp_path), _pr(),
                                               _review_dir(tmp_path), worktree, "tok")

    def test_it_fans_out_with_no_lane_split(self, tmp_path):
        text = self._objective(tmp_path)
        assert "Run three independent reviewers in parallel as sub-agents" in text
        assert "There is no persona split and no lane" in text

    def test_an_already_merged_defect_is_demoted_not_dropped(self, tmp_path):
        text = self._objective(tmp_path)
        assert "Demote that finding. Never drop it." in text
        assert 'set its severity to "suggestion"' in text
        assert "Pre-existing (<commit>, <author>):" in text

    def test_it_asks_for_clearances(self, tmp_path):
        assert "CLEARANCES:" in self._objective(tmp_path)

    def test_it_names_the_staged_diff_and_the_post_it_owes(self, tmp_path):
        text = self._objective(tmp_path)
        assert str(_review_dir(tmp_path) / "diff.txt") in text
        assert "http://board/api/reviews/raven/4536/task-review?token=tok" in text
        assert "The POST is the deliverable." in text

    def test_it_forbids_editing_the_code_it_reviews(self, tmp_path):
        text = self._objective(tmp_path)
        assert "Do not modify any source file, do not commit" in text

    def test_it_carries_the_severity_and_schema_rules_the_pipeline_uses(self, tmp_path):
        text = self._objective(tmp_path)
        assert "Silent data loss is blocking." in text
        assert '"issues":[' in text

    def test_it_names_the_checkout_when_there_is_one(self, tmp_path):
        assert "checked out read-only at /wt" in self._objective(tmp_path, worktree="/wt")

    def test_it_says_so_when_there_is_no_checkout(self, tmp_path):
        assert "no checkout of this branch" in self._objective(tmp_path)


def _finding(severity="blocking", body="the write is dropped", path="a.ts", line=3):
    return {"path": path, "line": line, "body": body, "severity": severity}


class TestStoringWhatTheTaskFound:
    def _store(self, tmp_path, review, token=None):
        if token is None:
            token = _token(tmp_path)
        return reviewer.store_task_review(_cfg(tmp_path), "raven", 4536, review, token)

    def test_no_open_task_review_is_reported_not_invented(self, tmp_path):
        _review_dir(tmp_path)
        assert reviewer.store_task_review(_cfg(tmp_path), "raven", 4536,
                                          {"issues": []}, "tok") is None

    def test_the_objective_and_the_placeholder_carry_the_same_token(
            self, tmp_path, launched):
        _item_id, launch = launched
        assert f"task-review?token={_token(tmp_path)}" in launch.call_args.args[0]

    def test_a_task_that_does_not_know_the_token_stores_nothing(self, tmp_path, launched):
        assert self._store(tmp_path, {"issues": [_finding()]}, token="wrong") is None
        assert json.loads(
            (_review_dir(tmp_path) / "queued_comments.tasks.json").read_text()) == []

    def test_an_empty_token_stores_nothing(self, tmp_path, launched):
        assert self._store(tmp_path, {"issues": [_finding()]}, token="") is None

    def test_a_stored_review_cannot_be_replaced(self, tmp_path, launched):
        """A second POST with the same token, after the first one stored a
        blocking finding, would otherwise approve the pull request."""
        token = _token(tmp_path)
        self._store(tmp_path, {"issues": [_finding()]}, token=token)
        assert self._store(tmp_path, {"issues": []}, token=token) is None
        merged = json.loads((_review_dir(tmp_path) / "review.tasks.json").read_text())
        assert merged["verdict"] == "changes_requested"

    def test_a_rerun_shuts_out_the_task_the_previous_run_started(self, tmp_path):
        """The stale task would otherwise file findings it made against the
        previous revision's diff against the one on the page now."""
        with patch.object(reviewer.work_launch, "launch",
                          return_value={"item_id": 1}), patch.object(reviewer, "log"):
            reviewer.launch_task_review(_cfg(tmp_path), _pr(), _review_dir(tmp_path), None)
            stale = _token(tmp_path)
            reviewer.launch_task_review(_cfg(tmp_path), _pr(), _review_dir(tmp_path), None)
        assert _token(tmp_path) != stale
        assert self._store(tmp_path, {"issues": [_finding()]}, token=stale) is None
        assert self._store(tmp_path, {"issues": [_finding()]}) is not None

    def test_two_posts_that_arrive_together_are_not_both_accepted(
            self, tmp_path, launched):
        """A retried curl can put two posts in flight at once. Accepting both
        lets the one that writes last decide, so an empty review erases the
        blocking finding the other one carried."""
        token = _token(tmp_path)
        real = reviewer._write_review_files
        gate = threading.Barrier(2)
        stored, cfg = [], _cfg(tmp_path)

        def slow(*args, **kwargs):
            time.sleep(0.05)
            return real(*args, **kwargs)

        def post(review):
            gate.wait()
            stored.append(reviewer.store_task_review(cfg, "raven", 4536, review, token))

        with patch.object(reviewer, "_write_review_files", slow):
            threads = [threading.Thread(target=post, args=(r,))
                       for r in ({"issues": [_finding()]}, {"issues": []})]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        assert sum(r is not None for r in stored) == 1
        merged = json.loads((_review_dir(tmp_path) / "review.tasks.json").read_text())
        accepted = next(r for r in stored if r is not None)
        assert merged["verdict"] == accepted["verdict"]
        assert len(merged["issues"]) == len(accepted["issues"])

    def test_the_token_is_not_kept_once_the_findings_are_in(self, tmp_path, launched):
        self._store(tmp_path, {"issues": [_finding()]})
        assert "token" not in json.loads(
            (_review_dir(tmp_path) / "review.tasks.json").read_text())

    def test_the_findings_become_comments_under_the_tasks_provider(
            self, tmp_path, launched):
        self._store(tmp_path, {"summary": "s", "issues": [_finding()]})
        queued = json.loads(
            (_review_dir(tmp_path) / "queued_comments.tasks.json").read_text())
        assert [c["body"] for c in queued] == ["the write is dropped"]
        assert queued[0]["found_by"] == ["tasks"]
        assert queued[0]["path"] == "a.ts"

    def test_a_blocking_finding_makes_the_verdict_changes_requested(
            self, tmp_path, launched):
        merged = self._store(tmp_path, {"issues": [_finding()]})
        assert merged["verdict"] == "changes_requested"

    def test_a_verdict_the_agent_claims_never_beats_its_own_findings(
            self, tmp_path, launched):
        """The page shows the verdict beside the comment list, so a claimed
        'approved' over a blocking finding is a lie the store must not keep."""
        merged = self._store(tmp_path, {"verdict": "approved", "issues": [_finding()]})
        assert merged["verdict"] == "changes_requested"

    def test_no_blocking_finding_is_approved(self, tmp_path, launched):
        merged = self._store(tmp_path, {"issues": [_finding(severity="suggestion")]})
        assert merged["verdict"] == "approved"

    def test_an_unknown_severity_becomes_a_suggestion(self, tmp_path, launched):
        """An invented severity must not be read as blocking, and must not blank
        the badge the page renders from it."""
        merged = self._store(tmp_path, {"issues": [_finding(severity="critical")]})
        assert merged["issues"][0]["severity"] == "suggestion"
        assert merged["verdict"] == "approved"

    def test_a_finding_with_no_body_is_dropped(self, tmp_path, launched):
        merged = self._store(tmp_path, {"issues": [_finding(body=""), _finding()]})
        assert len(merged["issues"]) == 1

    def test_it_keeps_the_pr_it_was_opened_for(self, tmp_path, launched):
        self._store(tmp_path, {"issues": [_finding()]})
        data = json.loads((_review_dir(tmp_path) / "review.tasks.json").read_text())
        assert data["pr_id"] == 4536
        assert data["pr_url"] == "https://example/pr/4536"
        assert data["status"] == "done"

    def test_it_does_not_restage_the_diff(self, tmp_path, launched):
        """The pipeline staged diff.txt. An ingest that rewrote it would replace
        the diff every comment on the page is anchored to."""
        (_review_dir(tmp_path) / "diff.txt").write_text("the staged diff\n")
        self._store(tmp_path, {"issues": [_finding()]})
        assert (_review_dir(tmp_path) / "diff.txt").read_text() == "the staged diff\n"

    def test_it_leaves_the_pipeline_review_alone(self, tmp_path, launched):
        d = _review_dir(tmp_path)
        (d / "review.json").write_text('{"verdict": "approved", "pr_id": 4536}')
        (d / "queued_comments.json").write_text("[]")
        self._store(tmp_path, {"issues": [_finding()]})
        assert json.loads((d / "review.json").read_text())["verdict"] == "approved"
        assert json.loads((d / "queued_comments.json").read_text()) == []


class TestTheReviewPageTogglesBetweenTheTwoReviews:
    """The toggle used to pick between two model providers in an A/B. It now
    picks between the pipeline review and the board task review, and its values
    are the provider ids the store writes, so a rename on one side alone shows
    an empty review page."""

    def _template(self):
        return Path("templates/review_detail.html").read_text()

    def test_it_offers_the_pipeline_review(self):
        assert 'value="claude" v-model="provider" class="accent-green-500"> pipeline' in self._template()

    def test_it_offers_the_task_review(self):
        assert 'value="tasks" v-model="provider" class="accent-blue-500"> task' in self._template()

    def test_the_task_value_is_the_provider_the_store_writes(self):
        assert reviewer.TASK_REVIEW_PROVIDER == "tasks"
        assert review_store.provider_suffix(reviewer.TASK_REVIEW_PROVIDER) == ".tasks"

    def test_the_old_ab_labels_are_gone(self):
        template = self._template()
        assert "> claude\n" not in template
        assert "> codex\n" not in template
