from unittest.mock import patch, MagicMock

import pytest

from features import tickets
from tests.conftest import make_ticket, make_ticket_state


class TestMakeSlug:
    def test_basic(self):
        assert tickets._make_slug("PROJ-1", "Fix the login bug") == "PROJ-1-fix-the-login-bug"

    def test_truncates_to_7_words(self):
        slug = tickets._make_slug("T-1", "one two three four five six seven eight nine")
        parts = slug.replace("T-1-", "").split("-")
        assert len(parts) <= 7

    def test_special_chars(self):
        slug = tickets._make_slug("T-1", "Fix: the @#$ thing!!!")
        assert "@" not in slug
        assert "#" not in slug

    def test_empty_summary(self):
        assert tickets._make_slug("T-1", "") == "T-1"


class TestResolveStatus:
    def test_mapped_status(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"status_map": {"In Progress": "planning"}}}
        assert tickets._resolve_status(config, "In Progress") == "planning"

    def test_unmapped_returns_none(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"status_map": {"Done": "done"}}}
        assert tickets._resolve_status(config, "In Progress") is None

    def test_no_status_map(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {}}
        assert tickets._resolve_status(config, "In Progress") is None

    def test_no_ticket_system(self):
        config = {"job": {}}
        assert tickets._resolve_status(config, "In Progress") is None


class TestImageFilename:
    def test_from_alt(self):
        assert tickets._image_filename("Screenshot", "http://x/img") == "Screenshot.png"

    def test_from_url(self):
        result = tickets._image_filename("", "http://x/image.jpg")
        assert result == "image.jpg"

    def test_dedup(self):
        seen = set()
        f1 = tickets._image_filename("pic", "http://x/a", seen)
        f2 = tickets._image_filename("pic", "http://x/b", seen)
        assert f1 != f2
        assert "_2" in f2


class TestLocalizeImages:
    def test_replaces_remote_with_local(self, tmp_path):
        docs = tmp_path / "docs"
        att = docs / "attachments"
        att.mkdir(parents=True)
        (att / "pic.png").write_bytes(b"fake")
        md = "![pic](https://example.com/pic.png)"
        result = tickets._localize_images(md, docs)
        assert "attachments/pic.png" in result

    def test_keeps_undownloaded(self, tmp_path):
        docs = tmp_path / "docs"
        docs.mkdir(parents=True)
        md = "![pic](https://example.com/missing.png)"
        result = tickets._localize_images(md, docs)
        assert "https://example.com/missing.png" in result


class TestRepoGate:
    """Per-repo serialization gate: at most one ticket per instance can occupy
    the pipeline at a time (status in planning/reviewing/pr_ready/in_review).
    Prevents concurrent PRs against the same base producing unresolvable merge conflicts.
    """

    def test_gate_clear_when_no_other_tickets(self, fresh_db):
        import core.state as state
        state.init("inst")
        assert tickets._repo_gate_blocked("inst", "T-1") is None

    def test_gate_clear_when_other_tickets_terminal(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "merged", "slug": "x", "branch": "x",
                                   "merged_external_status": "Done"})
        state.save_ticket("T-3", {"status": "done", "slug": "x", "branch": "x"})
        state.save_ticket("T-4", {"status": "pr_failed", "slug": "x", "branch": "x"})
        state.save_ticket("T-5", {"status": "new"})
        assert tickets._repo_gate_blocked("inst", "T-1") is None

    def test_gate_blocks_when_other_new_with_slug(self, fresh_db):
        """A ticket whose worktree has been materialized (slug set) but whose
        status is still "new" (start_planning hasn't fired yet) holds the gate.
        Without this, two setup_prd_ticket workers can race past the gate."""
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "new", "slug": "T-2-slug", "branch": "T-2"})
        assert tickets._repo_gate_blocked("inst", "T-1") == "T-2"

    def test_gate_blocked_when_other_in_planning(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "planning", "slug": "x", "branch": "x"})
        assert tickets._repo_gate_blocked("inst", "T-1") == "T-2"

    def test_gate_blocked_when_other_in_reviewing(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "reviewing", "slug": "x", "branch": "x"})
        assert tickets._repo_gate_blocked("inst", "T-1") == "T-2"

    def test_gate_blocked_when_other_in_pr_ready(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "pr_ready", "slug": "x", "branch": "x"})
        assert tickets._repo_gate_blocked("inst", "T-1") == "T-2"

    def test_gate_blocked_when_other_in_review(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "in_review", "slug": "x", "branch": "x"})
        assert tickets._repo_gate_blocked("inst", "T-1") == "T-2"

    def test_gate_excludes_self(self, fresh_db):
        """A ticket already in the pipeline can advance — the gate only blocks
        OTHER tickets, not the ticket itself transitioning further."""
        import core.state as state
        state.init("inst")
        state.save_ticket("T-1", {"status": "planning", "slug": "x", "branch": "x"})
        assert tickets._repo_gate_blocked("inst", "T-1") is None

    def test_gate_isolates_per_instance(self, fresh_db):
        """A ticket in a different instance does not block this one."""
        import core.state as state
        state.init("inst-a")
        state.save_ticket("T-A1", {"status": "planning", "slug": "x", "branch": "x"})
        state.init("inst-b")
        assert tickets._repo_gate_blocked("inst-b", "T-B1") is None

    def test_enqueue_stage_skips_setup_prd_ticket_when_blocked(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "planning", "slug": "x", "branch": "x"})
        with patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "setup_prd_ticket")
            eq.assert_not_called()

    def test_enqueue_stage_skips_start_planning_when_blocked(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "in_review", "slug": "x", "branch": "x"})
        with patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_not_called()

    def test_enqueue_stage_skips_mark_ready_when_blocked(self, fresh_db):
        import core.state as state
        state.init("inst")
        state.save_ticket("T-2", {"status": "in_review", "slug": "x", "branch": "x"})
        with patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "mark_ready")
            eq.assert_not_called()

    def test_enqueue_stage_proceeds_when_gate_clear(self, fresh_db):
        import core.state as state
        state.init("inst")
        with patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_called_once_with("inst", "start_planning", ticket_key="T-1")

    def test_enqueue_stage_does_not_gate_non_pipeline_tasks(self, fresh_db):
        """resolve_conflicts, fix_ci_failures etc. happen DURING in_review for
        the active ticket — they must not be gated."""
        import core.state as state
        state.init("inst")
        state.save_ticket("T-1", {"status": "in_review", "slug": "x", "branch": "x"})
        for task in ("resolve_conflicts", "fix_ci_failures", "validate_merged_ticket"):
            with patch("core.queue.jobs_for_ticket", return_value=[]), \
                 patch("core.queue.enqueue_job") as eq:
                tickets._enqueue_stage("inst", "T-1", task)
                eq.assert_called_once()


class TestEnqueueStage:
    def test_enqueues_when_no_existing(self):
        with patch("core.queue.jobs_for_ticket", return_value=[]) as qj, \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "start_planning")
            qj.assert_called_once_with("inst", "T-1", limit=200)
            eq.assert_called_once_with("inst", "start_planning", ticket_key="T-1")

    def test_skips_when_already_queued(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "start_planning", "status": "queued"}]), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_not_called()

    def test_skips_when_already_running(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "start_planning", "status": "running"}]), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_not_called()

    def test_enqueues_when_only_finished_exist(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "start_planning", "status": "ok"}]), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_called_once()

    def test_caps_at_5_consecutive_recent_failures(self):
        from datetime import datetime, timezone
        recent = datetime.now(timezone.utc).isoformat()
        jobs = [{"task": "non_llm_task", "status": "failed", "finished_at": recent}] * 5
        with patch("core.queue.jobs_for_ticket", return_value=jobs), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "non_llm_task")
            eq.assert_not_called()

    def test_old_failures_age_out_of_cap_window(self):
        from datetime import datetime, timezone, timedelta
        old = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        jobs = [{"task": "non_llm_task", "status": "failed", "finished_at": old}] * 5
        with patch("core.queue.jobs_for_ticket", return_value=jobs), \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "non_llm_task")
            eq.assert_called_once()


class TestCreatePr:
    def test_no_diff_marks_merged(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        slug = "PROJ-1-slug"
        wt = tmp_path / "tickets" / slug / "myrepo"
        wt.mkdir(parents=True)

        ts = make_ticket_state(status="pr_ready", slug=slug, branch="PROJ-1-slug")
        ticket = make_ticket()

        mock_platform = MagicMock()
        mock_subprocess = MagicMock(returncode=0, stdout=b"")
        diff_result = MagicMock(returncode=0, stdout="")

        def fake_run(cmd, *a, **kw):
            if "diff" in cmd:
                return diff_result
            return mock_subprocess

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.get_repos", return_value=[{"name": "myrepo", "path": tmp_path / "myrepo"}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.subprocess.run", side_effect=fake_run), \
             patch("features.tickets.run_haiku", return_value="Short summary"), \
             patch("features.tickets.log"):
            result = tickets._create_pr(fake_config, ticket, ts, "http://base")
        assert result["status"] == "merged"

    def test_pr_failure_increments_attempts(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        slug = "PROJ-1-slug"
        wt = tmp_path / "tickets" / slug / "myrepo"
        wt.mkdir(parents=True)

        ts = make_ticket_state(status="pr_ready", slug=slug, branch="PROJ-1-slug")
        ticket = make_ticket()

        mock_platform = MagicMock()
        mock_platform.push_branch.return_value = {"ok": True}
        mock_platform.create_pr.return_value = {"error": "something broke"}

        diff_result = MagicMock(returncode=0, stdout="file.py | 5 +++++")

        def fake_run(cmd, *a, **kw):
            if "diff" in cmd:
                return diff_result
            return MagicMock(returncode=0, stdout=b"PROJ-1-slug\n")

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.get_repos", return_value=[{"name": "myrepo", "path": tmp_path / "myrepo"}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.subprocess.run", side_effect=fake_run), \
             patch("features.tickets.run_haiku", return_value="Summary"), \
             patch("features.tickets.log"):
            result = tickets._create_pr(fake_config, ticket, ts, "http://base")
        assert result.get("pr_attempts", 0) >= 1


class TestResolveConflicts:
    def test_no_prs_noop(self, fake_config):
        ts = make_ticket_state(status="in_review")
        result = tickets._resolve_conflicts(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "in_review"

    def test_not_conflicting_noop(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"mergeable": "MERGEABLE"}
        ts = make_ticket_state(status="in_review", prs=[{"repo": "r", "id": 1, "url": "http://u"}])

        with patch("features.tickets.make_platform", return_value=mock_platform):
            result = tickets._resolve_conflicts(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "in_review"

    def test_max_attempts_transitions_to_failed(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"mergeable": "CONFLICTING"}
        ts = make_ticket_state(
            status="in_review",
            slug="PROJ-1-slug",
            prs=[{"repo": "r", "id": 1, "url": "http://u"}],
            conflict_resolution_attempts=2,
        )

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._resolve_conflicts(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_failed"


class TestHasConflictingPr:
    def test_no_prs_returns_false(self, fake_config):
        assert tickets._has_conflicting_pr(fake_config, {"prs": []}) is False

    def test_any_conflicting_returns_true(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_pr_info.side_effect = [
            {"mergeable": "MERGEABLE"},
            {"mergeable": "CONFLICTING"},
        ]
        ts = {"prs": [{"repo": "r", "id": 1}, {"repo": "r", "id": 2}]}
        with patch("features.tickets.make_platform", return_value=mock_platform):
            assert tickets._has_conflicting_pr(fake_config, ts) is True

    def test_all_clean_returns_false(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"mergeable": "MERGEABLE"}
        ts = {"prs": [{"repo": "r", "id": 1}]}
        with patch("features.tickets.make_platform", return_value=mock_platform):
            assert tickets._has_conflicting_pr(fake_config, ts) is False

    def test_get_pr_info_exception_does_not_crash(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_pr_info.side_effect = RuntimeError("api down")
        ts = {"prs": [{"repo": "r", "id": 1}]}
        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            assert tickets._has_conflicting_pr(fake_config, ts) is False


class TestResolveConflictsPending:
    def test_no_instance_key_returns_false(self):
        assert tickets._resolve_conflicts_pending("", "T-1") is False

    def test_queued_resolve_conflicts_returns_true(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "resolve_conflicts", "status": "queued"}]):
            assert tickets._resolve_conflicts_pending("inst", "T-1") is True

    def test_running_resolve_conflicts_returns_true(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "resolve_conflicts", "status": "running"}]):
            assert tickets._resolve_conflicts_pending("inst", "T-1") is True

    def test_finished_resolve_conflicts_returns_false(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "resolve_conflicts", "status": "ok"}]):
            assert tickets._resolve_conflicts_pending("inst", "T-1") is False

    def test_other_task_inflight_returns_false(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "fix_ci_failures", "status": "running"}]):
            assert tickets._resolve_conflicts_pending("inst", "T-1") is False


class TestCheckEnqueuesResolveConflicts:
    def test_in_review_with_conflicting_pr_enqueues_and_skips_ci(self, fake_config, tmp_state):
        import core.state as state
        from tests.conftest import make_ticket
        slug = "PROJ-1-do-the-thing"
        state.save_ticket("PROJ-1", make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "r", "id": 1, "branch": slug, "url": "http://u"}],
        ))
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"mergeable": "CONFLICTING"}

        with patch("features.tickets._fetch_tickets",
                   return_value=[make_ticket(status="In Progress")]), \
             patch("features.tickets._fetch_open_prs",
                   return_value=[{"repo": "r", "id": 1, "branch": slug, "url": "http://u"}]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "r", "path": tmp_state / "r"}]), \
             patch("features.tickets._resolve_status", return_value=None), \
             patch("features.tickets._process_ticket_comments"), \
             patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._enqueue_stage") as eq:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        eq.assert_any_call("inst", "PROJ-1", "resolve_conflicts")
        mock_platform.monitor_ci.assert_not_called()

    def test_in_review_with_pending_resolve_skips_ci(self, fake_config, tmp_state):
        import core.state as state
        from tests.conftest import make_ticket
        slug = "PROJ-1-do-the-thing"
        state.save_ticket("PROJ-1", make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "r", "id": 1, "branch": slug, "url": "http://u"}],
        ))
        mock_platform = MagicMock()

        def jobs_for_ticket_fake(instance_key, ticket_key, limit=100):
            return [{"task": "resolve_conflicts", "status": "queued",
                     "id": 1, "enqueued_at": None, "started_at": None,
                     "finished_at": None, "response": None, "payload": "{}"}]

        with patch("features.tickets._fetch_tickets",
                   return_value=[make_ticket(status="In Progress")]), \
             patch("features.tickets._fetch_open_prs",
                   return_value=[{"repo": "r", "id": 1, "branch": slug, "url": "http://u"}]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "r", "path": tmp_state / "r"}]), \
             patch("features.tickets._resolve_status", return_value=None), \
             patch("features.tickets._process_ticket_comments"), \
             patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("core.queue.jobs_for_ticket", side_effect=jobs_for_ticket_fake), \
             patch("features.tickets._enqueue_stage"):
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        mock_platform.monitor_ci.assert_not_called()
        mock_platform.get_pr_info.assert_not_called()


class TestReconcilePrs:
    def test_match_by_branch_populates_prs(self):
        open_prs = [
            {"repo": "r", "id": 99, "branch": "other-branch", "url": "u1"},
            {"repo": "r", "id": 100, "branch": "PROJ-1-do-the-thing", "url": "u2"},
        ]
        ts = make_ticket_state(status="in_review", branch="PROJ-1-do-the-thing",
                               prs=[{"repo": "r", "id": 100, "branch": "PROJ-1-do-the-thing", "url": "u2"}])

        result = tickets._reconcile_prs(ts, open_prs)

        assert result["prs"] == [{"repo": "r", "id": 100, "branch": "PROJ-1-do-the-thing", "url": "u2"}]

    def test_no_match_leaves_ts_unchanged(self):
        open_prs = [
            {"repo": "r", "id": 99, "branch": "other-branch", "url": "u1"},
        ]
        ts = make_ticket_state(status="in_review", branch="PROJ-1-do-the-thing")

        result = tickets._reconcile_prs(ts, open_prs)

        assert "prs" not in result

    def test_multiple_matches_all_included(self):
        open_prs = [
            {"repo": "a", "id": 1, "branch": "shared-branch", "url": "u1"},
            {"repo": "b", "id": 2, "branch": "shared-branch", "url": "u2"},
            {"repo": "c", "id": 3, "branch": "other", "url": "u3"},
        ]
        ts = make_ticket_state(status="in_review", branch="shared-branch",
                               prs=[{"repo": "a", "id": 1, "branch": "shared-branch", "url": "u1"},
                                    {"repo": "b", "id": 2, "branch": "shared-branch", "url": "u2"}])

        result = tickets._reconcile_prs(ts, open_prs)

        assert len(result["prs"]) == 2
        assert {p["repo"] for p in result["prs"]} == {"a", "b"}

    def test_advances_pr_ready_and_resets_counters(self):
        open_prs = [{"repo": "r", "id": 100, "branch": "PROJ-1", "url": "u"}]
        ts = make_ticket_state(status="pr_ready", branch="PROJ-1")
        ts["conflict_resolution_attempts"] = 2
        ts["ci_fix_attempts"] = 2
        ts["ci_passed"] = True
        ts["checks_started_at"] = "2026-01-01T00:00:00+00:00"

        result = tickets._reconcile_prs(ts, open_prs)

        assert result["status"] == "in_review"
        assert result["conflict_resolution_attempts"] == 0
        assert result["ci_fix_attempts"] == 0
        assert "ci_passed" not in result
        assert "checks_started_at" not in result

    def test_same_pr_same_status_preserves_counters(self):
        open_prs = [{"repo": "r", "id": 100, "branch": "PROJ-1", "url": "u"}]
        ts = make_ticket_state(status="in_review", branch="PROJ-1",
                               prs=[{"repo": "r", "id": 100, "branch": "PROJ-1", "url": "u"}])
        ts["conflict_resolution_attempts"] = 1
        ts["ci_fix_attempts"] = 1

        result = tickets._reconcile_prs(ts, open_prs)

        assert result["conflict_resolution_attempts"] == 1
        assert result["ci_fix_attempts"] == 1

    def test_new_pr_identity_resets_counters(self):
        open_prs = [{"repo": "r", "id": 200, "branch": "PROJ-1", "url": "u2"}]
        ts = make_ticket_state(status="in_review", branch="PROJ-1",
                               prs=[{"repo": "r", "id": 100, "branch": "PROJ-1", "url": "u1"}])
        ts["conflict_resolution_attempts"] = 2
        ts["ci_fix_attempts"] = 2
        ts["ci_passed"] = True

        result = tickets._reconcile_prs(ts, open_prs)

        assert result["prs"][0]["id"] == 200
        assert result["conflict_resolution_attempts"] == 0
        assert result["ci_fix_attempts"] == 0
        assert "ci_passed" not in result


class TestMerge:
    def test_all_merged(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.merge_pr.return_value = {"status": "merged"}
        ts = make_ticket_state(status="in_review", prs=[{"repo": "r", "id": 1, "url": "u"}])

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._merge(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "merged"

    def test_merge_error_stays(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.merge_pr.return_value = {"error": "conflict"}
        ts = make_ticket_state(status="in_review", prs=[{"repo": "r", "id": 1, "url": "u"}])

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._merge(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "in_review"

    def test_no_prs_noop(self, fake_config):
        ts = make_ticket_state(status="in_review")
        result = tickets._merge(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "in_review"


class TestHandleCiFailureStub:
    def test_sets_flag_and_enqueues(self):
        ts = make_ticket_state(status="in_review")
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        assert result["_ci_failed_pending"] is True
        eq.assert_called_once_with("inst", "PROJ-1", "fix_ci_failures")
        assert result.get("ci_fix_attempts", 0) == 0

    def test_does_not_double_enqueue_when_job_already_inflight(self):
        import core.queue as q
        ts = make_ticket_state(status="in_review", _ci_failed_pending=True)
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch.object(q, "jobs_for_ticket",
                           return_value=[{"task": "fix_ci_failures", "status": "queued"}]), \
             patch.object(q, "enqueue_job") as eq, \
             patch("features.tickets.log"):
            tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        eq.assert_not_called()

    def test_no_instance_key_does_not_enqueue(self):
        ts = make_ticket_state(status="in_review")
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "")
        assert result["_ci_failed_pending"] is True
        eq.assert_not_called()

    def test_max_attempts_transitions_pr_failed_and_clears_flag(self):
        ts = make_ticket_state(status="in_review", ci_fix_attempts=2, _ci_failed_pending=True)
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        assert result["status"] == "pr_failed"
        assert "_ci_failed_pending" not in result
        eq.assert_not_called()

    def test_re_enqueues_when_flag_stuck_with_no_inflight_job(self):
        """Stuck _ci_failed_pending (from a prior fix_ci_failures that got
        skipped without clearing the flag) must not permanently block new
        CI fix attempts. With no in-flight job, the next failure detection
        should still produce a new enqueue."""
        import core.queue as q
        ts = make_ticket_state(status="in_review", _ci_failed_pending=True,
                                ci_fix_attempts=0)
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILURE"}]
        with patch.object(q, "jobs_for_ticket", return_value=[]), \
             patch.object(q, "enqueue_job") as eq, \
             patch("features.tickets.log"):
            tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        eq.assert_called_once_with("inst", "fix_ci_failures", ticket_key="PROJ-1")


class TestCheckSkipsBusyTicket:
    def test_skips_ticket_with_running_job(self, fake_config, tmp_state):
        import core.state as state
        from tests.conftest import make_ticket
        state.save("tickets", {"PROJ-1": make_ticket_state(status="planning", slug="PROJ-1-do-the-thing")})

        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets.get_repos", return_value=[]), \
             patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "start_planning", "status": "running"}]), \
             patch("features.tickets._enqueue_stage") as eq:
            tickets.check(fake_config, instance_key="inst")
        eq.assert_not_called()

    def test_processes_ticket_with_no_running_job(self, fake_config, tmp_state):
        import core.state as state
        from tests.conftest import make_ticket
        state.save("tickets", {"PROJ-1": make_ticket_state(status="planning", slug="PROJ-1-do-the-thing")})

        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets.get_repos", return_value=[]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._enqueue_stage"):
            tickets.check(fake_config, instance_key="inst")


class TestCheckRebuildsMissingTicketDir:
    def test_planning_with_missing_dir_triggers_setup(self, fake_config, tmp_state):
        import core.state as state
        from tests.conftest import make_ticket
        slug = "PROJ-1-do-the-thing"
        state.save("tickets", {"PROJ-1": make_ticket_state(status="planning", slug=slug)})

        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}) as setup, \
             patch("features.tickets._enqueue_stage") as eq:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        setup.assert_called_once()
        eq.assert_any_call("inst", "PROJ-1", "start_planning")
        saved = state.load_ticket("PROJ-1")
        assert saved is not None
        assert saved["status"] == "planning"

    def test_planning_with_existing_dir_skips_setup(self, fake_config, tmp_state):
        import core.state as state
        from tests.conftest import make_ticket
        slug = "PROJ-1-do-the-thing"
        (fake_config["workspace"]["root"] / "tickets" / slug).mkdir(parents=True)
        state.save("tickets", {"PROJ-1": make_ticket_state(status="planning", slug=slug)})

        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._reconcile_prs", side_effect=lambda ts, _prs: ts), \
             patch("features.tickets._setup_ticket") as setup, \
             patch("features.tickets._enqueue_stage") as eq:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        setup.assert_not_called()
        eq.assert_any_call("inst", "PROJ-1", "start_planning")

    def test_new_ticket_mapped_to_planning_runs_setup(self, fake_config, tmp_state):
        """When Jira status 'In Progress' maps to 'planning', a freshly
        assigned ticket must still run _setup_ticket to create its dir
        rather than fast-forwarding past setup."""
        import core.state as state
        from tests.conftest import make_ticket
        fake_config["jira"]["status_map"] = {"In Progress": "planning"}
        slug = "PROJ-1-do-the-thing"

        with patch("features.tickets._fetch_tickets",
                   return_value=[make_ticket(status="In Progress")]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._reconcile_prs", side_effect=lambda ts, _prs: ts), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}) as setup, \
             patch("features.tickets._enqueue_stage") as eq:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        setup.assert_called_once()
        eq.assert_any_call("inst", "PROJ-1", "start_planning")
        saved = state.load_ticket("PROJ-1")
        assert saved is not None
        assert saved["status"] == "planning"
        assert saved["slug"] == slug
        assert saved["discovered_at"] == "2026-04-22T00:00:00Z"

    def test_reviewing_with_missing_dir_triggers_setup(self, fake_config, tmp_state):
        import core.state as state
        from tests.conftest import make_ticket
        slug = "PROJ-1-do-the-thing"
        state.save("tickets", {"PROJ-1": make_ticket_state(status="reviewing", slug=slug)})

        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}) as setup, \
             patch("features.tickets._enqueue_stage"):
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        setup.assert_called_once()
        saved = state.load_ticket("PROJ-1")
        assert saved is not None
        assert saved["status"] == "reviewing"


class TestCheckEmitsTicketFoundOnce:
    @staticmethod
    def _found_calls(emit_mock):
        return [c for c in emit_mock.call_args_list
                if c.args and c.args[0] == "ticket_found"]

    def test_fresh_ticket_emits_once(self, fake_config, tmp_state):
        slug = "PROJ-1-do-the-thing"
        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.tickets.log.emit") as emit:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        found = self._found_calls(emit)
        assert len(found) == 1, (
            f"expected 1 ticket_found emit on first sighting, got {len(found)}: "
            f"{emit.call_args_list}"
        )

    def test_existing_ticket_does_not_re_emit(self, fake_config, tmp_state):
        import core.state as state
        slug = "PROJ-1-do-the-thing"
        state.save("tickets", {"PROJ-1": make_ticket_state(status="new", slug=slug)})
        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.tickets.log.emit") as emit:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        found = self._found_calls(emit)
        assert len(found) == 0, (
            f"expected 0 ticket_found emits for already-known ticket, "
            f"got {len(found)}: {emit.call_args_list}"
        )

    def test_two_consecutive_check_cycles_emit_exactly_once(self, fake_config, tmp_state):
        import core.state as state
        slug = "PROJ-1-do-the-thing"
        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.tickets.log.emit") as emit:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
            saved_after_first = state.load("tickets")
            assert "PROJ-1" in saved_after_first, (
                f"precondition broken: PROJ-1 not persisted after first check(), "
                f"got keys={list(saved_after_first)}"
            )
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        found = self._found_calls(emit)
        assert len(found) == 1, (
            f"expected exactly 1 ticket_found across two check() cycles, "
            f"got {len(found)}: {emit.call_args_list}"
        )

    def test_rebuild_path_does_not_emit_ticket_found(self, fake_config, tmp_state):
        import core.state as state
        slug = "PROJ-1-do-the-thing"
        state.save("tickets", {"PROJ-1": make_ticket_state(status="planning", slug=slug)})
        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.tickets.log.emit") as emit:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        found = self._found_calls(emit)
        assert len(found) == 0, (
            f"expected 0 ticket_found emits on ticket_dir_rebuild path, "
            f"got {len(found)}: {emit.call_args_list}"
        )


class TestCheckNewTicketIdempotency:
    def test_setup_ticket_called_at_most_once_across_two_cycles(self, fake_config, tmp_state):
        slug = "PROJ-1-do-the-thing"
        with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets._setup_ticket",
                   return_value={"status": "new", "slug": slug, "branch": slug,
                                 "discovered_at": "2026-04-22T00:00:00Z"}) as setup, \
             patch("features.tickets._enqueue_stage"), \
             patch("features.tickets.log.emit") as emit:
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        emit_summary = [(c.args[0], c.kwargs.get("meta", {}).get("ticket"))
                        for c in emit.call_args_list if c.args]
        assert setup.call_count <= 1, (
            f"_setup_ticket called {setup.call_count} times across two check() cycles "
            f"for a ticket whose first setup succeeded (discovered_at set); expected at "
            f"most 1. Each extra call re-emits ticket_worktree_created/error and spams "
            f"log_events. emits={emit_summary}"
        )


class TestFixCiFailuresTask:
    def _ctx(self, config, ticket_key="PROJ-1"):
        from core.tasks.registry import TaskContext
        from datetime import datetime, timezone
        return TaskContext(
            instance_key="inst", ticket_key=ticket_key, task="fix_ci_failures",
            payload={}, job_id=1, triggering_event_id=None,
            config=config, registry=None, now=datetime.now(timezone.utc),
        )

    def _seed(self, ts):
        import core.state as state
        state.save("tickets", {"PROJ-1": ts})

    def test_no_prs_fails_and_clears_flag(self, fake_config, tmp_state):
        from core.tasks.tickets import fix_ci_failures
        self._seed(make_ticket_state(status="in_review", _ci_failed_pending=True, prs=[]))
        result = fix_ci_failures(self._ctx(fake_config))
        assert result.status == "failed"
        import core.state as state
        ts = state.load("tickets")["PROJ-1"]
        assert "_ci_failed_pending" not in ts

    def test_worktree_missing_emits_skip(self, fake_config, tmp_state, tmp_log):
        from core.tasks.tickets import fix_ci_failures
        self._seed(make_ticket_state(
            status="in_review", _ci_failed_pending=True,
            prs=[{"repo": "r", "id": 1, "url": "u"}],
        ))
        mock_platform = MagicMock()
        mock_platform.get_pr_checks.return_value = [{"name": "lint", "state": "FAILED"}]
        with patch("core.tasks.tickets.make_platform", return_value=mock_platform):
            result = fix_ci_failures(self._ctx(fake_config))
        assert result.status == "ok"
        import core.state as state
        ts = state.load("tickets")["PROJ-1"]
        assert ts.get("ci_fix_attempts", 0) == 0
        assert "_ci_failed_pending" not in ts

    def test_not_caused_by_us_no_increment(self, fake_config, tmp_state, tmp_log):
        from core.tasks.tickets import fix_ci_failures
        slug = "PROJ-1-do-the-thing"
        self._seed(make_ticket_state(
            status="in_review", _ci_failed_pending=True, slug=slug,
            prs=[{"repo": "r", "id": 1, "url": "u"}],
        ))
        wt = fake_config["workspace"]["root"] / "tickets" / slug / "r"
        wt.mkdir(parents=True)

        mock_platform = MagicMock()
        mock_platform.get_pr_checks.return_value = [{"name": "lint", "state": "FAILED"}]
        mock_platform.get_failed_logs.return_value = "logs"
        mock_platform.get_pr_diff.return_value = "diff"
        with patch("core.tasks.tickets.make_platform", return_value=mock_platform), \
             patch("features.pr_ci.run_sonnet", return_value='{"caused_by_us": false, "reason": "flaky"}'), \
             patch("features.pr_ci.run_claude_code") as rcc:
            result = fix_ci_failures(self._ctx(fake_config))
        assert result.status == "ok"
        rcc.assert_not_called()
        import core.state as state
        ts = state.load("tickets")["PROJ-1"]
        assert ts.get("ci_fix_attempts", 0) == 0
        assert "_ci_failed_pending" not in ts

    def test_caused_by_us_increments_and_clears_flag(self, fake_config, tmp_state, tmp_log):
        from core.tasks.tickets import fix_ci_failures
        slug = "PROJ-1-do-the-thing"
        self._seed(make_ticket_state(
            status="in_review", _ci_failed_pending=True, slug=slug,
            prs=[{"repo": "r", "id": 1, "url": "u"}],
        ))
        wt = fake_config["workspace"]["root"] / "tickets" / slug / "r"
        wt.mkdir(parents=True)

        mock_platform = MagicMock()
        mock_platform.get_pr_checks.return_value = [{"name": "lint", "state": "FAILED"}]
        mock_platform.get_failed_logs.return_value = "logs"
        mock_platform.get_pr_diff.return_value = "diff"
        with patch("core.tasks.tickets.make_platform", return_value=mock_platform), \
             patch("features.pr_ci.run_sonnet",
                   return_value='{"caused_by_us": true, "reason": "bad", "fix_hint": "fix it"}'), \
             patch("features.pr_ci.run_claude_code", return_value="ok"):
            result = fix_ci_failures(self._ctx(fake_config))
        assert result.status == "ok"
        import core.state as state
        ts = state.load("tickets")["PROJ-1"]
        assert ts["ci_fix_attempts"] == 1
        assert "_ci_failed_pending" not in ts

    def test_exception_clears_pending_flag(self, fake_config, tmp_state, tmp_log):
        from core.tasks.tickets import fix_ci_failures
        slug = "PROJ-1-do-the-thing"
        self._seed(make_ticket_state(
            status="in_review", _ci_failed_pending=True, slug=slug,
            prs=[{"repo": "r", "id": 1, "url": "u"}],
        ))
        mock_platform = MagicMock()
        mock_platform.get_pr_checks.side_effect = RuntimeError("boom")
        with patch("core.tasks.tickets.make_platform", return_value=mock_platform):
            try:
                fix_ci_failures(self._ctx(fake_config))
            except RuntimeError:
                pass
        import core.state as state
        ts = state.load("tickets")["PROJ-1"]
        assert "_ci_failed_pending" not in ts


class TestCommentSnapshot:
    def test_empty(self):
        assert tickets._comment_snapshot([]) == {"count": 0, "latest_created_at": None, "comment_ids": []}

    def test_picks_max_date(self):
        snap = tickets._comment_snapshot([
            {"created_at": "2026-04-20T00:00:00Z", "id": "c1"},
            {"created_at": "2026-04-22T00:00:00Z", "id": "c2"},
            {"created_at": "2026-04-21T00:00:00Z", "id": "c3"},
        ])
        assert snap == {"count": 3, "latest_created_at": "2026-04-22T00:00:00Z", "comment_ids": ["c1", "c2", "c3"]}

    def test_ignores_missing_dates(self):
        snap = tickets._comment_snapshot([
            {"created_at": "2026-04-20T00:00:00Z", "id": "c1"}, {"id": "c2"},
        ])
        assert snap == {"count": 2, "latest_created_at": "2026-04-20T00:00:00Z", "comment_ids": ["c1", "c2"]}


class TestWriteCommentsMd:
    def test_empty(self, tmp_path):
        tickets._write_comments_md(tmp_path, [])
        assert (tmp_path / "comments.md").read_text() == "# Comments\n\nNo upstream comments.\n"

    def test_rendered(self, tmp_path):
        tickets._write_comments_md(tmp_path, [
            {"author": "Alice", "created_at": "2026-04-20T18:00:00Z", "body": "First"},
            {"author": "Bob", "created_at": "2026-04-21T09:00:00Z", "body": "Reply"},
        ])
        out = (tmp_path / "comments.md").read_text()
        assert "# Comments" in out
        assert "## Alice — 2026-04-20T18:00:00Z" in out
        assert "First" in out
        assert "## Bob — 2026-04-21T09:00:00Z" in out
        assert "Reply" in out


class TestMarkTicketMerged:
    def test_stores_snapshot(self, fake_config):
        ts = {"status": "in_review"}
        ticket = make_ticket()
        comments = [
            {"created_at": "2026-04-20T00:00:00Z", "body": "a"},
            {"created_at": "2026-04-21T00:00:00Z", "body": "b"},
        ]
        with patch("features.tickets._fetch_ticket_comments", return_value=comments):
            result = tickets._mark_ticket_merged(fake_config, ticket, ts)
        assert result["status"] == "merged"
        assert result["merged_comment_snapshot"] == {
            "count": 2, "latest_created_at": "2026-04-21T00:00:00Z", "comment_ids": []
        }
        assert "merged_at" in result

    def test_clears_ci_passed(self, fake_config):
        ts = {"status": "in_review", "ci_passed": True}
        with patch("features.tickets._fetch_ticket_comments", return_value=[]):
            result = tickets._mark_ticket_merged(fake_config, make_ticket(), ts)
        assert "ci_passed" not in result


class TestClearReingestDocs:
    def test_deletes_only_targeted(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        slug = "PROJ-1-slug"
        docs = tmp_path / "tickets" / slug / "docs"
        docs.mkdir(parents=True)
        for name in ("ticket.md", "technical-plan.md", "change-manifest.md",
                     "tri-review.md", "epic.md", "other.md"):
            (docs / name).write_text("x")
        (docs / "attachments").mkdir()
        (docs / "attachments" / "pic.png").write_bytes(b"p")

        deleted = tickets._clear_reingest_docs(fake_config, slug)
        assert set(deleted) == {"ticket.md", "technical-plan.md",
                                "change-manifest.md", "tri-review.md"}
        assert (docs / "epic.md").exists()
        assert (docs / "other.md").exists()
        assert (docs / "attachments" / "pic.png").exists()

    def test_missing_docs_dir_noop(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        assert tickets._clear_reingest_docs(fake_config, "nope") == []


class TestReingestMergedTicket:
    def _prep(self, tmp_path, fake_config, slug="PROJ-1-slug"):
        fake_config["workspace"]["root"] = tmp_path
        docs = tmp_path / "tickets" / slug / "docs"
        docs.mkdir(parents=True)
        for name in ("ticket.md", "technical-plan.md", "change-manifest.md", "tri-review.md"):
            (docs / name).write_text("old")
        return docs

    def test_resets_status_and_emits_requeued(self, tmp_path, fake_config):
        docs = self._prep(tmp_path, fake_config)
        ts = make_ticket_state(status="merged", slug="PROJ-1-slug",
                               merged_comment_snapshot={"count": 1,
                                                        "latest_created_at": "2026-04-20T00:00:00Z"},
                               merged_at="2026-04-20T01:00:00Z")
        ticket = make_ticket(status="Prioritized")

        fresh_comments = [
            {"id": "1", "author": "Alice", "body": "old", "created_at": "2026-04-20T00:00:00Z"},
            {"id": "2", "author": "Bob", "body": "new feedback", "created_at": "2026-04-22T00:00:00Z"},
        ]
        with patch("features.tickets._fetch_ticket_comments", return_value=fresh_comments), \
             patch("features.tickets._setup_ticket", return_value={"status": "new",
                                                                    "slug": "PROJ-1-slug",
                                                                    "branch": "PROJ-1-slug",
                                                                    "discovered_at": "2026-04-22T09:00:00Z"}), \
             patch("features.tickets.log") as mlog:
            result = tickets._reingest_merged_ticket(fake_config, ticket, ts, "http://base")

        assert result["status"] == "new"
        assert result["reopened_count"] == 1
        assert result["last_merged_at"] == "2026-04-20T01:00:00Z"
        assert result["last_merged_comment_snapshot"]["count"] == 1
        assert "merged_at" not in result
        assert "merged_comment_snapshot" not in result
        for name in ("ticket.md", "technical-plan.md", "change-manifest.md", "tri-review.md"):
            assert not (docs / name).exists()
        events = [c.args[0] for c in mlog.emit.call_args_list]
        assert "ticket_requeued" in events
        assert "ticket_requeued_without_comment" not in events

    def test_emits_stale_when_no_new_comment(self, tmp_path, fake_config):
        self._prep(tmp_path, fake_config)
        ts = make_ticket_state(status="merged", slug="PROJ-1-slug",
                               merged_comment_snapshot={"count": 2,
                                                        "latest_created_at": "2026-04-21T00:00:00Z"})
        ticket = make_ticket(status="Prioritized")
        comments = [
            {"id": "1", "author": "A", "body": "x", "created_at": "2026-04-20T00:00:00Z"},
            {"id": "2", "author": "B", "body": "y", "created_at": "2026-04-21T00:00:00Z"},
        ]
        with patch("features.tickets._fetch_ticket_comments", return_value=comments), \
             patch("features.tickets._setup_ticket", return_value={"status": "new",
                                                                    "slug": "PROJ-1-slug",
                                                                    "branch": "PROJ-1-slug"}), \
             patch("features.tickets.log") as mlog:
            tickets._reingest_merged_ticket(fake_config, ticket, ts, "http://base")
        events = [c.args[0] for c in mlog.emit.call_args_list]
        assert "ticket_requeued" in events
        assert "ticket_requeued_without_comment" in events

    def test_skips_stale_when_no_snapshot(self, tmp_path, fake_config):
        self._prep(tmp_path, fake_config)
        ts = make_ticket_state(status="merged", slug="PROJ-1-slug")
        ticket = make_ticket(status="Prioritized")
        with patch("features.tickets._fetch_ticket_comments", return_value=[]), \
             patch("features.tickets._setup_ticket", return_value={"status": "new",
                                                                    "slug": "PROJ-1-slug",
                                                                    "branch": "PROJ-1-slug"}), \
             patch("features.tickets.log") as mlog:
            tickets._reingest_merged_ticket(fake_config, ticket, ts, "http://base")
        events = [c.args[0] for c in mlog.emit.call_args_list]
        assert "ticket_requeued" in events
        assert "ticket_requeued_without_comment" not in events
        meta = mlog.emit.call_args_list[0].kwargs["meta"]
        assert meta["comment_check"] == "skipped_no_merge_snapshot"


class TestCheckRequeue:
    def _run_check(self, tmp_path, fake_config, saved_state, external_status,
                   setup_return=None, repos=None):
        import core.state as state
        state.init(tmp_path)
        fake_config["workspace"]["root"] = tmp_path
        slug = saved_state.get("slug", "PROJ-1-slug")
        docs = tmp_path / "tickets" / slug / "docs"
        docs.mkdir(parents=True, exist_ok=True)
        (docs / "ticket.md").write_text("old")

        state.save_ticket("PROJ-1", saved_state)
        ticket = make_ticket(status=external_status)
        repos = repos if repos is not None else [{"name": "myrepo", "path": tmp_path / "repo"}]
        setup_return = setup_return or {"status": "new", "slug": slug, "branch": slug}

        with patch("features.tickets._fetch_tickets", return_value=[ticket]), \
             patch("features.tickets._fetch_open_prs", return_value=[]), \
             patch("features.tickets.get_repos", return_value=repos), \
             patch("features.tickets._fetch_ticket_comments", return_value=[]), \
             patch("features.tickets._setup_ticket", return_value=setup_return), \
             patch("features.tickets._enqueue_stage") as menq, \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("features.tickets.log"):
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
        return state.load_ticket("PROJ-1"), menq

    def test_same_external_status_enqueues_validation(self, tmp_path, fake_config):
        saved, menq = self._run_check(tmp_path, fake_config,
            saved_state={"status": "merged", "slug": "PROJ-1-slug", "branch": "PROJ-1-slug",
                         "merged_external_status": "QA"},
            external_status="QA")
        assert saved is not None
        assert saved["status"] == "merged"
        assert saved["merged_external_status"] == "QA"
        assert "reopened_count" not in saved
        menq.assert_any_call("inst", "PROJ-1", "validate_merged_ticket")

    def test_changed_external_status_reingests(self, tmp_path, fake_config):
        saved, menq = self._run_check(tmp_path, fake_config,
            saved_state={"status": "merged", "slug": "PROJ-1-slug", "branch": "PROJ-1-slug",
                         "merged_external_status": "QA",
                         "merged_comment_snapshot": {"count": 0, "latest_created_at": None}},
            external_status="Prioritized")
        assert saved is not None
        assert saved["status"] == "new"
        assert saved["reopened_count"] == 1
        assert saved["last_merged_external_status"] == "QA"
        assert "merged_external_status" not in saved
        menq.assert_any_call("inst", "PROJ-1", "start_planning")


class TestRenderPrdTicketMd:
    """Renderer must not produce a near-empty `# Untitled` ticket.md when the
    saved ts dict has no usable content but is linked to a prd_section that
    does. Observed live: PRD-5_MOCK_STUB_CATALOGUE-20 went into planning with
    docs/ticket.md == '# Untitled\\n' (11 bytes) because gen_ticket landed
    with empty summary/description/AC/source_text — the linked section had
    4726 chars of content that never reached the worktree."""

    def _seed_section(self, fresh_db, header: str, content: str) -> int:
        import core.db as db
        db.execute(
            "INSERT INTO prd(instance_key, file_path) VALUES(?, ?)",
            ("inst", "/tmp/prd.md"),
        )
        prd_row = db.query_one("SELECT id FROM prd WHERE instance_key='inst'")
        db.execute(
            "INSERT INTO prd_section(prd_id, stable_key, header, content, content_hash) "
            "VALUES(?, ?, ?, ?, ?)",
            (prd_row["id"], "5-mock-stub-catalogue", header, content, "h"),
        )
        row = db.query_one(
            "SELECT id FROM prd_section WHERE prd_id=? AND stable_key=?",
            (prd_row["id"], "5-mock-stub-catalogue"),
        )
        return row["id"]

    def test_empty_ts_with_section_renders_section_content(self, fresh_db):
        section_header = "5. Mock / stub catalogue"
        section_content = (
            "Need a single registry of mock vendors used across tests so we can "
            "avoid duplication and divergence. Cover: payment provider mocks, "
            "shipping provider mocks, identity provider mocks."
        )
        section_id = self._seed_section(fresh_db, section_header, section_content)
        ts = {
            "summary": "",
            "description": "",
            "acceptance_criteria_json": None,
            "acceptance_criteria_source_text": "",
            "prd_section_id": section_id,
        }
        md = tickets.render_prd_ticket_md(ts)
        assert section_header in md, f"expected section header in ticket.md, got: {md!r}"
        assert "Mock vendors" in md or "mock vendors" in md, \
            f"expected section content in ticket.md, got: {md!r}"
        assert md.strip() != "# Untitled", \
            f"renderer fell back to # Untitled instead of using section: {md!r}"
        assert len(md) > 200, \
            f"ticket.md should have meaningful content, got {len(md)} chars: {md!r}"

    def test_populated_ts_unaffected(self, fresh_db):
        """When ts already has summary/description, renderer keeps using them
        and does not overwrite with section content."""
        section_id = self._seed_section(fresh_db, "Section header", "Section body")
        ts = {
            "summary": "Real ticket title",
            "description": "Real ticket description",
            "acceptance_criteria_json": {"criteria": [
                {"criterion": "Real criterion", "playwright": [], "tests_required": []}
            ]},
            "acceptance_criteria_source_text": "Real source text",
            "prd_section_id": section_id,
        }
        md = tickets.render_prd_ticket_md(ts)
        assert "Real ticket title" in md
        assert "Real ticket description" in md
        assert "Real criterion" in md
        assert "Real source text" in md
        assert "Section body" not in md

    def test_empty_ts_without_section_id_still_renders_safely(self, fresh_db):
        """No prd_section_id and no content fields — renderer should still
        return a non-crashing string. Documents the existing degenerate
        behavior so we don't regress to crashes."""
        ts = {"summary": "", "description": ""}
        md = tickets.render_prd_ticket_md(ts)
        assert isinstance(md, str)
        assert "Untitled" in md

    def test_hydrates_via_link_table_when_prd_section_id_missing(self, fresh_db):
        """Observed live: tickets created via _create_generated_ticket lost
        their prd_section_id field somewhere in the save/transition path —
        e.g. PRD-5_MOCK_STUB_CATALOGUE-20 had no prd_section_id in
        tickets.data but was linked in prd_section_ticket. Renderer must
        find the section via that link table when given (instance_key,
        ticket_key)."""
        import core.db as db
        section_header = "5. Mock / stub catalogue"
        section_content = "Single registry of mock vendors. Cover payment, shipping, identity."
        section_id = self._seed_section(fresh_db, section_header, section_content)
        db.execute(
            "INSERT INTO prd_section_ticket(prd_section_id, instance_key, ticket_key) "
            "VALUES(?, ?, ?)",
            (section_id, "lumeninv", "PRD-5_MOCK_STUB_CATALOGUE-20"),
        )
        ts = {"summary": "", "description": ""}
        md = tickets.render_prd_ticket_md(
            ts, instance_key="lumeninv", ticket_key="PRD-5_MOCK_STUB_CATALOGUE-20",
        )
        assert section_header in md, f"expected section header, got: {md!r}"
        assert "Single registry of mock vendors" in md
        assert md.strip() != "# Untitled"


class TestCheckInReviewFixFailedRetry:
    """Observed live on aimyable/root-cdk#23 (DEV-467 'update Lambda runtime
    to Node.js 24'): 3 reviewer review comments from Trevin Avery were processed
    in one scan after the run_thinking() positional-arg TypeError was fixed; 2
    landed in fix_failed (Claude produced no code change for inline comments on
    lib/runtime-aspect.ts:8 and lib/vpn-stack.ts:168) and 1 ambiguous->needs_reply.
    Then features/tickets.py:1258 set last_comment_ids['root-cdk/23'] to
    max(id)=795537372, so every subsequent scan filters all 3 comments out via
    `c["id"] > last_seen` — fix_failed comments are permanently locked out of
    retry, even if a future push to the PR would make a fix possible. Compare to
    features/own_prs.py:_check_comments which uses the stateful core.comments
    module (mark_comment_error reverts state to 'new') and naturally retries."""

    def _make_pr_comment(self, **overrides):
        base = {"id": 100, "body": "Please rename this variable",
                "author_id": "reviewer1", "author_name": "Bob",
                "path": "src/main.py", "line": 42, "parent_id": None,
                "created_on": "2026-01-01T12:00:00Z",
                "created_at": "2026-01-01T12:00:00Z",
                "updated_at": "2026-01-01T12:00:00Z"}
        base.update(overrides)
        return base

    def _setup_worktree(self, fake_config, slug):
        ws_root = fake_config["workspace"]["root"]
        wt = ws_root / "tickets" / slug / "repo"
        wt.mkdir(parents=True, exist_ok=True)
        (wt / ".git").mkdir(exist_ok=True)
        return wt

    def test_fix_failed_comment_reprocessed_on_next_scan(
        self, fresh_db, fake_config, tmp_state
    ):
        """When run_claude_code returns None (no fix produced), the comment
        lands at fix_failed. A subsequent _check_in_review on the same PR
        must reprocess that comment — i.e. the classification batch_prompt
        must include it again."""
        slug = "PROJ-1-do-the-thing"
        wt = self._setup_worktree(fake_config, slug)
        ts = make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "repo", "id": 99, "branch": slug, "url": "http://u"}],
        )
        ticket = {"key": "PROJ-1", "summary": "Do thing", "url": "http://j/PROJ-1"}
        c1 = self._make_pr_comment(id=100, body="rename helper to something clearer")
        c2 = self._make_pr_comment(id=200, body="this name is too generic")

        mock_platform = MagicMock()
        mock_platform.get_pr_state.return_value = "OPEN"
        mock_platform.get_pr_comments.return_value = [c1, c2]
        haiku_classify = (
            '{"results": [{"i": 0, "actionable": true}, {"i": 1, "actionable": true}]}'
        )
        bb_config = {
            **fake_config,
            "job": {**fake_config["job"], "platform": "bitbucket"},
            "bitbucket": {"org": "x", "user_account_id": "bot-self"},
        }

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "repo", "path": wt.parent}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.run_haiku", return_value=haiku_classify) as haiku, \
             patch("features.tickets.run_claude_code", return_value=None), \
             patch("features.tickets.subprocess.run",
                   return_value=MagicMock(returncode=0)):
            ts = tickets._check_in_review(bb_config, ticket, ts, "http://base")

            assert haiku.call_count >= 1, (
                "expected first scan to classify the 2 actionable comments"
            )
            first_batch_prompt = haiku.call_args_list[0].args[0]
            assert "rename helper" in first_batch_prompt and "too generic" in first_batch_prompt, \
                f"first batch should include both comments; got: {first_batch_prompt!r}"

            haiku.reset_mock()
            ts = tickets._check_in_review(bb_config, ticket, ts, "http://base")

            assert haiku.call_count >= 1, (
                "fix_failed comments must be reprocessed on subsequent scans, "
                "but the second scan did not re-classify them — the cursor "
                "advanced past fix_failed comments and locked them out of retry"
            )
            second_batch_prompt = haiku.call_args_list[0].args[0]
            assert "rename helper" in second_batch_prompt or "too generic" in second_batch_prompt, (
                "second scan's classification batch must include at least one of "
                "the previously fix_failed comments; got: "
                f"{second_batch_prompt!r}"
            )

    def test_mixed_outcomes_keep_failed_retryable_even_when_needs_reply_id_is_lower(
        self, fresh_db, fake_config, tmp_state
    ):
        """Mirrors the DEV-467 shape: failed-id-A < needs_reply-id < failed-id-B
        (bb returns ids 795535088 fix_failed, 795537065 needs_reply, 795537372
        fix_failed). A cursor-advance-to-max-successful would set cursor to the
        needs_reply id and permanently lock out the lower-id failed comment.
        Required behavior: when any fix_failed is in the batch, do NOT advance
        the cursor — both failed comments must reprocess on the next scan."""
        slug = "PROJ-1-do-the-thing"
        wt = self._setup_worktree(fake_config, slug)
        ts = make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "repo", "id": 99, "branch": slug, "url": "http://u"}],
        )
        ticket = {"key": "PROJ-1", "summary": "Do thing", "url": "http://j/PROJ-1"}
        c_low_failed = self._make_pr_comment(
            id=795535088, body="duplicate variable name on vpn-stack",
        )
        c_mid_ambiguous = self._make_pr_comment(
            id=795537065, body="dangerous to change runtime of high-level constructs",
        )
        c_high_failed = self._make_pr_comment(
            id=795537372, body="helper name is too generic on runtime-aspect",
        )

        mock_platform = MagicMock()
        mock_platform.get_pr_state.return_value = "OPEN"
        mock_platform.get_pr_comments.return_value = [
            c_low_failed, c_mid_ambiguous, c_high_failed,
        ]
        haiku_classify = (
            '{"results": ['
            '{"i": 0, "actionable": true},'
            '{"i": 1, "actionable": false},'
            '{"i": 2, "actionable": true}'
            ']}'
        )
        bb_config = {
            **fake_config,
            "job": {**fake_config["job"], "platform": "bitbucket"},
            "bitbucket": {"org": "x", "user_account_id": "bot-self"},
        }

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "repo", "path": wt.parent}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.run_haiku", return_value=haiku_classify), \
             patch("features.tickets.run_claude_code", return_value=None), \
             patch("features.tickets.subprocess.run",
                   return_value=MagicMock(returncode=0)):
            ts = tickets._check_in_review(bb_config, ticket, ts, "http://base")

        cursor = ts.get("last_comment_ids", {}).get("repo/99", 0)
        assert cursor < c_low_failed["id"], (
            "after fix_failed comments end a batch, the cursor must NOT advance "
            "past the lowest failed id (or it locks the comment out of retry). "
            f"got cursor={cursor}, lowest_failed_id={c_low_failed['id']}"
        )

    def test_fix_failed_caps_after_max_attempts_and_advances_cursor(
        self, fresh_db, fake_config, tmp_state
    ):
        """Observed live: DEV-467 surfaced ticket_pr_comment_fix_failed every 2 min
        for the same 3 comments because commit 63dcebf parks the cursor at last_seen
        whenever any comment in the batch ends fix_failed, with no upper bound. The
        retry must be capped at MAX_PR_COMMENT_FIX_ATTEMPTS — once a comment has
        burned its budget, the cursor advances past it, ticket_pr_comment_fix_capped
        is emitted, and subsequent scans do NOT re-classify it."""
        slug = "PROJ-1-do-the-thing"
        wt = self._setup_worktree(fake_config, slug)
        ts = make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "repo", "id": 99, "branch": slug, "url": "http://u"}],
        )
        ticket = {"key": "PROJ-1", "summary": "Do thing", "url": "http://j/PROJ-1"}
        c = self._make_pr_comment(id=100, body="rename this helper to something clearer")

        mock_platform = MagicMock()
        mock_platform.get_pr_state.return_value = "OPEN"
        mock_platform.get_pr_comments.return_value = [c]
        haiku_classify = '{"results": [{"i": 0, "actionable": true}]}'
        bb_config = {
            **fake_config,
            "job": {**fake_config["job"], "platform": "bitbucket"},
            "bitbucket": {"org": "x", "user_account_id": "bot-self"},
        }

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "repo", "path": wt.parent}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.run_haiku", return_value=haiku_classify) as haiku, \
             patch("features.tickets.run_claude_code", return_value=None), \
             patch("features.tickets.subprocess.run",
                   return_value=MagicMock(returncode=0)), \
             patch("features.tickets.log.emit") as emit:
            for _ in range(tickets.MAX_PR_COMMENT_FIX_ATTEMPTS):
                ts = tickets._check_in_review(bb_config, ticket, ts, "http://base")

            cap_events = [
                call for call in emit.call_args_list
                if call.args and call.args[0] == "ticket_pr_comment_fix_capped"
            ]
            assert len(cap_events) == 1, (
                "expected exactly one ticket_pr_comment_fix_capped after "
                f"{tickets.MAX_PR_COMMENT_FIX_ATTEMPTS} failed attempts, "
                f"got {len(cap_events)}"
            )
            assert cap_events[0].kwargs.get("meta", {}).get("comment_id") == c["id"]

            cursor = ts.get("last_comment_ids", {}).get("repo/99", 0)
            assert cursor >= c["id"], (
                "after cap, cursor must advance past the capped comment so the "
                f"next scan filters it out. got cursor={cursor}, comment_id={c['id']}"
            )

            attempts = ts.get("comment_fix_attempts", {}).get(f"repo/99/{c['id']}", 0)
            assert attempts == tickets.MAX_PR_COMMENT_FIX_ATTEMPTS, (
                f"expected attempts={tickets.MAX_PR_COMMENT_FIX_ATTEMPTS}, "
                f"got {attempts}"
            )

            haiku.reset_mock()
            emit.reset_mock()
            ts = tickets._check_in_review(bb_config, ticket, ts, "http://base")
            assert haiku.call_count == 0, (
                "post-cap scan must NOT re-classify the comment via haiku "
                f"(got {haiku.call_count} calls)"
            )
            new_fail_events = [
                call for call in emit.call_args_list
                if call.args and call.args[0] == "ticket_pr_comment_fix_failed"
            ]
            assert new_fail_events == [], (
                "post-cap scan must NOT emit further ticket_pr_comment_fix_failed; "
                f"got {len(new_fail_events)}"
            )


class TestRecheckPrFailed:
    """A pr_failed ticket must not be terminal in scan_tickets — observed on
    nectar 2026-05-12: 13 pr_failed tickets in DB included NEC-3039 (PR #691
    MERGED ~1h before the page load), NEC-3098 (PR #697 MERGED May 4, 8 days
    stale), and NEC-3064 (PR #692 still OPEN). The check() loop short-circuits
    at `if ts['status'] == TicketStatus.pr_failed: continue`, so a previously
    closed PR that gets reopened or merged is never re-examined and the ticket
    stays in pr_failed forever. _recheck_pr_failed re-fetches PR state on each
    scan and transitions out of pr_failed when reality has moved on."""

    def _ts(self, **overrides):
        ts = make_ticket_state(
            status="pr_failed",
            prs=[{"repo": "repo", "id": 99, "branch": "PROJ-1", "url": "http://u/99"}],
        )
        ts.update(overrides)
        return ts

    def _ticket(self):
        return {"key": "PROJ-1", "summary": "Do thing", "url": "http://j/PROJ-1",
                "status": "In Review"}

    def test_pr_now_merged_recovers_to_merged(self, fake_config):
        ts = self._ts()
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"state": "MERGED", "approvers": []}
        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets._fetch_ticket_comments", return_value=[]):
            ts = tickets._recheck_pr_failed(fake_config, self._ticket(), ts, "http://b")
        assert ts["status"] == "merged", (
            "tracked PR is now MERGED on the platform; pr_failed ticket must "
            f"recover to merged. got: {ts['status']}"
        )

    def test_pr_reopened_recovers_to_in_review(self, fake_config):
        ts = self._ts()
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"state": "OPEN", "approvers": []}
        with patch("features.tickets.make_platform", return_value=mock_platform):
            ts = tickets._recheck_pr_failed(fake_config, self._ticket(), ts, "http://b")
        assert ts["status"] == "in_review", (
            "tracked PR is now OPEN (was previously closed); pr_failed ticket "
            f"must recover to in_review so the normal review-loop picks it back "
            f"up. got: {ts['status']}"
        )

    def test_still_closed_unmerged_stays_pr_failed(self, fake_config):
        ts = self._ts()
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"state": "CLOSED", "approvers": []}
        with patch("features.tickets.make_platform", return_value=mock_platform):
            ts = tickets._recheck_pr_failed(fake_config, self._ticket(), ts, "http://b")
        assert ts["status"] == "pr_failed", (
            f"tracked PR is still CLOSED unmerged; ticket must stay pr_failed. "
            f"got: {ts['status']}"
        )

    def test_no_prs_stays_pr_failed(self, fake_config):
        ts = self._ts(prs=[])
        mock_platform = MagicMock()
        with patch("features.tickets.make_platform", return_value=mock_platform):
            ts = tickets._recheck_pr_failed(fake_config, self._ticket(), ts, "http://b")
        assert ts["status"] == "pr_failed"
        assert mock_platform.get_pr_info.call_count == 0, (
            "with no tracked PRs there's nothing to re-check; must not hit the platform"
        )

    def test_platform_error_stays_pr_failed(self, fake_config):
        ts = self._ts()
        mock_platform = MagicMock()
        mock_platform.get_pr_info.side_effect = RuntimeError("boom")
        with patch("features.tickets.make_platform", return_value=mock_platform):
            ts = tickets._recheck_pr_failed(fake_config, self._ticket(), ts, "http://b")
        assert ts["status"] == "pr_failed", (
            "transient platform error must not cause spurious recovery; "
            f"stay pr_failed and retry next scan. got: {ts['status']}"
        )

    def test_mixed_one_merged_one_open_recovers_to_in_review(self, fake_config):
        ts = self._ts(prs=[
            {"repo": "repo", "id": 99, "branch": "PROJ-1", "url": "http://u/99"},
            {"repo": "repo2", "id": 42, "branch": "PROJ-1", "url": "http://u/42"},
        ])
        mock_platform = MagicMock()
        def info(repo, pr_id):
            return {"state": "MERGED" if pr_id == 99 else "OPEN", "approvers": []}
        mock_platform.get_pr_info.side_effect = info
        with patch("features.tickets.make_platform", return_value=mock_platform):
            ts = tickets._recheck_pr_failed(fake_config, self._ticket(), ts, "http://b")
        assert ts["status"] == "in_review", (
            "with one MERGED and one OPEN, the ticket is not fully merged but "
            "the other PR is healthy; recover to in_review so the normal "
            f"review-loop picks it back up. got: {ts['status']}"
        )


class TestPrFailedReason:
    """pr_failed is overloaded — it covers four distinct failure modes that need
    different remediation (no PR created vs. PR rejected vs. merge conflict
    exhausted vs. CI exhausted). The pr_failed_reason field tags each transition
    site so /today and the ticket detail can show the cause at a glance.
    Clears on recovery (success, reopen, or manual Restart)."""

    def test_create_failed_tags_reason(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        slug = "PROJ-1-slug"
        (tmp_path / "tickets" / slug / "myrepo").mkdir(parents=True)
        ts = make_ticket_state(status="pr_ready", slug=slug, branch=slug, pr_attempts=2)

        mock_platform = MagicMock()
        mock_platform.push_branch.return_value = {"ok": True}
        mock_platform.create_pr.return_value = {"error": "auth"}
        diff_result = MagicMock(returncode=0, stdout="file.py | 5 +++++")

        def fake_run(cmd, *a, **kw):
            if "diff" in cmd:
                return diff_result
            return MagicMock(returncode=0, stdout=b"PROJ-1-slug\n")

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.get_repos", return_value=[{"name": "myrepo", "path": tmp_path / "myrepo"}]), \
             patch("features.tickets.ticket_worktree_path", return_value=tmp_path / "tickets" / slug / "myrepo"), \
             patch("features.tickets.subprocess.run", side_effect=fake_run), \
             patch("features.tickets.run_haiku", return_value="Summary"), \
             patch("features.tickets.log"):
            result = tickets._create_pr(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_failed"
        assert result.get("pr_failed_reason") == "create_failed", (
            "3× failed _create_pr must tag pr_failed_reason='create_failed' so /today shows "
            f"the cause as a creation failure, not CI/conflict/rejection. got: {result.get('pr_failed_reason')!r}"
        )

    def test_pr_rejected_tags_reason(self, fake_config):
        ts = make_ticket_state(
            status="in_review",
            prs=[{"repo": "r", "id": 1, "branch": "b", "url": "http://u/1"}],
        )
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"state": "CLOSED", "approvers": []}
        mock_platform.get_pr_comments.return_value = []
        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._check_in_review(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_failed"
        assert result.get("pr_failed_reason") == "pr_rejected", (
            "PR closed unmerged on the platform must tag pr_failed_reason='pr_rejected'. "
            f"got: {result.get('pr_failed_reason')!r}"
        )

    def test_conflict_failed_tags_reason(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"mergeable": "CONFLICTING"}
        ts = make_ticket_state(
            status="in_review",
            slug="PROJ-1-slug",
            prs=[{"repo": "r", "id": 1, "url": "http://u"}],
            conflict_resolution_attempts=2,
        )
        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._resolve_conflicts(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_failed"
        assert result.get("pr_failed_reason") == "conflict_failed", (
            "MAX_CONFLICT_ATTEMPTS reached must tag pr_failed_reason='conflict_failed'. "
            f"got: {result.get('pr_failed_reason')!r}"
        )

    def test_ci_failed_tags_reason(self):
        ts = make_ticket_state(status="in_review", ci_fix_attempts=2, _ci_failed_pending=True)
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage"), patch("features.tickets.log"):
            result = tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        assert result["status"] == "pr_failed"
        assert result.get("pr_failed_reason") == "ci_failed", (
            "MAX_CI_FIX_ATTEMPTS reached must tag pr_failed_reason='ci_failed'. "
            f"got: {result.get('pr_failed_reason')!r}"
        )

    def test_recovery_to_merged_clears_reason(self, fake_config):
        ts = make_ticket_state(
            status="pr_failed",
            prs=[{"repo": "r", "id": 1, "branch": "b", "url": "http://u/1"}],
            pr_failed_reason="ci_failed",
        )
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"state": "MERGED", "approvers": []}
        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets._fetch_ticket_comments", return_value=[]), \
             patch("features.tickets.log"):
            result = tickets._recheck_pr_failed(fake_config, {"key": "PROJ-1", "summary": "", "url": "", "status": ""}, ts, "http://b")
        assert result["status"] == "merged"
        assert "pr_failed_reason" not in result, (
            "recovering pr_failed → merged must clear pr_failed_reason; a merged ticket "
            f"has no failure cause. got reason still set: {result.get('pr_failed_reason')!r}"
        )

    def test_recovery_to_in_review_clears_reason(self, fake_config):
        ts = make_ticket_state(
            status="pr_failed",
            prs=[{"repo": "r", "id": 1, "branch": "b", "url": "http://u/1"}],
            pr_failed_reason="pr_rejected",
        )
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"state": "OPEN", "approvers": []}
        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._recheck_pr_failed(fake_config, {"key": "PROJ-1", "summary": "", "url": "", "status": ""}, ts, "http://b")
        assert result["status"] == "in_review"
        assert "pr_failed_reason" not in result, (
            "recovering pr_failed → in_review (PR reopened) must clear pr_failed_reason. "
            f"got reason still set: {result.get('pr_failed_reason')!r}"
        )
