from unittest.mock import patch, MagicMock

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


class TestEnqueueStage:
    def test_enqueues_when_no_existing(self):
        with patch("core.queue.jobs_for_ticket", return_value=[]) as qj, \
             patch("core.queue.enqueue_job") as eq:
            tickets._enqueue_stage("inst", "T-1", "start_planning")
            qj.assert_called_once_with("inst", "T-1", limit=20)
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
        ts = make_ticket_state(status="pr_created")
        result = tickets._resolve_conflicts(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_created"

    def test_not_conflicting_noop(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"mergeable": "MERGEABLE"}
        ts = make_ticket_state(status="pr_created", prs=[{"repo": "r", "id": 1, "url": "http://u"}])

        with patch("features.tickets.make_platform", return_value=mock_platform):
            result = tickets._resolve_conflicts(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_created"

    def test_max_attempts_transitions_to_failed(self, tmp_path, fake_config):
        fake_config["workspace"]["root"] = tmp_path
        mock_platform = MagicMock()
        mock_platform.get_pr_info.return_value = {"mergeable": "CONFLICTING"}
        ts = make_ticket_state(
            status="pr_created",
            slug="PROJ-1-slug",
            prs=[{"repo": "r", "id": 1, "url": "http://u"}],
            conflict_resolution_attempts=2,
        )

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._resolve_conflicts(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_failed"


class TestReconcilePrs:
    def test_match_by_branch_populates_prs(self):
        open_prs = [
            {"repo": "r", "id": 99, "branch": "other-branch", "url": "u1"},
            {"repo": "r", "id": 100, "branch": "PROJ-1-do-the-thing", "url": "u2"},
        ]
        ts = make_ticket_state(status="pr_created", branch="PROJ-1-do-the-thing",
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
        ts = make_ticket_state(status="pr_created", branch="shared-branch",
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

        assert result["status"] == "pr_created"
        assert result["conflict_resolution_attempts"] == 0
        assert result["ci_fix_attempts"] == 0
        assert "ci_passed" not in result
        assert "checks_started_at" not in result

    def test_same_pr_same_status_preserves_counters(self):
        open_prs = [{"repo": "r", "id": 100, "branch": "PROJ-1", "url": "u"}]
        ts = make_ticket_state(status="pr_created", branch="PROJ-1",
                               prs=[{"repo": "r", "id": 100, "branch": "PROJ-1", "url": "u"}])
        ts["conflict_resolution_attempts"] = 1
        ts["ci_fix_attempts"] = 1

        result = tickets._reconcile_prs(ts, open_prs)

        assert result["conflict_resolution_attempts"] == 1
        assert result["ci_fix_attempts"] == 1

    def test_new_pr_identity_resets_counters(self):
        open_prs = [{"repo": "r", "id": 200, "branch": "PROJ-1", "url": "u2"}]
        ts = make_ticket_state(status="pr_created", branch="PROJ-1",
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
        ts = make_ticket_state(status="pr_created", prs=[{"repo": "r", "id": 1, "url": "u"}])

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._merge(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "merged"

    def test_merge_error_stays(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.merge_pr.return_value = {"error": "conflict"}
        ts = make_ticket_state(status="pr_created", prs=[{"repo": "r", "id": 1, "url": "u"}])

        with patch("features.tickets.make_platform", return_value=mock_platform), \
             patch("features.tickets.log"):
            result = tickets._merge(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_created"

    def test_no_prs_noop(self, fake_config):
        ts = make_ticket_state(status="pr_created")
        result = tickets._merge(fake_config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_created"


class TestHandleCiFailureStub:
    def test_sets_flag_and_enqueues(self):
        ts = make_ticket_state(status="pr_created")
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        assert result["_ci_failed_pending"] is True
        eq.assert_called_once_with("inst", "PROJ-1", "fix_ci_failures")
        assert result.get("ci_fix_attempts", 0) == 0

    def test_does_not_double_enqueue(self):
        ts = make_ticket_state(status="pr_created", _ci_failed_pending=True)
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.log"):
            tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        eq.assert_not_called()

    def test_no_instance_key_does_not_enqueue(self):
        ts = make_ticket_state(status="pr_created")
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "")
        assert result["_ci_failed_pending"] is True
        eq.assert_not_called()

    def test_max_attempts_transitions_pr_failed_and_clears_flag(self):
        ts = make_ticket_state(status="pr_created", ci_fix_attempts=2, _ci_failed_pending=True)
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(make_ticket(), ts, pr, checks, "http://base", "inst")
        assert result["status"] == "pr_failed"
        assert "_ci_failed_pending" not in result
        eq.assert_not_called()


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
        self._seed(make_ticket_state(status="pr_created", _ci_failed_pending=True, prs=[]))
        result = fix_ci_failures(self._ctx(fake_config))
        assert result.status == "failed"
        import core.state as state
        ts = state.load("tickets")["PROJ-1"]
        assert "_ci_failed_pending" not in ts

    def test_worktree_missing_emits_skip(self, fake_config, tmp_state, tmp_log):
        from core.tasks.tickets import fix_ci_failures
        self._seed(make_ticket_state(
            status="pr_created", _ci_failed_pending=True,
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
            status="pr_created", _ci_failed_pending=True, slug=slug,
            prs=[{"repo": "r", "id": 1, "url": "u"}],
        ))
        wt = fake_config["workspace"]["root"] / "tickets" / slug / "r"
        wt.mkdir(parents=True)

        mock_platform = MagicMock()
        mock_platform.get_pr_checks.return_value = [{"name": "lint", "state": "FAILED"}]
        mock_platform.get_failed_logs.return_value = "logs"
        mock_platform.get_pr_diff.return_value = "diff"
        with patch("core.tasks.tickets.make_platform", return_value=mock_platform), \
             patch("features.pr_ci.run_haiku", return_value='{"caused_by_us": false, "reason": "flaky"}'), \
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
            status="pr_created", _ci_failed_pending=True, slug=slug,
            prs=[{"repo": "r", "id": 1, "url": "u"}],
        ))
        wt = fake_config["workspace"]["root"] / "tickets" / slug / "r"
        wt.mkdir(parents=True)

        mock_platform = MagicMock()
        mock_platform.get_pr_checks.return_value = [{"name": "lint", "state": "FAILED"}]
        mock_platform.get_failed_logs.return_value = "logs"
        mock_platform.get_pr_diff.return_value = "diff"
        with patch("core.tasks.tickets.make_platform", return_value=mock_platform), \
             patch("features.pr_ci.run_haiku",
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
            status="pr_created", _ci_failed_pending=True, slug=slug,
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
        assert tickets._comment_snapshot([]) == {"count": 0, "latest_created_at": None}

    def test_picks_max_date(self):
        snap = tickets._comment_snapshot([
            {"created_at": "2026-04-20T00:00:00Z"},
            {"created_at": "2026-04-22T00:00:00Z"},
            {"created_at": "2026-04-21T00:00:00Z"},
        ])
        assert snap == {"count": 3, "latest_created_at": "2026-04-22T00:00:00Z"}

    def test_ignores_missing_dates(self):
        snap = tickets._comment_snapshot([
            {"created_at": "2026-04-20T00:00:00Z"}, {},
        ])
        assert snap == {"count": 2, "latest_created_at": "2026-04-20T00:00:00Z"}


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
        ts = {"status": "pr_created"}
        ticket = make_ticket()
        comments = [
            {"created_at": "2026-04-20T00:00:00Z", "body": "a"},
            {"created_at": "2026-04-21T00:00:00Z", "body": "b"},
        ]
        with patch("features.tickets._fetch_ticket_comments", return_value=comments):
            result = tickets._mark_ticket_merged(fake_config, ticket, ts)
        assert result["status"] == "merged"
        assert result["merged_comment_snapshot"] == {
            "count": 2, "latest_created_at": "2026-04-21T00:00:00Z",
        }
        assert "merged_at" in result

    def test_clears_ci_passed(self, fake_config):
        ts = {"status": "pr_created", "ci_passed": True}
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

    def test_same_external_status_skips(self, tmp_path, fake_config):
        saved, menq = self._run_check(tmp_path, fake_config,
            saved_state={"status": "merged", "slug": "PROJ-1-slug", "branch": "PROJ-1-slug",
                         "merged_external_status": "QA"},
            external_status="QA")
        assert saved is not None
        assert saved["status"] == "merged"
        assert saved["merged_external_status"] == "QA"
        assert "reopened_count" not in saved
        menq.assert_not_called()

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
