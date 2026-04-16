from pathlib import Path
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


class TestCommandForStatus:
    def test_planning(self):
        assert tickets._command_for_status("planning") == "/confer-technical-plan docs/"

    def test_reviewing(self):
        assert "tri-review" in tickets._command_for_status("reviewing")

    def test_other_returns_none(self):
        assert tickets._command_for_status("new") is None
        assert tickets._command_for_status("pr_ready") is None


class TestCheckPlanning:
    def test_no_manifest_stays(self, tmp_path):
        ws = {"root": tmp_path, "tickets_dir": "tickets"}
        config = {"workspace": ws}
        ts = make_ticket_state(status="planning")
        ticket = make_ticket()
        result = tickets._check_planning(config, ticket, ts, "http://base")
        assert result["status"] == "planning"

    def test_manifest_exists_transitions(self, tmp_path):
        ws = {"root": tmp_path, "tickets_dir": "tickets"}
        slug = "PROJ-1-do-the-thing"
        manifest_dir = tmp_path / "tickets" / slug / "docs"
        manifest_dir.mkdir(parents=True)
        (manifest_dir / "change-manifest.md").write_text("plan done")

        config = {"workspace": ws}
        ts = make_ticket_state(status="planning", slug=slug)
        ticket = make_ticket()

        with patch("features.tickets.terminal.send_keys"), \
             patch("features.tickets.log"):
            result = tickets._check_planning(config, ticket, ts, "http://base")
        assert result["status"] == "reviewing"


class TestCheckReviewing:
    def test_no_review_file_stays(self, tmp_path):
        ws = {"root": tmp_path, "tickets_dir": "tickets"}
        config = {"workspace": ws}
        ts = make_ticket_state(status="reviewing")
        result = tickets._check_reviewing(config, make_ticket(), ts, "http://base")
        assert result["status"] == "reviewing"

    def test_pass_verdict_to_pr_ready(self, tmp_path):
        ws = {"root": tmp_path, "tickets_dir": "tickets"}
        slug = "PROJ-1-do-the-thing"
        review_dir = tmp_path / "tickets" / slug / "docs"
        review_dir.mkdir(parents=True)
        (review_dir / "tri-review.md").write_text("all good")

        config = {"workspace": ws}
        ts = make_ticket_state(status="reviewing", slug=slug)

        with patch("features.tickets.run_haiku", return_value="PASS"), \
             patch("features.tickets.log"), \
             patch("features.tickets.events"):
            result = tickets._check_reviewing(config, make_ticket(), ts, "http://base")
        assert result["status"] == "pr_ready"

    def test_fail_verdict_deletes_review_goes_planning(self, tmp_path):
        ws = {"root": tmp_path, "tickets_dir": "tickets"}
        slug = "PROJ-1-do-the-thing"
        review_dir = tmp_path / "tickets" / slug / "docs"
        review_dir.mkdir(parents=True)
        review_file = review_dir / "tri-review.md"
        review_file.write_text("blocking issues")

        config = {"workspace": ws}
        ts = make_ticket_state(status="reviewing", slug=slug)

        with patch("features.tickets.run_haiku", return_value="FAIL"), \
             patch("features.tickets.terminal.send_keys"), \
             patch("features.tickets.log"):
            result = tickets._check_reviewing(config, make_ticket(), ts, "http://base")
        assert result["status"] == "planning"
        assert not review_file.exists()

    def test_none_verdict_stays(self, tmp_path):
        ws = {"root": tmp_path, "tickets_dir": "tickets"}
        slug = "PROJ-1-do-the-thing"
        review_dir = tmp_path / "tickets" / slug / "docs"
        review_dir.mkdir(parents=True)
        (review_dir / "tri-review.md").write_text("review text")

        config = {"workspace": ws}
        ts = make_ticket_state(status="reviewing", slug=slug)

        with patch("features.tickets.run_haiku", return_value=None):
            result = tickets._check_reviewing(config, make_ticket(), ts, "http://base")
        assert result["status"] == "reviewing"


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


class TestDiscoverPrs:
    def test_match_by_branch_populates_prs(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.list_my_open_prs.return_value = [
            {"repo": "r", "id": 99, "branch": "other-branch", "url": "u1"},
            {"repo": "r", "id": 100, "branch": "PROJ-1-do-the-thing", "url": "u2"},
        ]
        ts = make_ticket_state(status="in_review", branch="PROJ-1-do-the-thing")

        with patch("features.tickets.make_platform", return_value=mock_platform):
            result = tickets._discover_prs(fake_config, ts)

        assert result["prs"] == [{"repo": "r", "id": 100, "branch": "PROJ-1-do-the-thing", "url": "u2"}]

    def test_no_match_leaves_ts_unchanged(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.list_my_open_prs.return_value = [
            {"repo": "r", "id": 99, "branch": "other-branch", "url": "u1"},
        ]
        ts = make_ticket_state(status="in_review", branch="PROJ-1-do-the-thing")

        with patch("features.tickets.make_platform", return_value=mock_platform):
            result = tickets._discover_prs(fake_config, ts)

        assert "prs" not in result

    def test_platform_error_is_swallowed(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.list_my_open_prs.side_effect = RuntimeError("api down")
        ts = make_ticket_state(status="in_review", branch="PROJ-1-do-the-thing")

        with patch("features.tickets.make_platform", return_value=mock_platform):
            result = tickets._discover_prs(fake_config, ts)

        assert "prs" not in result

    def test_multiple_matches_all_included(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.list_my_open_prs.return_value = [
            {"repo": "a", "id": 1, "branch": "shared-branch", "url": "u1"},
            {"repo": "b", "id": 2, "branch": "shared-branch", "url": "u2"},
            {"repo": "c", "id": 3, "branch": "other", "url": "u3"},
        ]
        ts = make_ticket_state(status="pr_created", branch="shared-branch")

        with patch("features.tickets.make_platform", return_value=mock_platform):
            result = tickets._discover_prs(fake_config, ts)

        assert len(result["prs"]) == 2
        assert {p["repo"] for p in result["prs"]} == {"a", "b"}


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


class TestHandleCiFailure:
    def test_not_caused_by_us(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_failed_logs.return_value = "logs"
        mock_platform.get_pr_diff.return_value = "diff"
        ts = make_ticket_state(status="pr_created")
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]

        with patch("features.tickets.run_haiku", return_value='{"caused_by_us": false, "reason": "flaky"}'), \
             patch("features.tickets.extract_json", return_value={"caused_by_us": False, "reason": "flaky"}), \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(fake_config, mock_platform, make_ticket(), ts, pr, checks, "http://base")
        assert result.get("ci_fix_attempts", 0) == 0

    def test_caused_by_us_increments(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_failed_logs.return_value = "logs"
        mock_platform.get_pr_diff.return_value = "diff"
        ts = make_ticket_state(status="pr_created")
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]

        with patch("features.tickets.run_haiku", return_value='{"caused_by_us": true, "reason": "bad", "fix_hint": "fix it"}'), \
             patch("features.tickets.extract_json", return_value={"caused_by_us": True, "reason": "bad", "fix_hint": "fix it"}), \
             patch("features.tickets.terminal.send_keys"), \
             patch("features.tickets.log"):
            result = tickets._handle_ci_failure(fake_config, mock_platform, make_ticket(), ts, pr, checks, "http://base")
        assert result["ci_fix_attempts"] == 1

    def test_max_attempts_stops(self, fake_config):
        mock_platform = MagicMock()
        mock_platform.get_failed_logs.return_value = "logs"
        mock_platform.get_pr_diff.return_value = "diff"
        ts = make_ticket_state(status="pr_created", ci_fix_attempts=2)
        pr = {"repo": "r", "id": 1, "url": "u"}
        checks = [{"name": "lint", "state": "FAILED"}]

        with patch("features.tickets.log"):
            result = tickets._handle_ci_failure(fake_config, mock_platform, make_ticket(), ts, pr, checks, "http://base")
        assert result["status"] == "pr_failed"
