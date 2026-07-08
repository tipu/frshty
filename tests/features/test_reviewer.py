import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import time
import pytest

import core.state as state
from features import reviewer
from tests.conftest import make_pr


class TestExtractChangedPaths:
    def test_extracts_paths(self):
        diff = "diff --git a/src/main.py b/src/main.py\n+++ b/src/main.py\n"
        assert reviewer._extract_changed_paths(diff) == ["src/main.py"]

    def test_multiple_files(self):
        diff = (
            "diff --git a/a.py b/a.py\n"
            "diff --git a/b.py b/b.py\n"
        )
        assert reviewer._extract_changed_paths(diff) == ["a.py", "b.py"]

    def test_empty_diff(self):
        assert reviewer._extract_changed_paths("") == []


class TestReadFunctionContext:
    def test_reads_around_line(self, tmp_path):
        f = tmp_path / "test.py"
        lines = [f"line {i}" for i in range(200)]
        f.write_text("\n".join(lines))
        context = reviewer._read_function_context(tmp_path, "test.py", 100)
        assert "line 100" in context
        assert "line 40" in context
        assert "line 159" in context

    def test_missing_file(self, tmp_path):
        assert reviewer._read_function_context(tmp_path, "nope.py", 10) == ""

    def test_near_start(self, tmp_path):
        f = tmp_path / "test.py"
        f.write_text("a\nb\nc\n")
        context = reviewer._read_function_context(tmp_path, "test.py", 1)
        assert "a" in context


class TestReadChangedFiles:
    def test_reads_files(self, tmp_path):
        (tmp_path / "a.py").write_text("content a")
        diff = "diff --git a/a.py b/a.py\n"
        result = reviewer._read_changed_files(diff, tmp_path)
        assert "content a" in result

    def test_skips_large_files(self, tmp_path):
        (tmp_path / "big.py").write_text("x" * 70_000)
        diff = "diff --git a/big.py b/big.py\n"
        result = reviewer._read_changed_files(diff, tmp_path)
        assert result == ""

    def test_skips_missing_files(self, tmp_path):
        diff = "diff --git a/gone.py b/gone.py\n"
        result = reviewer._read_changed_files(diff, tmp_path)
        assert result == ""


class TestBuildPersonaPrompt:
    def test_includes_persona_text(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("PERSONA TEXT", pr, "diff", "", "", False)
        assert "PERSONA TEXT" in prompt

    def test_includes_diff(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "my diff content", "", "", False)
        assert "my diff content" in prompt

    def test_includes_conventions(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "diff", "CONV TEXT", "", False)
        assert "CONV TEXT" in prompt

    def test_includes_tool_hint_when_tools(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "diff", "", "", True)
        assert "read-only access" in prompt

    def test_includes_ticket_context_when_given(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "diff", "", "", False,
                                                ticket_context="TICKET GOAL + SIBLING DIFFS")
        assert "TICKET GOAL + SIBLING DIFFS" in prompt
        assert "--- TICKET CONTEXT ---" in prompt

    def test_no_ticket_context_block_when_empty(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "diff", "", "", False)
        assert "TICKET CONTEXT" not in prompt


class TestTicketContextFor:
    def _prs(self):
        return [
            {"repo": "backend", "id": 1, "branch": "JIRA-9-x"},
            {"repo": "frontend", "id": 2, "branch": "JIRA-9-x"},
        ]

    def test_includes_goal_and_sibling_diffs(self, tmp_path):
        prs = self._prs()
        diffs = {"backend/1": "backend diff", "frontend/2": "frontend diff"}
        with patch("features.reviewer.presentation.resolve_ticket_goal", return_value="the goal"):
            ctx = reviewer._ticket_context_for({}, prs[0], "JIRA-9", prs, diffs)
        assert "the goal" in ctx
        assert "frontend diff" in ctx
        assert "backend diff" not in ctx

    def test_no_ticket_skips_sibling_diffs(self, tmp_path):
        prs = self._prs()
        diffs = {"backend/1": "backend diff", "frontend/2": "frontend diff"}
        with patch("features.reviewer.presentation.resolve_ticket_goal", return_value="the goal"):
            ctx = reviewer._ticket_context_for({}, prs[0], "__no_ticket__", prs, diffs)
        assert "the goal" in ctx
        assert "frontend diff" not in ctx

    def test_truncates_huge_sibling_diff(self, tmp_path):
        prs = self._prs()
        diffs = {"backend/1": "d", "frontend/2": "x" * (reviewer.SIBLING_DIFF_CHAR_CAP + 100)}
        with patch("features.reviewer.presentation.resolve_ticket_goal", return_value=""):
            ctx = reviewer._ticket_context_for({}, prs[0], "JIRA-9", prs, diffs)
        assert "[diff truncated]" in ctx
        assert len(ctx) < reviewer.SIBLING_DIFF_CHAR_CAP + 500


class TestMergeReviews:
    def test_single_result_wraps_agreed_by(self):
        data = {"verdict": "approved", "issues": [{"body": "issue1", "severity": "suggestion"}]}
        result = reviewer._merge_reviews([("spec", data)])
        assert result["issues"][0]["agreed_by"] == ["spec"]

    def test_multiple_results_fallback_on_haiku_failure(self):
        data1 = {"verdict": "approved", "issues": [{"body": "a", "severity": "suggestion"}]}
        data2 = {"verdict": "changes_requested", "issues": [{"body": "b", "severity": "blocking"}]}
        with patch("features.reviewer.run_haiku", return_value=None):
            result = reviewer._merge_reviews([("spec", data1), ("breakage", data2)])
        assert len(result["issues"]) == 2


class TestValidateSingle:
    def test_no_path_returns_issue(self):
        issue = {"body": "problem", "severity": "blocking"}
        result = reviewer._validate_single((issue, None))
        assert result == issue

    def test_false_positive_returns_none(self, tmp_path):
        f = tmp_path / "test.py"
        f.write_text("\n".join([f"line{i}" for i in range(200)]))
        issue = {"body": "problem", "severity": "blocking", "path": "test.py", "line": 50}
        with patch("features.reviewer.run_sonnet", return_value='{"decision": "false_positive", "reason": "guard clause"}'), \
             patch("features.reviewer.extract_json", return_value={"decision": "false_positive", "reason": "guard clause"}), \
             patch("features.reviewer.log"):
            result = reviewer._validate_single((issue, tmp_path))
        assert result is None

    def test_valid_returns_issue(self, tmp_path):
        f = tmp_path / "test.py"
        f.write_text("\n".join([f"line{i}" for i in range(200)]))
        issue = {"body": "problem", "severity": "blocking", "path": "test.py", "line": 50}
        with patch("features.reviewer.run_sonnet", return_value='{"decision": "valid", "reason": "real"}'), \
             patch("features.reviewer.extract_json", return_value={"decision": "valid", "reason": "real"}):
            result = reviewer._validate_single((issue, tmp_path))
        assert result == issue


class TestSimplifyBody:
    def test_returns_simplified(self):
        with patch("features.reviewer.run_haiku", return_value="simplified text"):
            assert reviewer._simplify_body("verbose body") == "simplified text"

    def test_fallback_on_none(self):
        with patch("features.reviewer.run_haiku", return_value=None):
            assert reviewer._simplify_body("original") == "original"


class TestStyleMatch:
    def test_no_examples_returns_body(self):
        assert reviewer._style_match("body", "") == "body"

    def test_with_examples_calls_haiku(self):
        with patch("features.reviewer.run_haiku", return_value="styled"):
            assert reviewer._style_match("body", "example1\nexample2") == "styled"


class TestReviewPr:
    def test_no_diff_returns_none(self, tmp_path):
        mock_platform = MagicMock()
        mock_platform.get_pr_diff.return_value = None
        pr = make_pr()
        config = {"_state_dir": tmp_path, "workspace": {"root": tmp_path, "repos": []}}
        result = reviewer.review_pr(config, mock_platform, pr)
        assert result is None

    def test_all_personas_fail_returns_none(self, tmp_path):
        mock_platform = MagicMock()
        mock_platform.get_pr_diff.return_value = "diff content"
        pr = make_pr()
        config = {"_state_dir": tmp_path, "workspace": {"root": tmp_path, "repos": []}}

        with patch("features.reviewer._ensure_review_worktree", return_value=None), \
             patch("features.reviewer._load_conventions", return_value=""), \
             patch("features.reviewer._run_all_personas", return_value=[("spec", None), ("breakage", None), ("maint", None)]):
            result = reviewer.review_pr(config, mock_platform, pr)
        assert result is None


class TestExtractTicketFromPr:
    def test_extracts_ticket_from_state(self):
        pr = {"repo": "backend", "id": 123, "branch": "feature/x"}
        ticket_state = {
            "JIRA-456": {"prs": [{"repo": "backend", "id": 123}]}
        }
        result = reviewer._extract_ticket_from_pr(pr, ticket_state)
        assert result == "JIRA-456"

    def test_extracts_ticket_from_branch_name(self):
        pr = {"repo": "frontend", "id": 789, "branch": "JIRA-789/ui-fix"}
        ticket_state = {}
        result = reviewer._extract_ticket_from_pr(pr, ticket_state)
        assert result == "JIRA-789"

    def test_returns_none_when_no_ticket(self):
        pr = {"repo": "backend", "id": 999, "branch": "feature/something"}
        ticket_state = {}
        result = reviewer._extract_ticket_from_pr(pr, ticket_state)
        assert result is None

    def test_state_lookup_takes_precedence(self):
        pr = {"repo": "backend", "id": 111, "branch": "JIRA-999/fix"}
        ticket_state = {
            "JIRA-111": {"prs": [{"repo": "backend", "id": 111}]}
        }
        result = reviewer._extract_ticket_from_pr(pr, ticket_state)
        assert result == "JIRA-111"


class TestPrNeedsTracking:
    def test_tracks_unreviewed_pr(self):
        pr = {"repo": "backend", "id": 123, "updated_on": "u1", "head_sha": "h1"}
        assert reviewer._pr_needs_tracking({}, pr) is True

    def test_skips_reviewed_unchanged_pr(self):
        pr = {"repo": "backend", "id": 123, "updated_on": "u1", "head_sha": "h1"}
        review_state = {
            "backend/123": {
                "reviewed": True,
                "last_updated": "u1",
                "last_head_sha": "h1",
            }
        }
        assert reviewer._pr_needs_tracking(review_state, pr) is False

    def test_tracks_reviewed_pr_when_head_changes(self):
        pr = {"repo": "backend", "id": 123, "updated_on": "u1", "head_sha": "h2"}
        review_state = {
            "backend/123": {
                "reviewed": True,
                "last_updated": "u1",
                "last_head_sha": "h1",
            }
        }
        assert reviewer._pr_needs_tracking(review_state, pr) is True


class TestTrackPendingPrs:
    def test_tracks_new_pr_for_ticket(self):
        pending = {}
        pr = {"repo": "backend", "id": 123}
        ticket_state = {"JIRA-456": {"prs": [{"repo": "backend", "id": 123}]}}

        with patch("features.reviewer.time.time", return_value=1000.0):
            reviewer._track_pending_prs(pending, [pr], ticket_state, {})

        assert "JIRA-456" in pending
        assert pending["JIRA-456"]["prs"] == [pr]
        assert pending["JIRA-456"]["tracked_at"] == 1000.0
        assert pending["JIRA-456"]["last_pr_at"] == 1000.0

    def test_extends_timeout_on_new_pr_for_same_ticket(self):
        pending = {
            "JIRA-456": {
                "tracked_at": 1000.0,
                "last_pr_at": 1000.0,
                "prs": [{"repo": "backend", "id": 123}]
            }
        }
        new_pr = {"repo": "frontend", "id": 789}
        ticket_state = {
            "JIRA-456": {
                "prs": [
                    {"repo": "backend", "id": 123},
                    {"repo": "frontend", "id": 789}
                ]
            }
        }

        with patch("features.reviewer.time.time", return_value=1300.0):
            reviewer._track_pending_prs(pending, [new_pr], ticket_state, {})

        assert pending["JIRA-456"]["last_pr_at"] == 1300.0
        assert len(pending["JIRA-456"]["prs"]) == 2

    def test_no_duplicate_prs(self):
        pending = {
            "JIRA-456": {
                "tracked_at": 1000.0,
                "last_pr_at": 1000.0,
                "prs": [{"repo": "backend", "id": 123}]
            }
        }
        same_pr = {"repo": "backend", "id": 123}
        ticket_state = {"JIRA-456": {"prs": [{"repo": "backend", "id": 123}]}}

        with patch("features.reviewer.time.time", return_value=1100.0):
            reviewer._track_pending_prs(pending, [same_pr], ticket_state, {})

        assert len(pending["JIRA-456"]["prs"]) == 1

    def test_skips_unchanged_reviewed_pr(self):
        pending = {}
        pr = {"repo": "backend", "id": 123, "updated_on": "u1", "head_sha": "h1"}
        ticket_state = {"JIRA-456": {"prs": [{"repo": "backend", "id": 123}]}}
        review_state = {
            "backend/123": {
                "reviewed": True,
                "last_updated": "u1",
                "last_head_sha": "h1",
            }
        }

        with patch("features.reviewer.time.time", return_value=1000.0):
            reviewer._track_pending_prs(pending, [pr], ticket_state, review_state)

        assert pending == {}


class TestProcessReadyTickets:
    def test_processes_ticket_after_quiet_period(self, tmp_path):
        pending = {
            "JIRA-456": {
                "tracked_at": 1000.0,
                "last_pr_at": 1000.0,
                "prs": [{"repo": "backend", "id": 123}]
            }
        }
        config = {"_state_dir": tmp_path, "_base_url": "http://localhost"}

        with patch("features.reviewer.time.time", return_value=1950.0), \
             patch("features.reviewer.review_ticket_prs", return_value=[]) as mock_review:
            reviewer._process_ready_tickets(config, pending)

        assert mock_review.called
        assert "JIRA-456" not in pending

    def test_ignores_ticket_with_active_timeout(self, tmp_path):
        pending = {
            "JIRA-456": {
                "tracked_at": 1000.0,
                "last_pr_at": 1500.0,
                "prs": [{"repo": "backend", "id": 123}]
            }
        }
        config = {"_state_dir": tmp_path}

        with patch("features.reviewer.time.time", return_value=1700.0), \
             patch("features.reviewer.review_ticket_prs") as mock_review:
            reviewer._process_ready_tickets(config, pending)

        assert not mock_review.called
        assert "JIRA-456" in pending

    def test_failed_ticket_enters_cooldown(self, tmp_path):
        pending = {
            "JIRA-456": {
                "tracked_at": 1000.0,
                "last_pr_at": 1000.0,
                "prs": [{"repo": "backend", "id": 123}],
            }
        }
        config = {"_state_dir": tmp_path, "_base_url": "http://localhost"}

        with patch("features.reviewer.time.time", return_value=1950.0), \
             patch("features.reviewer.review_ticket_prs", return_value=[{"repo": "backend", "id": 123}]):
            reviewer._process_ready_tickets(config, pending)

        assert "JIRA-456" in pending
        assert pending["JIRA-456"]["retry_after"] == 1950.0 + reviewer.REVIEW_RETRY_COOLDOWN_SECONDS
        assert pending["JIRA-456"]["prs"] == [{"repo": "backend", "id": 123}]


class TestCheckPersistsReviews:
    def test_check_does_not_clobber_reviews_written_by_review_ticket_prs(self, tmp_state, tmp_log):
        state.save("tickets", {"JIRA-1": {"prs": [{"repo": "myrepo", "id": 7}]}})
        state.save("reviews", {})
        state.save("reviews_pending", {
            "JIRA-1": {
                "tracked_at": 0.0,
                "last_pr_at": 0.0,
                "prs": [{"repo": "myrepo", "id": 7, "url": "u", "branch": "b"}],
            }
        })

        pr = make_pr(repo="myrepo", id=7, branch="b", url="u", head_sha="abc")
        fake_platform = MagicMock()
        fake_platform.list_pending_reviews_for_me.return_value = [pr]

        def fake_review_ticket_prs(config, ticket_key, prs):
            rs = state.load("reviews")
            for p in prs:
                rs[f"{p['repo']}/{p['id']}"] = {"reviewed": True, "ticket": ticket_key}
            state.save("reviews", rs)
            return []

        config = {"_state_dir": tmp_state, "_base_url": "http://localhost"}

        with patch("features.reviewer.make_platform", return_value=fake_platform), \
             patch("features.reviewer.review_ticket_prs", side_effect=fake_review_ticket_prs), \
             patch("features.reviewer.time.time", return_value=10_000.0):
            reviewer.check(config)

        saved = state.load("reviews")
        assert "myrepo/7" in saved
        assert saved["myrepo/7"]["reviewed"] is True

class TestReviewTicketPrsPersistence:
    def test_persists_reviewed_prs_and_fails_unreviewed(self, tmp_state, tmp_log):
        pr_a = make_pr(repo="r", id=1, branch="b1", url="u1", head_sha="sha-a")
        pr_b = make_pr(repo="r", id=2, branch="b2", url="u2", head_sha="sha-b")
        config = {"_state_dir": tmp_state, "_base_url": "http://localhost"}
        ok = {"verdict": "approved", "issues": []}

        with patch("features.reviewer.make_platform", return_value=MagicMock()), \
             patch("features.reviewer.review_ticket", return_value={"r/1": ok, "r/2": None}), \
             patch("features.reviewer.time.time", return_value=1234.0):
            failed = reviewer.review_ticket_prs(config, "JIRA-1", [pr_a, pr_b])

        saved = state.load("reviews")
        assert failed == [pr_b]
        assert "r/1" in saved
        assert saved["r/1"]["reviewed"] is True
        assert saved["r/1"]["last_head_sha"] == "sha-a"
        assert "r/2" not in saved

    def test_no_ticket_bucket_reviews_prs_individually(self, tmp_state, tmp_log):
        pr_a = make_pr(repo="r", id=1, branch="b1", url="u1", head_sha="sha-a")
        pr_b = make_pr(repo="r2", id=2, branch="b2", url="u2", head_sha="sha-b")
        config = {"_state_dir": tmp_state, "_base_url": "http://localhost"}
        ok = {"verdict": "approved", "issues": []}

        with patch("features.reviewer.make_platform", return_value=MagicMock()), \
             patch("features.reviewer.review_pr", return_value=ok) as mock_review, \
             patch("features.reviewer.review_ticket") as mock_ticket, \
             patch("features.reviewer._ticket_context_for", return_value="goal"), \
             patch("features.reviewer.time.time", return_value=1234.0):
            failed = reviewer.review_ticket_prs(config, "__no_ticket__", [pr_a, pr_b])

        assert failed == []
        assert mock_review.call_count == 2
        mock_ticket.assert_not_called()
        saved = state.load("reviews")
        assert saved["r/1"]["reviewed"] is True and saved["r2/2"]["reviewed"] is True

    def test_ticket_reviewed_in_single_pass(self, tmp_state, tmp_log):
        pr_a = make_pr(repo="r", id=1, branch="b1", url="u1", head_sha="sha-a")
        pr_b = make_pr(repo="r2", id=2, branch="b2", url="u2", head_sha="sha-b")
        config = {"_state_dir": tmp_state, "_base_url": "http://localhost"}
        ok = {"verdict": "approved", "issues": []}

        with patch("features.reviewer.make_platform", return_value=MagicMock()), \
             patch("features.reviewer.review_pr") as mock_review, \
             patch("features.reviewer.review_ticket", return_value={"r/1": ok, "r2/2": ok}) as mock_ticket, \
             patch("features.reviewer.time.time", return_value=1234.0):
            failed = reviewer.review_ticket_prs(config, "JIRA-1", [pr_a, pr_b])

        assert failed == []
        mock_ticket.assert_called_once()
        mock_review.assert_not_called()
        saved = state.load("reviews")
        assert saved["r/1"]["reviewed"] is True and saved["r2/2"]["reviewed"] is True


class TestSplitIssuesByPr:
    def _setup(self):
        prs = [{"repo": "backend", "id": 1}, {"repo": "frontend", "id": 2}]
        diffs = {
            "backend/1": "diff --git a/api/views.py b/api/views.py\n+x\n",
            "frontend/2": "diff --git a/src/App.tsx b/src/App.tsx\n+y\n",
        }
        return prs, diffs

    def test_attributes_by_repo_field(self):
        prs, diffs = self._setup()
        issues = [{"repo": "backend", "path": "api/views.py", "body": "a"},
                  {"repo": "frontend", "path": "src/App.tsx", "body": "b"}]
        out = reviewer._split_issues_by_pr(issues, prs, diffs)
        assert [i["body"] for i in out["backend/1"]] == ["a"]
        assert [i["body"] for i in out["frontend/2"]] == ["b"]

    def test_falls_back_to_unique_path_match(self):
        prs, diffs = self._setup()
        issues = [{"repo": "", "path": "src/App.tsx", "body": "b"}]
        out = reviewer._split_issues_by_pr(issues, prs, diffs)
        assert [i["body"] for i in out["frontend/2"]] == ["b"]
        assert out["backend/1"] == []

    def test_drops_unattributable_issue(self, tmp_log):
        prs, diffs = self._setup()
        issues = [{"repo": "", "path": "nowhere.py", "body": "x"}]
        out = reviewer._split_issues_by_pr(issues, prs, diffs)
        assert out["backend/1"] == [] and out["frontend/2"] == []

    def test_wrong_path_stays_with_named_repo(self):
        prs, diffs = self._setup()
        issues = [{"repo": "backend", "path": "not/in/diff.py", "body": "x"}]
        out = reviewer._split_issues_by_pr(issues, prs, diffs)
        assert [i["body"] for i in out["backend/1"]] == ["x"]


class TestReviewTicket:
    def test_single_pass_splits_and_writes_artifacts(self, tmp_state, tmp_log):
        prs = [{"repo": "backend", "id": 1, "branch": "JIRA-9-x", "url": "u1"},
               {"repo": "frontend", "id": 2, "branch": "JIRA-9-x", "url": "u2"}]
        diffs = {"backend/1": "diff --git a/api/views.py b/api/views.py\n+x\n",
                 "frontend/2": "diff --git a/src/App.tsx b/src/App.tsx\n+y\n"}
        persona_data = {
            "verdict": "changes_requested", "summary": "s", "date": "2026-01-01",
            "issues": [{"repo": "backend", "path": "api/views.py", "line": 1,
                        "severity": "blocking", "body": "bad"}],
        }
        config = {"_state_dir": tmp_state, "_base_url": "http://localhost"}

        with patch("features.reviewer.make_platform", return_value=MagicMock()), \
             patch("features.reviewer._fetch_ticket_diffs", return_value=diffs), \
             patch("features.reviewer.presentation.resolve_ticket_goal", return_value="the goal"), \
             patch("features.reviewer._ensure_review_worktree", return_value=None), \
             patch("features.reviewer._load_conventions", return_value=""), \
             patch("features.reviewer._run_single_persona",
                   side_effect=lambda args: (args[0], dict(persona_data))), \
             patch("features.reviewer._merge_reviews", return_value=dict(persona_data)), \
             patch("features.reviewer._simplify_all_issues", side_effect=lambda i: i), \
             patch("features.reviewer._style_match_all", side_effect=lambda c, i: i):
            results = reviewer.review_ticket(config, "JIRA-9", prs)

        assert results["backend/1"]["verdict"] == "changes_requested"
        assert len(results["backend/1"]["issues"]) == 1
        assert results["frontend/2"]["verdict"] == "approved"
        assert results["frontend/2"]["issues"] == []
        backend_review = tmp_state / "reviews" / "backend" / "JIRA-9-x" / "review.json"
        frontend_review = tmp_state / "reviews" / "frontend" / "JIRA-9-x" / "review.json"
        assert backend_review.exists() and frontend_review.exists()
        queued = json.loads((tmp_state / "reviews" / "backend" / "JIRA-9-x" / "queued_comments.json").read_text())
        assert len(queued) == 1 and queued[0]["path"] == "api/views.py"
