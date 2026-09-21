"""The ticket comment path must fail closed.

Three fail-opens shipped together on this path. A failed comment read became
an empty comment list, so an unreachable platform read as a pull request with
no comments. A run that produced no diff resolved the thread as "already
addressed" — 63 times, and work items 15, 22 and 46 are that complaint. The
return value of resolve_comment was discarded, so a thread that refused to
resolve was recorded as answered. A fix also started in a dirty worktree, so
git add -A credited someone else's file to the comment.
"""
import subprocess
from unittest.mock import MagicMock, patch

import features.tickets as tickets
from tests.conftest import make_ticket_state


COMMENT = {
    "id": 100,
    "body": "You are overriding the original definition without removing the old one",
    "author_id": "reviewer1",
    "author_name": "Reviewer",
    "path": "app.py",
    "line": 1,
    "parent_id": None,
    "resolved": False,
    "resolvable": True,
    "created_on": "2026-09-20T23:00:00Z",
    "created_at": "2026-09-20T23:00:00Z",
    "updated_at": "2026-09-20T23:00:00Z",
}


class TicketCommentHarness:
    def _init_git_pair(self, tmp_path, branch):
        origin = tmp_path / "origin.git"
        wt = tmp_path / "wt"
        subprocess.run(["git", "init", "--bare", str(origin)], check=True, capture_output=True)
        subprocess.run(["git", "clone", str(origin), str(wt)], check=True, capture_output=True)
        for k, v in (("user.email", "t@example.com"), ("user.name", "t"),
                     ("commit.gpgsign", "false")):
            subprocess.run(["git", "config", k, v], cwd=str(wt), check=True, capture_output=True)
        (wt / "app.py").write_text("original\n")
        subprocess.run(["git", "add", "-A"], cwd=str(wt), check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", "init"], cwd=str(wt), check=True, capture_output=True)
        subprocess.run(["git", "checkout", "-b", branch], cwd=str(wt), check=True, capture_output=True)
        subprocess.run(["git", "push", "-u", "origin", branch], cwd=str(wt), check=True, capture_output=True)
        return wt

    def _setup(self, fake_config, slug, comments=(COMMENT,)):
        ts = make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "repo", "id": 99, "branch": slug, "url": "http://u"}],
        )
        ticket = {"key": "PROJ-1", "summary": "Do thing", "url": "http://j/PROJ-1"}
        platform = MagicMock()
        platform.get_pr_state.return_value = "OPEN"
        platform.get_pr_comments.return_value = list(comments) if comments else comments
        platform.push_branch.return_value = {"ok": True}
        platform.self_id.return_value = "bot-self"
        bb_config = {
            **fake_config,
            "job": {**fake_config["job"], "platform": "bitbucket"},
            "bitbucket": {"org": "x", "user_account_id": "bot-self"},
        }
        return ts, ticket, platform, bb_config

    def _run(self, bb_config, ticket, ts, wt, platform, claude, classifier=None):
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "repo", "path": wt.parent}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.run_balanced",
                   return_value='{"results": [{"i": 0, "actionable": true}]}') as batch, \
             patch("features.tickets.run_claude_code", side_effect=claude):
            if classifier is not None:
                classifier.append(batch)
            return tickets._check_in_review(bb_config, ticket, ts, "http://base")

    def _fixes_app_py(self, wt):
        """An agent run that writes and commits its own fix, as the real one
        does. Committing inside the run keeps the pre-commit hook path out of
        these tests, which are about the fail-open branches."""
        def claude(prompt, cwd=None, **kwargs):
            (wt / "app.py").write_text("fixed\n")
            subprocess.run(["git", "add", "-A"], cwd=str(wt), check=True, capture_output=True)
            subprocess.run(["git", "commit", "-m", "fix the override"], cwd=str(wt),
                           check=True, capture_output=True)
            return "wrote the fix"
        return claude


class TestAFailedReadIsNotAnEmptyPr(TicketCommentHarness):
    def test_the_pull_request_is_skipped_and_nothing_advances(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug)
        platform.get_pr_comments.return_value = None
        ts["last_comment_ids"] = {"repo/99": 50}

        out = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: "")

        platform.resolve_comment.assert_not_called()
        platform.push_branch.assert_not_called()
        assert out["last_comment_ids"]["repo/99"] == 50, (
            "the cursor must not move on a poll that could not read the comments"
        )
        assert out[tickets.RECONCILE_READ_KEY] is False

    def test_a_pull_request_with_no_comments_is_not_a_failure(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """The control. An empty list and an unreadable platform must not
        produce the same reconcile result."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug, comments=[])

        out = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: "")

        assert out[tickets.RECONCILE_READ_KEY] is True
        assert out[tickets.RECONCILE_OWED_KEY] == 0


class TestAFailedResolveLeavesTheCommentOwed(TicketCommentHarness):
    def test_the_entry_is_fix_failed_and_the_cursor_holds(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug)
        platform.resolve_comment.return_value = {"status": "error", "detail": "thread gone"}

        out = self._run(bb_config, ticket, ts, wt, platform, self._fixes_app_py(wt))

        platform.resolve_comment.assert_called_once_with("repo", 99, 100)
        saved = tickets._load_pr_comments(bb_config, slug)
        entry = next(e for e in saved if e["id"] == 100)
        assert entry["status"] == "fix_failed"
        assert out.get("last_comment_ids", {}).get("repo/99") is None, (
            "a comment whose thread would not resolve must stay eligible"
        )

    def test_a_successful_resolve_still_settles_the_comment(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """The control: with resolve_comment reporting success the same run
        records the comment as addressed and advances the cursor."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug)
        platform.resolve_comment.return_value = {"status": "resolved"}

        out = self._run(bb_config, ticket, ts, wt, platform, self._fixes_app_py(wt))

        saved = tickets._load_pr_comments(bb_config, slug)
        entry = next(e for e in saved if e["id"] == 100)
        assert entry["status"] == "addressed"
        assert out["last_comment_ids"]["repo/99"] == 100


class TestTheOwedSetCannotBeEmptiedByAccident(TicketCommentHarness):
    def test_a_reply_in_an_unresolved_thread_is_owed(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """A reviewer's reply carries parent_id. The unresolved list on the
        page shows thread roots only, but the owed set must hold the reply
        as well, or the poll that detects it also releases the merge."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        reply = {**COMMENT, "id": 101, "parent_id": 100,
                 "body": "still not right"}
        ts, ticket, platform, bb_config = self._setup(
            fake_config, slug, comments=[COMMENT, reply])
        ts["last_comment_ids"] = {"repo/99": 100}
        platform.get_pr_comments.return_value = [
            {**COMMENT, "resolved": True}, reply]

        out = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: None)

        assert out[tickets.RECONCILE_OWED_KEY] == 1
        assert out["prs"][0]["unresolved_comments"] == [], (
            "the page list still shows thread roots only"
        )

    def test_an_unreadable_history_holds_the_merge(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """_load_pr_comments answers a truncated pr_comments.json with an
        empty list, which reads as 'nothing was ever owed'."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug, comments=[])
        path = tickets._pr_comments_path(bb_config, slug)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('[{"id": 1, "status": "needs_re')

        out = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: None)

        assert out[tickets.RECONCILE_READ_KEY] is False

    def test_an_unreadable_history_is_not_overwritten(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """The poll used to load the broken file as [] and save that back, so
        the second poll saw a readable empty history, found nothing owed and
        merged. The file has to survive the hold."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug, comments=[])
        path = tickets._pr_comments_path(bb_config, slug)
        path.parent.mkdir(parents=True, exist_ok=True)
        broken = '[{"id": 1, "status": "needs_re'
        path.write_text(broken)

        ts = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: None)
        out = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: None)

        assert path.read_text() == broken
        assert out[tickets.RECONCILE_READ_KEY] is False
        platform.get_pr_comments.assert_not_called()

    def test_a_history_row_that_is_not_a_record_holds_the_merge(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug, comments=[])
        path = tickets._pr_comments_path(bb_config, slug)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('["not a record"]')

        out = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: None)

        assert out[tickets.RECONCILE_READ_KEY] is False

    def test_a_readable_history_does_not_hold_the_merge(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """The control."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug, comments=[])
        path = tickets._pr_comments_path(bb_config, slug)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]")

        out = self._run(bb_config, ticket, ts, wt, platform, lambda *a, **k: None)

        assert out[tickets.RECONCILE_READ_KEY] is True


class TestNoWorktreeHoldsTheComment(TicketCommentHarness):
    def test_an_actionable_comment_with_no_worktree_holds_the_cursor(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """Without a worktree the fix cannot run. The entry used to stay
        'new', which is not a retryable failure, so the cursor advanced past
        the comment and nothing ever read it again."""
        slug = "PROJ-1-do-the-thing"
        self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug)

        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.get_repos", return_value=[]), \
             patch("features.tickets.run_balanced",
                   return_value='{"results": [{"i": 0, "actionable": true}]}'), \
             patch("features.tickets.run_claude_code", return_value="") as claude:
            out = tickets._check_in_review(bb_config, ticket, ts, "http://base")

        claude.assert_not_called()
        assert out.get("last_comment_ids", {}).get("repo/99") is None
        saved = tickets._load_pr_comments(bb_config, slug)
        entry = next(e for e in saved if e["id"] == 100)
        assert entry["status"] == "fix_failed"
        assert out.get("comment_fix_attempts", {}) == {}, (
            "a missing worktree says nothing about the comment; charging it "
            "caps the comment after two polls and strands it below the cursor"
        )

    def test_a_missing_worktree_never_spends_the_retry_budget(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """MAX_PR_COMMENT_FIX_ATTEMPTS is two. Two polls with no worktree
        used to cap the comment and advance the cursor past it, so restoring
        the worktree could not bring it back."""
        slug = "PROJ-1-do-the-thing"
        self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug)

        for _ in range(tickets.MAX_PR_COMMENT_FIX_ATTEMPTS + 1):
            with patch("features.tickets.make_platform", return_value=platform), \
                 patch("features.tickets.get_repos", return_value=[]), \
                 patch("features.tickets.run_balanced",
                       return_value='{"results": [{"i": 0, "actionable": true}]}'), \
                 patch("features.tickets.run_claude_code", return_value=""):
                ts = tickets._check_in_review(bb_config, ticket, ts, "http://base")

        assert ts.get("last_comment_ids", {}).get("repo/99") is None
        assert ts.get("comment_fix_attempts", {}) == {}


class TestADirtyWorktreeRefusesTheFix(TicketCommentHarness):
    def test_the_run_never_starts(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """A stray Pipfile was committed as the answer to a comment about an
        attachment id on django-drf-app 203. git add -A stages whatever was
        already there, so the run must refuse to start."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        (wt / "Pipfile").write_text("left over from other work\n")
        ts, ticket, platform, bb_config = self._setup(fake_config, slug)
        ran = []

        def claude(prompt, cwd=None, **kwargs):
            ran.append(prompt)
            return "wrote the fix"

        classifier: list = []
        self._run(bb_config, ticket, ts, wt, platform, claude, classifier)

        assert ran == [], "the fix must not run against a dirty worktree"
        assert classifier[0].call_count == 0, (
            "a worktree that stays dirty would otherwise pay for one model "
            "call per poll for as long as it stayed dirty"
        )
        platform.push_branch.assert_not_called()
        platform.resolve_comment.assert_not_called()
        saved = tickets._load_pr_comments(bb_config, slug)
        entry = next(e for e in saved if e["id"] == 100)
        assert entry["status"] == "fix_failed"
        status = subprocess.run(["git", "status", "--porcelain"], cwd=str(wt),
                                capture_output=True, text=True).stdout
        assert "Pipfile" in status, "the stray file must be left where it was"

    def test_a_clean_worktree_runs_the_fix(
        self, fresh_db, fake_config, tmp_state, tmp_path
    ):
        """The control: the same setup without the stray file runs and pushes."""
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts, ticket, platform, bb_config = self._setup(fake_config, slug)
        ran = []

        def claude(prompt, cwd=None, **kwargs):
            ran.append(prompt)
            (wt / "app.py").write_text("fixed\n")
            subprocess.run(["git", "add", "-A"], cwd=str(wt), check=True, capture_output=True)
            subprocess.run(["git", "commit", "-m", "fix the override"], cwd=str(wt),
                           check=True, capture_output=True)
            return "wrote the fix"

        classifier: list = []
        self._run(bb_config, ticket, ts, wt, platform, claude, classifier)

        assert len(ran) == 1
        assert platform.push_branch.call_count == 1
        assert classifier[0].call_count == 1, (
            "the control: a clean worktree does reach the classifier, so the "
            "assertion above measures the hold and not the harness"
        )
