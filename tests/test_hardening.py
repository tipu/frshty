import json
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import core.state as state
import core.log as log


@pytest.fixture()
def tmp_state(tmp_path):
    state.init(tmp_path)
    return tmp_path


def test_concurrent_state_no_corruption(tmp_state):
    def writer(key, value):
        for _ in range(50):
            d = state.load("shared")
            d[key] = value
            state.save("shared", d)

    t1 = threading.Thread(target=writer, args=("a", 1))
    t2 = threading.Thread(target=writer, args=("b", 2))
    t1.start(); t2.start()
    t1.join(); t2.join()

    final = state.load("shared")
    assert isinstance(final, dict), "state file must be valid JSON dict after concurrent writes"
    assert "a" in final or "b" in final, "at least one writer's data must survive"


def test_own_prs_worktree_uses_correct_repo(tmp_path):
    from features import own_prs

    repo_a = tmp_path / "repo-a"
    repo_b = tmp_path / "repo-b"
    repo_a.mkdir(); repo_b.mkdir()

    config = {"_state_dir": tmp_path}
    repos = [{"name": "repo-a", "path": repo_a}, {"name": "repo-b", "path": repo_b}]
    pr = {"repo": "repo-b", "id": 42, "branch": "fix/thing", "url": ""}

    calls = []
    def fake_run(cmd, *a, **kw):
        calls.append((cmd, kw.get("cwd", "")))
        m = MagicMock(returncode=0, stdout=b"", stderr=b"")
        m.stdout = b""
        return m

    with patch("features.own_prs.get_repos", return_value=repos), \
         patch("features.own_prs.subprocess.run", side_effect=fake_run):
        own_prs._ensure_worktree(config, pr)

    for cmd, cwd in calls:
        assert str(repo_a) not in str(cwd), f"Used repo-a path: {cmd} cwd={cwd}"


def test_check_ci_skips_when_head_unchanged(tmp_path):
    from features import own_prs

    pr = {"repo": "r", "id": 1, "branch": "fix/x", "url": "", "created_on": "2025-01-01T00:00:00Z"}
    seen = {"ci_fix_sha": "deadbeef"}
    platform = MagicMock()
    platform.get_pr_checks.return_value = [{"state": "FAILED", "name": "lint"}]
    config = {"_state_dir": tmp_path}

    worktree = tmp_path / "wt"
    worktree.mkdir()

    with patch("features.own_prs._ensure_worktree", return_value=worktree), \
         patch("features.own_prs.subprocess.run") as mock_run, \
         patch("features.own_prs.run_claude_code") as mock_cc, \
         patch("features.own_prs.log"):
        mock_run.return_value = MagicMock(returncode=0, stdout="deadbeef\n")
        own_prs._check_ci(config, platform, pr, seen, "http://base")

    mock_cc.assert_not_called()


def test_check_ci_no_push_when_claude_fails(tmp_path):
    from features import own_prs

    pr = {"repo": "r", "id": 1, "branch": "fix/x", "url": "http://u", "created_on": "2025-01-01T00:00:00Z"}
    seen = {}
    platform = MagicMock()
    platform.get_pr_checks.return_value = [{"state": "FAILED", "name": "lint"}]
    config = {"_state_dir": tmp_path}

    worktree = tmp_path / "wt"
    worktree.mkdir()

    with patch("features.own_prs._ensure_worktree", return_value=worktree), \
         patch("features.own_prs.subprocess.run") as mock_run, \
         patch("features.own_prs.run_claude_code", return_value=None):
        mock_run.return_value = MagicMock(returncode=0, stdout=b"abc123\n")
        own_prs._check_ci(config, platform, pr, seen, "http://base")

    platform.push_branch.assert_not_called()


def test_check_stale_emits_only_once():
    from features import own_prs

    pr = {"id": 7, "repo": "r", "url": "http://u", "created_on": "2020-01-01T00:00:00Z"}
    seen = {"stale_notified": True}

    with patch("features.own_prs.log.emit") as mock_emit:
        own_prs._check_stale(pr, seen, "http://base")

    mock_emit.assert_not_called()


def test_none_verdict_does_not_advance_ticket(tmp_path):
    from features import tickets

    ws = {"root": tmp_path, "tickets_dir": "tickets"}
    slug = "PROJ-1-do-thing"
    review_dir = tmp_path / "tickets" / slug / "docs"
    review_dir.mkdir(parents=True)
    (review_dir / "tri-review.md").write_text("some review text")

    config = {"workspace": ws}
    ticket = {"key": "PROJ-1", "summary": "do thing", "url": "", "status": "reviewing"}
    ts = {"status": "reviewing", "slug": slug, "branch": "feat/do-thing"}

    with patch("features.tickets.run_haiku", return_value=None):
        result = tickets._check_reviewing(config, ticket, ts, "http://base")

    assert result["status"] == "reviewing"


def test_tri_review_deleted_on_fail_verdict(tmp_path):
    from features import tickets

    ws = {"root": tmp_path, "tickets_dir": "tickets"}
    slug = "PROJ-2-fix-it"
    review_dir = tmp_path / "tickets" / slug / "docs"
    review_dir.mkdir(parents=True)
    review_file = review_dir / "tri-review.md"
    review_file.write_text("blocking issue found")

    config = {"workspace": ws}
    ticket = {"key": "PROJ-2", "summary": "fix it", "url": "", "status": "reviewing"}
    ts = {"status": "reviewing", "slug": slug, "branch": "fix/it"}

    with patch("features.tickets.run_haiku", return_value="FAIL"), \
         patch("features.tickets.terminal.send_keys"), \
         patch("features.tickets.log"):
        result = tickets._check_reviewing(config, ticket, ts, "http://base")

    assert not review_file.exists()
    assert result["status"] == "planning"


def test_done_ticket_with_prs_preserves_state_on_rediscovery(tmp_path):
    """When a ticket with open PRs goes to done and reappears,
    it should resume at in_review with PRs intact, not restart from new."""
    from features import tickets

    state.init(tmp_path)
    state.save("tickets", {
        "NEC-100": {
            "status": "done",
            "done_at": "2026-04-15T00:00:00+00:00",
            "slug": "NEC-100-fix-the-thing",
            "branch": "NEC-100-fix-the-thing",
            "prs": [{"repo": "backend", "id": 42, "url": "http://pr/42"}],
            "last_comment_ids": {"backend/42": 0},
        }
    })

    config = {
        "_base_url": "http://test",
        "job": {"ticket_system": "", "platform": "github"},
        "workspace": {"root": tmp_path, "repos": [{"name": "backend", "path": tmp_path}],
                      "tickets_dir": "tickets", "base_branch": "main", "ticket_layout": "flat"},
        "github": {"repo": "org/backend"},
        "pr": {},
        "features": {},
    }

    (tmp_path / "tickets" / "NEC-100-fix-the-thing").mkdir(parents=True)

    assigned = [{"key": "NEC-100", "summary": "Fix the thing", "status": "In Review", "url": "",
                 "description": "", "attachments": [], "related": [], "subtasks": []}]

    with patch("features.tickets._fetch_tickets", return_value=assigned), \
         patch("features.tickets.get_repos", return_value=[{"name": "backend", "path": tmp_path}]), \
         patch("features.tickets.make_platform") as mock_platform, \
         patch("features.tickets.subprocess.run") as mock_run, \
         patch("features.tickets.terminal.ensure_session"), \
         patch("features.tickets.terminal.send_keys"), \
         patch("features.tickets.time.sleep"), \
         patch("features.tickets.log"):
        mock_run.return_value = MagicMock(returncode=0, stdout="main\n", stderr="")
        p = MagicMock()
        p.monitor_ci.side_effect = lambda ticket, ts, base_url: ts
        p.get_pr_info.return_value = {"state": "OPEN", "updated_on": "", "mergeable": "MERGEABLE"}
        p.get_pr_state.return_value = "OPEN"
        p.get_pr_comments.return_value = []
        mock_platform.return_value = p

        tickets.check(config)

    result = state.load("tickets")["NEC-100"]
    assert result["status"] in ("pr_created", "in_review"), f"Expected pr_created or in_review, got {result['status']}"
    assert result.get("prs"), "PRs should be preserved"
    assert result["prs"][0]["id"] == 42


def test_done_ticket_without_prs_restarts_fresh(tmp_path):
    """When a ticket without PRs goes to done and reappears,
    it should restart from new."""
    from features import tickets

    state.init(tmp_path)
    state.save("tickets", {
        "NEC-200": {
            "status": "done",
            "done_at": "2026-04-15T00:00:00+00:00",
            "slug": "NEC-200-add-feature",
            "branch": "NEC-200-add-feature",
        }
    })

    config = {
        "_base_url": "http://test",
        "job": {"ticket_system": "", "platform": "github"},
        "workspace": {"root": tmp_path, "repos": [{"name": "backend", "path": tmp_path}],
                      "tickets_dir": "tickets", "base_branch": "main", "ticket_layout": "flat"},
        "github": {"repo": "org/backend"},
        "pr": {},
        "features": {},
    }

    assigned = [{"key": "NEC-200", "summary": "Add feature", "status": "In Progress", "url": "",
                 "description": "", "attachments": [], "related": [], "subtasks": []}]

    with patch("features.tickets._fetch_tickets", return_value=assigned), \
         patch("features.tickets.get_repos", return_value=[{"name": "backend", "path": tmp_path}]), \
         patch("features.tickets.make_platform"), \
         patch("features.tickets.subprocess.run") as mock_run, \
         patch("features.tickets.terminal.ensure_session"), \
         patch("features.tickets.terminal.send_keys"), \
         patch("features.tickets.time.sleep"), \
         patch("features.tickets.log"):
        mock_run.return_value = MagicMock(returncode=0, stdout="main\n", stderr="")
        tickets.check(config)

    result = state.load("tickets")["NEC-200"]
    assert result["status"] == "pr_ready", f"Expected pr_ready, got {result['status']}"


def test_empty_branch_falls_back_to_pr_id_slug(tmp_path):
    from features import reviewer

    pr = {"repo": "myrepo", "id": 99, "branch": "", "url": "http://u", "updated_on": "x"}
    config = {"_state_dir": tmp_path, "workspace": {"root": tmp_path, "repos": []}}

    with patch("features.reviewer._ensure_review_worktree", return_value=None), \
         patch("features.reviewer._load_conventions", return_value=""), \
         patch("features.reviewer._run_all_personas", return_value=[("spec", {"issues": [], "verdict": "approved"})]), \
         patch("features.reviewer._merge_reviews", return_value={"issues": [], "verdict": "approved"}):
        reviewer.review_pr(config, MagicMock(), pr)

    expected_dir = tmp_path / "reviews" / "myrepo" / "pr-99"
    assert expected_dir.exists()


def test_dismiss_all_truncates_log(tmp_state):
    log.init(tmp_state, "testjob")

    over = log.MAX_LOG_LINES + 50
    for i in range(over):
        log.emit("evt", f"msg {i}")

    log.dismiss_all()

    lines = (tmp_state / "logs" / "testjob.jsonl").read_text().splitlines()
    assert len(lines) <= log.MAX_LOG_LINES


def test_ticket_status_transition_rejects_illegal():
    from core.ticket_status import transition

    with pytest.raises(ValueError, match="Illegal transition"):
        transition("new", "merged")

    with pytest.raises(ValueError, match="Illegal transition"):
        transition("pr_failed", "pr_created")

    assert transition("pr_failed", "pr_ready") == "pr_ready"


def test_ticket_status_transition_allows_done_from_any():
    from core.ticket_status import transition

    for status in ["new", "planning", "reviewing", "pr_ready", "pr_created", "in_review", "merged", "pr_failed"]:
        assert transition(status, "done") == "done"


def test_push_branch_rejects_empty():
    from features.platforms import BitbucketPlatform

    platform = BitbucketPlatform.__new__(BitbucketPlatform)
    result = platform.push_branch("/tmp", "")
    assert result["ok"] is False
    assert "empty" in result["error"]
