"""A negative comment index must be rejected, not wrapped around.

The three comment mutations take the index straight out of the URL path and
guarded it only against being too large. Python reads a negative index from
the end of the list, so DELETE .../comments/-1 removed the last comment,
PUT .../comments/-1 edited it, and POST .../comments/-1/submit posted it to
the real pull request. None of the three routes had a request test.
"""
import json
import sys
from unittest.mock import MagicMock, patch

import pytest

import core.log as log
import core.state as state


def _comment(idx, pr_id=5):
    return {"pr_id": pr_id, "repo": "myrepo", "pr_url": "http://pr/5",
            "path": "a.py", "line": idx, "severity": "suggestion",
            "persona": "manual", "body": f"comment {idx}", "status": "draft"}


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")
    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        if "frshty" in sys.modules:
            frshty = sys.modules["frshty"]
        else:
            import frshty
    finally:
        sys.argv = saved_argv
    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config({
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {"root": tmp_path, "tickets_dir": "tickets",
                      "ticket_layout": "flat", "base_branch": "main"},
        "features": {"reviews": True}, "pr": {}, "slack": {},
        "_config_path": tmp_path / "config.toml", "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    })
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    branch_dir = tmp_path / "reviews" / "myrepo" / "JIRA-1-branch"
    branch_dir.mkdir(parents=True)
    (branch_dir / "queued_comments.json").write_text(
        json.dumps([_comment(0), _comment(1), _comment(2)]))
    return TestClient(frshty.app, raise_server_exceptions=False), branch_dir


def _stored(branch_dir):
    return json.loads((branch_dir / "queued_comments.json").read_text())


NEGATIVE = [-1, -3, -99]


class TestDeleteCommentIndex:
    @pytest.mark.parametrize("idx", NEGATIVE)
    def test_a_negative_index_is_rejected(self, client, idx):
        c, branch_dir = client
        r = c.delete(f"/api/reviews/myrepo/5/comments/{idx}")
        assert r.status_code == 400, r.text
        assert r.json()["error"] == "invalid index"
        assert len(_stored(branch_dir)) == 3

    def test_an_index_past_the_end_is_rejected(self, client):
        c, branch_dir = client
        assert c.delete("/api/reviews/myrepo/5/comments/3").status_code == 400
        assert len(_stored(branch_dir)) == 3

    def test_a_valid_index_still_deletes(self, client):
        c, branch_dir = client
        r = c.delete("/api/reviews/myrepo/5/comments/1")
        assert r.status_code == 200
        assert [x["body"] for x in _stored(branch_dir)] == ["comment 0", "comment 2"]


class TestUpdateCommentIndex:
    @pytest.mark.parametrize("idx", NEGATIVE)
    def test_a_negative_index_is_rejected(self, client, idx):
        c, branch_dir = client
        r = c.put(f"/api/reviews/myrepo/5/comments/{idx}", json={"body": "hijacked"})
        assert r.status_code == 400, r.text
        assert [x["body"] for x in _stored(branch_dir)] == [
            "comment 0", "comment 1", "comment 2"]

    def test_a_valid_index_still_updates(self, client):
        c, branch_dir = client
        r = c.put("/api/reviews/myrepo/5/comments/2", json={"body": "edited", "line": 9})
        assert r.status_code == 200
        assert _stored(branch_dir)[2]["body"] == "edited"
        assert _stored(branch_dir)[2]["line"] == 9


class TestSubmitCommentIndex:
    @pytest.mark.parametrize("idx", NEGATIVE)
    def test_a_negative_index_posts_nothing(self, client, idx):
        c, branch_dir = client
        platform = MagicMock()
        with patch("web.reviews.make_platform", return_value=platform), \
             patch("web.reviews.review_store.populate_repo_cache"):
            r = c.post(f"/api/reviews/myrepo/5/comments/{idx}/submit")
        assert r.status_code == 400, r.text
        platform.post_pr_comment.assert_not_called()
        platform.edit_pr_comment.assert_not_called()
        assert all(x["status"] == "draft" for x in _stored(branch_dir))

    def test_a_valid_index_still_posts(self, client):
        c, branch_dir = client
        platform = MagicMock()
        platform.post_pr_comment.return_value = {"status": "posted", "id": 77}
        with patch("web.reviews.make_platform", return_value=platform), \
             patch("web.reviews.review_store.populate_repo_cache"):
            r = c.post("/api/reviews/myrepo/5/comments/1/submit")
        assert r.status_code == 200
        assert platform.post_pr_comment.call_args.args[2] == "comment 1"
        assert _stored(branch_dir)[1]["status"] == "submitted"
        assert _stored(branch_dir)[1]["remote_id"] == 77
