"""The route a /tasks review posts its findings to.

The board agent has no access to the review store, so this route is the only
way its findings reach the review page. A POST that lands nowhere has to say
so: a silent 200 would leave the operator waiting for a review that arrived
and was thrown away.
"""
import json

from tests.api.test_routes import client  # noqa: F401

_TASKS_VIEW = "/api/reviews/myrepo/5/comments?provider=tasks"
_PIPELINE_VIEW = "/api/reviews/myrepo/5/comments"


TOKEN = "3f2a" * 8
_ROUTE = f"/api/reviews/myrepo/5/task-review?token={TOKEN}"


def _open_task_review(tmp_path, repo="myrepo", pr_id=5):
    branch_dir = tmp_path / "reviews" / repo / "JIRA-7-branch"
    branch_dir.mkdir(parents=True, exist_ok=True)
    (branch_dir / "review.tasks.json").write_text(json.dumps(
        {"pr_id": pr_id, "repo": repo, "pr_url": "http://pr/5", "provider": "tasks",
         "verdict": "", "status": "reviewing", "token": TOKEN, "summary": "", "issues": []}))
    (branch_dir / "queued_comments.tasks.json").write_text("[]")
    return branch_dir


def _body(severity="blocking"):
    return {"summary": "one line per cleared file", "issues": [
        {"path": "a.ts", "line": 3, "body": "the write is dropped", "severity": severity}]}


class TestPostingATaskReview:
    def test_it_stores_the_findings(self, client, tmp_path):
        branch_dir = _open_task_review(tmp_path)
        resp = client.post(_ROUTE, json=_body())
        assert resp.status_code == 200
        assert resp.json() == {"status": "stored", "verdict": "changes_requested",
                               "issues": 1}
        queued = json.loads((branch_dir / "queued_comments.tasks.json").read_text())
        assert queued[0]["body"] == "the write is dropped"

    def test_a_pr_with_no_open_task_review_is_a_404(self, client, tmp_path):
        resp = client.post(f"/api/reviews/norepo/999/task-review?token={TOKEN}",
                           json=_body())
        assert resp.status_code == 404
        assert resp.json()["error"] == "no /tasks review is open for norepo#999"

    def test_the_pipeline_review_is_not_a_task_review(self, client, tmp_path):
        """Only the placeholder this pipeline wrote may be posted to, or any
        caller could overwrite a review it never ran."""
        branch_dir = tmp_path / "reviews" / "other" / "JIRA-8-branch"
        branch_dir.mkdir(parents=True)
        (branch_dir / "review.json").write_text(json.dumps({"pr_id": 8}))
        (branch_dir / "queued_comments.json").write_text("[]")
        resp = client.post(f"/api/reviews/other/8/task-review?token={TOKEN}",
                           json=_body())
        assert resp.status_code == 404
        assert resp.json()["error"] == "no /tasks review is open for other#8"
        assert not (branch_dir / "review.tasks.json").exists()

    def test_issues_that_are_not_a_list_are_refused(self, client, tmp_path):
        _open_task_review(tmp_path)
        resp = client.post(_ROUTE, json={"issues": "the write is dropped"})
        assert resp.status_code == 400

    def test_a_review_with_no_findings_is_stored_as_approved(self, client, tmp_path):
        _open_task_review(tmp_path)
        resp = client.post(_ROUTE, json={"summary": "cleared", "issues": []})
        assert resp.status_code == 200
        assert resp.json()["verdict"] == "approved"

    def test_the_page_serves_what_was_posted(self, client, tmp_path):
        _open_task_review(tmp_path)
        client.post(_ROUTE, json=_body())
        served = client.get(_TASKS_VIEW).json()
        assert [c["body"] for c in served] == ["the write is dropped"]

    def test_the_pipeline_view_does_not_show_the_task_findings(self, client, tmp_path):
        """The toggle is the only thing that switches between the two reviews."""
        _open_task_review(tmp_path)
        client.post(_ROUTE, json=_body())
        assert len(client.get(_TASKS_VIEW).json()) == 1
        assert client.get(_PIPELINE_VIEW).json() == []

    def test_a_post_without_the_token_is_a_404(self, client, tmp_path):
        """Anything on this host can reach the route, so the token is what says
        the findings came from the task frshty started."""
        branch_dir = _open_task_review(tmp_path)
        resp = client.post("/api/reviews/myrepo/5/task-review", json=_body())
        assert resp.status_code == 404
        assert json.loads((branch_dir / "queued_comments.tasks.json").read_text()) == []

    def test_a_post_with_the_wrong_token_is_a_404(self, client, tmp_path):
        _open_task_review(tmp_path)
        resp = client.post("/api/reviews/myrepo/5/task-review?token=guessed", json=_body())
        assert resp.status_code == 404

    def test_a_second_post_cannot_replace_the_stored_review(self, client, tmp_path):
        branch_dir = _open_task_review(tmp_path)
        client.post(_ROUTE, json=_body())
        resp = client.post(_ROUTE, json={"summary": "cleared", "issues": []})
        assert resp.status_code == 404
        stored = json.loads((branch_dir / "review.tasks.json").read_text())
        assert stored["verdict"] == "changes_requested"
