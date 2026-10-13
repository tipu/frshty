import json
import time
import sys
from unittest.mock import MagicMock, patch

import pytest

import core.log as log
import core.state as state
from features import presentation


def _review(tmp_path, repo, branch, pr_id, diff=""):
    branch_dir = tmp_path / "reviews" / repo / branch
    branch_dir.mkdir(parents=True)
    (branch_dir / "queued_comments.json").write_text(json.dumps(
        [{"pr_id": pr_id, "repo": repo, "pr_url": f"http://pr/{repo}/{pr_id}", "status": "draft"}]))
    if diff:
        (branch_dir / "diff.txt").write_text(diff)
    return branch_dir


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
    _review(tmp_path, "api", "ana-dev-7-thing", 11, diff="diff api\n")
    _review(tmp_path, "web", "ana-dev-7-thing", 22, diff="diff web\n")
    _review(tmp_path, "web", "ana-dev-70-other", 33, diff="diff other\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def _wait(key):
    for _ in range(200):
        if not presentation.is_building(("review_ticket", key)):
            return
        time.sleep(0.02)


def test_page_renders(client):
    resp = client.get("/review-tickets/DEV-7/presentation")
    assert resp.status_code == 200
    assert "api/review-tickets" in resp.text


def test_detail_lists_only_the_ticket_prs(client):
    with patch("web.reviews.make_ticket_system", return_value=None):
        st = client.get("/api/review-tickets/DEV-7/detail").json()["state"]
    assert [(p["repo"], p["id"]) for p in st["prs"]] == [("api", 11), ("web", 22)]


def test_diff_reads_each_stored_review_diff(client):
    with patch("web.reviews.make_platform", return_value=MagicMock()):
        repos = client.get("/api/review-tickets/DEV-7/diff").json()["repos"]
    assert [r["diff"] for r in repos] == ["diff api\n", "diff web\n"]


def test_unknown_ticket_is_not_found(client):
    assert client.get("/api/review-tickets/DEV-8/presentation").json() == {"status": "not_found"}
    assert client.post("/api/review-tickets/DEV-8/presentation/preprocess").status_code == 404


def test_malformed_key_is_not_found(client):
    assert client.get("/api/review-tickets/..thing/presentation").json() == {"status": "not_found"}
    assert client.post("/api/review-tickets/dev7/presentation/preprocess").status_code == 404


def test_preprocess_builds_one_deck_across_all_prs(client, tmp_path):
    seen = {}

    def fake_build(goal, criteria, diffs, worktree):
        seen["goal"] = goal
        seen["diffs"] = diffs
        return {"steps": [], "requirements": []}

    with patch("features.presentation.build_ticket_presentation", side_effect=fake_build), \
         patch("features.presentation.make_platform", return_value=MagicMock()), \
         patch("features.presentation.ticket_goal_for_key", return_value="DEV-7: goal"):
        assert client.get("/api/review-tickets/DEV-7/presentation").json() == {"status": "missing"}
        assert client.post("/api/review-tickets/DEV-7/presentation/preprocess").json()["status"] == "ok"
        _wait("DEV-7")
    assert seen["goal"] == "DEV-7: goal"
    assert seen["diffs"] == [("api", "api#11", "diff api\n"), ("web", "web#22", "diff web\n")]
    assert (tmp_path / "review_tickets" / "DEV-7" / "ticket_presentation_cache.json").exists()
    data = client.get("/api/review-tickets/DEV-7/presentation").json()
    assert data["status"] == "ok"
    assert data["presentation"] == {"steps": [], "requirements": []}


def test_review_with_no_findings_is_part_of_the_ticket(client, tmp_path):
    d = tmp_path / "reviews" / "cdk" / "ana-dev-7-thing"
    d.mkdir(parents=True)
    (d / "queued_comments.json").write_text("[]")
    (d / "review.json").write_text(json.dumps({"pr_id": 44, "pr_url": "http://pr/cdk/44"}))
    with patch("web.reviews.make_ticket_system", return_value=None):
        st = client.get("/api/review-tickets/DEV-7/detail").json()["state"]
    assert [(p["repo"], p["id"], p["url"]) for p in st["prs"]] == [
        ("api", 11, "http://pr/api/11"), ("cdk", 44, "http://pr/cdk/44"), ("web", 22, "http://pr/web/22")]


def test_a_changed_review_rebuilds_the_cached_deck(client, tmp_path):
    builds = []

    def fake_build(goal, criteria, diffs, worktree):
        builds.append(diffs)
        return {"steps": [], "n": len(builds)}

    with patch("features.presentation.build_ticket_presentation", side_effect=fake_build), \
         patch("features.presentation.make_platform", return_value=MagicMock()), \
         patch("features.presentation.ticket_goal_for_key", return_value=""):
        client.post("/api/review-tickets/DEV-7/presentation/preprocess")
        _wait("DEV-7")
        assert client.get("/api/review-tickets/DEV-7/presentation").json()["presentation"]["n"] == 1
        _wait("DEV-7")
        assert len(builds) == 1
        (tmp_path / "reviews" / "web" / "ana-dev-7-thing" / "diff.txt").write_text("diff web v2\n")
        client.get("/api/review-tickets/DEV-7/presentation")
        _wait("DEV-7")
    assert len(builds) == 2
    assert client.get("/api/review-tickets/DEV-7/presentation").json()["presentation"]["n"] == 2


def test_build_thread_keeps_the_request_context(tmp_path):
    cv = presentation.contextvars.ContextVar("probe", default="primary")
    cv.set("secondary")
    seen = []

    def build():
        seen.append(cv.get())
        return {"steps": []}

    presentation._spawn(("review_ticket", "CTX-1"), tmp_path / "c.json", "", False, build)
    _wait("CTX-1")
    assert seen == ["secondary"]
