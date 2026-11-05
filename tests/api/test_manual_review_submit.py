"""A manual review runs on either platform and never stays stuck as reviewing.

The review process wrote the GitHub repo cache on every platform, so each
manual submit of a Bitbucket pull request died before review_started. The
crash emitted nothing and left the pending placeholder, so the review page
showed the pull request as reviewing forever.
"""
import json
from unittest.mock import patch

import pytest

import core.log as log
from features.platforms import BitbucketPlatform, GitHubPlatform
from services import review_store
from web import reviews

URL = "https://bitbucket.org/myorg/repo-a/pull-requests/78"


@pytest.fixture()
def bitbucket_config(fake_config, monkeypatch):
    monkeypatch.setenv("BB_USER", "user")
    monkeypatch.setenv("BB_TOKEN", "token")
    fake_config["job"]["platform"] = "bitbucket"
    fake_config["bitbucket"].update(user_env="BB_USER", token_env="BB_TOKEN")
    return fake_config


@pytest.fixture()
def pending_dir(tmp_log):
    path = tmp_log / "reviews" / "repo-a" / "pending-78"
    path.mkdir(parents=True)
    (path / "review.json").write_text(json.dumps({"verdict": "", "status": "reviewing"}))
    return path


def _run(config, state_dir, review_pr=None, load_config=None):
    review = patch("web.reviews.reviewer.review_pr", **(review_pr or {"return_value": {"verdict": "approved", "issues": []}}))
    load = patch("web.reviews.cfg.load_config", **(load_config or {"return_value": config}))
    branch = patch.object(BitbucketPlatform, "get_pr_branch", return_value="feature")
    with review as mock_review, load, branch:
        reviews._run_review("config.toml", state_dir, "test", "myorg/repo-a", 78, URL)
    return mock_review


def _events():
    return {e["event"]: e for e in log.get_events()}


def test_a_bitbucket_submit_reaches_review_started_and_finishes(bitbucket_config, pending_dir, tmp_log):
    mock_review = _run(bitbucket_config, tmp_log)

    mock_review.assert_called_once()
    assert mock_review.call_args.args[2]["branch"] == "feature"
    events = _events()
    assert "review_started" in events
    assert "review_complete" in events
    assert "cycle_error" not in events
    assert not pending_dir.exists()


def test_a_crash_in_the_review_emits_an_error_and_removes_the_placeholder(bitbucket_config, pending_dir, tmp_log):
    _run(bitbucket_config, tmp_log, review_pr={"side_effect": RuntimeError("boom")})

    error = _events()["cycle_error"]
    assert "RuntimeError: boom" in error["summary"]
    assert error["meta"] == {"repo": "repo-a", "pr_id": 78}
    assert not pending_dir.exists()


def test_a_crash_before_the_config_loads_emits_an_error_and_removes_the_placeholder(bitbucket_config, pending_dir, tmp_log):
    _run(bitbucket_config, tmp_log, load_config={"side_effect": ValueError("bad config")})

    assert "ValueError: bad config" in _events()["cycle_error"]["summary"]
    assert not pending_dir.exists()


def test_a_crash_in_the_process_setup_emits_an_error_and_removes_the_placeholder(bitbucket_config, pending_dir, tmp_log):
    with patch("web.reviews.log.init", side_effect=OSError("no logs dir")):
        _run(bitbucket_config, tmp_log)

    assert "OSError: no logs dir" in _events()["cycle_error"]["summary"]
    assert not pending_dir.exists()


def test_a_review_with_no_result_emits_an_error_and_removes_the_placeholder(bitbucket_config, pending_dir, tmp_log):
    _run(bitbucket_config, tmp_log, review_pr={"return_value": None})

    assert "Manual review failed" in _events()["cycle_error"]["summary"]
    assert not pending_dir.exists()


def test_populate_repo_cache_does_not_crash_on_bitbucket(bitbucket_config, tmp_path):
    branch_dir = tmp_path / "reviews" / "repo-a" / "pending-78"
    branch_dir.mkdir(parents=True)
    (branch_dir / "queued_comments.json").write_text(
        json.dumps([{"pr_url": "https://github.com/org/repo-a/pull/78"}]))

    review_store.populate_repo_cache(BitbucketPlatform(bitbucket_config), tmp_path, "repo-a")


def test_github_still_remembers_the_full_repo(fake_config):
    platform = GitHubPlatform(fake_config)

    platform.remember_repo("elsewhere", "other-org/elsewhere")

    assert platform._resolve_repo("elsewhere") == "other-org/elsewhere"
