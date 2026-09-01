from unittest.mock import patch, MagicMock

from features.platforms import make_platform, GitHubPlatform, BitbucketPlatform, _parse_ts


class TestMakePlatform:
    def test_github(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/repo"}, "workspace": {"base_branch": "main"}}
        p = make_platform(config)
        assert isinstance(p, GitHubPlatform)

    def test_bitbucket(self):
        config = {"job": {"platform": "bitbucket"}, "bitbucket": {"org": "myorg"}, "workspace": {"repos": []}}
        with patch("features.platforms.resolve_env", return_value="x"), \
             patch("features.platforms.get_repos", return_value=[]):
            p = make_platform(config)
        assert isinstance(p, BitbucketPlatform)

    def test_unknown_raises(self):
        import pytest
        with pytest.raises(ValueError, match="unknown platform"):
            make_platform({"job": {"platform": "gitlab"}})


class TestParseTs:
    def test_iso_with_z(self):
        dt = _parse_ts("2026-01-01T00:00:00Z")
        assert dt.year == 2026

    def test_iso_with_offset(self):
        dt = _parse_ts("2026-01-01T00:00:00+00:00")
        assert dt.year == 2026


class TestBitbucketNormalizePr:
    def test_normalizes_fields(self):
        config = {"job": {"platform": "bitbucket"}, "bitbucket": {"org": "o"}, "workspace": {"repos": []}}
        with patch("features.platforms.resolve_env", return_value="x"), \
             patch("features.platforms.get_repos", return_value=[]):
            p = BitbucketPlatform(config)

        raw = {
            "id": 42,
            "title": "Fix it",
            "author": {"display_name": "Alice", "account_id": "a1"},
            "source": {"branch": {"name": "fix/it"}},
            "destination": {"branch": {"name": "main"}},
            "created_on": "2026-01-01T00:00:00Z",
            "updated_on": "2026-01-02T00:00:00Z",
            "links": {"html": {"href": "http://bb.com/pr/42"}},
        }
        result = p._normalize_pr(raw, "myrepo")
        assert result["id"] == 42
        assert result["repo"] == "myrepo"
        assert result["branch"] == "fix/it"
        assert result["url"] == "http://bb.com/pr/42"


class TestBitbucketPushBranch:
    def test_empty_branch_rejects(self):
        p = BitbucketPlatform.__new__(BitbucketPlatform)
        result = p.push_branch("/tmp", "")
        assert result["ok"] is False
        assert "empty" in result["error"]

    def test_whitespace_only_rejects(self):
        p = BitbucketPlatform.__new__(BitbucketPlatform)
        result = p.push_branch("/tmp", "   ")
        assert result["ok"] is False


class TestGitHubRepoConfig:
    def test_string_repo_back_compat(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/main-repo"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p.repos == ["org/main-repo"]
        assert p.repo == "org/main-repo"

    def test_list_repo(self):
        config = {"job": {"platform": "github"}, "github": {"repo": ["org/a", "org/b", "org/c"]}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p.repos == ["org/a", "org/b", "org/c"]
        assert p.repo == "org/a"


class TestGitHubListMyOpenPrs:
    def test_iterates_all_repos(self):
        config = {"job": {"platform": "github"}, "github": {"repo": ["org/a", "org/b"]}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)

        def fake_run(args):
            repo = args[args.index("--repo") + 1]
            result = MagicMock()
            result.returncode = 0
            if repo == "org/a":
                result.stdout = '[{"number": 1, "title": "t1", "author": {"login": "me"}, "headRefName": "br1", "baseRefName": "main", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z", "url": "u1", "state": "OPEN"}]'
            else:
                result.stdout = '[{"number": 2, "title": "t2", "author": {"login": "me"}, "headRefName": "br2", "baseRefName": "main", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z", "url": "u2", "state": "OPEN"}]'
            return result

        with patch.object(p, "_run_gh", side_effect=fake_run):
            prs = p.list_my_open_prs()

        assert len(prs) == 2
        assert {pr["id"] for pr in prs} == {1, 2}
        assert {pr["repo"] for pr in prs} == {"a", "b"}

    def test_failure_in_one_repo_does_not_abort(self):
        config = {"job": {"platform": "github"}, "github": {"repo": ["org/a", "org/b"]}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)

        def fake_run(args):
            repo = args[args.index("--repo") + 1]
            result = MagicMock()
            if repo == "org/a":
                result.returncode = 1
                result.stdout = ""
            else:
                result.returncode = 0
                result.stdout = '[{"number": 2, "title": "t2", "author": {"login": "me"}, "headRefName": "br2", "baseRefName": "main", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z", "url": "u2", "state": "OPEN"}]'
            return result

        with patch.object(p, "_run_gh", side_effect=fake_run):
            prs = p.list_my_open_prs()

        assert len(prs) == 1
        assert prs[0]["id"] == 2


class TestGitHubResolveRepo:
    def test_full_name_passthrough(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/main-repo"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._resolve_repo("org/other") == "org/other"

    def test_short_name_prefixed(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/main-repo"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._resolve_repo("short") == "org/short"

    def test_caches_result(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/main-repo"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        p._resolve_repo("cached")
        assert "cached" in p._repo_cache


class TestGitHubEvaluateChecks:
    def test_empty_is_pending(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._evaluate_checks([]) == "pending"

    def test_all_success(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._evaluate_checks([{"state": "SUCCESS"}, {"state": "SUCCESS"}]) == "passed"

    def test_any_failure(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._evaluate_checks([{"state": "SUCCESS"}, {"state": "FAILURE"}]) == "failed"

    def test_pending_mixed(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._evaluate_checks([{"state": "SUCCESS"}, {"state": "PENDING"}]) == "pending"

    def test_neutral_skipped_pass(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._evaluate_checks([{"state": "SUCCESS"}, {"state": "NEUTRAL"}, {"state": "SKIPPED"}]) == "passed"

    def test_cancelled_is_failed(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        assert p._evaluate_checks([{"state": "SUCCESS"}, {"state": "CANCELLED"}]) == "failed"


class TestGitHubPushBranch:
    def test_empty_branch_rejects(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        result = p.push_branch("/tmp", "")
        assert result["ok"] is False


def _gh_platform():
    config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
    return GitHubPlatform(config)


def _gh_result(stdout="", stderr="", returncode=0):
    r = MagicMock()
    r.stdout = stdout
    r.stderr = stderr
    r.returncode = returncode
    return r


class TestGitHubGetPrChecks:
    def test_valid_json_returns_list_even_on_failing_checks(self):
        p = _gh_platform()
        out = '[{"name": "build", "state": "FAILURE", "link": "u"}]'
        with patch.object(p, "_run_gh", return_value=_gh_result(stdout=out, returncode=0)):
            checks = p.get_pr_checks("repo", 1)
        assert checks == [{"name": "build", "state": "FAILURE", "url": "u"}]

    def test_no_checks_reported_is_empty_list(self):
        p = _gh_platform()
        with patch.object(p, "_run_gh", return_value=_gh_result(
                stderr="no checks reported on the 'feat/x' branch", returncode=1)):
            assert p.get_pr_checks("repo", 1) == []

    def test_fetch_error_is_none_not_empty(self):
        p = _gh_platform()
        with patch.object(p, "_run_gh", return_value=_gh_result(
                stderr="GraphQL: Could not resolve to a PullRequest", returncode=1)):
            assert p.get_pr_checks("repo", 1) is None

    def test_unparseable_stdout_is_none(self):
        p = _gh_platform()
        with patch.object(p, "_run_gh", return_value=_gh_result(stdout="not json", returncode=0)):
            assert p.get_pr_checks("repo", 1) is None

    def test_monitor_ci_does_not_pass_on_fetch_failure(self):
        p = _gh_platform()
        ts = {"prs": [{"repo": "r", "id": 1, "url": "u"}], "status": "in_review"}
        with patch.object(p, "get_pr_checks", return_value=None), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "T-1"}, ts, "http://base")
        assert result.get("ci_passed") is not True


class TestBitbucketChecksNormalization:
    def test_successful_becomes_success(self):
        config = {"job": {"platform": "bitbucket"}, "bitbucket": {"org": "o"}, "workspace": {"repos": []}}
        with patch("features.platforms.resolve_env", return_value="x"), \
             patch("features.platforms.get_repos", return_value=[]):
            p = BitbucketPlatform(config)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"values": [{"name": "build", "state": "SUCCESSFUL", "url": "http://ci"}]}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        with patch("features.platforms.httpx.Client", return_value=mock_client):
            checks = p.get_pr_checks("repo", 1)
        assert checks[0]["state"] == "SUCCESS"


def _bb_platform():
    config = {"job": {"platform": "bitbucket"}, "bitbucket": {"org": "o"}, "workspace": {"repos": []}}
    with patch("features.platforms.resolve_env", return_value="x"), \
         patch("features.platforms.get_repos", return_value=[]):
        return BitbucketPlatform(config)


def _bb_get(json_value):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = json_value
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get.return_value = mock_resp
    return mock_client


_THREADS_RESPONSE = {
    "data": {"repository": {"pullRequest": {"reviews": {"nodes": []}, "reviewThreads": {"nodes": [
        {"id": "T_unresolved", "isResolved": False, "comments": {"nodes": [
            {"databaseId": 11, "body": "fix this", "path": "a.py", "line": 5,
             "originalLine": 5, "diffHunk": "@@", "url": "http://c/11",
             "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z",
             "author": {"login": "alice"}, "replyTo": None},
        ]}},
        {"id": "T_resolved", "isResolved": True, "comments": {"nodes": [
            {"databaseId": 22, "body": "done", "path": "b.py", "line": None,
             "originalLine": 9, "diffHunk": "@@", "url": "http://c/22",
             "createdAt": "2026-01-02T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z",
             "author": {"login": "bob"}, "replyTo": {"databaseId": 11}},
        ]}},
    ]}}}}
}


class TestGitHubGetPrComments:
    def test_flattens_threads_with_resolution(self):
        p = _gh_platform()
        import json as _json
        with patch.object(p, "_run_gh", return_value=_gh_result(stdout=_json.dumps(_THREADS_RESPONSE))):
            comments = p.get_pr_comments("r", 1)
        assert [c["id"] for c in comments] == [11, 22]
        first, second = comments
        assert first["resolved"] is False and first["thread_id"] == "T_unresolved"
        assert first["author_id"] == "alice" and first["diff_hunk"] == "@@"
        assert second["resolved"] is True and second["thread_id"] == "T_resolved"
        assert second["line"] == 9
        assert second["parent_id"] == 11

    def test_includes_nonempty_general_review_bodies(self):
        p = _gh_platform()
        import copy
        import json as _json
        response = copy.deepcopy(_THREADS_RESPONSE)
        response["data"]["repository"]["pullRequest"]["reviews"]["nodes"] = [
            {"databaseId": 33, "body": "Please add a fingerprint", "url": "http://r/33",
             "submittedAt": "2026-01-03T00:00:00Z", "updatedAt": "2026-01-03T00:00:00Z",
             "state": "COMMENTED", "author": {"login": "carol"}},
            {"databaseId": 44, "body": "", "url": "http://r/44",
             "submittedAt": "2026-01-04T00:00:00Z", "updatedAt": "2026-01-04T00:00:00Z",
             "state": "COMMENTED", "author": {"login": "dave"}},
        ]
        with patch.object(p, "_run_gh", return_value=_gh_result(stdout=_json.dumps(response))):
            comments = p.get_pr_comments("r", 1)
        review = next(c for c in comments if c["id"] == 33)
        assert review["comment_kind"] == "review_body"
        assert review["resolvable"] is False
        assert review["path"] is None and review["author_id"] == "carol"
        assert not any(c["id"] == 44 for c in comments)

    def test_fetch_failure_is_empty(self):
        p = _gh_platform()
        with patch.object(p, "_run_gh", return_value=_gh_result(stderr="boom", returncode=1)):
            assert p.get_pr_comments("r", 1) == []


class TestGitHubResolveComment:
    def test_resolves_owning_thread(self):
        p = _gh_platform()
        import json as _json
        calls = []

        def fake_run(args):
            calls.append(args)
            if "mutation" in args[3]:
                return _gh_result(returncode=0)
            return _gh_result(stdout=_json.dumps(_THREADS_RESPONSE))

        with patch.object(p, "_run_gh", side_effect=fake_run):
            result = p.resolve_comment("r", 1, 22)
        assert result == {"status": "resolved"}
        assert any("T_resolved" in a for call in calls for a in call)

    def test_unknown_comment_errors_without_mutation(self):
        p = _gh_platform()
        import json as _json
        with patch.object(p, "_run_gh", return_value=_gh_result(stdout=_json.dumps(_THREADS_RESPONSE))) as run:
            result = p.resolve_comment("r", 1, 999)
        assert result["status"] == "error"
        assert run.call_count == 1


class TestBitbucketGetPrComments:
    def test_maps_resolution_to_resolved_flag(self):
        p = _bb_platform()
        values = {"values": [
            {"id": 1, "content": {"raw": "open"}, "user": {"account_id": "u1", "display_name": "U1"},
             "created_on": "2026-01-01T00:00:00Z"},
            {"id": 2, "content": {"raw": "closed"}, "user": {"account_id": "u2", "display_name": "U2"},
             "created_on": "2026-01-01T00:00:00Z",
             "resolution": {"type": "pullrequest_comment_resolution"}},
        ]}
        with patch("features.platforms.httpx.Client", return_value=_bb_get(values)):
            comments = p.get_pr_comments("repo", 1)
        assert comments[0]["resolved"] is False
        assert comments[1]["resolved"] is True

    def test_empty_resolution_dict_and_reply_inherit_root(self):
        p = _bb_platform()
        values = {"values": [
            {"id": 10, "content": {"raw": "root resolved"}, "user": {"account_id": "u1", "display_name": "U1"},
             "created_on": "2026-01-01T00:00:00Z", "resolution": {}},
            {"id": 11, "content": {"raw": "reply"}, "user": {"account_id": "u2", "display_name": "U2"},
             "created_on": "2026-01-01T00:00:00Z", "parent": {"id": 10}},
            {"id": 20, "content": {"raw": "open root"}, "user": {"account_id": "u1", "display_name": "U1"},
             "created_on": "2026-01-01T00:00:00Z", "resolution": None},
            {"id": 21, "content": {"raw": "open reply"}, "user": {"account_id": "u2", "display_name": "U2"},
             "created_on": "2026-01-01T00:00:00Z", "parent": {"id": 20}},
        ]}
        with patch("features.platforms.httpx.Client", return_value=_bb_get(values)):
            by_id = {c["id"]: c for c in p.get_pr_comments("repo", 1)}
        assert by_id[10]["resolved"] is True
        assert by_id[11]["resolved"] is True
        assert by_id[20]["resolved"] is False
        assert by_id[21]["resolved"] is False


class TestBitbucketMonitorCI:
    """BitbucketPlatform.monitor_ci was a stub that unconditionally set
    ci_passed=True, so red Bitbucket pipelines never triggered
    fix_ci_failures (observed on the four red DEV-635 PRs, 2026-08-21).
    Both platforms now share _CIMonitorMixin."""

    def _ts(self):
        return {"prs": [{"repo": "r", "id": 1, "url": "u"}], "status": "in_review"}

    def test_failed_check_returns_ci_failed(self):
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "FAILED", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, self._ts(), "http://base")
        assert result.get("_ci_failed") is True
        assert result.get("ci_passed") is not True

    def test_stopped_check_returns_ci_failed(self):
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "STOPPED", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, self._ts(), "http://base")
        assert result.get("_ci_failed") is True

    def test_all_success_sets_ci_passed(self):
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "SUCCESS", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, self._ts(), "http://base")
        assert result.get("ci_passed") is True

    def test_inprogress_is_pending_not_passed(self):
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "INPROGRESS", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, self._ts(), "http://base")
        assert result.get("ci_passed") is not True
        assert result.get("_ci_failed") is not True

    def test_fetch_failure_holds(self):
        p = _bb_platform()
        with patch.object(p, "get_pr_checks", return_value=None), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, self._ts(), "http://base")
        assert result.get("ci_passed") is not True
        assert result.get("_ci_failed") is not True


class TestBitbucketGetFailedLogs:
    def test_fetches_failed_step_log(self):
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "FAILED",
                   "url": "https://bitbucket.org/o/repo/addon/pipelines/home#!/results/42"}]
        steps_resp = MagicMock()
        steps_resp.status_code = 200
        steps_resp.json.return_value = {"values": [
            {"uuid": "{s1}", "name": "Ruff",
             "state": {"name": "COMPLETED", "result": {"name": "FAILED"}}},
            {"uuid": "{s2}", "name": "Tests",
             "state": {"name": "COMPLETED", "result": {"name": "SUCCESSFUL"}}},
        ]}
        log_resp = MagicMock()
        log_resp.status_code = 200
        log_resp.text = "E501 line too long"
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = [steps_resp, log_resp]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.external_log.client", return_value=mock_client):
            logs = p.get_failed_logs("repo", 1)
        assert "E501 line too long" in logs
        assert "Ruff" in logs
        urls = [c.args[0] for c in mock_client.get.call_args_list]
        assert urls[0].endswith("/repositories/o/repo/pipelines/42/steps/")
        assert urls[1].endswith("/repositories/o/repo/pipelines/42/steps/{s1}/log")

    def test_no_pipeline_url_returns_empty(self):
        p = _bb_platform()
        checks = [{"name": "External CI", "state": "FAILED", "url": "https://elsewhere.example/run/9"}]
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.external_log.client", return_value=mock_client):
            assert p.get_failed_logs("repo", 1) == ""
        assert mock_client.get.call_count == 0
