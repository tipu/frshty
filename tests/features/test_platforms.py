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


class TestGitHubPushBranch:
    def test_empty_branch_rejects(self):
        config = {"job": {"platform": "github"}, "github": {"repo": "org/r"}, "workspace": {"base_branch": "main"}}
        p = GitHubPlatform(config)
        result = p.push_branch("/tmp", "")
        assert result["ok"] is False


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
