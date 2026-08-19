from unittest.mock import patch, MagicMock

from core import preflight


def _cfg(tmp_path, **overrides):
    workspace = tmp_path / "ws"
    repos_dir = workspace
    (repos_dir / "myrepo" / ".git").mkdir(parents=True)
    cfg = {
        "job": {"key": "test", "platform": "github"},
        "github": {"repo": "alice/myrepo"},
        "workspace": {"root": str(workspace), "repos": ["myrepo"]},
    }
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k].update(v)
        else:
            cfg[k] = v
    return cfg


class TestPreflightInstance:
    def test_all_pass(self, tmp_path):
        cfg = _cfg(tmp_path)
        with patch.object(preflight, "_run") as run:
            run.return_value = MagicMock(returncode=0, stdout="true\n", stderr="")
            ok, checks = preflight.preflight_instance(cfg)
        assert ok
        assert any(c["name"] == "github.repo[alice/myrepo]" and c["ok"] for c in checks)
        assert any(c["name"] == "workspace.root" and c["ok"] for c in checks)

    def test_workspace_root_missing(self, tmp_path):
        cfg = _cfg(tmp_path)
        cfg["workspace"]["root"] = str(tmp_path / "does-not-exist")
        with patch.object(preflight, "_run") as run:
            run.return_value = MagicMock(returncode=0, stdout="true\n", stderr="")
            ok, checks = preflight.preflight_instance(cfg)
        assert not ok
        failures = [c for c in checks if not c["ok"]]
        assert any("workspace.root" in c["name"] for c in failures)

    def test_repo_404_emits_active_account_diagnostic(self, tmp_path):
        cfg = _cfg(tmp_path)
        def fake_run(cmd, timeout=15, env=None):
            if cmd[:2] == ["gh", "api"] and "repos/" in cmd[2]:
                return MagicMock(returncode=1, stdout="",
                                 stderr="GraphQL: Could not resolve to a Repository (Not Found)")
            if cmd[:2] == ["gh", "api"] and cmd[2] == "user":
                return MagicMock(returncode=0, stdout="bob\n", stderr="")
            return MagicMock(returncode=0, stdout="", stderr="")
        with patch.object(preflight, "_run", side_effect=fake_run):
            ok, checks = preflight.preflight_instance(cfg)
        assert not ok
        active_check = next(c for c in checks if c["name"] == "gh account[alice/myrepo]")
        assert "bob" in active_check["detail"]
        assert "alice/myrepo" in active_check["detail"]

    def test_repo_list_checks_every_repo(self, tmp_path):
        cfg = _cfg(tmp_path, github={"repo": ["alice/one", "alice/two"]})
        def fake_run(cmd, timeout=15, env=None):
            if cmd[:2] == ["gh", "api"] and cmd[2] == "repos/alice/two":
                return MagicMock(returncode=1, stdout="", stderr="Not Found")
            if cmd[:2] == ["gh", "api"] and cmd[2] == "user":
                return MagicMock(returncode=0, stdout="bob\n", stderr="")
            return MagicMock(returncode=0, stdout="true\n", stderr="")
        with patch.object(preflight, "_run", side_effect=fake_run):
            ok, checks = preflight.preflight_instance(cfg)
        assert not ok
        assert any(c["name"] == "github.repo[alice/one]" and c["ok"] for c in checks)
        assert any(c["name"] == "github.repo[alice/two]" and not c["ok"] for c in checks)

    def test_named_account_checks_with_its_own_token(self, tmp_path):
        cfg = _cfg(tmp_path, github={"repo": "alice/myrepo", "account": "carol"})
        seen = {}
        def fake_run(cmd, timeout=15, env=None):
            if cmd[:3] == ["gh", "auth", "token"]:
                return MagicMock(returncode=0, stdout="gho_carol\n", stderr="")
            if cmd[:2] == ["gh", "api"] and "repos/" in cmd[2]:
                seen["token"] = (env or {}).get("GH_TOKEN")
                return MagicMock(returncode=0, stdout="true\n", stderr="")
            return MagicMock(returncode=0, stdout="", stderr="")
        with patch.object(preflight, "_run", side_effect=fake_run):
            ok, checks = preflight.preflight_instance(cfg)
        assert ok
        assert seen["token"] == "gho_carol"

    def test_named_account_without_token_fails(self, tmp_path):
        cfg = _cfg(tmp_path, github={"repo": "alice/myrepo", "account": "carol"})
        def fake_run(cmd, timeout=15, env=None):
            if cmd[:3] == ["gh", "auth", "token"]:
                return MagicMock(returncode=1, stdout="", stderr="no account")
            return MagicMock(returncode=0, stdout="true\n", stderr="")
        with patch.object(preflight, "_run", side_effect=fake_run):
            ok, checks = preflight.preflight_instance(cfg)
        assert not ok
        assert any("no token for account" in c["detail"] for c in checks if not c["ok"])


class TestGhHelpers:
    def test_active_account(self):
        with patch.object(preflight, "_run",
                          return_value=MagicMock(returncode=0, stdout="alice\n")):
            assert preflight.gh_active_account() == "alice"

    def test_active_account_none_on_failure(self):
        with patch.object(preflight, "_run",
                          return_value=MagicMock(returncode=1, stdout="", stderr="not authed")):
            assert preflight.gh_active_account() is None

    def test_logged_in_accounts_parses_status(self):
        status = (
            "github.com\n"
            "  ✓ Logged in to github.com account alice (keyring)\n"
            "  - Active account: true\n"
            "\n"
            "  ✓ Logged in to github.com account bob (/path/to/hosts.yml)\n"
            "  - Active account: false\n"
        )
        with patch.object(preflight, "_run",
                          return_value=MagicMock(returncode=0, stdout=status, stderr="")):
            assert preflight.gh_logged_in_accounts() == ["alice", "bob"]

    def test_repo_push_ok_true(self):
        with patch.object(preflight, "_run",
                          return_value=MagicMock(returncode=0, stdout="true\n")):
            ok, reason = preflight.gh_repo_push_ok("alice/myrepo")
            assert ok and reason == "ok"

    def test_repo_push_ok_404(self):
        with patch.object(preflight, "_run",
                          return_value=MagicMock(returncode=1, stdout="",
                                                 stderr="HTTP 404: Not Found")):
            ok, reason = preflight.gh_repo_push_ok("alice/myrepo")
            assert not ok and "404" in reason

    def test_repo_push_ok_read_only(self):
        with patch.object(preflight, "_run",
                          return_value=MagicMock(returncode=0, stdout="false\n")):
            ok, reason = preflight.gh_repo_push_ok("alice/myrepo")
            assert not ok and "no push permission" in reason
