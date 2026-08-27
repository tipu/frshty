import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core import runtime
from core.git_util import (configure_repo_identity, effective_identity,
                           git_common_dir)

ROOT_MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"

WANT_NAME = "work-account"
WANT_EMAIL = "me@work.example"
WANT = f"{WANT_NAME} <{WANT_EMAIL}>"


def _repo(path):
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.name", "personal"], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.email", "me@home.example"], check=True)
    (path / "f.txt").write_text("x")
    subprocess.run(["git", "-C", str(path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-qm", "init"], check=True)
    return path


class TestConfigureRepoIdentity:
    def test_sets_and_verifies(self, tmp_path):
        r = _repo(tmp_path / "r")
        ok, detail = configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        assert ok, detail
        assert detail == WANT
        assert effective_identity(r) == (WANT, WANT)

    def test_replaces_a_conflicting_local_identity(self, tmp_path):
        r = _repo(tmp_path / "r")
        assert effective_identity(r)[0] == "personal <me@home.example>"
        assert configure_repo_identity(r, WANT_NAME, WANT_EMAIL)[0]
        assert effective_identity(r)[0] == WANT

    def test_a_real_commit_carries_the_identity(self, tmp_path):
        r = _repo(tmp_path / "r")
        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        (r / "g.txt").write_text("y")
        subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(r), "commit", "-qm", "second"], check=True)
        out = subprocess.run(["git", "-C", str(r), "log", "-1", "--format=%an <%ae>|%cn <%ce>"],
                             capture_output=True, text=True, check=True).stdout.strip()
        assert out == f"{WANT}|{WANT}"

    def test_ambient_env_that_outranks_the_config_is_reported(self, tmp_path, monkeypatch):
        r = _repo(tmp_path / "r")
        monkeypatch.setenv("GIT_AUTHOR_NAME", "someone-else")
        monkeypatch.setenv("GIT_AUTHOR_EMAIL", "else@example.com")
        ok, detail = configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        assert not ok
        assert "outranks" in detail

    def test_linked_worktree_inherits_it(self, tmp_path):
        r = _repo(tmp_path / "r")
        wt = tmp_path / "wt"
        subprocess.run(["git", "-C", str(r), "worktree", "add", "-q", "-b", "t", str(wt)], check=True)
        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        assert effective_identity(wt) == (WANT, WANT)
        assert git_common_dir(wt) == git_common_dir(r)


def _cfg(key, root, repos, **git):
    c = {"job": {"key": key}, "workspace": {"root": root, "repos": repos}}
    if git:
        c["git"] = git
    return c


class TestEnforceGitIdentity:
    def test_instance_without_a_git_block_is_never_gated(self, tmp_path):
        _repo(tmp_path / "r")
        cfg = _cfg("plain", tmp_path, ["r"])
        assert runtime._enforce_git_identity([cfg]) == set()
        assert effective_identity(tmp_path / "r")[0] == "personal <me@home.example>"

    def test_configured_instance_is_applied_to_every_repo(self, tmp_path):
        _repo(tmp_path / "a")
        _repo(tmp_path / "b")
        cfg = _cfg("work", tmp_path, ["a", "b"], name=WANT_NAME, email=WANT_EMAIL)
        assert runtime._enforce_git_identity([cfg]) == set()
        assert effective_identity(tmp_path / "a") == (WANT, WANT)
        assert effective_identity(tmp_path / "b") == (WANT, WANT)

    def test_unverifiable_identity_fails_the_instance(self, tmp_path, monkeypatch):
        _repo(tmp_path / "r")
        monkeypatch.setenv("GIT_COMMITTER_NAME", "someone-else")
        monkeypatch.setenv("GIT_COMMITTER_EMAIL", "else@example.com")
        cfg = _cfg("work", tmp_path, ["r"], name=WANT_NAME, email=WANT_EMAIL)
        assert runtime._enforce_git_identity([cfg]) == {"work"}

    def test_missing_repo_fails_the_instance(self, tmp_path):
        cfg = _cfg("work", tmp_path, ["absent"], name=WANT_NAME, email=WANT_EMAIL)
        assert runtime._enforce_git_identity([cfg]) == {"work"}

    def test_two_instances_claiming_one_clone_both_fail(self, tmp_path):
        _repo(tmp_path / "shared")
        one = _cfg("one", tmp_path, ["shared"], name=WANT_NAME, email=WANT_EMAIL)
        two = _cfg("two", tmp_path, ["shared"], name="other", email="other@example.com")
        assert runtime._enforce_git_identity([one, two]) == {"one", "two"}

    def test_two_instances_agreeing_on_one_clone_are_fine(self, tmp_path):
        _repo(tmp_path / "shared")
        one = _cfg("one", tmp_path, ["shared"], name=WANT_NAME, email=WANT_EMAIL)
        two = _cfg("two", tmp_path, ["shared"], name=WANT_NAME, email=WANT_EMAIL)
        assert runtime._enforce_git_identity([one, two]) == set()


class TestEnforceRejects:
    def test_partial_git_block_fails_rather_than_disabling_the_gate(self, tmp_path):
        _repo(tmp_path / "r")
        cfg = _cfg("work", tmp_path, ["r"], name=WANT_NAME)
        assert runtime._enforce_git_identity([cfg]) == {"work"}

    def test_empty_email_fails(self, tmp_path):
        _repo(tmp_path / "r")
        cfg = _cfg("work", tmp_path, ["r"], name=WANT_NAME, email="")
        assert runtime._enforce_git_identity([cfg]) == {"work"}

    def test_agent_env_that_would_override_the_identity_fails(self, tmp_path):
        _repo(tmp_path / "r")
        cfg = _cfg("work", tmp_path, ["r"], name=WANT_NAME, email=WANT_EMAIL)
        cfg["llm"] = {"claude": {"env": {"GIT_AUTHOR_EMAIL": "else@example.com"}}}
        assert runtime._enforce_git_identity([cfg]) == {"work"}

    def test_unrelated_agent_env_is_allowed(self, tmp_path):
        _repo(tmp_path / "r")
        cfg = _cfg("work", tmp_path, ["r"], name=WANT_NAME, email=WANT_EMAIL)
        cfg["llm"] = {"claude": {"env": {"CLAUDE_CONFIG_DIR": "/tmp/x"}}}
        assert runtime._enforce_git_identity([cfg]) == set()

    def test_collision_leaves_no_repo_half_applied(self, tmp_path):
        _repo(tmp_path / "shared")
        one = _cfg("one", tmp_path, ["shared"], name=WANT_NAME, email=WANT_EMAIL)
        two = _cfg("two", tmp_path, ["shared"], name="other", email="other@example.com")
        assert runtime._enforce_git_identity([one, two]) == {"one", "two"}
        assert effective_identity(tmp_path / "shared")[0] == "personal <me@home.example>"

    def test_worktree_local_override_fails_the_instance(self, tmp_path):
        r = _repo(tmp_path / "r")
        wt = tmp_path / "wt"
        subprocess.run(["git", "-C", str(r), "worktree", "add", "-q", "-b", "t", str(wt)], check=True)
        subprocess.run(["git", "-C", str(r), "config", "extensions.worktreeConfig", "true"], check=True)
        subprocess.run(["git", "-C", str(wt), "config", "--worktree", "user.email",
                        "sneaky@example.com"], check=True)
        cfg = _cfg("work", tmp_path, ["r"], name=WANT_NAME, email=WANT_EMAIL)
        assert runtime._enforce_git_identity([cfg]) == {"work"}


class TestPrePushGuard:
    def _remote_backed(self, tmp_path):
        origin = tmp_path / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(origin)], check=True)
        r = _repo(tmp_path / "r")
        subprocess.run(["git", "-C", str(r), "remote", "add", "origin", str(origin)], check=True)
        subprocess.run(["git", "-C", str(r), "push", "-q", "origin", "main"], check=True)
        subprocess.run(["git", "-C", str(r), "checkout", "-qb", "feature"], check=True)
        return r

    def _commit(self, r, text):
        (r / f"{text}.txt").write_text(text)
        subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(r), "commit", "-qm", text], check=True)

    def _cfg(self, **git):
        c = {"job": {"key": "w"}, "workspace": {"base_branch": "main"}}
        if git:
            c["git"] = git
        return c

    def test_no_git_block_never_blocks(self, tmp_path):
        from features.platforms import _identity_block_reason
        r = self._remote_backed(tmp_path)
        self._commit(r, "a")
        assert _identity_block_reason(self._cfg(), r, "feature") == ""

    def test_matching_commits_pass(self, tmp_path):
        from features.platforms import _identity_block_reason
        r = self._remote_backed(tmp_path)
        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        self._commit(r, "a")
        assert _identity_block_reason(self._cfg(name=WANT_NAME, email=WANT_EMAIL),
                                      r, "feature") == ""

    def test_a_wrongly_authored_commit_blocks_the_push(self, tmp_path):
        from features.platforms import _identity_block_reason
        r = self._remote_backed(tmp_path)
        self._commit(r, "a")  # still the personal identity
        reason = _identity_block_reason(self._cfg(name=WANT_NAME, email=WANT_EMAIL),
                                        r, "feature")
        assert "not authored by" in reason
        assert "me@home.example" in reason

    def test_base_history_by_other_people_is_ignored(self, tmp_path):
        from features.platforms import _identity_block_reason
        r = self._remote_backed(tmp_path)  # 'init' commit is the personal identity
        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        self._commit(r, "a")
        assert _identity_block_reason(self._cfg(name=WANT_NAME, email=WANT_EMAIL),
                                      r, "feature") == ""


    def test_history_already_on_the_remote_branch_is_ignored(self, tmp_path):
        """A push publishes origin/<branch>..HEAD, not the whole branch.

        A long-lived PR branch carries CI-bot commits and commits the same
        person made under another git identity. Those are already on the
        remote, so pushing changes nothing about them, but judging the whole
        branch against the base blocks every later push — the agent can never
        answer a review comment on a branch that has ever merged."""
        from features.platforms import _identity_block_reason
        r = self._remote_backed(tmp_path)
        self._commit(r, "bot-commit")  # the personal identity, i.e. not ours
        subprocess.run(["git", "-C", str(r), "push", "-q", "origin", "feature"], check=True)

        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        self._commit(r, "our-fix")
        assert _identity_block_reason(self._cfg(name=WANT_NAME, email=WANT_EMAIL),
                                      r, "feature") == ""

    def test_a_wrong_author_not_yet_on_the_remote_still_blocks(self, tmp_path):
        from features.platforms import _identity_block_reason
        r = self._remote_backed(tmp_path)
        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        self._commit(r, "ours")
        subprocess.run(["git", "-C", str(r), "push", "-q", "origin", "feature"], check=True)

        configure_repo_identity(r, "personal", "me@home.example")
        self._commit(r, "theirs")
        reason = _identity_block_reason(self._cfg(name=WANT_NAME, email=WANT_EMAIL),
                                        r, "feature")
        assert "not authored by" in reason
        assert "me@home.example" in reason


class TestCollisionGrouping:
    def test_three_claimants_two_identities_all_fail(self, tmp_path):
        _repo(tmp_path / "shared")
        a = _cfg("a", tmp_path, ["shared"], name=WANT_NAME, email=WANT_EMAIL)
        b = _cfg("b", tmp_path, ["shared"], name=WANT_NAME, email=WANT_EMAIL)
        c = _cfg("c", tmp_path, ["shared"], name="other", email="other@example.com")
        assert runtime._enforce_git_identity([a, b, c]) == {"a", "b", "c"}
        assert effective_identity(tmp_path / "shared")[0] == "personal <me@home.example>"

    def test_three_claimants_one_identity_all_pass(self, tmp_path):
        _repo(tmp_path / "shared")
        cfgs = [_cfg(k, tmp_path, ["shared"], name=WANT_NAME, email=WANT_EMAIL)
                for k in ("a", "b", "c")]
        assert runtime._enforce_git_identity(cfgs) == set()
        assert effective_identity(tmp_path / "shared")[0] == WANT


class TestPushGuardFailsClosed:
    def test_unreadable_range_blocks_the_push(self, tmp_path):
        from features.platforms import _identity_block_reason
        r = _repo(tmp_path / "r")  # no origin remote at all
        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        reason = _identity_block_reason(
            {"job": {"key": "w"}, "workspace": {"base_branch": "main"},
             "git": {"name": WANT_NAME, "email": WANT_EMAIL}}, r, "feature")
        assert "authorship of this push is unknown" in reason

    def test_repo_name_comes_from_the_git_dir_not_the_folder_name(self, tmp_path):
        from features.platforms import _identity_block_reason
        origin = tmp_path / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "trunk", str(origin)], check=True)
        r = _repo(tmp_path / "realrepo")
        subprocess.run(["git", "-C", str(r), "branch", "-M", "trunk"], check=True)
        subprocess.run(["git", "-C", str(r), "remote", "add", "origin", str(origin)], check=True)
        subprocess.run(["git", "-C", str(r), "push", "-q", "origin", "trunk"], check=True)
        configure_repo_identity(r, WANT_NAME, WANT_EMAIL)
        # A PR worktree is named after the branch slug, not the repo.
        wt = tmp_path / "pr-1234-some-branch"
        subprocess.run(["git", "-C", str(r), "worktree", "add", "-q", "-b", "f", str(wt)], check=True)
        config = {"job": {"key": "w"},
                  "workspace": {"base_branch": "main",
                                "base_branches": {"realrepo": "trunk"}},
                  "git": {"name": WANT_NAME, "email": WANT_EMAIL}}
        # Resolves through the git common dir, so the per-repo override applies
        # and origin/trunk is found instead of a missing origin/main.
        assert _identity_block_reason(config, wt, "f") == ""


class TestSchedulerCleanup:
    def test_gated_instance_loses_its_scheduled_rows(self, tmp_path):
        import core.db as db
        import core.scheduler as scheduler
        db.init(tmp_path / "s.db", ROOT_MIGRATIONS)
        when = datetime(2030, 1, 1, tzinfo=timezone.utc)
        scheduler.upsert_recurring("ghost", "billing_check", "billing_check",
                                   cadence="weekly", next_run_at=when)
        scheduler.upsert_recurring("alive", "billing_check", "billing_check",
                                   cadence="weekly", next_run_at=when)
        assert scheduler.delete_instance("ghost") == 1
        assert scheduler.delete_instance("ghost") == 0
        rows = db.query_all("SELECT instance_key FROM scheduler")
        assert [r["instance_key"] for r in rows] == ["alive"]
