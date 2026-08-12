"""Tests for core.deps — shared pipenv venv store and pnpm store wiring."""
import os
import threading
import time
from unittest.mock import patch

from core import deps


STUB_BUILD = (
    "python3 -c \"import os, pathlib, sys; "
    "v = pathlib.Path(os.environ['WORKON_HOME']) / os.environ['PIPENV_CUSTOM_VENV_NAME']; "
    "(v / 'bin').mkdir(parents=True, exist_ok=True); "
    "(v / 'bin' / 'python').touch(); "
    "log = pathlib.Path(os.environ['BUILD_LOG']); "
    "f = open(log, 'a'); f.write('build\\n'); f.close()\""
)

STUB_FAIL = "python3 -c \"import sys; sys.exit(1)\""

STUB_SLOW_BUILD = (
    "python3 -c \"import os, pathlib, time; "
    "log = pathlib.Path(os.environ['BUILD_LOG']); "
    "f = open(log, 'a'); f.write('build\\n'); f.close(); "
    "time.sleep(0.3); "
    "v = pathlib.Path(os.environ['WORKON_HOME']) / os.environ['PIPENV_CUSTOM_VENV_NAME']; "
    "(v / 'bin').mkdir(parents=True, exist_ok=True); "
    "(v / 'bin' / 'python').touch()\""
)


def _make_worktree(root, name, lock_content='{"default": {}}'):
    wt = root / name
    wt.mkdir(parents=True)
    (wt / "Pipfile").write_text("[packages]\n")
    (wt / "Pipfile.lock").write_text(lock_content)
    return wt


class TestSharedVenvName:
    def test_stable_for_same_lock(self, tmp_path):
        wt_a = _make_worktree(tmp_path, "a")
        wt_b = _make_worktree(tmp_path, "b")
        assert deps.shared_venv_name("repo", wt_a) is not None, \
            "fixture worktree must produce a venv name"
        assert deps.shared_venv_name("repo", wt_a) == deps.shared_venv_name("repo", wt_b)

    def test_changes_with_lock_content(self, tmp_path):
        wt_a = _make_worktree(tmp_path, "a", '{"default": {"x": "1"}}')
        wt_b = _make_worktree(tmp_path, "b", '{"default": {"x": "2"}}')
        assert deps.shared_venv_name("repo", wt_a) != deps.shared_venv_name("repo", wt_b)

    def test_falls_back_to_pipfile(self, tmp_path):
        wt = tmp_path / "wt"
        wt.mkdir()
        (wt / "Pipfile").write_text("[packages]\n")
        name = deps.shared_venv_name("repo", wt)
        assert name is not None and name.startswith("repo-")

    def test_none_without_pipfile(self, tmp_path):
        wt = tmp_path / "wt"
        wt.mkdir()
        assert deps.shared_venv_name("repo", wt) is None


class TestEnsureSharedVenv:
    def test_builds_once_and_links_both_worktrees(self, fake_config, tmp_path, monkeypatch):
        build_log = tmp_path / "builds.log"
        monkeypatch.setenv("BUILD_LOG", str(build_log))
        wt_a = _make_worktree(tmp_path, "a")
        wt_b = _make_worktree(tmp_path, "b")

        assert deps.ensure_shared_venv(fake_config, "repo", wt_a, dep_cmd=STUB_BUILD)
        assert deps.ensure_shared_venv(fake_config, "repo", wt_b, dep_cmd=STUB_BUILD)

        assert build_log.read_text().count("build") == 1, \
            "second worktree with identical lock must reuse the venv, not rebuild"
        name = deps.shared_venv_name("repo", wt_a)
        target = deps.venv_store_dir(fake_config) / name
        assert (wt_a / ".venv").readlink() == target
        assert (wt_b / ".venv").readlink() == target

    def test_different_lock_builds_second_venv(self, fake_config, tmp_path, monkeypatch):
        build_log = tmp_path / "builds.log"
        monkeypatch.setenv("BUILD_LOG", str(build_log))
        wt_a = _make_worktree(tmp_path, "a", '{"default": {"x": "1"}}')
        wt_b = _make_worktree(tmp_path, "b", '{"default": {"x": "2"}}')

        assert deps.ensure_shared_venv(fake_config, "repo", wt_a, dep_cmd=STUB_BUILD)
        assert deps.ensure_shared_venv(fake_config, "repo", wt_b, dep_cmd=STUB_BUILD)

        assert build_log.read_text().count("build") == 2
        assert (wt_a / ".venv").readlink() != (wt_b / ".venv").readlink()

    def test_failed_build_leaves_no_venv(self, fake_config, tmp_path):
        wt = _make_worktree(tmp_path, "a")
        assert deps.ensure_shared_venv(fake_config, "repo", wt, dep_cmd=STUB_FAIL) is False
        name = deps.shared_venv_name("repo", wt)
        assert not (deps.venv_store_dir(fake_config) / name).exists()
        assert not (wt / ".venv").exists()

    def test_replaces_stale_symlink_on_lock_change(self, fake_config, tmp_path, monkeypatch):
        build_log = tmp_path / "builds.log"
        monkeypatch.setenv("BUILD_LOG", str(build_log))
        wt = _make_worktree(tmp_path, "a", '{"default": {"x": "1"}}')
        assert deps.ensure_shared_venv(fake_config, "repo", wt, dep_cmd=STUB_BUILD)
        old_target = (wt / ".venv").readlink()

        (wt / "Pipfile.lock").write_text('{"default": {"x": "2"}}')
        assert deps.ensure_shared_venv(fake_config, "repo", wt, dep_cmd=STUB_BUILD)
        new_target = (wt / ".venv").readlink()
        assert new_target != old_target
        assert (new_target / "bin" / "python").exists()

    def test_concurrent_first_build_runs_once(self, fake_config, tmp_path, monkeypatch):
        build_log = tmp_path / "builds.log"
        monkeypatch.setenv("BUILD_LOG", str(build_log))
        wt_a = _make_worktree(tmp_path, "a")
        wt_b = _make_worktree(tmp_path, "b")

        results = []
        threads = [
            threading.Thread(target=lambda wt=wt: results.append(
                deps.ensure_shared_venv(fake_config, "repo", wt, dep_cmd=STUB_SLOW_BUILD)))
            for wt in (wt_a, wt_b)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert results == [True, True]
        assert build_log.read_text().count("build") == 1, \
            "flock must serialize the first build so the loser skips it"

    def test_replaces_real_venv_directory_with_store_symlink(self, fake_config, tmp_path, monkeypatch):
        build_log = tmp_path / "builds.log"
        monkeypatch.setenv("BUILD_LOG", str(build_log))
        wt = _make_worktree(tmp_path, "a")
        real = wt / ".venv"
        real.mkdir()
        (real / "marker").touch()

        assert deps.ensure_shared_venv(fake_config, "repo", wt, dep_cmd=STUB_BUILD)

        link = wt / ".venv"
        assert link.is_symlink(), "a project-local .venv must be replaced so pipenv resolves the store"
        assert link.readlink() == deps.venv_store_dir(fake_config) / deps.shared_venv_name("repo", wt)
        assert not (link / "marker").exists()


class TestRelink:
    def test_restores_symlink_after_clean(self, fake_config, tmp_path, monkeypatch):
        build_log = tmp_path / "builds.log"
        monkeypatch.setenv("BUILD_LOG", str(build_log))
        wt = _make_worktree(tmp_path, "a")
        assert deps.ensure_shared_venv(fake_config, "repo", wt, dep_cmd=STUB_BUILD)
        target = (wt / ".venv").readlink()
        (wt / ".venv").unlink()

        deps.relink_shared_venv(fake_config, "repo", wt)
        assert (wt / ".venv").readlink() == target

    def test_noop_when_venv_not_built(self, fake_config, tmp_path):
        wt = _make_worktree(tmp_path, "a")
        deps.relink_shared_venv(fake_config, "repo", wt)
        assert not (wt / ".venv").exists()


class TestRunDepCommand:
    def test_routes_pipenv_through_shared_venv(self, fake_config, tmp_path):
        wt = _make_worktree(tmp_path, "a")
        with patch("core.deps.ensure_shared_venv", return_value=True) as ensure:
            assert deps.run_dep_command(fake_config, "repo", wt, "pipenv install --dev")
        ensure.assert_called_once_with(fake_config, "repo", wt,
                                       dep_cmd="pipenv install --dev")

    def test_pipenv_without_pipfile_runs_unchanged(self, fake_config, tmp_path):
        wt = tmp_path / "wt"
        wt.mkdir()
        with patch("core.deps.subprocess.run") as run:
            run.return_value.returncode = 0
            deps.run_dep_command(fake_config, "repo", wt, "pipenv install")
        assert run.call_args.args[0] == ["pipenv", "install"]

    def test_non_pipenv_passthrough(self, fake_config, tmp_path):
        wt = _make_worktree(tmp_path, "a")
        with patch("core.deps.subprocess.run") as run:
            run.return_value.returncode = 0
            assert deps.run_dep_command(fake_config, "repo", wt, "npm ci")
        assert run.call_args.args[0] == ["npm", "ci"]

    def test_missing_binary_is_nonfatal(self, fake_config, tmp_path):
        wt = tmp_path / "wt"
        wt.mkdir()
        assert deps.run_dep_command(fake_config, "repo", wt,
                                    "definitely-not-a-binary-xyz install") is False


class TestExportPnpmStore:
    def test_sets_store_dir(self, fake_config, monkeypatch):
        monkeypatch.delenv("npm_config_store_dir", raising=False)
        deps.export_pnpm_store(fake_config)
        assert os.environ["npm_config_store_dir"] == str(deps.pnpm_store_dir(fake_config))

    def test_respects_operator_override(self, fake_config, monkeypatch):
        monkeypatch.setenv("npm_config_store_dir", "/custom/store")
        deps.export_pnpm_store(fake_config)
        assert os.environ["npm_config_store_dir"] == "/custom/store"


class TestGc:
    def _seed_venv(self, store, name, age_days):
        venv = store / name
        (venv / "bin").mkdir(parents=True)
        (venv / "bin" / "python").touch()
        old = time.time() - age_days * 86400
        os.utime(venv, (old, old))
        return venv

    def test_deletes_old_keeps_young(self, fake_config):
        store = deps.venv_store_dir(fake_config)
        store.mkdir(parents=True)
        old = self._seed_venv(store, "repo-aaaaaaaa", age_days=15)
        young = self._seed_venv(store, "repo-bbbbbbbb", age_days=1)

        result = deps.gc_dep_stores(fake_config)
        assert result["deleted"] == ["repo-aaaaaaaa"]
        assert not old.exists()
        assert young.exists()

    def test_throttled_by_stamp(self, fake_config):
        store = deps.venv_store_dir(fake_config)
        store.mkdir(parents=True)
        (store / ".gc-stamp").touch()
        self._seed_venv(store, "repo-aaaaaaaa", age_days=15)

        result = deps.gc_dep_stores(fake_config)
        assert result.get("throttled") is True
        assert (store / "repo-aaaaaaaa").exists()

    def test_noop_without_store(self, fake_config):
        assert deps.gc_dep_stores(fake_config) == {"deleted": []}
