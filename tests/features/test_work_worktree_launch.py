"""The launch, resume and follow-up paths that put a run in a worktree."""
import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

import core.config as core_config
import core.db as db
import core.git_util as git_util
from services import work_launch, work_store, work_worktree
from tests.features.test_work_worktree import (_git, instances, make_repo,
                                               project_config)


def client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from web.work import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


@pytest.fixture()
def project(tmp_path, monkeypatch):
    """One project with one repository, and a personal instance beside it."""
    monkeypatch.setattr(core_config, "TASK_WORKTREE_ROOT", tmp_path / "worktrees")
    db.execute("DELETE FROM work_worktrees")
    root = tmp_path / "proj"
    root.mkdir()
    repo = make_repo(root, "app")
    personal_root = tmp_path / "personal"
    personal_root.mkdir()
    registry = instances(proj=project_config(root, ["app"]),
                         personal=project_config(personal_root, []))
    monkeypatch.setattr(work_launch.runtime, "instances", lambda: registry)
    monkeypatch.setattr(work_worktree.runtime, "instances", lambda: registry)
    monkeypatch.setattr(work_worktree.terminal, "session_healthy",
                        lambda key, agent="claude": {"alive": False, "agent_running": False})
    return {"root": root, "repo": repo, "personal": personal_root}


class TestLaunch:
    def test_a_launch_runs_in_a_worktree_of_the_selected_project(self, project):
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            result = work_launch.launch("build the widget", contexts=["proj"])
        assert "error" not in result, result
        row = work_worktree.for_item(result["item_id"])
        assert row is not None
        assert git_util.is_worktree(row["path"])
        assert launched.call_args.args[1] == row["path"]
        run = db.query_one("SELECT cwd, board_url FROM work_runs WHERE id = ?",
                           (result["run_id"],))
        assert run["cwd"] == row["path"]

    def test_the_launch_context_names_the_worktree_and_the_shared_checkout(self, project):
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            result = work_launch.launch("build the widget", contexts=["proj"])
        context = launched.call_args.args[3]
        row = work_worktree.for_item(result["item_id"])
        assert row["path"] in context
        assert row["branch"] in context
        assert str(project["repo"]) in context
        assert "Do not detach HEAD" in context

    def test_no_worktree_keeps_the_task_in_the_workspace_root(self, project):
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            result = work_launch.launch("read the code", contexts=["proj"],
                                        no_worktree=True)
        assert work_worktree.for_item(result["item_id"]) is None
        assert launched.call_args.args[1] == str(project["root"])
        item = db.query_one("SELECT worktree_opt_out FROM work_items WHERE id = ?",
                            (result["item_id"],))
        assert item["worktree_opt_out"] == 1

    def test_a_second_launch_of_the_same_item_reuses_the_worktree(self, project):
        with patch("services.work_launch.terminal.launch_agent"), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            first = work_launch.launch("do the thing", contexts=["proj"])
            row = work_worktree.for_item(first["item_id"])
            plan = work_launch._resolve_launch("do the thing", "", ["proj"], "claude",
                                               None, item_id=first["item_id"])
        assert plan["worktree"]["rule"] == "R1"
        assert plan["cwd"] == row["path"]

    def test_a_launch_rebuilds_a_worktree_the_sweep_took_after_it_materialized(self,
                                                                                project):
        """The sweep can remove the directory between materializing and the
        check under the lock. The launch rebuilds rather than failing."""
        real_ensure = work_worktree.ensure
        state = {"swept": False}

        def ensure_then_sweep(item_id, spec, objective=""):
            row = real_ensure(item_id, spec, objective)
            if row and not state["swept"]:
                state["swept"] = True
                _git(spec["repo_path"], "worktree", "remove", row["path"])
                db.execute("UPDATE work_worktrees SET removed_at = ? WHERE id = ?",
                           ("2026-01-01T00:00:00+00:00", row["id"]))
            return row

        with patch("services.work_launch.work_worktree.ensure", ensure_then_sweep), \
             patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            result = work_launch.launch("rebuild me", contexts=["proj"])
        assert "error" not in result, result
        landed = launched.call_args.args[1]
        assert os.path.isdir(landed)
        assert git_util.is_worktree(landed)

    def test_a_follow_up_rebuilds_when_the_sweep_took_its_source_worktree(self, project):
        """The follow-up matches R2 and owns nothing until adopt_path runs. If
        the sweep wins that race, the row at the path still names the
        repository."""
        item_id = work_store.create_item("parent swept", contexts="proj")
        work_store.add_run(item_id, f"sid-swept-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        got = client().post(f"/api/work/items/{item_id}/worktree",
                            json={"repo_path": str(project["repo"])}).json()
        work_store.apply_action(item_id, "done")
        real_adopt = work_worktree.adopt_path

        def adopt_after_sweep(child_id, path):
            _git(project["repo"], "worktree", "remove", path)
            db.execute("UPDATE work_worktrees SET removed_at = ? WHERE path = ?",
                       ("2026-01-01T00:00:00+00:00", path))
            return real_adopt(child_id, path)

        with patch("services.work_launch.work_worktree.adopt_path", adopt_after_sweep), \
             patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            child = work_launch.launch_followup(item_id, "carry on")
        assert "error" not in child, child
        landed = launched.call_args.args[1]
        assert os.path.isdir(landed)
        assert git_util.is_worktree(landed)
        assert landed != got["path"]

    def test_the_launch_is_refused_when_the_directory_vanishes_inside_the_lock(self, project):
        """gc holds the same lock, but a worktree removed by anything else must
        not send tmux to $HOME: a tmux session whose -c directory is gone
        starts there rather than failing."""
        real_ensure = work_worktree.ensure

        def ensure_then_lose(item_id, spec, objective=""):
            row = real_ensure(item_id, spec, objective)
            shutil.rmtree(row["path"])
            return row

        with patch("services.work_launch.work_worktree.ensure", ensure_then_lose), \
             patch("services.work_launch.terminal.launch_agent") as launched:
            result = work_launch.launch("vanishing act", contexts=["proj"])
        assert "cwd does not exist" in result["error"]
        launched.assert_not_called()
        assert work_store.has_run(result["item_id"]) is False


class TestResume:
    def _suspended(self, project, cwd):
        item_id = work_store.create_item("resume me", contexts="proj")
        sid = f"sid-resume-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", str(cwd))
        return item_id, sid

    def test_resume_moves_a_pre_feature_run_out_of_the_shared_checkout(self, project):
        item_id, sid = self._suspended(project, project["repo"])
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": False, "agent_running": False}):
            assert work_launch.resume_session(item_id) is True
        row = work_worktree.for_item(item_id)
        assert row is not None
        assert launched.call_args.args[1] == row["path"]
        run = db.query_one("SELECT cwd FROM work_runs WHERE session_id = ?", (sid,))
        assert run["cwd"] == row["path"]

    def test_resume_keeps_the_session_id_and_the_provider(self, project):
        item_id, sid = self._suspended(project, project["repo"])
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": False, "agent_running": False}):
            work_launch.resume_session(item_id)
        assert launched.call_args.args[2] == sid
        assert launched.call_args.kwargs["agent"] == "claude"

    def test_resume_rebuilds_a_worktree_the_sweep_removed(self, project):
        item_id, sid = self._suspended(project, project["repo"])
        with patch("services.work_launch.terminal.launch_agent"), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": False, "agent_running": False}):
            work_launch.resume_session(item_id)
            row = work_worktree.for_item(item_id)
            _git(project["repo"], "worktree", "remove", row["path"])
            db.execute("UPDATE work_worktrees SET removed_at = NULL WHERE id = ?",
                       (row["id"],))
            with patch("services.work_launch.terminal.launch_agent") as again:
                work_launch.resume_session(item_id)
        rebuilt = work_worktree.for_item(item_id)
        assert os.path.isdir(rebuilt["path"])
        assert again.call_args.args[1] == rebuilt["path"]

    def test_resume_stays_in_a_worktree_it_is_already_in(self, project):
        wt = project["root"] / "existing"
        _git(project["repo"], "worktree", "add", "-b", "already", str(wt))
        item_id, sid = self._suspended(project, wt)
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": False, "agent_running": False}):
            work_launch.resume_session(item_id)
        assert launched.call_args.args[1] == str(wt)
        assert work_worktree.for_item(item_id) is None

    def test_resume_refuses_when_no_directory_exists_at_all(self, project, tmp_path):
        """tmux does not fail on a missing -c directory. It starts in $HOME,
        where every relative path the agent writes would land."""
        item_id, sid = self._suspended(project, project["repo"])
        gone = tmp_path / "never-made"
        project_registry = work_launch.runtime.instances()
        project_registry["personal"].config["workspace"]["root"] = gone
        with patch("services.work_launch.work_worktree.ensure", lambda *a, **k: {}), \
             patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": False, "agent_running": False}):
            db.execute("UPDATE work_runs SET cwd = ? WHERE session_id = ?",
                       (str(gone), sid))
            assert work_launch.resume_session(item_id) is False
        launched.assert_not_called()

    def test_resume_never_falls_back_into_the_shared_checkout(self, project):
        """A task finished long enough for the sweep is exactly the task an
        operator reopens. If its worktree is gone and cannot be rebuilt, the
        resume is refused rather than run in the tree this feature exists to
        keep agents out of."""
        item_id, sid = self._suspended(project, project["repo"])
        with patch("services.work_launch.terminal.launch_agent"), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": False, "agent_running": False}):
            work_launch.resume_session(item_id)
            row = work_worktree.for_item(item_id)
            shutil.rmtree(row["path"])
            with patch("services.work_launch.work_worktree.rebuild",
                       lambda *a, **k: {}), \
                 patch("services.work_launch.work_worktree.ensure",
                       lambda *a, **k: {}), \
                 patch("services.work_launch.terminal.launch_agent") as again:
                assert work_launch.resume_session(item_id) is False
            again.assert_not_called()

    def test_resume_rebuilds_a_worktree_the_sweep_took_after_it_resolved(self, project):
        """gc runs in the window between resolving the directory and taking
        the lock. The row still names the repository, so the tree comes back
        rather than the resume landing in the shared checkout."""
        item_id, sid = self._suspended(project, project["repo"])
        with patch("services.work_launch.terminal.launch_agent"), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": False, "agent_running": False}):
            work_launch.resume_session(item_id)
            first = work_worktree.for_item(item_id)
            real_plan = work_worktree.plan

            def plan_then_sweep(*args, **kwargs):
                resolved = real_plan(*args, **kwargs)
                if os.path.isdir(first["path"]):
                    _git(project["repo"], "worktree", "remove", first["path"])
                    db.execute("UPDATE work_worktrees SET removed_at = ? WHERE id = ?",
                               ("2026-01-01T00:00:00+00:00", first["id"]))
                return resolved

            with patch("services.work_launch.work_worktree.plan", plan_then_sweep), \
                 patch("services.work_launch.terminal.launch_agent") as again:
                assert work_launch.resume_session(item_id) is True
        landed = again.call_args.args[1]
        assert os.path.isdir(landed)
        assert git_util.is_worktree(landed)
        assert landed != str(project["repo"])

    def test_resume_is_a_no_op_while_the_agent_runs(self, project):
        item_id, sid = self._suspended(project, project["repo"])
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            assert work_launch.resume_session(item_id) is True
        launched.assert_not_called()


class TestLaunchLockIsNotHeldOverTheSlowPart:
    """A fetch and a dependency install must not stall every other launch,
    resume and write gate behind the global lock."""

    def test_a_launch_does_not_hold_the_lock_while_it_materializes(self, project):
        seen = {}
        real_ensure = work_worktree.ensure

        def watching_ensure(item_id, spec, objective=""):
            seen["locked"] = not work_store.launch_lock.acquire(blocking=False)
            if not seen["locked"]:
                work_store.launch_lock.release()
            return real_ensure(item_id, spec, objective)

        with patch("services.work_launch.work_worktree.ensure", watching_ensure), \
             patch("services.work_launch.terminal.launch_agent"), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            work_launch.launch("materialize free of the lock", contexts=["proj"])
        assert seen == {"locked": False}

    def test_the_endpoint_does_not_hold_the_lock_while_it_materializes(self, project):
        item_id = work_store.create_item("endpoint free of the lock", contexts="proj")
        work_store.add_run(item_id, f"sid-free-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        seen = {}
        real_ensure = work_worktree.ensure_for_repo

        def watching(item, repo_dir, objective="", entries=None):
            seen["locked"] = not work_store.launch_lock.acquire(blocking=False)
            if not seen["locked"]:
                work_store.launch_lock.release()
            return real_ensure(item, repo_dir, objective, entries)

        with patch("web.work.work_worktree.ensure_for_repo", watching):
            r = client().post(f"/api/work/items/{item_id}/worktree",
                              json={"repo_path": str(project["repo"])})
        assert r.status_code == 200, r.text
        assert seen == {"locked": False}


class TestBoardAddress:
    """The hook asks the board for a worktree from its own process. Every
    instance config here names a public host over https behind a proxy, and
    --port moves the listener without changing any config, so the recorded
    address has to be the one the server published."""

    def test_the_run_records_the_published_address_not_the_config_host(self, project,
                                                                       tmp_path,
                                                                       monkeypatch):
        monkeypatch.setattr(core_config, "BOARD_FILE", tmp_path / "board.json")
        monkeypatch.setattr(core_config, "_BOARD_URL", "")
        registry = work_launch.runtime.instances()
        registry["personal"].config["_base_url"] = "https://personal.frshty.localhost"
        core_config.write_board_file("http://127.0.0.1:7100")
        with patch("services.work_launch.terminal.launch_agent"), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            result = work_launch.launch("record the address", contexts=["proj"])
        run = db.query_one("SELECT board_url FROM work_runs WHERE id = ?",
                           (result["run_id"],))
        assert run["board_url"] == "http://127.0.0.1:7100"

    def test_the_published_address_survives_a_fresh_process(self, tmp_path, monkeypatch):
        monkeypatch.setattr(core_config, "BOARD_FILE", tmp_path / "board.json")
        monkeypatch.setattr(core_config, "_BOARD_URL", "")
        core_config.write_board_file("http://127.0.0.1:9999")
        monkeypatch.setattr(core_config, "_BOARD_URL", "")
        assert core_config.board_url() == "http://127.0.0.1:9999"

    def test_no_published_address_is_an_empty_string(self, tmp_path, monkeypatch):
        monkeypatch.setattr(core_config, "BOARD_FILE", tmp_path / "missing.json")
        monkeypatch.setattr(core_config, "_BOARD_URL", "")
        assert core_config.board_url() == ""

    def test_the_server_publishes_the_bound_port_not_the_config_port(self):
        """frshty.py must write the address after --port and --host are read."""
        source = (Path(__file__).resolve().parent.parent.parent / "frshty.py").read_text()
        published = source.index("cfg.write_board_file(")
        assert source.index('port = args.port or _config["job"]["port"]') < published
        assert source.index('host = args.host or _config["job"].get("bind"') < published
        assert "_base_url" not in source[published:published + 300]


class TestWorktreeEndpoint:
    def test_the_endpoint_creates_the_worktree_and_moves_the_run(self, project):
        item_id = work_store.create_item("endpoint", contexts="proj")
        sid = f"sid-endpoint-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", str(project["repo"]))
        r = client().post(f"/api/work/items/{item_id}/worktree",
                          json={"repo_path": str(project["repo"])})
        assert r.status_code == 200, r.text
        got = r.json()
        assert git_util.is_worktree(got["path"])
        assert got["repo_path"] == str(project["repo"])
        run = db.query_one("SELECT cwd FROM work_runs WHERE session_id = ?", (sid,))
        assert run["cwd"] == got["path"]

    def test_the_endpoint_is_idempotent(self, project):
        item_id = work_store.create_item("endpoint twice", contexts="proj")
        work_store.add_run(item_id, f"sid-twice-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        c = client()
        first = c.post(f"/api/work/items/{item_id}/worktree",
                       json={"repo_path": str(project["repo"])}).json()
        second = c.post(f"/api/work/items/{item_id}/worktree",
                        json={"repo_path": str(project["repo"])}).json()
        assert first["path"] == second["path"]

    def test_a_missing_repo_path_is_refused(self, project):
        item_id = work_store.create_item("no repo path", contexts="proj")
        r = client().post(f"/api/work/items/{item_id}/worktree", json={})
        assert r.status_code == 400

    def test_a_path_outside_any_repository_is_refused(self, project):
        item_id = work_store.create_item("not a repo", contexts="proj")
        r = client().post(f"/api/work/items/{item_id}/worktree",
                          json={"repo_path": str(project["personal"])})
        assert r.status_code == 409

    def test_a_follow_up_lands_in_the_same_worktree(self, project):
        item_id = work_store.create_item("parent task", contexts="proj")
        sid = f"sid-parent-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", str(project["repo"]))
        got = client().post(f"/api/work/items/{item_id}/worktree",
                            json={"repo_path": str(project["repo"])}).json()
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            child = work_launch.launch_followup(item_id, "carry on")
        assert "error" not in child, child
        assert launched.call_args.args[1] == got["path"]

    def test_a_follow_up_that_names_the_same_projects_still_inherits(self, project):
        """The follow-up box on the task page always sends the projects, seeded
        from the source task. A rule keyed on them cut every follow-up launched
        from the board off from its source's worktree."""
        item_id = work_store.create_item("parent named", contexts="proj")
        work_store.add_run(item_id, f"sid-named-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        got = client().post(f"/api/work/items/{item_id}/worktree",
                            json={"repo_path": str(project["repo"])}).json()
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            child = work_launch.launch_followup(item_id, "carry on",
                                                contexts=["proj"], slack=False)
        assert "error" not in child, child
        assert launched.call_args.args[1] == got["path"]

    def test_the_projects_are_a_set_not_a_sequence(self, project):
        """The board sends the projects in whatever order the chips were
        toggled. The same selection in another order is the same selection."""
        item_id = work_store.create_item("ordered", contexts="proj,personal")
        work_store.add_run(item_id, f"sid-order-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        got = client().post(f"/api/work/items/{item_id}/worktree",
                            json={"repo_path": str(project["repo"])}).json()
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            work_launch.launch_followup(item_id, "carry on",
                                        contexts=["personal", "proj"], slack=False)
        assert launched.call_args.args[1] == got["path"]

    def test_a_follow_up_that_changes_the_projects_does_not_inherit(self, project):
        item_id = work_store.create_item("parent switched", contexts="proj")
        work_store.add_run(item_id, f"sid-switch-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        got = client().post(f"/api/work/items/{item_id}/worktree",
                            json={"repo_path": str(project["repo"])}).json()
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            work_launch.launch_followup(item_id, "carry on", contexts=[], slack=False)
        assert launched.call_args.args[1] != got["path"]

    def test_a_follow_up_becomes_a_holder_of_the_worktree_it_reuses(self, project):
        """R2 creates nothing, so without a row of its own the sweep would hold
        the directory to the source task's state alone."""
        item_id = work_store.create_item("parent held", contexts="proj")
        work_store.add_run(item_id, f"sid-held-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        got = client().post(f"/api/work/items/{item_id}/worktree",
                            json={"repo_path": str(project["repo"])}).json()
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.launch_agent"), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            child = work_launch.launch_followup(item_id, "carry on")
        row = work_worktree.for_item(child["item_id"])
        assert row is not None
        assert row["path"] == got["path"]
        assert row["origin"] == "reused_worktree"
        assert work_worktree.for_item(child["item_id"])["branch"] == got["branch"]

    def test_a_follow_up_uses_the_worktree_row_when_the_run_was_never_moved(self, project):
        item_id = work_store.create_item("parent task", contexts="proj")
        sid = f"sid-parent2-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", str(project["repo"]))
        row = work_worktree.ensure_for_repo(item_id, str(project["repo"]), entries=[])
        db.execute("UPDATE work_runs SET cwd = ? WHERE session_id = ?",
                   (str(project["repo"]), sid))
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.launch_agent") as launched, \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}):
            work_launch.launch_followup(item_id, "carry on")
        assert launched.call_args.args[1] == row["path"]

    def test_the_task_page_reports_the_worktree(self, project):
        item_id = work_store.create_item("shown on the page", contexts="proj")
        work_store.add_run(item_id, f"sid-page-{item_id}", f"work-{item_id}",
                           str(project["repo"]))
        c = client()
        got = c.post(f"/api/work/items/{item_id}/worktree",
                     json={"repo_path": str(project["repo"])}).json()
        detail = c.get(f"/api/work/items/{item_id}/detail").json()
        assert detail["worktree"]["path"] == got["path"]
