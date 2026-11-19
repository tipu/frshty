import os

from services import work_artifacts, work_store
from tests.api.test_artifact_file import _artifact
from web import work as work_routes


def _seed(tmp_path):
    run_cwd = tmp_path / "run-workspace"
    os.makedirs(run_cwd, exist_ok=True)
    item_id = work_store.create_item("discuss route test")
    work_store.add_run(item_id, f"sid-discuss-{item_id}", "work-discuss", str(run_cwd))
    report = run_cwd / "report.html"
    report.write_text("<p>report</p>")
    _artifact(item_id, str(report))
    return item_id, run_cwd, report


class TestDiscussRoute:
    def _stub(self, tmp_path, monkeypatch, running=False):
        launched = []
        scheduled = []
        monkeypatch.setattr(work_routes.terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "launch"))
        monkeypatch.setattr(work_routes.terminal, "session_healthy",
                            lambda key, agent="claude": {"alive": running,
                                                         "agent_running": running})
        monkeypatch.setattr(work_routes.terminal, "launch_pane_command",
                            lambda key, cwd, cmd: launched.append((key, cwd, cmd)))
        monkeypatch.setattr(work_routes.terminal, "claude_cmd", lambda config, cwd="": "claude")
        monkeypatch.setattr(work_routes.work_launch, "personal_config", lambda: {})
        monkeypatch.setattr(work_routes, "schedule_discuss_kill", scheduled.append)
        return launched, scheduled

    def test_launches_claude_in_the_artifact_folder_with_task_run_and_artifacts(self, tmp_path, monkeypatch):
        item_id, run_cwd, report = _seed(tmp_path)
        launched, scheduled = self._stub(tmp_path, monkeypatch)
        resp = work_routes.api_work_discuss_start(item_id)
        key = f"discuss-{item_id}"
        assert resp["key"] == key
        assert [(k, cwd) for k, cwd, _ in launched] == [
            (key, str(work_artifacts.item_dir(item_id)))]
        assert "--append-system-prompt" in launched[0][2]
        seed = (tmp_path / "launch" / f"{key}.md").read_text()
        assert "discuss route test" in seed
        assert str(report) in seed
        assert f"cwd {run_cwd}" in seed
        assert scheduled == [key]

    def test_launches_claude_in_the_task_worktree(self, tmp_path, monkeypatch):
        item_id, _, _ = _seed(tmp_path)
        launched, _ = self._stub(tmp_path, monkeypatch)
        tree = tmp_path / "worktree"
        tree.mkdir()
        monkeypatch.setattr(work_routes.work_worktree, "for_item",
                            lambda i: {"path": str(tree), "branch": "work-1"})
        work_routes.api_work_discuss_start(item_id)
        assert [cwd for _, cwd, _ in launched] == [str(tree)]

    def test_keeps_a_running_session(self, tmp_path, monkeypatch):
        item_id, _, _ = _seed(tmp_path)
        launched, scheduled = self._stub(tmp_path, monkeypatch, running=True)
        resp = work_routes.api_work_discuss_start(item_id)
        assert resp["status"] == "ok"
        assert launched == []
        assert scheduled == [f"discuss-{item_id}"]

    def test_restart_kills_the_session_first(self, tmp_path, monkeypatch):
        item_id, _, _ = _seed(tmp_path)
        launched, _ = self._stub(tmp_path, monkeypatch, running=True)
        killed = []

        def kill(key):
            killed.append(key)
            monkeypatch.setattr(work_routes.terminal, "session_healthy",
                                lambda key, agent="claude": {"alive": False,
                                                             "agent_running": False})
        monkeypatch.setattr(work_routes.terminal, "kill_terminal", kill)
        work_routes.api_work_discuss_start(item_id, restart=True)
        assert killed == [f"discuss-{item_id}"]
        assert [k for k, _, _ in launched] == [f"discuss-{item_id}"]

    def test_unknown_item_is_404(self, tmp_path, monkeypatch):
        launched, _ = self._stub(tmp_path, monkeypatch)
        resp = work_routes.api_work_discuss_start(999999)
        assert resp.status_code == 404
        assert launched == []

    def test_the_page_route_serves_the_discuss_template(self):
        resp = work_routes.task_discuss_page(1)
        assert b"discuss/start" in resp.body
