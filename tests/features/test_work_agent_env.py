import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import core.db as db
from core import terminal
from services import work_launch


QUILL_DIR = "~/.quill-claude"
DEFAULT_CMD = "claude --dangerously-skip-permissions"


def _instances(tmp_path, extra=None):
    entries = {
        "personal": SimpleNamespace(config={"workspace": {"root": str(tmp_path)}}),
        "quill": SimpleNamespace(config={
            "workspace": {"root": str(tmp_path / "quill")},
            "llm": {"provider": "claude", "claude": {"config_dir": QUILL_DIR}},
        }),
        "aimyable": SimpleNamespace(config={
            "workspace": {"root": str(tmp_path / "aimyable")},
            "llm": {"provider": "claude"},
        }),
    }
    entries.update(extra or {})
    return entries


def _launch(tmp_path, contexts, entries=None, agent="claude"):
    """Launch one work item and return the result and the launch_agent call."""
    launcher = MagicMock()
    with patch("services.work_launch.runtime.instances",
               return_value=entries if entries is not None else _instances(tmp_path)), \
         patch("services.work_launch.terminal.launch_agent", launcher), \
         patch("services.work_launch.terminal.session_healthy",
               return_value={"alive": True, "agent_running": True}), \
         patch("services.work_launch.threading.Thread"), \
         patch("services.work_launch.work_tags.schedule_implicit_tags"):
        out = work_launch.launch("do the work", contexts=contexts, agent=agent)
    return out, launcher.call_args


def _resume(tmp_path, item_id, entries=None):
    launcher = MagicMock()
    with patch("services.work_launch.runtime.instances",
               return_value=entries if entries is not None else _instances(tmp_path)), \
         patch("services.work_launch.terminal.launch_agent", launcher), \
         patch("services.work_launch.terminal.session_healthy",
               return_value={"alive": True, "agent_running": False}):
        assert work_launch.resume_session(item_id) is True
    return launcher.call_args


class TestLaunchEnvironment:
    def test_a_project_with_its_own_config_dir_supplies_the_pane_environment(self, tmp_path):
        _, call = _launch(tmp_path, ["quill"])
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser(QUILL_DIR)} ")

    def test_a_project_without_its_own_environment_keeps_the_default_account(self, tmp_path):
        _, call = _launch(tmp_path, ["aimyable"])
        assert terminal.claude_cmd(call.kwargs["config"]) == DEFAULT_CMD

    def test_a_launch_with_no_project_keeps_the_default_account(self, tmp_path):
        _, call = _launch(tmp_path, [])
        assert terminal.claude_cmd(call.kwargs["config"]) == DEFAULT_CMD

    def test_a_second_project_does_not_drop_the_selected_environment(self, tmp_path):
        _, call = _launch(tmp_path, ["aimyable", "quill"])
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser(QUILL_DIR)} ")

    def test_the_same_project_twice_still_supplies_its_environment(self, tmp_path):
        _, call = _launch(tmp_path, ["quill", "quill"])
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser(QUILL_DIR)} ")

    def test_two_environments_refuse_the_launch(self, tmp_path):
        entries = _instances(tmp_path, {
            "other": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path / "other")},
                "llm": {"claude": {"config_dir": "~/.other-claude"}},
            }),
        })
        before = db.query_one("SELECT COUNT(*) AS n FROM work_items")["n"]
        out, call = _launch(tmp_path, ["quill", "other"], entries=entries)
        assert "each pin their own claude environment" in out["error"]
        assert call is None
        assert db.query_one("SELECT COUNT(*) AS n FROM work_items")["n"] == before

    def test_a_codex_only_project_does_not_compete_for_the_claude_environment(self, tmp_path):
        entries = _instances(tmp_path, {
            "other": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path / "other")},
                "llm": {"codex": {"config_dir": "~/.other-codex"}},
            }),
        })
        _, call = _launch(tmp_path, ["quill", "other"], entries=entries)
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser(QUILL_DIR)} ")

    def test_a_claude_only_project_does_not_supply_a_codex_pane(self, tmp_path):
        _, call = _launch(tmp_path, ["quill"], agent="codex")
        assert terminal.codex_cmd(call.kwargs["config"]) == \
            "codex --dangerously-bypass-approvals-and-sandbox"

    def test_the_cross_check_reviewer_runs_in_the_project_environment(self, tmp_path):
        entries = _instances(tmp_path, {
            "quill": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path / "quill")},
                "llm": {"claude": {"config_dir": QUILL_DIR},
                        "codex": {"config_dir": "~/.quill-codex"}},
            }),
        })
        _, call = _launch(tmp_path, ["quill"], entries=entries)
        assert f"CODEX_HOME={os.path.expanduser('~/.quill-codex')} codex exec" in call.args[3]


class TestRecordedEnvironment:
    def test_the_run_records_the_project_and_the_config_dir(self, tmp_path):
        out, _ = _launch(tmp_path, ["quill"])
        run = db.query_one("SELECT env_key, env_config_dir FROM work_runs WHERE id = ?",
                           (out["run_id"],))
        assert run["env_key"] == "quill"
        assert run["env_config_dir"] == QUILL_DIR

    def test_a_default_run_records_no_environment(self, tmp_path):
        out, _ = _launch(tmp_path, ["aimyable"])
        run = db.query_one("SELECT env_key, env_config_dir FROM work_runs WHERE id = ?",
                           (out["run_id"],))
        assert run["env_key"] == ""
        assert run["env_config_dir"] == ""


class TestResumeEnvironment:
    def test_a_resume_keeps_the_environment_of_the_launch(self, tmp_path):
        out, _ = _launch(tmp_path, ["quill"])
        call = _resume(tmp_path, out["item_id"])
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser(QUILL_DIR)} ")

    def test_a_resume_of_a_default_task_keeps_the_default_account(self, tmp_path):
        out, _ = _launch(tmp_path, ["aimyable"])
        call = _resume(tmp_path, out["item_id"])
        assert terminal.claude_cmd(call.kwargs["config"]) == DEFAULT_CMD

    def test_a_resume_survives_the_project_dropping_its_environment(self, tmp_path):
        out, _ = _launch(tmp_path, ["quill"])
        stripped = _instances(tmp_path, {
            "quill": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path / "quill")},
                "llm": {"provider": "claude"},
            }),
        })
        call = _resume(tmp_path, out["item_id"], entries=stripped)
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser(QUILL_DIR)} ")

    def test_a_resume_is_refused_when_the_project_no_longer_loads(self, tmp_path):
        out, _ = _launch(tmp_path, ["quill"])
        gone = {"personal": SimpleNamespace(config={
            "workspace": {"root": str(tmp_path)},
            "llm": {"claude": {"env": {"ANTHROPIC_API_KEY": "sk-personal"}}},
        })}
        launcher = MagicMock()
        with patch("services.work_launch.runtime.instances", return_value=gone), \
             patch("services.work_launch.terminal.launch_agent", launcher), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": False}), \
             patch("services.work_launch.log.emit") as emit:
            assert work_launch.resume_session(out["item_id"]) is False
        launcher.assert_not_called()
        assert emit.call_args.args[0] == "work_agent_env_missing"

    def test_a_project_that_pins_only_a_key_keeps_the_default_directory(self, tmp_path):
        entries = _instances(tmp_path, {
            "quill": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path / "quill")},
                "llm": {"claude": {"env": {"ANTHROPIC_API_KEY": "sk-quill"}}},
            }),
        })
        out, call = _launch(tmp_path, ["quill"], entries=entries)
        assert "sk-quill" in terminal.claude_cmd(call.kwargs["config"])
        run = db.query_one("SELECT env_key, env_config_dir FROM work_runs WHERE id = ?",
                           (out["run_id"],))
        assert run["env_key"] == "quill"
        assert run["env_config_dir"] == ""
        later = _instances(tmp_path, {
            "quill": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path / "quill")},
                "llm": {"claude": {"config_dir": QUILL_DIR,
                                   "env": {"ANTHROPIC_API_KEY": "sk-quill"}}},
            }),
        })
        call = _resume(tmp_path, out["item_id"], entries=later)
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert "CLAUDE_CONFIG_DIR" not in cmd
        assert "sk-quill" in cmd

    def test_a_run_with_no_recorded_environment_keeps_the_live_default(self, tmp_path):
        out, _ = _launch(tmp_path, [])
        db.execute("UPDATE work_runs SET env_recorded = 0 WHERE id = ?", (out["run_id"],))
        pinned = _instances(tmp_path, {
            "personal": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path)},
                "llm": {"claude": {"config_dir": "~/.personal-claude"}},
            }),
        })
        call = _resume(tmp_path, out["item_id"], entries=pinned)
        cmd = terminal.claude_cmd(call.kwargs["config"])
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser('~/.personal-claude')} ")

    def test_a_recorded_default_run_ignores_a_new_personal_directory(self, tmp_path):
        out, _ = _launch(tmp_path, [])
        pinned = _instances(tmp_path, {
            "personal": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path)},
                "llm": {"claude": {"config_dir": "~/.personal-claude"}},
            }),
        })
        call = _resume(tmp_path, out["item_id"], entries=pinned)
        assert terminal.claude_cmd(call.kwargs["config"]) == DEFAULT_CMD

    def test_a_resume_of_a_default_task_ignores_a_new_project_environment(self, tmp_path):
        out, _ = _launch(tmp_path, ["aimyable"])
        pinned = _instances(tmp_path, {
            "aimyable": SimpleNamespace(config={
                "workspace": {"root": str(tmp_path / "aimyable")},
                "llm": {"claude": {"config_dir": "~/.aimyable-claude"}},
            }),
        })
        call = _resume(tmp_path, out["item_id"], entries=pinned)
        assert terminal.claude_cmd(call.kwargs["config"]) == DEFAULT_CMD
