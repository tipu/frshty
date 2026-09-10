import shutil
import subprocess

import pytest

import core.terminal as terminal
import core.tmux as tmux_target
from services import work_store


pytestmark = pytest.mark.skipif(shutil.which("tmux") is None,
                                reason="the exact-target check drives a real tmux server")

LONGER = "term-work-10"
SHORTER_KEY = "work-1"
SHORTER = "term-work-1"


@pytest.fixture()
def socket(tmp_path, monkeypatch):
    """One tmux server of our own, holding term-work-10 and nothing else.

    term-work-1 does not exist, and a bare tmux target resolves it to
    term-work-10 by prefix. That is the trap every call site has to refuse."""
    path = str(tmp_path / "tmux.sock")
    monkeypatch.setattr(terminal, "TMUX_SOCKET", path)
    monkeypatch.setattr(work_store, "TMUX_SOCKET", path)
    subprocess.run(["tmux", "-S", path, "new-session", "-d", "-s", LONGER, "sleep 120"],
                   check=True, capture_output=True)
    yield path
    subprocess.run(["tmux", "-S", path, "kill-server"], capture_output=True)


def _sessions(path):
    out = subprocess.run(["tmux", "-S", path, "list-sessions", "-F", "#{session_name}"],
                         capture_output=True, text=True)
    return sorted(out.stdout.split())


class TestBareTargetIsAPrefixMatch:
    def test_tmux_resolves_a_shorter_name_to_a_longer_session(self, socket):
        bare = subprocess.run(["tmux", "-S", socket, "has-session", "-t", SHORTER],
                              capture_output=True)
        exact = subprocess.run(
            ["tmux", "-S", socket, "has-session", "-t", tmux_target.session(SHORTER)],
            capture_output=True)
        assert bare.returncode == 0
        assert exact.returncode != 0


class TestTerminal:
    def test_the_existence_check_refuses_a_prefix(self, socket):
        assert terminal._tmux_session_exists(LONGER) is True
        assert terminal._tmux_session_exists(SHORTER) is False

    def test_kill_leaves_the_longer_session_alone(self, socket):
        terminal.kill_terminal(SHORTER_KEY)
        assert _sessions(socket) == [LONGER]

    def test_kill_still_kills_its_own_session(self, socket):
        terminal.kill_terminal("work-10")
        assert _sessions(socket) == []

    def test_the_health_check_reads_no_pane_from_a_prefix(self, socket):
        assert terminal.session_healthy(SHORTER_KEY)["alive"] is False
        assert terminal.session_healthy("work-10")["alive"] is True

    def test_the_pane_text_of_a_prefix_is_empty(self, socket):
        assert terminal.pane_text(SHORTER_KEY) == ""
        assert isinstance(terminal.pane_text("work-10"), str)


class TestWorkStore:
    def test_a_send_to_a_prefix_fails(self, socket):
        assert work_store.tmux_send(SHORTER_KEY, "echo hello") is False

    def test_a_send_to_its_own_session_lands(self, socket):
        assert work_store.tmux_send("work-10", "") is True

    def test_the_agent_check_reads_no_pane_from_a_prefix(self, socket):
        assert work_store.agent_running(SHORTER_KEY) is False

    def test_the_pane_activity_of_a_prefix_is_empty(self, socket):
        assert work_store.pane_activity(SHORTER_KEY) == ""
        assert work_store.pane_activity("work-10") != ""
