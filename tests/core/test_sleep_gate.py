import json
import os
import subprocess
import sys

import pytest

import core.db as db
import core.sleep_gate as gate
from services import work_store

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.mark.parametrize("command", [
    "sleep 31",
    "sleep 60",
    "sleep 1m",
    "sleep 2h",
    "sleep 0.6m",
    "sleep 20 15",
    "sleep infinity",
    "sleep '45'",
    "/bin/sleep 90",
    "while ! gh pr checks 12; do sleep 60; done",
    "until curl -s localhost:7311; do sleep 45; done",
    "for i in $(seq 10); do\n  gh pr view 3\n  sleep 120\ndone",
    "cd repo && sleep 300 && gh pr checks 4",
    "while ! test -f done; do sleep 10; done",
    "while true; do gh pr checks 5; sleep 20; done",
    "until gh pr checks 9\ndo\n  sleep 5\n  sleep 6\ndone",
    "for i in 1 2 3 4; do sleep 10; done",
    "sleep 20; sleep 20",
    "bash -c 'sleep 20'; sleep 20",
    "while true; do bash -c 'sleep 10'; done",
    "timeout 600 sleep 100",
    "bash -c 'sleep 90'",
    "echo start; sleep 31 # wait",
    "gh pr checks 12 --watch",
    "gh pr checks --watch --fail-fast 12",
    "gh -R owner/repo pr checks 12 --watch",
    "gh run watch 123456",
    "gh run watch --exit-status",
    "cd repo; timeout 1800 gh pr checks 7 --watch --interval 30",
])
def test_denies_a_foreground_wait(command):
    reason = gate.bash_reason(command)
    assert reason.startswith("Blocked by the wait gate"), command
    assert "waiting on PR #N checks" in reason


@pytest.mark.parametrize("command", [
    "sleep 30",
    "sleep 5",
    "sleep 0.5m",
    "until curl -s localhost:7311; do sleep 2; done",
    "for i in 1 2 3; do make test && break; sleep 5; done",
    "sleep 15; sleep 15",
    "sleep $DELAY",
    "echo 'sleep 600'",
    'git commit -m "sleep 60 then retry"',
    "python3 - <<'EOF'\nimport os\nos.system('sleep 600')\nEOF",
    "grep -rn 'sleep 600' scripts/",
    "gh pr checks 12",
    "gh pr checks 12 --json name,state",
    "gh run view 123 --log-failed",
    "gh pr view 12 --json statusCheckRollup",
    "echo gh run watch",
    "# sleep 600",
])
def test_allows_a_command_that_does_not_wait(command):
    assert gate.bash_reason(command) == "", command


def test_a_background_call_is_not_gated():
    assert gate.bash_reason("sleep 600", run_in_background=True) == ""
    assert gate.bash_reason("gh pr checks 12 --watch", run_in_background=True) == ""


def test_the_reason_names_the_wait():
    assert "120 seconds" in gate.bash_reason("sleep 2m")
    assert "gh run watch" in gate.bash_reason("gh run watch 1")


def _run_hook(session_id, command, run_in_background=None):
    tool_input = {"command": command}
    if run_in_background is not None:
        tool_input["run_in_background"] = run_in_background
    return subprocess.run(
        [sys.executable, "scripts/work_hook.py"], cwd=ROOT,
        input=json.dumps({"session_id": session_id, "hook_event_name": "PreToolUse",
                          "tool_name": "Bash", "cwd": ROOT, "transcript_path": "",
                          "tool_input": tool_input}),
        capture_output=True, text=True, timeout=120,
        env={**os.environ, "FRSHTY_DB": str(db._DB_PATH)},
    )


def _task_session():
    item_id = work_store.create_item("sleep gate")
    sid = f"sid-sleep-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    return sid


def test_hook_denies_a_sleep_loop_in_a_task():
    r = _run_hook(_task_session(), "until gh pr checks 3; do sleep 45; done")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert out["permissionDecisionReason"].startswith("Blocked by the wait gate")


def test_hook_allows_the_same_sleep_in_the_background():
    r = _run_hook(_task_session(), "sleep 600", run_in_background=True)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == ""


def test_hook_allows_a_short_sleep_in_a_task():
    r = _run_hook(_task_session(), "sleep 30")
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == ""


def test_hook_leaves_a_session_that_is_not_a_task_alone():
    r = _run_hook("sid-not-a-work-run-sleep", "sleep 600")
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == ""


@pytest.mark.parametrize("command", [
    'until ! pgrep -f "deepseek-v4.1-flash" >/dev/null; do sleep 5; done; echo done',
    'tail -40 out; pgrep -f "deepseek-v4.1-flash" >/dev/null && echo RUNNING',
    "while pgrep -f opencode; do sleep 2; done",
    "while [ $(pgrep -fc server.py) -gt 0 ]; do sleep 1; done",
    'until [ -z "$(pgrep -f deepseek-v4.1-flash)" ]; do sleep 5; done',
    'while test -n "`pgrep -f codex`"; do sleep 5; done',
    """until [ -z "$(pgrep -f 'deepseek(-v4.1-flash)?')" ]; do sleep 5; done""",
    'echo "$(echo $(pgrep -fc codex))"',
    "until [ -z `pgrep -f app` ]; do sleep 5; done",
    "x=`pkill -f app`; echo $x",
    "# wait for the process's exit\nuntil [ -z \"$(pgrep -f deepseek-v4.1-flash)\" ]; do sleep 5; done",
    "pgrep -u tipu -f python3",
    "pgrep --full codex",
    "pkill -f opencode",
    "until ! pgrep -f 'deepseek-v4.1-flash.*$' >/dev/null; do sleep 5; done",
    "bash -c 'until ! pgrep -f codex; do sleep 1; done'",
])
def test_denies_a_pgrep_that_matches_itself(command):
    reason = gate.self_match_reason(command)
    assert reason.startswith("Blocked by the wait gate"), command
    assert "matches its own command line" in reason
    assert "kill -0" in reason


@pytest.mark.parametrize("command", [
    "pgrep opencode",
    "pgrep -x opencode",
    'pgrep -f "[d]eepseek-v4.1-flash"',
    "pgrep -f $PATTERN",
    "pgrep -f '^python3 server.py'",
    "while kill -0 4242 2>/dev/null; do sleep 5; done",
    "grep -n 'pgrep -f' scripts/",
    "echo pgrep -f opencode",
    "echo '$(pgrep -f opencode)'",
    "echo x'$(pgrep -f app)'",
    "echo ok # $(pgrep -f app)",
    'echo "\\$(pgrep -f app)"',
    "python3 - <<'EOF'\nprint('$(pgrep -f app)')\nEOF",
    "pkill -x opencode",
    'pgrep -f "${NAME}"',
    "pgrep -f '(a+)+b'; echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
])
def test_allows_a_pgrep_that_cannot_match_itself(command):
    assert gate.self_match_reason(command) == "", command


def _run_tool_hook(session_id, tool, tool_input):
    return subprocess.run(
        [sys.executable, "scripts/work_hook.py"], cwd=ROOT,
        input=json.dumps({"session_id": session_id, "hook_event_name": "PreToolUse",
                          "tool_name": tool, "cwd": ROOT, "transcript_path": "",
                          "tool_input": tool_input}),
        capture_output=True, text=True, timeout=120,
        env={**os.environ, "FRSHTY_DB": str(db._DB_PATH)},
    )


@pytest.mark.parametrize("tool,tool_input", [
    ("Bash", {"command": 'until ! pgrep -f "deepseek-v4.1-flash"; do sleep 5; done',
              "run_in_background": True}),
    ("Bash", {"command": "pgrep -f opencode && echo RUNNING"}),
    ("Monitor", {"command": "until ! pgrep -f codex; do sleep 1; done; echo gone",
                 "description": "codex exit", "timeout_ms": 600000}),
])
def test_hook_denies_a_self_matching_wait_in_a_task(tool, tool_input):
    r = _run_tool_hook(_task_session(), tool, tool_input)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert "matches its own command line" in out["permissionDecisionReason"]


def test_hook_allows_a_pid_wait_in_a_monitor():
    r = _run_tool_hook(_task_session(), "Monitor",
                       {"command": "while kill -0 4242; do sleep 1; done; echo gone",
                        "description": "pid exit", "timeout_ms": 600000})
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == ""
