import json
import os
import subprocess
import sys

import pytest

import core.worktree_prune_gate as gate

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.mark.parametrize("command", [
    "git worktree prune",
    "git worktree list | grep scratchpad; git worktree prune; git worktree list | grep -c scratchpad",
    "git worktree prune && S=/tmp/s && mkdir -p $S && git worktree add -q --detach $S/m origin/x",
    "git -C /home/u/repo worktree prune -v",
    "git -c core.x=1 --git-dir=/r/.git worktree prune --expire now",
    "/usr/bin/git worktree prune",
    "cd /r\ngit worktree prune",
    "git wo''rktree prune",
    "sh -c 'git worktree prune'",
    "bash -lc \"cd /r && git worktree prune -v\"",
    "git worktree prune --verbose",
    "docker exec -w /app frshty-personal sh -c 'git -C /r worktree prune'",
    "ssh host 'cd ~/dev/r; git worktree prune'",
    "eval \"git worktree prune\"",
    "# inspect registered worktrees\ngit worktree prune",
    "echo \"$(git worktree prune --verbose)\"",
    "echo $(git worktree prune)",
    "echo \"`git worktree prune`\"",
    "x=`git worktree prune`",
    "git worktree prune --dry-run --no-dry-run",
    "git worktree prune -n --no-dry",
    "git worktree prune -- --dry-run",
    "git worktree prune --dry-run",
    "git worktree prune -nv",
    "git worktree prune # --dry-run",
    "git 2>/dev/null worktree prune",
    "git worktree prune > -n",
    "(git worktree prune); git worktree prune -n",
    "git worktree pru\\\nne",
    "git >& /dev/null worktree prune",
    "timeout 60 git worktree prune",
    "git -C $(pwd) worktree prune",
    "git worktree $'prune'",
    "eval 'git worktree' prune",
    "rg 'git worktree prune' core/git_util.py",
])
def test_denies_a_prune(command):
    assert gate.bash_reason(command) == gate.DENY_REASON


@pytest.mark.parametrize("command", [
    "echo \"$(git worktree list)\"",
    "git worktree list --porcelain",
    "git worktree remove /tmp/s/m",
    "grep -rn \"worktree prune\" core/",
    "git log --grep prune",
    "git remote prune origin",
    "",
])
def test_allows_everything_else(command):
    assert gate.bash_reason(command) == ""


def test_unbalanced_quotes_still_deny():
    assert gate.bash_reason("git worktree prune; echo 'open") == gate.DENY_REASON
    assert gate.bash_reason("git worktree list 2>&1 | grep prunable") == ""
    assert gate.bash_reason("git worktree list; echo 'open") == ""


def _run_hook(command, session_id):
    return subprocess.run(
        [sys.executable, "scripts/work_hook.py"], cwd=ROOT,
        input=json.dumps({"session_id": session_id, "hook_event_name": "PreToolUse",
                          "tool_name": "Bash", "cwd": ROOT,
                          "tool_input": {"command": command}}),
        capture_output=True, text=True, timeout=120,
    )


def test_hook_denies_a_prune_in_a_session_that_is_not_a_task():
    r = _run_hook("git worktree prune", "not-a-work-run")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert out["permissionDecisionReason"] == gate.DENY_REASON


def test_hook_lets_a_worktree_list_through():
    r = _run_hook("git worktree list --porcelain", "not-a-work-run")
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == ""
