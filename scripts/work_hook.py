import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.environ.get("FRSHTY_DB") or os.path.expanduser("~/.frshty/frshty.db")
BOARD_FILE = os.environ.get("FRSHTY_BOARD_FILE") or os.path.expanduser("~/.frshty/board.json")

QUESTION_DENY_REASON = (
    "Question recorded on the work board. The operator will answer it in a "
    "later message. Do not ask this question again. State that you are "
    "blocked on it and end your turn. Keep using AskUserQuestion for every "
    "later question or request: it is the only way one reaches the operator."
)

WRITE_TOOLS = ("Edit", "Write", "NotebookEdit", "MultiEdit")

NO_BOARD_REASON = (
    "Write blocked by the work-layer write gate: {path} is in a shared "
    "checkout that other agents are working in, and the address of the work "
    "board could not be resolved, so no worktree could be requested.\n\n"
    "The board writes its address to {board_file} at start-up. Tell the "
    "operator that file is missing or unreadable. Do not edit the shared "
    "checkout."
)

BOARD_ERROR_REASON = (
    "Write blocked by the work-layer write gate: {path} is in a shared "
    "checkout that other agents are working in, and the work board refused "
    "to create a worktree for this task.\n\n{detail}\n\n"
    "Do not edit the shared checkout. Report this to the operator."
)

WORKTREE_REASON = (
    "{what} blocked by the work-layer {gate}: {path} is in a shared checkout "
    "that other agents are working in. `git add -A` there would take their "
    "uncommitted work into your commit, and you may not run git checkout, "
    "git stash, git reset or git clean to clean it up.\n\n"
    "A worktree for this task is ready at {worktree} on branch {branch}. "
    "Change directory into it and make your change there. Leave the shared "
    "checkout untouched; read it if you need to."
)


def _board_urls(session_id: str) -> list[str]:
    """Every address this session could reach the board at, best first.

    The file the running server writes at start-up comes first, because it is
    the address the listener is on right now. The address the run recorded
    comes second: it is right for a run whose server is still up, and stale
    for a run whose server was restarted on another port, so it must never
    outrank the file."""
    found = []
    try:
        with open(BOARD_FILE) as f:
            published = str(json.load(f).get("base_url") or "")
        if published:
            found.append(published)
    except (OSError, ValueError):
        pass
    try:
        probe = sqlite3.connect(DB_PATH, timeout=0.25)
        try:
            probe.execute("PRAGMA busy_timeout = 250")
            row = probe.execute(
                "SELECT board_url FROM work_runs WHERE session_id = ?",
                (session_id,)).fetchone()
        finally:
            probe.close()
        if row and row[0] and str(row[0]) not in found:
            found.append(str(row[0]))
    except sqlite3.Error:
        pass
    return found


def _request_worktree(session_id: str, item_id: int, repo_path: str) -> dict:
    """Ask the board for a worktree of `repo_path` for this task.

    Returns the worktree, or {"error": ...}. A gate that cannot reach the
    board denies: a gate that opens on failure is not a gate."""
    bases = _board_urls(session_id)
    if not bases:
        return {"error": "no board address"}
    body = json.dumps({"repo_path": repo_path}).encode()
    failed = {"error": "no board address"}
    for base in bases:
        request = urllib.request.Request(
            f"{base.rstrip('/')}/api/work/items/{item_id}/worktree", data=body,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as e:
            # The board was reached and answered. That answer is final; trying
            # the next address would replace it with a connection error and
            # hide why the worktree was refused.
            return {"error": f"the board answered {e.code}: {e.read().decode()[:300]}"}
        except (urllib.error.URLError, OSError, ValueError) as e:
            failed = {"error": f"{base}: {type(e).__name__}: {e}"}
    return failed


def _existing_dir(path: str) -> str:
    """The nearest existing ancestor of `path`, "" when none exists.

    A new file has no directory of its own yet, and git has to be asked about
    a directory that exists."""
    current = os.path.dirname(os.path.abspath(path)) if path else ""
    while current and current != "/":
        if os.path.isdir(current):
            return current
        current = os.path.dirname(current)
    return ""


def _worktree_deny(session_id: str, item: dict, repo_path: str, target: str,
                   what: str, gate: str) -> str:
    """The message that sends a session out of a shared checkout, after making
    sure the worktree it names exists."""
    got = _request_worktree(session_id, item["id"], repo_path)
    if "error" in got:
        if got["error"] == "no board address":
            return NO_BOARD_REASON.format(path=target, board_file=BOARD_FILE)
        return BOARD_ERROR_REASON.format(path=target, detail=got["error"])
    return WORKTREE_REASON.format(what=what, gate=gate, path=target,
                                  worktree=got["path"], branch=got["branch"])


def _gate_write(session_id: str, tool_input: dict) -> str:
    """The deny reason for a write into a shared checkout, "" to allow.

    Fails closed on its own errors. A write outside any git checkout, or into
    a worktree, is allowed."""
    try:
        work_store = _bind_db()
        from services import work_worktree
        target = str(tool_input.get("file_path") or tool_input.get("notebook_path")
                     or tool_input.get("path") or "")
        if not target:
            return ""
        directory = _existing_dir(target)
        if not directory:
            return ""
        repo_path = work_worktree.shared_checkout(directory)
        if not repo_path:
            return ""
        item = work_worktree.session_item(session_id)
        if item is None or item["worktree_opt_out"]:
            return ""
        reason = _worktree_deny(session_id, item, repo_path, target,
                                "Write", "write gate")
        work_store.record_gate(session_id, "write_gate", "fail",
                               {"path": target[:300], "repo": repo_path})
        return reason
    except Exception as e:
        return (f"Write blocked by the work-layer write gate: the gate could "
                f"not decide ({type(e).__name__}: {e}). It denies rather than "
                f"letting a write into a shared checkout through. Report this "
                f"to the operator.")


def _bind_db():
    import core.db as db
    from services import work_store
    db._DB_PATH = Path(DB_PATH)
    try:
        db.query_one("SELECT 1 FROM work_runs LIMIT 1")
    except Exception:
        db.init(Path(DB_PATH), Path(__file__).resolve().parent.parent / "migrations")
    return work_store


def main() -> int:
    try:
        raw = sys.stdin.read()
        data = json.loads(raw) if raw.strip() else {}
        session_id = data.get("session_id") or ""
        kind = data.get("hook_event_name") or ""
        if not session_id or not kind:
            return 0
        probe = sqlite3.connect(DB_PATH, timeout=0.25)
        try:
            probe.execute("PRAGMA busy_timeout = 250")
            row = probe.execute(
                "SELECT 1 FROM work_runs WHERE session_id = ?", (session_id,)
            ).fetchone()
        finally:
            probe.close()
        if not row:
            return 0
        if kind == "PreToolUse":
            tool = data.get("tool_name") or ""
            if tool == "AskUserQuestion":
                work_store = _bind_db()
                if work_store.record_question(session_id, data.get("tool_input") or {}):
                    print(json.dumps({
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "deny",
                            "permissionDecisionReason": QUESTION_DENY_REASON,
                        }
                    }))
                return 0
            if tool in WRITE_TOOLS:
                reason = _gate_write(session_id, data.get("tool_input") or {})
                if reason:
                    print(json.dumps({
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "deny",
                            "permissionDecisionReason": reason,
                        }
                    }))
                return 0
            if tool == "Bash":
                command = (data.get("tool_input") or {}).get("command") or ""
                if "commit" not in command and "push" not in command:
                    return 0
                _bind_db()
                from services import work_launch
                cwd = data.get("cwd") or ""
                gate = {"decision": "allow", "reason": "not gated"}
                if "commit" in command:
                    gate = work_launch.gate_commit(session_id, command, cwd)
                    if gate.get("need_worktree"):
                        gate["reason"] = _worktree_deny(
                            session_id, {"id": gate["item_id"]},
                            gate["need_worktree"], gate["need_worktree"],
                            "Commit", "commit gate")
                if gate["decision"] == "allow" and "push" in command:
                    gate = work_launch.gate_push(session_id, command, cwd)
                if gate["decision"] == "deny":
                    print(json.dumps({
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "deny",
                            "permissionDecisionReason": gate["reason"],
                        }
                    }))
                return 0
            return 0
        transcript_path = data.get("transcript_path") or ""
        cursor = 0
        if transcript_path and os.path.isfile(transcript_path):
            cursor = os.path.getsize(transcript_path)
        payload = {
            "transcript_path": transcript_path,
            "transcript_cursor": cursor,
            "message": (data.get("message") or "")[:500],
            "reason": (data.get("reason") or "")[:200],
            "prompt": (data.get("prompt") or "")[:2000],
        }
        work_store = _bind_db()
        payload["last_assistant_message"] = work_store.last_assistant_text(transcript_path)[:300]
        work_store.record_event(session_id, kind, payload)
        if work_store.is_idle_stop(kind, payload):
            work_store.record_artifacts(session_id, transcript_path)
            work_store.maybe_autocontinue(session_id, transcript_path)
    except Exception:
        if os.environ.get("WORK_HOOK_DEBUG"):
            import traceback
            traceback.print_exc()
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
