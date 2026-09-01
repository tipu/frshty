import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.codex_session as codex_session  # noqa: E402

DB_PATH = os.environ.get("FRSHTY_DB") or os.path.expanduser("~/.frshty/frshty.db")


def _bind_db():
    import core.db as db
    from services import work_store
    db._DB_PATH = Path(DB_PATH)
    try:
        db.query_one("SELECT 1 FROM work_runs LIMIT 1")
    except Exception:
        db.init(Path(DB_PATH), Path(__file__).resolve().parent.parent / "migrations")
    return work_store


def main(argv: list[str]) -> int:
    """Codex notify program for a work-layer session.

    Codex runs it as `python3 codex_notify.py <work-session-id> <json>` after
    every agent turn. It is the codex analog of the Claude Stop hook: it
    records the turn, the last assistant message and the codex thread id, then
    runs the same done/question/autocontinue decision the Claude hook runs.
    """
    try:
        session_id = argv[1] if len(argv) > 1 else ""
        data = json.loads(argv[2]) if len(argv) > 2 else {}
        if not session_id or data.get("type") != "agent-turn-complete":
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
        message = (data.get("last-assistant-message") or "").strip()
        thread_id = data.get("thread-id") or ""
        work_store = _bind_db()
        transcript = codex_session.rollout_path(thread_id)
        work_store.record_agent_session(session_id, thread_id)
        work_store.record_event(session_id, "Stop", {
            "reason": "Codex finished its turn",
            "last_assistant_message": message[:300],
            "transcript_path": transcript,
        })
        work_store.record_artifacts(session_id, transcript, texts=[message])
        work_store.maybe_autocontinue(session_id, transcript, tail=message)
    except Exception:
        if os.environ.get("WORK_HOOK_DEBUG"):
            import traceback
            traceback.print_exc()
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
