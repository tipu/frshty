import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.environ.get("FRSHTY_DB") or os.path.expanduser("~/.frshty/frshty.db")


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
        transcript_path = data.get("transcript_path") or ""
        cursor = 0
        if transcript_path and os.path.isfile(transcript_path):
            cursor = os.path.getsize(transcript_path)
        payload = {
            "transcript_path": transcript_path,
            "transcript_cursor": cursor,
            "message": (data.get("message") or "")[:500],
            "reason": (data.get("reason") or "")[:200],
        }
        import core.db as db
        from services import work_store
        db.init(Path(DB_PATH), Path(__file__).resolve().parent.parent / "migrations")
        payload["last_assistant_message"] = work_store.last_assistant_text(transcript_path)[:300]
        work_store.record_event(session_id, kind, payload)
        if kind == "Stop":
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
