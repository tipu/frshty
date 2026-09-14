"""Snapshot one autonomous-loop checkout and print it as JSON.

This file runs on the loop's own host. loopwatch feeds it to python3 over ssh
stdin, so it imports nothing outside the standard library and holds to Python
3.8 syntax. It only reads: the controller database is opened read-only, and a
host that refuses a read-only open is read from a copy instead.

usage: python3 - REPO [DETAIL [SUMMARY]]"""

import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime

SCHEMA = 1
RUN_COLUMNS = ("run_id", "created_by_run", "declared_by_run", "verified_by_run")
LABEL_COLUMNS = ("title", "name", "objective", "hypothesis", "reason", "subject",
                 "metric", "rule", "slug", "note", "kind", "provider")
SKIP_TABLES = ("state", "runs", "sqlite_sequence")
JOURNALS = ("", "-wal", "-journal")
MAX_LABEL = 200
MAX_ROWS = 12
MAX_NARRATIVE = 4000
MAX_STATE_DOC = 1500
MAX_STATE_HEAD = 2000
TICK_TAIL_BYTES = 400000
NARRATIVE_TAIL_BYTES = 4000000


def read_text(path, limit=200000):
    with open(path, "rb") as handle:
        return handle.read(limit).decode("utf-8", "replace")


def clip(text, limit):
    text = "" if text is None else str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


def section(text, heading):
    out = []
    taking = False
    for line in text.splitlines():
        if line.strip().lower() == heading.lower():
            taking = True
            continue
        if taking and line.startswith("## "):
            break
        if taking:
            out.append(line)
    return "\n".join(out).strip()


def first_hex(text):
    for token in text.split():
        stripped = token.strip()
        if len(stripped) == 64:
            try:
                int(stripped, 16)
            except ValueError:
                continue
            return stripped.lower()
    return None


def charter(repo):
    path = None
    for name in ("CHARTER.md", "CHARTER"):
        candidate = os.path.join(repo, name)
        if os.path.exists(candidate):
            path = candidate
            break
    if path is None:
        return {"path": None, "objective": "", "sha_ok": None}
    text = read_text(path)
    digest = None
    for name in (path + ".sha256", os.path.join(repo, "CHARTER.sha256")):
        if os.path.exists(name):
            digest = first_hex(read_text(name, 4096))
            break
    sha_ok = None
    if digest:
        with open(path, "rb") as handle:
            sha_ok = hashlib.sha256(handle.read()).hexdigest() == digest
    return {"path": os.path.basename(path),
            "objective": clip(section(text, "## Objective"), 1200),
            "sha_ok": sha_ok}


def stamp(db_path):
    """The size and modification time of the database and every journal beside it."""
    marks = []
    for suffix in JOURNALS:
        try:
            info = os.stat(db_path + suffix)
            marks.append((suffix, info.st_size, info.st_mtime_ns))
        except OSError:
            marks.append((suffix, None, None))
    return marks


def copy_aside(db_path):
    """Copy the database and its journals into this host's temp directory.

    The rollback journal has to travel with the database. Deleting it is what
    commits a transaction in that mode, so a database copied without it can
    hold pages no commit ever confirmed. sqlite rolls those back off the copy
    once the journal is there. The -shm is left behind on purpose: it is a
    scratch index sqlite rebuilds for the copy, and a stale one would describe
    a log that is no longer there."""
    tmp = tempfile.mkdtemp(prefix="loopwatch-")
    copy = os.path.join(tmp, "controller.db")
    for suffix in JOURNALS:
        source = db_path + suffix
        if os.path.exists(source):
            shutil.copy2(source, copy + suffix)
    return tmp


def connect(db_path, attempts=5):
    """Read a copy of the controller database. The live file is never opened.

    Opening the live file would add its -shm and -wal when the controller is
    not holding them, and would take a lock a controller write has to wait on.
    A copy taken while the controller writes can hold halves of two databases,
    so the database is copied again whenever it changed during the copy. A
    database that never settles is reported, never read: a snapshot that might
    be half a database is worth less than nothing."""
    tmp = None
    for attempt in range(attempts):
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)
        before = stamp(db_path)
        tmp = copy_aside(db_path)
        if stamp(db_path) == before:
            conn = sqlite3.connect(os.path.join(tmp, "controller.db"), timeout=10)
            conn.row_factory = sqlite3.Row
            return conn, tmp
    shutil.rmtree(tmp, ignore_errors=True)
    raise RuntimeError(
        "the database changed during every one of %d copies" % attempts)


def tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    return [r["name"] for r in rows]


def columns(conn, table):
    return [r[1] for r in conn.execute("PRAGMA table_info(%s)" % table).fetchall()]


def run_links(conn):
    """Which tables record the run that wrote each row, and under which column.

    The two loops keep different tables, so the link is discovered from the
    schema rather than named here."""
    links = []
    for table in tables(conn):
        if table in SKIP_TABLES or table.startswith("sqlite_"):
            continue
        cols = columns(conn, table)
        for column in RUN_COLUMNS:
            if column in cols:
                links.append((table, column, cols))
                break
    return links


def label(row, cols):
    parts = []
    for column in LABEL_COLUMNS:
        if column in cols and row[column] not in (None, ""):
            parts.append(str(row[column]))
            break
    if "status" in cols and row["status"] not in (None, ""):
        parts.append("[%s]" % row["status"])
    if not parts:
        parts.append("#%s" % row["id"] if "id" in cols else "row")
    return clip(" ".join(parts), MAX_LABEL)


def fold(labels):
    """One line per distinct label, in order, with how often the run wrote it."""
    order = []
    seen = {}
    for text in labels:
        if text not in seen:
            seen[text] = 0
            order.append(text)
        seen[text] += 1
    return [(text, seen[text]) for text in order]


def wrote(conn, links, run_id):
    out = {}
    for table, column, cols in links:
        rows = conn.execute(
            "SELECT * FROM %s WHERE %s=? ORDER BY rowid" % (table, column),
            (run_id,)).fetchall()
        if not rows:
            continue
        folded = fold([label(r, cols) for r in rows])
        kept, dropped = folded[:MAX_ROWS], folded[MAX_ROWS:]
        out[table] = {
            "count": len(rows),
            "rows": [text if n == 1 else "%s \u00d7%d" % (text, n) for text, n in kept],
            "hidden": sum(n for _, n in dropped),
        }
    return out


def seconds_between(start, end):
    if not start or not end:
        return None
    try:
        a = datetime.strptime(start[:19], "%Y-%m-%d %H:%M:%S")
        b = datetime.strptime(end[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return int((b - a).total_seconds())


def result_file(repo, run_id):
    path = os.path.join(repo, "var", "controller-runs", str(run_id), "result.json")
    if not os.path.exists(path):
        return None
    try:
        data = json.loads(read_text(path, 400000))
    except ValueError as exc:
        return {"_unreadable": "%s: %s" % (type(exc).__name__, exc)}
    if isinstance(data, dict) and isinstance(data.get("state_doc"), str):
        data["state_doc"] = clip(data["state_doc"], MAX_STATE_DOC)
    return data


def narrative(repo, run_id):
    """What the session itself said, plus how many tool calls it made.

    The transcript is stream-json, one object per line, and a session that was
    cut off has no final object at all. Reading it line by line keeps a
    truncated transcript readable and keeps the whole file out of memory."""
    path = os.path.join(repo, "var", "controller-runs", str(run_id), "claude.json")
    if not os.path.exists(path):
        return None
    size = os.path.getsize(path)
    final = None
    last_text = None
    tools = {}
    lines = 0
    turns = None
    with open(path, "rb") as handle:
        if size > NARRATIVE_TAIL_BYTES:
            handle.seek(size - NARRATIVE_TAIL_BYTES)
            handle.readline()
        for raw in handle:
            lines += 1
            try:
                obj = json.loads(raw.decode("utf-8", "replace"))
            except ValueError:
                continue
            if not isinstance(obj, dict):
                continue
            if obj.get("type") == "result":
                final = obj.get("result")
                turns = obj.get("num_turns")
            message = obj.get("message")
            if obj.get("type") == "assistant" and isinstance(message, dict):
                for block in message.get("content") or []:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") == "text" and block.get("text", "").strip():
                        last_text = block["text"]
                    elif block.get("type") == "tool_use":
                        name = str(block.get("name") or "tool")
                        tools[name] = tools.get(name, 0) + 1
    return {"final": clip(final if final else last_text, MAX_NARRATIVE),
            "truncated": final is None,
            "tools": tools,
            "turns": turns,
            "lines": lines,
            "bytes": size}


def ticks(repo):
    """The controller's own log of each tick, indexed by run.

    It carries what the controller refused from a session, which no table
    records."""
    path = os.path.join(repo, "var", "tick.log")
    if not os.path.exists(path):
        return {}, []
    size = os.path.getsize(path)
    with open(path, "rb") as handle:
        if size > TICK_TAIL_BYTES:
            handle.seek(size - TICK_TAIL_BYTES)
            handle.readline()
        raw = handle.read().decode("utf-8", "replace")
    by_run = {}
    recent = []
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if not isinstance(entry, dict):
            continue
        recent.append(entry)
        run_id = entry.get("run_id")
        if isinstance(run_id, int):
            by_run[str(run_id)] = entry
    return by_run, recent[-5:]


def human_notes(conn, limit=10):
    """Steers the operator has already sent, and the run that first read one.

    bin/note.sh prepends 'HUMAN NOTE <stamp>.' to the state document, and a run
    reads the state at its base_version, so the first run whose base_version
    reaches the note's version is the run that was told."""
    rows = conn.execute(
        "SELECT version, created_at, substr(doc, 1, 600) AS head FROM state "
        "WHERE doc LIKE 'HUMAN NOTE%' ORDER BY version DESC LIMIT ?",
        (limit,)).fetchall()
    out = []
    for row in rows:
        head = row["head"].split("\n")[0]
        text = head
        marker = ". "
        if head.startswith("HUMAN NOTE") and marker in head:
            text = head.split(marker, 1)[1]
        picked = conn.execute(
            "SELECT MIN(id) AS id FROM runs WHERE base_version >= ?",
            (row["version"],)).fetchone()["id"]
        out.append({"version": row["version"], "at": row["created_at"],
                    "text": clip(text, 600), "read_by_run": picked})
    return out


def snapshot(repo, detail, summary):
    db_path = os.path.join(repo, "var", "controller.db")
    if not os.path.isdir(repo):
        return {"ok": False, "error": "no such directory: %s" % repo}
    if not os.path.exists(db_path):
        return {"ok": False, "error": "no controller database at %s" % db_path}
    conn, tmp = connect(db_path)
    try:
        return collect(conn, repo, db_path, detail, summary)
    finally:
        conn.close()
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


def collect(conn, repo, db_path, detail, summary):
    now = conn.execute("SELECT datetime('now') AS t").fetchone()["t"]
    links = run_links(conn)
    tick_by_run, tick_recent = ticks(repo)

    has_tasks = "tasks" in tables(conn)
    if has_tasks:
        sql = ("SELECT r.*, t.kind AS task_kind, t.objective AS objective, "
               "t.acceptance AS acceptance, t.status AS task_status, "
               "t.attempts AS attempts FROM runs r "
               "LEFT JOIN tasks t ON t.id = r.task_id "
               "ORDER BY r.id DESC LIMIT ?")
    else:
        sql = "SELECT r.* FROM runs r ORDER BY r.id DESC LIMIT ?"
    rows = conn.execute(sql, (detail + summary,)).fetchall()

    runs = []
    for index, row in enumerate(rows):
        keys = row.keys()
        run = {
            "id": row["id"],
            "task_id": row["task_id"],
            "role": row["role"],
            "kind": row["task_kind"] if "task_kind" in keys else None,
            "objective": clip(row["objective"] if "objective" in keys else "", 400),
            "task_status": row["task_status"] if "task_status" in keys else None,
            "attempts": row["attempts"] if "attempts" in keys else None,
            "base_version": row["base_version"],
            "result_version": row["result_version"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "heartbeat_at": row["heartbeat_at"],
            "exit_reason": row["exit_reason"],
            "cost_cents": row["cost_cents"],
            "seconds": seconds_between(row["started_at"],
                                       row["ended_at"] or row["heartbeat_at"]),
            "wrote": wrote(conn, links, row["id"]),
            "tick": tick_by_run.get(str(row["id"])),
            "detail": index < detail,
        }
        if run["detail"]:
            run["acceptance"] = clip(row["acceptance"] if "acceptance" in keys else "", 600)
            run["result"] = result_file(repo, row["id"])
            run["narrative"] = narrative(repo, row["id"])
        runs.append(run)

    halts = [dict(r) for r in conn.execute(
        "SELECT * FROM halts WHERE resolved_at IS NULL ORDER BY id").fetchall()] \
        if "halts" in tables(conn) else []
    active = [dict(r) for r in conn.execute(
        "SELECT id, task_id, role, pid, started_at, heartbeat_at, deadline_at "
        "FROM runs WHERE ended_at IS NULL ORDER BY id").fetchall()]

    counts = {}
    for table in tables(conn):
        if table.startswith("sqlite_"):
            continue
        counts[table] = conn.execute("SELECT count(*) AS n FROM %s" % table).fetchone()["n"]
    task_status = {}
    if has_tasks:
        for row in conn.execute(
                "SELECT status, count(*) AS n FROM tasks GROUP BY status").fetchall():
            task_status[row["status"]] = row["n"]

    state_row = conn.execute(
        "SELECT version, created_at, substr(doc, 1, ?) AS head FROM state "
        "ORDER BY version DESC LIMIT 1", (MAX_STATE_HEAD,)).fetchone()

    return {
        "ok": True,
        "schema": SCHEMA,
        "repo": repo,
        "db": db_path,
        "now": now,
        "charter": charter(repo),
        "halts": halts,
        "active": active,
        "counts": counts,
        "task_status": task_status,
        "runs": runs,
        "notes": human_notes(conn),
        "ticks": tick_recent,
        "state": {"version": state_row["version"], "at": state_row["created_at"],
                  "head": state_row["head"]} if state_row else None,
    }


def main(argv):
    if len(argv) < 2:
        print(json.dumps({"ok": False, "error": "usage: probe REPO [DETAIL [SUMMARY]]"}))
        return 2
    repo = argv[1]
    detail = int(argv[2]) if len(argv) > 2 else 5
    summary = int(argv[3]) if len(argv) > 3 else 15
    try:
        out = snapshot(repo, detail, summary)
    except Exception as exc:  # the caller renders the failure, it never guesses
        out = {"ok": False, "error": "%s: %s" % (type(exc).__name__, exc)}
    sys.stdout.write(json.dumps(out))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
