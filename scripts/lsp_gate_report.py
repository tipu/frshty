"""Measure the LSP gate and the cost of tasks before and after a change.

The gate part reads the claude transcripts under the given roots. For every
grep the LSP gate refused or removed from a command, it looks at the next
tool call. A ToolSearch that
loads LSP is skipped, because a session must load LSP before its first LSP
call. The next call is "lsp", "grep" (a Bash grep or the Grep tool),
"other", or "end" when the session stopped.

The cost part reads the work_task rows of claude_invocations, which the
board sweep writes for every task run, and sums the cost and the duration of
the runs of each task.

  python3 scripts/lsp_gate_report.py --split 2026-10-30T12:00:00+00:00
  python3 scripts/lsp_gate_report.py --split ... --check

--check exits 1 when, after the split, at least --min-denials refusals were
followed by an LSP call less than half the time, or when a finished claude
task run has no cost. It exits 0 otherwise.
"""
import argparse
import json
import os
import sqlite3
import statistics
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.lsp_gate as lsp_gate  # noqa: E402  (needs the path above)

DB_PATH = os.environ.get("FRSHTY_DB") or os.path.join(
    os.environ.get("FRSHTY_ROOT") or os.path.expanduser("~/.frshty"), "frshty.db")
DEFAULT_ROOTS = [os.path.expanduser("~/.claude/projects")]
MARKERS = (b"Blocked by the LSP gate", b"[LSP gate] The grep for")


def _parse_time(text: str) -> datetime:
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def _text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(str(b.get("text") or "") for b in content if isinstance(b, dict))
    return ""


def _is_grep(name: str, tool_input: dict) -> bool:
    if name == "Grep":
        return True
    return name == "Bash" and bool(lsp_gate._searches(str(tool_input.get("command") or "")))


def _kind(name: str, tool_input: dict) -> str:
    if name == "LSP":
        return "lsp"
    return "grep" if _is_grep(name, tool_input) else "other"


def transcript_denials(path: str) -> list[dict]:
    """The refused greps of one transcript, each with the kind of the next
    tool call."""
    uses: list[tuple[str, str, dict, str]] = []
    refused: set[str] = set()
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if '"tool_use"' not in line and '"tool_result"' not in line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            message = entry.get("message") if isinstance(entry, dict) else None
            content = message.get("content") if isinstance(message, dict) else None
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "tool_use":
                    raw_input = block.get("input")
                    tool_input = raw_input if isinstance(raw_input, dict) else {}
                    uses.append((str(block.get("id")), str(block.get("name")), tool_input,
                                 str(entry.get("timestamp") or "")))
                elif block.get("type") == "tool_result":
                    text = _text(block.get("content"))
                    if (block.get("is_error") and lsp_gate._REFUSED.match(text.strip())) or \
                            lsp_gate._REMOVED.search(text):
                        refused.add(str(block.get("tool_use_id")))
    found = []
    for index, (tool_id, _, _, stamp) in enumerate(uses):
        if tool_id not in refused or not stamp:
            continue
        kind = "end"
        for _, name, tool_input, _ in uses[index + 1:]:
            if name == "ToolSearch" and "LSP" in str(tool_input.get("query") or ""):
                continue
            kind = _kind(name, tool_input)
            break
        found.append({"id": tool_id, "at": _parse_time(stamp), "next": kind, "path": path})
    return found


def denials(roots: list[str], since: datetime) -> list[dict]:
    seen: dict[str, dict] = {}
    cutoff = since.timestamp()
    for root in roots:
        for folder, _, files in os.walk(root):
            for name in files:
                path = os.path.join(folder, name)
                if not name.endswith(".jsonl"):
                    continue
                try:
                    if os.path.getmtime(path) < cutoff:
                        continue
                    with open(path, "rb") as f:
                        data = f.read()
                    if not any(marker in data for marker in MARKERS):
                        continue
                    for found in transcript_denials(path):
                        if found["at"] >= since:
                            seen.setdefault(found["id"], found)
                except OSError:
                    continue
    return list(seen.values())


def gate_summary(found: list[dict]) -> dict:
    counts = {kind: sum(1 for d in found if d["next"] == kind)
              for kind in ("lsp", "grep", "other", "end")}
    total = len(found)
    return {"denials": total, "sessions": len({d["path"] for d in found}), **counts,
            "lsp_share": round(counts["lsp"] / total, 3) if total else None}


def task_rows(db_path: str, since: datetime) -> list[sqlite3.Row]:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return connection.execute(
            "SELECT job_key, model, started_at, status, duration_ms, cost_usd, "
            "input_tokens, output_tokens FROM claude_invocations "
            "WHERE function_name = 'work_task' AND job_key NOT LIKE 'BOOT-%' "
            "AND started_at >= ?", (since.isoformat(),)).fetchall()
    finally:
        connection.close()


def tasks(rows: list[sqlite3.Row]) -> list[dict]:
    by_task: dict[str, dict] = {}
    for row in rows:
        task = by_task.setdefault(row["job_key"], {
            "task": row["job_key"], "started": _parse_time(row["started_at"]),
            "runs": 0, "cost_usd": 0.0, "duration_ms": 0, "priced": True})
        task["runs"] += 1
        task["started"] = min(task["started"], _parse_time(row["started_at"]))
        task["duration_ms"] += int(row["duration_ms"] or 0)
        if row["cost_usd"] is None:
            task["priced"] = False
        else:
            task["cost_usd"] += float(row["cost_usd"])
    return list(by_task.values())


def task_summary(found: list[dict]) -> dict:
    priced = [t for t in found if t["priced"]]
    costs = [t["cost_usd"] for t in priced]
    minutes = [t["duration_ms"] / 60000 for t in found]
    return {"tasks": len(found), "priced": len(priced),
            "median_cost_usd": round(statistics.median(costs), 2) if costs else None,
            "mean_cost_usd": round(statistics.mean(costs), 2) if costs else None,
            "median_minutes": round(statistics.median(minutes), 1) if minutes else None,
            "mean_minutes": round(statistics.mean(minutes), 1) if minutes else None}


def unpriced_runs(rows: list[sqlite3.Row], split: datetime) -> list[str]:
    return [row["job_key"] for row in rows
            if _parse_time(row["started_at"]) >= split and row["status"] != "running"
            and str(row["model"]).startswith("claude-") and row["cost_usd"] is None]


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure the LSP gate and task cost around a change.")
    parser.add_argument("--split", required=True, help="ISO time of the change")
    parser.add_argument("--since", help="ISO start of the window; default 7 days before --split")
    parser.add_argument("--root", action="append", help="transcript root; repeatable")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--min-denials", type=int, default=10)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    split = _parse_time(args.split)
    since = _parse_time(args.since) if args.since else split - timedelta(days=7)

    found = denials(args.root or DEFAULT_ROOTS, since)
    rows = task_rows(args.db, since)
    all_tasks = tasks(rows)
    report = {
        "split": split.isoformat(), "since": since.isoformat(),
        "gate": {"before": gate_summary([d for d in found if d["at"] < split]),
                 "after": gate_summary([d for d in found if d["at"] >= split])},
        "tasks": {"before": task_summary([t for t in all_tasks if t["started"] < split]),
                  "after": task_summary([t for t in all_tasks if t["started"] >= split])},
    }
    print(json.dumps(report, indent=2))
    if not args.check:
        return 0
    failures = []
    after = report["gate"]["after"]
    if after["denials"] >= args.min_denials and after["lsp_share"] < 0.5:
        failures.append(f"only {after['lsp']} of {after['denials']} refused greps "
                        "were followed by an LSP call")
    elif after["denials"] < args.min_denials:
        print(f"gate: {after['denials']} refusals after the split, fewer than "
              f"{args.min_denials}; the share is not judged yet")
    missing = unpriced_runs(rows, split)
    if missing:
        failures.append(f"{len(missing)} finished claude task runs have no cost: "
                        + ", ".join(sorted(set(missing))[:10]))
    for failure in failures:
        print("FAIL: " + failure)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
