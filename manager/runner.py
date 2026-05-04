"""Manager daily-digest runner."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.log as log
from core.claude_runner import run_haiku
from core.config import get_repos
from manager import staleness
from manager.prompts import NARRATE_DIGEST


_PROMPT_BUDGET = 30000
_DEFAULT_PRIORITY_FALLBACK = (
    "(no priorities.md found — default order: stale own PRs, "
    "stale tickets, stuck in_review, regressions, pending approvals)"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_priorities_path(config: dict) -> Path | None:
    explicit = (config.get("manager") or {}).get("priorities_file")
    if explicit:
        return Path(explicit)
    ws = config.get("workspace") or {}
    root = ws.get("root")
    if root:
        candidate = Path(root) / "priorities.md"
        if candidate.is_file():
            return candidate
    repos = get_repos(config)
    if repos:
        candidate = Path(repos[0]["path"]) / "priorities.md"
        if candidate.is_file():
            return candidate
    return None


def _load_priorities(config: dict) -> tuple[str, str]:
    path = _resolve_priorities_path(config)
    if path and path.is_file():
        try:
            text = path.read_text(encoding="utf-8")
            return text, hashlib.sha256(text.encode("utf-8")).hexdigest()
        except (OSError, UnicodeDecodeError) as e:
            log.emit("manager_priorities_unreadable",
                     f"Could not read {path}: {type(e).__name__}: {e}")
    return _DEFAULT_PRIORITY_FALLBACK, ""


def _summarize(candidate_set: dict, max_per_bucket: int = 10) -> dict:
    return {k: v[:max_per_bucket] for k, v in candidate_set.items()}


def run_daily_digest(instance_key: str, config: dict) -> dict | None:
    priorities_text, priorities_hash = _load_priorities(config)
    candidate_set = staleness.aggregate_all(instance_key)
    counts = {k: len(v) for k, v in candidate_set.items()}

    if sum(counts.values()) == 0:
        body_md = "No flags today."
        db.execute(
            "INSERT INTO manager_digest"
            "(instance_key, generated_at, priorities_hash, body_md, candidate_counts_json)"
            " VALUES (?, ?, ?, ?, ?)",
            (instance_key, _now(), priorities_hash, body_md, json.dumps(counts)),
        )
        log.emit("manager_digest_clean",
                 f"[{instance_key}] no flags today",
                 meta={"counts": counts})
        return {"body_md": body_md, "counts": counts, "empty": True}

    summary = _summarize(candidate_set)
    summary_json = json.dumps(summary, indent=2)
    if len(summary_json) > _PROMPT_BUDGET:
        summary = _summarize(candidate_set, max_per_bucket=3)
        summary_json = json.dumps(summary, indent=2)

    prompt = NARRATE_DIGEST.format(
        priorities_text=priorities_text[:5000],
        candidate_summary_json=summary_json[:_PROMPT_BUDGET],
    )
    body_md = run_haiku(prompt, timeout=180)
    if not body_md:
        log.emit("manager_digest_failed",
                 f"[{instance_key}] haiku returned empty",
                 meta={"counts": counts})
        return None

    db.execute(
        "INSERT INTO manager_digest"
        "(instance_key, generated_at, priorities_hash, body_md, candidate_counts_json)"
        " VALUES (?, ?, ?, ?, ?)",
        (instance_key, _now(), priorities_hash, body_md, json.dumps(counts)),
    )
    log.emit(
        "manager_digest",
        f"[{instance_key}] daily digest: " + ", ".join(f"{k}={v}" for k, v in counts.items() if v),
        meta={"counts": counts},
    )
    return {"body_md": body_md, "counts": counts}


def latest(instance_key: str) -> dict | None:
    row = db.query_one(
        "SELECT id, generated_at, priorities_hash, body_md, candidate_counts_json"
        " FROM manager_digest WHERE instance_key=?"
        " ORDER BY generated_at DESC LIMIT 1",
        (instance_key,),
    )
    if not row:
        return None
    out = dict(row)
    try:
        out["candidate_counts"] = json.loads(out.pop("candidate_counts_json") or "{}")
    except (json.JSONDecodeError, ValueError):
        out["candidate_counts"] = {}
        out.pop("candidate_counts_json", None)
    return out
