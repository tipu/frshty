"""Today-agent planner: pick an ordered list of goals to drive the day.

A "goal" is {ticket_key, target_state, rationale, source_bucket}. The planner
ranks candidates deterministically from the buckets already aggregated by
manager.staleness, then optionally asks Haiku to pick the top 3 with one-line
rationales. If the LLM is unavailable, the deterministic pick is used as-is.

The planner does NOT advance tickets — that is what the existing pipeline
(features.tickets.check + worker pool) does. The plan only declares focus.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import core.llm as llm
import core.log as log
import core.state as state
from manager import staleness
from manager.runner import _load_priorities


_BUCKET_PRIORITY: list[str] = [
    "merge_ready",
    "ready_to_submit",
    "pr_comments_needs_reply",
    "pr_failed_tickets",
    "in_review_no_ci",
    "pending_approvals_stuck",
    "stale_own_prs",
    "regressions_recent",
    "stale_unattended",
    "pickup_new",
]

_TARGET_STATE_FOR_BUCKET: dict[str, str] = {
    "merge_ready": "merged",
    "ready_to_submit": "in_review",
    "pr_comments_needs_reply": "merged",
    "pr_failed_tickets": "merged",
    "in_review_no_ci": "merged",
    "pending_approvals_stuck": "new",
    "stale_own_prs": "merged",
    "regressions_recent": "merged",
    "stale_unattended": "in_review",
    "pickup_new": "pr_ready",
}

_GOAL_LIMIT = 3
_LLM_CANDIDATE_LIMIT = 12


def _ticket_key_of(item: Any) -> str | None:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return (item.get("ticket_key") or item.get("key")
                or item.get("ticket") or item.get("id"))
    return None


def _summary_of(item: Any) -> str:
    if isinstance(item, dict):
        return str(item.get("summary") or item.get("title") or "")
    return ""


def _rank_candidates(candidate_set: dict, recent_keys: list[str]) -> list[dict]:
    """Flatten buckets into an ordered candidate list.

    Order: bucket priority, then continuity (touched yesterday), then bucket
    insertion order. Same ticket_key is only kept once (highest-priority bucket
    wins).
    """
    seen: set[str] = set()
    out: list[dict] = []
    recent = set(recent_keys or [])
    for bucket in _BUCKET_PRIORITY:
        items = candidate_set.get(bucket) or []
        promoted = [i for i in items if _ticket_key_of(i) in recent]
        rest = [i for i in items if _ticket_key_of(i) not in recent]
        for item in promoted + rest:
            tk = _ticket_key_of(item)
            if not tk or tk in seen:
                continue
            seen.add(tk)
            out.append({
                "ticket_key": tk,
                "target_state": _TARGET_STATE_FOR_BUCKET.get(bucket, "in_review"),
                "summary": _summary_of(item)[:120],
                "source_bucket": bucket,
                "continuity": tk in recent,
            })
    return out


def _recent_ticket_keys(instance_key: str, days: int = 1) -> list[str]:
    """Tickets the user touched recently — read from ticket activity timestamps.

    Best-effort: returns [] on any failure, deterministic ranking still works.
    """
    try:
        tickets = state.list_tickets()
    except Exception:
        return []
    if not tickets:
        return []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    out: list[tuple[float, str]] = []
    for k, v in tickets.items():
        if not isinstance(v, dict):
            continue
        ts = v.get("last_activity_at") or v.get("updated_at")
        if not ts:
            continue
        try:
            t = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            continue
        if t >= cutoff:
            out.append((t, k))
    out.sort(reverse=True)
    return [k for _, k in out]


def _llm_pick_top(candidates: list[dict], priorities_text: str,
                  instance_key: str) -> list[dict] | None:
    if not candidates:
        return []
    payload = json.dumps(candidates[:_LLM_CANDIDATE_LIMIT], indent=2)
    prompt = (
        "You are picking today's focus tickets for an autonomous dev agent.\n"
        f"Priorities for {instance_key}:\n{priorities_text[:2000]}\n\n"
        f"Ranked candidates (pre-sorted by deterministic rules):\n{payload}\n\n"
        f"Pick the top {_GOAL_LIMIT}. Stay close to the order unless a candidate\n"
        "is clearly weaker. Output strict JSON only:\n"
        '{"goals":[{"ticket_key":"...","target_state":"...",'
        '"rationale":"one short line"}]}'
    )
    try:
        out = llm.run_fast(prompt, timeout=60)
    except Exception as e:
        log.emit("today_planner_llm_error",
                 f"[{instance_key}] {type(e).__name__}: {e}")
        return None
    if not out:
        return None
    parsed = llm.extract_json(out) or {}
    goals = parsed.get("goals")
    if not isinstance(goals, list):
        return None
    by_key = {c["ticket_key"]: c for c in candidates}
    picked: list[dict] = []
    for g in goals[:_GOAL_LIMIT]:
        if not isinstance(g, dict):
            continue
        tk = g.get("ticket_key")
        if not tk or tk not in by_key:
            continue
        base = dict(by_key[tk])
        base["rationale"] = str(g.get("rationale") or base.get("summary", ""))[:200]
        if g.get("target_state"):
            base["target_state"] = g["target_state"]
        picked.append(base)
    return picked or None


def build_plan(instance_key: str, config: dict, *,
               use_llm: bool = True) -> dict:
    """Return the day's plan: ordered goals + bucket counts + timestamp."""
    thresholds = (config.get("manager") or {}).get("thresholds") or {}
    candidate_set = staleness.aggregate_all(
        instance_key, config=config, thresholds=thresholds
    )
    counts = {k: len(v or []) for k, v in candidate_set.items()}
    recent = _recent_ticket_keys(instance_key)
    ranked = _rank_candidates(candidate_set, recent)
    if not ranked:
        return {
            "instance_key": instance_key,
            "date": datetime.now(timezone.utc).date().isoformat(),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "goals": [],
            "bucket_counts": counts,
            "skipped": [],
        }

    goals: list[dict] | None = None
    if use_llm:
        priorities_text, _ = _load_priorities(config, instance_key)
        goals = _llm_pick_top(ranked, priorities_text, instance_key)

    if goals is None:
        goals = [
            {**c, "rationale": c.get("summary") or c["source_bucket"]}
            for c in ranked[:_GOAL_LIMIT]
        ]

    return {
        "instance_key": instance_key,
        "date": datetime.now(timezone.utc).date().isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "goals": goals,
        "bucket_counts": counts,
        "skipped": [],
    }
