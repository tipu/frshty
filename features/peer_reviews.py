from datetime import datetime, timezone

import core.log as log
import core.state as state
from features.platforms import make_platform


def refresh(config: dict) -> list[dict]:
    """Refresh the peer_reviews cache by calling the platform once and saving
    the result to state. Read by manager.staleness.peer_pr_reviews so /today
    doesn't have to hit the source-control API on every 30s poll."""
    platform = make_platform(config)
    fn = getattr(platform, "list_pending_reviews_for_me", None)
    if not fn:
        state.save("peer_reviews", {"items": [], "refreshed_at": _now()})
        return []
    try:
        prs = fn() or []
    except Exception as e:
        log.emit("peer_reviews_refresh_failed",
                 f"list_pending_reviews_for_me crashed: {type(e).__name__}: {e}")
        existing = state.load("peer_reviews") or {}
        return existing.get("items") or []
    items = [{
        "repo": pr.get("repo", ""),
        "pr_id": pr.get("id"),
        "title": (pr.get("title") or "")[:140],
        "author": pr.get("author", ""),
        "url": pr.get("url", ""),
        "created_on": pr.get("created_on", ""),
        "updated_on": pr.get("updated_on", ""),
    } for pr in prs]
    state.save("peer_reviews", {"items": items, "refreshed_at": _now()})
    return items


def cached() -> list[dict]:
    """Return the cached peer-review list (empty list if never polled)."""
    blob = state.load("peer_reviews") or {}
    return blob.get("items") or []


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
