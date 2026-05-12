"""Periodic snapshot of bill.com remote invoices so /today doesn't have to
make a live API call on every 30s poll. Distinct from features.billing's
'billing_invoices' kv key, which only tracks invoices that frshty itself
created — this one mirrors the full remote list including invoices created
directly on bill.com."""
import asyncio
from datetime import datetime, timezone

import core.log as log
import core.state as state


_STATE_KEY = "billing_invoices_remote"


def refresh(config: dict) -> list[dict]:
    if not config.get("features", {}).get("billing"):
        return []
    b = config.get("billing") or {}
    if not b.get("billcom_customer_id"):
        return []
    from features import billing
    try:
        invoices = asyncio.run(billing.list_invoices(config)) or []
    except Exception as e:
        log.emit("billing_snapshot_refresh_failed",
                 f"list_invoices crashed: {type(e).__name__}: {e}")
        existing = state.load(_STATE_KEY) or {}
        return list(existing.get("items") or [])
    state.save(_STATE_KEY, {"items": invoices, "refreshed_at": _now()})
    return invoices


def cached() -> list[dict]:
    blob = state.load(_STATE_KEY) or {}
    return list(blob.get("items") or [])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
