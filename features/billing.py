import asyncio
import calendar
import re
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import httpx

import core.log as log
import core.state as state
from features import billcom
from features.timesheet import _fetch_worklogs

FIRE_TZ = ZoneInfo("America/Los_Angeles")
FIRE_HOUR = 19


def _billing_cfg(config: dict) -> dict:
    return config.get("billing") or {}


def _enabled(config: dict) -> bool:
    return bool(config.get("features", {}).get("billing")) and bool(_billing_cfg(config))


def get_client(config: dict) -> dict:
    b = _billing_cfg(config)
    return {
        "name": b.get("name", ""),
        "rate": b.get("rate", 0),
        "billing_freq": b.get("billing_freq", "weekly"),
        "billcom_customer_id": b.get("billcom_customer_id", ""),
        "invoice_prefix": b.get("invoice_prefix", "INV"),
        "extras": b.get("extras", {}) or {},
        "include_daily_descriptions": bool(b.get("include_daily_descriptions", False)),
        "billcom_available": billcom.has_credentials(),
        "timesheet_available": bool(config.get("features", {}).get("timesheet")) and bool(config.get("jira", {}).get("base_url")),
    }


def _load_entries() -> dict:
    return state.load("billing_entries")


def _save_entries(entries: dict) -> None:
    state.save("billing_entries", entries)


def list_entries(config: dict, month: str = "") -> list[dict]:
    entries = _load_entries()
    out = sorted(entries.values(), key=lambda e: e["date"])
    if month:
        out = [e for e in out if e["date"].startswith(month)]
    return out


def upsert_entries(config: dict, payload) -> dict:
    items = payload if isinstance(payload, list) else [payload]
    entries = _load_entries()
    count = 0
    for item in items:
        d = item.get("date")
        if not d:
            continue
        entries[d] = {
            "date": d,
            "type": item.get("type", "work"),
            "hours": item.get("hours", 8 if item.get("type", "work") == "work" else 0),
        }
        count += 1
    _save_entries(entries)
    return {"ok": True, "count": count}


def delete_entry(config: dict, date_str: str) -> dict:
    entries = _load_entries()
    if date_str in entries:
        del entries[date_str]
        _save_entries(entries)
    return {"ok": True}


def _local_invoices() -> dict:
    return state.load("billing_invoices")


def _save_local_invoices(invs: dict) -> None:
    state.save("billing_invoices", invs)


def _normalize_remote_invoice(inv: dict, customer_id: str) -> dict | None:
    cust = inv.get("customerId") or inv.get("customer", {}).get("id", "")
    if cust != customer_id:
        return None
    line_items = inv.get("invoiceLineItems", [])
    inv_date = inv.get("invoiceDate", inv.get("dueDate", ""))
    start, end = (None, None)
    if line_items and inv_date:
        descs = [li.get("description", "") for li in line_items if li.get("description")]
        start, end = billcom.parse_line_item_dates(descs, inv_date)
    hours = sum(li.get("quantity", 0) for li in line_items if li.get("description") != "AI tool")
    status = "paid" if inv.get("status") == "PAID_IN_FULL" else "pending"
    return {
        "id": inv["id"],
        "number": inv.get("invoiceNumber", ""),
        "date": inv_date,
        "start": start,
        "end": end,
        "hours": hours,
        "amount": inv.get("totalAmount", 0),
        "status": status,
        "source": "billcom",
    }


async def list_invoices(config: dict) -> list[dict]:
    b = _billing_cfg(config)
    customer_id = b.get("billcom_customer_id", "")
    result = []

    if billcom.has_credentials() and customer_id:
        try:
            data = await billcom.list_invoices()
            all_invoices = data.get("results", data) if isinstance(data, dict) else data
            for inv in all_invoices or []:
                norm = _normalize_remote_invoice(inv, customer_id)
                if norm:
                    result.append(norm)
        except httpx.HTTPError as e:
            log.emit("invoice_list_failed", f"bill.com list failed: {e}", meta={"err": str(e)[:200]})

    remote_ids = {r["id"] for r in result}
    remote_numbers = {r["number"] for r in result if r.get("number")}
    for inv in _local_invoices().values():
        if inv.get("id") in remote_ids or inv.get("number") in remote_numbers:
            continue
        result.append({**inv, "source": inv.get("source", "local")})

    result.sort(key=lambda r: r.get("date", ""), reverse=True)
    return result


async def next_invoice_number(config: dict) -> dict:
    b = _billing_cfg(config)
    prefix = b.get("invoice_prefix", "INV")
    customer_id = b.get("billcom_customer_id", "")

    max_num = 0
    for inv in _local_invoices().values():
        m = re.match(re.escape(prefix) + r"-(\d+)$", inv.get("number", ""))
        if m:
            max_num = max(max_num, int(m.group(1)))

    if billcom.has_credentials() and customer_id:
        try:
            data = await billcom.list_invoices()
            all_invoices = data.get("results", data) if isinstance(data, dict) else data
            pat = re.compile(re.escape(prefix) + r"-(\d+)$")
            for inv in all_invoices or []:
                inv_cust = inv.get("customerId") or inv.get("customer", {}).get("id", "")
                if inv_cust != customer_id:
                    continue
                m = pat.match(inv.get("invoiceNumber", ""))
                if m:
                    max_num = max(max_num, int(m.group(1)))
        except httpx.HTTPError:
            pass

    return {"number": f"{prefix}-{max_num + 1}"}


def _work_days_in(start: str, end: str) -> list[dict]:
    entries = _load_entries()
    return sorted(
        [e for e in entries.values() if e.get("type") == "work" and start <= e["date"] <= end],
        key=lambda e: e["date"],
    )


def _totals(config: dict, work_entries: list[dict]) -> tuple[float, float]:
    b = _billing_cfg(config)
    rate = b.get("rate", 0)
    hours = sum(e.get("hours", 8) for e in work_entries)
    amount = hours * rate + sum((b.get("extras") or {}).values())
    return hours, amount


def _build_line_items(config: dict, body: dict, work_entries: list[dict]) -> list[dict]:
    b = _billing_cfg(config)
    rate = b.get("rate", 0)
    hours = sum(e.get("hours", 8) for e in work_entries)
    items = []

    if b.get("include_daily_descriptions") and work_entries:
        worklogs_by_date = {}
        if config.get("features", {}).get("timesheet") and config.get("jira", {}).get("base_url"):
            try:
                wl = _fetch_worklogs(config, date.fromisoformat(body["start"]), date.fromisoformat(body["end"]))
                for d_str, logs in wl.items():
                    worklogs_by_date[d_str] = logs
            except Exception as e:
                log.emit("invoice_description_fallback", f"worklog fetch failed: {e}",
                    meta={"err": str(e)[:200]})

        any_wl = any(worklogs_by_date.get(e["date"]) for e in work_entries)
        if not any_wl:
            log.emit("invoice_description_fallback", "no worklogs; using date-only descriptions",
                meta={"start": body["start"], "end": body["end"]})

        for e in work_entries:
            d = date.fromisoformat(e["date"])
            pretty = f"{d.strftime('%A')}, {d.strftime('%B')} {d.day}"
            logs = worklogs_by_date.get(e["date"]) or []
            if logs:
                summary = "; ".join(f"{w['ticket']}: {w.get('summary','')}" for w in logs[:3])
                items.append({
                    "quantity": sum(w.get("hours", 0) for w in logs) or e.get("hours", 8),
                    "price": rate,
                    "description": f"{pretty} — {summary}",
                })
            else:
                items.append({
                    "quantity": e.get("hours", 8),
                    "price": rate,
                    "description": f"{pretty} · {e.get('hours', 8)}h",
                })
    else:
        if not work_entries:
            items.append({"quantity": hours, "price": rate, "description": "Consulting services"})
        else:
            for e in work_entries:
                d = date.fromisoformat(e["date"])
                items.append({
                    "quantity": e.get("hours", 8),
                    "price": rate,
                    "description": f"{d.strftime('%A')}, {d.strftime('%B')} {d.day}",
                })

    for label, price in (b.get("extras") or {}).items():
        items.append({"quantity": 1, "price": price, "description": label.replace("_", " ")})

    return items


def _overlapping(invoices: list[dict], start: str, end: str) -> dict | None:
    for inv in invoices:
        i_start, i_end = inv.get("start"), inv.get("end")
        if not i_start or not i_end:
            continue
        if i_start <= end and i_end >= start:
            return inv
    return None


async def create_invoice(config: dict, body: dict, source: str = "manual") -> dict:
    b = _billing_cfg(config)
    customer_id = b.get("billcom_customer_id", "")
    if not customer_id:
        raise ValueError("billing not configured: missing billcom_customer_id")
    if not billcom.has_credentials():
        raise ValueError("billcom not configured")

    start = body["start"]
    end = body["end"]

    existing = await list_invoices(config)
    conflict = _overlapping(existing, start, end)
    if conflict:
        raise OverlapError(conflict)

    work_entries = _work_days_in(start, end)
    hours, amount = _totals(config, work_entries)
    number = body.get("number") or (await next_invoice_number(config))["number"]
    due_date = body.get("date") or end

    line_items = _build_line_items(config, {"start": start, "end": end}, work_entries)

    result = await billcom.create_invoice(
        customer_id=customer_id,
        invoice_number=number,
        due_date=due_date,
        line_items=line_items,
    )

    inv_record = {
        "id": result["id"],
        "number": number,
        "date": due_date,
        "start": start,
        "end": end,
        "hours": hours,
        "amount": amount,
        "status": "pending",
        "source": "billcom",
    }

    locals_ = _local_invoices()
    locals_[result["id"]] = inv_record
    _save_local_invoices(locals_)

    try:
        verify = await billcom.get_invoice(result["id"])
        pdf_id = verify.get("invoicePdfId", "")
        payments = verify.get("payments", []) or []
        if verify.get("recordStatus") != "ACTIVE" or any(ch != "0" for ch in pdf_id) or payments:
            log.emit(
                "invoice_verify_warning",
                f"invoice {number} created but draft state unexpected",
                meta={"id": result["id"], "recordStatus": verify.get("recordStatus"),
                      "invoicePdfId": pdf_id, "payments": len(payments)},
            )
    except httpx.HTTPError as e:
        log.emit("invoice_verify_failed", f"verify get failed for {number}: {e}",
                 meta={"id": result["id"], "err": str(e)[:200]})

    base_url = config.get("_base_url", "")
    log.emit(
        "invoice_autogenerated" if source == "autogen" else "invoice_created",
        f"{'Autogenerated' if source == 'autogen' else 'Created'} invoice {number} for {start}..{end} (${amount:.0f})",
        links={"detail": f"{base_url}/billing"},
        meta={"number": number, "start": start, "end": end, "hours": hours, "amount": amount},
    )
    return inv_record


class OverlapError(Exception):
    def __init__(self, conflict: dict):
        super().__init__(f"overlaps with {conflict.get('number', conflict.get('id'))}")
        self.conflict = conflict


def _due_period(now: datetime, freq: str) -> tuple[date, date] | None:
    today = now.date()
    if now.hour < FIRE_HOUR:
        return None
    if freq == "weekly":
        if today.weekday() != 4:
            return None
        mon = today - timedelta(days=4)
        return mon, today
    if freq == "monthly":
        last_day = calendar.monthrange(today.year, today.month)[1]
        if today.day != last_day:
            return None
        return today.replace(day=1), today
    return None


def _next_fire(now: datetime, freq: str) -> datetime | None:
    today = now.date()
    if freq == "weekly":
        days_ahead = (4 - today.weekday()) % 7
        candidate = datetime.combine(today + timedelta(days=days_ahead), time(FIRE_HOUR), tzinfo=FIRE_TZ)
        if candidate <= now:
            candidate += timedelta(days=7)
        return candidate
    if freq == "monthly":
        last = calendar.monthrange(today.year, today.month)[1]
        candidate = datetime.combine(date(today.year, today.month, last), time(FIRE_HOUR), tzinfo=FIRE_TZ)
        if candidate <= now:
            ny, nm = (today.year + 1, 1) if today.month == 12 else (today.year, today.month + 1)
            last = calendar.monthrange(ny, nm)[1]
            candidate = datetime.combine(date(ny, nm, last), time(FIRE_HOUR), tzinfo=FIRE_TZ)
        return candidate
    return None


def get_schedule_status(config: dict) -> dict:
    enabled = _enabled(config)
    freq = _billing_cfg(config).get("billing_freq", "weekly")
    now = datetime.now(FIRE_TZ)
    next_fire = _next_fire(now, freq) if enabled else None

    autogen = state.load("billing_autogen")
    last_run = None
    if autogen:
        latest_key = max(autogen.keys())
        record = autogen[latest_key]
        d, start, end = latest_key.split("|")
        if record.get("number"):
            result = "created"
        elif record.get("skipped"):
            result = f"skipped:{record['skipped']}"
        elif record.get("error"):
            result = "error"
        else:
            result = "unknown"
        last_run = {
            "date": d, "start": start, "end": end, "result": result,
            "number": record.get("number"), "error": record.get("error"),
        }

    return {
        "enabled": enabled,
        "freq": freq,
        "next_fire": next_fire.isoformat() if next_fire else None,
        "last_run": last_run,
    }


def check(config: dict) -> None:
    if not _enabled(config):
        return
    b = _billing_cfg(config)
    if not b.get("billcom_customer_id"):
        return
    if not billcom.has_credentials():
        return

    now = datetime.now(FIRE_TZ)
    freq = b.get("billing_freq", "weekly")
    period = _due_period(now, freq)
    if not period:
        return
    start, end = period

    marker_key = f"{now.date().isoformat()}|{start.isoformat()}|{end.isoformat()}"
    autogen = state.load("billing_autogen")
    if marker_key in autogen:
        return

    work_entries = _work_days_in(start.isoformat(), end.isoformat())
    if not work_entries:
        autogen[marker_key] = {"skipped": "no_work"}
        state.save("billing_autogen", autogen)
        return

    try:
        existing = asyncio.run(list_invoices(config))
    except Exception as e:
        log.emit("invoice_autogen_failed", f"list_invoices failed: {e}",
            meta={"start": start.isoformat(), "end": end.isoformat(), "err": str(e)[:200]})
        autogen[marker_key] = {"error": str(e)[:200]}
        state.save("billing_autogen", autogen)
        return

    if _overlapping(existing, start.isoformat(), end.isoformat()):
        autogen[marker_key] = {"skipped": "exists"}
        state.save("billing_autogen", autogen)
        return

    try:
        number = asyncio.run(next_invoice_number(config))["number"]
        hours, amount = _totals(config, work_entries)
        inv = asyncio.run(create_invoice(config, {
            "number": number,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "date": end.isoformat(),
            "hours": hours,
            "amount": amount,
        }, source="autogen"))
        autogen[marker_key] = {"invoice_id": inv["id"], "number": inv["number"]}
    except Exception as e:
        autogen[marker_key] = {"error": str(e)[:200]}
        log.emit("invoice_autogen_failed", f"autogen failed {start}..{end}: {e}",
            meta={"start": start.isoformat(), "end": end.isoformat(), "err": str(e)[:200]})
    state.save("billing_autogen", autogen)
