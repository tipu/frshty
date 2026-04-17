import os
import re
import time
from datetime import date

import httpx

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

_MONTH_PAT = "|".join(sorted(MONTHS.keys(), key=len, reverse=True))
_DATE_RE = re.compile(rf"({_MONTH_PAT})\s+(\d{{1,2}})", re.IGNORECASE)
_RANGE_RE = re.compile(rf"({_MONTH_PAT})\s+(\d{{1,2}})\s*[-–]\s*(\d{{1,2}})", re.IGNORECASE)
_RANGE_CROSS_RE = re.compile(rf"({_MONTH_PAT})\s+(\d{{1,2}})\s*[-–]\s*({_MONTH_PAT})\s+(\d{{1,2}})", re.IGNORECASE)

BASE = "https://gateway.prod.bill.com/connect/v3"
SESSION_TTL = 1800

_session_id = None
_session_ts = 0


def has_credentials() -> bool:
    return bool(os.environ.get("BILLCOM_DEV_KEY") and os.environ.get("BILLCOM_EMAIL")
                and os.environ.get("BILLCOM_PASSWORD") and os.environ.get("BILLCOM_ORG_ID"))


def parse_line_item_dates(descriptions, invoice_date_str):
    if isinstance(descriptions, str):
        descriptions = [descriptions]
    year = date.fromisoformat(invoice_date_str[:10]).year
    dates = []
    for desc in descriptions:
        for m in _RANGE_CROSS_RE.finditer(desc):
            try:
                dates.append(date(year, MONTHS[m.group(1).lower()], int(m.group(2))))
                dates.append(date(year, MONTHS[m.group(3).lower()], int(m.group(4))))
            except (ValueError, KeyError):
                pass
            continue
        for m in _RANGE_RE.finditer(desc):
            mon = MONTHS.get(m.group(1).lower())
            if not mon:
                continue
            try:
                dates.append(date(year, mon, int(m.group(2))))
                dates.append(date(year, mon, int(m.group(3))))
            except ValueError:
                pass
        for m in _DATE_RE.finditer(desc):
            mon = MONTHS.get(m.group(1).lower())
            if not mon:
                continue
            try:
                dates.append(date(year, mon, int(m.group(2))))
            except ValueError:
                pass
    if not dates:
        return None, None
    dates.sort()
    return dates[0].isoformat(), dates[-1].isoformat()


async def _get_session() -> str:
    global _session_id, _session_ts
    if _session_id and (time.time() - _session_ts) < SESSION_TTL:
        return _session_id
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{BASE}/login", json={
            "username": os.environ.get("BILLCOM_EMAIL", ""),
            "password": os.environ.get("BILLCOM_PASSWORD", ""),
            "organizationId": os.environ.get("BILLCOM_ORG_ID", ""),
            "devKey": os.environ.get("BILLCOM_DEV_KEY", ""),
        })
        r.raise_for_status()
        _session_id = r.json()["sessionId"]
        _session_ts = time.time()
        return _session_id


async def _headers() -> dict:
    sid = await _get_session()
    return {"devKey": os.environ.get("BILLCOM_DEV_KEY", ""), "sessionId": sid, "Content-Type": "application/json"}


async def create_invoice(customer_id: str, invoice_number: str, due_date: str, line_items: list) -> dict:
    h = await _headers()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(f"{BASE}/invoices", headers=h, json={
            "customer": {"id": customer_id},
            "invoiceNumber": invoice_number,
            "dueDate": due_date,
            "invoiceLineItems": line_items,
        })
        r.raise_for_status()
        return r.json()


async def list_invoices(max_results: int = 100) -> dict | list:
    h = await _headers()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{BASE}/invoices", headers=h, params={"max": max_results})
        r.raise_for_status()
        return r.json()


async def list_customers(max_results: int = 100) -> dict | list:
    h = await _headers()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{BASE}/customers", headers=h, params={"max": max_results})
        r.raise_for_status()
        return r.json()
