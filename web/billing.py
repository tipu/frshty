import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import core.log as log
import features.billing as billing
from features.billing import OverlapError
from web.state import _config


router = APIRouter()


@router.get("/api/billing/client")
def api_billing_client():
    return billing.get_client(_config)


@router.get("/api/billing/schedule-status")
async def api_billing_schedule_status():
    return await billing.get_schedule_status(_config)


@router.get("/api/billing/entries")
def api_billing_entries(month: str = ""):
    return billing.list_entries(_config, month)


@router.post("/api/billing/entries")
async def api_billing_upsert_entry(request: Request):
    body = await request.json()
    return billing.upsert_entries(_config, body)


@router.delete("/api/billing/entries/{day}")
def api_billing_delete_entry(day: str):
    return billing.delete_entry(_config, day)


@router.get("/api/billing/invoices")
async def api_billing_invoices():
    return await billing.list_invoices(_config)


@router.post("/api/billing/invoices")
async def api_billing_create_invoice(body: dict):
    try:
        return await billing.create_invoice(_config, body, source="manual")
    except OverlapError as e:
        return JSONResponse({"error": str(e), "conflict": e.conflict}, status_code=409)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except httpx.HTTPError as e:
        log.emit("invoice_create_failed", f"bill.com create failed: {e}", meta={"err": str(e)[:200]})
        return JSONResponse({"error": f"billcom failed: {e}"}, status_code=502)


@router.get("/api/billing/next-invoice-number")
async def api_billing_next_number():
    return await billing.next_invoice_number(_config)


@router.get("/api/billing/preview")
def api_billing_preview(start: str, end: str):
    return {"descriptions": billing.preview_descriptions(_config, start, end)}
