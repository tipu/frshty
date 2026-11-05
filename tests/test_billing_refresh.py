"""Covers billing.refresh_invoice and POST /api/billing/invoices/{id}/refresh."""
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tests.test_billing_preview import _install_billing_config


def _stale_invoice(status: str = "OPEN", payments: list | None = None) -> dict:
    return {
        "id": "inv1",
        "customerId": "cust1",
        "invoiceNumber": "PR-7",
        "invoiceDate": "2026-03-06",
        "dueDate": "2026-03-06",
        "status": status,
        "payments": payments or [],
        "totalAmount": 900.0,
        "invoiceLineItems": [
            {"description": "Monday, March 2 — PR-1: Old work", "quantity": 8, "price": 90},
            {"description": "Tuesday, March 3 · 2h", "quantity": 2, "price": 90},
        ],
    }


def _config(tmp_path):
    cfg_path, _ = _install_billing_config(tmp_path, include_daily_descriptions=True)
    import core.config as cfg_mod
    import core.state as state
    state.init("bp")
    config = cfg_mod.load_config(str(cfg_path))
    config.setdefault("features", {})["timesheet"] = True
    config["jira"] = {"base_url": "https://jira.example"}
    state.save("billing_entries", {
        "2026-03-04": {"date": "2026-03-04", "type": "vacation", "hours": 0},
        "2026-03-05": {"date": "2026-03-05", "type": "vacation", "hours": 0},
        "2026-03-06": {"date": "2026-03-06", "type": "vacation", "hours": 0},
    })
    return config


def test_refresh_invoice_staleInvoice_rebuildsLineItemsFromJira(fresh_db, tmp_path):
    config = _config(tmp_path)
    from features import billing
    import core.state as state

    worklogs = {
        "2026-03-02": [{"ticket": "PR-1", "summary": "Old work", "hours": 8}],
        "2026-03-03": [{"ticket": "PR-2", "summary": "Backfilled work", "hours": 8}],
    }
    replace = AsyncMock(return_value={"id": "inv1"})
    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value=_stale_invoice())), \
         patch("features.billing.billcom.replace_invoice", replace), \
         patch("features.billing._fetch_worklogs", return_value=worklogs):
        rec = asyncio.run(billing.refresh_invoice(config, "inv1"))

    kwargs = replace.await_args.kwargs
    assert kwargs["invoice_id"] == "inv1"
    assert kwargs["invoice_number"] == "PR-7"
    assert kwargs["customer_id"] == "cust1"
    descs = [li["description"] for li in kwargs["line_items"]]
    assert any(d.startswith("Tuesday, March 3 — PR-2: Backfilled work") for d in descs), descs
    assert not any("· 2h" in d for d in descs), descs
    tuesday = next(li for li in kwargs["line_items"] if li["description"].startswith("Tuesday"))
    assert tuesday["quantity"] == 8
    assert (rec["start"], rec["end"], rec["hours"]) == ("2026-03-02", "2026-03-03", 16)
    assert rec["amount"] == 16 * 90 + 20
    assert state.load("billing_invoices")["inv1"]["hours"] == 16


@pytest.mark.parametrize("status,payments", [
    ("PAID_IN_FULL", []),
    ("PARTIAL_PAYMENT", []),
    ("OPEN", [{"id": "pay1"}]),
])
def test_refresh_invoice_paidInvoice_refusesAndLeavesBillcomUntouched(fresh_db, tmp_path, status, payments):
    config = _config(tmp_path)
    from features import billing

    replace = AsyncMock()
    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value=_stale_invoice(status, payments))), \
         patch("features.billing.billcom.replace_invoice", replace):
        with pytest.raises(ValueError, match="payments"):
            asyncio.run(billing.refresh_invoice(config, "inv1"))
    replace.assert_not_awaited()


@pytest.mark.parametrize("fields", [{"archived": True}, {"recordStatus": "INACTIVE"}])
def test_refresh_invoice_archivedInvoice_refuses(fresh_db, tmp_path, fields):
    config = _config(tmp_path)
    from features import billing

    replace = AsyncMock()
    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value={**_stale_invoice(), **fields})), \
         patch("features.billing.billcom.replace_invoice", replace):
        with pytest.raises(ValueError, match="archived"):
            asyncio.run(billing.refresh_invoice(config, "inv1"))
    replace.assert_not_awaited()


def test_refresh_invoice_otherCustomer_refuses(fresh_db, tmp_path):
    config = _config(tmp_path)
    from features import billing

    other = {**_stale_invoice(), "customerId": "someone-else"}
    replace = AsyncMock()
    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value=other)), \
         patch("features.billing.billcom.replace_invoice", replace):
        with pytest.raises(ValueError, match="does not belong"):
            asyncio.run(billing.refresh_invoice(config, "inv1"))
    replace.assert_not_awaited()


def test_refresh_endpoint_paidInvoice_returns400(fresh_db, tmp_path):
    cfg_path, _ = _install_billing_config(tmp_path)
    sys.argv = ["frshty.py", str(cfg_path)]
    import core.config as cfg_mod
    import core.state as state
    import core.log as log

    config = cfg_mod.load_config(str(cfg_path))
    state.init(config["_state_dir"])
    log.init(config["_state_dir"], config["job"]["key"])

    import frshty
    frshty._set_primary_config(config)
    from fastapi.testclient import TestClient
    client = TestClient(frshty.app)

    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value=_stale_invoice("PAID_IN_FULL"))):
        r = client.post("/api/billing/invoices/inv1/refresh")
    assert r.status_code == 400, r.text
    assert "payments" in r.json()["error"]


@pytest.mark.parametrize("fetch", [
    {"side_effect": RuntimeError("Jira search failed: 401")},
    {"return_value": {}},
])
def test_refresh_invoice_jiraUnavailable_refusesAndLeavesBillcomUntouched(fresh_db, tmp_path, fetch):
    config = _config(tmp_path)
    from features import billing
    import core.state as state

    replace = AsyncMock()
    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value=_stale_invoice())), \
         patch("features.billing.billcom.replace_invoice", replace), \
         patch("features.billing._fetch_worklogs", **fetch):
        with pytest.raises(ValueError, match="Jira"):
            asyncio.run(billing.refresh_invoice(config, "inv1"))
    replace.assert_not_awaited()
    assert "inv1" not in state.load("billing_invoices")


def test_refresh_invoice_partialJiraDay_totalsMatchLineItems(fresh_db, tmp_path):
    config = _config(tmp_path)
    from features import billing

    worklogs = {
        "2026-03-02": [{"ticket": "PR-1", "summary": "Old work", "hours": 8}],
        "2026-03-03": [{"ticket": "PR-2", "summary": "Short day", "hours": 2}],
    }
    replace = AsyncMock(return_value={"id": "inv1"})
    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value=_stale_invoice())), \
         patch("features.billing.billcom.replace_invoice", replace), \
         patch("features.billing._fetch_worklogs", return_value=worklogs):
        rec = asyncio.run(billing.refresh_invoice(config, "inv1"))

    items = replace.await_args.kwargs["line_items"]
    assert rec["hours"] == 10
    assert rec["amount"] == sum(i["quantity"] * i["price"] for i in items) == 10 * 90 + 20


def test_refresh_invoice_descriptionsNotConfigured_refuses(fresh_db, tmp_path):
    config = _config(tmp_path)
    config["billing"]["include_daily_descriptions"] = False
    from features import billing

    replace = AsyncMock()
    with patch("features.billing.billcom.has_credentials", return_value=True), \
         patch("features.billing.billcom.get_invoice", AsyncMock(return_value=_stale_invoice())), \
         patch("features.billing.billcom.replace_invoice", replace):
        with pytest.raises(ValueError, match="not configured"):
            asyncio.run(billing.refresh_invoice(config, "inv1"))
    replace.assert_not_awaited()
