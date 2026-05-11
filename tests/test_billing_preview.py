"""Covers billing.preview_descriptions and GET /api/billing/preview."""
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _install_billing_config(tmp_path, include_daily_descriptions=False):
    cfg_path = tmp_path / "bp.toml"
    state_dir = tmp_path / "bp-state"
    state_dir.mkdir()
    (state_dir / "logs").mkdir()
    cfg_path.write_text(f"""
[job]
key = "bp"
platform = "github"
ticket_system = "linear"
port = 18800
host = "http://bp.localhost"

[github]
repo = "fake/bp"

[linear]
token = "x"
assignee_email = "x@x.com"

[workspace]
root = "{tmp_path}"
repos = ["repo"]
tickets_dir = "tickets"
base_branch = "main"

[pr]
auto_pr = false

[billing]
name = "Preview Client"
rate = 90
billing_freq = "weekly"
billcom_customer_id = "cust1"
invoice_prefix = "PR"
include_daily_descriptions = {"true" if include_daily_descriptions else "false"}
extras = {{ ai_tool = 20 }}
""")
    return cfg_path, state_dir


def test_preview_descriptions_matches_line_items(fresh_db, tmp_path):
    cfg_path, state_dir = _install_billing_config(tmp_path)

    import core.db as db
    import core.config as cfg_mod
    import core.state as state

    state.init("bp")

    config = cfg_mod.load_config(str(cfg_path))

    from features import billing
    # Seed 3 work entries Mon-Wed of first week of March (monthly extras apply for week starting day <= 7).
    # With weekly billing + extras, the first-of-month week gets the ai_tool extra added.
    tickets_state = {}  # unused; not needed for _work_days_in
    # Monkey-patch: _work_days_in reads state.load("billing_entries")
    state.save("billing_entries", {
        "2026-03-02": {"date": "2026-03-02", "type": "work", "hours": 8},
        "2026-03-03": {"date": "2026-03-03", "type": "work", "hours": 8},
        "2026-03-04": {"date": "2026-03-04", "type": "work", "hours": 4},
    })

    descs = billing.preview_descriptions(config, "2026-03-02", "2026-03-06")
    assert any("Monday" in d and "March" in d for d in descs), descs
    assert any("Tuesday" in d for d in descs), descs
    assert any("Wednesday" in d for d in descs), descs
    # Because Monday is day 2 (<=7), extras apply: "ai tool" line should be present.
    assert any("ai tool" in d for d in descs), f"expected extras line in {descs}"

    # Second-week invoice (Mar 9-13) should not have extras
    state.save("billing_entries", {
        "2026-03-09": {"date": "2026-03-09", "type": "work", "hours": 8},
        "2026-03-10": {"date": "2026-03-10", "type": "work", "hours": 8},
    })
    descs2 = billing.preview_descriptions(config, "2026-03-09", "2026-03-13")
    assert not any("ai tool" in d for d in descs2), f"extras should not apply in week 2: {descs2}"
    assert any("Monday" in d for d in descs2)


def test_preview_endpoint_returns_descriptions(fresh_db, tmp_path):
    cfg_path, state_dir = _install_billing_config(tmp_path)
    sys.argv = ["frshty.py", str(cfg_path)]

    import core.db as db
    import core.config as cfg_mod
    import core.state as state
    import core.log as log

    config = cfg_mod.load_config(str(cfg_path))
    state.init(config["_state_dir"])
    log.init(config["_state_dir"], config["job"]["key"])

    state.save("billing_entries", {
        "2026-03-02": {"date": "2026-03-02", "type": "work", "hours": 8},
    })

    import frshty
    frshty._set_primary_config(config)

    from fastapi.testclient import TestClient
    client = TestClient(frshty.app)
    r = client.get("/api/billing/preview", params={"start": "2026-03-02", "end": "2026-03-06"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert "descriptions" in body
    assert isinstance(body["descriptions"], list)
    assert any("Monday" in d for d in body["descriptions"])


def test_next_invoice_number_scopes_billcom_query_to_customer(fresh_db, tmp_path):
    """Observed bug: next_invoice_number called billcom.list_invoices() with no
    customer filter, which returned the GLOBAL 100 oldest invoices (sorted
    ASC by createdTime). A recent invoice like DJ-AIMY-INV-1 from 2026-04-14
    was never in the window, so the function returned DJ-AIMY-INV-1 again,
    causing a bill.com 422 'duplicate invoice number' (BDC_1171) on POST.
    Fix: pass customer_id to list_invoices so bill.com filters server-side."""
    import asyncio
    from unittest.mock import AsyncMock, patch

    cfg_path, _ = _install_billing_config(tmp_path)
    import core.config as cfg_mod
    import core.state as state
    state.init("bp")
    config = cfg_mod.load_config(str(cfg_path))

    from features import billing

    fake_list = AsyncMock(return_value=[
        {"invoiceNumber": "PR-1", "customerId": "cust1"},
        {"invoiceNumber": "PR-2", "customerId": "cust1"},
        {"invoiceNumber": "PR-9", "customerId": "different_customer"},
    ])
    with patch("features.billcom.has_credentials", return_value=True), \
         patch("features.billcom.list_invoices", fake_list):
        result = asyncio.run(billing.next_invoice_number(config))

    assert result == {"number": "PR-3"}, result
    fake_list.assert_called_once()
    _, kwargs = fake_list.call_args
    assert kwargs.get("customer_id") == "cust1", (
        f"next_invoice_number must pass customer_id so bill.com filters "
        f"server-side; got kwargs={kwargs}"
    )


def test_list_invoices_excludes_archived_remote_and_local_cache(fresh_db, tmp_path):
    """Observed: user created PR-2 via the popup, then archived it on
    bill.com. Bill.com's list endpoint still returns archived invoices
    (recordStatus=INACTIVE, archived=true). Frshty must drop them — both
    from the remote list AND from the local cache (which still has the
    entry from creation time)."""
    import asyncio
    from unittest.mock import AsyncMock, patch

    cfg_path, _ = _install_billing_config(tmp_path)
    import core.config as cfg_mod
    import core.state as state
    state.init("bp")
    config = cfg_mod.load_config(str(cfg_path))

    state.save("billing_invoices", {
        "i2": {"id": "i2", "number": "PR-2", "date": "2026-04-30",
               "start": "2026-04-01", "end": "2026-04-30", "hours": 176,
               "amount": 15840, "status": "pending", "source": "billcom"},
    })

    fake_list = AsyncMock(return_value=[
        {"id": "i1", "invoiceNumber": "PR-1", "customerId": "cust1",
         "invoiceDate": "2026-03-31", "totalAmount": 14400,
         "archived": False, "recordStatus": "ACTIVE", "invoiceLineItems": []},
        {"id": "i2", "invoiceNumber": "PR-2", "customerId": "cust1",
         "invoiceDate": "2026-04-30", "totalAmount": 16515,
         "archived": True, "recordStatus": "INACTIVE", "invoiceLineItems": []},
    ])

    from features import billing
    with patch("features.billcom.has_credentials", return_value=True), \
         patch("features.billcom.list_invoices", fake_list):
        result = asyncio.run(billing.list_invoices(config))

    numbers = sorted(r["number"] for r in result)
    assert numbers == ["PR-1"], (
        f"PR-2 was archived on bill.com — should not appear in unified list, "
        f"not from remote and not from the local cache. Got: {numbers}"
    )


def test_day_hours_capped_at_8_on_invoice(fresh_db, tmp_path):
    """Days with 7+ recorded hours bill as a flat 8 on the invoice; days
    under 7h pass through unchanged. Applies to both line items and totals
    so the displayed total matches what bill.com receives."""
    cfg_path, _ = _install_billing_config(tmp_path, include_daily_descriptions=True)
    import core.config as cfg_mod
    import core.state as state
    state.init("bp")
    config = cfg_mod.load_config(str(cfg_path))

    state.save("billing_entries", {
        "2026-03-02": {"date": "2026-03-02", "type": "work", "hours": 10},
        "2026-03-03": {"date": "2026-03-03", "type": "work", "hours": 9.5},
        "2026-03-04": {"date": "2026-03-04", "type": "work", "hours": 8},
        "2026-03-05": {"date": "2026-03-05", "type": "work", "hours": 7},
        "2026-03-06": {"date": "2026-03-06", "type": "work", "hours": 4},
    })

    from features import billing
    entries = billing._work_days_in("2026-03-02", "2026-03-06")
    items = billing._build_line_items(config, {"start": "2026-03-02", "end": "2026-03-06"}, entries)

    day_items = [i for i in items if i["description"] != "ai tool"]
    qtys = [i["quantity"] for i in day_items]
    assert qtys == [8, 8, 8, 8, 4], (
        f"expected [8,8,8,8,4] (cap≥7h→8, under 7 unchanged); got {qtys}"
    )

    descs = [i["description"] for i in day_items]
    assert "8h" in descs[0] and "10h" not in descs[0], descs[0]
    assert "4h" in descs[-1], descs[-1]

    hours, amount = billing._totals(config, entries, start="2026-03-02")
    assert hours == 36, (
        f"_totals must sum capped hours (8+8+8+8+4=36, not raw 10+9.5+8+7+4=38.5); got {hours}"
    )


def test_list_invoices_filters_to_instance_prefix(fresh_db, tmp_path):
    """A bill.com customer can carry invoices from multiple prefix schemes
    (e.g. legacy `DJ-AIMY-N` plus the current `DJ-AIMY-INV-N`). Each frshty
    instance owns its own series via [billing].invoice_prefix and must NOT
    treat cross-scheme invoices as part of its own — otherwise next-number
    computation and overlap detection both get polluted."""
    import asyncio
    from unittest.mock import AsyncMock, patch

    cfg_path, _ = _install_billing_config(tmp_path)
    import core.config as cfg_mod
    import core.state as state
    state.init("bp")
    config = cfg_mod.load_config(str(cfg_path))

    fake_list = AsyncMock(return_value=[
        {"id": "i1", "invoiceNumber": "PR-1", "customerId": "cust1",
         "invoiceDate": "2026-01-15", "totalAmount": 100, "invoiceLineItems": []},
        {"id": "i2", "invoiceNumber": "PR-2", "customerId": "cust1",
         "invoiceDate": "2026-02-15", "totalAmount": 200, "invoiceLineItems": []},
        {"id": "i3", "invoiceNumber": "LEGACY-9", "customerId": "cust1",
         "invoiceDate": "2025-09-15", "totalAmount": 999, "invoiceLineItems": []},
        {"id": "i4", "invoiceNumber": "PR-OTHER-1", "customerId": "cust1",
         "invoiceDate": "2025-10-15", "totalAmount": 555, "invoiceLineItems": []},
    ])
    from features import billing
    with patch("features.billcom.has_credentials", return_value=True), \
         patch("features.billcom.list_invoices", fake_list):
        result = asyncio.run(billing.list_invoices(config))

    numbers = sorted(r["number"] for r in result)
    assert numbers == ["PR-1", "PR-2"], (
        f"expected only PR-prefixed invoices, got {numbers}"
    )


def test_billcom_list_invoices_sends_customer_filter(fresh_db, tmp_path):
    """billcom.list_invoices(customer_id=...) must hit bill.com with a
    filters=customerId:eq:<id> query param so server-side scoping limits
    the result set to that customer's invoices."""
    import asyncio
    from unittest.mock import AsyncMock, patch, MagicMock

    captured = {}

    class FakeAsyncClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def get(self, url, headers=None, params=None):
            captured["url"] = url
            captured["params"] = params or {}
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json = MagicMock(return_value=[])
            return resp

    from features import billcom
    with patch.object(billcom, "_headers", AsyncMock(return_value={"x": "y"})), \
         patch("features.billcom.httpx.AsyncClient", FakeAsyncClient):
        asyncio.run(billcom.list_invoices(customer_id="cust1"))

    assert captured["url"].endswith("/invoices")
    assert "filters" in captured["params"], captured["params"]
    assert "customerId" in captured["params"]["filters"]
    assert "cust1" in captured["params"]["filters"]


if __name__ == "__main__":
    tests = [test_preview_descriptions_matches_line_items, test_preview_endpoint_returns_descriptions]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
