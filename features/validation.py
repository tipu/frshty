"""Post-merge validator + regression pool.

Runs the ticket's playwright steps against the configured live app URL,
records each criterion's result in validation_run, upserts into
regression_pool, then replays the full pool on every merge.

Step grammar (interpretive, minimal):
  navigate to <path>
  click button '<text>'
  click '<text>'
  fill '<selector>' with '<value>'
  assert URL contains <substring>
  assert element '<selector>' exists
  assert text '<substring>' visible
  wait <ms>

Anything unrecognized fails the criterion as 'error'.
"""
import json
import re
from datetime import datetime, timezone

import core.db as db
import core.log as log
import features.acceptance as acceptance


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_run(instance_key: str, ticket_key: str, criterion_id: str,
                phase: str, result: str, output: str | None,
                triggered_by: str | None) -> None:
    db.execute(
        "INSERT INTO validation_run"
        "(instance_key, ticket_key, criterion_id, phase, result, playwright_output,"
        " triggered_by_ticket_key, run_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (instance_key, ticket_key, criterion_id, phase, result, output, triggered_by, _now()),
    )


def _upsert_pool(instance_key: str, ticket_key: str, criterion_id: str,
                 steps: list[str]) -> None:
    db.execute(
        "INSERT INTO regression_pool"
        "(instance_key, ticket_key, criterion_id, playwright_steps, added_at, removed_at)"
        " VALUES (?, ?, ?, ?, ?, NULL)"
        " ON CONFLICT(instance_key, ticket_key, criterion_id) DO UPDATE SET"
        "  playwright_steps=excluded.playwright_steps, added_at=excluded.added_at, removed_at=NULL",
        (instance_key, ticket_key, criterion_id, json.dumps(steps), _now()),
    )


def _list_pool(instance_key: str, exclude_ticket_key: str | None = None) -> list[dict]:
    rows = db.query_all(
        "SELECT ticket_key, criterion_id, playwright_steps FROM regression_pool"
        " WHERE instance_key=? AND removed_at IS NULL"
        " ORDER BY added_at",
        (instance_key,),
    )
    if exclude_ticket_key:
        rows = [r for r in rows if r["ticket_key"] != exclude_ticket_key]
    return rows


def remove_from_pool(instance_key: str, ticket_key: str) -> None:
    """Soft-delete pool entries for a ticket (used on obsolete)."""
    db.execute(
        "UPDATE regression_pool SET removed_at=?"
        " WHERE instance_key=? AND ticket_key=? AND removed_at IS NULL",
        (_now(), instance_key, ticket_key),
    )


def latest_runs(instance_key: str, ticket_key: str, limit: int = 50) -> list[dict]:
    return db.query_all(
        "SELECT id, criterion_id, phase, result, playwright_output,"
        " triggered_by_ticket_key, run_at FROM validation_run"
        " WHERE instance_key=? AND ticket_key=?"
        " ORDER BY run_at DESC LIMIT ?",
        (instance_key, ticket_key, limit),
    )


def badge_for(instance_key: str, ticket_key: str) -> str:
    """One of: pass, fail, regression, pending."""
    rows = db.query_all(
        "SELECT phase, result FROM validation_run"
        " WHERE instance_key=? AND ticket_key=?"
        " ORDER BY run_at DESC LIMIT 50",
        (instance_key, ticket_key),
    )
    if not rows:
        return "pending"
    primary = [r for r in rows if r["phase"] == "primary"]
    regress = [r for r in rows if r["phase"] == "regression"]
    if primary and primary[0]["result"] != "pass":
        return "fail"
    if any(r["result"] != "pass" for r in regress):
        return "regression"
    return "pass"


_NAV = re.compile(r"^navigate to (.+)$", re.I)
_CLICK_BTN = re.compile(r"^click button ['\"](.+)['\"]$", re.I)
_CLICK_TXT = re.compile(r"^click ['\"](.+)['\"]$", re.I)
_FILL = re.compile(r"^fill ['\"](.+)['\"] with ['\"](.+)['\"]$", re.I)
_ASSERT_URL = re.compile(r"^assert URL contains (.+)$", re.I)
_ASSERT_EL = re.compile(r"^assert element ['\"](.+)['\"] exists$", re.I)
_ASSERT_TEXT = re.compile(r"^assert text ['\"](.+)['\"] visible$", re.I)
_WAIT = re.compile(r"^wait (\d+)$", re.I)


def _run_steps(steps: list[str], base_url: str) -> tuple[str, str]:
    """Returns (result, output). result in {pass, fail, error}."""
    try:
        from playwright.sync_api import TimeoutError as PWTimeout
        from playwright.sync_api import sync_playwright
    except ImportError:
        return "error", "playwright not installed"

    output_lines: list[str] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()
            try:
                for step in steps:
                    s = step.strip()
                    output_lines.append(f"> {s}")
                    if (m := _NAV.match(s)):
                        page.goto(base_url.rstrip("/") + "/" + m.group(1).lstrip("/"))
                    elif (m := _CLICK_BTN.match(s)):
                        page.get_by_role("button", name=m.group(1)).click()
                    elif (m := _CLICK_TXT.match(s)):
                        page.get_by_text(m.group(1)).first.click()
                    elif (m := _FILL.match(s)):
                        page.fill(m.group(1), m.group(2))
                    elif (m := _ASSERT_URL.match(s)):
                        if m.group(1).strip() not in page.url:
                            return "fail", "\n".join(output_lines + [f"URL is {page.url}"])
                    elif (m := _ASSERT_EL.match(s)):
                        if page.locator(m.group(1)).count() == 0:
                            return "fail", "\n".join(output_lines + [f"element '{m.group(1)}' missing"])
                    elif (m := _ASSERT_TEXT.match(s)):
                        if not page.get_by_text(m.group(1)).first.is_visible():
                            return "fail", "\n".join(output_lines + [f"text '{m.group(1)}' not visible"])
                    elif (m := _WAIT.match(s)):
                        page.wait_for_timeout(int(m.group(1)))
                    else:
                        return "error", "\n".join(output_lines + [f"unrecognized step: {s}"])
            except PWTimeout as e:
                return "error", "\n".join(output_lines + [f"timeout: {e}"])
            except Exception as e:
                return "fail", "\n".join(output_lines + [f"{type(e).__name__}: {e}"])
            finally:
                context.close()
                browser.close()
    except Exception as e:
        return "error", f"{type(e).__name__}: {e}"
    return "pass", "\n".join(output_lines)


def validate_merged(instance_key: str, ticket_key: str, ticket_data: dict,
                    base_url: str) -> dict:
    """Primary validation + regression replay. Returns summary dict.

    Records every criterion run in validation_run. Upserts owned criteria
    into regression_pool. Replays all pool entries (excluding owner) and
    records 'regression' results on the OWNED ticket of any failing replay.
    """
    summary = {"primary": [], "regression": []}
    crits = acceptance.all_criteria(ticket_data)

    for c in crits:
        cid = c.get("id") or "c0"
        steps = c.get("playwright") or []
        if not steps:
            continue
        result, output = _run_steps(steps, base_url)
        _record_run(instance_key, ticket_key, cid, "primary", result, output, None)
        summary["primary"].append({"criterion_id": cid, "result": result})
        if result == "pass":
            _upsert_pool(instance_key, ticket_key, cid, steps)

    pool = _list_pool(instance_key, exclude_ticket_key=ticket_key)
    for entry in pool:
        steps = json.loads(entry["playwright_steps"]) if entry["playwright_steps"] else []
        if not steps:
            continue
        result, output = _run_steps(steps, base_url)
        replay_result = "pass" if result == "pass" else "regression"
        _record_run(instance_key, entry["ticket_key"], entry["criterion_id"],
                    "regression", replay_result, output, ticket_key)
        summary["regression"].append({
            "ticket_key": entry["ticket_key"],
            "criterion_id": entry["criterion_id"],
            "result": replay_result,
        })

    log.emit(
        "validation_complete",
        f"[{instance_key}] {ticket_key}: primary={len(summary['primary'])} regression={len(summary['regression'])}",
        meta=summary,
    )
    return summary
