import json
import tempfile
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from core.discovery import discover_instances, find_instance, call_instance, fan_out

server = FastMCP("frshty", instructions="Supervisor for multiple frshty instances. Query health, tickets, reviews, and events across all running instances. Fix stuck tickets, reconcile state drift, and trigger actions.")


def _resolve(instance: str | None) -> tuple[list[dict], dict | None]:
    instances = discover_instances()
    if instance:
        found = find_instance(instances, instance)
        return instances, found
    return instances, None


@server.tool(description="List all frshty instances with their health status, unread counts, and enabled features.")
async def list_instances() -> str:
    instances = discover_instances()
    statuses = await fan_out(instances, "GET", "/api/status")
    result = []
    for inst in instances:
        status = statuses.get(inst["key"], {})
        result.append({
            "key": inst["key"],
            "port": inst["port"],
            "base_url": inst["base_url"],
            "platform": inst["platform"],
            "ticket_system": inst["ticket_system"],
            "status": status,
        })
    return json.dumps(result, indent=2)


@server.tool(description="Get active tickets. Pass instance name to query one, or omit for all instances.")
async def get_tickets(instance: str | None = None) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances
    results = await fan_out(targets, "GET", "/api/tickets/list")
    return json.dumps(results, indent=2)


@server.tool(description="Get full ticket detail including docs, terminal health, and history.")
async def get_ticket_detail(instance: str, ticket_key: str) -> str:
    _, found = _resolve(instance)
    if not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    result = await call_instance(found["base_url"], "GET", f"/api/tickets/{ticket_key}/detail")
    return json.dumps(result, indent=2)


@server.tool(description="Get recent events. Pass unread=true for only unread. Omit instance for all.")
async def get_events(instance: str | None = None, unread: bool = True) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances
    path = f"/api/events?unread={'true' if unread else 'false'}"
    results = await fan_out(targets, "GET", path)
    return json.dumps(results, indent=2)


@server.tool(description="Get open PR reviews. Omit instance for all.")
async def get_reviews(instance: str | None = None) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances
    results = await fan_out(targets, "GET", "/api/reviews")
    return json.dumps(results, indent=2)


@server.tool(description="Get scheduled PR creations and CI-pending tickets. Omit instance for all.")
async def get_scheduled(instance: str | None = None) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances
    results = await fan_out(targets, "GET", "/api/scheduled")
    return json.dumps(results, indent=2)


@server.tool(description="Restart a stuck ticket's Claude Code terminal session.")
async def restart_ticket(instance: str, ticket_key: str) -> str:
    _, found = _resolve(instance)
    if not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    result = await call_instance(found["base_url"], "POST", f"/api/tickets/{ticket_key}/restart")
    return json.dumps(result, indent=2)


@server.tool(description="Manually change a ticket's status (e.g. to pr_ready, done, etc).")
async def change_ticket_status(instance: str, ticket_key: str, status: str) -> str:
    _, found = _resolve(instance)
    if not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    result = await call_instance(found["base_url"], "POST", f"/api/tickets/{ticket_key}/status", {"status": status})
    return json.dumps(result, indent=2)


@server.tool(description="Trigger an immediate poll cycle on an instance. Omit instance for all.")
async def trigger_cycle(instance: str | None = None) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances
    results = await fan_out(targets, "POST", "/api/poll")
    return json.dumps(results, indent=2)


@server.tool(description="Get raw ticket data from the actual ticket system (Jira/Linear), not frshty's internal state. Useful for comparing what the ticket system says vs what frshty thinks.")
async def get_raw_tickets(instance: str | None = None) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances
    results = await fan_out(targets, "GET", "/api/raw/tickets")
    return json.dumps(results, indent=2)


@server.tool(description="Get raw PR data from the actual platform (GitHub/Bitbucket), not frshty's internal state. Returns open PRs authored by the configured user.")
async def get_raw_prs(instance: str | None = None) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances
    results = await fan_out(targets, "GET", "/api/raw/prs")
    return json.dumps(results, indent=2)


@server.tool(description="Compare frshty ticket state vs actual ticket system and PR state. Detects drift like tickets that exist in Jira/Linear but not in frshty, or PRs that are merged but frshty still shows pr_created.")
async def reconcile(instance: str | None = None) -> str:
    instances, found = _resolve(instance)
    if instance and not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})
    targets = [found] if found else instances

    frshty_tickets = await fan_out(targets, "GET", "/api/tickets/list")
    raw_tickets = await fan_out(targets, "GET", "/api/raw/tickets")
    raw_prs = await fan_out(targets, "GET", "/api/raw/prs")

    discrepancies = []

    for inst in targets:
        key = inst["key"]
        ft = frshty_tickets.get(key, {})
        rt = raw_tickets.get(key, [])
        rp = raw_prs.get(key, {})

        if isinstance(ft, dict) and "error" in ft:
            discrepancies.append({"instance": key, "type": "frshty_error", "error": ft["error"]})
            continue

        if isinstance(rt, list):
            raw_keys = {t["key"] for t in rt}
            frshty_keys = set(ft.keys()) if isinstance(ft, dict) else set()
            missing = raw_keys - frshty_keys
            for mk in missing:
                raw_ticket = next(t for t in rt if t["key"] == mk)
                discrepancies.append({
                    "instance": key,
                    "type": "ticket_missing_in_frshty",
                    "ticket": mk,
                    "external_status": raw_ticket.get("status", ""),
                })

        if isinstance(rp, dict) and "my_prs" in rp:
            open_pr_ids = {(pr.get("repo"), pr.get("id")) for pr in rp["my_prs"]}
            if isinstance(ft, dict):
                for ticket_key, ticket in ft.items():
                    for pr in ticket.get("prs", []):
                        pr_tuple = (pr.get("repo"), pr.get("id"))
                        frshty_status = ticket.get("status", "")
                        if frshty_status in ("pr_created", "in_review") and pr_tuple not in open_pr_ids:
                            discrepancies.append({
                                "instance": key,
                                "type": "pr_no_longer_open",
                                "ticket": ticket_key,
                                "pr": f"{pr.get('repo')}#{pr.get('id')}",
                                "frshty_status": frshty_status,
                                "action": "PR may have been merged or closed, frshty status is stale",
                            })

    if not discrepancies:
        return json.dumps({"status": "all_clean", "message": "No state drift detected"})
    return json.dumps(discrepancies, indent=2)


@server.tool(description="Take a screenshot of a frshty instance's web UI using Playwright. Returns the screenshot file path. Resolution is 1920x1080.")
async def check_ui(instance: str, page: str = "/") -> str:
    _, found = _resolve(instance)
    if not found:
        return json.dumps({"error": f"Instance '{instance}' not found"})

    url = f"{found['base_url']}{page}"
    screenshot_path = Path(tempfile.mktemp(suffix=f"_{instance}.png", prefix="frshty_ui_"))

    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            ctx = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                ignore_https_errors=True,
            )
            pg = await ctx.new_page()
            await pg.goto(url, wait_until="networkidle", timeout=15000)
            await pg.screenshot(path=str(screenshot_path), full_page=True)
            await browser.close()
        return json.dumps({"screenshot": str(screenshot_path), "url": url})
    except Exception as e:
        return json.dumps({"error": str(e), "url": url})


if __name__ == "__main__":
    server.run(transport="stdio")
