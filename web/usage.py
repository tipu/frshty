from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from starlette.routing import Match

from services import usage

router = APIRouter()

_MAX_EVENTS_PER_POST = 200


def _walk_api_routes(routes):
    """Every APIRoute reachable from an app, however deeply it is included.

    FastAPI used to flatten include_router into app.routes. From 0.141 it
    appends one _IncludedRouter that holds the APIRouter instead, so a flat
    scan of app.routes finds only the routes declared on the app itself and
    the usage report loses every route the frshty routers declare."""
    for r in routes:
        if isinstance(r, APIRoute):
            yield r
            continue
        nested = getattr(r, "routes", None)
        if nested is None:
            nested = getattr(getattr(r, "original_router", None), "routes", None)
        if nested:
            yield from _walk_api_routes(nested)


def _api_routes(request: Request) -> list[tuple[str, str]]:
    out = []
    for r in _walk_api_routes(request.app.routes):
        for method in sorted(r.methods or ()):
            if method != "HEAD":
                out.append((method, r.path))
    return out


def _page_template(request: Request, page: str) -> str:
    if not page.startswith("/"):
        return ""
    scope = {"type": "http", "path": page, "method": "GET"}
    for r in _walk_api_routes(request.app.routes):
        if "GET" in (r.methods or ()):
            match, _ = r.matches(scope)
            if match == Match.FULL:
                return r.path
    return page


@router.post("/api/usage/ui")
async def api_usage_ui(request: Request):
    body = await request.json()
    page = _page_template(request, str(body.get("page") or ""))
    if not page:
        return {"ok": False, "error": "page must be an absolute path"}
    host = (request.headers.get("host") or "").split(":")[0]
    events = body.get("events") or []
    recorded = 0
    for ev in events[:_MAX_EVENTS_PER_POST]:
        element = str(ev.get("element") or "")
        try:
            n = int(ev.get("count") or 0)
        except (TypeError, ValueError):
            n = 0
        if element and n > 0:
            usage.record("ui", f"{page} {element}", instance=host, n=n)
            recorded += 1
    return {"ok": True, "recorded": recorded}


@router.get("/api/usage/report")
def api_usage_report(request: Request):
    route_counts = {r["name"]: r for r in usage.aggregates("route")}
    used, unused, excluded = [], [], []
    for method, path in _api_routes(request):
        name = f"{method} {path}"
        if not usage.is_tracked(path):
            excluded.append(name)
            continue
        row = route_counts.get(name)
        if row:
            used.append(row)
        else:
            unused.append(name)
    return {
        "tracking_since": usage.tracking_since(),
        "routes": {"used": used, "unused": unused, "excluded_from_tracking": excluded},
        "ui": usage.aggregates("ui"),
        "mcp": usage.aggregates("mcp"),
    }
