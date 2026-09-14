"""Read the Upwork inbox from the logged-in Chrome profile on this machine.

The Upwork inbox is a plain REST API under `/api/v3/rooms`, not GraphQL and
not a websocket. Three endpoints carry everything a reader needs: the unread
counts, the room list, and the messages inside one room. Each of them answers
HTTP 200 to a bearer token and the cookie jar of a logged-in session, from any
client, so the reader does not have to live inside the browser.

The bearer is the part that does. It is minted by the page, not stored
anywhere on disk, so it is lifted from a live Chrome with remote debugging
open: a throwaway tab is pointed at the messages app, the header of the first
`/api/v3/rooms/` request it makes is recorded, and the tab is closed. That is
also where `callerOrgId` comes from, which every one of these endpoints
requires and refuses to guess: a room list called without it answers 400.

The session is held in memory and never written to disk. It carries the whole
Upwork cookie jar, `master_access_token` among it, and the frshty database is
an ordinary file. The cost is one lift per process, which is a few seconds.

The token expires and nothing announces it, so there is no expiry clock here.
A call that comes back 401 or 403 lifts a new session once and runs again,
which is self-correcting whatever the real lifetime turns out to be.

Reading is all this module is scheduled to do. `send_message` is here because
the operator answers a client from the board rather than from upwork.com, and
it is called from one place: the reply route, which a person clicks. See
core/correspondence.py, which closes that route to an agent.
"""
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://www.upwork.com/api/v3/rooms"
TRIGGER = "https://www.upwork.com/ab/messages/"
DEFAULT_CDP = "http://localhost:9222"
GRAB_TIMEOUT = 60
REQUEST_TIMEOUT = 30

_lock = threading.Lock()
# One session per browser, never one per process. Two instances can be
# configured against two Chrome profiles logged into two Upwork accounts, and
# one shared slot would hand the second instance the first one's bearer,
# cookies and organisation: it would index somebody else's inbox and its reply
# route would write as somebody else.
_sessions: dict[str, dict] = {}


class UpworkAuthError(RuntimeError):
    """No usable session could be lifted out of the local browser."""


class UpworkApiError(RuntimeError):
    """Upwork answered, and the answer was not a success."""

    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}: {body[:200]}")
        self.status = status
        self.body = body


def _capture(cdp_url: str, timeout: int) -> dict:
    """Open one throwaway tab and record what the messages app sends.

    The tab is opened in the browser's existing context, so it carries the
    human's login, and it is closed again in every case. The operator's own
    tab is never touched: a scan that navigated it would move the page the
    human is reading.

    playwright is imported here rather than at the top of the file. It is
    installed for this instance and not on CI, which installs the server's
    runtime dependencies alone, and every test that imports the app would
    otherwise fail on an import it never reaches."""
    from playwright.sync_api import sync_playwright

    found: dict = {}

    def on_request(req):
        if "/api/v3/rooms/" not in req.url:
            return
        headers = req.headers
        auth = headers.get("authorization", "")
        if "bearer" in auth.lower():
            found.setdefault("authorization", auth)
            found.setdefault("user_agent", headers.get("user-agent", ""))
            found.setdefault("tenant", headers.get("x-upwork-api-tenantid", ""))
        query = urllib.parse.parse_qs(urllib.parse.urlparse(req.url).query)
        if query.get("callerOrgId"):
            found.setdefault("org_id", query["callerOrgId"][0])

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(cdp_url)
        if not browser.contexts:
            raise UpworkAuthError(f"no browser context on {cdp_url}")
        ctx = browser.contexts[0]
        ctx.on("request", on_request)
        page = ctx.new_page()
        try:
            page.goto(TRIGGER, wait_until="domcontentloaded",
                      timeout=timeout * 1000)
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if "authorization" in found and "org_id" in found:
                    break
                time.sleep(0.5)
            cookies = ctx.cookies()
        finally:
            ctx.remove_listener("request", on_request)
            page.close()

    if "authorization" not in found or "org_id" not in found:
        raise UpworkAuthError(
            "the messages app sent no authorized request; the profile behind"
            f" {cdp_url} is probably logged out")
    found["cookie"] = "; ".join(
        f"{c['name']}={c['value']}" for c in cookies
        if c["domain"].endswith("upwork.com"))
    found["user_id"] = next(
        (c["value"] for c in cookies
         if c["name"] == "user_uid" and c["domain"].endswith("upwork.com")), "")
    return found


def _target(config: dict | None) -> tuple[str, int]:
    settings = (config or {}).get("upwork") or {}
    return (str(settings.get("cdp_url") or DEFAULT_CDP),
            int(settings.get("grab_timeout") or GRAB_TIMEOUT))


def session(config: dict | None = None, refresh: bool = False) -> dict:
    """The lifted session for this config's browser, lifting one when there is
    none or the one held was rejected.

    The lift runs outside the lock. It drives a browser and can take a minute,
    and `held` is asked on every render of /upwork, which must not wait on it.
    Two lifts racing for one browser cost one wasted tab and agree on the
    result, which is cheaper than a page that hangs."""
    cdp_url, timeout = _target(config)
    if not refresh:
        with _lock:
            found = _sessions.get(cdp_url)
        if found is not None:
            return found
    fresh = _capture(cdp_url, timeout)
    with _lock:
        _sessions[cdp_url] = fresh
    return fresh


def held(config: dict | None = None) -> dict | None:
    """The session this process already lifted for this config's browser.

    Reading it never lifts one. A page render that asked `session` for the
    operator's own user id would open a browser tab every time somebody looked
    at /upwork."""
    with _lock:
        return _sessions.get(_target(config)[0])


def forget(config: dict | None = None) -> None:
    """Drop the held session for this config's browser, or every one of them
    when no config names one. The next call lifts a new one."""
    with _lock:
        if config is None:
            _sessions.clear()
        else:
            _sessions.pop(_target(config)[0], None)


def _headers(auth: dict) -> dict:
    return {"authorization": auth["authorization"],
            "user-agent": auth.get("user_agent", ""),
            "accept": "application/json",
            "x-upwork-api-tenantid": auth.get("tenant", ""),
            "cookie": auth.get("cookie", "")}


def _request(auth: dict, method: str, path: str, params: dict,
             body: dict | None) -> dict:
    query = dict(params)
    query["callerOrgId"] = auth["org_id"]
    url = f"{BASE}{path}?{urllib.parse.urlencode(query)}"
    headers = _headers(auth)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        raise UpworkApiError(e.code, e.read().decode("utf8", "replace")) from e
    if not raw:
        return {}
    return json.loads(raw)


def call(config: dict | None, method: str, path: str, params: dict | None = None,
         body: dict | None = None) -> dict:
    """One call against the inbox API, lifting a fresh session if this one is
    no longer accepted.

    Upwork answers an expired bearer with 401 and a bearer for another account
    with 403, and neither is a reason to fail the scan while a live browser
    sits on the same machine. Every other status is reported as it is: a 400
    means the request was wrong, and lifting a new token would not fix it."""
    auth = session(config)
    try:
        return _request(auth, method, path, params or {}, body)
    except UpworkApiError as e:
        if e.status not in (401, 403):
            raise
    return _request(session(config, refresh=True), method, path, params or {},
                    body)


def message_counts(config: dict | None = None) -> dict:
    return call(config, "GET", "/users/messageCounts")


def rooms(config: dict | None = None, limit: int = 20,
          cursor: str = "") -> dict:
    params: dict = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    return call(config, "GET", "/rooms/simplified", params)


def stories(room_id: str, config: dict | None = None, limit: int = 20,
            older_than: str = "") -> dict:
    """One page of a room's messages, newest first.

    `older_than` is a `created` stamp in epoch milliseconds, and the page it
    returns holds the messages strictly older than it. It is not the `cursor`
    the answer carries, which is a story id: passing that is answered 404
    whether or not older messages exist."""
    params: dict = {"limit": limit}
    if older_than:
        params["olderThan"] = older_than
    return call(config, "GET", f"/rooms/{room_id}/stories/simplified", params)


def send_message(room_id: str, text: str, config: dict | None = None) -> dict:
    """Post one message into a room as the logged-in account.

    The route is Upwork's own: their PHP and Python SDKs both publish
    `POST /messages/v3/{company}/rooms/{roomId}/stories`, which is this path
    with the company named by `callerOrgId` instead. Only the operator reaches
    it, from the reply button on /upwork, and whatever Upwork answers is
    handed back to that button rather than swallowed."""
    return call(config, "POST", f"/rooms/{room_id}/stories",
                body={"story": {"message": text}})
