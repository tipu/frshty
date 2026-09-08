"""A page on another site must not be able to change this board.

The board has no login. Every check that drives HTTP or a WebSocket goes
through the real middleware stack, so a route added later is covered without a
new test.
"""
import sys

import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import web.middleware as middleware
import web.origin as origin
import web.state as state
from web.origin import origin_allowed

BOARD_HOST = "board.frshty.localhost"
BOARD_ORIGIN = f"http://{BOARD_HOST}"
FOREIGN_ORIGIN = "https://evil.example"


@pytest.fixture(autouse=True)
def board_identity():
    """Declare BOARD_HOST as this board's hostname, the way a config does."""
    saved_primary = state._primary_config
    saved_hosts = dict(state._configs_by_host)
    origin._reported_senders.clear()
    state.set_primary_config({"job": {"key": "test", "port": 7100},
                              "_base_url": BOARD_ORIGIN})
    yield
    state._configs_by_host.clear()
    state._configs_by_host.update(saved_hosts)
    state.set_primary_config(saved_primary)
    origin._reported_senders.clear()


@pytest.fixture
def app():
    a = FastAPI()
    middleware.install(a)

    @a.get("/api/thing")
    def read_thing():
        return {"read": True}

    @a.post("/api/thing")
    def write_thing():
        return {"wrote": True}

    @a.put("/api/thing")
    def replace_thing():
        return {"replaced": True}

    @a.patch("/api/thing")
    def amend_thing():
        return {"amended": True}

    @a.delete("/api/thing")
    def drop_thing():
        return {"dropped": True}

    @a.websocket("/ws/terminal/{key}")
    async def ws_terminal(websocket: WebSocket, key: str):
        await websocket.accept()
        await websocket.send_text("served")
        await websocket.close()

    return a


@pytest.fixture
def client(app):
    return TestClient(app, base_url=BOARD_ORIGIN)


def test_same_origin_post_is_served(client):
    r = client.post("/api/thing", headers={"origin": BOARD_ORIGIN})
    assert r.status_code == 200
    assert r.json() == {"wrote": True}


def test_foreign_origin_post_is_blocked(client):
    r = client.post("/api/thing", headers={"origin": FOREIGN_ORIGIN})
    assert r.status_code == 403
    assert "cross-site" in r.json()["error"]


def test_opaque_origin_post_is_blocked(client):
    assert client.post("/api/thing",
                       headers={"origin": "null"}).status_code == 403


@pytest.mark.parametrize("method", ["put", "patch", "delete"])
def test_every_state_changing_method_is_guarded(client, method):
    call = getattr(client, method)
    assert call("/api/thing",
                headers={"origin": FOREIGN_ORIGIN}).status_code == 403
    assert call("/api/thing", headers={"origin": BOARD_ORIGIN}).status_code == 200


def test_get_is_not_guarded(client):
    assert client.get("/api/thing",
                      headers={"origin": FOREIGN_ORIGIN}).status_code == 200


def test_missing_origin_is_served(client):
    assert client.post("/api/thing").status_code == 200


def test_a_rebound_hostname_is_blocked(app):
    """The attacker owns the hostname, so Origin and Host agree. Refuse it."""
    rebound = TestClient(app, base_url="http://rebind.example")
    r = rebound.post("/api/thing", headers={"origin": "http://rebind.example"})
    assert r.status_code == 403


def test_the_hostname_a_proxy_received_is_served(client):
    """Caddy rewrites Host and keeps the original hostname in X-Forwarded-Host."""
    r = client.post("/api/thing",
                    headers={"origin": "https://board.example.com",
                             "x-forwarded-host": "board.example.com"})
    assert r.status_code == 200


def test_a_rebound_page_cannot_forge_the_forwarded_host(app):
    """The page is same-origin with the board, so it can set any header it likes.

    Only the Host header stops it, and the browser writes that one from the
    address of the page itself.
    """
    rebound = TestClient(app, base_url="http://rebind.example")
    r = rebound.post("/api/thing",
                     headers={"origin": "http://rebind.example",
                              "x-forwarded-host": "rebind.example"})
    assert r.status_code == 403


def test_a_foreign_origin_is_blocked_behind_a_proxy(client):
    r = client.post("/api/thing",
                    headers={"origin": FOREIGN_ORIGIN,
                             "x-forwarded-host": "board.example.com"})
    assert r.status_code == 403


def test_a_second_instance_hostname_is_served(client, tmp_path):
    state._configs_by_host["other.frshty.localhost"] = {
        "job": {"key": "other", "port": 7100},
        "_base_url": "https://other.frshty.localhost",
        "_state_dir": tmp_path}
    other = TestClient(client.app, base_url="http://other.frshty.localhost")
    r = other.post("/api/thing",
                   headers={"origin": "http://other.frshty.localhost"})
    assert r.status_code == 200


def test_one_instance_cannot_post_to_another(client):
    """Two instances of this board share a registrable domain but not an origin."""
    state._configs_by_host["other.frshty.localhost"] = {
        "job": {"key": "other", "port": 7100},
        "_base_url": "https://other.frshty.localhost"}
    r = client.post("/api/thing",
                    headers={"origin": "https://other.frshty.localhost"})
    assert r.status_code == 403


def test_same_origin_websocket_is_served(client):
    with client.websocket_connect(
            "/ws/terminal/work-1",
            headers={"host": BOARD_HOST, "origin": BOARD_ORIGIN}) as ws:
        assert ws.receive_text() == "served"


def test_foreign_origin_websocket_is_closed(client):
    with pytest.raises(WebSocketDisconnect) as caught:
        with client.websocket_connect(
                "/ws/terminal/work-1",
                headers={"host": BOARD_HOST, "origin": FOREIGN_ORIGIN}) as ws:
            ws.receive_text()
    assert caught.value.code == 1008


def test_opaque_origin_websocket_is_closed(client):
    with pytest.raises(WebSocketDisconnect) as caught:
        with client.websocket_connect(
                "/ws/terminal/work-1",
                headers={"host": BOARD_HOST, "origin": "null"}) as ws:
            ws.receive_text()
    assert caught.value.code == 1008


def test_rebound_hostname_websocket_is_closed(client):
    with pytest.raises(WebSocketDisconnect) as caught:
        with client.websocket_connect(
                "/ws/terminal/work-1",
                headers={"host": "rebind.example",
                         "origin": "http://rebind.example"}) as ws:
            ws.receive_text()
    assert caught.value.code == 1008


def test_proxied_websocket_is_served(client):
    with client.websocket_connect(
            "/ws/terminal/work-1",
            headers={"host": BOARD_HOST,
                     "origin": "https://board.example.com",
                     "x-forwarded-host": "board.example.com"}) as ws:
        assert ws.receive_text() == "served"


def test_missing_origin_websocket_is_served(client):
    with client.websocket_connect("/ws/terminal/work-1",
                                  headers={"host": BOARD_HOST}) as ws:
        assert ws.receive_text() == "served"


@pytest.mark.parametrize("origin_header,host,allowed", [
    ("http://127.0.0.1:7100", "127.0.0.1:7100", True),
    ("http://localhost:7100", "localhost:7100", True),
    ("http://[::1]:7100", "[::1]:7100", True),
    (f"https://{BOARD_HOST}", BOARD_HOST, True),
    (f"https://{BOARD_HOST}", f"{BOARD_HOST}:443", True),
    (f"http://{BOARD_HOST}", f"{BOARD_HOST}:80", True),
    (f"HTTPS://{BOARD_HOST.upper()}", BOARD_HOST, True),
    ("", BOARD_HOST, True),
    (None, BOARD_HOST, True),
    ("null", BOARD_HOST, False),
    ("http://localhost:3000", "localhost:7100", False),
    ("http://127.0.0.1:7100", "127.0.0.1:7101", False),
    (f"https://{BOARD_HOST}", "other.frshty.localhost", False),
    (f"http://{BOARD_HOST}", f"{BOARD_HOST}:443", False),
    (BOARD_HOST, BOARD_HOST, False),
    (f"https://user@{BOARD_HOST}", BOARD_HOST, False),
    (f"https://{BOARD_HOST}", "", False),
    (f"https://{BOARD_HOST}.evil.example", BOARD_HOST, False),
    ("http://rebind.example", "rebind.example", False),
    ("http://rebind.example:7100", "rebind.example:7100", False),
    ("data:text/html,x", BOARD_HOST, False),
    ("file://", BOARD_HOST, False),
    ("chrome-extension://abc", BOARD_HOST, False),
])
def test_origin_allowed_rule(origin_header, host, allowed):
    assert origin_allowed(origin_header, host) is allowed


@pytest.mark.parametrize("origin_header,forwarded,allowed", [
    ("https://board.example.com", "board.example.com", True),
    ("https://board.example.com", "board.example.com:443", True),
    ("https://board.example.com", "other.example.com", False),
    ("https://evil.example", "board.example.com", False),
    ("null", "board.example.com", False),
])
def test_forwarded_host_rule(origin_header, forwarded, allowed):
    """The proxy rewrote Host to a hostname the board declares.

    The browser's Origin names the public hostname instead, so it never matches
    Host and only X-Forwarded-Host can answer for it.
    """
    assert origin_allowed(origin_header, BOARD_HOST, forwarded) is allowed


@pytest.mark.parametrize("host", ["rebind.example", "internal.rewritten", ""])
def test_a_forwarded_host_never_answers_for_an_undeclared_host(host):
    """X-Forwarded-Host is trusted only behind a Host this board declares."""
    assert origin_allowed("https://board.example.com", host,
                          "board.example.com") is False


def test_a_sender_is_reported_once():
    assert origin.report_once(FOREIGN_ORIGIN, BOARD_HOST) is True
    assert origin.report_once(FOREIGN_ORIGIN, BOARD_HOST) is False
    assert origin.report_once(FOREIGN_ORIGIN, "other.frshty.localhost") is True


def test_reporting_stops_at_the_cap_but_blocking_does_not(client):
    for n in range(origin.MAX_REPORTED_SENDERS + 5):
        assert origin.report_once(f"https://evil{n}.example", BOARD_HOST) is (
            n < origin.MAX_REPORTED_SENDERS)
    assert len(origin._reported_senders) == origin.MAX_REPORTED_SENDERS
    assert client.post("/api/thing",
                       headers={"origin": "https://late.example"}).status_code == 403


@pytest.fixture()
def real_client(tmp_path):
    """The app frshty.py builds, so the wiring in frshty.py is under test too."""
    import core.log as log
    import core.state as core_state

    core_state.init(tmp_path)
    log.init(tmp_path, "test")
    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        if "frshty" in sys.modules:
            frshty = sys.modules["frshty"]
        else:
            import frshty
    finally:
        sys.argv = saved_argv
    state.set_primary_config({
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {"root": tmp_path, "tickets_dir": "tickets",
                      "ticket_layout": "flat", "base_branch": "main"},
        "features": {"reviews": True, "slack": False},
        "pr": {"auto_pr": True},
        "slack": {},
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://testserver",
    })
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


REAL_ROUTES = [
    ("post", "/api/poll"),
    ("post", "/api/events/dismiss-all"),
    ("post", "/api/tickets/ABC-1/status"),
    ("post", "/api/reviews/submit"),
    ("post", "/api/work/items/1/approve"),
    ("put", "/api/settings"),
    ("patch", "/api/tickets/ABC-1/auto-pr"),
    ("delete", "/api/tickets/ABC-1"),
]


@pytest.mark.parametrize("method,path", REAL_ROUTES)
def test_real_route_blocks_a_foreign_origin(real_client, method, path):
    call = getattr(real_client, method)
    assert call(path, headers={"origin": FOREIGN_ORIGIN}).status_code == 403
    assert call(path, headers={"origin": "null"}).status_code == 403
    assert call(path, headers={"origin": "http://testserver"}).status_code != 403


@pytest.mark.parametrize("path", ["/ws/terminal/ABC-1", "/ws/discuss/sess-1"])
@pytest.mark.parametrize("origin_header", [FOREIGN_ORIGIN, "null"])
def test_real_socket_closes_a_foreign_origin(real_client, path, origin_header):
    with pytest.raises(WebSocketDisconnect) as caught:
        with real_client.websocket_connect(
                path, headers={"origin": origin_header}) as ws:
            ws.receive()
    assert caught.value.code == 1008
