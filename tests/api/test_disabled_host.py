"""A disabled instance must not be reachable, over HTTP or WebSocket.

Leaving its hostname unregistered is not enough: an unregistered host falls
back to the primary config, so the request would be served by a different
instance under this instance's name.
"""
import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient

import web.middleware as middleware
from web.state import _disabled_hosts, host_is_disabled


@pytest.fixture
def app():
    a = FastAPI()
    middleware.install(a)

    @a.get("/api/thing")
    def thing():
        return {"served": True}

    @a.websocket("/ws/thing")
    async def ws_thing(websocket: WebSocket):
        if host_is_disabled(websocket.headers.get("host")):
            await websocket.close(code=1011, reason="instance disabled")
            return
        await websocket.accept()
        await websocket.send_text("served")
        await websocket.close()

    return a


@pytest.fixture(autouse=True)
def clean():
    _disabled_hosts.clear()
    yield
    _disabled_hosts.clear()


def test_enabled_host_is_served(app):
    c = TestClient(app)
    assert c.get("/api/thing", headers={"host": "good.frshty.localhost"}).status_code == 200


def test_disabled_host_gets_503(app):
    _disabled_hosts.add("bad.frshty.localhost")
    c = TestClient(app)
    r = c.get("/api/thing", headers={"host": "bad.frshty.localhost"})
    assert r.status_code == 503
    assert "disabled" in r.json()["error"]


def test_disabled_host_ignores_the_port(app):
    _disabled_hosts.add("bad.frshty.localhost")
    c = TestClient(app)
    assert c.get("/api/thing", headers={"host": "bad.frshty.localhost:7100"}).status_code == 503


def test_disabled_host_websocket_is_closed(app):
    _disabled_hosts.add("bad.frshty.localhost")
    c = TestClient(app)
    with pytest.raises(Exception):
        with c.websocket_connect("/ws/thing", headers={"host": "bad.frshty.localhost"}) as ws:
            ws.receive_text()


def test_enabled_host_websocket_is_served(app):
    c = TestClient(app)
    with c.websocket_connect("/ws/thing", headers={"host": "good.frshty.localhost"}) as ws:
        assert ws.receive_text() == "served"
