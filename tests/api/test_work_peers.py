import asyncio
import threading
import time

import httpx
import pytest
import websockets
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import core.log as log
from services import work_peers
from web import work as work_routes


@pytest.fixture
def peers_file(tmp_path, monkeypatch):
    path = tmp_path / "peers.toml"
    monkeypatch.setattr(work_peers, "PEERS_PATH", path)
    monkeypatch.setattr(work_peers, "_cache", None)
    return path


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(work_routes.router)
    return TestClient(app)


class _StubClient:
    def __init__(self, calls, response):
        self.calls = calls
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None):
        self.calls.append(("GET", url, params, None))
        return self.response

    async def post(self, url, params=None, json=None):
        self.calls.append(("POST", url, params, json))
        return self.response


class _StubResponse:
    def __init__(self, status_code=200, payload=None, text="", content_type="application/json"):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = {"content-type": content_type}

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


def _stub_httpx(monkeypatch, response):
    calls = []
    monkeypatch.setattr(work_peers.httpx, "AsyncClient",
                        lambda **kw: _StubClient(calls, response))
    return calls


class TestPeerList:
    def test_no_file_means_no_peers(self, peers_file):
        assert work_peers.peers() == []

    def test_reads_key_base_url_and_label(self, peers_file):
        peers_file.write_text(
            '[[peers]]\nkey = "astroco"\nbase_url = "http://10.0.0.2:7100/"\n'
            'label = "Astroco"\n')
        assert work_peers.peers() == [
            {"key": "astroco", "base_url": "http://10.0.0.2:7100", "label": "Astroco"}]

    def test_own_entry_is_skipped(self, peers_file, monkeypatch):
        monkeypatch.setenv("FRSHTY_PEER_SELF", "frshty")
        peers_file.write_text('[[peers]]\nkey = "frshty"\nbase_url = "http://x:1"\n'
                              '[[peers]]\nkey = "quartz"\nbase_url = "http://x:2"\n')
        assert [p["key"] for p in work_peers.peers()] == ["quartz"]

    def test_label_defaults_to_key(self, peers_file):
        peers_file.write_text('[[peers]]\nkey = "astroco"\nbase_url = "http://x:1"\n')
        assert work_peers.peers()[0]["label"] == "astroco"

    def test_incomplete_and_duplicate_entries_are_dropped(self, peers_file):
        peers_file.write_text(
            '[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n'
            '[[peers]]\nkey = "a"\nbase_url = "http://y:2"\n'
            '[[peers]]\nkey = ""\nbase_url = "http://z:3"\n'
            '[[peers]]\nkey = "b"\n')
        assert [p["key"] for p in work_peers.peers()] == ["a"]

    def test_broken_file_reports_the_failure(self, peers_file, monkeypatch):
        emitted = []
        monkeypatch.setattr(log, "emit",
                            lambda event, summary, **kw: emitted.append(event))
        peers_file.write_text("this is not toml = = =")
        assert work_peers.peers() == []
        assert "work_peers_load_error" in emitted

    def test_edited_file_is_reread(self, peers_file):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        assert [p["key"] for p in work_peers.peers()] == ["a"]
        peers_file.write_text('[[peers]]\nkey = "b"\nbase_url = "http://x:1"\n')
        import os
        os.utime(peers_file, (1, 1))
        assert [p["key"] for p in work_peers.peers()] == ["b"]


class TestPeerRequest:
    def test_unknown_peer_is_404(self, peers_file):
        status, payload = asyncio.run(work_peers.request("ghost", "GET", "api/work/items"))
        assert status == 404
        assert "unknown peer" in payload["error"]

    def test_only_the_work_api_is_reachable(self, peers_file):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        for path in ("api/config", "api/work/../config", "tasks"):
            status, payload = asyncio.run(work_peers.request("a", "GET", path))
            assert status == 403, path
            assert "not allowed" in payload["error"]

    def test_get_is_forwarded_with_the_query(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"groups": {}}))
        status, payload = asyncio.run(work_peers.request("a", "GET", "api/work/items",
                                                         params={"q": "release"}))
        assert status == 200
        assert payload == {"groups": {}}
        assert calls == [("GET", "http://x:1/api/work/items", {"q": "release"}, None)]

    def test_post_is_forwarded_with_the_body(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"item_id": 7}))
        status, payload = asyncio.run(work_peers.request("a", "POST", "/api/work/intake",
                                                         body={"text": "go"}))
        assert (status, payload) == (200, {"item_id": 7})
        assert calls == [("POST", "http://x:1/api/work/intake", {}, {"text": "go"})]

    def test_peer_status_is_passed_through(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        _stub_httpx(monkeypatch, _StubResponse(status_code=409, payload={"error": "busy"}))
        status, payload = asyncio.run(work_peers.request("a", "POST", "api/work/items/3/reply",
                                                         body={"text": "hi"}))
        assert (status, payload) == (409, {"error": "busy"})

    def test_unreachable_peer_is_502_and_is_logged(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        emitted = []
        monkeypatch.setattr(log, "emit",
                            lambda event, summary, **kw: emitted.append(event))

        def boom(**kw):
            raise httpx.ConnectError("no route to host")

        monkeypatch.setattr(work_peers.httpx, "AsyncClient", boom)
        status, payload = asyncio.run(work_peers.request("a", "GET", "api/work/items"))
        assert status == 502
        assert "unreachable" in payload["error"]
        assert "work_peer_unreachable" in emitted

    def test_slow_peer_is_cut_at_the_total_timeout(self, peers_file, monkeypatch):
        emitted = []
        monkeypatch.setattr(log, "emit",
                            lambda event, summary, **kw: emitted.append(event))

        async def dribble(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
                         b"content-length: 3\r\n\r\n")
            for byte in (b"{", b" ", b"}"):
                await asyncio.sleep(0.4)
                writer.write(byte)
                await writer.drain()
            writer.close()

        async def run():
            server = await asyncio.start_server(dribble, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            peers_file.write_text(f'[[peers]]\nkey = "a"\nbase_url = "http://127.0.0.1:{port}"\n')
            async with server:
                started = time.monotonic()
                result = await work_peers.request("a", "GET", "api/work/items", timeout=0.6)
                return result, time.monotonic() - started

        (status, payload), elapsed = asyncio.run(run())
        assert status == 502
        assert "no answer within 0.6s" in payload["error"]
        assert elapsed < 1.0
        assert "work_peer_unreachable" in emitted

    def test_non_json_answer_is_502_and_is_logged(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        emitted = []
        monkeypatch.setattr(log, "emit",
                            lambda event, summary, **kw: emitted.append(event))
        _stub_httpx(monkeypatch, _StubResponse(status_code=502, text="<html>",
                                               content_type="text/html"))
        status, payload = asyncio.run(work_peers.request("a", "GET", "api/work/items"))
        assert status == 502
        assert "not JSON" in payload["error"]
        assert "work_peer_bad_response" in emitted


class TestPeerRoutes:
    def test_peer_list_route(self, peers_file, client):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        resp = client.get("/api/work/peers")
        assert resp.status_code == 200
        assert resp.json() == {"peers": [{"key": "a", "base_url": "http://x:1",
                                          "label": "a"}]}

    def test_get_proxy_route(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"counts": {"done": 2}}))
        resp = client.get("/api/work/peers/a/api/work/items?done_page=3")
        assert resp.status_code == 200
        assert resp.json() == {"counts": {"done": 2}}
        assert calls[0][:3] == ("GET", "http://x:1/api/work/items", {"done_page": "3"})

    def test_post_proxy_route(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"ok": True}))
        resp = client.post("/api/work/peers/a/api/work/items/4/action",
                           json={"action": "ack"})
        assert resp.status_code == 200
        assert calls[0] == ("POST", "http://x:1/api/work/items/4/action", {},
                            {"action": "ack"})

    def test_post_proxy_route_without_a_body(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"archived": 0}))
        resp = client.post("/api/work/peers/a/api/work/items/archive-completed")
        assert resp.status_code == 200
        assert calls[0] == ("POST", "http://x:1/api/work/items/archive-completed",
                            {}, {})

    def test_thread_archive_is_forwarded_to_the_peer(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"root_id": 4, "archived": 2}))
        resp = client.post("/api/work/peers/a/api/work/threads/4/archive")
        assert resp.status_code == 200
        assert resp.json() == {"root_id": 4, "archived": 2}
        assert calls[0] == ("POST", "http://x:1/api/work/threads/4/archive", {}, {})

    def test_proxy_route_refuses_a_non_work_path(self, peers_file, client):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        resp = client.get("/api/work/peers/a/api/config")
        assert resp.status_code == 403

    def test_local_board_route_still_answers(self, peers_file, client):
        resp = client.get("/api/work/items")
        assert resp.status_code == 200
        assert "groups" in resp.json()


class _PeerStream(httpx.AsyncByteStream):
    def __init__(self, data=b""):
        self.data = data

    async def __aiter__(self):
        yield self.data


def _mock_peer(monkeypatch, handler):
    real_client = httpx.AsyncClient
    seen = []

    def record(request):
        seen.append(request)
        return handler(request)

    monkeypatch.setattr(work_peers.httpx, "AsyncClient",
                        lambda **kw: real_client(transport=httpx.MockTransport(record), **kw))
    return seen


class TestPeerArtifactFile:
    def test_file_is_served_from_this_board(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        seen = _mock_peer(monkeypatch, lambda r: httpx.Response(
            200, stream=_PeerStream(b"\x89PNG data"),
            headers={"content-type": "image/png", "content-security-policy": "sandbox",
                     "set-cookie": "s=1"}))
        resp = client.get("/api/work/peers/a/api/work/artifact_file/7")
        assert resp.status_code == 200
        assert resp.content == b"\x89PNG data"
        assert resp.headers["content-type"] == "image/png"
        assert resp.headers["content-security-policy"] == "sandbox"
        assert "set-cookie" not in resp.headers
        assert str(seen[0].url) == "http://x:1/api/work/artifact_file/7"

    def test_page_redirect_stays_inside_the_proxy(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        _mock_peer(monkeypatch, lambda r: httpx.Response(
            307, stream=_PeerStream(), headers={"location": "/api/work/artifact_file/7/"}))
        resp = client.get("/api/work/peers/a/api/work/artifact_file/7", follow_redirects=False)
        assert resp.status_code == 307
        assert resp.headers["location"] == "/api/work/peers/a/api/work/artifact_file/7/"

    def test_page_folder_keeps_its_trailing_slash(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        seen = _mock_peer(monkeypatch, lambda r: httpx.Response(
            200, stream=_PeerStream(b"<html></html>"), headers={"content-type": "text/html"}))
        resp = client.get("/api/work/peers/a/api/work/artifact_file/7/", follow_redirects=False)
        assert resp.status_code == 200
        assert str(seen[0].url) == "http://x:1/api/work/artifact_file/7/"

    def test_page_asset_is_proxied(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        seen = _mock_peer(monkeypatch, lambda r: httpx.Response(
            200, stream=_PeerStream(b"<html></html>"), headers={"content-type": "text/html"}))
        resp = client.get("/api/work/peers/a/api/work/artifact_file/7/img/a.png")
        assert resp.status_code == 200
        assert str(seen[0].url) == "http://x:1/api/work/artifact_file/7/img/a.png"

    def test_range_header_is_forwarded(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        seen = _mock_peer(monkeypatch, lambda r: httpx.Response(
            206, stream=_PeerStream(b"01"),
            headers={"content-type": "video/mp4", "content-range": "bytes 0-1/10"}))
        resp = client.get("/api/work/peers/a/api/work/artifact_file/7",
                          headers={"range": "bytes=0-1"})
        assert resp.status_code == 206
        assert resp.headers["content-range"] == "bytes 0-1/10"
        assert seen[0].headers["range"] == "bytes=0-1"

    def test_redirect_outside_the_artifact_files_is_refused(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        _mock_peer(monkeypatch, lambda r: httpx.Response(
            302, stream=_PeerStream(), headers={"location": "/api/config"}))
        resp = client.get("/api/work/peers/a/api/work/artifact_file/7", follow_redirects=False)
        assert resp.status_code == 502
        assert "redirected outside" in resp.json()["error"]

    def test_unknown_peer_is_404(self, peers_file, client):
        resp = client.get("/api/work/peers/nope/api/work/artifact_file/7")
        assert resp.status_code == 404
        assert "unknown peer" in resp.json()["error"]

    def test_unreachable_peer_is_502_and_is_logged(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        emitted = []
        monkeypatch.setattr(log, "emit", lambda kind, msg, **kw: emitted.append(kind))

        def refuse(request):
            raise httpx.ConnectError("refused")

        _mock_peer(monkeypatch, refuse)
        resp = client.get("/api/work/peers/a/api/work/artifact_file/7")
        assert resp.status_code == 502
        assert "unreachable" in resp.json()["error"]
        assert emitted == ["work_peer_unreachable"]


@pytest.fixture
def peer_terminal():
    """A websocket server that stands in for a peer's /ws/terminal route."""
    seen = []
    ready = threading.Event()
    box = {}

    async def handler(conn):
        seen.append(conn.request.path)
        if conn.request.path == "/ws/terminal/artifact-9":
            await conn.close(code=1008)
            return
        await conn.send(b"scrollback")
        async for message in conn:
            await conn.send(message)

    async def serve():
        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            box["port"] = server.sockets[0].getsockname()[1]
            box["stop"] = asyncio.get_running_loop().create_future()
            box["loop"] = asyncio.get_running_loop()
            ready.set()
            await box["stop"]

    thread = threading.Thread(target=asyncio.run, args=(serve(),), daemon=True)
    thread.start()
    ready.wait(5)
    yield box["port"], seen
    box["loop"].call_soon_threadsafe(box["stop"].set_result, None)
    thread.join(5)


class TestPeerTerminalProxy:
    def test_only_an_artifact_terminal_is_reachable(self, peers_file):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "https://10.0.0.2:7100"\n')
        assert work_peers.terminal_url("a", "artifact-12") == ("wss://10.0.0.2:7100/ws/terminal/artifact-12", "")
        assert work_peers.terminal_url("a", "work-12")[1] == "peer terminal not allowed: work-12"
        assert work_peers.terminal_url("a", "artifact-12-discuss")[1].startswith("peer terminal not allowed")
        assert work_peers.terminal_url("b", "artifact-12")[1] == "unknown peer 'b'"

    def test_frames_are_relayed_both_ways(self, peers_file, client, peer_terminal):
        port, seen = peer_terminal
        peers_file.write_text(f'[[peers]]\nkey = "a"\nbase_url = "http://127.0.0.1:{port}"\n')
        with client.websocket_connect("/ws/peers/a/terminal/artifact-7") as ws:
            assert ws.receive_bytes() == b"scrollback"
            ws.send_text("ls\r")
            assert ws.receive_text() == "ls\r"
            ws.send_bytes(b"\x03")
            assert ws.receive_bytes() == b"\x03"
        assert seen == ["/ws/terminal/artifact-7"]

    def test_peer_refusal_reaches_the_browser_as_1008(self, peers_file, client, peer_terminal):
        port, _ = peer_terminal
        peers_file.write_text(f'[[peers]]\nkey = "a"\nbase_url = "http://127.0.0.1:{port}"\n')
        with client.websocket_connect("/ws/peers/a/terminal/artifact-9") as ws:
            with pytest.raises(WebSocketDisconnect) as exc:
                ws.receive_bytes()
        assert exc.value.code == 1008

    def test_non_artifact_terminal_is_refused(self, peers_file, client, peer_terminal):
        port, seen = peer_terminal
        peers_file.write_text(f'[[peers]]\nkey = "a"\nbase_url = "http://127.0.0.1:{port}"\n')
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect("/ws/peers/a/terminal/work-7"):
                pass
        assert exc.value.code == 1008
        assert seen == []

    def test_unreachable_peer_is_1008_and_is_logged(self, peers_file, client, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://127.0.0.1:1"\n')
        emitted = []
        monkeypatch.setattr(log, "emit", lambda event, *a, **kw: emitted.append(event))
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect("/ws/peers/a/terminal/artifact-7"):
                pass
        assert exc.value.code == 1008
        assert emitted == ["work_peer_unreachable"]

    def test_opaque_origin_is_refused(self, peers_file, client):
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect("/ws/peers/a/terminal/artifact-7", headers={"origin": "null"}):
                pass
        assert exc.value.code == 1008


class TestPeerArtifactLinks:
    def _read(self, path):
        return (work_peers.Path(__file__).parent.parent.parent / path).read_text()

    def test_viewer_reads_a_peer_artifact_through_the_proxy(self):
        text = self._read("templates/work_artifact.html")
        assert 'const apiBase = peer ? "/api/work/peers/" + encodeURIComponent(peer) : "";' in text
        assert 'const fileUrl = apiBase + "/api/work/artifact_file/" + id;' in text
        assert 'fetch(apiBase + "/api/work/artifacts/" + id)' in text

    def test_viewer_asks_claude_on_the_peer(self):
        text = self._read("templates/work_artifact.html")
        assert 'getElementById("bottom").style.display' not in text
        assert "fetch(`${apiBase}/api/work/artifacts/${id}/claude/start`" in text
        assert "`/ws/peers/${encodeURIComponent(peer)}/terminal/artifact-${id}`" in text

    def test_no_page_sends_the_browser_to_a_peer_base_url_for_an_artifact(self):
        for path in ("templates/work_detail.html", "templates/work_summary.html",
                     "templates/thread_detail.html", "static/frshty-redesign.js"):
            text = self._read(path)
            assert '"/artifacts/" + a.id + (' in text, path
            assert ') + "/artifacts/"' not in text, path
            assert ') + "/api/work/artifact_file/"' not in text, path
