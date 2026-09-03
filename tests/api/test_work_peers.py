
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, url, params=None):
        self.calls.append(("GET", url, params, None))
        return self.response

    def post(self, url, params=None, json=None):
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
    monkeypatch.setattr(work_peers.httpx, "Client",
                        lambda **kw: _StubClient(calls, response))
    return calls


class TestPeerList:
    def test_no_file_means_no_peers(self, peers_file):
        assert work_peers.peers() == []

    def test_reads_key_base_url_and_label(self, peers_file):
        peers_file.write_text(
            '[[peers]]\nkey = "atropos"\nbase_url = "http://10.0.0.2:7100/"\n'
            'label = "Atropos"\n')
        assert work_peers.peers() == [
            {"key": "atropos", "base_url": "http://10.0.0.2:7100", "label": "Atropos"}]

    def test_label_defaults_to_key(self, peers_file):
        peers_file.write_text('[[peers]]\nkey = "atropos"\nbase_url = "http://x:1"\n')
        assert work_peers.peers()[0]["label"] == "atropos"

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
        status, payload = work_peers.request("ghost", "GET", "api/work/items")
        assert status == 404
        assert "unknown peer" in payload["error"]

    def test_only_the_work_api_is_reachable(self, peers_file):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        for path in ("api/config", "api/work/../config", "tasks"):
            status, payload = work_peers.request("a", "GET", path)
            assert status == 403, path
            assert "not allowed" in payload["error"]

    def test_get_is_forwarded_with_the_query(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"groups": {}}))
        status, payload = work_peers.request("a", "GET", "api/work/items",
                                             params={"q": "release"})
        assert status == 200
        assert payload == {"groups": {}}
        assert calls == [("GET", "http://x:1/api/work/items", {"q": "release"}, None)]

    def test_post_is_forwarded_with_the_body(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        calls = _stub_httpx(monkeypatch, _StubResponse(payload={"item_id": 7}))
        status, payload = work_peers.request("a", "POST", "/api/work/intake",
                                             body={"text": "go"})
        assert (status, payload) == (200, {"item_id": 7})
        assert calls == [("POST", "http://x:1/api/work/intake", {}, {"text": "go"})]

    def test_peer_status_is_passed_through(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        _stub_httpx(monkeypatch, _StubResponse(status_code=409, payload={"error": "busy"}))
        status, payload = work_peers.request("a", "POST", "api/work/items/3/reply",
                                             body={"text": "hi"})
        assert (status, payload) == (409, {"error": "busy"})

    def test_unreachable_peer_is_502_and_is_logged(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        emitted = []
        monkeypatch.setattr(log, "emit",
                            lambda event, summary, **kw: emitted.append(event))

        def boom(**kw):
            raise httpx.ConnectError("no route to host")

        monkeypatch.setattr(work_peers.httpx, "Client", boom)
        status, payload = work_peers.request("a", "GET", "api/work/items")
        assert status == 502
        assert "unreachable" in payload["error"]
        assert "work_peer_unreachable" in emitted

    def test_non_json_answer_is_502_and_is_logged(self, peers_file, monkeypatch):
        peers_file.write_text('[[peers]]\nkey = "a"\nbase_url = "http://x:1"\n')
        emitted = []
        monkeypatch.setattr(log, "emit",
                            lambda event, summary, **kw: emitted.append(event))
        _stub_httpx(monkeypatch, _StubResponse(status_code=502, text="<html>",
                                               content_type="text/html"))
        status, payload = work_peers.request("a", "GET", "api/work/items")
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
