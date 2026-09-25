import httpx
import pytest
from fastapi.testclient import TestClient

import gateway
from services import work_peers


@pytest.fixture
def peers(tmp_path, monkeypatch):
    path = tmp_path / "peers.toml"
    path.write_text('[[peers]]\nkey = "frshty"\nbase_url = "http://127.0.0.1:7131"\n'
                    '[[peers]]\nkey = "quill"\nbase_url = "http://127.0.0.1:7134"\nlabel = "Quill"\n')
    monkeypatch.setattr(work_peers, "PEERS_PATH", path)
    monkeypatch.setattr(work_peers, "_cache", None)
    monkeypatch.delenv("FRSHTY_PEER_SELF", raising=False)
    return path


@pytest.fixture
def upstream(monkeypatch):
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path == "/page":
            return httpx.Response(200, headers={"content-type": "text/html; charset=utf-8"},
                                  content=b"<html><body><h1>Tickets</h1></body></html>")
        if request.url.path == "/cookies":
            return httpx.Response(200, headers=[("set-cookie", "a=1; Path=/"), ("set-cookie", "b=2; Path=/")],
                                  json={"query": str(request.url.query, "ascii") if isinstance(request.url.query, bytes) else request.url.query})
        if request.url.path == "/moved":
            return httpx.Response(303, headers={"location": "http://127.0.0.1:7134/tickets/DEV-1"})
        return httpx.Response(201, json={"from": request.url.host + ":" + str(request.url.port),
                                         "path": request.url.path})

    monkeypatch.setattr(gateway, "client", httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    return seen


@pytest.fixture
def client():
    return TestClient(gateway.app, follow_redirects=False)


class TestPicker:
    def test_lists_instances_and_defaults_to_the_first(self, peers, client):
        assert client.get("/api/gateway/instances").json() == {
            "current": "frshty",
            "instances": [{"key": "frshty", "label": "frshty"}, {"key": "quill", "label": "Quill"}]}

    def test_select_sets_the_cookie_and_returns_to_the_page(self, peers, client):
        resp = client.get("/api/gateway/select", params={"key": "quill", "next": "/tickets?x=1"})
        assert resp.status_code == 303
        assert resp.headers["location"] == "/tickets?x=1"
        assert "frshty_instance=quill" in resp.headers["set-cookie"]

    def test_select_refuses_an_unknown_instance(self, peers, client):
        assert client.get("/api/gateway/select", params={"key": "nope"}).status_code == 404

    def test_select_never_redirects_off_the_gateway(self, peers, client):
        resp = client.get("/api/gateway/select", params={"key": "quill", "next": "//evil.example/x"})
        assert resp.headers["location"] == "/"


class TestForward:
    def test_default_instance_gets_the_request(self, peers, upstream, client):
        resp = client.post("/api/tickets/DEV-1/approve?force=1", json={"a": 1})
        assert resp.status_code == 201
        assert resp.json() == {"from": "127.0.0.1:7131", "path": "/api/tickets/DEV-1/approve"}
        sent = upstream[0]
        assert sent.method == "POST"
        assert sent.url.params["force"] == "1"
        assert sent.content == b'{"a":1}'
        assert sent.headers["host"] == "127.0.0.1:7131"

    def test_the_cookie_routes_to_the_picked_instance(self, peers, upstream, client):
        client.cookies.set("frshty_instance", "quill")
        assert client.get("/reviews").json()["from"] == "127.0.0.1:7134"

    def test_a_stale_cookie_falls_back_to_the_first_instance(self, peers, upstream, client):
        client.cookies.set("frshty_instance", "gone")
        assert client.get("/reviews").json()["from"] == "127.0.0.1:7131"

    def test_a_redirect_to_the_instance_stays_on_the_gateway(self, peers, upstream, client):
        client.cookies.set("frshty_instance", "quill")
        resp = client.get("/moved")
        assert resp.status_code == 303
        assert resp.headers["location"] == "/tickets/DEV-1"

    def test_every_html_page_gets_the_picker(self, peers, upstream, client):
        resp = client.get("/page")
        assert resp.text == ('<html><body><h1>Tickets</h1>'
                             '<script src="/api/gateway/picker.js"></script></body></html>')
        assert client.get("/api/gateway/picker.js").text.startswith("(function ()")

    def test_json_passes_through_untouched(self, peers, upstream, client):
        assert "picker" not in client.get("/api/x").text

    def test_repeated_query_params_and_cookies_survive(self, peers, upstream, client):
        resp = client.get("/cookies?repo=a&repo=b")
        assert resp.json() == {"query": "repo=a&repo=b"}
        assert resp.headers.get_list("set-cookie") == ["a=1; Path=/", "b=2; Path=/"]

    def test_an_encoded_question_mark_stays_in_the_path(self, peers, upstream, client):
        client.get("/artifacts/report%3Ffinal.html")
        assert upstream[-1].url.raw_path == b"/artifacts/report%3Ffinal.html"

    def test_a_redirect_is_never_made_scheme_relative(self):
        base = "http://127.0.0.1:7131"
        assert gateway._local_location(base + "//evil.example/x", base) == base + "//evil.example/x"
        assert gateway._local_location("http://127.0.0.1:71310/x", base) == "http://127.0.0.1:71310/x"
        assert gateway._local_location(base, base) == "/"
        assert gateway._local_location(base + "/tickets", base) == "/tickets"

    def test_an_unreachable_instance_answers_502(self, peers, client, monkeypatch):
        def handler(request):
            raise httpx.ConnectError("refused")

        monkeypatch.setattr(gateway, "client", httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        resp = client.get("/tickets")
        assert resp.status_code == 502
        assert "frshty" in resp.json()["error"]

    def test_no_instances_answers_503(self, tmp_path, monkeypatch, client):
        monkeypatch.setattr(work_peers, "PEERS_PATH", tmp_path / "missing.toml")
        monkeypatch.setattr(work_peers, "_cache", None)
        assert client.get("/tickets").status_code == 503
