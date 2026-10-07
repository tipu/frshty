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
        if request.url.path == "/head":
            return httpx.Response(200, headers={"content-type": "text/html"},
                                  content=b"<html><head lang=en><title>T</title></head><body></body></html>")
        if request.url.path.startswith("/artifact/"):
            if request.url.path == "/artifact/5":
                return httpx.Response(307, headers={"location": "/artifact/5/"})
            if request.url.path == "/artifact/5/":
                return httpx.Response(200, headers={"content-type": "text/html",
                                                    "content-security-policy": "sandbox allow-scripts; frame-src 'none'"},
                                      content=b"<html><head></head><body><img src=a.png></body></html>")
            return httpx.Response(200, headers={"content-type": "image/png", "content-security-policy": "sandbox"},
                                  content=request.url.host.encode() + b":" + str(request.url.port).encode())
        if request.url.path == "/api/work/peers":
            return httpx.Response(200, json={"peers": [
                {"key": "quill", "base_url": "http://127.0.0.1:7134", "label": "Quill"},
                {"key": "atropos", "base_url": "http://192.168.1.117:7100", "label": "atropos"}]})
        if request.url.path == "/api/global/events":
            return httpx.Response(200, json={"events": [
                {"id": "1", "instance_key": "frshty", "base_url": "", "links": {"detail": "/tasks/7"}},
                {"id": "2", "instance_key": "quill", "base_url": "http://127.0.0.1:7134", "links": {}},
                {"id": "3", "instance_key": "atropos", "base_url": "http://192.168.1.117:7100", "links": {}}],
                "errors": {}})
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


class TestPin:
    def test_peer_links_point_at_the_gateway(self, peers, upstream, client):
        assert client.get("/api/work/peers").json() == {"peers": [
            {"key": "quill", "base_url": "/api/gateway/at/quill", "label": "Quill"},
            {"key": "atropos", "base_url": "http://192.168.1.117:7100", "label": "atropos"}]}

    def test_global_event_links_point_at_the_gateway(self, peers, upstream, client):
        events = client.get("/api/global/events").json()["events"]
        assert [e["base_url"] for e in events] == [
            "/api/gateway/at/frshty", "/api/gateway/at/quill", "http://192.168.1.117:7100"]
        assert events[0]["links"] == {"detail": "/tasks/7"}

    def test_a_peer_path_redirects_to_a_pinned_page(self, peers, client):
        resp = client.get("/api/gateway/at/quill/tasks/5/terminal?x=1&frshty_instance=frshty")
        assert resp.status_code == 303
        assert resp.headers["location"] == "/tasks/5/terminal?x=1&frshty_instance=quill"
        assert "set-cookie" not in resp.headers
        assert client.get("/api/gateway/at/quill").headers["location"] == "/?frshty_instance=quill"

    def test_a_peer_path_never_leaves_the_gateway(self, peers, client):
        resp = client.get("/api/gateway/at/quill//evil.example/x")
        assert resp.headers["location"] == "/evil.example/x?frshty_instance=quill"

    def test_a_peer_path_refuses_an_unknown_instance(self, peers, client):
        assert client.get("/api/gateway/at/nope/tasks").status_code == 404

    def test_the_pin_beats_the_cookie_and_is_not_forwarded(self, peers, upstream, client):
        client.cookies.set("frshty_instance", "frshty")
        resp = client.get("/api/x?a=1&frshty_instance=quill&b=2")
        assert resp.json()["from"] == "127.0.0.1:7134"
        assert str(upstream[-1].url.query, "ascii") == "a=1&b=2"
        assert client.get("/api/gateway/instances?frshty_instance=quill").json()["current"] == "quill"
        assert client.get("/api/x").json()["from"] == "127.0.0.1:7131"

    def test_an_unknown_pin_answers_404(self, peers, upstream, client):
        assert client.get("/api/x?frshty_instance=nope").status_code == 404
        assert upstream == []

    def test_a_pinned_page_loads_the_pin_script_first(self, peers, upstream, client):
        resp = client.get("/head?frshty_instance=quill")
        assert resp.text.startswith('<html><head lang=en><script src="/api/gateway/pin.js"></script><title>')
        assert "pin.js" not in client.get("/head").text
        assert client.get("/api/gateway/pin.js").text.startswith("(function ()")

    def test_a_redirect_in_a_pinned_tab_stays_pinned(self, peers, upstream, client):
        resp = client.get("/moved?frshty_instance=quill")
        assert resp.headers["location"] == "/tickets/DEV-1?frshty_instance=quill"

    def test_a_navigation_from_a_pinned_page_stays_pinned(self, peers, upstream, client):
        resp = client.get("/tasks/9?x=1", headers={
            "referer": "http://testserver/tasks/5?frshty_instance=quill"})
        assert resp.status_code == 303
        assert resp.headers["location"] == "/tasks/9?x=1&frshty_instance=quill"
        assert upstream == []

    def test_a_subresource_of_a_pinned_page_gets_its_own_url(self, peers, upstream, client):
        resp = client.get("/api/work/items/5/transcript-image/0-0", headers={
            "referer": "https://personal.frshty.local/tasks/5?frshty_instance=quill"})
        assert resp.status_code == 303
        assert resp.headers["location"] == "/api/work/items/5/transcript-image/0-0?frshty_instance=quill"
        assert resp.headers["cache-control"] == "no-store"
        assert upstream == []

    def test_a_post_from_a_pinned_page_follows_the_referer(self, peers, upstream, client):
        client.cookies.set("frshty_instance", "frshty")
        resp = client.post("/api/x", headers={"referer": "http://testserver/tasks/5?frshty_instance=quill"})
        assert resp.json()["from"] == "127.0.0.1:7134"

    def test_picking_an_instance_drops_the_referer(self, peers, client):
        resp = client.get("/api/gateway/select", params={"key": "quill", "next": "/tasks"})
        assert resp.headers["referrer-policy"] == "no-referrer"


class TestSandboxedPage:
    def test_a_sandboxed_page_moves_under_its_instance_path(self, peers, upstream, client):
        client.cookies.set("frshty_instance", "quill")
        resp = client.get("/artifact/5")
        assert resp.headers["location"] == "/artifact/5/"
        resp = client.get("/artifact/5/?x=1")
        assert resp.status_code == 303
        assert resp.headers["location"] == "/api/gateway/in/quill/artifact/5/?x=1"
        assert resp.headers["cache-control"] == "no-store"

    def test_a_pinned_sandboxed_page_moves_under_the_pinned_instance(self, peers, upstream, client):
        resp = client.get("/artifact/5?frshty_instance=quill")
        assert resp.headers["location"] == "/artifact/5/?frshty_instance=quill"
        resp = client.get("/artifact/5/?frshty_instance=quill")
        assert resp.headers["location"] == "/api/gateway/in/quill/artifact/5/"

    def test_the_page_and_its_files_load_from_the_path_instance_without_cookie_or_referer(
            self, peers, upstream, client):
        resp = client.get("/api/gateway/in/quill/artifact/5/")
        assert resp.status_code == 200
        assert resp.text == "<html><head></head><body><img src=a.png></body></html>"
        assert upstream[-1].url.path == "/artifact/5/"
        assert upstream[-1].url.port == 7134
        resp = client.get("/api/gateway/in/quill/artifact/5/a.png", headers={"referer": "http://testserver/"})
        assert resp.content == b"127.0.0.1:7134"
        assert upstream[-1].url.path == "/artifact/5/a.png"

    def test_a_redirect_under_the_instance_path_stays_there(self, peers, upstream, client):
        resp = client.get("/api/gateway/in/quill/artifact/5")
        assert resp.headers["location"] == "/api/gateway/in/quill/artifact/5/"
        resp = client.get("/api/gateway/in/quill/moved")
        assert resp.headers["location"] == "/api/gateway/in/quill/tickets/DEV-1"

    def test_an_unknown_path_instance_answers_404(self, peers, upstream, client):
        assert client.get("/api/gateway/in/nope/artifact/5/a.png").status_code == 404
        assert upstream == []

    def test_a_page_without_a_sandbox_stays_in_place(self, peers, upstream, client):
        assert client.get("/page").status_code == 200
