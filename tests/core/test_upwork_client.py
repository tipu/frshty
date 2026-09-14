"""Tests for core.upwork_client — the session it holds and how it retries.

The browser is never driven here. `_capture` is patched, because what is under
test is which session a caller is handed and when a new one is lifted.
"""
from unittest.mock import patch

import pytest

import core.upwork_client as upwork_client


@pytest.fixture(autouse=True)
def _clean():
    upwork_client.forget()
    yield
    upwork_client.forget()


def _config(cdp_url):
    return {"upwork": {"cdp_url": cdp_url}}


def _session(name):
    return {"authorization": f"bearer {name}", "org_id": f"org-{name}",
            "cookie": f"c={name}", "user_agent": "ua", "tenant": f"org-{name}",
            "user_id": f"user-{name}"}


class TestSessionCache:
    def test_one_lift_serves_every_later_call_for_the_same_browser(self):
        with patch.object(upwork_client, "_capture",
                          return_value=_session("a")) as capture:
            first = upwork_client.session(_config("http://localhost:9222"))
            second = upwork_client.session(_config("http://localhost:9222"))
        assert capture.call_count == 1
        assert first is second

    def test_two_browsers_never_share_a_session(self):
        """Two instances can be pointed at two Chrome profiles logged into two
        Upwork accounts. A shared slot would hand the second one the first
        one's bearer, cookies and organisation."""
        def _by_port(cdp_url, timeout):
            return _session(cdp_url.rsplit(":", 1)[-1])

        with patch.object(upwork_client, "_capture", side_effect=_by_port) as capture:
            a = upwork_client.session(_config("http://localhost:9222"))
            b = upwork_client.session(_config("http://localhost:9333"))
        assert capture.call_count == 2
        assert a["authorization"] == "bearer 9222"
        assert b["authorization"] == "bearer 9333"
        assert upwork_client.held(_config("http://localhost:9333")) is b

    def test_held_never_lifts_a_session(self):
        with patch.object(upwork_client, "_capture") as capture:
            assert upwork_client.held(_config("http://localhost:9222")) is None
        assert capture.call_count == 0

    def test_forget_with_no_config_drops_every_browser(self):
        with patch.object(upwork_client, "_capture", return_value=_session("a")):
            upwork_client.session(_config("http://localhost:9222"))
            upwork_client.session(_config("http://localhost:9333"))
        upwork_client.forget()
        assert upwork_client.held(_config("http://localhost:9222")) is None
        assert upwork_client.held(_config("http://localhost:9333")) is None


class TestRetry:
    def test_an_expired_bearer_is_lifted_again_once_and_the_call_repeats(self):
        calls = []

        def _request(auth, method, path, params, body):
            calls.append(auth["authorization"])
            if len(calls) == 1:
                raise upwork_client.UpworkApiError(401, "expired")
            return {"rooms": []}

        with patch.object(upwork_client, "_capture",
                          side_effect=[_session("old"), _session("new")]), \
                patch.object(upwork_client, "_request", side_effect=_request):
            out = upwork_client.rooms(_config("http://localhost:9222"))
        assert out == {"rooms": []}
        assert calls == ["bearer old", "bearer new"]

    def test_a_bad_request_is_raised_rather_than_retried(self):
        """A 400 says the request was wrong. A new token does not fix it, and
        retrying would open a browser tab for every malformed call."""
        with patch.object(upwork_client, "_capture",
                          return_value=_session("a")) as capture, \
                patch.object(upwork_client, "_request",
                             side_effect=upwork_client.UpworkApiError(400, "bad")):
            with pytest.raises(upwork_client.UpworkApiError) as caught:
                upwork_client.rooms(_config("http://localhost:9222"))
        assert caught.value.status == 400
        assert capture.call_count == 1


class TestRequests:
    def _record(self):
        seen = {}

        def _request(auth, method, path, params, body):
            seen.update({"method": method, "path": path, "params": dict(params),
                         "body": body})
            return {}
        return seen, _request

    def test_every_call_carries_the_caller_organisation(self):
        seen, recorder = self._record()

        def _request(auth, method, path, params, body):
            out = recorder(auth, method, path, params, body)
            seen["org"] = auth["org_id"]
            return out

        with patch.object(upwork_client, "_capture", return_value=_session("a")), \
                patch.object(upwork_client, "_request", side_effect=_request):
            upwork_client.rooms(_config("http://localhost:9222"), limit=5)
        assert seen["path"] == "/rooms/simplified"
        assert seen["params"] == {"limit": 5}
        assert seen["org"] == "org-a"

    def test_a_story_page_is_asked_for_by_time_not_by_story_id(self):
        seen, recorder = self._record()
        with patch.object(upwork_client, "_capture", return_value=_session("a")), \
                patch.object(upwork_client, "_request", side_effect=recorder):
            upwork_client.stories("room_1", _config("http://localhost:9222"),
                                  limit=3, older_than="1789412707602")
        assert seen["path"] == "/rooms/room_1/stories/simplified"
        assert seen["params"] == {"limit": 3, "olderThan": "1789412707602"}

    def test_a_reply_posts_the_story_body(self):
        seen, recorder = self._record()
        with patch.object(upwork_client, "_capture", return_value=_session("a")), \
                patch.object(upwork_client, "_request", side_effect=recorder):
            upwork_client.send_message("room_1", "On it.",
                                       _config("http://localhost:9222"))
        assert seen["method"] == "POST"
        assert seen["path"] == "/rooms/room_1/stories"
        assert seen["body"] == {"story": {"message": "On it."}}
