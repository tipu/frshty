from unittest.mock import patch

from web import observability, state


def _config(key, tmp_path):
    return {"job": {"key": key}, "_base_url": f"https://{key}.frshty.localhost", "_state_dir": tmp_path / key}


def test_the_serving_instance_links_its_own_events_relative(tmp_path, monkeypatch):
    personal, quill = _config("personal", tmp_path), _config("quill", tmp_path)
    monkeypatch.setattr(state, "_configs_by_host", {})
    monkeypatch.setattr(observability, "_configs_by_host",
                        {"personal.frshty.localhost": personal, "quill.frshty.localhost": quill})
    token = state._cv_config.set(personal)
    try:
        with patch.object(observability.log, "use", return_value=None), \
             patch.object(observability.log, "reset"), \
             patch.object(observability.log, "get_events", side_effect=lambda **_: [{"id": "1", "links": {
                 "detail": "https://personal.frshty.localhost/tasks/7", "pr": "https://github.com/o/r/pull/1"}}]):
            events = observability._fetch_local_global_events(limit=10, unread_only=False, after="")
    finally:
        state._cv_config.reset(token)
    assert {e["instance_key"]: e["base_url"] for e in events} == {
        "personal": "", "quill": "https://quill.frshty.localhost"}
    links = {e["instance_key"]: e["links"] for e in events}
    assert links["personal"] == {"detail": "/tasks/7", "pr": "https://github.com/o/r/pull/1"}
    assert links["quill"]["detail"] == "https://personal.frshty.localhost/tasks/7"


def test_relative_links_strip_only_the_instance_host():
    events = [{"links": {"a": "https://personal.frshty.localhost", "b": "https://personal.frshty.localhost/tasks/7?x=1",
                         "c": "https://personal.frshty.localhost.evil/x", "d": "/billing", "e": "https://github.com/x"}}]
    assert observability._relative_links(events, "https://personal.frshty.localhost/")[0]["links"] == {
        "a": "/", "b": "/tasks/7?x=1", "c": "https://personal.frshty.localhost.evil/x", "d": "/billing",
        "e": "https://github.com/x"}


def test_a_single_instance_links_its_own_events_relative(tmp_path, monkeypatch):
    personal = _config("personal", tmp_path)
    monkeypatch.setattr(observability, "_configs_by_host", {})
    monkeypatch.setattr(state, "_primary_config", personal)
    token = state._cv_config.set({})
    try:
        with patch.object(observability.log, "use", return_value=None), \
             patch.object(observability.log, "reset"), \
             patch.object(observability.log, "get_events", side_effect=lambda **_: [{"id": "1", "links": {}}]):
            events = observability._fetch_local_global_events(limit=10, unread_only=False, after="")
    finally:
        state._cv_config.reset(token)
    assert [e["base_url"] for e in events] == [""]


def test_the_event_feed_links_its_own_pages_relative(tmp_path):
    token = state._cv_config.set(_config("aimyable", tmp_path))
    try:
        with patch.object(observability.log, "get_events",
                          return_value=[{"id": "1", "links": {"detail": "https://aimyable.frshty.localhost/tickets/DEV-1"}}]):
            events = observability.api_events(limit=10)
    finally:
        state._cv_config.reset(token)
    assert events[0]["links"] == {"detail": "/tickets/DEV-1"}


def test_the_pending_work_endpoint_reads_the_serving_instance(tmp_path):
    token = state._cv_config.set(_config("aimyable", tmp_path))
    try:
        with patch.object(observability.pending_work, "snapshot", return_value={"agent": {}, "person": {}}) as snap:
            assert observability.api_work_pending() == {"agent": {}, "person": {}}
    finally:
        state._cv_config.reset(token)
    snap.assert_called_once_with("aimyable")
