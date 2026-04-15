from unittest.mock import MagicMock

import core.events as events


class TestRegisterAction:
    def test_registers_callable(self):
        fn = MagicMock()
        events.register_action("test_action", fn)
        assert events._actions["test_action"] is fn

    def test_overwrites_existing(self):
        fn1 = MagicMock()
        fn2 = MagicMock()
        events.register_action("dup", fn1)
        events.register_action("dup", fn2)
        assert events._actions["dup"] is fn2


class TestDispatch:
    def test_calls_matching_trigger(self):
        fn = MagicMock()
        events._actions["do_thing"] = fn
        config = {"events": {"triggers": [{"on": "ticket_done", "action": "do_thing"}]}}
        payload = {"key": "PROJ-1"}
        events.dispatch("ticket_done", payload, config)
        fn.assert_called_once_with(payload, {"on": "ticket_done", "action": "do_thing"}, config)

    def test_skips_non_matching(self):
        fn = MagicMock()
        events._actions["do_thing"] = fn
        config = {"events": {"triggers": [{"on": "other_event", "action": "do_thing"}]}}
        events.dispatch("ticket_done", {}, config)
        fn.assert_not_called()

    def test_no_triggers(self):
        config = {"events": {"triggers": []}}
        events.dispatch("ticket_done", {}, config)

    def test_no_events_section(self):
        events.dispatch("ticket_done", {}, {})

    def test_missing_action_in_registry(self):
        config = {"events": {"triggers": [{"on": "evt", "action": "nonexistent"}]}}
        events.dispatch("evt", {}, config)

    def test_multiple_matching_triggers(self):
        fn1 = MagicMock()
        fn2 = MagicMock()
        events._actions["a1"] = fn1
        events._actions["a2"] = fn2
        config = {"events": {"triggers": [
            {"on": "evt", "action": "a1"},
            {"on": "evt", "action": "a2"},
        ]}}
        events.dispatch("evt", {}, config)
        fn1.assert_called_once()
        fn2.assert_called_once()
