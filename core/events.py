from collections.abc import Callable

_actions: dict[str, Callable] = {}


def register_action(name: str, fn: Callable):
    _actions[name] = fn


def dispatch(event_name: str, payload: dict, config: dict):
    triggers = config.get("events", {}).get("triggers", [])
    for trigger in triggers:
        if trigger["on"] == event_name:
            action_fn = _actions.get(trigger["action"])
            if action_fn:
                action_fn(payload, trigger, config)
