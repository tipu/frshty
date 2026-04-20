from pathlib import Path
from typing import Any


class InstanceRegistry:
    def __init__(self, config: dict):
        self.config = config
        self.instance_key = config["job"]["key"]
        self._cache: dict[str, Any] = {}

    @property
    def slack_workspace(self) -> str | None:
        return self.config.get("slack", {}).get("workspace")

    @property
    def base_url(self) -> str:
        return self.config.get("_base_url", "")

    @property
    def state_dir(self) -> Path:
        return self.config["_state_dir"]

    def lazy(self, name: str, factory):
        if name not in self._cache:
            self._cache[name] = factory(self.config)
        return self._cache[name]


class Instances:
    def __init__(self):
        self._by_key: dict[str, InstanceRegistry] = {}
        self._by_slack_ws: dict[str, str] = {}

    def add(self, config: dict) -> InstanceRegistry:
        reg = InstanceRegistry(config)
        if reg.instance_key in self._by_key:
            raise ValueError(f"duplicate instance_key: {reg.instance_key}")
        self._by_key[reg.instance_key] = reg
        ws = reg.slack_workspace
        if ws:
            if ws in self._by_slack_ws:
                raise ValueError(f"slack workspace {ws} already claimed by {self._by_slack_ws[ws]}")
            self._by_slack_ws[ws] = reg.instance_key
        return reg

    def get(self, instance_key: str) -> InstanceRegistry | None:
        return self._by_key.get(instance_key)

    def keys(self) -> list[str]:
        return list(self._by_key.keys())

    def all(self) -> list[InstanceRegistry]:
        return list(self._by_key.values())

    def route_slack(self, workspace: str) -> str | None:
        return self._by_slack_ws.get(workspace)

    def as_dict(self) -> dict[str, InstanceRegistry]:
        return dict(self._by_key)
