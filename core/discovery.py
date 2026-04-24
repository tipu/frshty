import asyncio
import tomllib
from pathlib import Path

import httpx

CONFIG_DIR = Path(__file__).parent.parent / "config"
SKIP_CONFIGS = {"example.toml", "test.toml", "discovery.toml"}


def discover_instances() -> list[dict]:
    instances = []
    seen_keys = set()

    discovery_path = CONFIG_DIR / "discovery.toml"
    if discovery_path.exists():
        try:
            with open(discovery_path, "rb") as f:
                raw = tomllib.load(f)
            for inst_config in raw.get("instances", {}).get("list", []):
                inst_key = inst_config.get("key", "")
                if inst_key and inst_key not in seen_keys:
                    instances.append({
                        "key": inst_key,
                        "port": inst_config.get("port", 0),
                        "base_url": inst_config.get("base_url", ""),
                        "config_path": str(discovery_path),
                        "platform": inst_config.get("platform", ""),
                        "ticket_system": inst_config.get("ticket_system", ""),
                    })
                    seen_keys.add(inst_key)
        except Exception:
            pass

    for path in sorted(CONFIG_DIR.glob("*.toml")):
        if path.name in SKIP_CONFIGS:
            continue
        try:
            with open(path, "rb") as f:
                raw = tomllib.load(f)

            job = raw.get("job", {})
            key = job.get("key", "")
            port = job.get("port", 0)
            if key and port and key not in seen_keys:
                instances.append({
                    "key": key,
                    "port": port,
                    "base_url": job.get("host") or f"http://localhost:{port}",
                    "config_path": str(path),
                    "platform": job.get("platform", ""),
                    "ticket_system": job.get("ticket_system", ""),
                })
                seen_keys.add(key)
        except Exception:
            continue

    return instances


def find_instance(instances: list[dict], key: str) -> dict | None:
    for inst in instances:
        if inst["key"] == key:
            return inst
    return None


async def call_instance(base_url: str, method: str, path: str, body: dict | None = None, timeout: float = 10.0) -> dict:
    try:
        async with httpx.AsyncClient(timeout=timeout, verify=False) as client:
            if method.upper() == "GET":
                resp = await client.get(f"{base_url}{path}")
            else:
                resp = await client.post(f"{base_url}{path}", json=body or {})
            return resp.json()
    except Exception as e:
        return {"error": str(e)}


async def fan_out(instances: list[dict], method: str, path: str, body: dict | None = None) -> dict[str, dict]:
    tasks = {
        inst["key"]: call_instance(inst["base_url"], method, path, body)
        for inst in instances
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    return {
        key: (r if isinstance(r, dict) else {"error": str(r)})
        for key, r in zip(tasks.keys(), results)
    }
