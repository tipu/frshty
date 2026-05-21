"""Per-instance JSONL log of every outbound HTTP call to an external provider
(Jira, Bitbucket, Linear, Bill.com, etc).

Lives at ~/.frshty/<instance>/logs/external_calls.YYYY-MM-DD.jsonl — one file
per UTC day, append-only, never queried by application code. Purely for
debugging "what did we send to provider X and what came back" without
polluting the sqlite log_events table (which the UI activity feed depends on
for relational queries against tickets / read-state).

Files older than RETENTION_DAYS (30) are deleted opportunistically — the
cleanup runs at most once per process per day, on the next write that crosses
a new UTC day.

Usage at a call site:

    with external_log.client("bitbucket", auth=self._auth(), timeout=30) as c:
        resp = c.get(url)

Drop-in replacement for `httpx.Client(...)`. The wrapper attaches httpx event
hooks that capture method/url/status/duration_ms after each response and
append a row to the per-instance per-day jsonl. URLs with `token=` or `?auth=`
style secrets in the query string are sanitised before logging.

Failures (DNS, connection refused, timeout) are logged with status=None and
error=<exception class + message>.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

from core.state import active_instance_key

_REQUEST_START_TIMES: dict[int, float] = {}

_SECRET_QUERY_KEYS = {"token", "access_token", "auth", "key", "api_key", "apikey"}

RETENTION_DAYS = 30
_FILENAME_PAT = re.compile(r"^external_calls\.(\d{4}-\d{2}-\d{2})\.jsonl$")
_last_cleanup_date: dict[str, str] = {}  # per-instance "YYYY-MM-DD" of last prune


def _sanitise_url(url: str) -> str:
	"""Strip secret-looking values from URL query strings."""
	try:
		split = urlsplit(url)
	except ValueError:
		return url
	if not split.query:
		return url
	pairs = []
	for chunk in split.query.split("&"):
		if "=" in chunk:
			k, v = chunk.split("=", 1)
			if k.lower() in _SECRET_QUERY_KEYS and v:
				v = "<redacted>"
			pairs.append(f"{k}={v}")
		else:
			pairs.append(chunk)
	return urlunsplit((split.scheme, split.netloc, split.path, "&".join(pairs), split.fragment))


def _today_str() -> str:
	return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _log_path(today: str | None = None) -> Path | None:
	"""Per-instance, per-day jsonl path. Returns None when no instance is
	active (e.g. tooling that imports the module outside of a frshty run)."""
	instance = active_instance_key()
	if not instance:
		return None
	logs_dir = Path.home() / ".frshty" / instance / "logs"
	logs_dir.mkdir(parents=True, exist_ok=True)
	return logs_dir / f"external_calls.{today or _today_str()}.jsonl"


def _prune_old_files(logs_dir: Path, today: str) -> None:
	"""Delete external_calls.YYYY-MM-DD.jsonl files older than RETENTION_DAYS.

	Cheap: only runs once per process per UTC day (gated by _last_cleanup_date)
	so a worker doing thousands of HTTP calls per day pays the directory scan
	exactly once."""
	try:
		today_dt = datetime.strptime(today, "%Y-%m-%d").replace(tzinfo=timezone.utc)
	except ValueError:
		return
	cutoff = today_dt - timedelta(days=RETENTION_DAYS)
	for entry in logs_dir.iterdir():
		m = _FILENAME_PAT.match(entry.name)
		if not m:
			continue
		try:
			entry_dt = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
		except ValueError:
			continue
		if entry_dt < cutoff:
			try:
				entry.unlink()
			except OSError:
				pass


def _maybe_prune(instance: str, logs_dir: Path, today: str) -> None:
	"""Per-instance, per-day memo around _prune_old_files so we don't scan the
	directory on every HTTP call."""
	if _last_cleanup_date.get(instance) == today:
		return
	_last_cleanup_date[instance] = today
	_prune_old_files(logs_dir, today)


def _append(record: dict[str, Any]) -> None:
	instance = active_instance_key()
	if not instance:
		return
	today = _today_str()
	logs_dir = Path.home() / ".frshty" / instance / "logs"
	try:
		logs_dir.mkdir(parents=True, exist_ok=True)
		_maybe_prune(instance, logs_dir, today)
		path = logs_dir / f"external_calls.{today}.jsonl"
		with path.open("a", encoding="utf-8") as fh:
			fh.write(json.dumps(record, separators=(",", ":")) + "\n")
	except OSError:
		# Logging must never break the calling request path.
		pass


def _on_request(provider: str):
	def hook(request: httpx.Request) -> None:
		_REQUEST_START_TIMES[id(request)] = time.monotonic()
	return hook


def _on_response(provider: str):
	def hook(response: httpx.Response) -> None:
		start = _REQUEST_START_TIMES.pop(id(response.request), None)
		duration_ms = round((time.monotonic() - start) * 1000) if start is not None else None
		_append({
			"ts": datetime.now(timezone.utc).isoformat(),
			"instance": active_instance_key(),
			"provider": provider,
			"method": response.request.method,
			"url": _sanitise_url(str(response.request.url)),
			"status": response.status_code,
			"duration_ms": duration_ms,
		})
	return hook


def _wrap_with_failure_log(client: httpx.Client, provider: str) -> httpx.Client:
	"""Patch client.send so transport failures (no response object) still get
	a row in the jsonl with error=<class>: <message>."""
	original_send = client.send

	def send_with_log(request, *args, **kwargs):
		start = time.monotonic()
		try:
			return original_send(request, *args, **kwargs)
		except Exception as exc:
			duration_ms = round((time.monotonic() - start) * 1000)
			_append({
				"ts": datetime.now(timezone.utc).isoformat(),
				"instance": active_instance_key(),
				"provider": provider,
				"method": request.method,
				"url": _sanitise_url(str(request.url)),
				"status": None,
				"duration_ms": duration_ms,
				"error": f"{type(exc).__name__}: {exc}",
			})
			raise

	client.send = send_with_log  # type: ignore[assignment]
	return client


def client(provider: str, **kwargs: Any) -> httpx.Client:
	"""Drop-in replacement for httpx.Client(...) that logs every request to
	~/.frshty/<instance>/logs/external_calls.jsonl. Accepts the same kwargs
	as httpx.Client; merges its own event_hooks with any the caller passes."""
	user_hooks = kwargs.pop("event_hooks", None) or {}
	request_hooks = list(user_hooks.get("request") or []) + [_on_request(provider)]
	response_hooks = list(user_hooks.get("response") or []) + [_on_response(provider)]
	merged_hooks = {**user_hooks, "request": request_hooks, "response": response_hooks}
	c = httpx.Client(event_hooks=merged_hooks, **kwargs)
	return _wrap_with_failure_log(c, provider)


def _wrap_async_with_failure_log(client: httpx.AsyncClient, provider: str) -> httpx.AsyncClient:
	"""Async analogue of _wrap_with_failure_log."""
	original_send = client.send

	async def send_with_log(request, *args, **kwargs):
		start = time.monotonic()
		try:
			return await original_send(request, *args, **kwargs)
		except Exception as exc:
			duration_ms = round((time.monotonic() - start) * 1000)
			_append({
				"ts": datetime.now(timezone.utc).isoformat(),
				"instance": active_instance_key(),
				"provider": provider,
				"method": request.method,
				"url": _sanitise_url(str(request.url)),
				"status": None,
				"duration_ms": duration_ms,
				"error": f"{type(exc).__name__}: {exc}",
			})
			raise

	client.send = send_with_log  # type: ignore[assignment]
	return client


def _on_request_async(provider: str):
	async def hook(request: httpx.Request) -> None:
		_REQUEST_START_TIMES[id(request)] = time.monotonic()
	return hook


def _on_response_async(provider: str):
	async def hook(response: httpx.Response) -> None:
		start = _REQUEST_START_TIMES.pop(id(response.request), None)
		duration_ms = round((time.monotonic() - start) * 1000) if start is not None else None
		_append({
			"ts": datetime.now(timezone.utc).isoformat(),
			"instance": active_instance_key(),
			"provider": provider,
			"method": response.request.method,
			"url": _sanitise_url(str(response.request.url)),
			"status": response.status_code,
			"duration_ms": duration_ms,
		})
	return hook


def aclient(provider: str, **kwargs: Any) -> httpx.AsyncClient:
	"""Drop-in replacement for httpx.AsyncClient(...) — same logging behaviour
	as `client()` but for `async with` callers (Bill.com uses these). httpx
	requires async event hooks when the client itself is async, so the request/
	response hooks are async variants of the same logic."""
	user_hooks = kwargs.pop("event_hooks", None) or {}
	request_hooks = list(user_hooks.get("request") or []) + [_on_request_async(provider)]
	response_hooks = list(user_hooks.get("response") or []) + [_on_response_async(provider)]
	merged_hooks = {**user_hooks, "request": request_hooks, "response": response_hooks}
	c = httpx.AsyncClient(event_hooks=merged_hooks, **kwargs)
	return _wrap_async_with_failure_log(c, provider)
