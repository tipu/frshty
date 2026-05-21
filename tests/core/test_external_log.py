"""Tests for core.external_log — the per-instance JSONL logger that wraps
httpx.Client so every outbound provider call gets appended to
~/.frshty/<instance>/logs/external_calls.jsonl.
"""
import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from core import external_log


@pytest.fixture
def tmp_home(monkeypatch, tmp_path):
	"""Redirect ~/.frshty/<instance>/logs/ writes to a tmp dir."""
	monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
	return tmp_path


@pytest.fixture
def active_instance(monkeypatch):
	"""Patch active_instance_key to return a fixed value."""
	monkeypatch.setattr(external_log, "active_instance_key", lambda: "testinst")
	return "testinst"


def _read_lines(tmp_home: Path, instance: str) -> list[dict]:
	"""Read every external_calls.*.jsonl file in chronological order so tests
	written before the per-day rotation don't need to know today's date."""
	logs_dir = tmp_home / ".frshty" / instance / "logs"
	if not logs_dir.exists():
		return []
	rows: list[dict] = []
	for path in sorted(logs_dir.glob("external_calls.*.jsonl")):
		rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
	return rows


def test_successful_get_appends_one_row_with_method_url_status(tmp_home, active_instance):
	def handler(request):
		return httpx.Response(200, json={"ok": True})

	transport = httpx.MockTransport(handler)
	with external_log.client("jira", transport=transport) as c:
		resp = c.get("https://example.com/rest/api/3/search")
	assert resp.status_code == 200

	rows = _read_lines(tmp_home, active_instance)
	assert len(rows) == 1
	row = rows[0]
	assert row["provider"] == "jira"
	assert row["method"] == "GET"
	assert row["url"] == "https://example.com/rest/api/3/search"
	assert row["status"] == 200
	assert row["instance"] == "testinst"
	assert isinstance(row["duration_ms"], int)
	assert row["duration_ms"] >= 0


def test_post_with_4xx_response_logs_status_not_treated_as_error(tmp_home, active_instance):
	def handler(request):
		return httpx.Response(404, text="not found")

	transport = httpx.MockTransport(handler)
	with external_log.client("bitbucket", transport=transport) as c:
		resp = c.post("https://api.bitbucket.org/2.0/foo", json={"x": 1})
	assert resp.status_code == 404

	rows = _read_lines(tmp_home, active_instance)
	assert len(rows) == 1
	assert rows[0]["status"] == 404
	assert rows[0]["method"] == "POST"
	assert "error" not in rows[0]


def test_transport_failure_logs_row_with_error_field(tmp_home, active_instance):
	def handler(request):
		raise httpx.ConnectError("connection refused")

	transport = httpx.MockTransport(handler)
	with external_log.client("linear", transport=transport) as c:
		with pytest.raises(httpx.ConnectError):
			c.post("https://api.linear.app/graphql", json={"query": "{}"})

	rows = _read_lines(tmp_home, active_instance)
	assert len(rows) == 1
	row = rows[0]
	assert row["status"] is None
	assert row["error"].startswith("ConnectError:")
	assert row["provider"] == "linear"
	assert row["method"] == "POST"


def test_secret_query_values_are_redacted(tmp_home, active_instance):
	def handler(request):
		return httpx.Response(200, text="ok")

	transport = httpx.MockTransport(handler)
	with external_log.client("jira", transport=transport) as c:
		c.get("https://example.com/api?token=supersecret&foo=bar")

	rows = _read_lines(tmp_home, active_instance)
	assert "supersecret" not in rows[0]["url"]
	assert "token=%3Credacted%3E" in rows[0]["url"] or "token=<redacted>" in rows[0]["url"]
	assert "foo=bar" in rows[0]["url"]


def test_multiple_requests_append_multiple_rows(tmp_home, active_instance):
	def handler(request):
		return httpx.Response(200)

	transport = httpx.MockTransport(handler)
	with external_log.client("jira", transport=transport) as c:
		c.get("https://example.com/a")
		c.get("https://example.com/b")
		c.get("https://example.com/c")

	rows = _read_lines(tmp_home, active_instance)
	assert len(rows) == 3
	assert [r["url"].split("/")[-1] for r in rows] == ["a", "b", "c"]


def test_no_instance_active_skips_logging_silently(tmp_home, monkeypatch):
	"""When called outside an instance context, the wrapper still works (no
	crash) but the row isn't written anywhere."""
	monkeypatch.setattr(external_log, "active_instance_key", lambda: "")

	def handler(request):
		return httpx.Response(200)

	transport = httpx.MockTransport(handler)
	with external_log.client("jira", transport=transport) as c:
		resp = c.get("https://example.com/foo")
	assert resp.status_code == 200

	assert not (tmp_home / ".frshty").exists() or list((tmp_home / ".frshty").rglob("external_calls.jsonl")) == []


def test_aclient_logs_successful_async_request(tmp_home, active_instance):
	import asyncio

	def handler(request):
		return httpx.Response(200, json={"ok": True})

	async def run():
		transport = httpx.MockTransport(handler)
		async with external_log.aclient("billcom", transport=transport) as c:
			resp = await c.post("https://api.bill.com/login", json={"username": "x"})
		return resp

	resp = asyncio.run(run())
	assert resp.status_code == 200

	rows = _read_lines(tmp_home, active_instance)
	assert len(rows) == 1
	assert rows[0]["provider"] == "billcom"
	assert rows[0]["method"] == "POST"
	assert rows[0]["status"] == 200


def test_aclient_transport_failure_logs_error(tmp_home, active_instance):
	import asyncio

	def handler(request):
		raise httpx.ConnectError("network down")

	async def run():
		transport = httpx.MockTransport(handler)
		async with external_log.aclient("billcom", transport=transport) as c:
			with pytest.raises(httpx.ConnectError):
				await c.post("https://api.bill.com/login")

	asyncio.run(run())

	rows = _read_lines(tmp_home, active_instance)
	assert len(rows) == 1
	assert rows[0]["status"] is None
	assert rows[0]["error"].startswith("ConnectError:")


def test_write_creates_dated_filename_and_prunes_old_files(tmp_home, active_instance, monkeypatch):
	"""Files older than RETENTION_DAYS get unlinked on the next write that
	bumps into a new UTC day."""
	# Drop the cleanup memo so the next write definitely triggers prune.
	external_log._last_cleanup_date.clear()

	logs_dir = tmp_home / ".frshty" / active_instance / "logs"
	logs_dir.mkdir(parents=True, exist_ok=True)

	# Pre-seed 3 historical files: one inside the window, two outside.
	(logs_dir / "external_calls.2026-05-21.jsonl").write_text('{"old":"today"}\n')
	(logs_dir / "external_calls.2026-04-30.jsonl").write_text('{"old":"21d-ago"}\n')  # inside 30d
	(logs_dir / "external_calls.2026-04-15.jsonl").write_text('{"old":"36d-ago"}\n')  # OUTSIDE 30d
	(logs_dir / "external_calls.2025-12-01.jsonl").write_text('{"old":"way-old"}\n')  # OUTSIDE

	# Pretend today is 2026-05-21 (UTC).
	monkeypatch.setattr(external_log, "_today_str", lambda: "2026-05-21")

	def handler(request):
		return httpx.Response(200)

	transport = httpx.MockTransport(handler)
	with external_log.client("jira", transport=transport) as c:
		c.get("https://example.com/x")

	remaining = sorted(p.name for p in logs_dir.glob("external_calls.*.jsonl"))
	assert remaining == [
		"external_calls.2026-04-30.jsonl",  # 21d ago — kept
		"external_calls.2026-05-21.jsonl",  # today — kept (and just got the new row)
	]
	# Today's file gained the new request row.
	today_lines = (logs_dir / "external_calls.2026-05-21.jsonl").read_text().splitlines()
	assert len(today_lines) == 2  # the pre-existing dummy line + the new one


def test_prune_only_runs_once_per_day_per_instance(tmp_home, active_instance, monkeypatch):
	"""Repeated writes on the same UTC day must not re-scan the directory."""
	external_log._last_cleanup_date.clear()
	monkeypatch.setattr(external_log, "_today_str", lambda: "2026-05-21")

	scan_count = 0
	original_prune = external_log._prune_old_files

	def counting_prune(logs_dir, today):
		nonlocal scan_count
		scan_count += 1
		original_prune(logs_dir, today)

	monkeypatch.setattr(external_log, "_prune_old_files", counting_prune)

	def handler(request):
		return httpx.Response(200)

	transport = httpx.MockTransport(handler)
	with external_log.client("jira", transport=transport) as c:
		for _ in range(5):
			c.get("https://example.com/x")

	assert scan_count == 1  # not 5


def test_user_provided_event_hooks_are_preserved(tmp_home, active_instance):
	"""If the caller passes their own request/response hooks, ours run alongside
	rather than replacing them."""
	user_responses = []

	def user_hook(response):
		user_responses.append(response.status_code)

	def handler(request):
		return httpx.Response(201)

	transport = httpx.MockTransport(handler)
	with external_log.client(
		"jira", transport=transport,
		event_hooks={"response": [user_hook]},
	) as c:
		c.get("https://example.com/x")

	assert user_responses == [201]
	rows = _read_lines(tmp_home, active_instance)
	assert len(rows) == 1
	assert rows[0]["status"] == 201
