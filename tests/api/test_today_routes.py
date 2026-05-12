import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import core.state as state
import core.log as log
import core.db as db


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")

    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        if "frshty" in sys.modules:
            frshty = sys.modules["frshty"]
        else:
            import frshty
    finally:
        sys.argv = saved_argv

    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config({
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": tmp_path,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
        },
        "features": {"reviews": True, "slack": False},
        "pr": {"auto_pr": True},
        "slack": {},
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    })
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


class TestTodaySnoozesTableExists:
    def test_table_present(self, client):
        row = db.query_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='today_snoozes'"
        )
        assert row and row["name"] == "today_snoozes"


class TestTodayLoopsShape:
    def test_empty_state_returns_all_buckets(self, client):
        resp = client.get("/api/today/loops")
        assert resp.status_code == 200
        data = resp.json()
        assert data["instance_key"] == "test"
        assert isinstance(data["loops"], dict)
        for key in ("merge_ready", "ready_to_submit", "pr_failed_tickets",
                    "pr_comments_needs_reply", "peer_pr_reviews",
                    "stale_own_prs", "timesheet_underfilled",
                    "billcom_invoice_due", "pickup_new",
                    "pending_approvals_stuck", "in_review_no_ci",
                    "stale_unattended", "regressions_recent"):
            assert key in data["loops"], f"missing bucket: {key}"
        for k, v in data["counts"].items():
            assert v == len(data["loops"][k]), f"count mismatch for {k}"
        assert data["snoozed"] == []
        assert data["manager_latest"] is None
        assert data["policy_stale"] is False
        assert data["errors"] == []

    def test_no_instance_returns_400(self, client):
        from web.state import set_primary_config
        set_primary_config({
            "job": {},
            "workspace": {"root": "/tmp", "tickets_dir": "tickets", "base_branch": "main"},
            "_state_dir": "/tmp",
            "_base_url": "http://localhost",
        })
        resp = client.get("/api/today/loops")
        assert resp.status_code == 400
        assert resp.json()["error"] == "no instance"

    def test_aggregate_failure_returns_500_and_logs(self, client):
        with patch("manager.staleness.aggregate_all", side_effect=RuntimeError("boom")):
            resp = client.get("/api/today/loops")
        assert resp.status_code == 500
        events = log.get_events(limit=10)
        assert any(e["event"] == "today_loops_aggregate_failed" for e in events), \
            f"expected log emission; got events: {[e['event'] for e in events]}"


class TestSnoozeCrud:
    def test_create_and_list(self, client):
        resp = client.post("/api/today/snoozes", json={
            "loop_type": "stale_own_prs", "entity_id": "myrepo/123",
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "snoozed"
        loops = client.get("/api/today/loops").json()
        match = [s for s in loops["snoozed"]
                 if s["loop_type"] == "stale_own_prs" and s["entity_id"] == "myrepo/123"]
        assert len(match) == 1

    def test_upsert_overwrites_reason(self, client):
        client.post("/api/today/snoozes", json={
            "loop_type": "stale_own_prs", "entity_id": "x/1", "reason": "first"})
        r2 = client.post("/api/today/snoozes", json={
            "loop_type": "stale_own_prs", "entity_id": "x/1", "reason": "second"})
        assert r2.status_code == 200
        loops = client.get("/api/today/loops").json()
        match = next(s for s in loops["snoozed"] if s["entity_id"] == "x/1")
        assert match["reason"] == "second"

    def test_invalid_loop_type_400(self, client):
        resp = client.post("/api/today/snoozes", json={
            "loop_type": "bogus_loop", "entity_id": "x"})
        assert resp.status_code == 400
        assert "unknown loop_type" in resp.json()["error"]

    def test_missing_fields_400(self, client):
        resp = client.post("/api/today/snoozes", json={"loop_type": "stale_own_prs"})
        assert resp.status_code == 400
        assert "required" in resp.json()["error"]

    def test_past_snooze_until_filtered_out(self, client):
        client.post("/api/today/snoozes", json={
            "loop_type": "stale_own_prs", "entity_id": "expired/1",
            "snooze_until": "2020-01-01 00:00:00"})
        loops = client.get("/api/today/loops").json()
        assert not any(s["entity_id"] == "expired/1" for s in loops["snoozed"])

    def test_future_snooze_until_visible(self, client):
        client.post("/api/today/snoozes", json={
            "loop_type": "stale_own_prs", "entity_id": "future/1",
            "snooze_until": "2099-01-01 00:00:00"})
        loops = client.get("/api/today/loops").json()
        assert any(s["entity_id"] == "future/1" for s in loops["snoozed"])

    def test_delete(self, client):
        client.post("/api/today/snoozes", json={
            "loop_type": "stale_own_prs", "entity_id": "x/1"})
        r = client.delete("/api/today/snoozes/stale_own_prs/x%2F1")
        assert r.status_code == 200
        loops = client.get("/api/today/loops").json()
        assert not any(s["entity_id"] == "x/1" for s in loops["snoozed"])

    def test_delete_nonexistent_idempotent(self, client):
        r = client.delete("/api/today/snoozes/stale_own_prs/never-existed")
        assert r.status_code == 200
        assert r.json()["status"] == "removed"

    def test_instance_isolation(self, client):
        """Snoozes on instance A must not surface on instance B."""
        client.post("/api/today/snoozes", json={
            "loop_type": "stale_own_prs", "entity_id": "shared/1"})
        # rewrite config to a different instance — same DB, different instance_key
        from web.state import set_primary_config
        cfg = dict(client.app.dependency_overrides)  # no-op, just access
        # actually mutate _config in place since set_primary_config replaces
        from web.state import _config
        _config["job"]["key"] = "other-instance"
        loops = client.get("/api/today/loops").json()
        assert loops["instance_key"] == "other-instance"
        assert not any(s["entity_id"] == "shared/1" for s in loops["snoozed"])
        _config["job"]["key"] = "test"
        loops_back = client.get("/api/today/loops").json()
        assert any(s["entity_id"] == "shared/1" for s in loops_back["snoozed"])


class TestMergeEndpoint:
    def test_not_found_404(self, client):
        resp = client.post("/api/tickets/NOPE/merge")
        assert resp.status_code == 404

    def test_wrong_status_400(self, client):
        state.save_ticket("T-1", {"status": "pr_ready", "slug": "T-1-s",
                                  "prs": [{"repo": "r", "id": 1}]})
        resp = client.post("/api/tickets/T-1/merge")
        assert resp.status_code == 400
        assert "not in_review" in resp.json()["error"]

    def test_no_prs_400(self, client):
        state.save_ticket("T-2", {"status": "in_review", "slug": "T-2-s", "prs": []})
        resp = client.post("/api/tickets/T-2/merge")
        assert resp.status_code == 400
        assert "no PRs" in resp.json()["error"]

    def test_happy_path_invokes_merge_and_saves(self, client):
        state.save_ticket("T-3", {"status": "in_review", "slug": "T-3-s",
                                   "prs": [{"repo": "r", "id": 1, "url": "u"}]})

        def fake_merge(config, ticket, ts, base_url):
            return {**ts, "status": "merged", "merged_external_status": "Done"}

        with patch("features.tickets._merge", side_effect=fake_merge):
            resp = client.post("/api/tickets/T-3/merge")
        assert resp.status_code == 200
        assert resp.json()["new_status"] == "merged"
        saved = state.load_ticket("T-3")
        assert saved["status"] == "merged"


class TestPrCommentBucketIncludesId:
    def test_comment_id_included(self, client, tmp_path):
        """After the staleness patch, pr_comments_needs_reply must include
        each comment's id so the reply endpoint URL can be built client-side."""
        state.save_ticket("T-7", {"status": "in_review",
                                   "slug": "T-7-fix",
                                   "summary": "fix the thing"})
        slug_dir = tmp_path / "tickets" / "T-7-fix"
        slug_dir.mkdir(parents=True, exist_ok=True)
        (slug_dir / "pr_comments.json").write_text(json.dumps([{
            "id": 100,
            "status": "needs_reply",
            "pr_repo": "r",
            "pr_id": 5,
            "path": "src/a.py",
            "line": 12,
            "body": "please rename",
            "suggested_reply": "renamed",
        }]))

        resp = client.get("/api/today/loops")
        data = resp.json()
        bucket = data["loops"]["pr_comments_needs_reply"]
        assert len(bucket) >= 1, f"expected ticket in bucket, got {bucket}"
        entry = next(e for e in bucket if e["ticket_key"] == "T-7")
        assert entry["comments"]
        first = entry["comments"][0]
        assert first["id"] == 100, f"comment id missing from staleness output: {first}"


class TestTodayDoesNotHitExternalsOnDefaultPoll:
    """Regression guard: the default GET /api/today/loops (no ?live=1) must
    not call into source-control or bill.com — those calls used to fire on
    every 30s poll, multiplied across 4 instance tabs. Now they're cache-only
    by default; live re-fetch happens only when the user hits Refresh."""

    def test_merge_ready_reads_cached_approvers_not_live(self, client):
        state.save_ticket("T-10", {
            "status": "in_review",
            "slug": "t10",
            "source": "jira",
            "summary": "approved already",
            "discovered_at": "2025-01-01T00:00:00",
            "ci_passed": 1,
            "prs": [{"repo": "r", "id": 1, "url": "http://u",
                     "author": "a", "approvers": ["reviewer1"]}],
        })

        with patch("features.platforms.make_platform") as mp:
            resp = client.get("/api/today/loops")
            assert resp.status_code == 200
            mp.assert_not_called()

        bucket = resp.json()["loops"]["merge_ready"]
        entry = next(e for e in bucket if e["ticket_key"] == "T-10")
        assert entry["prs"][0]["approvers"] == ["reviewer1"]

    def test_peer_reviews_reads_state_cache_not_live(self, client):
        state.save("peer_reviews", {
            "items": [{"repo": "r", "pr_id": 9, "title": "fix bug",
                       "author": "alice", "url": "http://u",
                       "created_on": "2025-01-01", "updated_on": "2025-01-02"}],
            "refreshed_at": "2025-01-02",
        })
        with patch("features.platforms.make_platform") as mp:
            resp = client.get("/api/today/loops")
            assert resp.status_code == 200
            mp.assert_not_called()
        bucket = resp.json()["loops"]["peer_pr_reviews"]
        assert len(bucket) == 1
        assert bucket[0]["pr_id"] == 9

    def test_billing_snapshot_reads_cache_not_live(self, client):
        from web.state import set_primary_config, _config
        cfg = dict(_config)
        cfg["features"] = {**cfg.get("features", {}), "billing": True}
        cfg["billing"] = {"billcom_customer_id": "c1", "billing_freq": "monthly"}
        set_primary_config(cfg)
        state.save("billing_invoices_remote", {
            "items": [{"start": "2026-04-01", "end": "2026-04-30",
                       "id": "iv1", "status": "pending"}],
            "refreshed_at": "2026-05-01",
        })

        with patch("features.billing.list_invoices") as live:
            resp = client.get("/api/today/loops")
            assert resp.status_code == 200
            live.assert_not_called()

        bucket = resp.json()["loops"]["billcom_invoice_due"]
        assert bucket == [], f"April snapshot covers last month; bucket should be empty: {bucket}"


class TestTodayLiveQueryParam:
    def test_live_param_triggers_refresh_calls(self, client):
        state.save_ticket("T-11", {
            "status": "in_review",
            "slug": "t11",
            "source": "jira",
            "summary": "live-only approver",
            "discovered_at": "2025-01-01T00:00:00",
            "ci_passed": 1,
            "prs": [{"repo": "r", "id": 2, "url": "http://u",
                     "author": "a", "approvers": []}],
        })
        from unittest.mock import MagicMock
        fake_platform = MagicMock()
        fake_platform.get_pr_info.return_value = {"approvers": ["fresh-reviewer"]}
        fake_platform.list_pending_reviews_for_me.return_value = []

        with patch("features.platforms.make_platform", return_value=fake_platform):
            resp = client.get("/api/today/loops?live=1")
            assert resp.status_code == 200

        bucket = resp.json()["loops"]["merge_ready"]
        entry = next(e for e in bucket if e["ticket_key"] == "T-11")
        assert entry["prs"][0]["approvers"] == ["fresh-reviewer"], (
            "live=1 must bypass cache and use the live get_pr_info result"
        )
        assert fake_platform.get_pr_info.call_count >= 1


class TestMergeReadyExcludesTicketsWithoutPrs:
    def test_in_review_with_no_prs_excluded(self, client):
        state.save_ticket("T-NOPRS", {
            "status": "in_review",
            "slug": "t-noprs",
            "source": "jira",
            "summary": "stale ci_passed but no tracked PRs",
            "discovered_at": "2025-01-01T00:00:00",
            "ci_passed": 1,
        })
        state.save_ticket("T-APPROVED", {
            "status": "in_review",
            "slug": "t-approved",
            "source": "jira",
            "summary": "real merge candidate",
            "discovered_at": "2025-01-02T00:00:00",
            "ci_passed": 1,
            "prs": [{"repo": "r", "id": 1, "url": "http://u",
                     "author": "a", "approvers": ["reviewer1"]}],
        })

        resp = client.get("/api/today/loops")
        assert resp.status_code == 200
        bucket = resp.json()["loops"]["merge_ready"]
        keys = {e["ticket_key"] for e in bucket}
        assert "T-NOPRS" not in keys, (
            "ticket with no tracked PRs must not surface in merge_ready — "
            "there is nothing to merge"
        )
        assert "T-APPROVED" in keys

    def test_in_review_with_unapproved_prs_excluded(self, client):
        state.save_ticket("T-UNAPPROVED", {
            "status": "in_review",
            "slug": "t-unapp",
            "source": "jira",
            "summary": "open PR with no approvers yet",
            "discovered_at": "2025-01-01T00:00:00",
            "ci_passed": 1,
            "prs": [{"repo": "r", "id": 2, "url": "http://u",
                     "author": "a", "approvers": []}],
        })
        resp = client.get("/api/today/loops")
        assert resp.status_code == 200
        keys = {e["ticket_key"] for e in resp.json()["loops"]["merge_ready"]}
        assert "T-UNAPPROVED" not in keys
