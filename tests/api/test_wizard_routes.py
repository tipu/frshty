import sys
from unittest.mock import patch

import pytest

import core.state as state
import core.log as log


def _ticket(key, **extra):
    base = {
        "ticket_key": key,
        "summary": f"summary for {key}",
        "discovered_at": "2026-01-01T00:00:00Z",
    }
    base.update(extra)
    return base


def _pr(repo="org/repo", pr_id=42, **extra):
    base = {"repo": repo, "id": pr_id, "url": f"https://example/pr/{pr_id}"}
    base.update(extra)
    return base


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
            "root": tmp_path, "tickets_dir": "tickets", "ticket_layout": "flat",
            "base_branch": "main",
        },
        "features": {},
        "pr": {"auto_pr": True},
        "slack": {},
        "slack_targets": {
            "alice": {"workspace": "tipucorp", "slack_user_id": "U1", "dm_channel_id": "D1"},
        },
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    })
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def _empty_loops():
    return {
        "merge_ready": [], "ready_to_submit": [], "pr_comments_needs_reply": [],
        "peer_pr_reviews": [], "pickup_new": [], "in_review_no_ci": [],
        "pr_failed_tickets": [], "stale_own_prs": [], "stale_unattended": [],
        "pending_approvals_stuck": [], "regressions_recent": [],
        "timesheet_underfilled": [], "billcom_invoice_due": [],
    }


class TestQueueEmpty:
    def test_empty_aggregate_returns_empty_items(self, client):
        with patch("manager.staleness.aggregate_all", return_value=_empty_loops()):
            resp = client.get("/api/wizard/queue")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["items"] == []
        assert data["address_book_logins"] == ["alice"]
        assert "generated_at" in data


class TestPriorityOrdering:
    def test_one_in_each_bucket_emits_in_priority_order(self, client):
        loops = _empty_loops()
        loops["merge_ready"]      = [_ticket("MR-1", prs=[_pr(pr_id=1, approvers=["bob"])])]
        loops["pr_failed_tickets"] = [_ticket("PF-1", prs=[_pr(pr_id=2)],
                                              ci_fix_attempts=3, pr_failed_reason="ci_failed",
                                              url="https://example/PF-1")]
        loops["pr_comments_needs_reply"] = [{
            "ticket_key": "PC-1", "summary": "pc",
            "comments": [{"id": 10, "pr_repo": "org/repo", "pr_id": 3,
                          "pr_url": "https://example/pr/3", "body": "?",
                          "suggested_reply": "ok"}],
            "count": 1,
        }]
        loops["pending_approvals_stuck"] = [_ticket("PA-1", source="jira")]
        loops["stale_own_prs"] = [{
            "repo": "org/repo", "pr_id": "9", "title": "stale", "url": "https://example/pr/9",
            "created_on": "2025-12-01T00:00:00Z", "review_state": "REVIEW_REQUIRED",
            "approvers": ["alice"], "ci_state": "PASSED",
        }]
        with patch("manager.staleness.aggregate_all", return_value=loops):
            resp = client.get("/api/wizard/queue")
        assert resp.status_code == 200, resp.text
        priorities = [i["priority"] for i in resp.json()["items"]]
        assert priorities == [0, 1, 2, 3, 4], f"got priorities {priorities}"


class TestCommentCap:
    def test_more_than_three_comments_caps_at_three_actions(self, client):
        loops = _empty_loops()
        loops["pr_comments_needs_reply"] = [{
            "ticket_key": "PC-9", "summary": "many",
            "comments": [
                {"id": i, "pr_repo": "org/repo", "pr_id": 1,
                 "pr_url": "https://example/pr/1", "body": f"c{i}", "suggested_reply": ""}
                for i in range(5)
            ],
            "count": 5,
        }]
        with patch("manager.staleness.aggregate_all", return_value=loops):
            resp = client.get("/api/wizard/queue")
        items = resp.json()["items"]
        assert len(items) == 1, f"expected 1 wizard item, got {items}"
        replies = [a for a in items[0]["actions"] if a["action_type"] == "post_reply"]
        assert len(replies) == 3, f"expected 3 post_reply actions, got {len(replies)}"
        assert items[0]["block_detail"]["total_needs_reply"] == 5


class TestSnoozeEntityIdConventions:
    def test_merge_ready_entity_id_is_ticket_key(self, client):
        loops = _empty_loops()
        loops["merge_ready"] = [_ticket("MR-1", prs=[_pr(pr_id=1, approvers=["bob"])])]
        with patch("manager.staleness.aggregate_all", return_value=loops):
            items = client.get("/api/wizard/queue").json()["items"]
        assert items[0]["snooze_entity_id"] == "MR-1"
        assert items[0]["source_loop"] == "merge_ready"

    def test_stale_own_prs_entity_id_is_repo_slash_pr_id(self, client):
        loops = _empty_loops()
        loops["stale_own_prs"] = [{
            "repo": "org/repo", "pr_id": "7", "title": "old", "url": "https://example/pr/7",
            "created_on": "2025-12-01T00:00:00Z",
        }]
        with patch("manager.staleness.aggregate_all", return_value=loops):
            items = client.get("/api/wizard/queue").json()["items"]
        assert items[0]["snooze_entity_id"] == "org/repo/7"
        assert items[0]["source_loop"] == "stale_own_prs"


class TestReviewerMappingFallback:
    def test_unmapped_reviewer_gets_request_re_review_stub(self, client):
        loops = _empty_loops()
        loops["stale_own_prs"] = [{
            "repo": "org/repo", "pr_id": "5", "title": "t", "url": "https://example/pr/5",
            "created_on": "2025-12-01T00:00:00Z", "approvers": ["unknown_user"],
        }]
        with patch("manager.staleness.aggregate_all", return_value=loops):
            items = client.get("/api/wizard/queue").json()["items"]
        actions = items[0]["actions"]
        assert len(actions) == 1
        assert actions[0]["action_type"] == "request_re_review"
        assert "unknown_user" in actions[0]["payload"]["cli_hint"]

    def test_mapped_reviewer_gets_slack_ping_action(self, client):
        loops = _empty_loops()
        loops["stale_own_prs"] = [{
            "repo": "org/repo", "pr_id": "6", "title": "t", "url": "https://example/pr/6",
            "created_on": "2025-12-01T00:00:00Z", "approvers": ["alice"],
        }]
        with patch("manager.staleness.aggregate_all", return_value=loops):
            items = client.get("/api/wizard/queue").json()["items"]
        actions = items[0]["actions"]
        assert len(actions) == 1
        assert actions[0]["action_type"] == "slack_ping"
        assert actions[0]["payload"]["target"] == {"workspace": "tipucorp", "channel": "D1"}


class TestSlackPing:
    def test_dry_run_returns_would_send_without_calling_bridge(self, client):
        with patch("web.wizard._post_slack_bridge") as bridge:
            resp = client.post("/api/wizard/slack_ping", json={
                "github_login": "alice", "text": "hello", "dry_run": True,
            })
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"would_send": {"workspace": "tipucorp", "channel": "D1", "text": "hello"}}
        bridge.assert_not_called()

    def test_real_send_forwards_to_bridge_and_returns_its_response(self, client):
        with patch("web.wizard._post_slack_bridge",
                   return_value={"ok": True, "ts": "1234.5678", "channel": "D1"}) as bridge:
            resp = client.post("/api/wizard/slack_ping", json={
                "github_login": "alice", "text": "real ping", "dry_run": False,
            })
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"ok": True, "ts": "1234.5678", "channel": "D1"}
        bridge.assert_called_once()
        payload = bridge.call_args[0][0]
        assert payload == {"workspace": "tipucorp", "channel": "D1", "text": "real ping"}

    def test_bridge_unavailable_returns_502(self, client):
        with patch("web.wizard._post_slack_bridge",
                   return_value={"ok": False, "error": "slack bridge unavailable: ConnectionRefusedError",
                                 "_status": 502}):
            resp = client.post("/api/wizard/slack_ping", json={
                "github_login": "alice", "text": "x", "dry_run": False,
            })
        assert resp.status_code == 502, resp.text
        assert resp.json()["ok"] is False
        assert "slack bridge unavailable" in resp.json()["error"]

    def test_no_mapping_returns_400(self, client):
        resp = client.post("/api/wizard/slack_ping", json={
            "github_login": "nobody", "text": "x", "dry_run": False,
        })
        assert resp.status_code == 400, resp.text
        assert "no slack target" in resp.json()["error"]

    def test_missing_text_returns_400(self, client):
        resp = client.post("/api/wizard/slack_ping", json={
            "github_login": "alice", "text": "", "dry_run": True,
        })
        assert resp.status_code == 400, resp.text


class TestDraftPing:
    def test_no_mapping_returns_target_null(self, client):
        with patch("core.llm.run_fast", return_value="hey can you take a look?"):
            resp = client.post("/api/wizard/draft_ping", json={
                "github_login": "nobody", "pr_url": "https://example/pr/1",
                "pr_title": "fix x", "context": "",
            })
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["target"] is None
        assert body["text"] == "hey can you take a look?"

    def test_with_mapping_returns_target_object(self, client):
        with patch("core.llm.run_fast", return_value="ping text"):
            resp = client.post("/api/wizard/draft_ping", json={
                "github_login": "alice", "pr_url": "u", "pr_title": "t", "context": "",
            })
        body = resp.json()
        assert body["target"] == {"workspace": "tipucorp", "channel": "D1"}
        assert body["text"] == "ping text"


class TestConfigDefaults:
    def test_load_config_without_slack_targets_section_defaults_to_empty_dict(self, tmp_path):
        cfg_path = tmp_path / "c.toml"
        cfg_path.write_text(
            "[job]\nkey='t'\nport=8000\nplatform='github'\nticket_system='jira'\n"
            "[workspace]\nroot='" + str(tmp_path) + "'\n"
        )
        from core.config import load_config
        cfg = load_config(str(cfg_path))
        assert cfg["slack_targets"] == {}

    def test_load_config_round_trips_slack_targets_entries(self, tmp_path):
        cfg_path = tmp_path / "c.toml"
        cfg_path.write_text(
            "[job]\nkey='t'\nport=8000\nplatform='github'\nticket_system='jira'\n"
            "[workspace]\nroot='" + str(tmp_path) + "'\n"
            "[slack_targets.bob]\nworkspace='ws'\nslack_user_id='U2'\ndm_channel_id='D2'\n"
        )
        from core.config import load_config
        cfg = load_config(str(cfg_path))
        assert cfg["slack_targets"]["bob"]["dm_channel_id"] == "D2"
