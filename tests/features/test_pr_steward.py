import json
from unittest.mock import MagicMock, patch

import pytest

import core.comments as comments
import core.db as db
import core.state as state
import features.pr_steward as steward
import features.ticket_states as ticket_states
from core.config import load_config
from core.tasks.registry import TaskContext, get_task
from tests.conftest import make_check, make_comment, make_ticket, make_ticket_state
from tests.features.test_scope_review import _reconciled

PR = {"repo": "api", "id": 7, "url": "https://example.com/pr/7", "branch": "PROJ-1-do-the-thing"}
HEAD = "a" * 40


def _comments():
    return [make_comment(id=100, body="Rename this to total"),
            make_comment(id=101, body="Looks good", comment_kind="issue_comment")]


def _checks(state="FAILURE"):
    return [make_check(name="lint", state="SUCCESS"), make_check(name="e2e", state=state)]


def _platform(comments=None, checks=None):
    platform = MagicMock()
    platform.get_pr_comments.return_value = _comments() if comments is None else comments
    platform.get_pr_checks.return_value = _checks() if checks is None else checks
    platform.get_pr_diff.return_value = "diff --git a/total.py b/total.py\n+def total(): ...\n"
    platform.self_id.return_value = "frshty-bot"
    platform.get_pr_info.return_value = {"state": "OPEN", "author_id": "frshty-bot",
                                         "approver_ids": [], "head_sha": HEAD}
    return platform


def _shadow(config):
    config["pr"]["steward"] = "shadow"
    return config


class TestFingerprint:
    def test_the_listing_order_does_not_change_it(self):
        one = steward.pr_fingerprint(HEAD, "main", _comments(), _checks())
        two = steward.pr_fingerprint(HEAD, "main", list(reversed(_comments())),
                                     list(reversed(_checks())))
        assert one == two

    @pytest.mark.parametrize("change", ["head", "base", "comment", "check"])
    def test_each_fact_changes_it(self, change):
        facts = {"head": HEAD, "base": "main", "comments": _comments(), "checks": _checks()}
        before = steward.pr_fingerprint(facts["head"], facts["base"], facts["comments"], facts["checks"])
        if change == "head":
            facts["head"] = "b" * 40
        elif change == "base":
            facts["base"] = "develop"
        elif change == "comment":
            facts["comments"] = _comments() + [make_comment(id=102)]
        else:
            facts["checks"] = _checks("SUCCESS")
        after = steward.pr_fingerprint(facts["head"], facts["base"], facts["comments"], facts["checks"])
        assert before != after

    def test_an_issue_comment_and_a_review_comment_with_one_id_differ(self):
        review = steward.pr_fingerprint(HEAD, "main", [make_comment(id=5)], [])
        issue = steward.pr_fingerprint(HEAD, "main", [make_comment(id=5, comment_kind="issue_comment")], [])
        assert review != issue

    def test_a_comment_body_edit_does_not_change_it(self):
        edited = [make_comment(id=100, body="Rename this to sum"), _comments()[1]]
        assert (steward.pr_fingerprint(HEAD, "main", _comments(), _checks())
                == steward.pr_fingerprint(HEAD, "main", edited, _checks()))


class TestConfigKey:
    def _write(self, tmp_path, pr_section):
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[job]\nkey = "x"\nport = 9999\nplatform = "github"\n\n'
            '[workspace]\nroot = "' + str(tmp_path) + '"\n\n[pr]\n' + pr_section)
        return str(config_file)

    def test_the_steward_is_off_by_default(self, tmp_path):
        assert load_config(self._write(tmp_path, ""))["pr"]["steward"] == "off"

    def test_shadow_is_accepted(self, tmp_path):
        assert load_config(self._write(tmp_path, 'steward = "shadow"\n'))["pr"]["steward"] == "shadow"

    def test_an_unknown_mode_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="pr.steward"):
            load_config(self._write(tmp_path, 'steward = "on"\n'))


class TestObserve:
    @pytest.fixture(autouse=True)
    def _db(self, fresh_db):
        yield

    def _fingerprints(self):
        rows = db.query_all("SELECT key, data FROM kv WHERE instance_key = 'inst'")
        return {r["key"]: json.loads(r["data"]) for r in rows}

    def _observe(self, config, ts, platform, info=None):
        info_map = {(PR["repo"], PR["id"]): {"head_sha": HEAD} if info is None else info}
        with patch("features.pr_steward.make_platform", return_value=platform), \
             patch("features.pr_steward.q.enqueue_job", return_value=1) as enqueue, \
             patch("features.pr_steward.log") as log:
            steward.observe(config, make_ticket(), ts, "http://base", "inst", info_map)
        return enqueue, log

    def test_off_reads_nothing_and_queues_nothing(self, fake_config):
        ts = make_ticket_state(status="in_review", prs=[dict(PR)])
        platform = _platform()
        enqueue, _ = self._observe(fake_config, ts, platform)
        enqueue.assert_not_called()
        platform.get_pr_comments.assert_not_called()
        assert self._fingerprints() == {}

    def test_a_new_fingerprint_queues_one_ticketless_run(self, fake_config):
        ts = make_ticket_state(status="in_review", prs=[dict(PR)])
        enqueue, log = self._observe(_shadow(fake_config), ts, _platform())
        enqueue.assert_called_once()
        args, kwargs = enqueue.call_args
        assert args == ("inst", steward.TASK)
        assert "ticket_key" not in kwargs
        fingerprint = steward.pr_fingerprint(HEAD, "main", _comments(), _checks())
        assert kwargs["payload"] == {"ticket_key": "PROJ-1", "repo": "api", "pr_id": 7,
                                     "fingerprint": fingerprint, "head_sha": HEAD}
        assert self._fingerprints() == {"pr_steward:PROJ-1/api/7": fingerprint}
        assert log.emit.call_args[0][0] == "ticket_pr_steward_queued"

    def test_the_ticket_state_is_left_as_it_was(self, fake_config):
        ts = make_ticket_state(status="in_review", prs=[dict(PR)])
        before = json.dumps(ts, sort_keys=True)
        self._observe(_shadow(fake_config), ts, _platform())
        assert json.dumps(ts, sort_keys=True) == before

    def test_an_unchanged_fingerprint_queues_nothing(self, fake_config):
        ts = make_ticket_state(status="in_review", prs=[dict(PR)])
        self._observe(_shadow(fake_config), ts, _platform())
        enqueue, _ = self._observe(fake_config, ts, _platform())
        enqueue.assert_not_called()

    def test_a_racing_scan_of_the_same_change_queues_nothing(self, fake_config):
        assert steward.claim_fingerprint("inst", "pr_steward:PROJ-1/api/7", "f1") is True
        assert steward.claim_fingerprint("inst", "pr_steward:PROJ-1/api/7", "f1") is False
        assert steward.claim_fingerprint("inst", "pr_steward:PROJ-2/api/8", "f1") is True
        assert steward.claim_fingerprint("other", "pr_steward:PROJ-1/api/7", "f1") is True
        assert steward.claim_fingerprint("inst", "pr_steward:PROJ-1/api/7", "f2") is True

    def test_another_ticket_keeps_its_fingerprint(self, fake_config):
        ts1 = make_ticket_state(status="in_review", prs=[dict(PR)])
        self._observe(_shadow(fake_config), ts1, _platform())
        other = {"repo": "web", "id": 8, "url": ""}
        ts2 = make_ticket_state(status="in_review", prs=[other])
        with patch("features.pr_steward.make_platform", return_value=_platform()), \
             patch("features.pr_steward.q.enqueue_job", return_value=1), \
             patch("features.pr_steward.log"):
            steward.observe(fake_config, make_ticket(key="PROJ-2"), ts2, "http://base", "inst",
                            {("web", 8): {"head_sha": HEAD}})
        enqueue, _ = self._observe(fake_config, ts1, _platform())
        enqueue.assert_not_called()
        assert set(self._fingerprints()) == {"pr_steward:PROJ-1/api/7", "pr_steward:PROJ-2/web/8"}

    def test_a_check_that_turns_green_queues_another_run(self, fake_config):
        ts = make_ticket_state(status="in_review", prs=[dict(PR)])
        self._observe(_shadow(fake_config), ts, _platform())
        enqueue, _ = self._observe(fake_config, ts, _platform(checks=_checks("SUCCESS")))
        enqueue.assert_called_once()

    @pytest.mark.parametrize("unreadable", ["comments", "checks", "head"])
    def test_an_unreadable_fact_queues_nothing_and_keeps_no_fingerprint(self, fake_config, unreadable):
        ts = make_ticket_state(status="in_review", prs=[dict(PR)])
        platform = _platform()
        info = None
        if unreadable == "comments":
            platform.get_pr_comments.return_value = None
        elif unreadable == "checks":
            platform.get_pr_checks.return_value = None
        else:
            info = {}
        enqueue, _ = self._observe(_shadow(fake_config), ts, platform, info)
        enqueue.assert_not_called()
        assert self._fingerprints() == {}


class TestTheInReviewHandler:
    def _handle(self, config, ts, observe):
        platform = MagicMock()
        platform.monitor_ci.return_value = ts
        with patch("features.tickets._scope_review_state", return_value="disabled"), \
             patch("features.tickets._build_pr_info_map", return_value={}), \
             patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets._merge", side_effect=lambda c, t, ts, b: ts) as merge, \
             patch("features.tickets._check_in_review", side_effect=_reconciled), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.ticket_states.pr_steward.observe", side_effect=observe) as obs, \
             patch("features.ticket_states.log") as log, \
             patch("features.ticket_states.state"):
            out, stop = ticket_states._handle_in_review_ticket(
                config, make_ticket(), ts, "http://base", "inst", True)
        return out, stop, merge, obs, log

    def test_the_poll_hands_the_pr_to_the_steward(self, fake_config):
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=[dict(PR)])
        _, stop, _, obs, _ = self._handle(fake_config, ts, None)
        obs.assert_called_once()
        assert obs.call_args[0][4] == "inst"
        assert stop is False

    def test_a_steward_error_is_logged_and_the_merge_still_runs(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=[dict(PR)])
        _, stop, merge, _, log = self._handle(fake_config, ts, RuntimeError("boom"))
        merge.assert_called_once()
        assert stop is False
        assert log.emit.call_args[0][0] == "ticket_pr_steward_failed"
        assert log.emit.call_args[1]["meta"]["reason"] == "observe_error"


def _decisions(**overrides):
    out = {
        "comments": [
            {"id": "review:100", "action": "fix", "why": "asks for a rename", "draft_reply": ""},
            {"id": "issue_comment:101", "action": "no_reply_needed", "why": "praise", "draft_reply": ""},
        ],
        "checks": [{"name": "e2e", "action": "unrelated", "why": "fails on main too"}],
        "ready_to_merge": False,
        "why": "one comment needs a fix",
    }
    out.update(overrides)
    return json.dumps(out)


class TestRunShadow:
    def _payload(self):
        return {"ticket_key": "PROJ-1", "repo": "api", "pr_id": 7,
                "fingerprint": "f" * 64, "head_sha": HEAD}

    def _run(self, config, ts, output, platform=None):
        platform = platform or _platform()
        with patch("features.pr_steward.state.load_ticket", return_value=ts), \
             patch("features.pr_steward.make_platform", return_value=platform), \
             patch("features.tickets.make_platform", return_value=platform), \
             patch("features.pr_steward.run_haiku", return_value=output) as agent, \
             patch("features.pr_steward.log") as log:
            result = steward.run_shadow(config, self._payload())
        return result, log, agent, platform

    @pytest.fixture(autouse=True)
    def _state(self, tmp_state):
        yield

    def _ts(self, fake_config, ci_unrelated_checks=None, **extra):
        ts = make_ticket_state(status="in_review", prs=[dict(PR)], **extra)
        state.save("own_prs", {"api/7": {"comments_read_ok": True,
                                         "ci_unrelated_checks": (ci_unrelated_checks or {}).get("api/7", [])}})
        comments.mark_comment_seen("test", "pr", "api/7", "100", "2026-01-01T00:00:00Z")
        comments.record_comment_fix("test", "pr", "api/7", "100")
        return ts

    def test_it_logs_the_steward_beside_the_current_code(self, fake_config):
        ts = self._ts(fake_config, ci_unrelated_checks={"api/7": ["e2e"]})
        result, log, _, _ = self._run(_shadow(fake_config), ts, _decisions())
        assert result == ("ok", "")
        event, = [c for c in log.emit.call_args_list if c[0][0] == "ticket_pr_steward_shadow"]
        meta = event[1]["meta"]
        assert meta["comments"] == [
            {"id": "review:100", "steward": "fix", "why": "asks for a rename",
             "draft_reply": "", "current": "addressed"},
            {"id": "issue_comment:101", "steward": "no_reply_needed", "why": "praise",
             "draft_reply": "", "current": "unregistered"},
        ]
        assert meta["checks"] == [{"name": "e2e", "steward": "unrelated",
                                   "why": "fails on main too", "current": "unrelated"}]
        assert meta["merge"]["steward"] is False
        assert meta["merge"]["current"]["ready_to_merge"] is False
        assert meta["merge"]["current"]["merge_enabled"] is False
        assert meta["fingerprint"] == "f" * 64

    def test_it_changes_nothing_on_the_pr(self, fake_config):
        ts = self._ts(fake_config)
        before = json.dumps(ts, sort_keys=True)
        _, _, _, platform = self._run(_shadow(fake_config), ts, _decisions())
        called = {c[0] for c in platform.method_calls}
        assert called <= {"get_pr_comments", "get_pr_checks", "get_pr_diff", "self_id",
                          "get_pr_info"}
        assert json.dumps(ts, sort_keys=True) == before

    def test_the_agent_gets_the_diff_and_no_tools(self, fake_config):
        ts = self._ts(fake_config)
        _, _, agent, _ = self._run(_shadow(fake_config), ts, _decisions())
        args, kwargs = agent.call_args
        assert "+def total(): ..." in args[0]
        assert kwargs["schema"] == steward.DECISIONS_SCHEMA
        assert set(kwargs) == {"timeout", "schema"}

    def test_the_prompt_asks_one_decision_per_comment_and_red_check(self, fake_config):
        ts = self._ts(fake_config)
        own = make_comment(id=103, body="frshty reply", author_id="frshty-bot")
        resolved = make_comment(id=104, body="done", resolved=True)
        platform = _platform(comments=_comments() + [own, resolved])
        _, _, agent, _ = self._run(_shadow(fake_config), ts, _decisions(), platform=platform)
        prompt = agent.call_args[0][0]
        assert "id review:100" in prompt and "id issue_comment:101" in prompt
        assert "review:103" not in prompt and "review:104" not in prompt
        assert "One decision for each red check: e2e" in prompt

    def test_a_decision_the_steward_left_out_is_logged_as_missing(self, fake_config):
        ts = self._ts(fake_config)
        result, log, _, _ = self._run(_shadow(fake_config), ts,
                                         _decisions(comments=[], checks=[]))
        assert result[0] == "ok"
        meta = log.emit.call_args[1]["meta"]
        assert {r["steward"] for r in meta["comments"]} == {"missing"}
        assert meta["checks"][0]["steward"] == "missing"

    def test_an_answer_outside_the_schema_fails_the_job_and_is_logged(self, fake_config):
        ts = self._ts(fake_config)
        result, log, _, _ = self._run(_shadow(fake_config), ts, '{"ready_to_merge": "yes"}')
        assert result[0] == "failed"
        assert log.emit.call_args[0][0] == "ticket_pr_steward_failed"

    @pytest.mark.parametrize("unreadable", ["get_pr_comments", "get_pr_checks", "get_pr_diff"])
    def test_a_platform_read_failure_fails_the_job_and_is_logged(self, fake_config, unreadable):
        ts = self._ts(fake_config)
        platform = _platform()
        getattr(platform, unreadable).return_value = None
        result, log, agent, _ = self._run(_shadow(fake_config), ts, _decisions(),
                                          platform=platform)
        assert result[0] == "failed"
        agent.assert_not_called()
        assert log.emit.call_args[1]["meta"]["reason"] == "platform_read_failed"

    def test_a_ticket_that_left_in_review_is_skipped(self, fake_config):
        ts = self._ts(fake_config)
        ts["status"] = "merged"
        result, _, agent, _ = self._run(_shadow(fake_config), ts, _decisions())
        assert result[0] == "skipped"
        agent.assert_not_called()

    def test_off_mode_skips_a_queued_run(self, fake_config):
        result, _, agent, _ = self._run(fake_config, self._ts(fake_config), _decisions())
        assert result[0] == "skipped"
        agent.assert_not_called()


class TestCurrentMergeReadiness:
    @pytest.fixture(autouse=True)
    def _read(self, tmp_state):
        state.save("own_prs", {"api/7": {"comments_read_ok": True}})
        yield
    def test_green_ci_and_nothing_owed_is_ready_under_auto_merge(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=[dict(PR)])
        assert steward._current_merge(fake_config, ts)["ready_to_merge"] is True

    def test_without_a_merge_rule_the_code_never_merges(self, fake_config):
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=[dict(PR)])
        current = steward._current_merge(fake_config, ts)
        assert current["gates_clear"] is True
        assert current["ready_to_merge"] is False

    def test_a_failed_scope_review_holds_auto_merge(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=[dict(PR)])
        with patch("features.tickets._scope_review_state", return_value="fail"):
            assert steward._current_merge(fake_config, ts)["ready_to_merge"] is False

    def test_an_owed_comment_is_not_ready(self, fake_config):
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=[dict(PR)])
        comments.mark_comment_processing("test", "pr", "api/7", "5", "2026-01-01T00:00:00Z")
        comments.mark_comment_manual("test", "pr", "api/7", "5", "needs a reply")
        assert steward._current_merge(fake_config, ts)["ready_to_merge"] is False


class TestTheTask:
    def test_the_task_is_registered(self):
        assert get_task(steward.TASK) is not None

    def test_the_task_reports_the_shadow_result(self, fake_config):
        ctx = TaskContext(instance_key="inst", ticket_key=None, task=steward.TASK,
                          payload={"ticket_key": "PROJ-1"}, job_id=1, triggering_event_id=None,
                          config=fake_config, registry=None, now=None)
        with patch("core.tasks.tickets.pr_steward.run_shadow", return_value=("skipped", "off")):
            result = get_task(steward.TASK)["fn"](ctx)
        assert (result.status, result.reason) == ("skipped", "off")
