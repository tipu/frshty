"""Tests for manager.watchdog — staleness buckets opening their own tasks.

Every launch is patched. These tests assert what decides whether a task is
opened, not that a tmux pane starts."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

import core.db as db
import core.queue as q
import core.state as state
from core.registry import Instances
from core.tasks.registry import get_task
from core.tasks.routes import _cron_routes
from manager import watchdog

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    state._instance_key_cv.set("test")
    yield


def _config(**watchdog_settings):
    return {
        "job": {"key": "test"},
        "workspace": {"root": "/tmp/ws", "tickets_dir": "tickets"},
        "manager": {"watchdog": watchdog_settings},
        "_base_url": "http://localhost:8000",
    }


def _park_ticket(key, reason="ci_failed"):
    state.save_ticket(key, {
        "status": "pr_failed",
        "slug": key.lower(),
        "summary": f"summary for {key}",
        "discovered_at": "2026-08-01T00:00:00Z",
        "pr_failed_reason": reason,
        "ci_fix_attempts": 3,
        "prs": [{"repo": "api", "id": 1, "url": "http://pr/1"}],
    })


def _blocked_comment(resource_id, comment_id, error_count=3, tracked=True):
    db.execute(
        "INSERT INTO comment_state(instance_key, resource_type, resource_id,"
        " comment_id, last_checked_at, state, error_count, last_error)"
        " VALUES(?,?,?,?,?,?,?,?)",
        ("test", "pr", resource_id, comment_id, "2026-09-01T00:00:00Z",
         "new", error_count, "Could not create worktree"),
    )
    if not tracked:
        return
    blob = state.load("own_prs") or {}
    blob[resource_id] = {"title": f"PR {resource_id}", "url": f"http://pr/{resource_id}",
                         "created_on": "2026-08-01T00:00:00Z"}
    state.save("own_prs", blob)


def _work_item(item_id, objective, item_state="agent_working", archived_at=None,
               contexts="", run_status="running"):
    db.execute(
        "INSERT INTO work_items(id, objective, state, archived_at, contexts,"
        " created_at, updated_at)"
        " VALUES (?, ?, ?, ?, ?, '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')",
        (item_id, objective, item_state, archived_at, contexts),
    )
    if run_status:
        _work_run(item_id, run_status)


def _work_run(item_id, status):
    db.execute(
        "INSERT INTO work_runs(work_item_id, session_id, tmux_key, cwd, status, started_at)"
        " VALUES (?, ?, ?, '/tmp', ?, '2026-09-01T00:00:00Z')",
        (item_id, f"sid-{item_id}", f"work-{item_id}", status),
    )


def _scan(config, now, **kw):
    with patch("services.ticket_doctor.launch",
               return_value={"item_id": 500, "state": "agent_working"}) as doctor, \
            patch("services.work_launch.launch",
                  return_value={"item_id": 501, "state": "agent_working"}) as launch:
        opened = watchdog.scan(config, instance_key="test", now=now, **kw)
    return opened, doctor, launch


class TestEscalationWindow:
    def test_a_first_sighting_opens_nothing(self):
        _park_ticket("DEV-200")

        opened, doctor, _ = _scan(_config(), NOW)

        assert opened == []
        assert doctor.call_count == 0
        row = db.query_one("SELECT first_seen_at FROM watchdog_observations"
                           " WHERE entity_id='DEV-200'")
        assert row["first_seen_at"] == NOW.isoformat()

    def test_the_same_entity_past_the_window_opens_a_doctor_task(self):
        _park_ticket("DEV-201")
        _scan(_config(), NOW)

        later = NOW + timedelta(hours=7)
        opened, doctor, _ = _scan(_config(), later)

        assert [o["entity_id"] for o in opened] == ["DEV-201"]
        assert opened[0]["work_item_id"] == 500
        assert doctor.call_args.args[0] is not None
        assert doctor.call_args.args[1] == "DEV-201"
        description = doctor.call_args.args[2]
        assert "pr_failed_tickets" in description
        assert "pr_failed_reason=ci_failed" in description

    def test_just_short_of_the_window_still_opens_nothing(self):
        """Negative control on the clock itself."""
        _park_ticket("DEV-202")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=5, minutes=59))

        assert opened == []

    def test_first_seen_is_not_moved_by_later_scans(self):
        _park_ticket("DEV-203")
        _scan(_config(), NOW)
        _scan(_config(), NOW + timedelta(hours=1))

        row = db.query_one("SELECT first_seen_at, last_seen_at FROM"
                           " watchdog_observations WHERE entity_id='DEV-203'")

        assert row["first_seen_at"] == NOW.isoformat()
        assert row["last_seen_at"] == (NOW + timedelta(hours=1)).isoformat()


class TestCooldown:
    def test_a_second_scan_after_opening_does_not_open_again(self):
        _park_ticket("DEV-210")
        _scan(_config(), NOW)
        _scan(_config(), NOW + timedelta(hours=7))
        db.execute("UPDATE work_items SET state='done' WHERE id=500")

        opened, doctor, _ = _scan(_config(), NOW + timedelta(hours=8))

        assert opened == []
        assert doctor.call_count == 0

    def test_after_the_cooldown_it_opens_again(self):
        _park_ticket("DEV-211")
        _scan(_config(), NOW)
        _scan(_config(), NOW + timedelta(hours=7))
        db.execute("UPDATE work_items SET state='done' WHERE id=500")

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7 + 48))

        assert [o["entity_id"] for o in opened] == ["DEV-211"]

    def test_a_failed_launch_is_retried_within_the_hour(self):
        _park_ticket("DEV-212")
        _scan(_config(), NOW)
        with patch("services.ticket_doctor.launch",
                   return_value={"error": "personal instance not loaded"}):
            assert watchdog.scan(_config(), instance_key="test",
                                 now=NOW + timedelta(hours=7)) == []
        row = db.query_one("SELECT opened_at, work_item_id FROM watchdog_observations"
                           " WHERE entity_id='DEV-212'")
        assert row["work_item_id"] is None

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=9))

        assert [o["entity_id"] for o in opened] == ["DEV-212"]


class TestAlreadyCovered:
    def test_an_open_task_naming_the_ticket_blocks_the_open(self):
        _park_ticket("DEV-220")
        _work_item(1, "why is DEV-220 stuck?")
        _scan(_config(), NOW)

        opened, doctor, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []
        assert doctor.call_count == 0

    def test_a_finished_task_naming_the_ticket_does_not_block(self):
        _park_ticket("DEV-221")
        _work_item(2, "why is DEV-221 stuck?", item_state="done")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-221"]

    def test_an_archived_task_naming_the_ticket_does_not_block(self):
        _park_ticket("DEV-222")
        _work_item(3, "why is DEV-222 stuck?", archived_at="2026-09-02T00:00:00Z")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-222"]

    def test_a_longer_key_does_not_cover_a_shorter_one(self):
        """DEV-22 must not be silenced by an open task about DEV-220."""
        _park_ticket("DEV-22")
        _work_item(4, "why is DEV-220 stuck?")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-22"]

    def test_an_item_that_never_got_a_run_does_not_cover(self):
        """work_launch writes the item and its run in separate transactions,
        so a crash between them leaves an agent_working item no agent saw."""
        _park_ticket("DEV-229")
        _work_item(14, "why is DEV-229 stuck?", contexts="test", run_status=None)
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-229"]

    def test_an_unscoped_task_does_not_cover_a_key_two_instances_know(self):
        """The database models a ticket as (instance_key, ticket_key), so a
        prefix two upstream projects share must not cross instances."""
        _park_ticket("DEV-233")
        state._instance_key_cv.set("other")
        state.save_ticket("DEV-233", {"status": "new", "slug": "dev-233",
                                      "discovered_at": "2026-08-01T00:00:00Z"})
        state._instance_key_cv.set("test")
        _work_item(15, "why is DEV-233 stuck?")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-233"]

    def test_covered_by_open_task_returns_the_covering_id(self):
        _work_item(5, "Doctor ticket DEV-223 (test, status pr_failed)")
        entry = watchdog.Entry("DEV-223", "DEV-223", "DEV-223", "")

        assert watchdog.covered_by_open_task(entry, "test") == 5

    def test_a_task_tagged_for_another_project_does_not_cover(self):
        """api#12 repeats across projects; one project must not silence another."""
        _park_ticket("DEV-224")
        _work_item(8, "why is DEV-224 stuck?", contexts="other,frshty")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-224"]

    def test_a_task_tagged_only_frshty_covers(self):
        """118 of 328 live items carry frshty alone. That label marks a task
        about the tool, which is what a doctor task is, not a project."""
        _park_ticket("DEV-227")
        _work_item(11, "why is DEV-227 stuck?", contexts="frshty")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []

    def test_a_task_tagged_for_this_project_covers(self):
        _park_ticket("DEV-225")
        _work_item(9, "why is DEV-225 stuck?", contexts="test,frshty")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []

    def test_a_launch_that_never_started_does_not_block_the_retry(self):
        """work_launch returns an item id with its error, and the item it left
        behind is failed_stale with a launch_failed run. That item started no
        agent, so it must not cover the entity forever."""
        _park_ticket("DEV-226")
        _work_item(10, "Doctor ticket DEV-226 (test, status pr_failed).",
                   item_state="failed_stale", contexts="test,frshty",
                   run_status="launch_failed")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-226"]

    def test_a_task_whose_agent_died_mid_run_still_covers(self):
        """Negative control on the same state: failed_stale also means an agent
        that started and stopped. That work exists and is resumable."""
        _park_ticket("DEV-228")
        _work_item(12, "why is DEV-228 stuck?", item_state="failed_stale",
                   contexts="test,frshty", run_status="stopped")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []


class TestSnooze:
    def test_a_snoozed_entity_opens_nothing_and_its_clock_restarts(self):
        _park_ticket("DEV-230")
        _scan(_config(), NOW)
        db.execute(
            "INSERT INTO today_snoozes(instance_key, loop_type, entity_id,"
            " snooze_until, created_at) VALUES ('test', 'pr_failed_tickets',"
            " 'DEV-230', '2099-01-01T00:00:00Z', '2026-09-03T12:00:00Z')")

        opened, doctor, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []
        assert doctor.call_count == 0
        assert db.query_one("SELECT COUNT(*) AS n FROM watchdog_observations"
                            " WHERE entity_id='DEV-230'")["n"] == 0

    def test_an_expired_snooze_on_the_same_day_does_not_block(self):
        """'2026-09-03T10:00Z' sorts after SQLite's '2026-09-03 12:00:00', so a
        text comparison reads an expired same-day snooze as still active."""
        _park_ticket("DEV-232")
        db.execute(
            "INSERT INTO today_snoozes(instance_key, loop_type, entity_id,"
            " snooze_until, created_at) VALUES ('test', 'pr_failed_tickets',"
            " 'DEV-232', ?, '2026-09-03T12:00:00Z')",
            (db.query_one("SELECT datetime('now', '-1 hour') AS t")["t"].replace(" ", "T") + "Z",))
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-232"]

    def test_an_expired_snooze_does_not_block(self):
        _park_ticket("DEV-231")
        db.execute(
            "INSERT INTO today_snoozes(instance_key, loop_type, entity_id,"
            " snooze_until, created_at) VALUES ('test', 'pr_failed_tickets',"
            " 'DEV-231', '2020-01-01T00:00:00Z', '2020-01-01T00:00:00Z')")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-231"]


class TestConditionClears:
    def test_a_cleared_condition_drops_the_row_and_restarts_the_clock(self):
        _park_ticket("DEV-240")
        _scan(_config(), NOW)
        state.transition_ticket("DEV-240", "done")

        _scan(_config(), NOW + timedelta(hours=1))

        assert db.query_one("SELECT COUNT(*) AS n FROM watchdog_observations"
                            " WHERE entity_id='DEV-240'")["n"] == 0

    def test_a_row_inside_its_cooldown_survives_a_clear(self):
        """A flapping condition must not shed its cooldown by vanishing once."""
        _park_ticket("DEV-241")
        _scan(_config(), NOW)
        _scan(_config(), NOW + timedelta(hours=7))
        state.transition_ticket("DEV-241", "done")

        _scan(_config(), NOW + timedelta(hours=8))

        row = db.query_one("SELECT opened_at FROM watchdog_observations"
                           " WHERE entity_id='DEV-241'")
        assert row is not None
        assert row["opened_at"] == (NOW + timedelta(hours=7)).isoformat()


class TestPipelineStillWorking:
    def _job(self, ticket_key, enqueued_at, status="queued"):
        job_id = q.enqueue_job("test", "fix_ci_failures", {}, ticket_key=ticket_key)
        db.execute("UPDATE jobs SET enqueued_at=?, status=? WHERE id=?",
                   (enqueued_at.isoformat(), status, job_id))
        return job_id

    def test_a_running_job_for_the_ticket_blocks_the_open(self):
        """A bucket stays populated while the job that would empty it runs."""
        _park_ticket("DEV-300")
        _scan(_config(), NOW)
        self._job("DEV-300", NOW + timedelta(hours=6), status="running")

        opened, doctor, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []
        assert doctor.call_count == 0

    def test_a_finished_job_does_not_block(self):
        _park_ticket("DEV-301")
        _scan(_config(), NOW)
        job_id = self._job("DEV-301", NOW + timedelta(hours=6))
        q.mark_done(job_id, "failed", {"error": "gave up"})

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-301"]

    def test_a_dispatch_job_does_not_block(self):
        """advance_ticket is enqueued on every cron tick and returns early when
        anything else runs, so its presence proves no progress."""
        _park_ticket("DEV-303")
        _scan(_config(), NOW)
        job_id = q.enqueue_job("test", "advance_ticket", {}, ticket_key="DEV-303")
        db.execute("UPDATE jobs SET enqueued_at=? WHERE id=?",
                   ((NOW + timedelta(hours=6)).isoformat(), job_id))

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-303"]

    def test_a_state_change_job_does_block(self):
        """Negative control on the exclusion list: set_state moves the ticket,
        so it is progress, unlike the per-tick dispatcher."""
        _park_ticket("DEV-304")
        _scan(_config(), NOW)
        job_id = q.enqueue_job("test", "set_state", {"target": "new"},
                               ticket_key="DEV-304")
        db.execute("UPDATE jobs SET enqueued_at=? WHERE id=?",
                   ((NOW + timedelta(hours=6)).isoformat(), job_id))

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []

    def test_a_job_wedged_in_the_queue_does_not_block(self):
        """A job queued for hours is the silence the watchdog exists to catch,
        so it must not be able to silence the watchdog."""
        _park_ticket("DEV-302")
        _scan(_config(), NOW)
        self._job("DEV-302", NOW - timedelta(hours=1))

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["DEV-302"]


class TestClockRestart:
    def test_a_condition_that_returns_starts_its_window_again(self):
        """A fault that cleared and came back has not persisted six hours."""
        _park_ticket("DEV-310")
        _scan(_config(), NOW)
        _scan(_config(), NOW + timedelta(hours=7))
        state.transition_ticket("DEV-310", "done")
        _scan(_config(), NOW + timedelta(hours=8))
        _park_ticket("DEV-310")

        back, _, _ = _scan(_config(), NOW + timedelta(hours=56))
        soon, _, _ = _scan(_config(), NOW + timedelta(hours=57))
        later, _, _ = _scan(_config(), NOW + timedelta(hours=63))

        assert back == []
        assert soon == []
        assert [o["entity_id"] for o in later] == ["DEV-310"]


class TestUnwatchedBuckets:
    def test_a_ticket_awaiting_a_human_reply_is_not_watched(self):
        """features/tickets.py:2473 sets needs_reply when a comment is a
        question, not when a step failed. The human reply is the design."""
        assert "pr_comments_needs_reply" not in [r.bucket for r in watchdog.RULES]

    def test_a_stale_own_pr_never_opens_a_task(self):
        """stale_own_prs waits on a reviewer, so it is not a frshty fault."""
        state.save("own_prs", {"api/9": {"title": "DEV-250", "url": "http://pr/9",
                                         "created_on": "2026-08-01T00:00:00Z"}})

        opened, doctor, launch = _scan(_config(), NOW)
        opened2, _, _ = _scan(_config(), NOW + timedelta(hours=99))

        assert opened == [] and opened2 == []
        assert doctor.call_count == 0 and launch.call_count == 0
        assert db.query_one("SELECT COUNT(*) AS n FROM watchdog_observations")["n"] == 0

    def test_every_rule_bucket_maps_seeded_rows_to_entries(self):
        """An empty-input check would pass on a mapping that returns [] always."""
        _park_ticket("DEV-320")
        _blocked_comment("api/320", "c-320")
        state.save_ticket("DEV-321", {
            "status": "in_review", "slug": "dev-321", "summary": "no ci yet",
            "discovered_at": "2026-08-01T00:00:00Z",
            "prs": [{"repo": "api", "id": 2, "url": "http://pr/2"}]})
        state.save_ticket("DEV-322", {
            "status": "reviewing", "slug": "dev-322", "summary": "not moving",
            "discovered_at": "2026-08-01T00:00:00Z"})

        found = {rule.bucket: watchdog._entries(rule.bucket, "test", _config())
                 for rule in watchdog.RULES}

        assert [e.entity_id for e in found["pr_failed_tickets"]] == ["DEV-320"]
        assert [e.entity_id for e in found["blocked_pr_comments"]] == ["api/320"]
        assert [e.entity_id for e in found["in_review_no_ci"]] == ["DEV-321"]
        assert [e.entity_id for e in found["stale_unattended"]] == ["DEV-322"]

    def test_an_in_review_ticket_with_no_ci_opens_a_task_after_two_days(self):
        state.save_ticket("DEV-323", {
            "status": "in_review", "slug": "dev-323", "summary": "no ci yet",
            "discovered_at": "2026-08-01T00:00:00Z",
            "prs": [{"repo": "api", "id": 3, "url": "http://pr/3"}]})
        _scan(_config(), NOW)

        early, _, _ = _scan(_config(), NOW + timedelta(hours=47))
        late, doctor, _ = _scan(_config(), NOW + timedelta(hours=49))

        assert early == []
        assert [o["entity_id"] for o in late] == ["DEV-323"]
        assert "in_review_no_ci" in doctor.call_args.args[2]

    def test_a_ticket_stuck_in_reviewing_opens_a_task_after_a_day(self):
        state.save_ticket("DEV-324", {
            "status": "reviewing", "slug": "dev-324", "summary": "not moving",
            "discovered_at": "2026-08-01T00:00:00Z"})
        _scan(_config(), NOW)

        early, _, _ = _scan(_config(), NOW + timedelta(hours=23))
        late, doctor, _ = _scan(_config(), NOW + timedelta(hours=25))

        assert early == []
        assert [o["entity_id"] for o in late] == ["DEV-324"]
        assert "stale_unattended" in doctor.call_args.args[2]


class TestPullRequestEntities:
    def test_several_stuck_comments_on_one_pr_open_one_task(self):
        _blocked_comment("api/42", "c-1")
        _blocked_comment("api/42", "c-2")
        _scan(_config(), NOW)

        opened, doctor, launch = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["api/42"]
        assert doctor.call_count == 0
        assert launch.call_count == 1
        objective = launch.call_args.args[0]
        assert objective.startswith("Doctor PR api#42 (test).")
        assert "c-1" in launch.call_args.kwargs["brief"]
        assert "c-2" in launch.call_args.kwargs["brief"]
        assert launch.call_args.kwargs["contexts"] == ["test", "frshty"]

    def test_a_comment_on_a_pr_outside_the_own_prs_cache_is_ignored(self):
        """A cheap pre-filter: comment_state rows outlive the PR that made
        them, and staleness fills url only from the own_prs cache. It is not
        the guarantee — own_prs.check cannot prune the last closed PR, because
        an empty fetch is also what a total API failure looks like — so
        _pr_is_live reads the state from the platform before opening."""
        _blocked_comment("api/45", "c-7", tracked=False)
        _scan(_config(), NOW)

        opened, _, launch = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []
        assert launch.call_count == 0

    def test_an_open_task_naming_the_pr_blocks_the_open(self):
        _blocked_comment("api/43", "c-3")
        _work_item(6, "frshty didnt fix the comments on api#43", contexts="test")
        _scan(_config(), NOW)

        opened, _, launch = _scan(_config(), NOW + timedelta(hours=7))

        assert opened == []
        assert launch.call_count == 0

    def test_an_untagged_task_naming_a_bare_pr_does_not_block(self):
        """api#43 repeats across projects, so an unscoped task cannot claim it.
        A ticket key carries its project in its prefix and still can."""
        _blocked_comment("api/46", "c-8")
        _work_item(13, "look at api#46 sometime")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["api/46"]

    def test_a_pr_the_platform_reports_closed_is_dropped_not_attempted(self):
        """comment_state rows outlive the PR; own_prs cannot always prune it.
        The check is a guard, not a launch: a dead PR recorded as a failed
        launch would spend an attempt every hour for as long as its rows live."""
        _blocked_comment("api/47", "c-9")
        _scan(_config(), NOW)

        class _Closed:
            def get_pr_info(self, repo, pr_id):
                return {"state": "MERGED", "approvers": []}

        with patch("manager.watchdog.make_platform", return_value=_Closed()), \
                patch("services.work_launch.launch",
                      return_value={"item_id": 501}) as launch:
            opened = watchdog.scan(_config(), instance_key="test",
                                   now=NOW + timedelta(hours=7))

        assert opened == []
        assert launch.call_count == 0
        assert db.query_one("SELECT COUNT(*) AS n FROM watchdog_launches")["n"] == 0
        assert db.query_one("SELECT COUNT(*) AS n FROM watchdog_observations"
                            " WHERE entity_id='api/47'")["n"] == 0

    def test_a_pr_the_platform_reports_open_is_opened(self):
        """Negative control: the live check must not swallow a real fault."""
        _blocked_comment("api/48", "c-10")
        _scan(_config(), NOW)

        class _Open:
            def get_pr_info(self, repo, pr_id):
                return {"state": "OPEN", "approvers": []}

        with patch("manager.watchdog.make_platform", return_value=_Open()), \
                patch("services.work_launch.launch",
                      return_value={"item_id": 501}) as launch:
            opened = watchdog.scan(_config(), instance_key="test",
                                   now=NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["api/48"]
        assert launch.call_count == 1

    def test_a_longer_pr_number_does_not_cover_a_shorter_one(self):
        _blocked_comment("api/4", "c-4")
        _work_item(7, "fix api#43 comments")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["api/4"]

    def test_a_snoozed_comment_row_is_dropped_but_a_sibling_still_fires(self):
        _blocked_comment("api/44", "c-5")
        _blocked_comment("api/44", "c-6")
        db.execute(
            "INSERT INTO today_snoozes(instance_key, loop_type, entity_id,"
            " snooze_until, created_at) VALUES ('test', 'blocked_pr_comments',"
            " 'api/44/c-5', '2099-01-01T00:00:00Z', '2026-09-03T12:00:00Z')")
        _scan(_config(), NOW)

        opened, _, launch = _scan(_config(), NOW + timedelta(hours=7))

        assert [o["entity_id"] for o in opened] == ["api/44"]
        assert "c-5" not in launch.call_args.kwargs["brief"]
        assert "c-6" in launch.call_args.kwargs["brief"]


class TestOpenBudget:
    def test_max_opens_caps_one_scan_but_still_tracks_the_rest(self):
        for n in range(4):
            _park_ticket(f"DEV-26{n}")
        _scan(_config(), NOW)

        opened, _, _ = _scan(_config(), NOW + timedelta(hours=7), max_opens=2)

        assert len(opened) == 2
        assert db.query_one("SELECT COUNT(*) AS n FROM watchdog_observations")["n"] == 4

    def test_repeated_failures_on_one_entity_spend_the_daily_ceiling(self):
        """The observation row carries only the latest attempt, so counting
        observations counted three retries against one entity as one."""
        _park_ticket("DEV-340")
        config = _config(max_opens_per_scan=1, max_opens_per_day=2)
        _scan(config, NOW)
        with patch("services.ticket_doctor.launch",
                   return_value={"error": "personal instance not loaded"}) as failed:
            for hours in (7, 8.1, 9.2, 10.3):
                watchdog.scan(config, instance_key="test",
                              now=NOW + timedelta(hours=hours))

        assert failed.call_count == 2
        assert watchdog._opened_today("test", NOW + timedelta(hours=10.3)) == 2

    def test_a_launch_that_raises_spends_the_budget_and_fails_the_scan(self):
        """An exception out of work_launch left no record, so the next cron
        tick six minutes later attempted the same entity again. It is also a
        defect, not an expected launch failure, so it must not read as a scan
        that simply found nothing."""
        _park_ticket("DEV-341")
        _park_ticket("DEV-342")
        _scan(_config(), NOW)
        with patch("services.ticket_doctor.launch",
                   side_effect=RuntimeError("tmux exploded")) as raised:
            with pytest.raises(RuntimeError):
                watchdog.scan(_config(), instance_key="test",
                              now=NOW + timedelta(hours=7), max_opens=1)

        assert raised.call_count == 1
        assert watchdog._opened_today("test", NOW + timedelta(hours=7)) == 1
        row = db.query_one("SELECT entity_id, error, work_item_id"
                           " FROM watchdog_launches")
        assert row["entity_id"] == "DEV-341"
        assert row["work_item_id"] is None
        assert "tmux exploded" in row["error"]

    def test_a_raised_launch_does_not_abandon_the_other_entities(self):
        """The scan finishes, then reports. Stopping at the first defect would
        leave every other bucket unobserved."""
        _park_ticket("DEV-343")
        _park_ticket("DEV-344")
        _scan(_config(), NOW)
        with patch("services.ticket_doctor.launch",
                   side_effect=RuntimeError("tmux exploded")):
            with pytest.raises(RuntimeError):
                watchdog.scan(_config(), instance_key="test",
                              now=NOW + timedelta(hours=7), max_opens=1)

        rows = db.query_all("SELECT entity_id FROM watchdog_observations"
                            " ORDER BY entity_id")
        assert [r["entity_id"] for r in rows] == ["DEV-343", "DEV-344"]

    def test_the_daily_ceiling_holds_across_scans(self):
        """A backlog must not keep launching agents for hours."""
        for n in range(5):
            _park_ticket(f"DEV-28{n}")
        config = _config(max_opens_per_scan=2, max_opens_per_day=3)
        _scan(config, NOW)

        first, _, _ = _scan(config, NOW + timedelta(hours=7))
        second, _, _ = _scan(config, NOW + timedelta(hours=8))
        third, _, _ = _scan(config, NOW + timedelta(hours=9))

        assert len(first) == 2
        assert len(second) == 1
        assert third == []

    def test_the_ceiling_lifts_once_the_day_rolls_off(self):
        for n in range(4):
            _park_ticket(f"DEV-29{n}")
        config = _config(max_opens_per_scan=2, max_opens_per_day=2)
        _scan(config, NOW)
        assert len(_scan(config, NOW + timedelta(hours=7))[0]) == 2

        later, _, _ = _scan(config, NOW + timedelta(hours=32))

        assert len(later) == 2

    def test_a_failed_launch_still_spends_the_budget(self):
        """A work layer that is down must not be hammered once per due entity."""
        for n in range(4):
            _park_ticket(f"DEV-33{n}")
        _scan(_config(), NOW)
        with patch("services.ticket_doctor.launch",
                   return_value={"error": "personal instance not loaded"}) as failed:
            watchdog.scan(_config(), instance_key="test",
                          now=NOW + timedelta(hours=7), max_opens=2)

        assert failed.call_count == 2

    def test_an_upgrade_fence_stops_every_launch_for_a_day(self):
        """migrations/029 writes the fence. It is a marker, not a row count, so
        it holds whatever max_opens_per_day is set to."""
        _park_ticket("DEV-350")
        config = _config(max_opens_per_day=99)
        _scan(config, NOW)
        db.execute(
            "INSERT INTO watchdog_launches(instance_key, bucket, entity_id,"
            " error, created_at) VALUES ('test', 'migration', 'upgrade-fence',"
            " '', ?)", ((NOW + timedelta(hours=1)).isoformat(),))

        fenced, doctor, _ = _scan(config, NOW + timedelta(hours=7))
        lifted, _, _ = _scan(config, NOW + timedelta(hours=26))

        assert fenced == []
        assert doctor.call_count == 0
        assert [o["entity_id"] for o in lifted] == ["DEV-350"]

    def test_a_fence_row_is_not_counted_as_an_attempt(self):
        """Negative control: once the fence lapses it must not also eat cap."""
        db.execute(
            "INSERT INTO watchdog_launches(instance_key, bucket, entity_id,"
            " error, created_at) VALUES ('test', 'migration', 'upgrade-fence',"
            " '', ?)", (NOW.isoformat(),))

        assert watchdog._opened_today("test", NOW + timedelta(hours=1)) == 0

    def test_the_budget_comes_from_config(self):
        for n in range(3):
            _park_ticket(f"DEV-27{n}")
        config = _config(max_opens_per_scan=1)
        _scan(config, NOW)

        opened, _, _ = _scan(config, NOW + timedelta(hours=7))

        assert len(opened) == 1


class TestRun:
    def test_disabled_scans_nothing(self):
        _park_ticket("DEV-280")
        with patch.object(watchdog, "scan") as scan:
            assert watchdog.run(_config(enabled=False), instance_key="test") == []
        assert scan.call_count == 0

    def test_the_interval_gates_the_next_scan(self):
        config = _config(scan_interval_minutes=30)
        with patch.object(watchdog, "scan", return_value=[]) as scan:
            watchdog.run(config, instance_key="test", now=NOW)
            watchdog.run(config, instance_key="test", now=NOW + timedelta(minutes=20))
            watchdog.run(config, instance_key="test", now=NOW + timedelta(minutes=31))

        assert scan.call_count == 2

    def test_the_scan_timestamp_is_recorded(self):
        with patch.object(watchdog, "scan", return_value=[]):
            watchdog.run(_config(), instance_key="test", now=NOW)

        assert state.load("watchdog")["last_scan_at"] == NOW.isoformat()

    def test_a_crashed_scan_does_not_suppress_the_next_one(self):
        """Recording the timestamp first would hide a crash for 30 minutes."""
        with patch.object(watchdog, "scan", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError):
                watchdog.run(_config(), instance_key="test", now=NOW)

        assert state.load("watchdog").get("last_scan_at") is None
        assert watchdog.due_for_scan(_config(), NOW + timedelta(minutes=1)) is True


class TestInstanceIsolation:
    def test_another_instances_parked_ticket_is_not_watched(self):
        """Negative control: the watchdog is per instance."""
        _park_ticket("DEV-290")
        _scan(_config(), NOW)

        opened = watchdog.scan(_config(), instance_key="other",
                               now=NOW + timedelta(hours=7))

        assert opened == []


class TestCronRouting:
    """The scan has to be reached by something. Registration alone is not
    delivery: a task nobody enqueues is dead code."""

    def _tasks(self, features):
        instances = Instances()
        reg = instances.add({"job": {"key": "test"}, "features": features,
                             "workspace": {"root": "/tmp/ws"}})
        jobs = _cron_routes({"instance_key": "test"}, {"test": reg})
        return {j["task"] for j in jobs}

    def test_a_ticket_instance_is_scanned(self):
        assert "watchdog_scan" in self._tasks({"tickets": True})

    def test_a_pr_only_instance_is_scanned(self):
        assert "watchdog_scan" in self._tasks({"review_prs": True})

    def test_an_instance_with_neither_is_not_scanned(self):
        assert "watchdog_scan" not in self._tasks({})

    def test_the_task_is_registered(self):
        assert get_task("watchdog_scan") is not None
