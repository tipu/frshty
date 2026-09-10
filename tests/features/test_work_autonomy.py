"""The board changes that took the operator out of the loop.

Each class covers one finding of the autonomy review: the delivery rule, the
push gate baseline, the doctor's repair scope, the debrief retry budget,
progress lines, the stop detector, duplicate questions, the commit gate
rewrite, auto-archiving, and the proposals and follow-ups the board acts on by
itself.
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import core.db as db
import core.llm as llm
from features import slack_conversations
from manager import watchdog
from services import ticket_doctor, work_debrief, work_launch, work_store


def _mkrun(objective="autonomy item", provider="claude"):
    item_id = work_store.create_item(objective)
    sid = f"sid-auto-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp", provider=provider)
    return item_id, sid


def _events(item_id, kind):
    return db.query_all(
        "SELECT payload FROM work_events WHERE work_item_id = ? AND kind = ? ORDER BY id",
        (item_id, kind))


def _commit(repo, name, body="x = 1\n"):
    (repo / name).write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t",
                    "commit", "-q", "-m", name], cwd=repo, check=True)


def _repo_with_origin(tmp_path):
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", str(origin)], check=True)
    repo = tmp_path / "clone"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    subprocess.run(["git", "remote", "add", "origin", str(origin)], cwd=repo, check=True)
    _commit(repo, "base.py")
    subprocess.run(["git", "push", "-q", "-u", "origin", "HEAD"], cwd=repo, check=True)
    _commit(repo, "change.py")
    return repo


class TestDeliveryRule:
    def test_launch_and_continue_prompts_put_delivery_inside_the_objective(self):
        for prompt in (work_store.DELIVERY_RULE, work_store.CONTINUE_PROMPT):
            assert "Delivery is part of the objective" in prompt
            assert "Do not end with an offer" in prompt

    def test_progress_rule_names_the_marker(self):
        assert work_store.PROGRESS_MARKER in work_store.PROGRESS_RULE
        assert work_store.PROGRESS_MARKER in work_store.CONTINUE_PROMPT


class TestPushGateBaseline:
    def _lint_passes(self, monkeypatch):
        monkeypatch.setattr(work_launch.git_util, "lint_files",
                            lambda repo, files: {"status": "pass", "exit_code": 0,
                                                 "output": "ok"})

    def test_a_suite_already_red_at_the_merge_base_allows_the_push(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun("already red")
        repo = _repo_with_origin(tmp_path)
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: repo)
        self._lint_passes(monkeypatch)
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda d: (["bash", "-c", "echo boom; exit 1"], {}))
        out = work_launch.gate_push(sid, "git push", str(repo))
        assert out["decision"] == "allow"
        assert "merge base" in out["reason"]
        payload = json.loads(_events(item_id, "push_gate")[0]["payload"])
        assert payload["verdict"] == "already_red"
        assert payload["baseline"]["result"] == "fail"

    def test_a_suite_green_at_the_merge_base_still_denies(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun("newly red")
        repo = _repo_with_origin(tmp_path)
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: repo)
        self._lint_passes(monkeypatch)
        # The head of the branch adds the file the runner fails on, so the
        # same command passes at the merge base and fails at HEAD.
        (repo / "boom").write_text("")
        runner = ["bash", "-c", "test ! -e boom || { echo boom; exit 1; }"]
        monkeypatch.setattr(work_launch, "_detect_runner", lambda d: (runner, {}))
        out = work_launch.gate_push(sid, "git push", str(repo))
        assert out["decision"] == "deny"
        assert "passes at the merge base" in out["reason"]
        payload = json.loads(_events(item_id, "push_gate")[0]["payload"])
        assert payload["verdict"] == "fail"
        assert payload["baseline"]["result"] == "pass"

    def test_an_unresolvable_merge_base_keeps_denying(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun("no base")
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: tmp_path)
        monkeypatch.setattr(work_launch, "_outgoing_files", lambda r: ["a.py"])
        self._lint_passes(monkeypatch)
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda d: (["bash", "-c", "echo 1 failed; exit 1"], {}))
        out = work_launch.gate_push(sid, "git push", str(tmp_path))
        assert out["decision"] == "deny"
        assert json.loads(_events(item_id, "push_gate")[0]["payload"])["verdict"] == "fail"

    def test_the_baseline_runs_once_per_repository_and_commit(self, tmp_path, monkeypatch):
        _, sid = _mkrun("cached baseline")
        repo = _repo_with_origin(tmp_path)
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: repo)
        self._lint_passes(monkeypatch)
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda d: (["bash", "-c", "echo boom; exit 1"], {}))
        runs = []
        real = work_launch._run_repo_tests

        def counted(repo_dir, cmd, env, timeout):
            runs.append(str(repo_dir))
            return real(repo_dir, cmd, env, timeout)

        monkeypatch.setattr(work_launch, "_run_repo_tests", counted)
        work_launch.gate_push(sid, "git push", str(repo))
        work_launch.gate_push(sid, "git push", str(repo))
        assert len([r for r in runs if r != str(repo)]) == 1
        assert work_launch._baseline_store(repo).is_file(), "the answer outlives the process"

    def test_a_cached_baseline_never_answers_for_a_different_suite(self, tmp_path, monkeypatch):
        _, sid = _mkrun("different suite")
        repo = _repo_with_origin(tmp_path)
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: repo)
        self._lint_passes(monkeypatch)
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda d: (["bash", "-c", "echo boom; exit 1"], {}))
        assert work_launch.gate_push(sid, "git push", str(repo))["decision"] == "allow"
        # A different suite at HEAD, still failing. The cached answer is about
        # the old command and must not excuse this one.
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda d: (["bash", "-c", "echo other; exit 1"], {}))
        monkeypatch.setattr(work_launch, "_run_baseline",
                            lambda repo, base, head: {"result": "unresolved", "base": base,
                                                      "head_cmd": head, "note": "stub", "cmd": ""})
        assert work_launch.gate_push(sid, "git push", str(repo))["decision"] == "deny"

    def test_a_dependency_tree_that_reaches_into_the_repository_is_not_linked(self, tmp_path):
        """An editable install points the virtualenv at the code under test, so
        linking it would make the baseline run HEAD and call every regression
        pre-existing."""
        repo = tmp_path / "repo"
        (repo / ".venv" / "lib" / "site-packages").mkdir(parents=True)
        (repo / ".venv" / "lib" / "site-packages" / "app.pth").write_text(str(repo) + "\n")
        (repo / "node_modules").mkdir()
        tree = tmp_path / "tree"
        tree.mkdir()
        assert work_launch._link_dependencies(repo, tree) == (["node_modules"], [".venv"])
        assert not (tree / ".venv").exists()

    def test_a_workspace_link_is_read_but_an_internal_one_is_not(self, tmp_path):
        """A dependency tree is full of links into its own directory: pnpm
        builds one per stored package. Reading those as workspace links would
        refuse to link node_modules in every repository that has one."""
        repo = tmp_path / "repo"
        modules = repo / "node_modules"
        (modules / "store").mkdir(parents=True)
        (modules / "internal").symlink_to(modules / "store", target_is_directory=True)
        (modules / "loop").symlink_to(modules, target_is_directory=True)
        assert work_launch._points_into(modules, repo) is False
        (repo / "packages" / "core").mkdir(parents=True)
        (modules / "core").symlink_to(repo / "packages" / "core", target_is_directory=True)
        assert work_launch._points_into(modules, repo) is True

    def test_a_virtualenv_of_ordinary_dependencies_is_linked(self, tmp_path):
        repo = tmp_path / "repo"
        site = repo / ".venv" / "lib" / "site-packages"
        site.mkdir(parents=True)
        (site / "other.pth").write_text("/usr/lib/python3/dist-packages\n")
        tree = tmp_path / "tree"
        tree.mkdir()
        assert work_launch._link_dependencies(repo, tree) == ([".venv"], [])

    def test_an_internal_bin_link_does_not_refuse_the_whole_tree(self, tmp_path):
        """node_modules/.bin/jest -> ../jest/bin/jest.js resolves inside the
        repository. Reading it as a workspace link would refuse node_modules in
        every repository that has one."""
        repo = tmp_path / "repo"
        (repo / "node_modules" / "jest" / "bin").mkdir(parents=True)
        (repo / "node_modules" / "jest" / "bin" / "jest.js").write_text("x")
        (repo / "node_modules" / ".bin").mkdir()
        (repo / "node_modules" / ".bin" / "jest").symlink_to("../jest/bin/jest.js")
        assert work_launch._points_into(repo / "node_modules", repo) is False

    def test_a_baseline_that_cannot_use_the_dependencies_is_unresolved(self, tmp_path, monkeypatch):
        """A baseline run without the dependencies fails for the wrong reason,
        and a failing baseline is what lets the push through."""
        item_id, sid = _mkrun("refused dependencies")
        repo = _repo_with_origin(tmp_path)
        (repo / "node_modules" / "pkg").mkdir(parents=True)
        (repo / "src").mkdir()
        (repo / "node_modules" / "own").symlink_to(repo / "src", target_is_directory=True)
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: repo)
        self._lint_passes(monkeypatch)
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda d: (["bash", "-c", "echo boom; exit 1"], {}))
        out = work_launch.gate_push(sid, "git push", str(repo))
        assert out["decision"] == "deny"
        payload = json.loads(_events(item_id, "push_gate")[0]["payload"])
        assert payload["baseline"]["result"] == "unresolved"
        assert "node_modules" in payload["baseline"]["note"]

    def test_a_baseline_that_cannot_make_its_directory_is_unresolved(self, tmp_path, monkeypatch):
        def full_disk(prefix=""):
            raise OSError("No space left on device")

        monkeypatch.setattr(work_launch.tempfile, "mkdtemp", full_disk)
        out = work_launch._run_baseline(_repo_with_origin(tmp_path), "abc123", "anything")
        assert out["result"] == "unresolved"
        assert "OSError" in out["note"]

    def test_a_cached_baseline_expires(self, tmp_path):
        store = tmp_path / "baseline.json"
        old = (datetime.now(timezone.utc)
               - timedelta(seconds=work_launch.BASELINE_MAX_AGE_SECONDS + 60)).isoformat()
        work_launch._write_baseline(store, {"base": "sha", "head_cmd": "cmd",
                                            "result": "fail", "at": old})
        assert work_launch._read_baseline(store, "sha", "cmd") is None
        work_launch._write_baseline(store, {"base": "sha", "head_cmd": "cmd",
                                            "result": "fail", "at": work_launch._now_iso()})
        assert work_launch._read_baseline(store, "sha", "cmd")["result"] == "fail"

    def test_a_baseline_leaves_no_worktree_and_no_temporary_directory(self, tmp_path, monkeypatch):
        repo = _repo_with_origin(tmp_path)

        def boom(_):
            raise TypeError("bad package.json")

        monkeypatch.setattr(work_launch, "_detect_runner", boom)
        out = work_launch._run_baseline(repo, work_launch._merge_base(repo), "anything")
        assert out["result"] == "unresolved"
        assert "TypeError" in out["note"]
        listed = subprocess.run(["git", "worktree", "list"], cwd=repo,
                                capture_output=True, text=True).stdout
        assert "frshty-gate-baseline" not in listed

    def test_a_commit_on_the_same_line_as_the_push_still_runs_the_suite(self, tmp_path, monkeypatch):
        """`git commit -m fix && git push` reads as no outgoing change, because
        the commit has not run yet. Skipping the suite there would push exactly
        the work the gate exists to test."""
        _, sid = _mkrun("commit then push")
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: tmp_path)
        monkeypatch.setattr(work_launch, "_outgoing_files", lambda r: [])
        self._lint_passes(monkeypatch)
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda d: (["bash", "-c", "echo 1 failed; exit 1"], {}))
        out = work_launch.gate_push(sid, 'git commit -m "fix" && git push', str(tmp_path))
        assert out["decision"] == "deny"
        assert "1 failed" in out["reason"]

    def test_a_push_that_changes_nothing_here_does_not_run_the_suite(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun("nothing outgoing")
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: tmp_path)
        monkeypatch.setattr(work_launch, "_outgoing_files", lambda r: [])
        called = []
        monkeypatch.setattr(work_launch, "_gate_tests", lambda repo: called.append(1))
        out = work_launch.gate_push(sid, "git push", str(tmp_path))
        assert out["decision"] == "allow"
        assert called == []
        assert json.loads(_events(item_id, "push_gate")[0]["payload"])["verdict"] == "pass"


class TestRepairScope:
    def test_the_doctor_repairs_instead_of_waiting(self):
        assert "until the operator approves" not in ticket_doctor.PIPELINE_MAP
        assert "recorded\nauthority" in ticket_doctor.PIPELINE_MAP

    def test_the_watchdog_repairs_but_never_posts(self):
        assert "until the operator approves" not in watchdog._PR_COMMENT_MAP
        assert "recorded authority" in " ".join(watchdog._PR_COMMENT_MAP.split())
        assert "external communication" in " ".join(watchdog._PR_COMMENT_MAP.split())


class TestQuotaGuard:
    def test_the_observed_spend_limit_message_trips_the_guard(self):
        observed = ("You've hit your individual spend limit · run /usage-credits "
                    "to ask your admin for a higher limit · your session limit "
                    "resets 8:50am (America/Los_Angeles)")
        assert llm._llm_limit_reason(observed) is not None

    def test_an_ordinary_error_does_not_trip_the_guard(self):
        assert llm._llm_limit_reason("API Error: 529 Overloaded") is None


class TestDebriefBudget:
    def _transcript(self, tmp_path, name="t.jsonl"):
        t = tmp_path / name
        t.write_text(json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "did the work"}]}}) + "\n")
        return t

    def _done_item(self, tmp_path, objective):
        item_id = work_store.create_item(objective)
        work_store.add_run(item_id, f"sid-deb-{item_id}", f"work-{item_id}", "/tmp")
        with db.tx() as c:
            c.execute("UPDATE work_runs SET transcript_path = ? WHERE work_item_id = ?",
                      (str(self._transcript(tmp_path, f"t{item_id}.jsonl")), item_id))
        work_store.apply_action(item_id, "done")
        return item_id

    def test_a_quota_outage_postpones_and_spends_no_attempt(self, tmp_path, monkeypatch):
        item_id = self._done_item(tmp_path, "quota debrief")

        def blocked(prompt):
            llm._flag_guard_blocked()
            raise RuntimeError("llm invocation failed")

        monkeypatch.setattr(work_debrief, "_run_claude", blocked)
        out = work_debrief.run_debrief(item_id)
        assert "postponed" in out["error"]
        assert _events(item_id, "debrief_failed") == []
        assert len(_events(item_id, "debrief_postponed")) == 1

    def test_a_postponed_item_waits_for_its_retry_time_then_runs(self, tmp_path, monkeypatch):
        item_id = self._done_item(tmp_path, "postponed debrief")
        work_debrief._postpone(item_id, "llm guard active")
        assert item_id not in work_debrief._pending_done_items()
        with db.tx() as c:
            c.execute("UPDATE work_events SET payload = ? WHERE work_item_id = ? "
                      "AND kind = 'debrief_postponed'",
                      (db.dump_json({"reason": "llm guard active",
                                     "retry_after": "2000-01-01T00:00:00+00:00"}),
                       item_id))
        assert item_id in work_debrief._pending_done_items()

    def test_a_missing_transcript_postpones_instead_of_failing(self, tmp_path):
        item_id = work_store.create_item("no transcript debrief")
        work_store.add_run(item_id, f"sid-nt-{item_id}", f"work-{item_id}", "/tmp")
        work_store.apply_action(item_id, "done")
        out = work_debrief.run_debrief(item_id)
        assert "no transcript" in out["error"]
        assert _events(item_id, "debrief_failed") == []

    def test_endless_postponement_is_skipped_for_good(self, tmp_path):
        item_id = self._done_item(tmp_path, "endless postpone")
        for _ in range(work_debrief.MAX_POSTPONED_ATTEMPTS):
            work_debrief._postpone(item_id, "llm guard active")
        assert len(_events(item_id, "debrief_skipped")) == 1
        assert item_id not in work_debrief._pending_done_items()
        assert work_debrief.debrief_status(item_id) == "exhausted"

    def test_a_new_run_makes_a_summarised_item_pending_again(self, tmp_path, monkeypatch):
        item_id = self._done_item(tmp_path, "stale summary")
        monkeypatch.setattr(work_debrief, "_run_claude", lambda p: json.dumps(
            {"summary": "done", "followups": []}))
        work_debrief.run_debrief(item_id)
        assert item_id not in work_debrief._pending_done_items()
        work_store.add_run(item_id, f"sid-again-{item_id}", f"work-{item_id}", "/tmp")
        assert item_id in work_debrief._pending_done_items()

    def test_a_message_written_during_the_debrief_leaves_the_item_pending(self, tmp_path, monkeypatch):
        """The revision is what says the summary is current. Sampled after the
        dialogue was rendered, a message written between the two reads would be
        stamped onto a summary that never saw it."""
        item_id = self._done_item(tmp_path, "raced summary")
        path = db.query_one("SELECT transcript_path FROM work_runs WHERE work_item_id = ?",
                            (item_id,))["transcript_path"]

        def grows_the_transcript(prompt):
            with open(path, "a") as f:
                f.write(json.dumps({"type": "assistant", "message": {"content": [
                    {"type": "text", "text": "one more correction"}]}}) + "\n")
            return json.dumps({"summary": "done", "followups": []})

        monkeypatch.setattr(work_debrief, "_run_claude", grows_the_transcript)
        work_debrief.run_debrief(item_id)
        assert item_id in work_debrief._pending_done_items()

    def test_an_old_summary_without_a_run_id_stays_settled(self, tmp_path, monkeypatch):
        item_id = self._done_item(tmp_path, "legacy summary")
        work_debrief._record_debrief_event(item_id, "debrief_done", {"followups": 0})
        assert item_id not in work_debrief._pending_done_items()

    def test_the_rolling_window_gives_a_stuck_item_another_day(self, tmp_path):
        item_id = self._done_item(tmp_path, "stuck yesterday")
        old = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        for _ in range(work_debrief.MAX_FAILED_ATTEMPTS):
            work_debrief._record_debrief_event(item_id, "debrief_failed", {"error": "x"})
        with db.tx() as c:
            c.execute("UPDATE work_events SET created_at = ? WHERE work_item_id = ? "
                      "AND kind = 'debrief_failed'", (old, item_id))
        assert item_id in work_debrief._pending_done_items()

    def test_todays_failures_hold_the_item_back(self, tmp_path):
        item_id = self._done_item(tmp_path, "stuck today")
        for _ in range(work_debrief.MAX_FAILED_ATTEMPTS):
            work_debrief._record_debrief_event(item_id, "debrief_failed", {"error": "x"})
        assert item_id not in work_debrief._pending_done_items()
        assert work_debrief.debrief_status(item_id) == "retrying"


class TestProgressLines:
    def test_a_progress_line_becomes_the_checkpoint(self):
        item_id, sid = _mkrun("progress item")
        published = work_store.record_progress(
            sid, "", texts=["PROGRESS: read the gate; writing the baseline next"])
        assert published.startswith("read the gate")
        item = db.query_one("SELECT current_checkpoint FROM work_items WHERE id = ?", (item_id,))
        assert item["current_checkpoint"] == published
        assert len(_events(item_id, "progress")) == 1

    def test_the_same_line_twice_records_one_event(self):
        item_id, sid = _mkrun("repeat progress")
        work_store.record_progress(sid, "", texts=["PROGRESS: same line"])
        assert work_store.record_progress(sid, "", texts=["PROGRESS: same line"]) == ""
        assert len(_events(item_id, "progress")) == 1

    def test_the_newest_line_wins(self):
        item_id, sid = _mkrun("newest progress")
        work_store.record_progress(sid, "", texts=["PROGRESS: first", "PROGRESS: second"])
        item = db.query_one("SELECT current_checkpoint FROM work_items WHERE id = ?", (item_id,))
        assert item["current_checkpoint"] == "second"

    def test_a_message_without_the_marker_publishes_nothing(self):
        item_id, sid = _mkrun("no progress")
        assert work_store.record_progress(sid, "", texts=["ordinary text"]) == ""
        assert _events(item_id, "progress") == []


class TestStopDetector:
    def test_a_question_mark_alone_no_longer_parks_the_item(self):
        for tail in ("Did that work? Yes: the suite is green now.",
                     "## What changed?\nThe gate reads the merge base.",
                     "Ran `git log --oneline | head -3`. All three commits are on main."):
            assert work_store._blocked_on_operator(tail) is False, tail

    def test_a_real_request_still_parks_the_item(self):
        assert work_store._blocked_on_operator(
            "Send another code when the prompt appears.") is True

    def test_a_question_written_in_bold_still_parks_the_item(self):
        assert work_store._blocked_on_operator(
            "**Should I delete the production database?**") is True

    def test_a_question_followed_by_its_options_still_parks_the_item(self):
        assert work_store._blocked_on_operator(
            "Should I deploy to production or staging?\n"
            "- production, it is the release branch\n"
            "- staging, and I verify there first") is True

    def test_a_question_whose_options_wrap_still_parks_the_item(self):
        assert work_store._blocked_on_operator(
            "Which destination should I use?\n"
            "- staging\n"
            "  (internal only)\n"
            "- production") is True

    def test_a_heading_above_prose_still_does_not_park_the_item(self):
        assert work_store._blocked_on_operator(
            "## What changed?\nThe gate now reads the merge base.") is False

    def test_a_spent_budget_records_why_it_stopped(self, tmp_path):
        item_id, sid = _mkrun("capped item")
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_you', continues_used = 5, "
                      "continue_cap = 5 WHERE id = ?", (item_id,))
        assert work_store.maybe_autocontinue(sid, "", tail="still working on it") == "capped"
        payload = json.loads(_events(item_id, "autocontinue_stopped")[0]["payload"])
        assert payload["outcome"] == "capped"
        assert "budget" in payload["reason"]

    def test_a_reopen_gives_a_new_budget(self):
        item_id, _ = _mkrun("reopened item")
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_ack', continues_used = 5 "
                      "WHERE id = ?", (item_id,))
        work_store.apply_action(item_id, "reopen")
        item = db.query_one("SELECT continues_used FROM work_items WHERE id = ?", (item_id,))
        assert item["continues_used"] == 0

    def test_a_background_wait_parks_instead_of_spending_the_budget(self, tmp_path):
        item_id, sid = _mkrun("background wait")
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))
        transcript = tmp_path / "bg.jsonl"
        transcript.write_text("")
        import services.work_store as ws
        original = ws.pending_background_tasks
        ws.pending_background_tasks = lambda path: True
        try:
            outcome = work_store.maybe_autocontinue(
                sid, str(transcript), tail="The background task notifies me when it finishes.")
        finally:
            ws.pending_background_tasks = original
        assert outcome == "waiting_external"
        item = db.query_one("SELECT state, continues_used FROM work_items WHERE id = ?",
                            (item_id,))
        assert item["state"] == "waiting_external"
        assert item["continues_used"] == 0


class TestDuplicateQuestions:
    def _ask(self, sid, header, question):
        return work_store.record_question(
            sid, {"questions": [{"header": header, "question": question}]})

    def test_the_same_header_asked_twice_is_dropped(self):
        item_id, sid = _mkrun("repeat asker")
        assert self._ask(sid, "ECS safety", "Is the ECS change safe?") == "recorded"
        assert self._ask(sid, "ECS safety", "Can I roll the ECS change?") == "duplicate"
        assert len(_events(item_id, "question_asked")) == 1
        assert len(_events(item_id, "question_dropped")) == 1

    def test_a_new_question_is_recorded(self):
        item_id, sid = _mkrun("new asker")
        assert self._ask(sid, "ECS safety", "Is the ECS change safe?") == "recorded"
        assert self._ask(sid, "Prod rollout", "Roll it to prod?") == "recorded"
        assert len(_events(item_id, "question_asked")) == 2

    def test_the_same_question_after_an_answer_is_recorded_again(self):
        item_id, sid = _mkrun("answered asker")
        self._ask(sid, "ECS safety", "Is the ECS change safe?")
        with db.tx() as c:
            c.execute("UPDATE work_items SET pending_question = '' WHERE id = ?", (item_id,))
        assert self._ask(sid, "ECS safety", "Is the ECS change safe?") == "recorded"

    def test_the_hook_tells_the_agent_the_repeat_was_dropped(self, tmp_path):
        item_id, sid = _mkrun("hook asker")
        self._ask(sid, "ECS safety", "Is the ECS change safe?")
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=json.dumps({"session_id": sid, "hook_event_name": "PreToolUse",
                              "tool_name": "AskUserQuestion",
                              "tool_input": {"questions": [
                                  {"header": "ECS safety", "question": "Safe?"}]}}),
            capture_output=True, text=True, timeout=120,
            env={**os.environ, "FRSHTY_DB": str(db._DB_PATH)})
        assert r.returncode == 0, r.stderr
        reason = json.loads(r.stdout)["hookSpecificOutput"]["permissionDecisionReason"]
        assert "already asked this question" in reason


class TestCommitGateRewrite:
    def _heredoc(self, message):
        return 'git commit -m "$(cat <<\'MSG\'\n' + message + '\nMSG\n)"'

    def test_a_session_trailer_is_stripped_and_the_commit_allowed(self):
        item_id, sid = _mkrun("stripped commit")
        command = self._heredoc(
            "Gate the push on the delta\n\n"
            "Claude-Session: https://claude.ai/code/session_01Rc4rHada8sYLmk4zYTW8Gu")
        out = work_launch.gate_commit(sid, command, "/tmp")
        assert out["decision"] == "allow"
        assert "Claude-Session" not in out["command"]
        assert "Gate the push on the delta" in out["command"]
        payload = json.loads(_events(item_id, "commit_gate")[0]["payload"])
        assert payload["verdict"] == "stripped"

    def test_a_message_that_is_only_attribution_still_denies(self):
        _, sid = _mkrun("all attribution commit")
        out = work_launch.gate_commit(
            sid, 'git commit -m "Generated with Claude"', "/tmp")
        assert out["decision"] == "deny"

    def test_a_message_file_is_corrected_in_place(self, tmp_path):
        _, sid = _mkrun("message file commit")
        message = tmp_path / "msg.txt"
        message.write_text("Gate the push on the delta\n\n"
                           "Claude-Session: https://claude.ai/code/session_01A\n")
        out = work_launch.gate_commit(sid, "git commit -F msg.txt", str(tmp_path))
        assert out["decision"] == "allow"
        assert "Claude-Session" not in message.read_text()
        assert "Gate the push on the delta" in message.read_text()

    def test_a_rewrite_that_would_drop_a_command_is_refused(self, tmp_path):
        """`cd /elsewhere # Generated with Claude` reads as attribution and is a
        cd. Dropping the line sends the commit to another repository, and both
        the shlex parse and the git parse still succeed."""
        item_id, sid = _mkrun("cd line commit")
        command = f'cd {tmp_path} # Generated with Claude\ngit commit -am "fix"'
        out = work_launch.gate_commit(sid, command, "/tmp")
        assert out["decision"] == "deny"
        assert json.loads(_events(item_id, "commit_gate")[0]["payload"])["verdict"] == "fail"

    def test_a_comment_ends_at_its_own_line(self, tmp_path):
        """shlex reads # as a comment introducer for its whole input, and
        newlines are folded into ';' before tokenizing, so one commented line
        took every command after it with it. A comment ends at its line: what
        follows on the next line is a command, and what follows on its own line
        is not."""
        assert work_launch.parse_commit('cd /x # note\ngit commit -am "fix"') == {"chdir": "/x"}
        assert work_launch.parse_commit('# git commit -am "fix"') is None
        assert work_launch.parse_commit('printf done # next; git commit -m fix') is None
        assert work_launch.parse_commit('git commit -m "a # b"') == {"chdir": ""}
        # A quote carries across lines, so a hash inside a message written over
        # several lines is text, not a comment that would truncate the quote.
        assert work_launch.parse_commit(
            'git commit -m "fix the thing\nsee issue #42 for why"') == {"chdir": ""}

    def test_an_apostrophe_in_a_comment_still_finds_the_message_file(self, tmp_path):
        """The apostrophe used to make the whole command untokenizable, and the
        message file then went unread while the commit itself was still found."""
        _, sid = _mkrun("apostrophe comment")
        message = tmp_path / "msg.txt"
        message.write_text("fix the gate\n\nClaude-Session: https://claude.ai/code/s01\n")
        out = work_launch.gate_commit(
            sid, "git commit -F msg.txt # don't add trailers", str(tmp_path))
        assert out["decision"] == "allow"
        assert "Claude-Session" not in message.read_text()

    def test_a_message_that_runs_commands_is_never_rewritten(self, tmp_path):
        """`-m "$(git add b; printf fix)"` is a message and a command
        substitution. Dropping a line inside it changes what gets committed."""
        item_id, sid = _mkrun("substituting message")
        command = ('git commit -m "$(\ngit add b # Generated with Claude\n'
                   'printf fix\n)"')
        out = work_launch.gate_commit(sid, command, str(tmp_path))
        assert out["decision"] == "deny"
        assert json.loads(_events(item_id, "commit_gate")[0]["payload"])["verdict"] == "fail"

    def test_the_bundled_short_flag_still_carries_a_message(self, tmp_path):
        """git takes its short flags bundled, so -am introduces the message
        exactly as -m does."""
        _, sid = _mkrun("bundled flag commit")
        out = work_launch.gate_commit(
            sid, 'git commit -am "fix the gate\nGenerated with Claude\n"', str(tmp_path))
        assert out["decision"] == "allow"
        assert "Generated with Claude" not in out["command"]
        assert "fix the gate" in out["command"]

    def test_an_unrelated_dash_f_argument_is_not_read_as_a_message_file(self, tmp_path):
        """-F is not only git's flag. `grep -F README.md` used to hand the gate
        README.md as a commit message, and correcting a message would then have
        rewritten the documentation."""
        item_id, sid = _mkrun("unrelated dash f")
        readme = tmp_path / "README.md"
        readme.write_text("docs\n\nGenerated with Claude\n\nmore docs\n")
        out = work_launch.gate_commit(
            sid, 'git commit -m "fix the gate" && grep -F README.md log', str(tmp_path))
        assert out["decision"] == "allow"
        assert "Generated with Claude" in readme.read_text()
        assert _events(item_id, "commit_gate") == []

    def test_a_clean_commit_is_untouched(self, tmp_path):
        item_id, sid = _mkrun("clean commit")
        out = work_launch.gate_commit(sid, 'git commit -m "fix the gate"', str(tmp_path))
        assert out["decision"] == "allow"
        assert "command" not in out
        assert _events(item_id, "commit_gate") == []

    def test_the_hook_hands_git_the_corrected_command(self, tmp_path):
        _, sid = _mkrun("hook stripped commit")
        command = self._heredoc(
            "fix the gate\n\nClaude-Session: https://claude.ai/code/session_01B")
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=json.dumps({"session_id": sid, "hook_event_name": "PreToolUse",
                              "tool_name": "Bash", "cwd": str(tmp_path),
                              "tool_input": {"command": command, "description": "commit"}}),
            capture_output=True, text=True, timeout=120,
            env={**os.environ, "FRSHTY_DB": str(db._DB_PATH)})
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)["hookSpecificOutput"]
        assert out["permissionDecision"] == "allow"
        assert "Claude-Session" not in out["updatedInput"]["command"]
        assert out["updatedInput"]["description"] == "commit"


class TestAutoArchive:
    def _reported_done(self, objective, age_hours=48):
        item_id, sid = _mkrun(objective)
        stamp = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_ack' WHERE id = ?", (item_id,))
            c.execute("INSERT INTO work_events(work_item_id, kind, payload, created_at) "
                      "VALUES (?, 'self_reported_done', '{}', ?)", (item_id, stamp))
        return item_id, sid

    def test_a_quiet_reported_task_files_itself(self):
        item_id, _ = self._reported_done("quiet task")
        assert item_id in work_store.auto_archive_quiet_items()
        item = db.query_one("SELECT state, archived_at FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "done" and item["archived_at"]
        assert len(_events(item_id, "auto_archived")) == 1

    def test_a_fresh_report_waits_out_the_window(self):
        item_id, _ = self._reported_done("fresh task", age_hours=1)
        assert item_id not in work_store.auto_archive_quiet_items()

    def test_a_reopened_task_waits_on_its_newest_report(self):
        """A task completed two days ago, reopened, and completed again a
        minute ago has a result nobody has read."""
        item_id, _ = self._reported_done("reopened task")
        with db.tx() as c:
            c.execute("INSERT INTO work_events(work_item_id, kind, payload, created_at) "
                      "VALUES (?, 'self_reported_done', '{}', ?)",
                      (item_id, work_store._now()))
            c.execute("UPDATE work_items SET updated_at = ? WHERE id = ?",
                      (work_store._now(), item_id))
        assert item_id not in work_store.auto_archive_quiet_items()

    def test_a_task_that_asked_something_waits_for_a_real_read(self):
        item_id, sid = self._reported_done("asked something")
        with db.tx() as c:
            c.execute("INSERT INTO work_events(work_item_id, kind, payload, created_at) "
                      "VALUES (?, 'question_asked', '{}', ?)", (item_id, work_store._now()))
        assert item_id not in work_store.auto_archive_quiet_items()

    def test_a_task_a_gate_denied_waits_for_a_real_read(self):
        item_id, sid = self._reported_done("gate denied")
        work_store.record_gate(sid, "push_gate", "fail", {"repo": "x"})
        assert item_id not in work_store.auto_archive_quiet_items()

    def test_a_gate_that_passed_does_not_hold_the_task(self):
        item_id, sid = self._reported_done("gate passed")
        work_store.record_gate(sid, "push_gate", "pass", {"repo": "x"})
        assert item_id in work_store.auto_archive_quiet_items()


class TestRepeatProposals:
    def test_a_reworded_declined_proposal_is_dropped(self):
        item_id = work_store.create_proposal(
            "Add an \"exclude Saturday and Sunday\" option to audio deletion hours")
        work_store.apply_action(item_id, "decline")
        assert slack_conversations._repeats_a_declined_proposal(
            item_id,
            "Add an \"Exclude Saturday and Sunday\" option under audio deletion hours") is True

    def test_two_requests_that_differ_only_in_a_number_are_both_opened(self):
        item_id = work_store.create_proposal("Delete audio older than 7 days")
        work_store.apply_action(item_id, "decline")
        assert slack_conversations._repeats_a_declined_proposal(
            item_id, "Delete audio older than 9 days") is False

    def test_a_different_request_still_opens_a_task(self):
        item_id = work_store.create_proposal("Investigate how the Copilot Probe was enrolled")
        work_store.apply_action(item_id, "decline")
        assert slack_conversations._repeats_a_declined_proposal(
            item_id, "Raise the audio retention window to ninety days") is False

    def test_an_approved_proposal_does_not_block_a_repeat(self):
        item_id = work_store.create_proposal("Do the thing")
        assert work_store.claim_proposal(item_id) is True
        assert slack_conversations._repeats_a_declined_proposal(item_id, "Do the thing") is False


class TestRequiredFollowups:
    def _finished_with_followup(self, objective, required, kind="work_item"):
        item_id, _ = _mkrun(objective)
        now = work_store._now()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_ack' WHERE id = ?", (item_id,))
            c.execute(
                "INSERT INTO work_followups(work_item_id, kind, workspace, recipient, "
                "draft, required, created_at, updated_at) VALUES (?, ?, '', '', ?, ?, ?, ?)",
                (item_id, kind, "push the branch", 1 if required else 0, now, now))
        return item_id

    def test_a_required_work_item_followup_runs_by_itself(self, monkeypatch):
        item_id = self._finished_with_followup("required delivery", True)
        monkeypatch.setattr(work_debrief, "_deliver_work_item",
                            lambda row, contexts, slack, agent: "launched work item #999")
        sent = work_debrief.dispatch_required_followups()
        assert [s["item_id"] for s in sent] == [item_id]
        assert len(_events(item_id, "followup_auto_sent")) == 1

    def test_an_automatic_followup_inherits_the_source_task_context(self, monkeypatch):
        """launch_followup reads an omitted project, archive or agent as
        "inherit". Naming them launches a codex task's follow-up as claude in
        the default workspace."""
        self._finished_with_followup("inherit context", True)
        seen = {}
        monkeypatch.setattr(
            work_debrief, "_deliver_work_item",
            lambda row, contexts, slack, agent: seen.update(
                contexts=contexts, slack=slack, agent=agent) or "launched work item #999")
        work_debrief.dispatch_required_followups()
        assert seen == {"contexts": None, "slack": None, "agent": ""}

    def test_an_optional_followup_waits_for_the_operator(self, monkeypatch):
        item_id = self._finished_with_followup("optional expansion", False)
        monkeypatch.setattr(work_debrief, "_deliver_work_item",
                            lambda row, contexts, slack, agent: "launched work item #999")
        assert [s["item_id"] for s in work_debrief.dispatch_required_followups()] != [item_id]

    def test_a_slack_followup_is_never_sent_by_itself(self, monkeypatch):
        item_id = self._finished_with_followup("slack draft", True, kind="slack_message")
        monkeypatch.setattr(work_debrief, "_deliver_slack", lambda row: "sent")
        assert [s["item_id"] for s in work_debrief.dispatch_required_followups()] != [item_id]

    def test_the_automatic_chain_stops_at_its_depth(self, monkeypatch):
        first = self._finished_with_followup("chain root", True)
        monkeypatch.setattr(work_debrief, "_deliver_work_item",
                            lambda row, contexts, slack, agent: "launched work item #999")
        work_debrief.dispatch_required_followups()
        chain = [first]
        for step in range(work_debrief.AUTO_FOLLOWUP_DEPTH):
            child = self._finished_with_followup(f"chain step {step}", True)
            with db.tx() as c:
                c.execute("UPDATE work_items SET source_item_id = ? WHERE id = ?",
                          (chain[-1], child))
            chain.append(child)
            work_debrief.dispatch_required_followups()
        assert _events(chain[-1], "followup_auto_sent") == []

    def test_the_parser_only_marks_a_work_item_required(self):
        parsed = work_debrief._parse_debrief(json.dumps({"summary": "s", "followups": [
            {"kind": "work_item", "required": True, "draft": "push it"},
            {"kind": "slack_message", "required": True, "recipient": "Sam", "draft": "ping"},
        ]}))
        assert parsed["followups"][0]["required"] is True
        assert parsed["followups"][1]["required"] is False
