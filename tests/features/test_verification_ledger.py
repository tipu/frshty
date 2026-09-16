"""The pipeline writes the verification ledger and never reads it.

Each claim is established where the pipeline already establishes it, and
staled where the pipeline already pushes a new head or moves the base. The
ledger is shadow: these tests hold it to recording what happened, and to
leaving every decision exactly where it was.
"""
import subprocess
from unittest.mock import MagicMock, patch

import core.db as db
import core.freshness as freshness
import core.state as state
import core.tasks.tickets as T
import features.ticket_states as ticket_states
import features.tickets as tickets
from core.tasks.registry import TaskContext
from tests.conftest import make_ticket, make_ticket_state

KEY = "DEV-738"
SLUG = "DEV-738-paced-stream"

_counter = 0


def _inst():
    global _counter
    _counter += 1
    return f"ledger-wiring-{_counter}"


def _claim(instance_key, claim):
    return db.query_one(
        "SELECT * FROM verification_claim"
        " WHERE instance_key=? AND ticket_key=? AND claim=?",
        (instance_key, KEY, claim))


def _rows(instance_key):
    return db.query_all(
        "SELECT * FROM verification_claim WHERE instance_key=?", (instance_key,))


def _ctx(tmp_path, instance_key, task="fix_scope_findings", **feature_cfg):
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "features": {"scope_review": True},
        "_base_url": "http://localhost:8000",
    }
    config.update(feature_cfg)
    return TaskContext(
        instance_key=instance_key, ticket_key=KEY, task=task,
        payload={}, job_id=0, triggering_event_id=None, config=config,
        registry=None, now=None,
    )


class TestTheProofClaim:
    def test_the_proof_records_the_branch_diff_it_stood_on(self, tmp_path, tmp_state):
        inst = _inst()
        state.save_ticket(KEY, {"status": "proving", "slug": SLUG, "branch": SLUG})
        with patch("core.tasks.tickets.scope_fingerprint", return_value="r:abc"):
            T._record_proof_fingerprint(_ctx(tmp_path, inst))
        assert _claim(inst, "proof")["established_against"] == "r:abc"

    def test_a_re_proof_of_the_same_branch_does_not_restart_the_clock(
            self, tmp_path, tmp_state):
        inst = _inst()
        state.save_ticket(KEY, {"status": "proving", "slug": SLUG, "branch": SLUG})
        ctx = _ctx(tmp_path, inst)
        with patch("core.tasks.tickets.scope_fingerprint", return_value="r:abc"):
            T._record_proof_fingerprint(ctx)
            first = _claim(inst, "proof")["established_at"]
            T._record_proof_fingerprint(ctx)
        assert _claim(inst, "proof")["established_at"] == first

    def test_the_lever_off_writes_no_row_and_still_records_the_fingerprint(
            self, tmp_path, tmp_state):
        inst = _inst()
        state.save_ticket(KEY, {"status": "proving", "slug": SLUG, "branch": SLUG})
        ctx = _ctx(tmp_path, inst, freshness={"enabled": False})
        with patch("core.tasks.tickets.scope_fingerprint", return_value="r:abc"):
            T._record_proof_fingerprint(ctx)
        assert _rows(inst) == []
        assert state.load_ticket(KEY)["proof_fingerprint"] == "r:abc"


class TestTheScopeCorrectionStalesTheProof:
    def _seed(self, tmp_path):
        docs = tmp_path / "tickets" / SLUG / "docs"
        docs.mkdir(parents=True, exist_ok=True)
        (tmp_path / "tickets" / SLUG / "workspace").mkdir(parents=True, exist_ok=True)
        (docs / "scope-review.md").write_text(
            "# Consensus scope review\n\nOffending changes:\n\n"
            "- `saas-dashboard`: paced-stream catch-up fix\n\nSCOPE VERDICT: FAIL\n")
        state.save_ticket(KEY, {
            "status": "in_review", "slug": SLUG, "branch": SLUG,
            "scope_review": {"fingerprint": "r:abc", "verdict": "fail",
                             "reason": "votes agy=FAIL, codex=FAIL"}})

    def _run(self, tmp_path, inst):
        seq = iter(["r:abc", "r:corrected", "r:corrected", "r:corrected"])
        heads = iter([{"saas-dashboard": "aaa"}, {"saas-dashboard": "bbb"}])
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=lambda *_a: next(seq)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets._push_to_open_prs",
                   return_value=(["saas-dashboard"], [])), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"):
            return T.fix_scope_findings(_ctx(tmp_path, inst))

    def test_a_correction_at_in_review_stales_the_proof_and_the_ci_verdict(
            self, tmp_path, tmp_state):
        inst = _inst()
        self._seed(tmp_path)
        freshness.record(KEY, "proof", "r:abc", instance_key=inst)
        freshness.record(KEY, "ci", "repo/1@sha", instance_key=inst)
        assert self._run(tmp_path, inst).status == "ok"
        assert _claim(inst, "proof")["invalidated_by"] == "ticket_scope_fix_committed"
        assert _claim(inst, "ci")["invalidated_by"] == "ticket_scope_fix_committed"


class TestTheInReviewPollWritesTheClaims:
    def _handle(self, fake_config, ts, inst, ci_result, checked=None):
        platform = MagicMock()
        platform.monitor_ci.return_value = ci_result
        info = {("repo", 1): {"head_sha": "sha-1"}}
        with patch("features.tickets._scope_review_state", return_value="disabled"), \
             patch("features.tickets._resolve_conflicts_pending", return_value=False), \
             patch("features.tickets._build_pr_info_map", return_value=info), \
             patch("features.tickets._has_conflicting_pr", return_value=False), \
             patch("features.tickets._pr_base_moved", return_value=False), \
             patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets._merge"), \
             patch("features.tickets._check_in_review",
                   side_effect=checked or (lambda c, t, s, b, **kw: s)), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.ticket_states.log"), \
             patch("features.tickets.log"), \
             patch("features.tickets.state"), \
             patch("features.ticket_states.state"):
            out, _ = ticket_states._handle_in_review_ticket(
                fake_config, make_ticket(key=KEY), ts, "http://base", inst, True)
        return out

    def _green(self, **extra):
        ts = make_ticket_state(status="in_review",
                               prs=[{"repo": "repo", "id": 1, "url": "u"}])
        ts.update(extra)
        return ts

    def test_a_green_poll_records_the_heads_ci_was_measured_against(self, fake_config):
        inst = _inst()
        self._handle(fake_config, self._green(), inst, self._green(ci_passed=True))
        assert _claim(inst, "ci")["established_against"] == "repo/1@sha-1"

    def test_a_poll_that_is_not_green_records_no_ci_claim(self, fake_config):
        inst = _inst()
        self._handle(fake_config, self._green(), inst, self._green())
        assert _claim(inst, "ci") is None

    def test_the_comment_claim_records_the_watermark_that_was_triaged(
            self, fake_config):
        inst = _inst()
        self._handle(fake_config, self._green(), inst, self._green(ci_passed=True),
                     checked=lambda c, t, s, b, **kw: {**s, "last_comment_ids": {"repo/1": 42}})
        assert '"repo/1": 42' in _claim(inst, "comments")["established_against"]

    def test_a_moved_watermark_re_establishes_the_comment_claim(self, fake_config):
        inst = _inst()
        for last in (42, 77):
            self._handle(
                fake_config, self._green(), inst, self._green(ci_passed=True),
                checked=lambda c, t, s, b, _l=last, **kw: {**s, "last_comment_ids": {"repo/1": _l}})
        assert '"repo/1": 77' in _claim(inst, "comments")["established_against"]

    def test_the_lever_off_writes_no_row(self, fake_config):
        inst = _inst()
        fake_config["freshness"] = {"enabled": False}
        self._handle(fake_config, self._green(), inst, self._green(ci_passed=True))
        assert _rows(inst) == []


class TestTheBaseSyncStalesTheProof:
    def _run(self, fake_config, inst, result):
        fake_config["pr"]["auto_update_branch"] = True
        ts = make_ticket_state(status="in_review", slug=SLUG, branch=SLUG,
                               prs=[{"repo": "repo", "id": 1, "branch": SLUG,
                                     "url": "u"}])
        with patch("features.tickets.make_platform", return_value=MagicMock()), \
             patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path",
                   return_value=MagicMock(is_dir=lambda: True)), \
             patch("features.tickets.branch_sync.sync_branch_with_base",
                   return_value={"result": result, "attempts": 0}), \
             patch("features.tickets.log"), \
             patch("core.freshness.state.active_instance_key", return_value=inst):
            return tickets._sync_pr_base(fake_config, make_ticket(key=KEY), ts,
                                         "http://base")

    def test_a_base_merge_stales_the_proof_and_the_ci_verdict(self, fake_config):
        inst = _inst()
        freshness.record(KEY, "proof", "r:abc", instance_key=inst)
        freshness.record(KEY, "ci", "repo/1@sha", instance_key=inst)
        self._run(fake_config, inst, "synced")
        assert _claim(inst, "proof")["invalidated_by"] == "ticket_base_synced"
        assert _claim(inst, "ci")["invalidated_by"] == "ticket_base_synced"

    def test_a_base_sync_that_did_not_merge_leaves_the_proof_standing(
            self, fake_config):
        inst = _inst()
        freshness.record(KEY, "proof", "r:abc", instance_key=inst)
        self._run(fake_config, inst, "dirty_worktree")
        assert _claim(inst, "proof")["invalidated_at"] is None


class TestTheCommentFixStalesTheProof:
    def _init_git_pair(self, tmp_path, branch):
        origin = tmp_path / "origin.git"
        wt = tmp_path / "wt"
        subprocess.run(["git", "init", "--bare", str(origin)], check=True,
                       capture_output=True)
        subprocess.run(["git", "clone", str(origin), str(wt)], check=True,
                       capture_output=True)
        for k, v in (("user.email", "t@example.com"), ("user.name", "t"),
                     ("commit.gpgsign", "false")):
            subprocess.run(["git", "config", k, v], cwd=str(wt), check=True,
                           capture_output=True)
        (wt / "app.py").write_text("original\n")
        subprocess.run(["git", "add", "-A"], cwd=str(wt), check=True,
                       capture_output=True)
        subprocess.run(["git", "commit", "-m", "init"], cwd=str(wt), check=True,
                       capture_output=True)
        subprocess.run(["git", "checkout", "-b", branch], cwd=str(wt), check=True,
                       capture_output=True)
        subprocess.run(["git", "push", "-u", "origin", branch], cwd=str(wt),
                       check=True, capture_output=True)
        return wt

    def _run(self, fake_config, tmp_path, inst, fix):
        slug = "PROJ-1-do-the-thing"
        wt = self._init_git_pair(tmp_path, slug)
        ts = make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "repo", "id": 99, "branch": slug, "url": "http://u"}])
        comment = {"id": 100, "body": "Remove the duplicate definition",
                   "author_id": "reviewer1", "author_name": "Reviewer",
                   "path": "app.py", "line": 1, "parent_id": None,
                   "created_on": "2026-08-20T23:00:00Z",
                   "created_at": "2026-08-20T23:00:00Z",
                   "updated_at": "2026-08-20T23:00:00Z"}
        platform = MagicMock()
        platform.get_pr_state.return_value = "OPEN"
        platform.get_pr_comments.return_value = [comment]
        platform.push_branch.return_value = {"ok": True}
        bb_config = {
            **fake_config,
            "job": {**fake_config["job"], "platform": "bitbucket"},
            "bitbucket": {"org": "x", "user_account_id": "bot-self"},
        }

        def claude(prompt, cwd=None, **kwargs):
            if fix:
                (wt / "app.py").write_text("fixed\n")
                subprocess.run(["git", "add", "-A"], cwd=str(wt), check=True,
                               capture_output=True)
                subprocess.run(["git", "commit", "-m", "remove duplicate"],
                               cwd=str(wt), check=True, capture_output=True)
            return "done"

        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "repo", "path": wt.parent}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.run_balanced",
                   return_value='{"results": [{"i": 0, "actionable": true}]}'), \
             patch("features.tickets.run_claude_code", side_effect=claude), \
             patch("core.freshness.state.active_instance_key", return_value=inst):
            tickets._check_in_review(bb_config, make_ticket(key=KEY), ts,
                                     "http://base")
        return platform

    def test_a_pushed_comment_fix_stales_the_proof_and_the_ci_verdict(
            self, fresh_db, fake_config, tmp_state, tmp_path):
        inst = _inst()
        freshness.record(KEY, "proof", "r:abc", instance_key=inst)
        freshness.record(KEY, "ci", "repo/99@sha", instance_key=inst)
        platform = self._run(fake_config, tmp_path, inst, fix=True)
        assert platform.push_branch.call_count == 1
        assert _claim(inst, "proof")["invalidated_by"] == "ticket_pr_comment_fixed"
        assert _claim(inst, "ci")["invalidated_by"] == "ticket_pr_comment_fixed"

    def test_a_comment_that_needed_no_commit_leaves_the_proof_standing(
            self, fresh_db, fake_config, tmp_state, tmp_path):
        inst = _inst()
        freshness.record(KEY, "proof", "r:abc", instance_key=inst)
        platform = self._run(fake_config, tmp_path, inst, fix=False)
        assert platform.push_branch.call_count == 0
        assert _claim(inst, "proof")["invalidated_at"] is None


class TestTheSmokeClaim:
    def test_the_criteria_that_ran_name_the_claim(self):
        subject = T._smoke_subject({
            "primary": [{"criterion_id": "c1", "result": "pass"}],
            "regression": [{"ticket_key": "DEV-1", "criterion_id": "c9",
                            "result": "pass"}]})
        assert subject.startswith("primary=1 regression=1 ")

    def test_a_different_pool_gives_a_different_claim(self):
        one = T._smoke_subject({"primary": [{"criterion_id": "c1"}],
                                "regression": []})
        two = T._smoke_subject({"primary": [{"criterion_id": "c2"}],
                                "regression": []})
        assert one != two

    def test_an_empty_run_still_names_its_counts(self):
        assert T._smoke_subject({}) == "primary=0 regression=0 "
