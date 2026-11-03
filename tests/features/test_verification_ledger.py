"""The pipeline writes the verification ledger and never reads it.

Each claim is established where the pipeline already establishes it, and
staled where the pipeline already pushes a new head or moves the base. The
ledger is shadow: these tests hold it to recording what happened, and to
leaving every decision exactly where it was.
"""
from unittest.mock import MagicMock, patch

import core.comments as comments
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
            "- `spa-dashboard`: paced-stream catch-up fix\n\nSCOPE VERDICT: FAIL\n")
        state.save_ticket(KEY, {
            "status": "in_review", "slug": SLUG, "branch": SLUG,
            "scope_review": {"fingerprint": "r:abc", "verdict": "fail",
                             "reason": "votes agy=FAIL, codex=FAIL"}})

    def _run(self, tmp_path, inst):
        seq = iter(["r:abc", "r:corrected", "r:corrected", "r:corrected"])
        heads = iter([{"spa-dashboard": "aaa"}, {"spa-dashboard": "bbb"}])
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=lambda *_a: next(seq)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["spa-dashboard"]), \
             patch("core.tasks.tickets._push_to_open_prs",
                   return_value=(["spa-dashboard"], [])), \
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
             patch("features.tickets._build_pr_info_map", return_value=info), \
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
        comments.mark_comment_seen(inst, "pr", "repo/1", "42", "2026-01-01T00:00:00Z")
        self._handle(fake_config, self._green(), inst, self._green(ci_passed=True))
        assert (_claim(inst, "comments")["established_against"]
                == freshness.watermark(self._green(), inst))

    def test_a_moved_watermark_re_establishes_the_comment_claim(self, fake_config):
        inst = _inst()
        seen = []
        for cid in ("42", "77"):
            comments.mark_comment_seen(inst, "pr", "repo/1", cid, "2026-01-01T00:00:00Z")
            self._handle(fake_config, self._green(), inst, self._green(ci_passed=True))
            seen.append(_claim(inst, "comments")["established_against"])
        assert seen[0] != seen[1]
        assert seen[1] == freshness.watermark(self._green(), inst)

    def test_the_lever_off_writes_no_row(self, fake_config):
        inst = _inst()
        fake_config["freshness"] = {"enabled": False}
        self._handle(fake_config, self._green(), inst, self._green(ci_passed=True))
        assert _rows(inst) == []


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
