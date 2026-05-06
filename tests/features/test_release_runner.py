"""Tests for pm.release_runner.run_release_inspection.

run_haiku is monkeypatched directly. AttributeError fires explicitly if the
symbol moves, rather than silently skipping."""
import pytest

import core.state as state
import features.releases as releases
import pm.release_runner as runner


@pytest.fixture()
def _INST(tmp_state):
    return state.active_instance_key()


def _ticket(key, status, slug=None, **extra):
    base = {"status": status, "slug": slug or f"{key}-slug"}
    if status == "merged":
        base.setdefault("merged_external_status", "Done")
    base.update(extra)
    return base


def _seed_terminal_release(_INST, release_key="v1.0", n=2):
    for i in range(1, n + 1):
        state.save_ticket(f"T-{i}", _ticket(f"T-{i}", "merged",
                                             release_key=release_key,
                                             summary=f"Ticket {i}"))
    return releases.upsert_release(_INST, release_key)


class TestRunReleaseInspection:
    def test_persists_pass_verdict_and_emits_event(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        monkeypatch.setattr(runner, "run_haiku",
                            lambda *a, **k: '```json\n{"verdict":"pass","findings":[]}\n```')
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        review = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert review is not None, "expected review row to be inserted"
        assert review["verdict"] == "pass"
        assert review["findings"] == []
        # Release marked inspected
        rel2 = releases.get_release_by_id(_INST, rel["id"])
        assert rel2["status"] == "inspected"
        assert rel2["last_inspected_at"] is not None

    def test_persists_fail_verdict_with_findings(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        monkeypatch.setattr(
            runner, "run_haiku",
            lambda *a, **k: (
                '```json\n{"verdict":"fail","findings":'
                '[{"category":"cohesion","severity":"high",'
                '"message":"missing logout","related_ticket_keys":["T-1"]}]}\n```'
            ),
        )
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        review = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert review["verdict"] == "fail"
        assert len(review["findings"]) == 1
        assert review["findings"][0]["category"] == "cohesion"

    def test_skipped_when_release_not_terminal(self, _INST, monkeypatch, tmp_path):
        # Mix terminal + non-terminal
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        state.save_ticket("T-2", _ticket("T-2", "planning", release_key="v1.0"))
        rel = releases.upsert_release(_INST, "v1.0")
        called = {"n": 0}
        monkeypatch.setattr(runner, "run_haiku",
                            lambda *a, **k: called.__setitem__("n", called["n"] + 1) or "")
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        result = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert result is None
        assert called["n"] == 0, "must not call LLM when release is not terminal"

    def test_idempotent_on_unchanged_inputs(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        call_count = {"n": 0}

        def _haiku(*a, **k):
            call_count["n"] += 1
            return '```json\n{"verdict":"pass","findings":[]}\n```'

        monkeypatch.setattr(runner, "run_haiku", _haiku)
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}

        runner.run_release_inspection(_INST, rel["id"], cfg)
        assert call_count["n"] == 1

        # Re-run with same content → idempotent
        result2 = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert result2 is None, "second run with unchanged inputs must skip"
        assert call_count["n"] == 1, "LLM must NOT be called on idempotent re-run"

    def test_force_bypasses_idempotency(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        monkeypatch.setattr(runner, "run_haiku",
                            lambda *a, **k: '```json\n{"verdict":"pass","findings":[]}\n```')
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        runner.run_release_inspection(_INST, rel["id"], cfg)
        review2 = runner.run_release_inspection(_INST, rel["id"], cfg, force=True)
        assert review2 is not None, "force=True must bypass idempotency"

    def test_release_md_changes_break_idempotency(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        monkeypatch.setattr(runner, "run_haiku",
                            lambda *a, **k: '```json\n{"verdict":"pass","findings":[]}\n```')
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        runner.run_release_inspection(_INST, rel["id"], cfg)

        # Drop release.md after first run
        rel_dir = tmp_path / "releases" / "v1.0"
        rel_dir.mkdir(parents=True)
        (rel_dir / "release.md").write_text("focus: must include logout\n")

        review2 = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert review2 is not None, \
            "release.md content change must invalidate idempotency hash"
        assert review2["release_md_hash"] is not None

    def test_unparseable_haiku_skips_without_inserting(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        monkeypatch.setattr(runner, "run_haiku",
                            lambda *a, **k: "not json at all")
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        result = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert result is None
        assert releases.latest_review(rel["id"]) is None

    def test_invalid_verdict_skips(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        monkeypatch.setattr(runner, "run_haiku",
                            lambda *a, **k: '```json\n{"verdict":"maybe","findings":[]}\n```')
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        result = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert result is None
        assert releases.latest_review(rel["id"]) is None

    def test_empty_haiku_skips(self, _INST, monkeypatch, tmp_path):
        rel = _seed_terminal_release(_INST)
        monkeypatch.setattr(runner, "run_haiku", lambda *a, **k: "")
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        result = runner.run_release_inspection(_INST, rel["id"], cfg)
        assert result is None
