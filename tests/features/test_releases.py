"""Unit tests for features.releases data access + trigger."""
import json
from pathlib import Path

import pytest

import core.state as state
import core.queue as q
import features.releases as releases


@pytest.fixture()
def _INST(tmp_state):
    return state.active_instance_key()


def _ticket(key, status, slug=None, **extra):
    base = {"status": status, "slug": slug or f"{key}-slug"}
    if status == "merged":
        base.setdefault("merged_external_status", "Done")
    base.update(extra)
    return base


class TestKeyValidation:
    def test_accepts_standard_keys(self):
        assert releases.is_valid_release_key("v1.4")
        assert releases.is_valid_release_key("auth-rework")
        assert releases.is_valid_release_key("R_001")

    def test_rejects_path_traversal(self):
        assert not releases.is_valid_release_key("has/slash")
        assert not releases.is_valid_release_key("../escape")
        assert not releases.is_valid_release_key("with space")
        assert not releases.is_valid_release_key("")

    def test_rejects_too_long(self):
        assert not releases.is_valid_release_key("a" * 65)


class TestUpsertRelease:
    def test_creates_when_missing(self, _INST):
        rel = releases.upsert_release(_INST, "v1.0", title="First")
        assert rel["release_key"] == "v1.0"
        assert rel["status"] == "open"
        assert rel["title"] == "First"

    def test_idempotent_when_exists(self, _INST):
        a = releases.upsert_release(_INST, "v1.0")
        b = releases.upsert_release(_INST, "v1.0")
        assert a["id"] == b["id"]

    def test_updates_title_on_existing(self, _INST):
        releases.upsert_release(_INST, "v1.0", title="Old")
        rel = releases.upsert_release(_INST, "v1.0", title="New")
        assert rel["title"] == "New"

    def test_rejects_invalid_key(self, _INST):
        with pytest.raises(ValueError):
            releases.upsert_release(_INST, "bad/key")


class TestAssignTicket:
    def test_assign_creates_release_and_links(self, _INST):
        state.save_ticket("T-1", _ticket("T-1", "new"))
        releases.assign_ticket(_INST, "T-1", "v1.0")
        assert releases.ticket_release_key(_INST, "T-1") == "v1.0"
        assert releases.get_release_by_key(_INST, "v1.0") is not None

    def test_unassign_clears_link(self, _INST):
        state.save_ticket("T-1", _ticket("T-1", "new"))
        releases.assign_ticket(_INST, "T-1", "v1.0")
        releases.assign_ticket(_INST, "T-1", None)
        assert releases.ticket_release_key(_INST, "T-1") is None

    def test_reassign_to_different_release(self, _INST):
        state.save_ticket("T-1", _ticket("T-1", "new"))
        releases.assign_ticket(_INST, "T-1", "v1.0")
        releases.assign_ticket(_INST, "T-1", "v1.1")
        assert releases.ticket_release_key(_INST, "T-1") == "v1.1"

    def test_assign_nonexistent_ticket_returns_none(self, _INST):
        result = releases.assign_ticket(_INST, "NOPE", "v1.0")
        assert result is None

    def test_assign_invalid_key_raises(self, _INST):
        state.save_ticket("T-1", _ticket("T-1", "new"))
        with pytest.raises(ValueError):
            releases.assign_ticket(_INST, "T-1", "bad/key")


class TestAllTerminal:
    def test_empty_release_is_not_terminal(self, _INST):
        rel = releases.upsert_release(_INST, "v1.0")
        assert releases.all_terminal(_INST, rel["id"]) is False, \
            "empty release must not be considered terminal"

    def test_one_non_terminal_blocks(self, _INST):
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        state.save_ticket("T-2", _ticket("T-2", "planning", release_key="v1.0"))
        rel = releases.upsert_release(_INST, "v1.0")
        assert releases.all_terminal(_INST, rel["id"]) is False

    def test_all_terminal_returns_true(self, _INST):
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        state.save_ticket("T-2", _ticket("T-2", "done", release_key="v1.0"))
        state.save_ticket("T-3", _ticket("T-3", "validation", release_key="v1.0"))
        rel = releases.upsert_release(_INST, "v1.0")
        assert releases.all_terminal(_INST, rel["id"]) is True

    def test_only_some_in_release_count(self, _INST):
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        state.save_ticket("T-2", _ticket("T-2", "planning"))  # different release
        rel = releases.upsert_release(_INST, "v1.0")
        assert releases.all_terminal(_INST, rel["id"]) is True


class TestSummaries:
    def test_counts_terminal_and_total(self, _INST):
        rel = releases.upsert_release(_INST, "v1.0")
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        state.save_ticket("T-2", _ticket("T-2", "planning", release_key="v1.0"))
        s = releases.list_summaries(_INST)
        assert len(s) == 1
        assert s[0]["ticket_count"] == 2
        assert s[0]["terminal_count"] == 1
        assert s[0]["latest_verdict"] is None


class TestComputeHash:
    def test_stable_under_reorder(self, _INST):
        a = [{"ticket_key": "T-1"}, {"ticket_key": "T-2"}]
        b = [{"ticket_key": "T-2"}, {"ticket_key": "T-1"}]
        assert releases.compute_ticket_set_hash(a, None) == \
               releases.compute_ticket_set_hash(b, None), \
               "hash must be stable under reorder (must sort first)"

    def test_changes_with_release_md(self, _INST):
        ts = [{"ticket_key": "T-1"}]
        h_no_md = releases.compute_ticket_set_hash(ts, None)
        h_with_md = releases.compute_ticket_set_hash(ts, "abc123")
        assert h_no_md != h_with_md


class TestReleaseMd:
    def test_returns_none_when_missing(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        text, h = releases.read_release_md(cfg, "v1.0")
        assert text is None and h is None

    def test_reads_when_present(self, _INST, tmp_path):
        rel_dir = tmp_path / "releases" / "v1.0"
        rel_dir.mkdir(parents=True)
        (rel_dir / "release.md").write_text("# focus\n")
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        text, h = releases.read_release_md(cfg, "v1.0")
        assert text == "# focus\n"
        assert isinstance(h, str) and len(h) == 64


class TestMaybeTriggerInspect:
    def test_skipped_when_runtime_uninitialized(self, _INST):
        # No runtime → no-op, no exception
        releases.maybe_trigger_inspect(_INST, "T-1", "merged", "in_review")

    def test_no_op_when_status_not_terminal(self, _INST, monkeypatch):
        called = {"n": 0}
        monkeypatch.setattr(q, "enqueue_job", lambda *a, **k: called.__setitem__("n", called["n"] + 1))
        releases.maybe_trigger_inspect(_INST, "T-1", "planning", "new")
        assert called["n"] == 0

    def test_enqueues_when_all_terminal_and_enabled(self, _INST, monkeypatch):
        # Set up release with terminal tickets
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        rel = releases.upsert_release(_INST, "v1.0")

        # Stub runtime + queue
        class _Reg:
            config = {"features": {"releases": True}}

        class _Inst:
            def keys(self): return [_INST]
            def get(self, k): return _Reg()

        import core.runtime as rt
        monkeypatch.setattr(rt, "instances", lambda: _Inst())
        captured = []
        monkeypatch.setattr(q, "enqueue_job",
                            lambda instance, task, payload=None, **kw:
                            captured.append((instance, task, payload)))

        releases.maybe_trigger_inspect(_INST, "T-1", "merged", "in_review")
        assert len(captured) == 1, f"expected one enqueue, got {captured}"
        assert captured[0][1] == "release_inspect"
        assert captured[0][2]["release_id"] == rel["id"]

    def test_no_enqueue_when_feature_off(self, _INST, monkeypatch):
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        releases.upsert_release(_INST, "v1.0")

        class _Reg:
            config = {"features": {"releases": False}}

        class _Inst:
            def keys(self): return [_INST]
            def get(self, k): return _Reg()

        import core.runtime as rt
        monkeypatch.setattr(rt, "instances", lambda: _Inst())
        captured = []
        monkeypatch.setattr(q, "enqueue_job",
                            lambda *a, **k: captured.append((a, k)))

        releases.maybe_trigger_inspect(_INST, "T-1", "merged", "in_review")
        assert captured == [], "feature flag off must prevent enqueue"
