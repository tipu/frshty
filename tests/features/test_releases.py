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


class TestBuildInspectionPayload:
    """Direct coverage for the LLM prompt builder. Regressions here silently
    change the verdict the LLM produces, so we assert structure + truncation."""

    def _release_dict(self, release_key="v1.0", title=None):
        return {"release_key": release_key, "title": title}

    def test_includes_release_key_and_optional_title(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0", title="Auth"),
            [{"ticket_key": "T-1", "status": "merged",
              "summary": "Add login", "description": "", "slug": "T-1"}],
            None,
        )
        assert "RELEASE: v1.0" in out
        assert "TITLE: Auth" in out
        assert "## T-1 [merged] — Add login" in out

    def test_omits_title_when_absent(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [{"ticket_key": "T-1", "status": "done", "summary": "x",
              "description": "", "slug": "T-1"}],
            None,
        )
        assert "TITLE:" not in out

    def test_includes_release_md_when_provided(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [{"ticket_key": "T-1", "status": "merged", "summary": "x",
              "description": "", "slug": "T-1"}],
            "FOCUS: ship logout flow safely",
        )
        assert "RELEASE.MD (focus):" in out
        assert "FOCUS: ship logout flow safely" in out

    def test_truncates_release_md_at_6000_chars(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        long_md = "X" * 7000
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"), [], long_md,
        )
        assert "... [truncated]" in out
        assert out.count("X") == 6000

    def test_truncates_description_at_1500_chars(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        long_desc = "Y" * 2000
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [{"ticket_key": "T-1", "status": "merged", "summary": "x",
              "description": long_desc, "slug": "T-1"}],
            None,
        )
        assert out.count("Y") == 1500
        assert "... [truncated]" in out

    def test_reads_change_manifest_from_disk(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        slug = "T-1-add-login"
        docs = tmp_path / "tickets" / slug / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("## What changed\nLogin form")
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [{"ticket_key": "T-1", "status": "merged", "summary": "x",
              "description": "", "slug": slug}],
            None,
        )
        assert "change-manifest.md:" in out
        assert "Login form" in out

    def test_truncates_change_manifest_at_4000_chars(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        slug = "T-1"
        docs = tmp_path / "tickets" / slug / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("Z" * 5000)
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [{"ticket_key": "T-1", "status": "merged", "summary": "x",
              "description": "", "slug": slug}],
            None,
        )
        assert out.count("Z") == 4000
        assert "... [truncated]" in out

    def test_skips_manifest_when_file_missing(self, _INST, tmp_path):
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [{"ticket_key": "T-1", "status": "merged", "summary": "x",
              "description": "no manifest here", "slug": "T-1-nope"}],
            None,
        )
        assert "no manifest here" in out
        assert "change-manifest.md:" not in out

    def test_handles_str_workspace_root(self, _INST, tmp_path):
        """workspace.root can be a string; must normalize to Path before joining."""
        slug = "T-1"
        docs = tmp_path / "tickets" / slug / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("from str root")
        cfg = {"workspace": {"root": str(tmp_path), "tickets_dir": "tickets"}}
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [{"ticket_key": "T-1", "status": "merged", "summary": "x",
              "description": "", "slug": slug}],
            None,
        )
        assert "from str root" in out

    def test_orders_tickets_as_provided(self, _INST, tmp_path):
        """Runner passes sorted list; payload must preserve order (no incidental
        re-sort) so the prompt is stable across runs."""
        cfg = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        out = releases.build_inspection_payload(
            cfg, self._release_dict("v1.0"),
            [
                {"ticket_key": "T-A", "status": "merged", "summary": "a",
                 "description": "", "slug": "T-A"},
                {"ticket_key": "T-B", "status": "done", "summary": "b",
                 "description": "", "slug": "T-B"},
            ],
            None,
        )
        assert out.index("T-A") < out.index("T-B")


class TestMaybeTriggerInspect:
    def test_skipped_when_runtime_uninitialized(self, _INST, monkeypatch):
        """Tighter than 'didn't raise': must not call enqueue_job either."""
        captured: list = []
        monkeypatch.setattr(q, "enqueue_job",
                            lambda *a, **k: captured.append((a, k)))
        releases.maybe_trigger_inspect(_INST, "T-1", "merged", "in_review")
        assert captured == [], "no runtime → no enqueue"

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

    def test_state_outer_guard_logs_when_trigger_raises(self, _INST, monkeypatch):
        """core.state._maybe_fire_release_trigger swallows trigger exceptions
        and logs release_trigger_error. Verify the log fires so a broken
        trigger doesn't die silently in production."""
        # Make the trigger itself raise (e.g. an import error or surprise bug)
        import features.releases as _releases_mod
        monkeypatch.setattr(_releases_mod, "maybe_trigger_inspect",
                            lambda *a, **kw: (_ for _ in ()).throw(
                                RuntimeError("trigger broke")))

        emitted: list = []
        import core.log as _log
        monkeypatch.setattr(_log, "emit",
                            lambda event, *a, **kw: emitted.append(event))

        # Drive a status transition through update_ticket — must NOT raise
        state.save_ticket("T-1", _ticket("T-1", "in_review"))
        state.update_ticket("T-1", lambda cur: {**cur, "status": "merged",
                                                  "merged_external_status": "Done"})
        assert "release_trigger_error" in emitted, \
            f"expected release_trigger_error in emitted={emitted}"

    def test_enqueue_failure_emits_log_event(self, _INST, monkeypatch):
        """If q.enqueue_job raises, the trigger must catch it AND emit
        release_inspect_enqueue_error so the failure is visible in the feed."""
        state.save_ticket("T-1", _ticket("T-1", "merged", release_key="v1.0"))
        releases.upsert_release(_INST, "v1.0")

        class _Reg:
            config = {"features": {"releases": True}}

        class _Inst:
            def keys(self): return [_INST]
            def get(self, k): return _Reg()

        import core.runtime as rt
        monkeypatch.setattr(rt, "instances", lambda: _Inst())
        monkeypatch.setattr(q, "enqueue_job",
                            lambda *a, **k: (_ for _ in ()).throw(
                                RuntimeError("queue offline")))

        emitted: list = []
        import core.log as _log
        monkeypatch.setattr(_log, "emit",
                            lambda event, *a, **kw: emitted.append(event))

        # Must not raise — the trigger swallows + logs
        releases.maybe_trigger_inspect(_INST, "T-1", "merged", "in_review")
        assert "release_inspect_enqueue_error" in emitted, \
            f"expected release_inspect_enqueue_error in emitted={emitted}"
