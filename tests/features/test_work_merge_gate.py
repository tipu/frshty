"""The work board asks a project whether a task may merge its own PR.

The board used to tell every agent on every project to merge. A task on the
atropos instance merged a company pull request while that instance's config
said auto_merge = false, because nothing on the work board read that switch.
These cover the switch, the delivery rule that states it, and the gate that
enforces it.
"""
import json

import core.db as db
from services import work_launch, work_store


def _mkrun(objective="merge gate item", contexts=""):
    item_id = work_store.create_item(objective, contexts=contexts)
    sid = f"sid-merge-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    return item_id, sid


def _gate_events(item_id):
    return [json.loads(r["payload"]) for r in db.query_all(
        "SELECT payload FROM work_events WHERE work_item_id = ? "
        "AND kind = 'merge_gate' ORDER BY id", (item_id,))]


def _projects(monkeypatch, **auto_merge):
    """Load the named projects, each with the given auto_merge setting."""
    monkeypatch.setattr(
        work_launch, "_instance_config",
        lambda key: {"pr": {"auto_merge": auto_merge[key]}} if key in auto_merge else None)


class TestParsePrMerge:
    def test_detects_a_pull_request_merge(self):
        for command in (
            "gh pr merge 184 --repo atroposhealth/text-to-tql-service --squash",
            "gh pr merge --admin --merge",
            "cd /x && gh pr merge 1",
            "/opt/homebrew/bin/gh pr merge 1 --rebase",
            "GH_TOKEN=x gh pr merge 1",
            "glab mr merge 7",
            "gh api -X PUT repos/o/r/pulls/184/merge",
            "gh api --method PUT repos/o/r/pulls/184/merge",
            "curl -X POST https://api.bitbucket.org/2.0/repositories/o/r/pullrequests/9/merge",
            "curl -XPUT https://api.github.com/repos/o/r/pulls/184/merge",
            "curl -s -X POST http://localhost:7100/api/tickets/LSC-78/merge",
            'curl -X PUT "https://api.github.com/repos/o/r/pulls/$PR/merge"',
            "git status; gh pr merge 1",
            "gh pr --repo atroposhealth/text-to-tql-service merge 184",
            "env gh pr merge 184",
            "env -u GH_TOKEN gh pr merge 184",
            "sudo -u root gh pr merge 184",
            "sudo --user root gh pr merge 184",
            "env -S gh pr merge 184",
            "stdbuf -o L gh pr merge 184",
            "stdbuf --output L gh pr merge 184",
            "timeout 60 gh pr merge 184",
            "timeout -k 5 60 gh pr merge 184",
            "timeout --signal KILL 60 gh pr merge 184",
            "gh api -X GET -X PUT repos/o/r/pulls/184/merge",
            "http --auth user:pass PUT https://api.github.com/repos/o/r/pulls/184/merge",
            "curl -T body.json https://api.github.com/repos/o/r/pulls/184/merge",
            "curl -Tbody.json https://api.github.com/repos/o/r/pulls/184/merge",
            "curl -d'{}' https://api.bitbucket.org/2.0/repositories/o/r/pullrequests/9/merge",
            "sudo --chdir /repo gh pr merge 184",
            "env --file .env gh pr merge 184",
            "bash -c 'gh pr merge 184'",
            "env -S 'gh pr merge 184'",
            'sh -c "cd /x && gh pr merge 184"',
            "timeout 60 bash -c 'gh pr merge 184'",
            "if true; then gh pr merge 184; fi",
            "http PUT https://api.github.com/repos/o/r/pulls/184/merge",
            "xh post https://api.bitbucket.org/2.0/repositories/o/r/pullrequests/9/merge",
            "wget --post-data='' https://api.bitbucket.org/2.0/repositories/o/r/pullrequests/9/merge",
        ):
            assert work_launch.parse_pr_merge(command) is True, command

    def test_ignores_everything_that_is_not_one(self):
        for command in (
            "git merge origin/main --no-edit",
            "git merge --abort",
            "git -C /x merge development",
            "gh pr view 1 --json mergeable",
            "gh pr create --title 'merge the agents'",
            "gh pr comment 1 --body 'ready to merge'",
            'echo "gh pr merge 1"',
            'grep -rn "/pulls/1/merge" web/',
            "gh search code pr merge --limit 1",
            "gh api repos/o/r/pulls/184/merge",
            "gh api -X GET -f q=x repos/o/r/pulls/184/merge",
            "gh api -X PUT -X GET repos/o/r/pulls/184/merge",
            "curl -s https://api.github.com/repos/o/r/pulls/184/merge",
            "http https://api.github.com/repos/o/r/pulls/184/merge",
            "curl -f https://api.github.com/repos/o/r/pulls/184/merge",
            "bash -c 'git status'",
            "echo bash -c",
            "ls -la",
        ):
            assert work_launch.parse_pr_merge(command) is False, command

    def test_a_command_it_cannot_tokenize_still_matches(self):
        assert work_launch.parse_pr_merge("gh pr merge 1 # don't") is True
        assert work_launch.parse_pr_merge("echo don't merge") is False


class TestSegmentProgram:
    def test_a_wrapper_option_value_does_not_hide_the_command(self):
        for command in ("sudo -u root git push", "env --unset GH_TOKEN git push",
                        "stdbuf -o L git push", "timeout --signal KILL 60 git push",
                        "env -S git push", "nohup git push"):
            assert work_launch.parse_push(command) == {"chdir": ""}, command

    def test_a_wrapped_commit_still_reaches_the_commit_parser(self):
        for command in ("env git commit -m fix", "sudo --user root git commit -m fix",
                        "if true; then git commit -m fix; fi"):
            assert work_launch.parse_commit(command) == {"chdir": ""}, command

    def test_a_command_string_handed_to_a_shell_is_parsed_too(self):
        assert work_launch.parse_push("bash -c 'git push'") == {"chdir": ""}
        assert work_launch.parse_push("env -S 'git push'") == {"chdir": ""}
        assert work_launch.parse_push('sh -c "cd /x && git push"') == {"chdir": "/x"}
        assert work_launch.parse_commit("bash -c 'git commit -m fix'") == {"chdir": ""}
        assert work_launch.parse_push("bash -c 'git status'") is None

    def test_an_unwrapped_segment_keeps_its_first_token_as_the_program(self):
        assert work_launch.parse_push("ls -la push") is None
        assert work_launch.parse_push("git -C /x push") == {"chdir": "/x"}


class TestMergePolicy:
    def test_a_project_that_allows_a_merge_holds_nothing(self, monkeypatch):
        _projects(monkeypatch, bh=True)
        assert work_launch.merge_review_required(["bh"]) == []

    def test_a_project_that_forbids_a_merge_holds_it(self, monkeypatch):
        _projects(monkeypatch, aimyable=False)
        assert work_launch.merge_review_required(["aimyable"]) == ["aimyable"]

    def test_a_project_with_no_config_holds_it(self, monkeypatch):
        _projects(monkeypatch)
        assert work_launch.merge_review_required(["frshty"]) == ["frshty"]

    def test_one_holding_project_holds_the_whole_task(self, monkeypatch):
        _projects(monkeypatch, bh=True, clarivis=False)
        assert work_launch.merge_review_required(["bh", "clarivis"]) == ["clarivis"]

    def test_the_slack_label_is_not_a_project(self, monkeypatch):
        _projects(monkeypatch, bh=True)
        assert work_launch.merge_review_required(f"bh,{work_launch.SLACK_LABEL}") == []

    def test_a_task_with_no_project_falls_back_to_the_board(self, monkeypatch):
        _projects(monkeypatch)
        assert work_launch.merge_review_required("") == [work_launch.BOARD_PROJECT_KEY]
        _projects(monkeypatch, **{work_launch.BOARD_PROJECT_KEY: True})
        assert work_launch.merge_review_required("") == []

    def test_a_registry_that_is_not_loaded_reads_the_config_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(work_launch, "_instance_config", lambda key: None)
        monkeypatch.setattr(work_launch, "_CONFIG_DIR", str(tmp_path))
        # A file is found by the key it declares, not by its name: the atropos
        # instance is keyed "frshty" and lives in config/local.toml.
        (tmp_path / "local.toml").write_text('[job]\nkey = "opener"\n[pr]\nauto_merge = true\n')
        (tmp_path / "shut.toml").write_text('[job]\nkey = "shut"\n[pr]\nauto_merge = false\n')
        (tmp_path / "broken.toml").write_text("[pr\n")
        (tmp_path / "notatable.toml").write_text('[job]\nkey = "notatable"\npr = true\n')
        # example.toml is one of the files the instance loader skips, so the
        # board holds no config for the key it declares.
        (tmp_path / "example.toml").write_text(
            '[job]\nkey = "myproject"\n[pr]\nauto_merge = true\n')
        assert work_launch._auto_merge_on_disk("opener") is True
        assert work_launch._auto_merge_on_disk("local") is False
        assert work_launch._auto_merge_on_disk("shut") is False
        assert work_launch._auto_merge_on_disk("notatable") is False
        assert work_launch._auto_merge_on_disk("missing") is False
        assert work_launch._auto_merge_on_disk("myproject") is False
        assert work_launch.merge_review_required(["opener"]) == []

    def test_a_config_directory_it_cannot_read_holds_every_merge(self, monkeypatch, tmp_path):
        monkeypatch.setattr(work_launch, "_instance_config", lambda key: None)
        monkeypatch.setattr(work_launch, "_CONFIG_DIR", str(tmp_path / "gone"))
        assert work_launch.merge_review_required(["anything"]) == ["anything"]

    def test_a_string_that_is_not_true_is_not_permission(self, monkeypatch):
        monkeypatch.setattr(work_launch, "_instance_config",
                            lambda key: {"pr": {"auto_merge": "true"}})
        assert work_launch.merge_review_required(["x"]) == ["x"]


class TestDeliveryRule:
    def test_a_project_that_allows_a_merge_keeps_the_merge_step(self):
        rule = work_store.delivery_rule([])
        assert "merge it when it is mergeable" in rule
        assert "Delivery is part of the objective" in rule

    def test_a_holding_project_removes_the_merge_step_and_is_named(self):
        rule = work_store.delivery_rule(["frshty", "clarivis"])
        assert "merge it when it is mergeable" not in rule
        assert "Stop at the pull request" in rule
        assert "frshty, clarivis" in rule
        assert "Delivery is part of the objective" in rule

    def test_the_continue_prompt_carries_the_rule_of_the_task(self, monkeypatch):
        _projects(monkeypatch, bh=True, clarivis=False)
        assert "merge it when it is mergeable" in work_store.continue_prompt("bh")
        held = work_store.continue_prompt("clarivis")
        assert "merge it when it is mergeable" not in held
        assert "Stop at the pull request" in held


class TestMergeGate:
    def test_a_holding_project_denies_the_merge(self, monkeypatch):
        _projects(monkeypatch, aimyable=False)
        item_id, sid = _mkrun("fix the filter endpoint", contexts="aimyable")
        out = work_launch.gate_merge(sid, "gh pr merge 60 --squash")
        assert out["decision"] == "deny"
        assert "aimyable" in out["reason"]
        assert _gate_events(item_id)[0]["verdict"] == "fail"

    def test_an_allowing_project_permits_the_merge(self, monkeypatch):
        _projects(monkeypatch, bh=True)
        item_id, sid = _mkrun("add the job runner", contexts="bh")
        out = work_launch.gate_merge(sid, "gh pr merge 3 --squash")
        assert out["decision"] == "allow"
        assert _gate_events(item_id)[0]["verdict"] == "pass"

    def test_an_objective_that_asks_for_the_merge_permits_it(self, monkeypatch):
        _projects(monkeypatch, frshty=False)
        item_id, sid = _mkrun("Merge PR tipu/frshty#20 into main", contexts="frshty")
        out = work_launch.gate_merge(sid, "gh pr merge 20 --squash")
        assert out["decision"] == "allow"
        assert _gate_events(item_id)[0]["verdict"] == "operator_asked"

    def test_an_operator_reply_that_asks_for_the_merge_permits_it(self, monkeypatch):
        _projects(monkeypatch, frshty=False)
        item_id, sid = _mkrun("harden the scope gate", contexts="frshty")
        assert work_launch.gate_merge(sid, "gh pr merge 21")["decision"] == "deny"
        run = db.query_one("SELECT id FROM work_runs WHERE session_id = ?", (sid,))
        with db.tx() as c:
            c.execute(
                "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                "VALUES (?, ?, 'operator_reply', ?, ?)",
                (item_id, run["id"], db.dump_json({"text": "looks good, merge it"}),
                 "2026-09-14T17:00:00+00:00"))
        assert work_launch.gate_merge(sid, "gh pr merge 21")["decision"] == "allow"

    def test_a_git_merge_is_not_gated(self, monkeypatch):
        _projects(monkeypatch, aimyable=False)
        item_id, sid = _mkrun("rebase onto main", contexts="aimyable")
        out = work_launch.gate_merge(sid, "git merge origin/main --no-edit")
        assert out["decision"] == "allow"
        assert _gate_events(item_id) == []

    def test_an_operator_reply_payload_that_is_not_text_does_not_crash(self, monkeypatch):
        _projects(monkeypatch, frshty=False)
        item_id, sid = _mkrun("harden the scope gate", contexts="frshty")
        run = db.query_one("SELECT id FROM work_runs WHERE session_id = ?", (sid,))
        with db.tx() as c:
            for payload in (db.dump_json({"text": 1}), db.dump_json(["merge"]), "not json"):
                c.execute(
                    "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, "
                    "created_at) VALUES (?, ?, 'operator_reply', ?, ?)",
                    (item_id, run["id"], payload, "2026-09-14T17:00:00+00:00"))
        assert work_launch.gate_merge(sid, "gh pr merge 21")["decision"] == "deny"

    def test_a_session_with_no_work_item_is_not_gated(self):
        out = work_launch.gate_merge("sid-not-a-task", "gh pr merge 1")
        assert out["decision"] == "allow"

    def test_the_incident_that_produced_this_gate(self, monkeypatch):
        """Work item 125 on atropos, whose instance sets auto_merge = false."""
        _projects(monkeypatch, frshty=False)
        item_id, sid = _mkrun(
            "Remove or simplify the explicit code generation in "
            "text-to-tql-service/pull/184 to use the automatic backend handling "
            "provided by create_deep_agent function",
            contexts=f"frshty,{work_launch.SLACK_LABEL}")
        out = work_launch.gate_merge(
            sid, "gh pr merge 184 --repo atroposhealth/text-to-tql-service --merge")
        assert out["decision"] == "deny"
        assert _gate_events(item_id)[0]["projects"] == ["frshty"]
