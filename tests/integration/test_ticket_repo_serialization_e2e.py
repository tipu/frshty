"""End-to-end regression: 10 PRD tickets that all want to edit the same file
must execute sequentially through the pipeline, never two at once. Without the
per-repo serialization gate, all 10 land at pr_failed because their concurrent
PRs conflict on the shared file."""
import json
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import core.db as db
import core.log as log
import core.queue as q
import core.state as state
import core.tasks  # noqa: F401
import core.worker as worker_mod


N_TICKETS = 10
REPO_NAME = "app"


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )


def _init_repo(workspace_root: Path) -> tuple[Path, Path]:
    remote = workspace_root / "remote.git"
    local = workspace_root / REPO_NAME
    remote.parent.mkdir(parents=True, exist_ok=True)

    _git(workspace_root, "init", "--bare", str(remote))
    _git(workspace_root, "clone", str(remote), str(local))
    _git(local, "config", "user.name", "frshty")
    _git(local, "config", "user.email", "frshty@example.com")
    (local / "app.txt").write_text("base\n")
    _git(local, "add", "app.txt")
    _git(local, "commit", "-m", "initial")
    _git(local, "branch", "-M", "main")
    _git(local, "push", "-u", "origin", "main")
    return remote, local


class FakeTicketSystem:
    def __init__(self, tickets: list[dict]):
        self.tickets = tickets

    def fetch_tickets(self) -> list[dict]:
        return [dict(t) for t in self.tickets]

    def fetch_comments(self, key: str) -> list[dict]:
        return []


class FakePlatform:
    def __init__(self, repo_path: Path, bot_user: str):
        self.repo_path = repo_path
        self.bot_user = bot_user
        self._next_pr_id = 1
        self.prs: dict[int, dict] = {}
        # Production BitbucketPlatform.merge_pr is a single HTTP call so it
        # serialises naturally at the API layer. Our fake performs three git
        # commands against a SHARED local clone (fetch → checkout main →
        # merge → push); without a lock, two in_review tickets reaching
        # merge_pr concurrently race on .git/index and the second one's
        # `git checkout main` fails with "Another git process seems to be
        # running" or similar. The per-repo gate intentionally excludes
        # pr_ready/in_review (see _GATE_OCCUPYING_STATUSES) to prevent a
        # deadlock with auto_merge=false, so the fake has to do the
        # serialisation itself.
        self._merge_lock = threading.Lock()

    def list_my_open_prs(self) -> list[dict]:
        return [
            {"id": pr["id"], "repo": pr["repo"], "title": pr["title"],
             "author": pr["author"], "branch": pr["branch"], "base": pr["base"],
             "created_on": pr["created_on"], "updated_on": pr["updated_on"],
             "url": pr["url"]}
            for pr in self.prs.values() if pr["state"] == "OPEN"
        ]

    def push_branch(self, repo_path: Path, branch: str, force: bool = False) -> dict:
        args = ["push", "-u", "origin", branch]
        if force:
            args = ["push", "--force-with-lease", "-u", "origin", branch]
        result = subprocess.run(["git", *args], cwd=str(repo_path),
                                capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return {"ok": False, "error": result.stderr.strip()}
        for pr in self.prs.values():
            if pr["branch"] == branch:
                pr["push_count"] += 1
                pr["updated_on"] = "2026-04-25T12:00:00Z"
        return {"ok": True}

    def create_pr(self, repo: str, repo_path: Path, branch: str, title: str,
                  body: str, base_branch: str) -> dict:
        pr_id = self._next_pr_id
        self._next_pr_id += 1
        pr = {"id": pr_id, "repo": repo, "title": title, "body": body,
              "author": self.bot_user, "branch": branch, "base": base_branch,
              "url": f"https://example.test/{repo}/pull/{pr_id}",
              "state": "OPEN", "created_on": "2026-04-25T12:00:00Z",
              "updated_on": "2026-04-25T12:00:00Z", "comments": [],
              "resolved_comment_ids": set(), "push_count": 1}
        self.prs[pr_id] = pr
        return {"url": pr["url"], "id": pr_id}

    def get_pr_info(self, repo: str, pr_id: int) -> dict:
        pr = self.prs[pr_id]
        return {"state": pr["state"], "updated_on": pr["updated_on"],
                "title": pr["title"], "description": pr["body"],
                "author": pr["author"], "mergeable": "MERGEABLE", "approvers": []}

    def get_pr_state(self, repo: str, pr_id: int) -> str:
        return self.prs[pr_id]["state"]

    def get_pr_comments(self, repo: str, pr_id: int) -> list[dict]:
        return [dict(c) for c in self.prs[pr_id]["comments"]]

    def resolve_comment(self, repo: str, pr_id: int, comment_id: int) -> dict:
        self.prs[pr_id]["resolved_comment_ids"].add(comment_id)
        return {"status": "resolved"}

    def get_pr_checks(self, repo: str, pr_id: int) -> list[dict]:
        return [{"name": "CI", "state": "SUCCESS",
                 "url": f"https://ci.example.test/{pr_id}"}]

    def get_failed_logs(self, repo: str, pr_id: int) -> str:
        return ""

    def get_pr_diff(self, repo: str, pr_id: int) -> str:
        pr = self.prs[pr_id]
        _git(self.repo_path, "fetch", "origin", pr["branch"])
        return _git(self.repo_path, "diff",
                    f"origin/main...origin/{pr['branch']}").stdout

    def monitor_ci(self, ticket: dict, ts: dict, base_url: str) -> dict:
        ts["ci_passed"] = True
        ts.pop("checks_started_at", None)
        return ts

    def merge_pr(self, repo: str, pr_id: int) -> dict:
        with self._merge_lock:
            pr = self.prs[pr_id]
            # Idempotent: the dispatcher's done-ticket revival path
            # (check() ~ts.status==done with prs → in_review) re-invokes
            # _merge → merge_pr on every poll. Real Bitbucket/GitHub return
            # "already merged" immediately; we mirror that so the fake doesn't
            # try to fetch a deleted branch on each revival cycle.
            if pr["state"] == "MERGED":
                return {"status": "merged"}
            pr["state"] = "MERGED"
            _git(self.repo_path, "fetch", "origin", pr["branch"])
            _git(self.repo_path, "checkout", "main")
            _git(self.repo_path, "merge", "--no-edit", "--no-ff",
                 f"origin/{pr['branch']}")
            _git(self.repo_path, "push", "origin", "main")
            return {"status": "merged"}


def test_ten_concurrent_prd_tickets_serialize_and_complete(tmp_path):
    """All 10 PRD tickets target the same hot-spot file. With the per-repo gate,
    they execute sequentially: at most one ticket occupies the pipeline
    (planning/reviewing/pr_ready/in_review) at any given snapshot. Without the
    gate, multiple PRs would race and most would land at pr_failed."""
    state_dir = tmp_path / "state" / tmp_path.name
    state.init(state_dir)
    log.init(state_dir, "test")
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    _, repo_path = _init_repo(workspace_root)
    instance_key = state_dir.name

    tickets_data = []
    for i in range(1, N_TICKETS + 1):
        tickets_data.append({
            "key": f"PRD-{i}",
            "summary": f"feature {i} touches app.txt",
            "status": "approved",
            "description": f"PRD-derived ticket {i}; touches the shared file.",
            "url": f"https://prd.example.test/PRD-{i}",
            "attachments": [], "related": [], "parent": None,
            "subtasks": [], "estimate_seconds": 600,
        })
    ticket_system = FakeTicketSystem(tickets_data)
    platform = FakePlatform(repo_path, bot_user="frshty-bot")

    config = {
        "job": {"key": instance_key, "platform": "github", "ticket_system": "prd"},
        "workspace": {"root": workspace_root, "tickets_dir": "tickets",
                      "ticket_layout": "flat", "base_branch": "main",
                      "branch_prefix": "", "exclude": [], "dep_commands": [],
                      "repos": [REPO_NAME]},
        "features": {"tickets": True},
        "pr": {"auto_pr": True, "auto_merge": True, "merge_strategy": "squash",
               "merge_flags": []},
        "ticket_approval": {"required": False, "sources": []},
        "pm_agent": {"enabled": False},
        "github": {"repo": "org/app", "user": "frshty-bot"},
        "bitbucket": {"user_account_id": "frshty-bot"},
        "jira": {},
        "_state_dir": state_dir,
        "_base_url": "http://testserver",
    }

    # Pre-seed approved PRD ticket rows so enqueue_prd_backfill picks them up
    # (the FakeTicketSystem also returns them, but PRD intake is via state rows).
    for t in tickets_data:
        state.save_ticket(t["key"], {
            "status": "new", "source": "prd", "approval_status": "approved",
            "summary": t["summary"], "description": t["description"],
            "url": t["url"],
        })

    counter = {"i": 0}

    def fake_run_haiku(prompt: str, timeout: int = 0) -> str:
        if "Classify each PR review comment" in prompt:
            return '{"results": []}'
        if "Reply with JSON:" in prompt and "actionable" in prompt:
            return '{"actionable": false, "reason": "no comment"}'
        if "caused_by_us" in prompt:
            return '{"caused_by_us": false, "reason": "ci passed"}'
        if "ranking software tickets" in prompt:
            # _RANKER_PROMPT in features/tickets.py — features.tickets.extract_json
            # is patched to a strict json.loads (no fallback), so any non-JSON
            # response triggers JSONDecodeError → rank_new_tickets fails on
            # every dispatcher tick. Return an empty dependency graph so each
            # PRD ticket's blocked_by stays [] (which is what this test expects
            # — none of the tickets depend on each other).
            return '{"dependencies": []}'
        if "per-repo pull request description" in prompt:
            # _PR_DESCRIPTION_PROMPT in core/tasks/tickets.py — fake_extract_json
            # uses strict json.loads so a plain "ok" would JSONDecode-fail.
            return '{"title": "Test PR", "description": "Sequential test fixture."}'
        if prompt.startswith("Summarize this PR description"):
            return "Sequential implementation."
        return "ok"

    def fake_extract_json(raw: str):
        return json.loads(raw)

    def fake_run_claude_code(prompt: str, cwd: Path, timeout: int = 0,
                              session_id: str | None = None,
                              resume: bool = False):
        cwd = Path(cwd)
        if "/ctp docs/" in prompt:
            docs = cwd / "docs"
            docs.mkdir(parents=True, exist_ok=True)
            (docs / "technical-plan.md").write_text("# Plan\n\nTest Plan: ok\n")
            (docs / "change-manifest.md").write_text("# Change Manifest\n")
            repo = cwd / REPO_NAME
            counter["i"] += 1
            existing = (repo / "app.txt").read_text()
            (repo / "app.txt").write_text(
                existing + f"line_added_by_ticket_{counter['i']}\n"
            )
            _git(repo, "add", "app.txt")
            _git(repo, "commit", "--no-verify", "-m", f"feat: ticket {counter['i']}")
            return "planned"
        if "Run /tri-review" in prompt:
            (cwd / "docs" / "tri-review.md").write_text(
                "# Tri Review\n\nAll good.\n\nVERDICT: PASS\n"
            )
            return "reviewed"
        if "Read docs/tri-review.md and identify" in prompt:
            return "no-fix-needed"
        if "Independent verification step" in prompt:
            (cwd / "docs" / "tri-review.md").write_text(
                "# Tri Review\n\nVerified.\n\nVERDICT: PASS\n"
            )
            return "verified"
        if "Produce docs/test-plan.md" in prompt:
            # plan_tests postcondition is file_exists("docs/test-plan.md").
            (cwd / "docs" / "test-plan.md").write_text(
                "# Test Plan\n\n| # | Test | Layer |\n|---|---|---|\n"
                "| 1 | smoke | unit |\n"
            )
            return "test-plan-written"
        if "Read docs/test-plan.md" in prompt:
            # write_tests postcondition is file_exists("docs/test-files-written.txt").
            (cwd / "docs" / "test-files-written.txt").write_text(
                f"{REPO_NAME}/tests/smoke.txt\n"
            )
            return "test-files-written"
        if "A PROOF.md guide is provided" in prompt:
            # prove writes docs/proof.md; the test workspace has no PROOF.md so
            # the real prove() short-circuits to NOT_APPLICABLE before calling
            # Claude — but in case it's invoked anyway, emit a NOT_APPLICABLE.
            (cwd / "docs" / "proof.md").write_text("PROOF: NOT_APPLICABLE\n")
            return "proof-written"
        return "ok"

    registry = SimpleNamespace(instance_key=instance_key, config=config,
                                base_url=config["_base_url"])
    pool = worker_mod.WorkerPool({instance_key: registry}, size=4,
                                  poll_interval=0.05)

    pipeline_statuses = {"planning", "reviewing", "pr_ready", "in_review"}
    occupancy_snapshots: list[set[str]] = []

    def snapshot_occupancy() -> set[str]:
        rows = db.query_all(
            "SELECT ticket_key, status FROM tickets WHERE instance_key=?",
            (instance_key,),
        )
        return {r["ticket_key"] for r in rows if r["status"] in pipeline_statuses}

    with patch("features.tickets.make_ticket_system", return_value=ticket_system), \
         patch("features.tickets.make_platform", return_value=platform), \
         patch("core.tasks.tickets.make_platform", return_value=platform), \
         patch("features.tickets.run_haiku", side_effect=fake_run_haiku), \
         patch("features.tickets.extract_json", side_effect=fake_extract_json), \
         patch("core.tasks.tickets.run_haiku", side_effect=fake_run_haiku), \
         patch("core.tasks.tickets.extract_json", side_effect=fake_extract_json), \
         patch("features.pr_ci.run_balanced", side_effect=fake_run_haiku), \
         patch("features.pr_ci.extract_json", side_effect=fake_extract_json), \
         patch("features.pr_ci.run_claude_code", side_effect=fake_run_claude_code), \
         patch("core.tasks.tickets.run_claude_code", side_effect=fake_run_claude_code), \
         patch("features.tickets.run_claude_code", side_effect=fake_run_claude_code):
        pool.start()
        try:
            # 10 tickets serialized through the per-repo gate at ~6s/ticket
            # (plan_tests → write_tests → run_tests_and_fix → enter_proving →
            # mark_ready → pr_ready → create_pr → in_review → merge) needs at
            # least ~60s wall-clock even with the fakes returning instantly;
            # 180s buffer for dispatcher poll interval + scan_tickets cadence.
            deadline = time.time() + 180
            last_kick = 0.0
            while time.time() < deadline:
                occupancy_snapshots.append(snapshot_occupancy())
                rows = db.query_all(
                    "SELECT status, COUNT(*) AS c FROM tickets WHERE instance_key=? "
                    "GROUP BY status", (instance_key,))
                counts = {r["status"]: r["c"] for r in rows}
                if counts.get("merged", 0) >= N_TICKETS or counts.get("done", 0) >= N_TICKETS:
                    break
                if counts.get("pr_failed", 0) > 0:
                    # Fail fast — gate failure manifests as pr_failed
                    break
                if time.time() - last_kick > 0.4:
                    q.enqueue_job(instance_key, "scan_tickets")
                    last_kick = time.time()
                time.sleep(0.05)
        finally:
            pool.stop()

    final = {r["ticket_key"]: r["status"] for r in db.query_all(
        "SELECT ticket_key, status FROM tickets WHERE instance_key=?",
        (instance_key,))}

    failed = [k for k, s in final.items() if s == "pr_failed"]
    assert not failed, f"tickets landed at pr_failed (gate failed): {failed}"

    not_done = [k for k, s in final.items() if s not in ("merged", "done", "validation")]
    assert not not_done, (
        f"tickets did not reach a terminal state: "
        f"{[(k, final[k]) for k in not_done]}"
    )
    assert len(final) == N_TICKETS, f"expected {N_TICKETS} tickets, got {len(final)}"

    over = [(i, snap) for i, snap in enumerate(occupancy_snapshots) if len(snap) > 1]
    assert not over, (
        f"per-repo gate violated — {len(over)} snapshots had >1 ticket in pipeline.\n"
        f"first violation: cycle {over[0][0]}, occupants={over[0][1]}\n"
        f"max concurrent: {max(len(s) for s in occupancy_snapshots)}"
    )

    open_now = [pr for pr in platform.prs.values() if pr["state"] == "OPEN"]
    assert not open_now, f"PRs still open: {[pr['id'] for pr in open_now]}"
    # PRs are MERGED on close; "no-diff" tickets merge without creating a PR.
    assert len(platform.prs) >= N_TICKETS - 2, (
        f"expected most tickets to create PRs, got only {len(platform.prs)}"
    )
