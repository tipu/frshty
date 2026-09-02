import json
import subprocess
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


TICKET_KEY = "PROJ-1"
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


def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.05, message: str = "condition not met"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(message)


def _job_row(task: str, ticket_key: str | None = None) -> dict | None:
    query = "SELECT * FROM jobs WHERE instance_key=? AND task=?"
    params: list[object] = [state.active_instance_key(), task]
    if ticket_key is None:
        query += " ORDER BY id DESC LIMIT 1"
    else:
        query += " AND ticket_key=? ORDER BY id DESC LIMIT 1"
        params.append(ticket_key)
    return db.query_one(query, tuple(params))


def _wait_for_job(task: str, ticket_key: str | None = None, status: str = "ok") -> dict:
    def _done():
        row = _job_row(task, ticket_key)
        if row and row["status"] not in ("queued", "running"):
            return row
        return None

    row = _wait_for(_done, message=f"job {task} for {ticket_key} did not finish")
    assert row["status"] == status, row
    assert row["started_at"], row
    assert row["finished_at"], row
    return row


def _enqueue_and_wait(instance_key: str, task: str, ticket_key: str | None = None, payload: dict | None = None) -> dict:
    job_id = q.enqueue_job(instance_key, task, ticket_key=ticket_key, payload=payload or {})

    def _done():
        row = db.query_one("SELECT * FROM jobs WHERE id=?", (job_id,))
        if row and row["status"] not in ("queued", "running"):
            return row
        return None

    row = _wait_for(_done, message=f"job {job_id} did not finish")
    assert row["status"] == "ok", row
    return row


def _ticket(key: str = TICKET_KEY) -> dict:
    ticket = state.load_ticket(key)
    assert ticket is not None, key
    return ticket


def _response_artifacts(row: dict) -> dict:
    return json.loads(row["response"] or "{}").get("artifacts", {})


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
    def __init__(self, ticket: dict):
        self.ticket = ticket

    def fetch_tickets(self) -> list[dict]:
        return [dict(self.ticket)]

    def fetch_comments(self, key: str) -> list[dict]:
        assert key == self.ticket["key"]
        return []


class FakePlatform:
    def __init__(self, repo_path: Path, bot_user: str):
        self.repo_path = repo_path
        self.bot_user = bot_user
        self._next_pr_id = 1
        self.prs: dict[int, dict] = {}

    def self_id(self) -> str:
        return self.bot_user

    def list_my_open_prs(self) -> list[dict]:
        return [
            {
                "id": pr["id"],
                "repo": pr["repo"],
                "title": pr["title"],
                "author": pr["author"],
                "branch": pr["branch"],
                "base": pr["base"],
                "created_on": pr["created_on"],
                "updated_on": pr["updated_on"],
                "url": pr["url"],
            }
            for pr in self.prs.values()
            if pr["state"] == "OPEN"
        ]

    def push_branch(self, repo_path: Path, branch: str, force: bool = False) -> dict:
        args = ["push", "-u", "origin", branch]
        if force:
            args = ["push", "--force-with-lease", "-u", "origin", branch]
        result = subprocess.run(
            ["git", *args],
            cwd=str(repo_path),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            return {"ok": False, "error": result.stderr.strip()}
        self.record_push(repo_path, branch)
        return {"ok": True}

    def record_push(self, repo_path: Path, branch: str) -> None:
        for pr in self.prs.values():
            if pr["branch"] != branch:
                continue
            contents = (repo_path / "app.txt").read_text()
            sha = _git(repo_path, "rev-parse", "HEAD").stdout.strip()
            pr["push_count"] += 1
            pr["snapshots"].append({"sha": sha, "contents": contents})
            pr["updated_on"] = "2026-04-25T12:00:00Z"

    def create_pr(self, repo: str, repo_path: Path, branch: str, title: str, body: str, base_branch: str) -> dict:
        pr_id = self._next_pr_id
        self._next_pr_id += 1
        pr = {
            "id": pr_id,
            "repo": repo,
            "title": title,
            "body": body,
            "author": self.bot_user,
            "branch": branch,
            "base": base_branch,
            "url": f"https://example.test/{repo}/pull/{pr_id}",
            "state": "OPEN",
            "created_on": "2026-04-25T12:00:00Z",
            "updated_on": "2026-04-25T12:00:00Z",
            "comments": [],
            "resolved_comment_ids": set(),
            "push_count": 0,
            "snapshots": [],
        }
        self.prs[pr_id] = pr
        self.record_push(repo_path, branch)
        return {"url": pr["url"], "id": pr_id}

    def get_pr_info(self, repo: str, pr_id: int) -> dict:
        pr = self.prs[pr_id]
        return {
            "state": pr["state"],
            "updated_on": pr["updated_on"],
            "title": pr["title"],
            "description": pr["body"],
            "author": pr["author"],
            "mergeable": "MERGEABLE",
            "approvers": [],
        }

    def get_pr_state(self, repo: str, pr_id: int) -> str:
        return self.prs[pr_id]["state"]

    def get_pr_comments(self, repo: str, pr_id: int) -> list[dict]:
        pr = self.prs[pr_id]
        return [dict(comment) for comment in pr["comments"]]

    def resolve_comment(self, repo: str, pr_id: int, comment_id: int) -> dict:
        pr = self.prs[pr_id]
        pr["resolved_comment_ids"].add(comment_id)
        return {"status": "resolved"}

    def get_pr_checks(self, repo: str, pr_id: int) -> list[dict]:
        pr = self.prs[pr_id]
        latest = pr["snapshots"][-1]["contents"] if pr["snapshots"] else ""
        state_name = "SUCCESS" if "ci_fixed\n" in latest else "FAILED"
        return [{"name": "CI", "state": state_name, "url": f"https://ci.example.test/{pr_id}"}]

    def get_failed_logs(self, repo: str, pr_id: int) -> str:
        latest = self.prs[pr_id]["snapshots"][-1]["contents"]
        if "ci_fixed\n" in latest:
            return ""
        return "CI failed because the generated change is missing the ci_fixed marker."

    def get_pr_diff(self, repo: str, pr_id: int) -> str:
        pr = self.prs[pr_id]
        _git(self.repo_path, "fetch", "origin", pr["branch"])
        return _git(self.repo_path, "diff", "origin/main...origin/" + pr["branch"]).stdout

    def monitor_ci(self, ticket: dict, ts: dict, base_url: str) -> dict:
        for pr in ts.get("prs", []):
            checks = self.get_pr_checks(pr["repo"], pr["id"])
            failing = [c for c in checks if c["state"] == "FAILED"]
            if failing:
                return {"_ci_failed": True, "pr": pr, "checks": checks}
        ts["ci_passed"] = True
        ts.pop("checks_started_at", None)
        return ts

    def merge_pr(self, repo: str, pr_id: int) -> dict:
        self.prs[pr_id]["state"] = "MERGED"
        return {"status": "merged"}

    def add_comment(self, pr_id: int, body: str, *, path: str = "app.txt", line: int = 1) -> dict:
        pr = self.prs[pr_id]
        comment = {
            "id": len(pr["comments"]) + 1,
            "body": body,
            "author_id": "reviewer-1",
            "author_name": "Reviewer",
            "path": path,
            "line": line,
            "created_on": "2026-04-25T12:30:00Z",
            "parent_id": None,
        }
        pr["comments"].append(comment)
        return comment


def test_ticket_lifecycle_end_to_end(tmp_path):
    state_dir = tmp_path / "state" / tmp_path.name
    state.init(state_dir)
    log.init(state_dir, "test")

    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True)
    _, repo_path = _init_repo(workspace_root)

    instance_key = state_dir.name
    ticket = {
        "key": TICKET_KEY,
        "summary": "Build lifecycle flow",
        "status": "In Progress",
        "description": "Implement the requested lifecycle automation.",
        "url": "https://jira.example.test/browse/PROJ-1",
        "attachments": [],
        "related": [],
        "parent": None,
        "subtasks": [],
        "estimate_seconds": 3600,
    }
    ticket_system = FakeTicketSystem(ticket)
    platform = FakePlatform(repo_path, bot_user="frshty-bot")

    config = {
        "job": {"key": instance_key, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": workspace_root,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
            "branch_prefix": "",
            "exclude": [],
            "dep_commands": [],
            "repos": [REPO_NAME],
        },
        "features": {"tickets": True},
        "pr": {"auto_pr": True, "auto_merge": False},
        "pm_agent": {"enabled": False},
        "github": {"repo": "org/app", "user": "frshty-bot"},
        "bitbucket": {"user_account_id": "frshty-bot"},
        "jira": {},
        "_state_dir": state_dir,
        "_base_url": "http://testserver",
    }

    def fake_run_haiku(prompt: str, timeout: int = 0) -> str:
        if "Classify this engineering ticket" in prompt:
            return "code"
        if "Triage each PR review comment" in prompt:
            return '{"results": [{"i": 0, "actionable": true}]}'
        if "Reply with JSON:" in prompt and "actionable" in prompt:
            return '{"actionable": true, "reason": "specific requested code change"}'
        if "Reply with EXACTLY one JSON object" in prompt and "caused_by_us" in prompt:
            return '{"caused_by_us": true, "reason": "the branch is missing the ci_fixed marker", "fix_hint": "add ci_fixed to app.txt"}'
        if prompt.startswith("Summarize this PR description"):
            return "Lifecycle implementation with generated code, review fixes, and CI fixes."
        return "ok"

    def fake_extract_json(raw: str):
        return json.loads(raw)

    def fake_consensus_plan(config, ticket_dir: Path, slug: str, *, ticket_key: str = ""):
        """start_planning delegates planning to core.consensus_plan, which fans
        out to claude/codex/agy. Stub the subsystem at its boundary: this test
        covers the lifecycle, not consensus internals."""
        ticket_dir = Path(ticket_dir)
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True, exist_ok=True)
        (docs / "technical-plan.md").write_text("# Plan\n\nImplement lifecycle flow.\n")
        (docs / "change-manifest.md").write_text("# Change Manifest\n\nGenerated lifecycle implementation.\n")
        repo = ticket_dir / REPO_NAME
        if repo.is_dir():
            (repo / "app.txt").write_text("base\ngenerated_feature\n")
        return True, ""

    def fake_run_claude_code(prompt: str, cwd: Path, timeout: int = 0,
                              session_id: str | None = None, resume: bool = False):
        cwd = Path(cwd)
        if "/ctp docs/" in prompt:
            docs = cwd / "docs"
            docs.mkdir(parents=True, exist_ok=True)
            (docs / "technical-plan.md").write_text("# Plan\n\nImplement lifecycle flow.\n")
            (docs / "change-manifest.md").write_text("# Change Manifest\n\nGenerated lifecycle implementation.\n")
            repo = cwd / REPO_NAME
            (repo / "app.txt").write_text("base\ngenerated_feature\n")
            return "planned"

        if "save it as docs/FLOW.html" in prompt:
            (cwd / "docs" / "FLOW.html").write_text("<html><body>flow</body></html>")
            return "flow-doc-written"

        if "Run /tri-review" in prompt and "Fix all blocking findings" not in prompt:
            (cwd / "docs" / "tri-review.md").write_text(
                "# Tri Review\n\nBlocking issue: generated_feature needs review_fix.\n\nVERDICT: FAIL\n"
            )
            return "review-fail"

        if "Read docs/tri-review.md and identify the blocking findings" in prompt:
            repo = cwd / REPO_NAME
            (repo / "app.txt").write_text("base\ngenerated_feature\nreview_fixed\n")
            return "review-fixed"

        if "Independent verification step" in prompt:
            (cwd / "docs" / "tri-review.md").write_text(
                "# Tri Review\n\nAll blocking findings addressed.\n\nVERDICT: PASS\n"
            )
            return "review-verified"

        if "CI failed:" in prompt or "CI checks failed:" in prompt:
            repo = cwd
            branch = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
            (repo / "app.txt").write_text((repo / "app.txt").read_text() + "ci_fixed\n")
            _git(repo, "add", "app.txt")
            _git(repo, "commit", "--no-verify", "-m", "fix: ci failure")
            _git(repo, "push", "-u", "origin", branch)
            platform.record_push(repo, branch)
            return "ci-fixed"

        if "Fix this review comment." in prompt:
            repo = cwd
            existing = (repo / "app.txt").read_text()
            if "review_comment_fixed\n" not in existing:
                (repo / "app.txt").write_text(existing + "review_comment_fixed\n")
            return "comment-fixed"

        if "Produce docs/test-plan.md" in prompt:
            (cwd / "docs" / "test-plan.md").write_text(
                "# Test Plan\n\n"
                "| # | Acceptance criterion | Layer | Repo | File path | Test name | Assertion | Preconditions |\n"
                "|---|---|---|---|---|---|---|---|\n"
                "| 1 | feature works | unit | app | tests/test_feature.py | test_feature_works | feature returns expected | none |\n\n"
                "Layer counts: unit: 1  integration: 0  e2e: 0  total: 1\n"
            )
            return "test-plan-written"

        if "Read docs/test-plan.md" in prompt:
            (cwd / "docs" / "test-files-written.txt").write_text(
                f"{REPO_NAME}/tests/test_feature.py\n"
            )
            return "tests-written"

        raise AssertionError(f"unexpected prompt: {prompt}")

    registry = SimpleNamespace(instance_key=instance_key, config=config, base_url=config["_base_url"])
    pool = worker_mod.WorkerPool({instance_key: registry}, size=1, poll_interval=0.05)

    with patch("features.tickets.make_ticket_system", return_value=ticket_system), \
         patch("features.tickets.make_platform", return_value=platform), \
         patch("core.tasks.tickets.make_platform", return_value=platform), \
         patch("features.tickets.run_haiku", side_effect=fake_run_haiku), \
         patch("features.tickets.run_balanced", side_effect=fake_run_haiku), \
         patch("features.tickets.extract_json", side_effect=fake_extract_json), \
         patch("features.pr_ci.run_balanced", side_effect=fake_run_haiku), \
         patch("features.pr_ci.extract_json", side_effect=fake_extract_json), \
         patch("features.pr_ci.run_claude_code", side_effect=fake_run_claude_code), \
         patch("core.tasks.tickets.run_claude_code", side_effect=fake_run_claude_code), \
         patch("features.tickets.run_claude_code", side_effect=fake_run_claude_code), \
         patch("core.tasks.tickets.run_consensus_plan", side_effect=fake_consensus_plan), \
         patch("features.acceptance.run_haiku", side_effect=fake_run_haiku), \
         patch("features.acceptance.extract_json", side_effect=lambda raw: None):
        pool.start()
        try:
            _enqueue_and_wait(instance_key, "scan_tickets")
            _wait_for_job("start_planning", TICKET_KEY)
            assert _ticket()["status"] == "reviewing"
            ticket_dir = workspace_root / "tickets" / f"{TICKET_KEY}-build-lifecycle-flow"
            assert (ticket_dir / "docs" / "change-manifest.md").exists()
            assert (ticket_dir / REPO_NAME / "app.txt").read_text() == "base\ngenerated_feature\n"

            _enqueue_and_wait(instance_key, "scan_tickets")
            _wait_for_job("generate_flow_doc", TICKET_KEY)
            assert (ticket_dir / "docs" / "FLOW.html").exists()

            _enqueue_and_wait(instance_key, "scan_tickets")
            _wait_for_job("start_reviewing", TICKET_KEY)
            assert _ticket()["status"] == "reviewing"
            assert "VERDICT: FAIL" in (ticket_dir / "docs" / "tri-review.md").read_text()

            _enqueue_and_wait(instance_key, "scan_tickets")
            _wait_for_job("fix_review_findings", TICKET_KEY)
            assert _ticket()["status"] == "reviewing"
            assert "VERDICT: PASS" in (ticket_dir / "docs" / "tri-review.md").read_text()
            assert "review_fixed\n" in (ticket_dir / REPO_NAME / "app.txt").read_text()

            _enqueue_and_wait(instance_key, "scan_tickets")
            # Every PASS ticket now walks the testing state: enter_testing →
            # plan_tests → write_tests → run_tests_and_fix → mark_ready. The
            # synthetic repo here has no detectable test runner; the
            # permissive verdict (no failures + no_runner = PASS) lets the
            # ticket complete the gate without a real test runner.
            enter_testing_job = _wait_for_job("enter_testing", TICKET_KEY)
            assert enter_testing_job["status"] == "ok"
            assert _ticket()["status"] == "testing"

            _enqueue_and_wait(instance_key, "scan_tickets")
            _wait_for_job("plan_tests", TICKET_KEY)
            assert (ticket_dir / "docs" / "test-plan.md").exists()

            _enqueue_and_wait(instance_key, "scan_tickets")
            _wait_for_job("write_tests", TICKET_KEY)
            assert (ticket_dir / "docs" / "test-files-written.txt").exists()

            _enqueue_and_wait(instance_key, "scan_tickets")
            run_tests_job = _wait_for_job("run_tests_and_fix", TICKET_KEY)
            assert run_tests_job["status"] == "ok"
            test_runs = (ticket_dir / "docs" / "test-runs.md").read_text()
            assert "VERDICT: PASS" in test_runs

            # No PROOF.md at workspace.root → enter_proving takes the skip
            # path: fires ticket_dev_complete inline and transitions
            # testing → pr_ready directly. The previous mark_ready step is
            # now reached only when a PROOF.md gates a ticket through the
            # proving state; covered separately in unit tests.
            _enqueue_and_wait(instance_key, "scan_tickets")
            enter_proving_job = _wait_for_job("enter_proving", TICKET_KEY)
            assert enter_proving_job["status"] == "ok"
            assert _ticket()["status"] == "pr_ready"

            _enqueue_and_wait(instance_key, "scan_tickets")
            state_after_pr = _ticket()
            assert state_after_pr["status"] == "in_review"
            assert len(state_after_pr["prs"]) == 1
            pr_id = state_after_pr["prs"][0]["id"]
            assert "review_fixed\n" in platform.prs[pr_id]["snapshots"][0]["contents"]

            ci_fix = _wait_for_job("fix_ci_failures", TICKET_KEY)
            assert _response_artifacts(ci_fix)["ci_fix_attempts"] == 1
            assert _ticket()["status"] == "in_review"
            assert _ticket()["ci_fix_attempts"] == 1
            assert platform.prs[pr_id]["push_count"] >= 2
            assert "ci_fixed\n" in platform.prs[pr_id]["snapshots"][-1]["contents"]

            _enqueue_and_wait(instance_key, "scan_tickets")
            assert _ticket()["status"] == "in_review"
            assert _ticket()["ci_passed"] is True

            comment = platform.add_comment(pr_id, "Please add the review_comment_fixed marker to app.txt.")
            _enqueue_and_wait(instance_key, "scan_tickets")
            assert _ticket()["status"] == "in_review"
            assert comment["id"] in platform.prs[pr_id]["resolved_comment_ids"]
            assert platform.prs[pr_id]["push_count"] == 3
            assert "review_comment_fixed\n" in platform.prs[pr_id]["snapshots"][-1]["contents"]
            comment_log = json.loads((ticket_dir / "pr_comments.json").read_text())
            assert comment_log[-1]["status"] == "addressed"

            config["pr"]["auto_merge"] = True
            _enqueue_and_wait(instance_key, "scan_tickets")
            final_state = _ticket()
            assert final_state["status"] == "merged"
            assert platform.prs[pr_id]["state"] == "MERGED"
            assert "merged_at" in final_state
        finally:
            pool.stop()
