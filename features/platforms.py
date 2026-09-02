import json
import os
import re
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx

import core.log as log
from core import external_log
from core.config import resolve_env, get_repos, base_branch_for
from core.claude_runner import run_claude_code
from features.pr_ci import FAILED_STATES


_CONFLICT_RESOLVE_MODEL = os.environ.get(
    "FRSHTY_CONFLICT_RESOLVE_MODEL", "claude-opus-4-7"
)


def make_platform(config: dict):
    platform = config["job"]["platform"]
    if platform == "bitbucket":
        return BitbucketPlatform(config)
    if platform == "github":
        return GitHubPlatform(config)
    raise ValueError(f"unknown platform: {platform}")


def _parse_ts(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def _run_git(cwd, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git"] + args, cwd=str(cwd), capture_output=True, text=True, timeout=60,
    )


def _resolve_merge_conflicts(repo_path, base_branch: str, prev_error: str | None = None,
                             merge_error: str = "") -> dict:
    conflicted = _run_git(repo_path, ["diff", "--name-only", "--diff-filter=U"])
    if conflicted.returncode != 0 or not conflicted.stdout.strip():
        _run_git(repo_path, ["merge", "--abort"])
        detail = (merge_error or "").strip()[:300]
        return {"ok": False, "error": f"no conflicted files found: {detail}" if detail else "no conflicted files found"}

    files = [f for f in conflicted.stdout.strip().split("\n") if f]
    for filepath in files:
        if not (Path(repo_path) / filepath).exists():
            _run_git(repo_path, ["merge", "--abort"])
            return {"ok": False, "error": f"conflicted file not found: {filepath}"}

    file_list = "\n".join(f"  - {f}" for f in files)
    prev_attempt_block = ""
    if prev_error:
        prev_attempt_block = (
            "PREVIOUS ATTEMPT FAILED post-checks with this error:\n"
            f"  {prev_error[:400]}\n\n"
            "Address that specific failure mode this time (e.g. if a file still had conflict markers, "
            "actually remove them; if a syntax error remained, fix it before staging).\n\n"
        )
    prompt = (
        f"{prev_attempt_block}"
        f"You are resolving a git merge of origin/{base_branch} into the current branch. "
        f"Conflicted files:\n{file_list}\n\n"
        "For each file:\n"
        "1. Read it. Identify every <<<<<<< / ======= / >>>>>>> region.\n"
        "2. Resolve by combining both sides where intents are compatible (union of imports, "
        "both new functions/endpoints, etc.); only pick one side when they truly contradict.\n"
        "3. Confirm zero conflict markers remain.\n"
        "4. Run an appropriate syntax check (e.g. `python -c 'import ast; ast.parse(open(p).read())'` "
        "for .py; `node --check p` for .js/.cjs; for .ts/.tsx use the project's typecheck if cheap, "
        "else verify structurally). Fix any syntax error you introduce.\n"
        "5. `git add <file>` once clean.\n\n"
        "Do NOT run `git commit` — leave the resolved files staged. If you cannot resolve a file, "
        "leave it conflicted and explain which file and why."
    )
    run_claude_code(prompt, cwd=Path(repo_path), timeout=900,
                    model=_CONFLICT_RESOLVE_MODEL)

    still_conflicted = _run_git(repo_path, ["diff", "--name-only", "--diff-filter=U"])
    if still_conflicted.stdout.strip():
        _run_git(repo_path, ["merge", "--abort"])
        return {"ok": False, "error": f"failed to resolve conflicts in {still_conflicted.stdout.strip()}"}

    for filepath in files:
        full_path = Path(repo_path) / filepath
        if not full_path.exists():
            continue
        text = full_path.read_text(errors="replace")
        if "<<<<<<<" in text or ">>>>>>>" in text:
            _run_git(repo_path, ["merge", "--abort"])
            return {"ok": False, "error": f"conflict markers remain in {filepath}"}
        if filepath.endswith(".py"):
            try:
                import ast
                ast.parse(text)
            except SyntaxError as e:
                _run_git(repo_path, ["merge", "--abort"])
                return {"ok": False, "error": f"syntax error in {filepath}: {e}"}
        _run_git(repo_path, ["add", filepath])

    from core.git_util import commit_with_hooks
    result = commit_with_hooks(
        repo_path, message=None, extra_commit_args=["--no-edit"], timeout=300,
    )
    if result.returncode != 0:
        _run_git(repo_path, ["merge", "--abort"])
        return {"ok": False, "error": result.stderr.strip()}

    return {"ok": True}


def _bb_thread_resolved(comment: dict, by_id: dict) -> bool:
    cur = comment
    seen = set()
    while cur is not None:
        cid = str(cur["id"])
        if cur.get("resolved"):
            return True
        if cid in seen:
            return False
        seen.add(cid)
        parent_id = cur.get("parent_id")
        cur = by_id.get(str(parent_id)) if parent_id is not None else None
    return False


def _identity_block_reason(config: dict, repo_path, branch: str) -> str:
    """Why this push must not happen, or "" when it may.

    The startup gate proves the identity once. This is the only check that
    looks at the commits themselves, so it is the one that catches an identity
    changed after startup — by a human, by the agent, or by a `--author` flag.
    Only the commits this push would publish are examined; history the remote
    already holds belongs to whoever wrote it."""
    git_cfg = config.get("git") or {}
    name, email = git_cfg.get("name", ""), git_cfg.get("email", "")
    if not (name and email):
        return ""
    want = f"{name} <{email}>"
    repo_path = Path(repo_path)
    # A PR worktree is named after the branch slug, not the repo, so the
    # directory name is not a safe key for a per-repo base override. The shared
    # git directory always sits inside the canonical checkout.
    from core.git_util import git_common_dir, run_git
    common = git_common_dir(repo_path)
    repo_name = Path(common).parent.name if common else repo_path.name
    base = base_branch_for(config, repo_name)
    # Two exclusions, and this check needs both. A push publishes
    # origin/<branch>..HEAD, so commits the remote branch already holds are not
    # this push's to answer for: a long-lived branch collects CI-bot commits
    # and commits the same person made under another git identity, and judging
    # them again blocks every later push. Commits already on the base are not
    # this push's either, even when the push does publish them — merging the
    # base carries other people's work by design, and base sync would never
    # complete. What is left is what this push adds and nobody has seen.
    ref = f"origin/{branch}"
    on_remote = run_git(repo_path, ["rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
                        allowed_codes=(0, 1)).returncode == 0
    if not on_remote:
        ref = f"origin/{base}"
    result = subprocess.run(
        ["git", "log", f"{ref}..HEAD", "--not", f"origin/{base}",
         "--format=%h%x00%an <%ae>%x00%cn <%ce>"],
        cwd=str(repo_path), capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        # Fail closed. An unreadable range is not evidence of correct authorship,
        # and a wrong author cannot be taken back once it is on the remote.
        return (f"cannot read {ref}..HEAD in {repo_name}, so the "
                f"authorship of this push is unknown: "
                f"{(result.stderr or '').strip()[:200]}")
    wrong = []
    for line in result.stdout.splitlines():
        parts = line.split("\x00")
        if len(parts) != 3:
            continue
        sha, author, committer = parts
        if author != want or committer != want:
            wrong.append(f"{sha} by {author}")
    if not wrong:
        return ""
    return (f"{len(wrong)} commit(s) on {branch} are not authored by {want}: "
            f"{'; '.join(wrong[:5])}")


class _CIMonitorMixin:
    CI_TIMEOUT_SECS = 3600
    NO_CI_GRACE_SECS = 300

    def monitor_ci(self, ticket, ts, base_url) -> dict:
        prs = ts.get("prs", [])
        if not prs:
            return ts

        if not ts.get("checks_started_at"):
            ts["checks_started_at"] = datetime.now(timezone.utc).isoformat()

        all_passed = True
        for pr in prs:
            checks = self.get_pr_checks(pr["repo"], pr["id"])
            if checks is None:
                if not ts.get("_ci_fetch_failed_logged"):
                    log.emit("ticket_ci_fetch_failed",
                        f"Could not fetch CI checks for {ticket['key']} PR #{pr['id']}; holding (not treating as passed)",
                        links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                        meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})
                    ts["_ci_fetch_failed_logged"] = True
                all_passed = False
                continue
            ts.pop("_ci_fetch_failed_logged", None)
            verdict = self._evaluate_checks(checks)

            if verdict == "pending":
                elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(ts["checks_started_at"])).total_seconds()
                if not checks and elapsed > self.NO_CI_GRACE_SECS:
                    if not ts.get("_ci_no_ci_logged"):
                        log.emit("ticket_no_ci_configured",
                            f"No CI checks for {ticket['key']} PR #{pr['id']} after {int(elapsed/60)}m, treating as passed",
                            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                            meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})
                        ts["_ci_no_ci_logged"] = True
                    ts.pop("_ci_timeout_state", None)
                    continue
                if elapsed > self.CI_TIMEOUT_SECS:
                    elapsed_mins = int(elapsed / 60)
                    timeout_state = ts.get("_ci_timeout_state", {})

                    is_same_pr = timeout_state.get("pr_id") == str(pr["id"])
                    is_same_duration = abs(int(timeout_state.get("last_elapsed_mins", "0")) - elapsed_mins) < 10

                    if is_same_pr and is_same_duration:
                        timeout_state["strike_count"] = str(int(timeout_state.get("strike_count", "1")) + 1)
                    else:
                        timeout_state["strike_count"] = "1"

                    timeout_state["pr_id"] = str(pr["id"])
                    timeout_state["last_elapsed_mins"] = str(elapsed_mins)
                    ts["_ci_timeout_state"] = timeout_state

                    if int(timeout_state["strike_count"]) >= 5:
                        log.emit("ticket_checks_stalled", f"CI checks stalled for {ticket['key']} PR #{pr['id']} after {elapsed_mins}m (5+ timeouts, stopping polling)",
                            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                            meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})
                        return {"_ci_stalled": True, "pr": pr}
                    else:
                        log.emit("ticket_checks_timeout", f"CI checks timed out for {ticket['key']} PR #{pr['id']} after {elapsed_mins}m ({timeout_state.get('strike_count', '1')}/5)",
                            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                            meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})
                    return ts
                all_passed = False
                continue

            if verdict == "failed":
                return {"_ci_failed": True, "pr": pr, "checks": checks}

        if not all_passed:
            return ts

        if not ts.get("ci_passed"):
            log.emit("ticket_checks_passed", f"All CI checks passed for {ticket['key']}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"]})
        ts["ci_passed"] = True
        ts.pop("checks_started_at", None)
        ts.pop("ci_fix_attempts", None)
        ts.pop("ci_fix_heads", None)
        ts.pop("_ci_timeout_state", None)
        ts.pop("_ci_no_ci_logged", None)
        return ts

    def _evaluate_checks(self, checks: list[dict]) -> str:
        if not checks:
            return "pending"
        states = {c["state"].upper() for c in checks}
        if states & set(FAILED_STATES):
            return "failed"
        if states <= {"SUCCESS", "NEUTRAL", "SKIPPED"}:
            return "passed"
        return "pending"


class BitbucketPlatform(_CIMonitorMixin):
    BASE_URL = "https://api.bitbucket.org/2.0"

    def __init__(self, config: dict):
        self.config = config
        bb = config["bitbucket"]
        self.org = bb["org"]
        self.user = resolve_env(config, "bitbucket", "user_env")
        self.token = resolve_env(config, "bitbucket", "token_env")
        self.user_account_id = bb.get("user_account_id", "")
        self.repos = [r["name"] for r in get_repos(config)]

    def self_id(self) -> str:
        return self.user_account_id

    def _auth(self):
        return (self.user, self.token)

    def list_my_open_prs(self) -> list[dict]:
        results = []
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            for repo in self.repos:
                url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests?state=OPEN&pagelen=50"
                resp = client.get(url)
                if resp.status_code != 200:
                    continue
                for pr in resp.json().get("values", []):
                    author_id = pr.get("author", {}).get("account_id", "")
                    if author_id != self.user_account_id:
                        continue
                    results.append(self._normalize_pr(pr, repo))
        return results

    def list_review_prs(self) -> list[dict]:
        import core.log as log
        results = []
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            for repo in self.repos:
                url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests?state=OPEN&pagelen=50"
                resp = client.get(url)
                if resp.status_code != 200:
                    log.emit("poll_error", f"Failed to fetch PRs from {repo}: HTTP {resp.status_code}",
                             meta={"repo": repo, "status": resp.status_code, "url": url})
                    continue
                try:
                    for pr in resp.json().get("values", []):
                        results.append(self._normalize_pr(pr, repo))
                except Exception as e:
                    log.emit("poll_error", f"Error parsing PRs from {repo}: {type(e).__name__}: {e}",
                             meta={"repo": repo, "error": str(e)})
        return results

    def list_pending_reviews_for_me(self) -> list[dict]:
        if not self.user_account_id:
            return []
        results = []
        q = f'state="OPEN" AND reviewers.account_id="{self.user_account_id}"'
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            for repo in self.repos:
                url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests"
                resp = client.get(url, params={"q": q, "pagelen": 50})
                if resp.status_code != 200:
                    continue
                for pr in resp.json().get("values", []):
                    if pr.get("author", {}).get("account_id", "") == self.user_account_id:
                        continue
                    approved_by_me = any(
                        p.get("approved") and p.get("user", {}).get("account_id", "") == self.user_account_id
                        for p in (pr.get("participants") or [])
                    )
                    if approved_by_me:
                        continue
                    results.append(self._normalize_pr(pr, repo))
        return results

    def get_pr_comments(self, repo: str, pr_id: int) -> list[dict]:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments?pagelen=100"
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return []
            out = [
                {
                    "id": c["id"],
                    "body": c["content"]["raw"],
                    "author_id": c.get("user", {}).get("account_id", ""),
                    "author_name": c.get("user", {}).get("display_name", ""),
                    "path": c.get("inline", {}).get("path"),
                    "line": c.get("inline", {}).get("to"),
                    "html_url": c.get("links", {}).get("html", {}).get("href", ""),
                    "created_on": c["created_on"],
                    "created_at": c["created_on"],
                    "updated_at": c.get("updated_on", c["created_on"]),
                    "parent_id": c.get("parent", {}).get("id") if c.get("parent") else None,
                    "resolved": c.get("resolution") is not None,
                }
                for c in resp.json().get("values", [])
            ]
            by_id = {str(c["id"]): c for c in out}
            for c in out:
                c["resolved"] = _bb_thread_resolved(c, by_id)
            return out

    def get_pr_diff(self, repo: str, pr_id: int) -> str | None:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/diff"
        with external_log.client("bitbucket", auth=self._auth(), timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            return resp.text

    def get_pr_checks(self, repo: str, pr_id: int) -> list[dict] | None:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/statuses?pagelen=50"
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            return [
                {
                    "name": s.get("name", ""),
                    "state": s.get("state", "").upper().replace("SUCCESSFUL", "SUCCESS"),
                    "url": s.get("url", ""),
                }
                for s in resp.json().get("values", [])
            ]

    def get_failed_logs(self, repo: str, pr_id: int) -> str:
        checks = self.get_pr_checks(repo, pr_id) or []
        build_ids = []
        for c in checks:
            if c.get("state", "").upper() not in FAILED_STATES:
                continue
            m = re.search(r"/results/(\d+)", c.get("url", ""))
            if m and m.group(1) not in build_ids:
                build_ids.append(m.group(1))
        if not build_ids:
            return ""
        logs = []
        with external_log.client("bitbucket", auth=self._auth(), timeout=30, follow_redirects=True) as client:
            for build in build_ids[:3]:
                steps_url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pipelines/{build}/steps/"
                resp = client.get(steps_url)
                if resp.status_code != 200:
                    continue
                for step in resp.json().get("values", []):
                    result_name = ((step.get("state") or {}).get("result") or {}).get("name", "")
                    if result_name.upper() not in FAILED_STATES:
                        continue
                    log_resp = client.get(f"{steps_url}{step.get('uuid', '')}/log")
                    if log_resp.status_code != 200:
                        continue
                    logs.append(f"=== pipeline #{build} step: {step.get('name', '')} ===\n{log_resp.text[-4000:]}")
        return "\n\n".join(logs)

    def get_pr_state(self, repo: str, pr_id: int) -> str:
        info = self.get_pr_info(repo, pr_id)
        return info["state"]

    def get_pr_info(self, repo: str, pr_id: int) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}"
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.get(url)
            if resp.status_code == 404:
                return {"state": "DELETED", "updated_on": "", "mergeable": "UNKNOWN"}
            if resp.status_code != 200:
                return {"state": "OPEN", "updated_on": "", "mergeable": "UNKNOWN"}
            data = resp.json()
            approvers = [
                p.get("user", {}).get("display_name") or p.get("user", {}).get("account_id", "")
                for p in (data.get("participants") or [])
                if p.get("approved")
            ]
            reviewers = [
                r.get("display_name") or r.get("account_id", "")
                for r in (data.get("reviewers") or [])
            ]
            return {
                "state": data.get("state", "OPEN"),
                "updated_on": data.get("updated_on", ""),
                "title": data.get("title", ""),
                "description": (data.get("description", "") or ""),
                "author": data.get("author", {}).get("display_name", ""),
                "author_id": data.get("author", {}).get("account_id", ""),
                "mergeable": "CONFLICTING" if data.get("has_conflicts") else "MERGEABLE",
                "approvers": approvers,
                "reviewers": reviewers,
                "head_sha": data.get("source", {}).get("commit", {}).get("hash", ""),
            }

    def post_pr_comment(self, repo: str, pr_id: int, body: str, path: str | None = None, line: int | None = None, parent_id: int | None = None) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments"
        payload = {"content": {"raw": body}}
        if path and line:
            payload["inline"] = {"path": path, "to": line}
        if parent_id:
            payload["parent"] = {"id": parent_id}
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.post(url, json=payload)
            if resp.status_code in (200, 201):
                return {"status": "posted", "id": resp.json().get("id")}
            return {"status": "error", "detail": resp.text}

    def edit_pr_comment(self, repo: str, pr_id: int, comment_id: int, body: str) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments/{comment_id}"
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.put(url, json={"content": {"raw": body}})
            if resp.status_code in (200, 201):
                return {"status": "updated"}
            return {"status": "error", "detail": resp.text}

    def resolve_comment(self, repo: str, pr_id: int, comment_id: int) -> dict:
        base = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments"
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            root_id = comment_id
            for _ in range(20):
                resp = client.get(f"{base}/{root_id}")
                if resp.status_code != 200:
                    return {"status": "error", "detail": resp.text}
                data = resp.json()
                parent_id = (data.get("parent") or {}).get("id")
                if not parent_id:
                    if data.get("resolution"):
                        return {"status": "resolved"}
                    break
                root_id = parent_id
            resp = client.post(f"{base}/{root_id}/resolve")
            if resp.status_code in (200, 201, 409):
                return {"status": "resolved"}
            return {"status": "error", "detail": resp.text}

    def get_pr_branch(self, repo: str, pr_id: int) -> str:
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.get(f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}")
            if resp.status_code == 200:
                return resp.json().get("source", {}).get("branch", {}).get("name", "")
        return ""

    def ensure_pr_worktree(self, repo: str, pr_id: int, target: Path) -> bool:
        if (target / ".git").exists():
            _run_git(target, ["fetch", "origin"])
            return True
        target.mkdir(parents=True, exist_ok=True)
        user = self._auth()[0]
        token = self._auth()[1]
        clone_url = f"https://{user}:{token}@bitbucket.org/{self.org}/{repo}.git"
        result = subprocess.run(["git", "clone", "--depth=1", clone_url, str(target)], capture_output=True, timeout=120)
        if result.returncode != 0:
            return False
        branch = ""
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.get(f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}")
            if resp.status_code == 200:
                branch = resp.json().get("source", {}).get("branch", {}).get("name", "")
        if branch:
            _run_git(target, ["fetch", "origin", branch])
            _run_git(target, ["checkout", branch])
        return True

    def push_branch(self, repo_path, branch: str, force: bool = False) -> dict:
        if not branch.strip():
            return {"ok": False, "error": "empty branch name"}
        blocked = _identity_block_reason(self.config, repo_path, branch)
        if blocked:
            log.emit("git_identity_push_blocked", blocked,
                     meta={"repo": Path(repo_path).name, "branch": branch})
            return {"ok": False, "error": blocked}
        args = ["push", "-u", "origin", f"HEAD:refs/heads/{branch}"]
        if force:
            args.insert(1, "--force-with-lease")
        result = _run_git(repo_path, args)
        if result.returncode == 0:
            return {"ok": True}
        return {"ok": False, "error": result.stderr.strip()}

    def merge_base(self, repo_path, base_branch: str, prev_error: str | None = None) -> dict:
        _run_git(repo_path, ["fetch", "origin", base_branch])
        result = _run_git(repo_path, ["merge", f"origin/{base_branch}", "--no-edit"])
        if result.returncode == 0:
            return {"ok": True}
        return _resolve_merge_conflicts(repo_path, base_branch, prev_error=prev_error, merge_error=result.stderr)

    def sync_remote_branch(self, repo_path, branch: str) -> dict:
        fetched = _run_git(repo_path, ["fetch", "origin", branch])
        if fetched.returncode != 0:
            return {"ok": True}
        result = _run_git(repo_path, ["merge", f"origin/{branch}", "--no-edit"])
        if result.returncode == 0:
            return {"ok": True}
        return _resolve_merge_conflicts(repo_path, branch, merge_error=result.stderr)

    def create_pr(self, repo: str, repo_path, branch: str, title: str, body: str, base_branch: str) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests"
        payload = {
            "title": title,
            "description": body,
            "source": {"branch": {"name": branch}},
            "destination": {"branch": {"name": base_branch}},
        }
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.post(url, json=payload)
            if resp.status_code in (200, 201):
                data = resp.json()
                return {"url": data["links"]["html"]["href"], "id": data["id"]}
            return {"error": resp.text}

    def merge_pr(self, repo: str, pr_id: int) -> dict:
        strategy = self.config.get("pr", {}).get("merge_strategy", "squash")
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/merge"
        payload = {"merge_strategy": strategy}
        with external_log.client("bitbucket", auth=self._auth(), timeout=30) as client:
            resp = client.post(url, json=payload)
            if resp.status_code in (200, 201):
                return {"status": "merged"}
            return {"error": resp.text}

    def pr_url(self, repo: str, pr_id: int) -> str:
        return f"https://bitbucket.org/{self.org}/{repo}/pull-requests/{pr_id}"

    def _normalize_pr(self, pr: dict, repo: str) -> dict:
        return {
            "id": pr["id"],
            "repo": repo,
            "title": pr["title"],
            "author": pr["author"]["display_name"],
            "branch": pr["source"]["branch"]["name"],
            "base": pr["destination"]["branch"]["name"],
            "created_on": pr["created_on"],
            "updated_on": pr["updated_on"],
            "url": pr["links"]["html"]["href"],
            "head_sha": pr.get("source", {}).get("commit", {}).get("hash", ""),
        }


class GitHubPlatform(_CIMonitorMixin):

    def __init__(self, config: dict):
        self.config = config
        raw = config["github"]["repo"]
        self.repos = [raw] if isinstance(raw, str) else list(raw)
        self.repo = self.repos[0]
        self.base_branch = config["workspace"].get("base_branch", "main")
        self._me_login_cache: str | None = None
        self._gh_env_cache: dict | None = None

    def _gh_env(self) -> dict | None:
        """Child env carrying this instance's gh credentials, or None to
        inherit the active account.

        The active gh account is global state shared by every instance, so two
        instances owned by different accounts flip it back and forth and each
        flip costs one failed call first. A token in the child env scopes the
        credential to this instance and leaves the active account alone."""
        account = (self.config.get("github") or {}).get("account") or ""
        if not account:
            return None
        if self._gh_env_cache is None:
            from core.preflight import gh_token_for
            token = gh_token_for(account)
            if not token:
                log.emit("gh_account_token_missing",
                         f"gh has no token for configured account '{account}'; "
                         f"falling back to the active account",
                         meta={"account": account})
            self._gh_env_cache = {**os.environ, "GH_TOKEN": token} if token else {}
        return self._gh_env_cache or None

    def _run_gh(self, args: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["gh"] + args, capture_output=True, text=True, timeout=60,
            env=self._gh_env(),
        )

    def _graphql(self, query: str, **variables) -> dict | None:
        args = ["api", "graphql", "-f", f"query={query}"]
        for k, v in variables.items():
            flag = "-F" if isinstance(v, int) and not isinstance(v, bool) else "-f"
            args += [flag, f"{k}={v}"]
        result = self._run_gh(args)
        if result.returncode != 0:
            return None
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return None

    _repo_cache: dict[str, str] = {}

    def _resolve_repo(self, short_name: str) -> str:
        if "/" in short_name:
            return short_name
        if short_name in self._repo_cache:
            return self._repo_cache[short_name]
        org = self.repo.split("/")[0]
        full = f"{org}/{short_name}"
        self._repo_cache[short_name] = full
        return full

    def list_my_open_prs(self) -> list[dict]:
        prs = []
        for repo in self.repos:
            result = self._run_gh([
                "pr", "list", "--repo", repo, "--author", "@me",
                "--json", "number,title,author,headRefName,baseRefName,createdAt,updatedAt,url,state",
                "--limit", "50",
            ])
            if result.returncode != 0:
                continue
            prs.extend(self._normalize_pr(pr, repo) for pr in json.loads(result.stdout))
        return prs

    def list_open_prs(self) -> list[dict]:
        prs = []
        for repo in self.repos:
            result = self._run_gh([
                "pr", "list", "--repo", repo,
                "--json", "number,title,author,headRefName,baseRefName,createdAt,updatedAt,url,state",
                "--limit", "50",
            ])
            if result.returncode != 0:
                continue
            prs.extend(self._normalize_pr(pr, repo) for pr in json.loads(result.stdout))
        return prs

    def find_merged_pr_by_key(self, key: str) -> dict | None:
        if not key:
            return None
        candidates = []
        for repo in self.repos:
            result = self._run_gh([
                "pr", "list", "--repo", repo, "--state", "merged",
                "--search", f"{key} in:title",
                "--json", "number,title,author,headRefName,baseRefName,mergedAt,url",
                "--limit", "20",
            ])
            if result.returncode != 0:
                continue
            for pr in json.loads(result.stdout):
                title = pr.get("title", "")
                branch = pr.get("headRefName", "")
                matches = (
                    title.startswith(f"{key}:") or title.startswith(f"{key} ")
                    or branch == key or branch.startswith(f"{key}-") or branch.startswith(f"{key}/")
                )
                if not matches:
                    continue
                merged_at = pr.get("mergedAt") or ""
                if not merged_at:
                    continue
                candidates.append({
                    "id": pr.get("number"),
                    "repo": repo,
                    "title": title,
                    "branch": branch,
                    "base": pr.get("baseRefName", ""),
                    "author": (pr.get("author") or {}).get("login", ""),
                    "merged_at": merged_at,
                    "url": pr.get("url", ""),
                })
        if not candidates:
            return None
        candidates.sort(key=lambda p: p["merged_at"], reverse=True)
        return candidates[0]

    def _me_login(self) -> str:
        if self._me_login_cache is not None:
            return self._me_login_cache
        result = self._run_gh(["api", "user", "--jq", ".login"])
        self._me_login_cache = result.stdout.strip() if result.returncode == 0 else ""
        return self._me_login_cache

    def self_id(self) -> str:
        """The login frshty posts as, for the self-authored comment guard.

        Almost no instance sets github.user, and an empty id makes that guard
        match nothing, so frshty reads its own replies back as new reviewer
        comments. The authenticated gh account is the same account frshty
        posts with, so it is the correct fallback."""
        return (self.config.get("github") or {}).get("user", "") or self._me_login()

    def list_review_prs(self) -> list[dict]:
        result = self._run_gh([
            "search", "prs", "--review-requested=@me", "--state=open",
            "--json", "number,title,author,createdAt,updatedAt,url,repository",
            "--limit", "50",
        ])
        if result.returncode != 0:
            return []
        prs = [self._normalize_search_pr(pr) for pr in json.loads(result.stdout)]
        for pr in prs:
            if pr.get("full_repo"):
                self._repo_cache[pr["repo"]] = pr["full_repo"]
        if prs:
            fragments = []
            for i, pr in enumerate(prs):
                full = pr.get("full_repo") or self._resolve_repo(pr["repo"])
                owner, name = full.split("/", 1)
                fragments.append(f'pr{i}: repository(owner:"{owner}",name:"{name}") {{ pullRequest(number:{pr["id"]}) {{ headRefName headRefOid }} }}')
            query = "{ " + " ".join(fragments) + " }"
            gql = self._run_gh(["api", "graphql", "-f", f"query={query}"])
            if gql.returncode == 0:
                data = json.loads(gql.stdout).get("data", {})
                for i, pr in enumerate(prs):
                    node = data.get(f"pr{i}", {}).get("pullRequest", {})
                    if not node:
                        continue
                    if not pr.get("branch") and node.get("headRefName"):
                        pr["branch"] = node["headRefName"]
                    if node.get("headRefOid"):
                        pr["head_sha"] = node["headRefOid"]
        return prs

    def list_pending_reviews_for_me(self) -> list[dict]:
        me = self._me_login()
        out = []
        for pr in self.list_review_prs():
            if me and pr.get("author") == me:
                continue
            out.append(pr)
        return out

    _REVIEW_COMMENTS_QUERY = (
        "query($owner:String!,$name:String!,$number:Int!){"
        " repository(owner:$owner,name:$name){"
        " pullRequest(number:$number){"
        " reviews(first:100){nodes{"
        " databaseId body url submittedAt updatedAt state author{login}"
        "}}"
        " comments(first:100){nodes{"
        " databaseId body url createdAt updatedAt author{login}"
        "}}"
        " reviewThreads(first:100){nodes{"
        " id isResolved"
        " comments(first:100){nodes{"
        " databaseId body path line originalLine diffHunk url createdAt updatedAt"
        " author{login} replyTo{databaseId}"
        "}}}}}}}"
    )

    def _review_data(self, repo: str, pr_id: int) -> dict:
        full = self._resolve_repo(repo)
        owner, _, name = full.partition("/")
        data = self._graphql(self._REVIEW_COMMENTS_QUERY, owner=owner, name=name, number=pr_id)
        if not data:
            return {}
        return (((data.get("data") or {}).get("repository") or {}).get("pullRequest") or {})

    def _review_threads(self, repo: str, pr_id: int) -> list[dict]:
        pr = self._review_data(repo, pr_id)
        return (pr.get("reviewThreads") or {}).get("nodes") or []

    def get_pr_comments(self, repo: str, pr_id: int) -> list[dict]:
        out: list[dict] = []
        pr = self._review_data(repo, pr_id)
        for t in (pr.get("reviewThreads") or {}).get("nodes") or []:
            resolved = bool(t.get("isResolved"))
            thread_id = t.get("id", "")
            for c in (t.get("comments") or {}).get("nodes") or []:
                created = c.get("createdAt", "")
                out.append({
                    "id": c.get("databaseId"),
                    "body": c.get("body", ""),
                    "author_id": (c.get("author") or {}).get("login", ""),
                    "author_name": (c.get("author") or {}).get("login", ""),
                    "path": c.get("path"),
                    "line": c.get("line") if c.get("line") is not None else c.get("originalLine"),
                    "diff_hunk": c.get("diffHunk", ""),
                    "html_url": c.get("url", ""),
                    "created_on": created,
                    "created_at": created,
                    "updated_at": c.get("updatedAt", created),
                    "parent_id": (c.get("replyTo") or {}).get("databaseId"),
                    "resolved": resolved,
                    "resolvable": True,
                    "thread_id": thread_id,
                })
        # A review can carry both inline comments and a top-level body. GitHub
        # exposes only the inline comments as reviewThreads; omitting reviews
        # here silently drops general feedback submitted in the same review.
        # General review bodies cannot be resolved through
        # resolveReviewThread, so callers mark them processed after the code
        # is pushed instead of attempting an impossible mutation.
        for review in (pr.get("reviews") or {}).get("nodes") or []:
            body = review.get("body", "")
            if not body.strip():
                continue
            created = review.get("submittedAt", "")
            out.append({
                "id": review.get("databaseId"),
                "body": body,
                "author_id": (review.get("author") or {}).get("login", ""),
                "author_name": (review.get("author") or {}).get("login", ""),
                "path": None,
                "line": None,
                "diff_hunk": "",
                "html_url": review.get("url", ""),
                "created_on": created,
                "created_at": created,
                "updated_at": review.get("updatedAt", created),
                "parent_id": None,
                "resolved": review.get("state") == "DISMISSED",
                "resolvable": False,
                "thread_id": "",
                "comment_kind": "review_body",
            })
        # Bot reviewers (the Claude Code Review workflow) publish their whole
        # verdict as a plain PR comment, not as a review. Those live outside
        # both reviews and reviewThreads, so reading only those two sources
        # hides every finding such a reviewer reports. Issue comments cannot
        # be resolved through resolveReviewThread either.
        for c in (pr.get("comments") or {}).get("nodes") or []:
            body = c.get("body", "")
            if not body.strip():
                continue
            created = c.get("createdAt", "")
            out.append({
                "id": c.get("databaseId"),
                "body": body,
                "author_id": (c.get("author") or {}).get("login", ""),
                "author_name": (c.get("author") or {}).get("login", ""),
                "path": None,
                "line": None,
                "diff_hunk": "",
                "html_url": c.get("url", ""),
                "created_on": created,
                "created_at": created,
                "updated_at": c.get("updatedAt", created),
                "parent_id": None,
                "resolved": False,
                "resolvable": False,
                "thread_id": "",
                "comment_kind": "issue_comment",
            })
        return out

    def get_pr_diff(self, repo: str, pr_id: int) -> str | None:
        result = self._run_gh(["pr", "diff", str(pr_id), "--repo", self._resolve_repo(repo)])
        if result.returncode != 0:
            return None
        return result.stdout

    def get_pr_checks(self, repo: str, pr_id: int) -> list[dict] | None:
        """Returns the check list, [] when the PR genuinely has no checks, or
        None when the fetch itself failed (auth/rate-limit/network). Callers
        must treat None as "unknown - retry", never as "no CI -> passed". With
        --json, gh exits 0 and emits the JSON for pass/fail/pending alike; a
        missing-checks PR exits non-zero with empty stdout and a "no checks
        reported" stderr, while a real error has empty stdout and other stderr."""
        full = self._resolve_repo(repo)
        result = self._run_gh([
            "pr", "checks", str(pr_id), "--repo", full,
            "--json", "name,state,link",
        ])
        out = result.stdout.strip()
        if out:
            try:
                data = json.loads(out)
            except json.JSONDecodeError:
                return None
            return [
                {"name": c["name"], "state": c["state"], "url": c.get("link", "")}
                for c in data
            ]
        if "no checks reported" in (result.stderr or "").lower():
            return []
        if result.returncode == 0:
            return []
        return None

    def get_failed_logs(self, repo: str, pr_id: int) -> str:
        full = self._resolve_repo(repo)
        checks = self.get_pr_checks(repo, pr_id) or []
        run_ids = set()
        for c in checks:
            if c["state"] == "FAILURE" and c.get("url"):
                parts = c["url"].split("/runs/")
                if len(parts) > 1:
                    run_id = parts[1].split("/")[0]
                    run_ids.add(run_id)
        logs = []
        for run_id in run_ids:
            result = self._run_gh(["run", "view", run_id, "--repo", full, "--log-failed"])
            if result.returncode == 0 and result.stdout:
                logs.append(result.stdout[:3000])
        return "\n".join(logs)[:6000]

    def get_pr_state(self, repo: str, pr_id: int) -> str:
        info = self.get_pr_info(repo, pr_id)
        return info["state"]

    def get_pr_info(self, repo: str, pr_id: int) -> dict:
        full = self._resolve_repo(repo)
        result = self._run_gh([
            "pr", "view", str(pr_id), "--repo", full,
            "--json", "state,updatedAt,mergeable,author,latestReviews,headRefOid,assignees,reviewRequests",
        ])
        if result.returncode != 0:
            return {"state": "OPEN", "updated_on": "", "mergeable": "UNKNOWN"}
        data = json.loads(result.stdout)
        approvers = [
            r.get("author", {}).get("login", "")
            for r in (data.get("latestReviews") or [])
            if r.get("state") == "APPROVED"
        ]
        reviewers = [
            a.get("login") or a.get("name", "")
            for a in (data.get("assignees") or []) + (data.get("reviewRequests") or [])
        ]
        return {
            "state": data.get("state", "OPEN"),
            "updated_on": data.get("updatedAt", ""),
            "author": data.get("author", {}).get("login", ""),
            "author_id": data.get("author", {}).get("login", ""),
            "mergeable": data.get("mergeable", "UNKNOWN"),
            "approvers": approvers,
            "reviewers": [x for x in reviewers if x],
            "head_sha": data.get("headRefOid", ""),
        }

    def post_pr_comment(self, repo: str, pr_id: int, body: str, path: str | None = None, line: int | None = None, parent_id: int | None = None) -> dict:
        full = self._resolve_repo(repo)
        if parent_id:
            result = self._run_gh([
                "api", f"repos/{full}/pulls/{pr_id}/comments",
                "-f", f"body={body}", "-F", f"in_reply_to={parent_id}",
            ])
        elif path and line:
            head_result = self._run_gh([
                "pr", "view", str(pr_id), "--repo", full,
                "--json", "headRefOid", "-q", ".headRefOid",
            ])
            commit_id = head_result.stdout.strip() if head_result.returncode == 0 else ""
            result = self._run_gh([
                "api", f"repos/{full}/pulls/{pr_id}/comments",
                "-f", f"body={body}", "-f", f"path={path}", "-F", f"line={line}",
                "-f", f"commit_id={commit_id}",
            ])
        else:
            result = self._run_gh([
                "pr", "comment", str(pr_id), "--repo", full, "--body", body,
            ])
        if result.returncode == 0:
            return {"status": "posted"}
        return {"status": "error", "detail": result.stderr}

    def edit_pr_comment(self, repo: str, pr_id: int, comment_id: int, body: str) -> dict:
        full = self._resolve_repo(repo)
        result = self._run_gh([
            "api", "-X", "PATCH", f"repos/{full}/pulls/comments/{comment_id}",
            "-f", f"body={body}",
        ])
        if result.returncode == 0:
            return {"status": "updated"}
        return {"status": "error", "detail": result.stderr}

    def resolve_comment(self, repo: str, pr_id: int, comment_id: int) -> dict:
        thread_id = ""
        for t in self._review_threads(repo, pr_id):
            for c in (t.get("comments") or {}).get("nodes") or []:
                if c.get("databaseId") == comment_id:
                    thread_id = t.get("id", "")
                    break
            if thread_id:
                break
        if not thread_id:
            return {"status": "error", "detail": f"no review thread for comment {comment_id}"}
        mutation = (
            "mutation($threadId:ID!){"
            " resolveReviewThread(input:{threadId:$threadId}){thread{isResolved}}}"
        )
        result = self._run_gh([
            "api", "graphql", "-f", f"query={mutation}", "-f", f"threadId={thread_id}",
        ])
        if result.returncode == 0:
            return {"status": "resolved"}
        return {"status": "error", "detail": result.stderr}

    def get_pr_branch(self, repo: str, pr_id: int) -> str:
        full = self._resolve_repo(repo)
        result = self._run_gh(["pr", "view", str(pr_id), "--repo", full, "--json", "headRefName", "-q", ".headRefName"])
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        return ""

    def ensure_pr_worktree(self, repo: str, pr_id: int, target: Path) -> bool:
        full = self._resolve_repo(repo)
        if (target / ".git").exists():
            _run_git(target, ["fetch", "origin"])
            _run_git(target, ["checkout", f"pr-{pr_id}"])
            return True
        target.mkdir(parents=True, exist_ok=True)
        result = self._run_gh(["repo", "clone", full, str(target), "--", "--depth=1"])
        if result.returncode != 0:
            return False
        subprocess.run(
            ["gh", "pr", "checkout", str(pr_id), "--repo", full],
            cwd=str(target), capture_output=True, text=True, timeout=60,
        )
        return True

    def push_branch(self, repo_path, branch: str, force: bool = False) -> dict:
        if not branch.strip():
            return {"ok": False, "error": "empty branch name"}
        blocked = _identity_block_reason(self.config, repo_path, branch)
        if blocked:
            log.emit("git_identity_push_blocked", blocked,
                     meta={"repo": Path(repo_path).name, "branch": branch})
            return {"ok": False, "error": blocked}
        args = ["push", "-u", "origin", f"HEAD:refs/heads/{branch}"]
        if force:
            args.insert(1, "--force-with-lease")
        result = _run_git(repo_path, args)
        if result.returncode == 0:
            return {"ok": True}
        return {"ok": False, "error": result.stderr.strip()}

    def merge_base(self, repo_path, base_branch: str, prev_error: str | None = None) -> dict:
        _run_git(repo_path, ["fetch", "origin", base_branch])
        result = _run_git(repo_path, ["merge", f"origin/{base_branch}", "--no-edit"])
        if result.returncode == 0:
            return {"ok": True}
        return _resolve_merge_conflicts(repo_path, base_branch, prev_error=prev_error, merge_error=result.stderr)

    def sync_remote_branch(self, repo_path, branch: str) -> dict:
        fetched = _run_git(repo_path, ["fetch", "origin", branch])
        if fetched.returncode != 0:
            return {"ok": True}
        result = _run_git(repo_path, ["merge", f"origin/{branch}", "--no-edit"])
        if result.returncode == 0:
            return {"ok": True}
        return _resolve_merge_conflicts(repo_path, branch, merge_error=result.stderr)

    def create_pr(self, repo: str, repo_path, branch: str, title: str, body: str, base_branch: str) -> dict:
        full = self._resolve_repo(repo)
        gh_args = [
            "pr", "create", "--repo", full,
            "--base", base_branch, "--head", branch,
            "--title", title, "--body", body,
        ]
        result = self._run_gh(gh_args)
        if result.returncode != 0:
            err = result.stderr.strip()
            if self._is_repo_not_visible_error(err):
                if self._try_recover_gh_auth(full, err):
                    result = self._run_gh(gh_args)
                    if result.returncode == 0:
                        url = result.stdout.strip()
                        m = re.search(r"/pull/(\d+)", url)
                        return {"url": url, "id": int(m.group(1)) if m else None}
                    err = result.stderr.strip()
            return {"error": err}
        url = result.stdout.strip()
        m = re.search(r"/pull/(\d+)", url)
        pr_id = int(m.group(1)) if m else None
        return {"url": url, "id": pr_id}

    @staticmethod
    def _is_repo_not_visible_error(stderr: str) -> bool:
        return ("Could not resolve to a Repository" in stderr
                or "Not Found" in stderr
                or "HTTP 404" in stderr)

    def _try_recover_gh_auth(self, full_repo: str, original_err: str) -> bool:
        """When gh can't see the repo, check whether one of the other logged-in
        accounts can. The target is github.account when the instance names one,
        otherwise the repo owner; if it is logged in, switch to it and let the
        caller retry. Emits gh_auth_mismatch + (on attempt) gh_auth_switched
        events for observability."""
        from core.preflight import (
            gh_active_account, gh_logged_in_accounts, gh_switch_to,
            gh_repo_push_ok,
        )
        owner = full_repo.split("/", 1)[0] if "/" in full_repo else full_repo
        configured = (self.config.get("github") or {}).get("account") or ""
        if configured:
            log.emit("gh_repo_not_visible",
                     f"gh can't access {full_repo} as configured account "
                     f"'{configured}'. The account's token is already used for "
                     f"every call, so switching the active account would not "
                     f"help; check the account's access to the repo.",
                     meta={"repo": full_repo, "account": configured,
                           "stderr": original_err[:300]})
            return False
        target = owner
        active = gh_active_account() or "<none>"
        accounts = gh_logged_in_accounts()
        log.emit("gh_auth_mismatch",
                 f"gh can't access {full_repo} as active account '{active}'. "
                 f"Logged-in accounts: {accounts}. "
                 f"Fix: gh auth switch -u {target}",
                 meta={"repo": full_repo, "active": active, "target": target,
                       "logged_in": accounts, "stderr": original_err[:300]})
        if target in accounts and target != active:
            ok, msg = gh_switch_to(target)
            if not ok:
                log.emit("gh_auth_switch_failed",
                         f"auto-switch to '{target}' failed: {msg}",
                         meta={"target": target, "error": msg})
                return False
            new_active = gh_active_account() or "<none>"
            push_ok, push_reason = gh_repo_push_ok(full_repo)
            log.emit("gh_auth_switched",
                     f"auto-switched gh active to '{new_active}' for {full_repo} "
                     f"(push perm: {push_ok}, {push_reason})",
                     meta={"from": active, "to": new_active,
                           "repo": full_repo, "push_ok": push_ok})
            return push_ok
        return False

    def merge_pr(self, repo: str, pr_id: int) -> dict:
        pr_config = self.config.get("pr", {})
        strategy = pr_config.get("merge_strategy", "squash")
        flags = pr_config.get("merge_flags", [])
        args = ["pr", "merge", str(pr_id), "--repo", self._resolve_repo(repo), f"--{strategy}"]
        args.extend(flags)
        result = self._run_gh(args)
        if result.returncode != 0:
            return {"error": result.stderr.strip()}
        return {"status": "merged"}

    def pr_url(self, repo: str, pr_id: int) -> str:
        return f"https://github.com/{self._resolve_repo(repo)}/pull/{pr_id}"

    def _normalize_pr(self, pr: dict, repo: str | None = None) -> dict:
        repo_full = repo or self.repo
        return {
            "id": pr["number"],
            "repo": repo_full.split("/")[-1],
            "title": pr["title"],
            "author": pr["author"]["login"],
            "branch": pr["headRefName"],
            "base": pr["baseRefName"],
            "created_on": pr["createdAt"],
            "updated_on": pr["updatedAt"],
            "url": pr["url"],
        }

    def _normalize_search_pr(self, pr: dict) -> dict:
        repo_full = pr.get("repository", {}).get("nameWithOwner", self.repo)
        return {
            "id": pr["number"],
            "repo": repo_full.split("/")[-1],
            "full_repo": repo_full,
            "title": pr["title"],
            "author": pr["author"]["login"],
            "branch": "",
            "base": "",
            "created_on": pr["createdAt"],
            "updated_on": pr["updatedAt"],
            "url": pr["url"],
            "head_sha": "",
        }
