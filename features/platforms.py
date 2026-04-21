import json
import re
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx

import core.log as log
from core.config import resolve_env, get_repos
from core.claude_runner import run_haiku


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


def _resolve_merge_conflicts(repo_path, base_branch: str) -> dict:
    conflicted = _run_git(repo_path, ["diff", "--name-only", "--diff-filter=U"])
    if conflicted.returncode != 0 or not conflicted.stdout.strip():
        _run_git(repo_path, ["merge", "--abort"])
        return {"ok": False, "error": "no conflicted files found"}

    files = conflicted.stdout.strip().split("\n")
    for filepath in files:
        full_path = Path(repo_path) / filepath
        if not full_path.exists():
            _run_git(repo_path, ["merge", "--abort"])
            return {"ok": False, "error": f"conflicted file not found: {filepath}"}

        content = full_path.read_text()
        resolved = run_haiku(
            f"This file has git merge conflicts. Resolve them by picking the correct code. "
            f"Output ONLY the resolved file contents, no explanation, no markdown fences.\n\n{content}",
            timeout=120,
        )
        if not resolved or "<<<<<<<" in resolved or ">>>>>>>" in resolved:
            _run_git(repo_path, ["merge", "--abort"])
            return {"ok": False, "error": f"failed to resolve conflicts in {filepath}"}

        full_path.write_text(resolved)
        _run_git(repo_path, ["add", filepath])

    result = _run_git(repo_path, ["commit", "--no-edit"])
    if result.returncode != 0:
        _run_git(repo_path, ["merge", "--abort"])
        return {"ok": False, "error": result.stderr.strip()}

    return {"ok": True}


class BitbucketPlatform:
    BASE_URL = "https://api.bitbucket.org/2.0"

    def __init__(self, config: dict):
        self.config = config
        bb = config["bitbucket"]
        self.org = bb["org"]
        self.user = resolve_env(config, "bitbucket", "user_env")
        self.token = resolve_env(config, "bitbucket", "token_env")
        self.user_account_id = bb.get("user_account_id", "")
        self.repos = [r["name"] for r in get_repos(config)]

    def _auth(self):
        return (self.user, self.token)

    def list_my_open_prs(self) -> list[dict]:
        results = []
        with httpx.Client(auth=self._auth(), timeout=30) as client:
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
        results = []
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            for repo in self.repos:
                url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests?state=OPEN&pagelen=50"
                resp = client.get(url)
                if resp.status_code != 200:
                    continue
                for pr in resp.json().get("values", []):
                    author_id = pr.get("author", {}).get("account_id", "")
                    if author_id == self.user_account_id:
                        continue
                    results.append(self._normalize_pr(pr, repo))
        return results

    def get_pr_comments(self, repo: str, pr_id: int) -> list[dict]:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments?pagelen=100"
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return []
            return [
                {
                    "id": c["id"],
                    "body": c["content"]["raw"],
                    "author_id": c.get("user", {}).get("account_id", ""),
                    "author_name": c.get("user", {}).get("display_name", ""),
                    "path": c.get("inline", {}).get("path"),
                    "line": c.get("inline", {}).get("to"),
                    "created_on": c["created_on"],
                    "parent_id": c.get("parent", {}).get("id") if c.get("parent") else None,
                }
                for c in resp.json().get("values", [])
            ]

    def get_pr_diff(self, repo: str, pr_id: int) -> str | None:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/diff"
        with httpx.Client(auth=self._auth(), timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            return resp.text

    def get_pr_checks(self, repo: str, pr_id: int) -> list[dict]:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/statuses?pagelen=50"
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return []
            return [
                {
                    "name": s.get("name", ""),
                    "state": s.get("state", "").upper().replace("SUCCESSFUL", "SUCCESS"),
                    "url": s.get("url", ""),
                }
                for s in resp.json().get("values", [])
            ]

    def get_failed_logs(self, repo: str, pr_id: int) -> str:
        return ""

    def get_pr_state(self, repo: str, pr_id: int) -> str:
        info = self.get_pr_info(repo, pr_id)
        return info["state"]

    def get_pr_info(self, repo: str, pr_id: int) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}"
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return {"state": "OPEN", "updated_on": "", "mergeable": "UNKNOWN"}
            data = resp.json()
            approvers = [
                p.get("user", {}).get("account_id", "")
                for p in (data.get("participants") or [])
                if p.get("approved")
            ]
            return {
                "state": data.get("state", "OPEN"),
                "updated_on": data.get("updated_on", ""),
                "title": data.get("title", ""),
                "description": (data.get("description", "") or ""),
                "author": data.get("author", {}).get("display_name", ""),
                "mergeable": "CONFLICTING" if data.get("has_conflicts") else "MERGEABLE",
                "approvers": approvers,
            }

    def post_pr_comment(self, repo: str, pr_id: int, body: str, path: str | None = None, line: int | None = None, parent_id: int | None = None) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments"
        payload = {"content": {"raw": body}}
        if path and line:
            payload["inline"] = {"path": path, "to": line}
        if parent_id:
            payload["parent"] = {"id": parent_id}
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.post(url, json=payload)
            if resp.status_code in (200, 201):
                return {"status": "posted", "id": resp.json().get("id")}
            return {"status": "error", "detail": resp.text}

    def edit_pr_comment(self, repo: str, pr_id: int, comment_id: int, body: str) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments/{comment_id}"
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.put(url, json={"content": {"raw": body}})
            if resp.status_code in (200, 201):
                return {"status": "updated"}
            return {"status": "error", "detail": resp.text}

    def resolve_comment(self, repo: str, pr_id: int, comment_id: int) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/comments/{comment_id}"
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.put(url, json={"resolution": {"type": "RESOLVED"}})
            if resp.status_code in (200, 201):
                return {"status": "resolved"}
            return {"status": "error", "detail": resp.text}

    def get_pr_branch(self, repo: str, pr_id: int) -> str:
        with httpx.Client(auth=self._auth(), timeout=30) as client:
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
        with httpx.Client(auth=self._auth(), timeout=30) as client:
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
        args = ["push", "-u", "origin", branch]
        if force:
            args.insert(1, "--force-with-lease")
        result = _run_git(repo_path, args)
        if result.returncode == 0:
            return {"ok": True}
        return {"ok": False, "error": result.stderr.strip()}

    def merge_base(self, repo_path, base_branch: str) -> dict:
        _run_git(repo_path, ["fetch", "origin", base_branch])
        result = _run_git(repo_path, ["merge", f"origin/{base_branch}", "--no-edit"])
        if result.returncode == 0:
            return {"ok": True}
        return _resolve_merge_conflicts(repo_path, base_branch)

    def create_pr(self, repo: str, repo_path, branch: str, title: str, body: str, base_branch: str) -> dict:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests"
        payload = {
            "title": title,
            "description": body,
            "source": {"branch": {"name": branch}},
            "destination": {"branch": {"name": base_branch}},
        }
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.post(url, json=payload)
            if resp.status_code in (200, 201):
                data = resp.json()
                return {"url": data["links"]["html"]["href"], "id": data["id"]}
            return {"error": resp.text}

    def monitor_ci(self, ticket, ts, base_url) -> dict:
        ts["ci_passed"] = True
        return ts

    def merge_pr(self, repo: str, pr_id: int) -> dict:
        strategy = self.config.get("pr", {}).get("merge_strategy", "squash")
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}/merge"
        payload = {"merge_strategy": strategy}
        with httpx.Client(auth=self._auth(), timeout=30) as client:
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
        }


class GitHubPlatform:

    def __init__(self, config: dict):
        self.config = config
        raw = config["github"]["repo"]
        self.repos = [raw] if isinstance(raw, str) else list(raw)
        self.repo = self.repos[0]
        self.base_branch = config["workspace"].get("base_branch", "main")

    def _run_gh(self, args: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["gh"] + args, capture_output=True, text=True, timeout=60,
        )

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

    def list_review_prs(self) -> list[dict]:
        result = self._run_gh([
            "search", "prs", "--review-requested=@me", "--state=open",
            "--json", "number,title,author,createdAt,updatedAt,url,repository",
            "--limit", "50",
        ])
        if result.returncode != 0:
            return []
        prs = [self._normalize_search_pr(pr) for pr in json.loads(result.stdout)]
        needs_branch = []
        for pr in prs:
            if pr.get("full_repo"):
                self._repo_cache[pr["repo"]] = pr["full_repo"]
            if not pr.get("branch"):
                needs_branch.append(pr)
        if needs_branch:
            fragments = []
            for i, pr in enumerate(needs_branch):
                full = pr.get("full_repo") or self._resolve_repo(pr["repo"])
                owner, name = full.split("/", 1)
                fragments.append(f'pr{i}: repository(owner:"{owner}",name:"{name}") {{ pullRequest(number:{pr["id"]}) {{ headRefName }} }}')
            query = "{ " + " ".join(fragments) + " }"
            gql = self._run_gh(["api", "graphql", "-f", f"query={query}"])
            if gql.returncode == 0:
                data = json.loads(gql.stdout).get("data", {})
                for i, pr in enumerate(needs_branch):
                    node = data.get(f"pr{i}", {}).get("pullRequest", {})
                    if node and node.get("headRefName"):
                        pr["branch"] = node["headRefName"]
        return prs

    def get_pr_comments(self, repo: str, pr_id: int) -> list[dict]:
        full = self._resolve_repo(repo)
        result = self._run_gh([
            "api", f"repos/{full}/pulls/{pr_id}/comments",
            "--jq", ".",
        ])
        if result.returncode != 0:
            return []
        comments = json.loads(result.stdout)
        return [
            {
                "id": c["id"],
                "body": c["body"],
                "author_id": c.get("user", {}).get("login", ""),
                "author_name": c.get("user", {}).get("login", ""),
                "path": c.get("path"),
                "line": c.get("line"),
                "created_on": c.get("created_at", ""),
                "parent_id": c.get("in_reply_to_id"),
            }
            for c in comments
        ]

    def get_pr_diff(self, repo: str, pr_id: int) -> str | None:
        result = self._run_gh(["pr", "diff", str(pr_id), "--repo", self._resolve_repo(repo)])
        if result.returncode != 0:
            return None
        return result.stdout

    def get_pr_checks(self, repo: str, pr_id: int) -> list[dict]:
        full = self._resolve_repo(repo)
        result = self._run_gh([
            "pr", "checks", str(pr_id), "--repo", full,
            "--json", "name,state,link",
        ])
        if result.returncode != 0:
            return []
        return [
            {"name": c["name"], "state": c["state"], "url": c.get("link", "")}
            for c in json.loads(result.stdout)
        ]

    def get_failed_logs(self, repo: str, pr_id: int) -> str:
        full = self._resolve_repo(repo)
        checks = self.get_pr_checks(repo, pr_id)
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
            "--json", "state,updatedAt,mergeable,author,latestReviews",
        ])
        if result.returncode != 0:
            return {"state": "OPEN", "updated_on": "", "mergeable": "UNKNOWN"}
        data = json.loads(result.stdout)
        approvers = [
            r.get("author", {}).get("login", "")
            for r in (data.get("latestReviews") or [])
            if r.get("state") == "APPROVED"
        ]
        return {
            "state": data.get("state", "OPEN"),
            "updated_on": data.get("updatedAt", ""),
            "author": data.get("author", {}).get("login", ""),
            "mergeable": data.get("mergeable", "UNKNOWN"),
            "approvers": approvers,
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
        result = self._run_gh([
            "api", "graphql", "-f", f'query=mutation {{ minimizeComment(input: {{subjectId: "{comment_id}", classifier: RESOLVED}}) {{ minimizedComment {{ isMinimized }} }} }}',
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
        args = ["push", "-u", "origin", branch]
        if force:
            args.insert(1, "--force-with-lease")
        result = _run_git(repo_path, args)
        if result.returncode == 0:
            return {"ok": True}
        return {"ok": False, "error": result.stderr.strip()}

    def merge_base(self, repo_path, base_branch: str) -> dict:
        _run_git(repo_path, ["fetch", "origin", base_branch])
        result = _run_git(repo_path, ["merge", f"origin/{base_branch}", "--no-edit"])
        if result.returncode == 0:
            return {"ok": True}
        return _resolve_merge_conflicts(repo_path, base_branch)

    def create_pr(self, repo: str, repo_path, branch: str, title: str, body: str, base_branch: str) -> dict:
        full = self._resolve_repo(repo)
        result = self._run_gh([
            "pr", "create", "--repo", full,
            "--base", base_branch, "--head", branch,
            "--title", title, "--body", body,
        ])
        if result.returncode != 0:
            return {"error": result.stderr.strip()}
        url = result.stdout.strip()
        m = re.search(r"/pull/(\d+)", url)
        pr_id = int(m.group(1)) if m else None
        return {"url": url, "id": pr_id}

    CI_TIMEOUT_SECS = 3600

    def monitor_ci(self, ticket, ts, base_url) -> dict:
        prs = ts.get("prs", [])
        if not prs:
            return ts

        if not ts.get("checks_started_at"):
            ts["checks_started_at"] = datetime.now(timezone.utc).isoformat()

        all_passed = True
        for pr in prs:
            checks = self.get_pr_checks(pr["repo"], pr["id"])
            verdict = self._evaluate_checks(checks)

            if verdict == "pending":
                elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(ts["checks_started_at"])).total_seconds()
                if elapsed > self.CI_TIMEOUT_SECS:
                    log.emit("ticket_checks_timeout", f"CI checks timed out for {ticket['key']} PR #{pr['id']} after {int(elapsed/60)}m",
                        links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                        meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})
                    return ts
                all_passed = False
                continue

            if verdict == "failed":
                return {"_ci_failed": True, "pr": pr, "checks": checks}

        if not all_passed:
            return ts

        log.emit("ticket_checks_passed", f"All CI checks passed for {ticket['key']}",
            links={"detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts["ci_passed"] = True
        ts.pop("checks_started_at", None)
        ts.pop("ci_fix_attempts", None)
        return ts

    def _evaluate_checks(self, checks: list[dict]) -> str:
        if not checks:
            return "pending"
        states = {c["state"].upper() for c in checks}
        if "FAILURE" in states or "FAILED" in states:
            return "failed"
        if states <= {"SUCCESS"}:
            return "passed"
        return "pending"

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
        }
