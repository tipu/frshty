import json
import re
import subprocess
from datetime import datetime, timezone, timedelta

import httpx

from core.config import resolve_env, get_repos


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
                    "state": s.get("state", ""),
                    "url": s.get("url", ""),
                }
                for s in resp.json().get("values", [])
            ]

    def get_pr_state(self, repo: str, pr_id: int) -> str:
        url = f"{self.BASE_URL}/repositories/{self.org}/{repo}/pullrequests/{pr_id}"
        with httpx.Client(auth=self._auth(), timeout=30) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return "OPEN"
            return resp.json().get("state", "OPEN")

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

    def push_branch(self, repo_path, branch: str) -> bool:
        result = _run_git(repo_path, ["push", "-u", "origin", branch])
        return result.returncode == 0

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
        self.repo = config["github"]["repo"]
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
        result = self._run_gh([
            "pr", "list", "--repo", self.repo, "--author", "@me",
            "--json", "number,title,author,headRefName,baseRefName,createdAt,updatedAt,url,state",
            "--limit", "50",
        ])
        if result.returncode != 0:
            return []
        return [self._normalize_pr(pr) for pr in json.loads(result.stdout)]

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
            "--json", "name,state,detailsUrl",
        ])
        if result.returncode != 0:
            return []
        return [
            {"name": c["name"], "state": c["state"], "url": c.get("detailsUrl", "")}
            for c in json.loads(result.stdout)
        ]

    def get_pr_state(self, repo: str, pr_id: int) -> str:
        full = self._resolve_repo(repo)
        result = self._run_gh([
            "pr", "view", str(pr_id), "--repo", full,
            "--json", "state", "-q", ".state",
        ])
        if result.returncode != 0:
            return "OPEN"
        return result.stdout.strip()

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

    def push_branch(self, repo_path, branch: str) -> bool:
        result = _run_git(repo_path, ["push", "-u", "origin", branch])
        return result.returncode == 0

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

    def _normalize_pr(self, pr: dict) -> dict:
        return {
            "id": pr["number"],
            "repo": self.repo.split("/")[-1],
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
