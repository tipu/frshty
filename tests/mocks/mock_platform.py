"""In-memory GitHub/Bitbucket implementation for testing."""


class MockGitHubPlatform:
    """In-memory implementation of GitHub/Bitbucket platform."""

    def __init__(self, config: dict):
        self.config = config
        self.repos = {}  # repo_name -> {"branches": {}, "prs": {}, "comments": []}
        self.prs = {}  # pr_id -> PR object
        self.pr_counter = 0
        self.checks = {}  # pr_id -> {"status": "in_progress", "conclusion": ""}
        self.comments = {}  # pr_id -> [comment objects]
        self.merged_prs = set()

    async def create_repo(self, name: str) -> str:
        """Create a repo and return its URL."""
        self.repos[name] = {
            "branches": {"main": "abc123"},
            "prs": {},
            "checks": {},
        }
        return f"https://github.com/test/{name}"

    async def create_branch(self, repo: str, head: str, base: str = "main") -> None:
        """Create a branch."""
        if repo not in self.repos:
            self.repos[repo] = {"branches": {}, "prs": {}, "checks": {}}
        self.repos[repo]["branches"][head] = "initial-commit-hash"

    async def create_pr(self, repo: str, title: str, body: str, head: str, base: str) -> int:
        """Create a PR and return its number."""
        if repo not in self.repos:
            await self.create_repo(repo)

        self.pr_counter += 1
        pr_id = self.pr_counter
        self.prs[pr_id] = {
            "id": pr_id,
            "repo": repo,
            "title": title,
            "body": body,
            "head": head,
            "base": base,
            "commits": ["initial-commit"],
            "status": "open",
            "merged": False,
        }
        self.checks[pr_id] = {
            "status": "in_progress",
            "conclusion": "",
        }
        self.comments[pr_id] = []
        return pr_id

    async def get_pr_info(self, pr_num: int, **kwargs) -> dict:
        """Get PR info."""
        if pr_num not in self.prs:
            return {}
        pr = self.prs[pr_num]
        return {
            "id": pr["id"],
            "title": pr["title"],
            "status": "merged" if pr["merged"] else "open",
            "commits": pr["commits"],
            "head": pr["head"],
            "base": pr["base"],
        }

    async def push_branch(self, branch: str, **kwargs) -> None:
        """Push commits to branch (update PR)."""
        # Find PR with this head branch and update commits
        for pr_id, pr in self.prs.items():
            if pr["head"] == branch:
                pr["commits"] = pr.get("commits", []) + ["new-commit"]
                break

    async def get_pr_checks(self, pr_num: int, **kwargs) -> list[dict]:
        """Get CI check status for PR."""
        if pr_num not in self.checks:
            return []
        check = self.checks[pr_num]
        return [
            {
                "name": "CI",
                "status": check["status"],
                "conclusion": check["conclusion"],
            }
        ]

    async def add_comment(self, pr_num: int, comment: str, **kwargs) -> None:
        """Add a review comment to PR."""
        if pr_num not in self.comments:
            self.comments[pr_num] = []
        self.comments[pr_num].append({
            "body": comment,
            "author": "reviewer",
        })

    async def list_comments(self, pr_num: int, **kwargs) -> list[dict]:
        """List all comments on PR."""
        return self.comments.get(pr_num, [])

    def merge_pr(self, pr_num: int, **kwargs) -> dict:
        """Merge a PR."""
        if pr_num in self.prs:
            self.prs[pr_num]["merged"] = True
            self.merged_prs.add(pr_num)
            return {"status": "merged"}
        return {"error": "PR not found"}

    async def get_pr_diff(self, pr_num: int, **kwargs) -> str:
        """Get PR diff."""
        if pr_num in self.prs:
            return f"diff for PR {pr_num}"
        return ""

    async def post_pr_comment(self, pr_num: int, comment: str, **kwargs) -> None:
        """Post a comment to PR."""
        await self.add_comment(pr_num, comment)

    async def resolve_comment(self, pr_num: int, comment_id: str, **kwargs) -> None:
        """Resolve/dismiss a comment."""
        pass

    def inject_check_failure(self, pr_num: int, conclusion: str = "failure") -> None:
        """Test helper: inject CI failure."""
        if pr_num in self.checks:
            self.checks[pr_num]["status"] = "completed"
            self.checks[pr_num]["conclusion"] = conclusion

    def inject_check_success(self, pr_num: int) -> None:
        """Test helper: inject CI success."""
        if pr_num in self.checks:
            self.checks[pr_num]["status"] = "completed"
            self.checks[pr_num]["conclusion"] = "success"

    # Methods that may be called by frshty
    def list_review_prs(self) -> list[dict]:
        """List PRs for review."""
        return list(self.prs.values())

    def list_my_open_prs(self) -> list[dict]:
        """List my open PRs."""
        return [pr for pr in self.prs.values() if not pr.get("merged")]

    def get_failed_logs(self, pr_num: int, **kwargs) -> str:
        """Get CI failure logs."""
        return "mock failure logs"

    def merge_base(self, base: str, head: str, **kwargs) -> str:
        """Get merge base."""
        return "base-sha"

    def monitor_ci(self, **kwargs) -> dict:
        """Monitor CI status (legacy)."""
        return {"status": "complete"}
