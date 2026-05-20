import json
import re
from pathlib import Path


def find_review(state_dir: Path, repo: str, pr_id: int):
    """Return (branch_dir, comments, worktree_or_None) or None if not found.

    Worktree is returned when a `.git` exists under branch_dir/worktree, else None.
    """
    reviews_dir = state_dir / "reviews" / repo
    if not reviews_dir.exists():
        return None
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        try:
            comments = json.loads(queued.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        matched = bool(comments) and comments[0].get("pr_id") == pr_id
        if not matched:
            review_json = branch_dir / "review.json"
            if review_json.exists():
                try:
                    matched = json.loads(review_json.read_text()).get("pr_id") == pr_id
                except (json.JSONDecodeError, OSError):
                    matched = False
        if matched:
            wt = branch_dir / "worktree"
            worktree = wt if (wt / ".git").exists() else None
            return branch_dir, comments, worktree
    return None


def populate_repo_cache(platform, state_dir: Path, repo: str) -> None:
    reviews_dir = state_dir / "reviews" / repo
    if not reviews_dir.exists():
        return
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if not comments:
            continue
        pr_url = comments[0].get("pr_url", "")
        m = re.match(r"https?://github\.com/([^/]+/[^/]+)/pull/", pr_url)
        if m:
            platform._repo_cache[repo] = m.group(1)
            return


def extract_code_context(diff_text: str, target_file: str, target_line: int, context_lines: int = 5):
    lines = diff_text.split("\n")
    in_file = False
    current_line_num = 0
    context: list[str] = []

    for line in lines:
        if line.startswith(f"+++ b/{target_file}"):
            in_file = True
            continue
        if in_file and line.startswith("@@"):
            m = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)", line)
            if m:
                current_line_num = int(m.group(1))
            continue
        if in_file and line.startswith("diff --git"):
            break
        if not in_file:
            continue

        if line.startswith(("-", "+")):
            content = line[1:]
        elif line.startswith(" "):
            content = line[1:]
        else:
            continue

        if abs(current_line_num - target_line) <= context_lines:
            prefix = "+ " if line.startswith("+") else "  "
            context.append(f"{current_line_num:4d} {prefix}{content}")

        if not line.startswith("-"):
            current_line_num += 1

        if current_line_num > target_line + context_lines:
            break

    return "\n".join(context) if context else None
