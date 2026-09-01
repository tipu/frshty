"""The verifier must see committed work, and a failed commit must not leave a PASS.

Two failures came out of the old order (fix, verify, commit):

DEV-635 — the verify step wrote "VERDICT: PASS" at 01:24:35, then the commit
raised at 01:25:27. The record said the review was satisfied while the work
backing it sat uncommitted, and the next stage refused on a dirty worktree.

DEV-644 — fix_review_findings ran 19 times. The verify prompt inspects the
working tree, but the previous attempt had already committed, so every retry saw
an empty diff and wrote FAIL for work that was done.

Committing first fixes the first and breaks the second harder, unless the
verifier is given explicit per-repo commit ranges instead of the working tree.
"""
import subprocess
from pathlib import Path

import core.tasks.tickets as T


def _repo(tmp_path: Path, name: str, *, flat: bool = False) -> Path:
    r = tmp_path / name if flat else tmp_path / "workspace" / name
    r.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.name", "t"], check=True)
    (r / "a.py").write_text("x = 1\n")
    subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(r), "commit", "-qm", "base", "--no-verify"], check=True)
    return r


class TestBaselineCapture:
    def test_it_records_a_sha_per_repo(self, tmp_path):
        _repo(tmp_path, "one"); _repo(tmp_path, "two")
        base = T._capture_repo_heads(tmp_path)
        assert set(base) == {"one", "two"}
        assert all(len(sha) == 40 for sha in base.values())

    def test_a_repo_it_cannot_read_is_absent_not_blank(self, tmp_path):
        """A blank baseline would silently widen the range to the whole history.

        Uses a real repo with no commits, so rev-parse HEAD genuinely fails and
        the error branch runs. A directory without .git is skipped earlier and
        never reaches it."""
        _repo(tmp_path, "one")
        empty = tmp_path / "workspace" / "noheadyet"
        empty.mkdir()
        subprocess.run(["git", "init", "-q", str(empty)], check=True)
        base = T._capture_repo_heads(tmp_path)
        assert "noheadyet" not in base, "a repo with no readable HEAD must be omitted"
        assert "" not in base.values()


class TestRangesGivenToTheVerifier:
    def test_a_committed_fix_is_still_visible(self, tmp_path):
        """The DEV-644 loop: after the commit the working tree is clean, so only
        an explicit range shows the verifier what changed."""
        r = _repo(tmp_path, "one")
        before = T._capture_repo_heads(tmp_path)
        (r / "a.py").write_text("x = 2\n")
        subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(r), "commit", "-qm", "fix", "--no-verify"], check=True)

        assert subprocess.run(["git", "-C", str(r), "diff"],
                              capture_output=True, text=True).stdout == ""
        ranges = T._review_diff_ranges(tmp_path, before)
        assert "one" in ranges
        body = subprocess.run(["git", "-C", str(r), "diff", ranges["one"]],
                              capture_output=True, text=True).stdout
        assert "x = 2" in body

    def test_a_repo_with_no_new_commits_is_omitted(self, tmp_path):
        _repo(tmp_path, "one")
        before = T._capture_repo_heads(tmp_path)
        assert T._review_diff_ranges(tmp_path, before) == {}

    def test_several_commits_are_covered_not_just_the_last(self, tmp_path):
        """The agent may commit on its own, and a retry adds more, so HEAD^..HEAD
        is not enough."""
        r = _repo(tmp_path, "one")
        before = T._capture_repo_heads(tmp_path)
        for i in (2, 3, 4):
            (r / "a.py").write_text(f"x = {i}\n")
            subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
            subprocess.run(["git", "-C", str(r), "commit", "-qm", f"c{i}", "--no-verify"], check=True)
        rng = T._review_diff_ranges(tmp_path, before)["one"]
        n = subprocess.run(["git", "-C", str(r), "rev-list", "--count", rng],
                           capture_output=True, text=True).stdout.strip()
        assert n == "3"

    def test_flat_ticket_layout_is_supported(self, tmp_path):
        """Production uses repos directly below the ticket directory."""
        r = _repo(tmp_path, "one", flat=True)
        before = T._capture_repo_heads(tmp_path)
        (r / "a.py").write_text("x = 2\n")
        subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
        subprocess.run(
            ["git", "-C", str(r), "commit", "-qm", "fix", "--no-verify"],
            check=True,
        )

        ranges = T._review_diff_ranges(tmp_path, before)
        after = subprocess.run(
            ["git", "-C", str(r), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        assert ranges["one"] == f"{before['one']}..{after}"
