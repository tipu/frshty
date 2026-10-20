"""Reject a push that carries a client or person name.

The pre-push hook in scripts/git-hooks runs this script. Git passes the remote
name as the first argument and one line per pushed ref on stdin. The script
scans every pushed branch or tag name, every new commit's author, committer
and message, and every new file path and blob. "New" means not reachable from
a ref the remote already has. A hit on any term rejects the whole push.

The term list never enters the repository. It is read from the gitignored
config/name_guard_terms.tsv beside this script, or from the file that
FRSHTY_NAME_TERMS names. Each line holds one term, optionally followed by a
tab and a source tag. A missing or empty list rejects the push, because a
guard with no terms cannot fail.
"""
import os
import re
import subprocess
import sys
import threading
from pathlib import Path

TERMS_FILE = Path(__file__).resolve().parent.parent / "config" / "name_guard_terms.tsv"
ZERO = re.compile(r"^0+$")
TOKEN = re.compile(r"[a-z0-9À-ÿ]+")
BINARY_PROBE = 8000


def load_terms(path: Path) -> list[str]:
    terms = []
    for line in path.read_text(encoding="utf-8").splitlines():
        term = line.split("\t", 1)[0].strip().lower()
        if term and not term.startswith("#"):
            terms.append(term)
    return terms


class Matcher:
    """Whole-word matching, the same rule the work-150 crawl used.

    A term made only of word tokens matches a run of the same tokens. Any other
    term, such as an email address or a domain, matches as a substring that no
    letter or digit touches on either side."""

    def __init__(self, terms: list[str]):
        self.words: set[str] = set()
        special = []
        for term in terms:
            if " ".join(TOKEN.findall(term)) == term:
                self.words.add(term)
            else:
                special.append(term)
        self.longest = max((t.count(" ") + 1 for t in self.words), default=0)
        self.special = re.compile(
            r"(?<![^\W_])(?:" + "|".join(re.escape(t) for t in sorted(special, key=len, reverse=True))
            + r")(?![^\W_])") if special else None

    def find(self, text: str) -> set[str]:
        found = set()
        low = text.lower()
        for view in (low, re.sub(r"\\[ntr]", "  ", low)):
            tokens = TOKEN.findall(view)
            for n in range(1, self.longest + 1):
                for i in range(len(tokens) - n + 1):
                    key = " ".join(tokens[i:i + n])
                    if key in self.words:
                        found.add(key)
            if self.special:
                found.update(m.group(0) for m in self.special.finditer(view))
        return found


def _git(repo: str, *args: str) -> str:
    got = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True,
                         errors="replace")
    if got.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {got.stderr.strip()}")
    return got.stdout


def _blobs(repo: str, ids: list[str]):
    proc = subprocess.Popen(["git", "-C", repo, "cat-file", "--batch"],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def feed():
        proc.stdin.write(("\n".join(ids) + "\n").encode())
        proc.stdin.close()

    threading.Thread(target=feed, daemon=True).start()
    for oid in ids:
        header = proc.stdout.readline().split()
        if len(header) < 3:
            raise RuntimeError(f"git cat-file: no object {oid}")
        data = proc.stdout.read(int(header[2]))
        proc.stdout.read(1)
        if header[1] == b"blob" and b"\0" not in data[:BINARY_PROBE]:
            yield oid, data.decode("utf-8", errors="replace")
    proc.wait()


def scan_push(repo: str, remote: str, lines: list[str], matcher: Matcher) -> list[tuple[str, str]]:
    """Every (place, term) hit in what this push sends."""
    hits = []
    tips, exclude = [], [f"--remotes={remote}"]
    for line in lines:
        parts = line.split()
        if len(parts) != 4:
            continue
        _, local_sha, remote_ref, remote_sha = parts
        if ZERO.match(local_sha):
            continue
        for term in matcher.find(remote_ref):
            hits.append((f"ref {remote_ref}", term))
        tips.append(local_sha)
        if not ZERO.match(remote_sha) and subprocess.run(
                ["git", "-C", repo, "cat-file", "-e", f"{remote_sha}^{{commit}}"],
                capture_output=True).returncode == 0:
            exclude.append(remote_sha)
    if not tips:
        return hits
    log = _git(repo, "log", "--format=%x00%H%x01%an <%ae>%n%cn <%ce>%n%B", *tips, "--not", *exclude)
    for record in log.split("\0")[1:]:
        sha, body = record.split("\x01", 1)
        for term in matcher.find(body):
            hits.append((f"commit {sha[:10]}", term))
    paths: dict[str, str] = {}
    for line in _git(repo, "rev-list", "--objects", *tips, "--not", *exclude).splitlines():
        oid, _, path = line.partition(" ")
        if path:
            paths.setdefault(oid, path)
            for term in matcher.find(path):
                hits.append((f"path {path}", term))
    for oid, text in _blobs(repo, list(paths)):
        for term in matcher.find(text):
            hits.append((f"file {paths[oid]} ({oid[:10]})", term))
    return hits


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[0] != "pre-push":
        print("usage: name_guard.py pre-push <remote> [url] < pushed refs", file=sys.stderr)
        return 2
    terms_path = Path(os.environ.get("FRSHTY_NAME_TERMS") or TERMS_FILE)
    terms = load_terms(terms_path) if terms_path.is_file() else []
    if not terms:
        print(f"name guard: no terms in {terms_path}; the push is rejected. "
              "Copy the term list there.", file=sys.stderr)
        return 1
    repo = os.getcwd()
    hits = scan_push(repo, argv[1], sys.stdin.read().splitlines(), Matcher(terms))
    if not hits:
        return 0
    print("name guard: the push carries a client or person name. "
          "Replace it with a name from scripts/fake_name.py.", file=sys.stderr)
    for place, term in sorted(set(hits)):
        print(f"  {place}: {term}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
