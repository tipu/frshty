"""No tracked file may name a client, company or person.

frshty is public and runs for several organizations. Their names belong in the
gitignored instance config, never in code, tests, docs or examples. Tests and
examples use fictional placeholders.

tests/forbidden_names.sha256 holds a SHA-256 hash and the length of each
forbidden name, so the list itself names nobody. Add a name with
`python3 scripts/forbid_name.py <name>`.

A name of five or more characters is found inside a longer run of letters and
digits too, so `frshty_<name>` and `<name>Health` fail. A shorter name is found
only as a whole word, because a short run occurs inside ordinary words.

The repository owner's account name is allowed in a GitHub repository path,
because the Dockerfile and the docs install the owner's other repositories.
LICENSE names the copyright holder. Vendored third-party files are not ours.
"""
import hashlib
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LIST = ROOT / "tests" / "forbidden_names.sha256"
SKIP_FILES = {"LICENSE", "uv.lock", "static/tailwind.js"}
SKIP_PREFIXES = ("static/vendor/",)
SUBSTRING_MIN = 5
RUN = re.compile(r"[a-z0-9]+")


def forbidden() -> dict[str, int]:
    out = {}
    for line in LIST.read_text().splitlines():
        if line and not line.startswith("#"):
            digest, length = line.split()
            out[digest] = int(length)
    return out


def owner() -> str:
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not repo:
        url = subprocess.run(["git", "-C", str(ROOT), "remote", "get-url", "origin"],
                             capture_output=True, text=True, check=True).stdout.strip()
        match = re.search(r"github\.com[:/]([^/]+)/", url)
        repo = match.group(1) if match else ""
    return repo.split("/", 1)[0].lower()


def tracked() -> list[str]:
    out = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z"],
                         capture_output=True, text=True, check=True).stdout
    return [p for p in out.split("\0") if p and p not in SKIP_FILES and not p.startswith(SKIP_PREFIXES)]


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def hits(text: str, names: dict[str, int], allowed_owner: str = "") -> set[str]:
    text = text.lower()
    if allowed_owner:
        text = re.sub(rf"github(?:\.com)?[:/]{re.escape(allowed_owner)}/", "github.com/", text)
    lengths = sorted({n for n in names.values() if n >= SUBSTRING_MIN})
    found = set()
    for run in set(RUN.findall(text)):
        digest = _sha(run)
        if digest in names:
            found.add(digest)
        for size in lengths:
            for i in range(len(run) - size + 1):
                digest = _sha(run[i:i + size])
                if digest in names:
                    found.add(digest)
    return found


def test_hits_finds_a_name_alone_inside_a_word_and_in_any_case():
    names = {_sha("acmeco"): 6, _sha("zq"): 2}
    assert hits("see acmeco", names) == {_sha("acmeco")}
    assert hits("frshty_AcmeCoHealth", names) == {_sha("acmeco")}
    assert hits("a ZQ b", names) == {_sha("zq")}
    assert hits("azqb", names) == set()
    assert hits("github.com/acmeco/tool", names, "acmeco") == set()
    assert hits("github.com/acmeco/tool", names, "other") == {_sha("acmeco")}


def test_no_tracked_file_names_a_client_company_or_person():
    names = forbidden()
    allowed_owner = owner()
    offenders = []
    for path in tracked():
        try:
            text = (ROOT / path).read_text()
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        if hits(text, names, allowed_owner):
            offenders += [f"{path}:{n}" for n, line in enumerate(text.splitlines(), 1)
                          if hits(line, names, allowed_owner)]
    assert not offenders, (
        "These lines name a client, company or person. Replace each name with a fictional placeholder, "
        "or read it from config:\n" + "\n".join(offenders))
