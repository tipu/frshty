import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import name_guard  # noqa: E402

TERMS = "nesiorkly\tknown\nlus laico\tslack\nlus@pufoundworks.io\temail\n"


def _git(cwd, *args, check=True, input_text=None):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True,
                          check=check, timeout=60, input=input_text)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    terms = tmp_path / "terms.tsv"
    terms.write_text(TERMS)
    monkeypatch.setenv("FRSHTY_NAME_TERMS", str(terms))
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "--bare", "-b", "main", str(origin))
    work = tmp_path / "work"
    _git(tmp_path, "clone", str(origin), str(work))
    _git(work, "config", "user.name", "test")
    _git(work, "config", "user.email", "test@example.com")
    _git(work, "config", "core.hooksPath", str(ROOT / "scripts" / "git-hooks"))
    (work / "README.md").write_text("hello\n")
    _git(work, "add", "-A")
    _git(work, "commit", "-m", "init")
    _git(work, "push", "origin", "main")
    return work


def _commit(work, path, text, message="change"):
    target = work / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    _git(work, "add", "-A")
    _git(work, "commit", "-m", message)


def _push(work, *refspec):
    return _git(work, "push", "origin", *(refspec or ("HEAD",)), check=False)


def test_a_clean_push_passes(repo):
    _commit(repo, "app.py", "owner = 'someone'\n")
    got = _push(repo)
    assert got.returncode == 0, got.stderr


@pytest.mark.parametrize("path, text, message", [
    ("app.py", "client = 'Nesiorkly'\n", "change"),
    ("app.py", "reviewer: Lus\n  Laico\n", "change"),
    ("app.json", '{"note": "mail lus@pufoundworks.io now"}\n', "change"),
    ("app.json", '{"note": "hi\\nLus Laico"}\n', "change"),
    ("app.py", "x = 1\n", "Fix the report for nesiorkly"),
    ("docs/nesiorkly/notes.txt", "x\n", "change"),
])
def test_a_term_anywhere_in_the_push_rejects_it(repo, path, text, message):
    before = _git(repo, "rev-parse", "origin/main").stdout
    _commit(repo, path, text, message)
    got = _push(repo)
    assert got.returncode != 0
    assert "name guard" in got.stderr
    assert _git(repo, "ls-remote", "origin", "main").stdout.split()[0] == before.strip()


def test_a_rename_to_a_term_rejects_it(repo):
    _git(repo, "mv", "README.md", "nesiorkly.md")
    _git(repo, "commit", "-m", "rename")
    got = _push(repo)
    assert got.returncode != 0
    assert "path nesiorkly.md: nesiorkly" in got.stderr


@pytest.mark.parametrize("make_tag", [
    lambda repo: _git(repo, "tag", "-a", "v1", "-m", "release for Nesiorkly"),
    lambda repo: _git(repo, "tag", "raw", _git(
        repo, "hash-object", "-w", "--stdin", input_text="client Nesiorkly\n").stdout.strip()),
])
def test_a_term_in_a_pushed_tag_rejects_it(repo, make_tag):
    make_tag(repo)
    got = _push(repo, "--tags")
    assert got.returncode != 0
    assert "nesiorkly" in got.stderr


def test_a_tag_on_a_tree_with_a_term_path_rejects_it(repo):
    (repo / "nesiorkly-new.txt").write_text("clean\n")
    _git(repo, "add", "-A")
    tree = _git(repo, "write-tree").stdout.strip()
    _git(repo, "reset", "-q", "--hard")
    _git(repo, "tag", "treetag", tree)
    got = _push(repo, "refs/tags/treetag")
    assert got.returncode != 0
    assert "path nesiorkly-new.txt: nesiorkly" in got.stderr


def test_deleting_a_term_path_the_remote_holds_passes(repo):
    _commit(repo, "nesiorkly.txt", "clean\n")
    _git(repo, "-c", "core.hooksPath=/dev/null", "push", "origin", "HEAD")
    _git(repo, "rm", "-q", "nesiorkly.txt")
    _git(repo, "commit", "-m", "remove")
    got = _push(repo)
    assert got.returncode == 0, got.stderr


def test_retagging_a_blob_the_remote_holds_passes(repo):
    blob = _git(repo, "hash-object", "-w", "--stdin", input_text="client Nesiorkly\n").stdout.strip()
    _git(repo, "tag", "raw", blob)
    _git(repo, "-c", "core.hooksPath=/dev/null", "push", "origin", "refs/tags/raw")
    _git(repo, "tag", "-f", "-a", "raw", "-m", "release", blob)
    got = _push(repo, "--force", "refs/tags/raw")
    assert got.returncode == 0, got.stderr


def test_a_term_in_the_branch_name_rejects_it(repo):
    _commit(repo, "app.py", "x = 1\n")
    got = _push(repo, "HEAD:refs/heads/work-1-fix-nesiorkly-login")
    assert got.returncode != 0
    assert "ref refs/heads/work-1-fix-nesiorkly-login: nesiorkly" in got.stderr


def test_a_term_inside_a_longer_word_passes(repo):
    _commit(repo, "app.py", "a = 'xnesiorklyx'\nb = 'alus@pufoundworks.iox'\n")
    got = _push(repo)
    assert got.returncode == 0, got.stderr


def test_content_the_remote_already_holds_is_not_scanned_again(repo):
    _commit(repo, "old.py", "client = 'Nesiorkly'\n")
    _git(repo, "-c", "core.hooksPath=/dev/null", "push", "origin", "HEAD")
    _commit(repo, "app.py", "x = 1\n")
    got = _push(repo)
    assert got.returncode == 0, got.stderr


def test_a_missing_term_list_rejects_the_push(repo, tmp_path, monkeypatch):
    monkeypatch.setenv("FRSHTY_NAME_TERMS", str(tmp_path / "absent.tsv"))
    _commit(repo, "app.py", "x = 1\n")
    got = _push(repo)
    assert got.returncode != 0
    assert "no terms" in got.stderr


def test_a_branch_deletion_passes(repo):
    _git(repo, "-c", "core.hooksPath=/dev/null", "push", "origin", "HEAD:refs/heads/side")
    got = _push(repo, ":refs/heads/side")
    assert got.returncode == 0, got.stderr


def test_load_terms_takes_the_first_column_and_skips_comments(tmp_path):
    terms = tmp_path / "terms.tsv"
    terms.write_text("# list\nNesiorkly\tknown\n\nLus Laico\n")
    assert name_guard.load_terms(terms) == ["nesiorkly", "lus laico"]
    matcher = name_guard.Matcher(name_guard.load_terms(terms))
    assert matcher.find("Lus  LAICO wrote it") == {"lus laico"}
    assert matcher.find("nothing here") == set()
