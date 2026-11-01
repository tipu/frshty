"""Git history for Claude memory folders.

A memory folder is `<claude config dir>/projects/<project>/memory`. Agents
write it with Edit and Write, and nothing kept the old text: a wrong edit
could not be traced to the session that made it or rolled back. The agent
hook in scripts/work_hook.py calls `record_pending` before every write and
`record_write` after it, so each write becomes one commit in a git
repository inside the folder, with the session id in the message.

Both hooks run synchronously. The agent does not take its next step until
the commit is made, so a later write cannot land in an earlier write's
commit.
"""
import fcntl
import os
from pathlib import Path

from core.git_util import run_git

GIT_IDENTITY = ("-c", "user.name=frshty memory", "-c", "user.email=memory@frshty.invalid",
                "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false")


def memory_dir(path: str) -> Path | None:
    """The memory folder that holds `path`, None when it is not in one."""
    if not path:
        return None
    current = Path(os.path.abspath(path)).parent
    while current != current.parent:
        if current.name == "memory" and current.parent.parent.name == "projects":
            return current
        current = current.parent
    return None


def _staged(directory: Path) -> bool:
    return run_git(directory, ["diff", "--cached", "--quiet"], allowed_codes=(0, 1)).returncode == 1


def _commit(directory: Path, message: str) -> str:
    run_git(directory, [*GIT_IDENTITY, "commit", "-q", "-m", message])
    return run_git(directory, ["rev-parse", "HEAD"]).stdout.strip()


def _read(path: Path) -> str:
    try:
        return path.read_text(errors="replace")
    except OSError:
        return ""


def _locked(directory: Path):
    fd = os.open(directory, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)
    if not (directory / ".git").exists():
        run_git(directory, ["init", "-q"])
    return fd


def _commit_outside(directory: Path, session_id: str, exclude: str = "") -> list[str]:
    spec = [".", f":(exclude,literal){exclude}"] if exclude else ["."]
    run_git(directory, ["add", "-A", "--", *spec])
    if not _staged(directory):
        return []
    return [_commit(directory, f"Record memory changes found before a write by session "
                               f"{session_id}\n\nThese changes did not come through a tracked "
                               f"Edit or Write.")]


def record_pending(path: str, session_id: str) -> list[str]:
    """Commit what changed in the folder before a write, such as a file
    removed with `rm` or the files that predate the repository, so the
    session about to write is never named for a change it did not make."""
    directory = memory_dir(path)
    if directory is None or not directory.is_dir():
        return []
    fd = _locked(directory)
    try:
        return _commit_outside(directory, session_id)
    finally:
        os.close(fd)


def record_write(path: str, session_id: str, tool: str = "", transcript_path: str = "",
                 expected: str = "") -> list[str]:
    """Commit one memory write under the session that made it.

    Other changes in the folder go into a commit of their own first.
    `expected` is text the write put in the file. Two sessions that write
    the same folder within the same moment can still interleave, and when
    the file no longer holds that text the commit says so instead of
    crediting the session with another writer's text without a word.
    Returns the new commit ids, [] when `path` is outside a memory folder or
    the write changed nothing."""
    directory = memory_dir(path)
    if directory is None or not directory.is_dir():
        return []
    rel = os.path.relpath(os.path.abspath(path), directory)
    fd = _locked(directory)
    try:
        commits = _commit_outside(directory, session_id, rel)
        run_git(directory, ["add", "-A", "--", f":(literal){rel}"])
        if _staged(directory):
            body = f"Session: {session_id}"
            if tool:
                body += f"\nTool: {tool}"
            if transcript_path:
                body += f"\nTranscript: {transcript_path}"
            if expected and expected not in _read(directory / rel):
                body += ("\n\nWarning: the file no longer holds the text of this write. "
                         "Another writer changed it before this commit.")
            commits.append(_commit(directory, f"Memory write {rel} by session {session_id}\n\n{body}"))
        return commits
    finally:
        os.close(fd)
