"""Refuse a plain `git worktree prune` from an agent shell.

One repository is mounted into several instance containers and the host at
once, and each one sees only its own worktree paths. `git worktree prune`
reads every registered worktree it cannot see as deleted and removes its
admin directory under .git/worktrees. The worktree on disk survives without
its admin directory, so `git status` in it fails with "not a git repository",
and git later hands the freed admin name to a new worktree, so the old one
reads the new one's HEAD and index.

Task agents ran exactly this command: on 2026-10-06 from work-21 and on
2026-10-12 from work-90, both in the frshty container. Each run removed the
admin directories of personal worktrees, and the worktree sweep reported the
faults within two minutes. core/git_util.prune_worktrees is the scoped
replacement inside frshty; this gate closes the shell route.

The gate fails closed and reads text, not shell grammar. It denies a command
whose text holds the words `git`, `worktree` and `prune` in that order, a dry
run included. Each attempt to read the shell grammar exactly met a spelling
that hid the prune: a redirection, a comment, `$'...'`, `eval` joining its
arguments. The words are matched after quotes, `$` and backslashes are
dropped, so a word split by quoting is whole again. A word built from a
variable still passes; this closes the routes a task takes by accident or by
the obvious path, not every route. `git worktree list` marks the same entries
prunable and stays open.
"""
import re

DENY_REASON = (
    "Blocked by the worktree prune gate: `git worktree prune` deletes the "
    "admin directory of every worktree whose path this process cannot see. "
    "This repository is shared by several instance containers and the host, "
    "and each one sees only its own worktree paths, so a prune here breaks "
    "worktrees that other tasks still use.\n\n"
    "To drop one worktree you created, run `git worktree remove <path>`. To "
    "see which worktrees a prune would drop, run `git worktree list` and read "
    "the `prunable` entries. If the command only mentions these words, for "
    "example in a message, put that text in a file and pass the file. Do not "
    "look for another route to the same prune."
)

_DROPPED = re.compile(r"\\\n|['\"$\\]")
_WORDS = tuple(re.compile(rf"\b{w}\b") for w in ("git", "worktree", "prune"))


def _names_prune(text: str) -> bool:
    """Whether `text` holds git, worktree and prune in that order.

    Taking the first match of each word after the one before is enough: any
    later occurrence leaves less text for the words that follow. Three
    searches keep this linear on a long command."""
    position = 0
    for word in _WORDS:
        match = word.search(text, position)
        if match is None:
            return False
        position = match.end()
    return True


def bash_reason(command: str) -> str:
    """The deny reason for a shell command whose text runs or could run
    `git worktree prune`, "" to allow."""
    text = command or ""
    if _names_prune(text) or _names_prune(_DROPPED.sub("", text)):
        return DENY_REASON
    return ""
