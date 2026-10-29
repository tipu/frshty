"""Refuse a foreground shell command that waits: a long sleep or a CI watch.

The usage review of work 284 found frshty-board agents spending 28 of 65
tool-hours on CI watches, gh polls and sleep, and 60 client-board sessions
sleeping although the launch prompt forbade sleep loops. Every pass of such a
loop holds the session and costs a turn, while the board can poll the checks
of a pull request for free and resume the session when they finish. The
prompt is a request; this gate is the constraint.

The gate denies a foreground Bash command that runs `sleep` for more than
MAX_SLEEP_SECONDS in total, and `gh pr checks --watch` or `gh run watch`. The
sleeps of one command add up. A `for` body counts once for each word of a
literal list. A `while` or `until` body that sleeps LOOP_SLEEP_SECONDS or more
in one pass is a polling loop with no known end, so it counts as forever; a
shorter retry sleep counts once. A command counts only when it runs as a command: the word
in a heredoc body, a quoted string or an echo argument does not count. The
commands are read with the lexer of core/lsp_gate, which honors quotes,
backslashes and comments. A call with run_in_background is not gated: it does
not hold the turn. A sleep whose length is a variable passes, because the
gate cannot read it.
"""
import re

import core.lsp_gate as lsp_gate

MAX_SLEEP_SECONDS = 30
LOOP_SLEEP_SECONDS = 10
WAIT_MARKER = "waiting on PR #{pr} checks"

DENY_REASON = (
    "Blocked by the wait gate: {what}. A foreground wait holds this session "
    f"and costs a turn on every pass. Sleeps of {MAX_SLEEP_SECONDS} seconds or "
    "less still run, and a command with run_in_background still runs.\n\n"
    "To wait for the checks of a pull request, end your turn with the line "
    f"`{WAIT_MARKER.format(pr='N')}`, where N is the pull request number. "
    "The board polls those checks and resumes this session with the result "
    "when they finish, pass or fail. To wait for a process of your own, run it "
    "with run_in_background and use the notification the harness sends when "
    "it ends. Do not look for another route to the same wait."
)

_DURATION = re.compile(r"(\d+(?:\.\d*)?|\.\d+)([smhd]?)")
_UNIT_SECONDS = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}
_GH_VALUE_OPTIONS = {"-R", "--repo"}


def _seconds(word: str) -> float | None:
    """The length of one `sleep` argument in seconds, None when it is not a
    literal duration."""
    text = lsp_gate._unquote(word)
    if text in ("inf", "infinity"):
        return float("inf")
    match = _DURATION.fullmatch(text)
    if match is None:
        return None
    return float(match.group(1)) * _UNIT_SECONDS[match.group(2)]


def _command_at(words: list[str]) -> int:
    """The index of the command a simple command runs once its assignments,
    wrappers and their options are skipped, len(words) when there is none."""
    i = 0
    while i < len(words):
        word = words[i]
        if (lsp_gate._ASSIGNMENT.match(word) or lsp_gate._command(word) in lsp_gate._WRAPPERS
                or word.startswith("-") or _seconds(word) is not None):
            i += 1
            continue
        return i
    return i


def _gh_wait(words: list[str]) -> str:
    """The CI watch `gh ...` runs, "" when it runs none."""
    positional = []
    skip = False
    for word in words:
        text = lsp_gate._unquote(word)
        if skip:
            skip = False
        elif text in _GH_VALUE_OPTIONS:
            skip = True
        elif not text.startswith("-"):
            positional.append(text)
    if positional[:2] == ["pr", "checks"] and any(
            lsp_gate._unquote(w) == "--watch" or lsp_gate._unquote(w).startswith("--watch=")
            for w in words):
        return "`gh pr checks --watch` waits in the foreground for CI"
    if positional[:2] == ["run", "watch"]:
        return "`gh run watch` waits in the foreground for CI"
    return ""


def _loop_frame(words: list[str]) -> dict | None:
    """The loop a segment opens, None when it opens none. passes is the
    number of times its body runs, None when the gate cannot count them."""
    head = lsp_gate._command(words[0])
    if head in ("while", "until"):
        return {"passes": None, "slept": 0.0}
    if head == "for":
        items = [lsp_gate._unquote(w) for w in words[1:]]
        if "in" in items:
            listed = items[items.index("in") + 1:]
            if listed and not any(re.search(r"[$`*?\[]", w) for w in listed):
                return {"passes": len(listed), "slept": 0.0}
        return {"passes": None, "slept": 0.0}
    return None


def _close(frame: dict) -> float:
    """The seconds a finished loop waits in all its passes."""
    if frame["passes"] is not None:
        return frame["slept"] * frame["passes"]
    if frame["slept"] >= LOOP_SLEEP_SECONDS:
        return float("inf")
    return frame["slept"]


def _scan(command: str) -> tuple[str, float]:
    """The CI watch `command` runs, "" when none, and the seconds it sleeps.
    A command run through `bash -c` counts toward the command around it."""
    try:
        segments = lsp_gate._segments(command)
    except ValueError:
        return "", 0.0
    frames = [{"passes": 1, "slept": 0.0}]
    for words in segments:
        frame = _loop_frame(words)
        if frame is not None:
            frames.append(frame)
        if lsp_gate._command(words[0]) == "done" and len(frames) > 1:
            closed = frames.pop()
            frames[-1]["slept"] += _close(closed)
            continue
        i = _command_at(words)
        if i == len(words):
            continue
        name = lsp_gate._command(words[i])
        if name in lsp_gate._SHELLS:
            for j in range(i + 1, len(words) - 1):
                if not words[j].startswith("-"):
                    break
                if re.fullmatch(r"-\w*c\w*", words[j]):
                    watch, slept = _scan(lsp_gate._unquote(words[j + 1]))
                    if watch:
                        return watch, 0.0
                    frames[-1]["slept"] += slept
                    break
        elif name == "sleep":
            lengths = [_seconds(w) for w in words[i + 1:]]
            frames[-1]["slept"] += sum(s for s in lengths if s is not None)
        elif name == "gh":
            found = _gh_wait(words[i + 1:])
            if found:
                return found, 0.0
    while len(frames) > 1:
        closed = frames.pop()
        frames[-1]["slept"] += _close(closed)
    return "", frames[0]["slept"]


def _wait(command: str) -> str:
    """What `command` waits on, "" when it does not wait."""
    watch, total = _scan(command)
    if watch:
        return watch
    if total > MAX_SLEEP_SECONDS:
        shown = "forever" if total == float("inf") else f"{total:g} seconds"
        return (f"`sleep` waits {shown} in the foreground, over the limit "
                f"of {MAX_SLEEP_SECONDS} seconds")
    return ""


def bash_reason(command: str, run_in_background: bool = False) -> str:
    """The deny reason for a foreground command that sleeps past the limit or
    watches CI, "" to allow."""
    if run_in_background or not isinstance(command, str):
        return ""
    what = _wait(command)
    return DENY_REASON.format(what=what) if what else ""
