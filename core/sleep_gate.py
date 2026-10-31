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

The gate also denies a `pgrep -f` or `pkill -f` whose pattern matches the
command that runs it, in the foreground or the background and in a Monitor.
`-f` matches the full command line of every process, and the shell that runs
the command holds the pattern in its own command line. On 2026-10-31 work 328
waited seven and a half hours on `until ! pgrep -f deepseek-v4.1-flash`,
because that pgrep found the loop itself. The same `pkill -f` kills its own
shell. A pattern that holds a variable passes, because the gate cannot read
it.
"""
import re
import subprocess

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

SELF_MATCH_REASON = (
    "Blocked by the wait gate: `{tool} -f {pattern}` matches its own command "
    "line. `-f` matches the full command line of every process, and the shell "
    "that runs this command holds `{pattern}` in its command line, so {effect}."
    "\n\nTo wait for a process of your own, run it with run_in_background and "
    "use the notification the harness sends when it ends. To wait for a "
    "process that already runs, wait on its PID: keep `$!` when you start it, "
    "and loop on `kill -0 <pid>`. Do not look for another spelling of the same "
    "pattern."
)
_SELF_MATCH_EFFECT = {
    "pgrep": "it always finds a match and a wait on it never ends",
    "pkill": "it kills the shell that runs it",
}
_PGREP_VALUE_OPTIONS = {"-d", "--delimiter", "-g", "--pgroup", "-G", "--group",
                        "-P", "--parent", "-s", "--session", "-t", "--terminal",
                        "-u", "--euid", "-U", "--uid", "-F", "--pidfile", "-r",
                        "--runstates", "--ns", "--nslist", "--signal", "-q", "--queue"}
_PGREP_FULL = re.compile(r"-[A-Za-z]*f[A-Za-z]*")

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


def _full_pattern(words: list[str]) -> str:
    """The pattern a `pgrep` or `pkill` matches against full command lines,
    "" when it does not match full command lines or names no pattern."""
    full = False
    pattern = ""
    skip = False
    for word in words:
        text = lsp_gate._unquote(word)
        if skip:
            skip = False
        elif text in _PGREP_VALUE_OPTIONS:
            skip = True
        elif text == "--full" or _PGREP_FULL.fullmatch(text):
            full = True
        elif text.startswith("-") or lsp_gate._REDIRECT.match(text):
            continue
        elif not pattern:
            if not word.startswith("'") and re.search(r"\$[\w{(]|`", word):
                return ""
            pattern = text
    return pattern if full else ""


def _matches(pattern: str, line: str) -> bool:
    """Whether the extended regular expression `pattern` matches `line`, as
    pgrep reads it. grep runs it because it does not backtrack, so a pattern
    cannot hold the hook. A pattern grep cannot read is matched as text."""
    try:
        result = subprocess.run(["grep", "-Ezq", "--", pattern], input=line,
                                capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        return False
    if result.returncode == 2:
        return pattern in line
    return result.returncode == 0


def _substitutions(text: str) -> list[str]:
    """The bodies of the `$(...)` and backtick command substitutions the
    shell runs in `text`. One inside single quotes or after a backslash does
    not run. Parentheses inside quotes in a body do not close it."""
    bodies = []
    double = False
    i = 0
    while i < len(text):
        char = text[i]
        if char == "\\":
            i += 2
            continue
        if char == "#" and not double and (i == 0 or text[i - 1] in " \t\n;&|()"):
            end = text.find("\n", i)
            i = len(text) if end < 0 else end
            continue
        if char == "'" and not double:
            end = text.find("'", i + 1)
            i = len(text) if end < 0 else end + 1
            continue
        if char == '"':
            double = not double
        elif text.startswith("$(", i):
            depth, quote, j = 1, "", i + 2
            while j < len(text) and depth:
                inner = text[j]
                if quote:
                    if inner == quote:
                        quote = ""
                elif inner in "'\"":
                    quote = inner
                elif inner == "(":
                    depth += 1
                elif inner == ")":
                    depth -= 1
                j += 1
            bodies.append(text[i + 2:j - 1] if depth == 0 else text[i + 2:])
            i = j
            continue
        elif char == "`":
            end = text.find("`", i + 1)
            end = len(text) if end < 0 else end
            bodies.append(text[i + 1:end])
            i = end + 1
            continue
        i += 1
    return bodies


def _self_matches(command: str, scanned: str) -> tuple[str, str]:
    """The `pgrep -f` or `pkill -f` in `scanned` whose pattern matches the
    command line of the shell that runs `command`, as (tool, pattern), and
    ("", "") when there is none."""
    try:
        segments = lsp_gate._segments(scanned)
    except ValueError:
        return "", ""
    shell_line = "bash -c " + command
    for inner in _substitutions(lsp_gate._strip_heredocs(scanned)):
        found = _self_matches(command, inner)
        if found[0]:
            return found
    for words in segments:
        i = _command_at(words)
        if i == len(words):
            continue
        name = lsp_gate._command(words[i])
        if name in lsp_gate._SHELLS:
            for j in range(i + 1, len(words) - 1):
                if re.fullmatch(r"-\w*c\w*", words[j]):
                    found = _self_matches(command, lsp_gate._unquote(words[j + 1]))
                    if found[0]:
                        return found
                    break
        if name not in _SELF_MATCH_EFFECT:
            continue
        pattern = _full_pattern(words[i + 1:])
        if pattern and _matches(pattern, shell_line):
            return name, pattern
    return "", ""


def self_match_reason(command: str) -> str:
    """The deny reason for a command whose `pgrep -f` or `pkill -f` matches
    the shell that runs it, "" to allow. It applies to a background command
    and a Monitor too: a background wait that never ends still holds the
    task."""
    if not isinstance(command, str):
        return ""
    tool, pattern = _self_matches(command, command)
    if not tool:
        return ""
    return SELF_MATCH_REASON.format(tool=tool, pattern=pattern,
                                    effect=_SELF_MATCH_EFFECT[tool])


def bash_reason(command: str, run_in_background: bool = False) -> str:
    """The deny reason for a foreground command that sleeps past the limit or
    watches CI, "" to allow."""
    if run_in_background or not isinstance(command, str):
        return ""
    what = _wait(command)
    return DENY_REASON.format(what=what) if what else ""
