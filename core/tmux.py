"""Exact tmux targets.

A bare `-t name` is not an exact match. tmux tries the name, then it as an
fnmatch pattern, then it as a prefix, so `-t term-work-1` resolves to
`term-work-10` when only that session exists. Every session name here carries
a work item id or a ticket key, and one id is routinely a prefix of another,
so a bare target sends a reply into another agent's pane, reads another
agent's health, or kills another agent's session.

An `=` prefix makes a target-session exact. A target-pane rejects `=name`
outright and needs `=name:`, which names the session exactly and takes its
current window and pane. Both forms answer "can't find session" for a name
that does not exist, rather than silently resolving to a longer one.
"""


def session(name: str) -> str:
    """An exact target-session: has-session, kill-session, attach-session."""
    return f"={name}"


def pane(name: str) -> str:
    """An exact target-pane or target-window in the session's current pane:
    list-panes, send-keys, capture-pane, display-message, respawn-pane."""
    return f"={name}:"
