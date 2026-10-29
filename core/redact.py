"""Replace secret values in text the board stores.

Agents ask the operator for passwords and keys, and the operator types them
into the task box or the answer box. Those strings were written to
work_items and work_events in plain text, and every board view showed them
again: one question payload held an AWS password, and five objectives held a
login and its password. Redaction runs on what is stored, never on what the
live agent is sent, so a reply still reaches the session whole.

The patterns are narrow on purpose. A key word counts only when a ':' or '='
follows it, so prose that says "the token expired" and code that names
`password_field` stay as they are, and a bare one-time code carries no key
word at all."""

import re

PLACEHOLDER = "[redacted]"

_KEY_WORDS = (r"password|passwd|pwd|secret|token|api[ _-]?key|access[ _-]?key|"
              r"secret[ _-]?access[ _-]?key|private[ _-]?key")
_KEY_VALUE_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9])((?:[A-Za-z0-9]+[_-])*(?:" + _KEY_WORDS + r"))"
    r"(?![A-Za-z0-9_])([\"']?[ \t]*[:=][ \t]*)"
    r"(\"[^\"\n]*\"|'[^'\n]*'|[^\s,;\"']+)")
_CALL_RE = re.compile(r"[A-Za-z_][\w.]*\(\)?")
_PATTERNS = (
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S),
    re.compile(r"(?<![A-Za-z0-9])(?:AKIA|ASIA)[0-9A-Z]{16}(?![A-Za-z0-9])"),
    re.compile(r"(?<![A-Za-z0-9])gh[pousr]_[A-Za-z0-9]{36,}"),
    re.compile(r"(?<![A-Za-z0-9])github_pat_[A-Za-z0-9_]{22,}"),
)


def _key_value(match: re.Match) -> str:
    value = match.group(3)
    if value == PLACEHOLDER or _CALL_RE.fullmatch(value):
        return match.group(0)
    return match.group(1) + match.group(2) + PLACEHOLDER


def redact_secrets(text: str) -> str:
    """The text with each secret value replaced by PLACEHOLDER.

    A value that is a call with no argument, or a call cut at its quoted
    argument, is left, because `password = getpass()` and
    `pwd = os.environ.get("PWD")` are code that names where a secret comes
    from, not the secret itself."""
    if not text:
        return text
    for pattern in _PATTERNS:
        text = pattern.sub(PLACEHOLDER, text)
    return _KEY_VALUE_RE.sub(_key_value, text)


def redact_json(value):
    """redact_secrets over every string inside a decoded JSON value."""
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, dict):
        return {k: redact_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_json(v) for v in value]
    return value
