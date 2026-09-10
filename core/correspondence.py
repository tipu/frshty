"""Refuse every outward message a task might send.

A work-board task holds a shell, a browser and whatever MCP servers the
operator connected, so the launch prompt asking it not to write to people is a
request rather than a control. This module is the control. Every route that
leaves the machine and reaches a person is named here and closed in one place:
Slack and other chat, email, and pull request or issue comments.

The gate is per instance and defaults open, because a client instance answers
pull request comments as its ordinary pipeline work. An instance that sets
features.correspondence = false may still read all of those surfaces. It may
not write to them, and neither may the agents it launches.

Reading stays open on purpose, and that shapes every matcher here. A task that
cannot read a review comment cannot act on one, so a rule is written against
the act of sending: the HTTP method, the write flag, the verb the MCP server
put at the front of its tool name. A rule written against the surface alone
would close reading with it.

The shell and the browser make the set of possible transports unbounded, so
this is not a proof that nothing can get out. It closes the routes a task
takes by accident or by the obvious path, which is what the prompt alone did
not do.
"""
import os
import re
import tomllib

DENY_REASON = (
    "Blocked by the work-layer correspondence gate: this task may not send a "
    "message to anybody. Slack and other chat messages, email, and pull "
    "request or issue comments all leave this machine and reach a person, and "
    "this instance does not allow a task to do that.\n\n"
    "Read those surfaces as much as you need. When you have something to say "
    "to a person, put the draft in your answer and tell the operator to send "
    "it. Do not look for another route to the same surface."
)

_CHAT = ("slack", "discord", "telegram", "twilio", "whatsapp", "mattermost")
_EMAIL = ("gmail", "outlook", "mailgun", "sendgrid", "postmark", "email", "mail",
          "ses", "smtp")
_FORGE = ("github", "gitlab", "bitbucket", "linear", "jira", "atlassian")
_WRITE_VERB = frozenset((
    "send", "post", "reply", "create", "write", "draft", "dm", "add", "update",
    "edit", "submit", "publish", "comment", "message", "notify", "share",
))
_COMMENT_NOUN = ("comment", "review", "note", "reply", "message", "discussion")

# Commands whose only purpose is to put a message in front of a person. No
# method test applies: running one at all is the send.
_ALWAYS = (
    r"\bgh\b[^|;&]*?\b(?:pr|issue)\s+comment\b",
    r"\bgh\b[^|;&]*?\bpr\s+review\b",
    r"\bgh\b[^|;&]*?\b(?:pr|issue)\s+close\b[^|;&]*?--comment\b",
    r"\bglab\b[^|;&]*?\b(?:mr|issue)\s+note\b",
    r"\bhooks\.slack\.com\b",
    r"\bdiscord(?:app)?\.com/api/webhooks\b",
    r"\bapi\.telegram\.org\b",
    r"\bslack\.com/api/(?:chat|files)\.",
    r"\bchat[_.]?postmessage\b",
    r"\bgmail\.googleapis\.com\b",
    r"\bgraph\.microsoft\.com\b[^|;&]*?\bsendmail\b",
    r"\bapi\.(?:mailgun|sendgrid|postmark(?:app)?)\.(?:net|com)\b",
    r"\bsmtp://",
    r"--mail-(?:from|rcpt)\b",
    r"\b(?:sendmail|msmtp|mailx|swaks|mutt|neomutt)\b",
    r"\bmail\b[^|;&]*?\s\S+@\S",
    r"\bsend\.py\b",
    r"\bsend_message\b",
    r"\b_slack_send\b",
    r"\baddcomment\b",
    r"\baddpullrequestreview\b",
    # The board's own instances expose the client pipeline's comment writers.
    # A task working on a client project reaches them with an ordinary curl.
    r"/api/(?:tickets|reviews)/[^|;&\s]*?/(?:pr-comments?|comments?)\b",
    r"/api/wizard/slack_ping\b",
)

# Surfaces that serve reading and writing from the same address. These deny
# only when the command also carries a write.
_MESSAGE_API = (
    r"\bapi\.github\.com\b[^|;&]*?\b(?:comments|reviews)\b",
    r"\bapi\.bitbucket\.org\b[^|;&]*?\bcomments\b",
    r"\bapi\.linear\.app\b",
    r"\bslack\.com/api/conversations\.",
    r"\bgh\s+api\b[^|;&]*?\b(?:comments|reviews|graphql)\b",
)

_CURL_WRITE = (
    r"(?:^|\s)-X\s*(?:POST|PUT|PATCH|DELETE)\b",
    r"--request\s*(?:POST|PUT|PATCH|DELETE)\b",
    r"(?:^|\s)(?:-d|--data|--data-raw|--data-binary|--data-urlencode|--json)(?:\s|=)",
    r"(?:^|\s)(?:-F|--form)(?:\s|=)",
    r"(?:^|\s)(?:-f|--field|--raw-field|--input)(?:\s|=)",
    r"--method\s*(?:POST|PUT|PATCH|DELETE)\b",
)

_ALWAYS_RE = tuple(re.compile(p, re.IGNORECASE) for p in _ALWAYS)
_MESSAGE_API_RE = tuple(re.compile(p, re.IGNORECASE) for p in _MESSAGE_API)
_CURL_WRITE_RE = tuple(re.compile(p, re.IGNORECASE) for p in _CURL_WRITE)
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def allowed(config: dict | None) -> bool:
    """Whether this instance lets a task send a message to a person.

    An instance that says nothing may correspond, because the client pipeline
    answers pull request comments as its ordinary work. Anything other than a
    missing key or a real `true` closes the gate, so a hand-edited
    `correspondence = "false"` does not read as permission."""
    features = (config or {}).get("features")
    if not isinstance(features, dict) or "correspondence" not in features:
        return True
    return features["correspondence"] is True


def allowed_on_disk(instance_key: str = "personal") -> bool:
    """Whether the named instance lets a task send a message, read off its
    config file.

    The tool hook runs in a process of its own, where no instance registry is
    loaded and allowed() would read an empty config as an open gate. This
    reads the same file the server reads. Anything that goes wrong closes the
    gate: blocking a message the operator wanted costs a retry, and sending
    one he did not want cannot be taken back."""
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "config", f"{instance_key}.toml")
    try:
        with open(path, "rb") as f:
            return allowed(tomllib.load(f))
    except Exception:
        return False


def _normalize(command: str) -> str:
    """One line of single-spaced text, so a wrapped command reads as one."""
    return re.sub(r"\s+", " ", (command or "").replace("\\\n", " "))


def bash_reason(command: str) -> str:
    """The deny reason for a shell command that would send a message, "" to
    allow.

    A surface that serves reading and writing from one address is judged by
    the write, not by the address: `gh api repos/o/r/pulls/1/comments` reads
    the thread and stays open, and the same path with a field flag writes to
    it and does not."""
    text = _normalize(command)
    for pattern in _ALWAYS_RE:
        if pattern.search(text):
            return DENY_REASON
    if any(p.search(text) for p in _MESSAGE_API_RE) and \
            any(p.search(text) for p in _CURL_WRITE_RE):
        return DENY_REASON
    return ""


def _verb(action: str) -> str:
    """The leading verb of an MCP action name.

    An MCP server writes its action verb first, in either snake or camel case,
    so the first token classifies the call and nothing later in the name does.
    Reading the whole name instead turned `list_drafts` into a draft and
    `search_messages_by_sender` into a send."""
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", _CAMEL.sub(" ", action)) if t]
    return tokens[0].lower() if tokens else ""


def tool_reason(tool_name: str) -> str:
    """The deny reason for an MCP tool that would send a message, "" to allow.

    An MCP tool names its server and its action, so the pair decides. A chat
    or mail server is closed to every write. A code host serves reading and
    shipping as well as commenting, so it needs the write verb and a comment
    noun together: `create_pull_request` ships and stays open, and
    `add_issue_comment` reaches a person and does not."""
    parts = (tool_name or "").split("__")
    if len(parts) < 3 or parts[0].lower() != "mcp":
        return ""
    server = parts[1].lower()
    action = "__".join(parts[2:])
    if _verb(action) not in _WRITE_VERB:
        return ""
    name = f"{server} {action}".lower()
    if any(s in server for s in _CHAT + _EMAIL):
        return DENY_REASON
    if any(s in server for s in _FORGE) and any(n in name for n in _COMMENT_NOUN):
        return DENY_REASON
    return ""
