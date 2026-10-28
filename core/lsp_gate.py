r"""Refuse a definition grep for a symbol that the session has not looked up with LSP.

Every claude task carries llm.LSP_RULE, which asks for workspaceSymbol before
grep. A scan of the nine task sessions after the rule shipped found zero LSP
calls. The same sessions ran greps such as
`grep -n "class OpenCodeProvider" core/llm.py` and
`grep -rn "def parse_structured" frshty/core/`, which is the position lookup
workspaceSymbol answers. The prompt is a request; this gate is the constraint.

A gate that opened for the whole session after one ToolSearch for LSP did not
change the habit: the work-273 rescan found five definition greps after the
gate opened, and one session loaded LSP and made zero LSP calls. So the gate
opens per symbol.

The gate denies a grep whose text names a definition (`def X`, `class X`,
`function X`, `interface X`), written plain or as a regular expression such
as `def\s+X`, and a Grep tool call whose pattern does the same. A grep counts
only when grep, egrep, fgrep, rg, ag, ack or `git grep` runs as a command, at
the start of the command line or after `|`, `&&`, `;` and the like. The word
in a heredoc body, a `python -c` string or an echo argument does not count.

The gate allows the grep for X once the transcript shows an LSP
workspaceSymbol, goToDefinition or findReferences call whose query or result
names X. A ToolSearch call alone opens nothing. Two cases open the gate for
the rest of the session: an LSP result that says "No LSP server available",
and a ToolSearch for LSP that finds no LSP tool. A transcript that cannot be
read opens the gate: a missed nudge costs a grep, not a defect.
"""
import json
import os
import re
import shlex

DENY_REASON = (
    "Blocked by the LSP gate: this grep looks up the definition of `{name}`, "
    "and this session has not looked up `{name}` with LSP. If LSP is not "
    "loaded, load it once with ToolSearch, query \"select:LSP\". Then call "
    "LSP with operation workspaceSymbol, query \"{name}\", and any file of "
    "the project as filePath with line 1 and character 1. It returns the "
    "file and line of the symbol. Use findReferences for its callers and "
    "goToDefinition for its definition. The gate opens per symbol: a grep "
    "for `{name}` passes after a workspaceSymbol, goToDefinition or "
    "findReferences call whose query or result names `{name}`. Loading LSP "
    "alone does not open it. When ToolSearch finds no LSP tool or LSP "
    "answers \"No LSP server available\", the gate opens for the rest of the "
    "session, so run the grep again."
)

_SEARCH_TOOLS = {"grep", "egrep", "fgrep", "rg", "ag", "ack"}
_WRAPPERS = {"sudo", "xargs", "env", "time", "nice", "nohup", "command", "exec",
             "timeout", "stdbuf", "then", "do", "else", "if", "elif", "while",
             "until", "!", "{"}
_SHELLS = {"bash", "sh", "zsh", "dash"}
_SEPARATORS = set(";&|()\n")
_ASSIGNMENT = re.compile(r"[A-Za-z_]\w*=")
_HEREDOC = re.compile(r"(?<!<)<<-?[ \t]*(['\"]?)([\w.-]+)\1")
_KEYWORD = r"(?:def|class|function|interface)"
_QUANTIFIER = r"(?:(?:\\?[+*?]|\\?\{\d*,?\d*\\?\})\??)"
_SPACE = (r"(?:\s|\\\\?[sthW]|\\\\? |\[(?: |\t|\\t|\\s|\[:space:\]|\[:blank:\])+\])"
          + _QUANTIFIER + r"?|\." + _QUANTIFIER)
_GROUP = r"\\b|\\<|\\>|\((?:\?:)?|\)" + _QUANTIFIER + "?"
_DEFINITION = re.compile(
    r"(?:\b|(?<=\\[A-Za-z]))" + _KEYWORD + r"(?:\\?\|" + _KEYWORD + r"){0,3}(?:\\?\))?"
    r"(?:" + _GROUP + r")*(?:" + _SPACE + r")(?:" + _SPACE + r"|" + _GROUP + r")*"
    r"((?:[A-Za-z_]|\$[\w$])[\w$]*)")
_LOOKUPS = {"workspaceSymbol", "goToDefinition", "findReferences"}
_NO_SERVER = "No LSP server available"
_LSP_DEFINITION = re.compile(r'"name"\s*:\s*"LSP"')


def _strip_heredocs(command: str) -> str:
    """`command` without the body of each heredoc."""
    out = []
    pos = 0
    while True:
        match = _HEREDOC.search(command, pos)
        newline = command.find("\n", match.end()) if match else -1
        if match is None or newline < 0:
            out.append(command[pos:])
            return "".join(out)
        out.append(command[pos:newline + 1])
        end = re.compile(r"^[ \t]*" + re.escape(match.group(2)) + r"[ \t]*$", re.MULTILINE).search(
            command, newline + 1)
        if not end:
            return "".join(out)
        pos = end.end()


def _segments(command: str) -> list[list[str]]:
    """The simple commands of `command` as word lists, split at `|`, `&&`,
    `;`, newlines and parentheses. Raises ValueError on unbalanced quotes."""
    lexer = shlex.shlex(_strip_heredocs(command), posix=True, punctuation_chars=";&|()<>\n")
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    lexer.commenters = ""
    segments: list[list[str]] = [[]]
    for token in lexer:
        if set(token) <= _SEPARATORS:
            segments.append([])
        else:
            segments[-1].append(token)
    return [s for s in segments if s]


def _search_words(words: list[str]) -> list[str] | None:
    """The words from the search tool on when `words` runs a search tool,
    None otherwise."""
    for i, word in enumerate(words):
        name = os.path.basename(word.lstrip("`"))
        if name in _SEARCH_TOOLS:
            return words[i:]
        if name == "git":
            rest = words[i + 1:]
            while rest and rest[0].startswith("-"):
                rest = rest[2:] if rest[0] in ("-C", "-c") else rest[1:]
            return rest if rest[:1] == ["grep"] else None
        if name == "find":
            for j, arg in enumerate(words[i + 1:-1], i + 1):
                if arg in ("-exec", "-execdir") and os.path.basename(words[j + 1]) in _SEARCH_TOOLS:
                    return words[j + 1:]
            return None
        if not (_ASSIGNMENT.match(word) or name in _WRAPPERS or word.startswith("-")
                or word.isdigit()):
            return None
    return None


def _symbol(command: str) -> str:
    """The symbol whose definition a grep in `command` looks for, "" when the
    command runs no such grep."""
    try:
        segments = _segments(command)
    except ValueError:
        return ""
    for words in segments:
        if os.path.basename(words[0]) in _SHELLS and len(words) > 2 and re.fullmatch(r"-\w*c", words[1]):
            name = _symbol(words[2])
            if name:
                return name
            continue
        search = _search_words(words)
        match = _DEFINITION.search(" ".join(search)) if search else None
        if match:
            return match.group(1)
    return ""


def _result_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(str(b.get("text") or "") for b in content if isinstance(b, dict))
    return ""


def _lsp_lookups(transcript_path: str) -> list[str] | None:
    """The query and result text of each LSP lookup in the transcript. None
    when the gate is open for the whole session: the transcript cannot be
    read, an LSP result says no server is available, or a ToolSearch for LSP
    found no LSP tool."""
    calls: dict[str, tuple[str, str]] = {}
    texts: list[str] = []
    try:
        with open(transcript_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if '"tool_use"' not in line and '"tool_result"' not in line:
                    continue
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                content = (entry.get("message") or {}).get("content")
                if not isinstance(content, list):
                    continue
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") == "tool_use":
                        tool_input = block.get("input") or {}
                        if block.get("name") == "LSP":
                            calls[str(block.get("id"))] = ("LSP", str(tool_input.get("operation") or ""))
                            if tool_input.get("operation") in _LOOKUPS:
                                texts.append(str(tool_input.get("query") or ""))
                        elif block.get("name") == "ToolSearch" and "LSP" in str(tool_input.get("query") or ""):
                            calls[str(block.get("id"))] = ("ToolSearch", "")
                    elif block.get("type") == "tool_result":
                        call = calls.get(str(block.get("tool_use_id")))
                        if not call:
                            continue
                        result = block.get("content")
                        text = _result_text(result)
                        if call[0] == "ToolSearch":
                            references = [b.get("tool_name") for b in result
                                          if isinstance(b, dict)] if isinstance(result, list) else []
                            if "LSP" not in references and not _LSP_DEFINITION.search(text):
                                return None
                        elif _NO_SERVER in text:
                            return None
                        elif call[1] in _LOOKUPS:
                            texts.append(text)
    except OSError:
        return None
    return texts


def bash_reason(command: str, transcript_path: str) -> str:
    """The deny reason for a definition grep for a symbol the session has not
    looked up with LSP, "" to allow."""
    return _reason(_symbol(command or ""), transcript_path)


def grep_reason(pattern: str, transcript_path: str) -> str:
    """The deny reason for a Grep tool call whose pattern looks up the
    definition of a symbol the session has not looked up with LSP, "" to
    allow."""
    match = _DEFINITION.search(pattern or "")
    return _reason(match.group(1) if match else "", transcript_path)


def _reason(name: str, transcript_path: str) -> str:
    if not name or not transcript_path:
        return ""
    lookups = _lsp_lookups(transcript_path)
    if lookups is None:
        return ""
    named = re.compile(r"(?<![\w$])" + re.escape(name) + r"(?![\w$])")
    if any(named.search(text) for text in lookups):
        return ""
    return DENY_REASON.format(name=name)
