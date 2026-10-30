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
workspaceSymbol, goToDefinition or findReferences call whose query, result
or position names X. The position names the identifier at the line and
character of the call in its file. A grep that names several definitions needs a lookup for each.
A ToolSearch call alone opens nothing. Two cases open the gate for
the rest of the session: an LSP result that says "No LSP server available",
and a ToolSearch for LSP that finds no LSP tool. A transcript that cannot be
read opens the gate: a missed nudge costs a grep, not a defect.
"""
import json
import os
import re

GATE_ENV = "FRSHTY_LSP_GATE"
DENY_REASON = (
    "Blocked by the LSP gate: this grep looks up the definition of `{name}`, "
    "and this session has not looked up `{name}` with LSP. If LSP is not "
    "loaded, load it once with ToolSearch, query \"select:LSP\". Then call "
    "LSP with operation workspaceSymbol, query \"{name}\", and any file of "
    "the project as filePath with line 1 and character 1. It returns the "
    "file and line of the symbol. Use findReferences for its callers and "
    "goToDefinition for its definition. The gate opens per symbol: a grep "
    "for `{name}` passes after a workspaceSymbol, goToDefinition or "
    "findReferences call whose query, result or position names `{name}`. "
    "Loading LSP "
    "alone does not open it. When ToolSearch finds no LSP tool or LSP "
    "answers \"No LSP server available\", the gate opens for the rest of the "
    "session, so run the grep again."
)

_SEARCH_TOOLS = {"grep", "egrep", "fgrep", "rg", "ag", "ack"}
_WRAPPERS = {"sudo", "xargs", "env", "time", "nice", "nohup", "command", "exec",
             "timeout", "stdbuf", "then", "do", "else", "if", "elif", "while",
             "until", "!", "{"}
_SHELLS = {"bash", "sh", "zsh", "dash"}
_SEPARATORS = ";&|()\n"
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
    r"((?:[A-Za-z_]|\$[\w$])[\w$]*(?:\\?\|(?!" + _KEYWORD + r"\b)(?:[A-Za-z_]|\$[\w$])[\w$]*)*)")
_ALTERNATIVE = re.compile(r"\\?\|")
_LOOKUPS = {"workspaceSymbol", "goToDefinition", "findReferences"}
_NO_SERVER = "No LSP server available"
_LSP_DEFINITION = re.compile(r'"name"\s*:\s*"LSP"')


def _strip_heredocs(command: str) -> str:
    """`command` without the body and end line of each heredoc."""
    out = []
    pending: list[str] = []
    for line in command.split("\n"):
        if pending:
            if line.strip() == pending[0]:
                pending.pop(0)
            continue
        out.append(line)
        pending = [m.group(2) for m in _HEREDOC.finditer(line)]
    return "\n".join(out)


def _segments(command: str) -> list[list[str]]:
    """The simple commands of `command` as word lists, split at unquoted `|`,
    `&`, `;`, newlines and parentheses, with comments dropped. Each word keeps
    its quotes and backslashes. Raises ValueError on an unclosed quote."""
    text = _strip_heredocs(command)
    segments: list[list[str]] = [[]]
    word: list[str] = []
    quote = ""
    i = 0
    while i < len(text):
        char = text[i]
        if quote:
            word.append(char)
            if char == "\\" and quote == '"' and i + 1 < len(text):
                i += 1
                word.append(text[i])
            elif char == quote:
                quote = ""
        elif char == "\\":
            if text[i + 1:i + 2] != "\n":
                word.append(text[i:i + 2])
            i += 1
        elif char in "'\"":
            quote = char
            word.append(char)
        elif char == "#" and not word:
            newline = text.find("\n", i)
            if newline < 0:
                break
            i = newline
            continue
        elif char in _SEPARATORS or char in " \t\r":
            if word:
                segments[-1].append("".join(word))
                word = []
            if char in _SEPARATORS:
                segments.append([])
        else:
            word.append(char)
        i += 1
    if quote:
        raise ValueError("unclosed quote")
    if word:
        segments[-1].append("".join(word))
    return [s for s in segments if s]


def _unquote(word: str) -> str:
    if len(word) > 1 and word[0] == word[-1] and word[0] in "'\"":
        return word[1:-1]
    return word


def _command(word: str) -> str:
    return os.path.basename(_unquote(word.lstrip("`")))


def _search_words(words: list[str]) -> list[str] | None:
    """The words from the search tool on when `words` runs a search tool,
    None otherwise."""
    i = 0
    while i < len(words) and _ASSIGNMENT.match(words[i]):
        i += 1
    if i == len(words):
        return None
    name = _command(words[i])
    if name in _SEARCH_TOOLS:
        return words[i:]
    if name == "git":
        rest = words[i + 1:]
        while rest and rest[0].startswith("-"):
            rest = rest[2:] if rest[0] in ("-C", "-c") else rest[1:]
        return rest if rest[:1] == ["grep"] else None
    if name == "find":
        for j in range(i + 1, len(words) - 1):
            if words[j] in ("-exec", "-execdir") and _command(words[j + 1]) in _SEARCH_TOOLS:
                return words[j + 1:]
        return None
    if name in _WRAPPERS:
        option_argument = False
        for j in range(i + 1, len(words)):
            if _command(words[j]) in _SEARCH_TOOLS or _command(words[j]) in ("git", "find"):
                return _search_words(words[j:])
            if words[j].startswith("-"):
                option_argument = True
            elif option_argument:
                option_argument = False
            elif not (_ASSIGNMENT.match(words[j]) or _command(words[j]) in _WRAPPERS
                      or words[j].isdigit()):
                return None
    return None


def _names(text: str) -> list[str]:
    """The symbols whose definitions `text` names, with `def (a|b)` giving
    both a and b."""
    return [name for m in _DEFINITION.finditer(text) for name in _ALTERNATIVE.split(m.group(1))]


def _symbols(command: str) -> list[str]:
    """The symbols whose definitions the greps in `command` look for."""
    try:
        segments = _segments(command)
    except ValueError:
        return []
    names: list[str] = []
    for words in segments:
        if _command(words[0]) in _SHELLS:
            for j in range(1, len(words) - 1):
                if not words[j].startswith("-"):
                    break
                if re.fullmatch(r"-\w*c\w*", words[j]):
                    names += _symbols(_unquote(words[j + 1]))
                    break
            continue
        search = _search_words(words)
        if search:
            names += _names(" ".join(search))
    return names


def _symbol(command: str) -> str:
    """The first symbol whose definition a grep in `command` looks for, ""
    when the command runs no such grep."""
    return next(iter(_symbols(command)), "")


def _word_at(path: str, line, character) -> str:
    """The identifier at the 1-based `line` and `character` of the file at
    `path`, "" when there is none or the file cannot be read."""
    if not isinstance(line, int) or not isinstance(character, int) or not os.path.isfile(path):
        return ""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = next((t for number, t in enumerate(f, 1) if number == line), "")
    except OSError:
        return ""
    return next((m.group() for m in re.finditer(r"[\w$]+", text)
                 if m.start() < character <= m.end() + 1), "")


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
                message = entry.get("message") if isinstance(entry, dict) else None
                content = message.get("content") if isinstance(message, dict) else None
                if not isinstance(content, list):
                    continue
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if block.get("type") == "tool_use":
                        tool_input = block.get("input")
                        if not isinstance(tool_input, dict):
                            continue
                        operation = str(tool_input.get("operation") or "")
                        if block.get("name") == "LSP":
                            calls[str(block.get("id"))] = ("LSP", operation)
                            if operation in _LOOKUPS:
                                texts.append(str(tool_input.get("query") or ""))
                                texts.append(_word_at(os.path.join(str(entry.get("cwd") or ""),
                                                                   str(tool_input.get("filePath") or "")),
                                                      tool_input.get("line"), tool_input.get("character")))
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
    return _reason(_symbols(command if isinstance(command, str) else ""), transcript_path)


def grep_reason(pattern: str, transcript_path: str) -> str:
    """The deny reason for a Grep tool call whose pattern looks up the
    definition of a symbol the session has not looked up with LSP, "" to
    allow."""
    return _reason(_names(pattern if isinstance(pattern, str) else ""), transcript_path)


def _reason(names: list[str], transcript_path: str) -> str:
    if not names or not transcript_path:
        return ""
    lookups = _lsp_lookups(transcript_path)
    if lookups is None:
        return ""
    for name in names:
        named = re.compile(r"(?<![\w$])" + re.escape(name) + r"(?![\w$])")
        if not any(named.search(text) for text in lookups):
            return DENY_REASON.format(name=name)
    return ""
