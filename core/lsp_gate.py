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

The work-314 scan of 361 denials found that the next tool call was another
Bash call 160 times and an LSP call only 111 times. 338 of the denied
commands were compound, such as `sed ...; grep -n "def X" f; sed ...`, and
115 of the Bash follow-ups ran the rest of the command again, often with
the grep for X rewritten without `def`. So when a compound command splits
safely into statements, bash_rewrite replaces only the refused grep with an
echo of REMOVED_NOTICE and the rest runs; the hook allows the rewritten
command. Any other refused command is denied with a message that says
nothing ran and asks for the LSP call first. After a denial or a removal
for X, every grep that searches for X as a word is refused until an LSP
lookup names X. About 40 denials were listings such as `grep -n "def test_" f`;
a prefix (`def test_`, `def test`, `def get_.*`) names no symbol.
"""
import json
import os
import re

GATE_ENV = "FRSHTY_LSP_GATE"
_NEXT_STEP = (
    "Nothing in this command ran. Make your next tool call this one: LSP with "
    "operation workspaceSymbol, query \"{name}\", any file of the project as "
    "filePath, line 1 and character 1. If LSP is not loaded yet, load it first "
    "with ToolSearch, query \"select:LSP\", and then make that call. It "
    "returns the file and line of `{name}`; read them with Read. Use "
    "findReferences for the callers of `{name}` and goToDefinition for its "
    "definition. After the LSP call, run the rest of this command again "
    "without the grep. You may send both in one response, the LSP call "
    "first. Do not grep for `{name}` in another form: every grep that "
    "searches for `{name}` is refused until an LSP workspaceSymbol, "
    "goToDefinition or findReferences call names `{name}`. When LSP answers "
    "\"No LSP server available\" or ToolSearch finds no LSP tool, the gate "
    "opens for the rest of the session."
)

DENY_REASON = (
    "Blocked by the LSP gate: this grep looks up the definition of `{name}`, "
    "and this session has not looked up `{name}` with LSP. " + _NEXT_STEP
)

REMOVED_NOTICE = (
    "[LSP gate] The grep for `{name}` in this command did not run; the rest "
    "of the command ran. This session has not looked up `{name}` with LSP. "
    "Make your next tool call LSP with operation workspaceSymbol, query "
    "\"{name}\", any file of the project as filePath, line 1 and character "
    "1. If LSP is not loaded yet, load it first with ToolSearch, query "
    "\"select:LSP\". Every grep that searches for `{name}` is refused until "
    "an LSP lookup names `{name}`."
)

REPEAT_REASON = (
    "Blocked by the LSP gate: this grep searches for `{name}`, and the gate "
    "refused a definition grep for `{name}` earlier in this session. " + _NEXT_STEP
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
_PREFIX = re.compile(r"\.|\*|\+|\[|\\\\?w")
_REMOVED = re.compile(r'\[LSP gate\] The grep for `[^`]+` in this command did not run.*?'
                      r'operation workspaceSymbol, query "([\w$]+)"', re.S)
_HARMLESS_REDIRECT = re.compile(r"2>&1|&>\s*/dev/null|[12]?>\s*/dev/null")
_FILTERS = {"cat", "find", "ls", "head", "tail", "sort", "uniq", "wc", "cut", "sed", "nl", "tr", "column"}
_EXIT_STATUS = re.compile(r"\b(?:set|exec|eval|source|trap|pipefail|errexit)\b|\$[?{(]|`|>&[3-9]|(?:^|[;&|\n])\s*\.\s")
_VARIABLE = re.compile(r"\$[\w!#@*_-]")
_SINGLE_QUOTED = re.compile(r"'[^']*'")
_GIT_PAGER = re.compile(r"-[A-Za-z]*O.*")
_SORT_OPTION = re.compile(r"-[a-np-zA-Z0-9]+")
_RUNS_A_PROGRAM = ("-O", "--open-files-in-pager", "--pre", "--pager", "-delete", "-fprint", "-fls")
_FIND_ACTIONS = ("-delete", "-exec", "-ok", "-fprint", "-fls")
_SED_PRINT = re.compile(r"\d+(?:,(?:\d+|\$))?p|\$p")
_KEYWORDS = {"if", "then", "elif", "else", "fi", "while", "until", "for", "do", "done",
             "case", "esac", "function", "select", "!", "[[", "time", "coproc"}
_REFUSED = re.compile(r'(?:PreToolUse:\w+ hook error: )?Blocked by the LSP gate:.*?'
                      r'operation workspaceSymbol, query "([\w$]+)"', re.S)
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
    both a and b. A prefix that lists definitions, such as `def test_`,
    `def test.*check` or `def get_[a-z]*`, names none."""
    return [name for m in _DEFINITION.finditer(text)
            if not _PREFIX.match(text, m.end())
            for name in _ALTERNATIVE.split(m.group(1))
            if name != "test" and not name.endswith("_")]


def _searches(command: str) -> list[str]:
    """The text of each grep in `command`, from the search tool on."""
    try:
        segments = _segments(command)
    except ValueError:
        return []
    texts: list[str] = []
    for words in segments:
        if _command(words[0]) in _SHELLS:
            for j in range(1, len(words) - 1):
                if not words[j].startswith("-"):
                    break
                if re.fullmatch(r"-\w*c\w*", words[j]):
                    texts += _searches(_unquote(words[j + 1]))
                    break
            continue
        search = _search_words(words)
        if search:
            texts.append(" ".join(search))
    return texts


def _symbols(command: str) -> list[str]:
    """The symbols whose definitions the greps in `command` look for."""
    return [name for text in _searches(command) for name in _names(text)]


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


def _lsp_lookups(transcript_path: str) -> tuple[list[str], list[str]] | None:
    """The query and result text of each LSP lookup in the transcript, and
    the symbols the gate refused a grep for. None when the gate is open for
    the whole session: the transcript cannot be read, an LSP result says no
    server is available, or a ToolSearch for LSP found no LSP tool."""
    calls: dict[str, tuple[str, str]] = {}
    texts: list[str] = []
    refused: list[str] = []
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
                        result = block.get("content")
                        text = _result_text(result)
                        refusal = _REFUSED.match(text.strip()) if block.get("is_error") else None
                        if refusal:
                            refused.append(refusal.group(1))
                        refused += _REMOVED.findall(text)
                        call = calls.get(str(block.get("tool_use_id")))
                        if not call:
                            continue
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
    return texts, refused


def bash_reason(command: str, transcript_path: str) -> str:
    """The deny reason for a definition grep for a symbol the session has not
    looked up with LSP, or for any grep for a symbol the gate refused a
    definition grep for, "" to allow."""
    return _reason(_searches(command if isinstance(command, str) else ""), transcript_path)


def grep_reason(pattern: str, transcript_path: str) -> str:
    """The deny reason for a Grep tool call whose pattern looks up the
    definition of a symbol the session has not looked up with LSP, or
    searches for a symbol the gate refused a definition grep for, "" to
    allow."""
    return _reason([pattern] if isinstance(pattern, str) and pattern else [], transcript_path)


def _named(name: str) -> re.Pattern:
    return re.compile(r"(?<![\w$])" + re.escape(name) + r"(?![\w$])")


def _reason(searches: list[str], transcript_path: str) -> str:
    if not searches or not transcript_path:
        return ""
    state = _lsp_lookups(transcript_path)
    if state is None:
        return ""
    template, name = _refusal(searches, state)
    return template.format(name=name) if template else ""


def _refusal(searches: list[str], state: tuple[list[str], list[str]]) -> tuple[str, str]:
    """The deny template and the symbol it names for `searches`, ("", "")
    to allow."""
    lookups, refused = state
    for name in [name for text in searches for name in _names(text)]:
        if not any(_named(name).search(text) for text in lookups):
            return DENY_REASON, name
    for name in dict.fromkeys(refused):
        if any(_named(name).search(text) for text in searches) and \
                not any(_named(name).search(text) for text in lookups):
            return REPEAT_REASON, name
    return "", ""


def _statements(command: str) -> list[tuple[str, str]] | None:
    """The top-level statements of `command` with the separator after each,
    `;`, a newline, `&&` or "" at the end. None when the command holds
    something this split does not model: a subshell, a group, a command
    substitution, a heredoc, a comment, `||`, a background `&`, an
    unclosed quote or a shell keyword."""
    statements: list[tuple[str, str]] = []
    start = 0
    quote = ""
    i = 0
    while i < len(command):
        char = command[i]
        if quote:
            if char == "\\" and quote == '"':
                i += 1
            elif char == quote:
                quote = ""
        elif char == "\\":
            i += 1
        elif char in "'\"":
            quote = char
        elif char in "(){}`#" or command.startswith("<<", i) or command.startswith("||", i):
            return None
        elif command.startswith("&&", i):
            statements.append((command[start:i], "&&"))
            i += 1
            start = i + 1
        elif char == "&" and not (command[i - 1:i] == ">" or command[i + 1:i + 2] == ">"):
            return None
        elif char in ";\n":
            statements.append((command[start:i], char))
            start = i + 1
        i += 1
    if quote:
        return None
    statements.append((command[start:], ""))
    for text, _ in statements:
        words = text.split()
        if words and words[0] in _KEYWORDS:
            return None
    return statements


def _stage_reads_only(words: list[str]) -> bool:
    """Whether one pipeline stage only reads: a search that runs no other
    program, or a filter from _FILTERS whose every argument is on its
    allowlist."""
    rest = [_unquote(w) for w in words if not _ASSIGNMENT.match(w)]
    if not rest:
        return False
    name = _command(rest[0])
    search = _search_words(words)
    if search is not None:
        if name not in _SEARCH_TOOLS | {"git", "xargs", "find"}:
            return False
        if any(w.startswith(_RUNS_A_PROGRAM) for w in rest) or \
                (name == "git" and any(w == "-c" or _GIT_PAGER.fullmatch(w) for w in rest)):
            return False
        actions = [w for w in rest if w.startswith(("-exec", "-ok"))]
        return name != "find" or len(actions) == 1
    if name not in _FILTERS:
        return False
    arguments = rest[1:]
    if name == "sed":
        return all(w == "-n" or _SED_PRINT.fullmatch(w) or (not w.startswith("-") and w == arguments[-1]
                                                             and len(arguments) > 1)
                   for w in arguments) and any(_SED_PRINT.fullmatch(w) for w in arguments)
    if name == "find":
        return not any(w.startswith(_FIND_ACTIONS) for w in arguments)
    if name in ("sort", "uniq"):
        return all(_SORT_OPTION.fullmatch(w) for w in arguments)
    return True


def _removable(statement: str) -> bool:
    """Whether every stage of the pipeline `statement` only reads, so
    dropping the statement drops nothing but its output. A nested shell, a
    file write, `tee`, `sort -o` or `sed -i` is not."""
    text = _HARMLESS_REDIRECT.sub(" ", statement)
    if ">" in text:
        return False
    try:
        segments = _segments(text)
    except ValueError:
        return False
    return all(_stage_reads_only(words) for words in segments)


def bash_rewrite(command: str, transcript_path: str) -> str:
    """`command` with each refused grep statement replaced by an echo of
    REMOVED_NOTICE, so the rest of the command runs. "" when the command
    cannot be split safely, when it holds `set`, `exec`, `eval`, `source`,
    a shell variable outside single quotes, `$?`, `${`, `$(`, a backtick, a descriptor above 2, pipefail or trap,
    when a refused statement is followed by `&&` or does
    more than search and filter, or when nothing but `cd` would be left to
    run."""
    statements = _statements(command if isinstance(command, str) else "")
    if not statements or not transcript_path or _EXIT_STATUS.search(command) or \
            _VARIABLE.search(_SINGLE_QUOTED.sub("", command)):
        return ""
    state = _lsp_lookups(transcript_path)
    if state is None:
        return ""
    parts: list[str] = []
    removed = kept = 0
    for text, separator in statements:
        template, name = _refusal(_searches(text), state)
        if template:
            if separator == "&&" or not _removable(text):
                return ""
            removed += 1
            indent = text[:len(text) - len(text.lstrip())]
            parts.append(indent + "echo '" + REMOVED_NOTICE.format(name=name) + "'" + separator)
            continue
        words = text.split()
        if words and words[0] != "cd":
            kept += 1
        parts.append(text + separator)
    return "".join(parts) if removed and kept else ""
