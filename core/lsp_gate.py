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
the grep for X rewritten without `def`. So the deny message says that
nothing ran and asks for the LSP call first, and after a denial for X every
grep that searches for X as a word is refused until an LSP lookup names X.

For one day the gate instead removed only the refused grep from a compound
command, ran the rest and printed a notice. The agents read the notice as
output and went on: 25 of 111 removals were followed by an LSP call, while
35 of 53 denials with the message above were. So the gate denies again. A
notice from that day still counts as a refusal for the repeat rule. About 40 denials were listings such as `grep -n "def test_" f`;
a prefix (`def test_`, `def test`, `def get_.*`) names no symbol.

workspaceSymbol matches every symbol that contains the query. The work-315
scan of 321 workspaceSymbol calls found that a query of 8 characters or
fewer returned more than 50 symbols in 40 of 59 calls, and a longer query in
7 of 262: the query "loop" returned 531 symbols. So for a name of SHORT_NAME
characters or fewer, the gate asks for documentSymbol on the file the grep
names, and a documentSymbol result that names X opens the gate for X. When
such a grep names no file, workspaceSymbol cannot narrow the name, and the
gate allows the grep.
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
    "documentSymbol, goToDefinition or findReferences call names `{name}`. "
    "When LSP answers "
    "\"No LSP server available\" or ToolSearch finds no LSP tool, the gate "
    "opens for the rest of the session."
)

DENY_REASON = (
    "Blocked by the LSP gate: this grep looks up the definition of `{name}`, "
    "and this session has not looked up `{name}` with LSP. " + _NEXT_STEP
)

REPEAT_REASON = (
    "Blocked by the LSP gate: this grep searches for `{name}`, and the gate "
    "refused a definition grep for `{name}` earlier in this session. " + _NEXT_STEP
)

SHORT_NAME = 8
_SHORT = (
    "`{name}` is a short name, and workspaceSymbol returns every symbol that "
    "contains it: the query \"loop\" returned 531 symbols. "
)
_SHORT_NEXT_STEP = (
    "Nothing in this command ran. " + _SHORT + "Make your next tool call this "
    "one: LSP with operation documentSymbol, filePath \"{file}\", line 1 and "
    "character 1. If LSP is not loaded yet, load it first with ToolSearch, "
    "query \"select:LSP\", and then make that call. It lists the symbols of "
    "{file} with their lines; read the line of `{name}` with Read. When "
    "`{name}` is not in that list, {file} does not define it, and a grep for "
    "`{name}` across a directory runs. When you know a line and character "
    "where `{name}` is used, goToDefinition at that position also finds the "
    "definition. After the LSP call, run the rest of this command again "
    "without the grep. You may send both in one response, the LSP call "
    "first. Every grep of a file that searches for `{name}` is refused until "
    "an LSP documentSymbol, workspaceSymbol, goToDefinition or findReferences "
    "call names `{name}`."
)

SHORT_DENY_REASON = (
    "Blocked by the LSP gate: this grep looks up the definition of `{name}`, "
    "and this session has not looked up `{name}` with LSP. " + _SHORT_NEXT_STEP
)

SHORT_REPEAT_REASON = (
    "Blocked by the LSP gate: this grep searches for `{name}`, and the gate "
    "refused a definition grep for `{name}` earlier in this session. " + _SHORT_NEXT_STEP
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
_REMOVED = re.compile(r"\[LSP gate\] The grep for `([\w$]+)` in this command did not run; "
                      r"the rest of the command ran\.")
_KEYWORDS = {"if", "then", "elif", "else", "fi", "while", "until", "for", "do", "done",
             "case", "esac", "function", "select", "!", "[[", "time", "coproc"}
_REFUSED = re.compile(r"(?:PreToolUse:\w+ hook error: )?Blocked by the LSP gate: this grep "
                      r"(?:looks up the definition of|searches for) `([\w$]+)`")
_LOOKUPS = {"workspaceSymbol", "goToDefinition", "findReferences"}
_RESULT_LOOKUPS = _LOOKUPS | {"documentSymbol"}
_PATTERN_OPTIONS = {"-e", "-f", "--regexp", "--file"}
_SHORT_ARGUMENTS = {"grep": "ABCmdD", "egrep": "ABCmdD", "fgrep": "ABCmdD", "rg": "ABCmgtTMj"}
_ARGUMENT_OPTIONS = {"--glob", "--iglob", "--type", "--type-not", "--max-count", "--context",
                     "--after-context", "--before-context", "--include", "--exclude",
                     "--exclude-dir", "--threads", "--max-depth"}
_FILE = re.compile(r"[\w./ -]*[\w-]\.[\w-]+")
_FILE_CHARS = re.compile(r"[\w./~ -]+")
_REDIRECT = re.compile(r"\d*(?:>>?|&>>?|<)&?")
_ATTACHED_REDIRECT = re.compile(r"((?:\"[^\"]*\"|'[^']*'|[^'\"<>])+?)(\d*&?[<>].*)")
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
    return [text for text, _ in _searches_in(command, "")[0]]


def _changed_directory(directory: str | None, target: str | None) -> str | None:
    """`directory` after a `cd` to `target`, None when either is unknown."""
    if directory is None or not target or target.startswith("-") or \
            not _FILE_CHARS.fullmatch(target):
        return None
    return os.path.join(directory, os.path.expanduser(target))


def _searches_in(command: str, directory: str | None
                 ) -> tuple[list[tuple[str, str | None]], str | None]:
    """The text of each grep in `command` that starts in `directory` with
    the directory it runs in, relative to the session directory, after each
    `cd` and `git -C` before it, and the directory after `command`. A
    directory is None when a `cd` goes somewhere the text does not name."""
    try:
        segments = _segments(command)
    except ValueError:
        return [], directory
    found: list[tuple[str, str | None]] = []
    grouped = _statements(command) is None
    for words in segments:
        if _command(words[0]) == "cd":
            directory = None if grouped else _changed_directory(
                directory, _unquote(words[1]) if len(words) == 2 else None)
            continue
        if _command(words[0]) in _SHELLS:
            for j in range(1, len(words) - 1):
                if not words[j].startswith("-"):
                    break
                if re.fullmatch(r"-\w*c\w*", words[j]):
                    found += _searches_in(_unquote(words[j + 1]), directory)[0]
                    break
            continue
        search = _search_words(words)
        if search:
            here = directory
            options = words[1:words.index("grep")] if _command(words[0]) == "git" else []
            if "-C" in options[:-1]:
                here = _changed_directory(directory, _unquote(options[options.index("-C") + 1]))
            found.append((" ".join(search), here))
    return found, directory


def _is_file(path: str) -> bool:
    """Whether `path` names one file: a plain path, not a glob or a quoted
    word, that is a file on disk, or that does not exist and whose last part
    has an extension."""
    if not _FILE_CHARS.fullmatch(path):
        return False
    if os.path.exists(path):
        return os.path.isfile(path)
    return bool(_FILE.fullmatch(path)) and "." in os.path.basename(path).lstrip(".")


def _file(text: str, directory: str = "") -> str:
    """The first file that the grep `text` run in `directory` searches, as a
    path from the session directory, "" when it names none: it reads stdin
    or searches only directories or globs."""
    try:
        tool, *raw = _segments(text)[0]
    except (ValueError, IndexError):
        return ""
    raw = [part for quoted in raw for part in
           (_ATTACHED_REDIRECT.fullmatch(quoted).groups() if _ATTACHED_REDIRECT.fullmatch(quoted)
            else (quoted,))]
    arguments = _SHORT_ARGUMENTS.get(_command(tool), "ABCm")
    positional: list[str] = []
    inputs: list[str] = []
    pattern_given = skip = ended = False
    redirect = ""
    for quoted in raw:
        word = _unquote(quoted)
        if skip:
            skip = False
        elif redirect:
            if redirect == "<":
                inputs.append(word)
            redirect = ""
        elif _REDIRECT.match(quoted):
            operator = _REDIRECT.match(quoted).group()
            target = _unquote(quoted[len(operator):])
            if not target:
                redirect = operator
            elif operator == "<":
                inputs.append(target)
        elif ended or not word.startswith("-") or word == "-":
            positional.append(word)
        elif word == "--":
            ended = True
        elif word.startswith("--"):
            if word in _PATTERN_OPTIONS:
                pattern_given = skip = True
            elif word.startswith(("--regexp=", "--file=")):
                pattern_given = True
            elif word in _ARGUMENT_OPTIONS:
                skip = True
        else:
            for k, letter in enumerate(word[1:], 1):
                if letter in "ef" or letter in arguments:
                    pattern_given = pattern_given or letter in "ef"
                    skip = k == len(word) - 1
                    break
    paths = [os.path.join(directory, p) for p in positional[0 if pattern_given else 1:] + inputs]
    return next((os.path.normpath(p) for p in paths if _is_file(p)), "")


def _short(name: str) -> bool:
    return len(name) <= SHORT_NAME


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
                        elif call[1] in _RESULT_LOOKUPS:
                            texts.append(text)
    except OSError:
        return None
    return texts, refused


def bash_reason(command: str, transcript_path: str) -> str:
    """The deny reason for a definition grep for a symbol the session has not
    looked up with LSP, or for any grep for a symbol the gate refused a
    definition grep for, "" to allow."""
    return _reason(_located(command if isinstance(command, str) else "")[0], transcript_path)


def grep_reason(pattern: str, transcript_path: str, path: str = "") -> str:
    """The deny reason for a Grep tool call whose pattern looks up the
    definition of a symbol the session has not looked up with LSP, or
    searches for a symbol the gate refused a definition grep for, "" to
    allow. `path` is the path the call searches."""
    if not isinstance(pattern, str) or not pattern:
        return _reason([], transcript_path)
    return _reason([(pattern, path if isinstance(path, str) and _is_file(path) else "")],
                   transcript_path)


def _located(command: str, directory: str | None = ""
             ) -> tuple[list[tuple[str, str]], str | None]:
    """The text of each grep in `command` that starts in `directory` with
    the first file it searches, as a path from the session directory, ""
    when it names none or a `cd` before it went somewhere the text does not
    name, and the directory after `command`."""
    found, after = _searches_in(command, directory)
    return [(text, _file(text, here) if here is not None else "") for text, here in found], after


def _named(name: str) -> re.Pattern:
    return re.compile(r"(?<![\w$])" + re.escape(name) + r"(?![\w$])")


def _reason(searches: list[tuple[str, str]], transcript_path: str) -> str:
    if not searches or not transcript_path:
        return ""
    state = _lsp_lookups(transcript_path)
    if state is None:
        return ""
    kind, name, file = _refusal(searches, state)
    if not kind:
        return ""
    templates = {"deny": (DENY_REASON, SHORT_DENY_REASON),
                 "repeat": (REPEAT_REASON, SHORT_REPEAT_REASON)}[kind]
    return templates[_short(name)].format(name=name, file=file)


def _refusal(searches: list[tuple[str, str]],
             state: tuple[list[str], list[str]]) -> tuple[str, str, str]:
    """The kind of refusal, "deny" or "repeat", the symbol it names and the
    file of its grep for `searches`, ("", "", "") to allow. A short name
    in a grep that names no file is allowed."""
    lookups, refused = state

    def looked_up(name: str) -> bool:
        return any(_named(name).search(text) for text in lookups)

    for text, file in searches:
        for name in _names(text):
            if (file or not _short(name)) and not looked_up(name):
                return "deny", name, file
    for name in dict.fromkeys(refused):
        if looked_up(name):
            continue
        for text, file in searches:
            if (file or not _short(name)) and _named(name).search(text):
                return "repeat", name, file
    return "", "", ""


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
