"""Refuse a symbol grep from a task session that has not loaded the LSP tool.

Every claude task carries llm.LSP_RULE, which asks for workspaceSymbol before
grep. LSP is a deferred tool, so a session must call ToolSearch once before
LSP exists for it. A scan of the nine task sessions after the rule shipped
found zero ToolSearch calls for LSP and zero LSP calls. The same sessions ran
greps such as `grep -n "class OpenCodeProvider" core/llm.py` and
`grep -rn "def parse_structured" frshty/core/`, which is the position lookup
workspaceSymbol answers. The prompt is a request; this gate is the constraint.

The gate denies a grep whose text names a definition (`def X`, `class X`,
`function X`, `interface X`) until the transcript shows a ToolSearch query
that names LSP or an LSP call. One such call opens the gate for the rest of
the session, so a session whose files have no LSP server, or whose ToolSearch
finds no LSP tool, pays one call and then greps freely. A transcript that
cannot be read opens the gate: a missed nudge costs a grep, not a defect.
"""
import json
import re

DENY_REASON = (
    "Blocked by the LSP gate: this grep looks up the definition of `{name}`, "
    "and this session has not loaded the LSP tool. Load it once with "
    "ToolSearch, query \"select:LSP\". Then call LSP with operation "
    "workspaceSymbol, query \"{name}\", and any file of the project as "
    "filePath with line 1 and character 1. It returns the file and line of "
    "the symbol. Use findReferences for its callers and goToDefinition for "
    "its definition. The gate opens for the rest of the session after the "
    "ToolSearch call, so when ToolSearch finds no LSP tool or LSP answers "
    "\"No LSP server available\", run the grep again."
)

_SEARCH = re.compile(r"(?<![\w-])(grep|egrep|rg|ag|ack)(?![\w-])")
_DEFINITION = re.compile(
    r"\b(?:def|class|function|interface)\s+([A-Za-z_$][\w$]*)")


def _symbol(command: str) -> str:
    """The symbol whose definition a grep in `command` looks for, "" when the
    command is not such a grep."""
    if not _SEARCH.search(command):
        return ""
    match = _DEFINITION.search(command)
    return match.group(1) if match else ""


def _loaded_lsp(transcript_path: str) -> bool:
    """Whether the transcript holds a ToolSearch query naming LSP or an LSP
    call. True when the transcript cannot be read."""
    try:
        with open(transcript_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if "LSP" not in line or '"tool_use"' not in line:
                    continue
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                content = (entry.get("message") or {}).get("content")
                if not isinstance(content, list):
                    continue
                for block in content:
                    if not isinstance(block, dict) or block.get("type") != "tool_use":
                        continue
                    if block.get("name") == "LSP":
                        return True
                    query = str((block.get("input") or {}).get("query") or "")
                    if block.get("name") == "ToolSearch" and "LSP" in query:
                        return True
    except OSError:
        return True
    return False


def bash_reason(command: str, transcript_path: str) -> str:
    """The deny reason for a symbol grep in a session that has not loaded
    LSP, "" to allow."""
    name = _symbol(command or "")
    if not name or not transcript_path or _loaded_lsp(transcript_path):
        return ""
    return DENY_REASON.format(name=name)
