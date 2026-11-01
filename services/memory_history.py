"""A stale-entry check for Claude memory folders.

core/memory_git.py keeps the history of each folder. The index MEMORY.md
also drifts. One entry said to test frontend changes on
zicioxly, and another entry two lines below said zicioxly was retired.
`stale_entries` finds index entries that name something another entry
declares retired, entries that link a file that does not exist, and entries
that link the same file twice. `run` reports them on the event feed for the
operator to review. It changes no memory.
"""
import glob
import hashlib
import os
import re
from pathlib import Path

import core.log as log
import core.state as state
from core.terminal import agent_config_dir

MEMORY_CHECK_INTERVAL_MINUTES = 24 * 60
MEMORY_CHECK_BOARD_KEY = "frshty"
STATE_MODULE = "memory_check"
INDEX = "MEMORY.md"
RETIRE_WORDS = r"(?:retired|decommissioned)"
_LINK = re.compile(r"\]\(([^)\s]+\.md)\)")
_RETIRE = re.compile(r"\b" + RETIRE_WORDS + r"\b", re.I)
_CLAUSE_END = re.compile(r"[;:()\u2014\u2013]|\.\s|\s-\s")
_LIST_SEP = re.compile(r",|&|/|\band\b", re.I)
_DATED_NAME = re.compile(r"\b([A-Za-z][\w.-]*)\s+(?:on\s+)?\d{4}-\d{2}-\d{2}")
_AUX = {"was", "is", "were", "are", "am", "be", "been", "being", "has", "have", "had",
        "now", "got", "gets", "get", "also", "all", "both", "just", "since", "then"}
_LEAD = {"the", "a", "an", "our", "old"}
_PRONOUNS = {"it", "its", "they", "them", "this", "that", "these", "those", "we", "he", "she",
             "which", "who", "one"}
_STOP = {"retired", "decommissioned", "on"} | _AUX | _LEAD | _PRONOUNS


def _entries(index: Path) -> list[dict]:
    entries = []
    for number, line in enumerate(index.read_text(errors="replace").splitlines(), 1):
        if not line.lstrip().startswith("- "):
            continue
        links = _LINK.findall(line)
        entries.append({"line": number, "text": line.strip(), "file": links[0] if links else ""})
    return entries


def _subject(clause: str) -> set[str]:
    """The names in the clause that ends at a retirement word: "alpha and
    beta are now" gives alpha and beta."""
    words = clause.split()
    while words and words[-1].lower() in _AUX:
        words.pop()
    names = set()
    for part in _LIST_SEP.split(" ".join(words)):
        tokens = [t for t in (w.strip(".,`'\"*-") for w in part.split()) if any(c.isalnum() for c in t)]
        while tokens and tokens[0].lower() in _LEAD:
            tokens.pop(0)
        if 0 < len(tokens) <= 3 and any(c.isalpha() for c in tokens[-1]):
            names.add(" ".join(tokens).lower())
    return names


def _retired_names(text: str) -> set[str]:
    """The names an entry declares retired.

    "zicioxly retired 2026-09-21, kifindware 2026-09-08" declares zicioxly and
    kifindware, and "alpha and beta are retired" declares alpha and beta: the
    subject of the clause that ends at the retirement word, and each name
    followed by a date after it."""
    names = set()
    for match in _RETIRE.finditer(text):
        names |= _subject(_CLAUSE_END.split(text[:match.start()])[-1])
        tail = _CLAUSE_END.split(text[match.end():])[0]
        names.update(m.group(1).lower() for m in _DATED_NAME.finditer(tail))
    return {n for n in names if n not in _STOP}


def _mentions(text: str, name: str) -> bool:
    return re.search(rf"(?<![\w.-]){re.escape(name)}(?![\w-])", text, re.I) is not None


def stale_entries(directory: Path) -> list[dict]:
    """Index entries that disagree with another entry or point nowhere."""
    index = Path(directory) / INDEX
    if not index.is_file():
        return []
    entries = _entries(index)
    findings = []
    retired = {}
    for entry in entries:
        for name in _retired_names(entry["text"]):
            retired.setdefault(name, entry["line"])
    for entry in entries:
        declares = _retired_names(entry["text"])
        for name, line in sorted(retired.items()):
            if name in declares or not _mentions(entry["text"], name):
                continue
            findings.append({
                "kind": "retired", "line": entry["line"], "other_line": line, "name": name,
                "text": entry["text"],
                "detail": f"{INDEX}:{entry['line']} names {name}, which {INDEX}:{line} declares retired"})
    seen = {}
    for entry in entries:
        target = entry["file"]
        if not target:
            continue
        if not (Path(directory) / target).is_file():
            findings.append({
                "kind": "missing", "line": entry["line"], "name": target, "text": entry["text"],
                "detail": f"{INDEX}:{entry['line']} links {target}, which does not exist"})
        if target in seen:
            findings.append({
                "kind": "duplicate", "line": entry["line"], "other_line": seen[target],
                "name": target, "text": entry["text"],
                "detail": f"{INDEX}:{entry['line']} links {target}, which {INDEX}:{seen[target]} links too"})
        else:
            seen[target] = entry["line"]
    return sorted(findings, key=lambda f: f["line"])


def enabled(config: dict | None) -> bool:
    """Whether this instance runs the memory check. `[memory_check] enabled`
    decides; without it only the frshty board runs it."""
    config = config or {}
    setting = (config.get("memory_check") or {}).get("enabled")
    if setting is not None:
        return bool(setting)
    return (config.get("job") or {}).get("key") == MEMORY_CHECK_BOARD_KEY


def memory_dirs(config: dict | None) -> list[Path]:
    """Every memory folder with an index under the instance's Claude config dir."""
    root = os.path.expanduser(agent_config_dir(config or {}, "claude") or "~/.claude")
    found = glob.glob(os.path.join(root, "projects", "*", "memory", INDEX))
    return sorted(Path(p).parent for p in found)


def run(config: dict) -> list[dict]:
    """Report the stale entries of every memory folder on the event feed.

    A folder is reported again only when its findings change, so an entry the
    operator chose to keep does not come back every day."""
    reported = state.load(STATE_MODULE) or {}
    results = []
    for directory in memory_dirs(config):
        findings = stale_entries(directory)
        digest = hashlib.sha256("\n".join(f["detail"] for f in findings).encode()).hexdigest()
        key = str(directory)
        if reported.get(key) == digest:
            continue
        reported[key] = digest
        results.append({"dir": key, "findings": findings})
        if findings:
            log.emit("memory_stale_entries",
                     f"{len(findings)} memory entr{'y' if len(findings) == 1 else 'ies'} "
                     f"to review in {key}: " + "; ".join(f["detail"] for f in findings),
                     meta={"dir": key, "findings": findings})
    state.save(STATE_MODULE, reported)
    return results
