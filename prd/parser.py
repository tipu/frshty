"""Strict ## section parser for PRD markdown.

A section starts at a line beginning with `## ` and ends at the next `## ` or EOF.
The section header is the text after `## `. Content is everything after the
header line up to the next section. A `stable_key` is derived as a slug of the
header so that section identity survives moderate edits to the header text.

Higher-level headings (`#`) are treated as preamble and ignored. Code-fenced
blocks are NOT scanned for headers (a `## ` inside ```...``` is content, not a
new section).
"""
import hashlib
import re

_SLUG = re.compile(r"[^a-z0-9]+")


def slug(header: str) -> str:
    s = _SLUG.sub("-", header.strip().lower()).strip("-")
    return s or "section"


def hash_content(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def parse(text: str) -> list[dict]:
    """Returns a list of {stable_key, header, content, content_hash} dicts."""
    sections: list[dict] = []
    lines = text.splitlines(keepends=True)
    i = 0
    n = len(lines)
    in_fence = False
    current_header: str | None = None
    current_content: list[str] = []

    def flush() -> None:
        if current_header is None:
            return
        body = "".join(current_content).rstrip("\n")
        sections.append({
            "stable_key": slug(current_header),
            "header": current_header.strip(),
            "content": body,
            "content_hash": hash_content(body),
        })

    while i < n:
        line = lines[i]
        stripped = line.lstrip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            if current_header is not None:
                current_content.append(line)
            i += 1
            continue
        if not in_fence and line.startswith("## ") and not line.startswith("### "):
            flush()
            current_header = line[3:].rstrip("\n")
            current_content = []
            i += 1
            continue
        if current_header is not None:
            current_content.append(line)
        i += 1
    flush()

    seen: dict[str, int] = {}
    for s in sections:
        key = s["stable_key"]
        if key in seen:
            seen[key] += 1
            s["stable_key"] = f"{key}-{seen[key]}"
        else:
            seen[key] = 1
    return sections
