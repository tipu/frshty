"""Structured acceptance criteria.

Shape stored in ticket data dict under `acceptance_criteria_json`:
    {
      "criteria": [
        {"id": "c1", "criterion": "...", "playwright": ["..."], "tests_required": ["..."]},
        ...
      ],
      "source_text": "<original verbatim author text>"
    }

source_text is the contract; structured fields are derivative.
"""
import json

import core.log as log
from core.claude_runner import extract_json, run_haiku

ENRICH_PROMPT = """You are translating an unstructured ticket description into structured acceptance criteria.

INPUT: free-form ticket text (description, AC bullets, anything the author wrote).

OUTPUT: a single JSON object with this exact shape:

{
  "criteria": [
    {
      "id": "c1",
      "criterion": "user-visible outcome in plain language",
      "playwright": ["navigate to /foo", "click button 'Bar'", "assert URL contains /baz"],
      "tests_required": ["unit: <what to assert>", "integration: <what to assert>"]
    }
  ]
}

Rules:
- Preserve the author's intent. Do not invent acceptance criteria the author did not state.
- Each criterion should be testable in isolation.
- playwright steps are imperative and concrete (one action per line).
- tests_required entries name the test layer + concise assertion.
- If the input has no testable criteria, return {"criteria": []}.
- Output JSON only, wrapped in a ```json fence. No prose.

INPUT:
"""


def is_structured(data: dict) -> bool:
    """True iff the ticket already has structured acceptance criteria."""
    j = data.get("acceptance_criteria_json")
    if not isinstance(j, dict):
        return False
    crit = j.get("criteria")
    return isinstance(crit, list) and len(crit) > 0


def enrich(source_text: str) -> dict | None:
    """Translate free-text into structured form. Returns the criteria object
    (without source_text), or None on failure. Caller wraps with source_text."""
    if not source_text or not source_text.strip():
        return None
    out = run_haiku(ENRICH_PROMPT + source_text)
    if not out:
        log.emit("enrich_failed", "haiku returned empty for acceptance enrichment")
        return None
    parsed = extract_json(out)
    if not parsed or not isinstance(parsed.get("criteria"), list):
        log.emit("enrich_parse_failed", "could not parse JSON criteria from haiku output",
                 meta={"raw": out[:500]})
        return None
    return parsed


def ensure_structured(data: dict) -> dict:
    """Run enrichment if the ticket lacks structured criteria. PRD-sourced
    tickets skip enrichment (they arrive structured). Mutates and returns
    the data dict. source_text is preserved verbatim."""
    if data.get("source") == "prd":
        return data
    if is_structured(data):
        return data
    src = data.get("acceptance_criteria_source_text") or data.get("description") or ""
    if not src.strip():
        return data
    if not data.get("acceptance_criteria_source_text"):
        data["acceptance_criteria_source_text"] = src
    parsed = enrich(src)
    if parsed is None:
        return data
    data["acceptance_criteria_json"] = {
        "criteria": parsed.get("criteria", []),
        "source_text": src,
    }
    return data


def all_criteria(data: dict) -> list[dict]:
    """Return the list of criterion dicts, or []."""
    j = data.get("acceptance_criteria_json")
    if not isinstance(j, dict):
        return []
    crit = j.get("criteria")
    return crit if isinstance(crit, list) else []


def serialize(data: dict) -> str | None:
    """JSON string for the acceptance_criteria_json blob, or None if absent."""
    j = data.get("acceptance_criteria_json")
    if not j:
        return None
    return json.dumps(j, default=str)
