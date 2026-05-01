"""Generate structured tickets from a PRD section via claude -p."""
import core.log as log
from core.claude_runner import extract_json, run_haiku


GENERATOR_PROMPT = """You are a PRD-to-tickets generator.

Given one ## section of a PRD, produce one or more tickets sized to a single PR.

Each ticket MUST include:
- a short title (max 80 chars)
- a 1-2 sentence summary
- a longer description (the body of the ticket)
- structured acceptance criteria with playwright steps and tests_required for each criterion

Output a single JSON object inside a ```json fence with this exact shape:

{
  "tickets": [
    {
      "title": "...",
      "summary": "...",
      "description": "...",
      "acceptance_criteria": {
        "criteria": [
          {
            "id": "c1",
            "criterion": "...",
            "playwright": ["navigate to /...", "click button '...'", "assert URL contains /..."],
            "tests_required": ["unit: ...", "integration: ..."]
          }
        ]
      }
    }
  ]
}

Rules:
- Each ticket is sized to one PR. Split the section into multiple tickets if work is large.
- Acceptance criteria MUST be testable. playwright steps are imperative and concrete.
- If the section is informational only (no engineering work), return {"tickets": []}.
- Output JSON only, no prose, in the fence.

PRD SECTION:
"""


def generate(section: dict) -> list[dict]:
    """Returns list of {title, summary, description, acceptance_criteria} dicts.
    Empty list on failure or no tickets."""
    body = f"## {section['header']}\n{section['content']}"
    raw = run_haiku(GENERATOR_PROMPT + body, timeout=300)
    if not raw:
        log.emit("prd_generator_empty",
                 f"haiku returned empty for section '{section['header']}'",
                 meta={"stable_key": section["stable_key"]})
        return []
    parsed = extract_json(raw)
    if not parsed or not isinstance(parsed.get("tickets"), list):
        log.emit("prd_generator_parse_failed",
                 f"could not parse generator output for '{section['header']}'",
                 meta={"stable_key": section["stable_key"], "raw": raw[:500]})
        return []
    out: list[dict] = []
    for t in parsed["tickets"]:
        if not isinstance(t, dict):
            continue
        ac = t.get("acceptance_criteria") or {}
        if not isinstance(ac, dict) or not isinstance(ac.get("criteria"), list):
            continue
        ac["source_text"] = body
        t["acceptance_criteria"] = ac
        out.append(t)
    return out
