"""Generate structured tickets from a PRD section via claude -p."""
import core.log as log
from core.claude_runner import parse_structured, run_haiku, strict_object
from features.acceptance import CRITERIA_SCHEMA


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

GENERATOR_SCHEMA = strict_object({
    "tickets": {"type": "array", "items": strict_object({
        "title": {"type": "string"},
        "summary": {"type": "string"},
        "description": {"type": "string"},
        "acceptance_criteria": CRITERIA_SCHEMA,
    })},
})


def generate(section: dict) -> list[dict]:
    """Returns list of {title, summary, description, acceptance_criteria} dicts.
    Empty list on failure or no tickets."""
    body = f"## {section['header']}\n{section['content']}"
    raw = run_haiku(GENERATOR_PROMPT + body, timeout=300, schema=GENERATOR_SCHEMA)
    if not raw:
        log.emit("prd_generator_empty",
                 f"haiku returned empty for section '{section['header']}'",
                 meta={"stable_key": section["stable_key"]})
        return []
    parsed = parse_structured(raw, GENERATOR_SCHEMA, fn_name="prd_generator")
    if parsed is None:
        log.emit("prd_generator_parse_failed",
                 f"could not parse generator output for '{section['header']}'",
                 meta={"stable_key": section["stable_key"], "raw": raw[:500]})
        return []
    out: list[dict] = []
    for t in parsed["tickets"]:
        t["acceptance_criteria"]["source_text"] = body
        out.append(t)
    return out
