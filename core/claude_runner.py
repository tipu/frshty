from core.llm import (
    configure,
    run_thinking as run_claude_code,
    run_balanced,
    run_fast as run_haiku,
    extract_json,
)

run_sonnet = run_balanced
