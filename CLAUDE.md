When I give you a URL, use playwright cli to see and investigate

If a tool call inside a subagent returns "requires approval" or any permission denial, return that error to the parent immediately. Do NOT invoke `fewer-permission-prompts` or any other skill to "fix" permissions from inside a subagent — scanning transcripts and rewriting settings is way out of scope and produces large retry-heavy logs.
