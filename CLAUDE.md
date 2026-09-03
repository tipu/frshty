When I give you a URL, use playwright cli to see and investigate

Count the browser commands a task needs before you run the first one. If it needs 3 or more, do not drive the browser here: send the whole block to the `browser-verifier` subagent in one Agent call, with the exact element text and the exact expected value written out first. Run blocks of 1 or 2 commands inline. Frontend verification on aimyable and nectar is almost always a 3+ block, so it goes to the subagent.

If a tool call inside a subagent returns "requires approval" or any permission denial, return that error to the parent immediately. Do NOT invoke `fewer-permission-prompts` or any other skill to "fix" permissions from inside a subagent — scanning transcripts and rewriting settings is way out of scope and produces large retry-heavy logs.
