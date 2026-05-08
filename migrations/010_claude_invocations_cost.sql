ALTER TABLE claude_invocations ADD COLUMN cost_usd REAL;
ALTER TABLE claude_invocations ADD COLUMN input_tokens INTEGER;
ALTER TABLE claude_invocations ADD COLUMN output_tokens INTEGER;
ALTER TABLE claude_invocations ADD COLUMN cache_creation_input_tokens INTEGER;
ALTER TABLE claude_invocations ADD COLUMN cache_read_input_tokens INTEGER;
ALTER TABLE claude_invocations ADD COLUMN num_turns INTEGER;
