ALTER TABLE slack_conversations ADD COLUMN proposed_ts TEXT NOT NULL DEFAULT '';
ALTER TABLE slack_conversation_messages ADD COLUMN text_dt TEXT NOT NULL DEFAULT '';
UPDATE slack_conversation_messages SET text_dt = created_at;
