-- Ticket pull request comments moved from pr_comments.json into comment_state.
-- The ticket pages show each comment with its text, its place in the code, the
-- reply frshty drafted and the evidence run on that reply.
ALTER TABLE comment_state ADD COLUMN body TEXT;
ALTER TABLE comment_state ADD COLUMN path TEXT;
ALTER TABLE comment_state ADD COLUMN line INTEGER;
ALTER TABLE comment_state ADD COLUMN url TEXT;
ALTER TABLE comment_state ADD COLUMN comment_kind TEXT;
ALTER TABLE comment_state ADD COLUMN diff_hunk TEXT;
ALTER TABLE comment_state ADD COLUMN created_at TEXT;
ALTER TABLE comment_state ADD COLUMN suggested_reply TEXT;
ALTER TABLE comment_state ADD COLUMN defence TEXT;
