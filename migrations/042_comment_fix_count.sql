ALTER TABLE comment_state ADD COLUMN fix_count INTEGER NOT NULL DEFAULT 0;
-- Rows written before this column existed carry no record of whether frshty
-- answered them with a push. Count a processed row as answered: a bot that
-- rewrites a comment frshty already finished with must not get one more free
-- pass through the fix pipeline the moment this ships.
UPDATE comment_state SET fix_count = 1 WHERE state = 'processed';
