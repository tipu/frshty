DROP INDEX IF EXISTS idx_work_worktrees_item_repo;
CREATE UNIQUE INDEX IF NOT EXISTS idx_work_worktrees_item_repo
  ON work_worktrees(work_item_id, repo_common_dir) WHERE removed_at IS NULL;
