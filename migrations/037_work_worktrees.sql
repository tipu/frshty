CREATE TABLE IF NOT EXISTS work_worktrees (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  work_item_id     INTEGER NOT NULL REFERENCES work_items(id),
  project_key      TEXT NOT NULL DEFAULT '',
  repo_name        TEXT NOT NULL,
  repo_path        TEXT NOT NULL,
  repo_common_dir  TEXT NOT NULL,
  path             TEXT NOT NULL,
  branch           TEXT NOT NULL,
  base_branch      TEXT NOT NULL,
  origin           TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  removed_at       TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_work_worktrees_item_repo
  ON work_worktrees(work_item_id, repo_common_dir);
CREATE INDEX IF NOT EXISTS idx_work_worktrees_common ON work_worktrees(repo_common_dir);
CREATE INDEX IF NOT EXISTS idx_work_worktrees_path ON work_worktrees(path);
