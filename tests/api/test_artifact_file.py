import os

from fastapi.responses import FileResponse

import core.db as db
from services import work_store
from web import work as work_routes


def _artifact(item_id: int, path: str) -> int:
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_artifacts(work_item_id, work_run_id, path, note, created_at) "
            "VALUES (?, NULL, ?, '', '2026-08-26T00:00:00Z')",
            (item_id, path))
        return int(cur.lastrowid)


def _seed(tmp_path, monkeypatch, artifact_dir: str):
    run_cwd = tmp_path / "run-workspace"
    other_root = tmp_path / "other-workspace"
    for d in (run_cwd, other_root / "tickets" / "DEV-1" / "docs", tmp_path / "elsewhere"):
        os.makedirs(d, exist_ok=True)
    item_id = work_store.create_item("artifact route test")
    work_store.add_run(item_id, f"sid-artifact-{item_id}", "work-artifact", str(run_cwd))
    target = tmp_path / artifact_dir / "note.md"
    target.write_text("hello")
    monkeypatch.setattr(work_routes.work_launch, "project_entries",
                        lambda: [{"key": "other", "root": str(other_root), "repos": []}])
    monkeypatch.setattr(work_routes, "_SCRATCH_ROOT", str(tmp_path / "scratch") + os.sep)
    return _artifact(item_id, str(target))


class TestArtifactFileRoute:
    def test_serves_file_in_another_project_root(self, tmp_path, monkeypatch):
        artifact_id = _seed(tmp_path, monkeypatch, "other-workspace/tickets/DEV-1/docs")
        resp = work_routes.api_work_artifact_file(artifact_id)
        assert isinstance(resp, FileResponse)

    def test_serves_file_in_the_run_cwd(self, tmp_path, monkeypatch):
        artifact_id = _seed(tmp_path, monkeypatch, "run-workspace")
        resp = work_routes.api_work_artifact_file(artifact_id)
        assert isinstance(resp, FileResponse)

    def test_rejects_file_outside_every_root(self, tmp_path, monkeypatch):
        artifact_id = _seed(tmp_path, monkeypatch, "elsewhere")
        resp = work_routes.api_work_artifact_file(artifact_id)
        assert resp.status_code == 403
        assert b"outside the run's workspace" in resp.body
