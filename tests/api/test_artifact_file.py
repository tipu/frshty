import os

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.testclient import TestClient

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


def _seed_report(tmp_path, monkeypatch):
    run_cwd = tmp_path / "run-workspace"
    os.makedirs(run_cwd / "docs", exist_ok=True)
    os.makedirs(tmp_path / "elsewhere", exist_ok=True)
    (tmp_path / "elsewhere" / "secret.txt").write_text("secret")
    item_id = work_store.create_item("artifact report test")
    work_store.add_run(item_id, f"sid-report-{item_id}", "work-artifact", str(run_cwd))
    report = run_cwd / "docs" / "report.html"
    report.write_text('<img src="shot.png">')
    (run_cwd / "docs" / "shot.png").write_text("png-bytes")
    monkeypatch.setattr(work_routes.work_launch, "project_entries",
                        lambda: [{"key": "other", "root": "", "repos": []}])
    monkeypatch.setattr(work_routes, "_SCRATCH_ROOT", str(tmp_path / "scratch") + os.sep)
    return _artifact(item_id, str(report))


class TestArtifactStoreRoute:
    def test_serves_a_file_in_the_artifact_store(self, tmp_path, monkeypatch):
        store = tmp_path / "artifact-store"
        os.makedirs(store / "work-1", exist_ok=True)
        page = store / "work-1" / "report.html"
        page.write_text("<p>hi</p>")
        item_id = work_store.create_item("artifact store route test")
        work_store.add_run(item_id, f"sid-store-{item_id}", "work-artifact",
                           str(tmp_path / "run-workspace"))
        monkeypatch.setattr(work_routes.work_launch, "project_entries", lambda: [])
        monkeypatch.setattr(work_routes, "_SCRATCH_ROOT", str(tmp_path / "scratch") + os.sep)
        monkeypatch.setattr(work_routes.work_artifacts, "root", lambda: store)
        artifact_id = _artifact(item_id, str(page))
        resp = work_routes.api_work_artifact_asset(artifact_id, "")
        assert isinstance(resp, FileResponse)
        assert resp.path.endswith("report.html")


class TestArtifactAssetRoute:
    def test_html_artifact_redirects_to_a_folder_url(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        resp = work_routes.api_work_artifact_file(artifact_id)
        assert resp.status_code == 307
        assert resp.headers["location"] == f"/api/work/artifact_file/{artifact_id}/"

    def test_folder_url_serves_the_artifact_itself(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        resp = work_routes.api_work_artifact_asset(artifact_id, "")
        assert isinstance(resp, FileResponse)
        assert resp.path.endswith("report.html")

    def test_serves_an_image_next_to_the_artifact(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        resp = work_routes.api_work_artifact_asset(artifact_id, "shot.png")
        assert isinstance(resp, FileResponse)
        assert resp.path.endswith("shot.png")
        assert resp.media_type == "image/png"

    def test_rejects_an_asset_outside_the_artifact_folder(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        resp = work_routes.api_work_artifact_asset(artifact_id, "../../elsewhere/secret.txt")
        assert resp.status_code == 403
        assert b"outside the artifact folder" in resp.body

    def test_reports_a_missing_asset(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        resp = work_routes.api_work_artifact_asset(artifact_id, "gone.png")
        assert resp.status_code == 404
        assert b"file missing" in resp.body


class TestArtifactAssetRouting:
    def _app(self):
        app = FastAPI()
        app.include_router(work_routes.router)
        return app

    def test_the_folder_url_and_asset_url_both_route(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        with TestClient(self._app()) as client:
            page = client.get(f"/api/work/artifact_file/{artifact_id}")
            assert page.status_code == 200
            assert page.url.path == f"/api/work/artifact_file/{artifact_id}/"
            asset = client.get(f"/api/work/artifact_file/{artifact_id}/shot.png")
            assert asset.status_code == 200
            assert asset.headers["content-type"] == "image/png"


class TestArtifactSandboxPolicy:
    def _app(self):
        app = FastAPI()
        app.include_router(work_routes.router)
        return app

    def test_html_artifact_may_run_its_own_scripts(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        with TestClient(self._app()) as client:
            page = client.get(f"/api/work/artifact_file/{artifact_id}/")
        policy = page.headers["content-security-policy"]
        assert policy.split()[0] == "sandbox"
        for token in ("allow-scripts", "allow-modals", "allow-popups",
                      "allow-popups-to-escape-sandbox", "allow-downloads"):
            assert token in policy
        assert "connect-src blob: data:" in policy

    def test_no_artifact_policy_grants_the_board_origin(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        with TestClient(self._app()) as client:
            page = client.get(f"/api/work/artifact_file/{artifact_id}/")
            asset = client.get(f"/api/work/artifact_file/{artifact_id}/shot.png")
        for resp in (page, asset):
            assert "allow-same-origin" not in resp.headers["content-security-policy"]

    def test_non_page_artifact_runs_nothing(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        with TestClient(self._app()) as client:
            asset = client.get(f"/api/work/artifact_file/{artifact_id}/shot.png")
        assert asset.headers["content-security-policy"] == "sandbox"

    def test_markdown_artifact_runs_nothing(self, tmp_path, monkeypatch):
        artifact_id = _seed(tmp_path, monkeypatch, "run-workspace")
        with TestClient(self._app()) as client:
            resp = client.get(f"/api/work/artifact_file/{artifact_id}")
        assert resp.headers["content-security-policy"] == "sandbox"


class TestArtifactClaudeRoute:
    def _stub(self, tmp_path, monkeypatch, running=False):
        launched = []
        scheduled = []
        monkeypatch.setattr(work_routes.terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "launch"))
        monkeypatch.setattr(work_routes.terminal, "session_healthy",
                            lambda key, agent="claude": {"alive": running,
                                                         "agent_running": running})
        monkeypatch.setattr(work_routes.terminal, "launch_pane_command",
                            lambda key, cwd, cmd: launched.append((key, cwd, cmd)))
        monkeypatch.setattr(work_routes.terminal, "claude_cmd", lambda config: "claude")
        monkeypatch.setattr(work_routes.work_launch, "personal_config", lambda: {})
        monkeypatch.setattr(work_routes, "schedule_discuss_kill", scheduled.append)
        return launched, scheduled

    def test_launches_claude_in_the_artifact_folder(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        launched, scheduled = self._stub(tmp_path, monkeypatch)
        resp = work_routes.api_artifact_claude_start(artifact_id)
        key = f"artifact-{artifact_id}"
        assert resp["key"] == key
        assert len(launched) == 1
        assert launched[0][0] == key
        assert launched[0][1] == str(tmp_path / "run-workspace" / "docs")
        assert "--append-system-prompt" in launched[0][2]
        seed = (tmp_path / "launch" / f"{key}.md").read_text()
        assert str(tmp_path / "run-workspace" / "docs" / "report.html") in seed
        assert "artifact report test" in seed
        assert scheduled == [key]

    def test_keeps_a_running_session(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        launched, scheduled = self._stub(tmp_path, monkeypatch, running=True)
        resp = work_routes.api_artifact_claude_start(artifact_id)
        assert resp["status"] == "ok"
        assert launched == []
        assert scheduled == [f"artifact-{artifact_id}"]

    def test_refuses_an_artifact_outside_every_root(self, tmp_path, monkeypatch):
        artifact_id = _seed(tmp_path, monkeypatch, "elsewhere")
        launched, _ = self._stub(tmp_path, monkeypatch)
        resp = work_routes.api_artifact_claude_start(artifact_id)
        assert resp.status_code == 403
        assert launched == []

    def test_refuses_an_unknown_artifact(self, tmp_path, monkeypatch):
        launched, _ = self._stub(tmp_path, monkeypatch)
        resp = work_routes.api_artifact_claude_start(999999999)
        assert resp.status_code == 404
        assert launched == []


class TestArtifactViewerRoutes:
    def _app(self):
        app = FastAPI()
        app.include_router(work_routes.router)
        return app

    def test_viewer_page_and_metadata_route(self, tmp_path, monkeypatch):
        artifact_id = _seed_report(tmp_path, monkeypatch)
        with TestClient(self._app()) as client:
            page = client.get(f"/artifacts/{artifact_id}")
            meta = client.get(f"/api/work/artifacts/{artifact_id}")
            search = client.get("/api/work/artifacts?q=report")
        assert page.status_code == 200
        assert "Ask Claude about this artifact" in page.text
        assert meta.json()["path"].endswith("report.html")
        assert search.status_code == 200
        assert "artifacts" in search.json()
