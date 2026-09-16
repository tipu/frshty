"""The images the operator pastes into the task compose box."""
import base64
import pathlib
from unittest.mock import MagicMock, patch

import pytest

import core.db as db
import core.state as state
from services import work_artifacts, work_debrief, work_store

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmM"
    "IQAAAABJRU5ErkJggg==")


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path, monkeypatch):
    """Each test gets its own artifact root. The session-wide root is shared,
    and fresh_db restarts item ids, so one test would otherwise read the
    folder an earlier test wrote for the same id."""
    monkeypatch.setenv(work_artifacts.ROOT_ENV, str(tmp_path / "artifacts"))
    state.init(tmp_path)
    state._default_instance_key = "personal"
    state._instance_key_cv.set("personal")
    yield


def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from web.work import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _launch_patches(tmp_path):
    reg = MagicMock()
    reg.config = {"workspace": {"root": tmp_path}}
    instances = MagicMock()
    instances.get.return_value = reg
    return (patch("services.work_launch.runtime.instances", return_value=instances),
            patch("services.work_launch.terminal.launch_agent"),
            patch("services.work_launch.terminal.session_healthy",
                  return_value={"alive": True, "agent_running": True}))


def _payload(data=None, media_type="image/png", name="image.png"):
    return {"name": name, "type": media_type,
            "data": base64.b64encode(PNG if data is None else data).decode()}


def _item_count():
    return db.query_one("SELECT COUNT(*) AS n FROM work_items")["n"]


def _done_item(objective="source task", source_item_id=None):
    """A finished task a follow-up can continue."""
    item_id = work_store.create_item(objective, instance_key="personal",
                                     source_item_id=source_item_id)
    work_store.add_run(item_id, f"sid-img-{item_id}", f"work-{item_id}", "/tmp")
    db.execute("UPDATE work_items SET state = 'done', summary = 'the source shipped' "
               "WHERE id = ?", (item_id,))
    return item_id


def _thread_root():
    """A thread is a follow-up chain, so it needs two members to exist."""
    root = _done_item("thread root")
    _done_item("thread member", source_item_id=root)
    return root


def _draft(item_id, kind="work_item"):
    now = work_store._now()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_followups(work_item_id, kind, workspace, recipient, draft, "
            "created_at, updated_at) VALUES (?, ?, 'aimyable', 'Sam', 'carry on', ?, ?)",
            (item_id, kind, now, now))
        return cur.lastrowid


def _attached(item_id):
    folder = work_artifacts.item_dir(item_id) / work_artifacts.INTAKE_DIR
    return sorted(p.name for p in folder.iterdir()) if folder.exists() else []


class TestDecode:
    def test_no_images_is_not_an_error(self):
        assert work_artifacts.decode_intake_images(None) == ([], "")
        assert work_artifacts.decode_intake_images([]) == ([], "")

    def test_a_png_decodes_to_its_bytes_and_suffix(self):
        decoded, error = work_artifacts.decode_intake_images([_payload()])
        assert error == ""
        assert decoded == [(PNG, ".png")]

    def test_every_supported_type_keeps_its_suffix(self):
        decoded, error = work_artifacts.decode_intake_images(
            [_payload(media_type=t) for t in work_artifacts.INTAKE_IMAGE_TYPES])
        assert error == ""
        assert [suffix for _, suffix in decoded] == \
            list(work_artifacts.INTAKE_IMAGE_TYPES.values())

    def test_an_unsupported_type_is_refused(self):
        decoded, error = work_artifacts.decode_intake_images(
            [_payload(media_type="application/pdf")])
        assert decoded == []
        assert "unsupported type" in error

    def test_a_broken_payload_is_refused(self):
        decoded, error = work_artifacts.decode_intake_images(
            [{"type": "image/png", "data": "not base64!!"}])
        assert decoded == []
        assert "not valid base64" in error

    def test_an_empty_payload_is_refused(self):
        decoded, error = work_artifacts.decode_intake_images(
            [{"type": "image/png", "data": ""}])
        assert decoded == []
        assert "is empty" in error

    def test_an_oversize_image_is_refused(self, monkeypatch):
        monkeypatch.setattr(work_artifacts, "MAX_INTAKE_IMAGE_BYTES", len(PNG) - 1)
        decoded, error = work_artifacts.decode_intake_images([_payload()])
        assert decoded == []
        assert "over the" in error

    def test_too_many_images_are_refused(self):
        many = [_payload()] * (work_artifacts.MAX_INTAKE_IMAGES + 1)
        decoded, error = work_artifacts.decode_intake_images(many)
        assert decoded == []
        assert "too many images" in error

    def test_a_non_list_is_refused(self):
        assert work_artifacts.decode_intake_images({"type": "image/png"}) == \
            ([], "images must be a list")

    def test_a_non_object_entry_is_refused(self):
        decoded, error = work_artifacts.decode_intake_images(["image/png"])
        assert decoded == []
        assert "is not an object" in error


class TestSave:
    def test_the_files_land_under_the_item_folder(self):
        paths = work_artifacts.save_intake_images(77, [(PNG, ".png"), (PNG, ".webp")])
        folder = work_artifacts.item_dir(77) / work_artifacts.INTAKE_DIR
        assert paths == [str(folder / "pasted-1.png"), str(folder / "pasted-2.webp")]
        assert [pathlib.Path(p).read_bytes() for p in paths] == [PNG, PNG]

    def test_nothing_pasted_writes_nothing(self):
        assert work_artifacts.save_intake_images(78, []) == []
        assert not (work_artifacts.item_dir(78) / work_artifacts.INTAKE_DIR).exists()


class TestLaunch:
    def test_a_pasted_image_is_stored_and_named_in_the_prompt(self, tmp_path):
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post("/api/work/intake",
                               json={"text": "fix what this screenshot shows",
                                     "images": [_payload()]})
        assert r.status_code == 200, r.text
        item_id = r.json()["item_id"]
        stored = work_artifacts.item_dir(item_id) / work_artifacts.INTAKE_DIR / "pasted-1.png"
        assert stored.read_bytes() == PNG
        context = launched.call_args.args[3]
        assert "## Attached images" in context
        assert str(stored) in context

    def test_a_task_without_images_names_no_attachment(self, tmp_path):
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post("/api/work/intake", json={"text": "plain work"})
        assert r.status_code == 200, r.text
        assert "## Attached images" not in launched.call_args.args[3]

    def test_a_bad_image_refuses_the_launch_and_files_no_task(self, tmp_path):
        before = _item_count()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post("/api/work/intake",
                               json={"text": "fix what this shows",
                                     "images": [_payload(media_type="text/html")]})
        assert r.status_code == 400, r.text
        assert "unsupported type" in r.json()["error"]
        assert _item_count() == before
        assert launched.call_count == 0

    def test_several_pasted_images_all_reach_the_agent(self, tmp_path):
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post("/api/work/intake",
                               json={"text": "compare these two screens",
                                     "images": [_payload(), _payload(media_type="image/jpeg")]})
        assert r.status_code == 200, r.text
        folder = work_artifacts.item_dir(r.json()["item_id"]) / work_artifacts.INTAKE_DIR
        assert sorted(p.name for p in folder.iterdir()) == ["pasted-1.png", "pasted-2.jpg"]
        context = launched.call_args.args[3]
        assert str(folder / "pasted-1.png") in context
        assert str(folder / "pasted-2.jpg") in context

    def test_a_store_that_cannot_write_cancels_the_task(self, tmp_path, monkeypatch):
        monkeypatch.setattr(work_artifacts, "save_intake_images",
                            MagicMock(side_effect=OSError("No space left on device")))
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post("/api/work/intake",
                               json={"text": "fix what this shows",
                                     "images": [_payload()]})
        assert r.status_code == 500, r.text
        assert "No space left on device" in r.json()["error"]
        assert launched.call_count == 0
        row = db.query_one("SELECT state FROM work_items WHERE id = ?",
                           (r.json()["item_id"],))
        assert row["state"] == "canceled"

    def test_an_empty_image_list_launches_normally(self, tmp_path):
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post("/api/work/intake",
                               json={"text": "no paste here", "images": []})
        assert r.status_code == 200, r.text
        row = db.query_one("SELECT objective FROM work_items WHERE id = ?",
                           (r.json()["item_id"],))
        assert row["objective"] == "no paste here"


class TestFollowupLaunch:
    """A follow-up is a new task, so it takes a pasted image the same way."""

    def test_a_pasted_image_reaches_the_followup_agent(self, tmp_path):
        source = _done_item()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post(f"/api/work/items/{source}/followup",
                               json={"text": "fix what this screenshot shows",
                                     "images": [_payload()]})
        assert r.status_code == 200, r.text
        child = r.json()["item_id"]
        stored = work_artifacts.item_dir(child) / work_artifacts.INTAKE_DIR / "pasted-1.png"
        assert stored.read_bytes() == PNG
        context = launched.call_args.args[3]
        assert "## Attached images" in context
        assert str(stored) in context

    def test_the_image_lands_under_the_followup_not_its_source(self, tmp_path):
        source = _done_item()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            r = _client().post(f"/api/work/items/{source}/followup",
                               json={"text": "carry on", "images": [_payload()]})
        assert r.status_code == 200, r.text
        assert _attached(r.json()["item_id"]) == ["pasted-1.png"]
        assert _attached(source) == []

    def test_a_bad_image_refuses_the_followup_and_files_no_task(self, tmp_path):
        source = _done_item()
        before = _item_count()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post(f"/api/work/items/{source}/followup",
                               json={"text": "carry on",
                                     "images": [_payload(media_type="text/html")]})
        assert r.status_code == 400, r.text
        assert "unsupported type" in r.json()["error"]
        assert _item_count() == before
        assert launched.call_count == 0

    def test_a_followup_without_images_names_no_attachment(self, tmp_path):
        source = _done_item()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post(f"/api/work/items/{source}/followup",
                               json={"text": "carry on"})
        assert r.status_code == 200, r.text
        assert "## Attached images" not in launched.call_args.args[3]


class TestThreadLaunch:
    """The thread page launches a new member of a thread."""

    def test_a_pasted_image_reaches_the_thread_task(self, tmp_path):
        source = _thread_root()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post(f"/api/work/threads/{source}/tasks",
                               json={"text": "next step in this thread",
                                     "images": [_payload()]})
        assert r.status_code == 200, r.text
        stored = (work_artifacts.item_dir(r.json()["item_id"])
                  / work_artifacts.INTAKE_DIR / "pasted-1.png")
        assert stored.read_bytes() == PNG
        assert str(stored) in launched.call_args.args[3]

    def test_a_bad_image_refuses_the_thread_task(self, tmp_path):
        source = _thread_root()
        before = _item_count()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post(f"/api/work/threads/{source}/tasks",
                               json={"text": "next step",
                                     "images": [_payload(media_type="text/html")]})
        assert r.status_code == 400, r.text
        assert "unsupported type" in r.json()["error"]
        assert _item_count() == before
        assert launched.call_count == 0


class TestDraftFollowupSend:
    """The board drafts a follow-up; the operator can paste into the draft."""

    def test_a_pasted_image_reaches_the_drafted_followup(self, tmp_path):
        source = _done_item()
        followup_id = _draft(source)
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post(f"/api/work/followups/{followup_id}/send",
                               json={"text": "carry on", "images": [_payload()]})
        assert r.status_code == 200, r.text
        child = db.query_one(
            "SELECT id FROM work_items WHERE source_item_id = ?", (source,))["id"]
        stored = (work_artifacts.item_dir(child)
                  / work_artifacts.INTAKE_DIR / "pasted-1.png")
        assert stored.read_bytes() == PNG
        assert str(stored) in launched.call_args.args[3]

    def test_a_bad_image_leaves_the_draft_a_draft(self, tmp_path):
        source = _done_item()
        followup_id = _draft(source)
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch as launched, healthy:
            r = _client().post(f"/api/work/followups/{followup_id}/send",
                               json={"text": "carry on",
                                     "images": [_payload(media_type="text/html")]})
        assert r.status_code == 409, r.text
        assert "unsupported type" in r.json()["error"]
        assert launched.call_count == 0
        row = db.query_one("SELECT status FROM work_followups WHERE id = ?",
                           (followup_id,))
        assert row["status"] == "draft"

    def test_a_slack_draft_carries_no_image(self):
        source = _done_item()
        followup_id = _draft(source, kind="slack_message")
        out = work_debrief.send_followup(followup_id, images=[_payload()])
        assert "carries no images" in out["error"]
        row = db.query_one("SELECT status FROM work_followups WHERE id = ?",
                           (followup_id,))
        assert row["status"] == "draft"


class TestTemplates:
    """Every page that makes a new task loads the shared paste module and
    wires its compose box to a tray."""

    PAGES = {
        "templates/work.html": {
            "trays": ["intakeTray", "fupTray[rowKey(it)]"],
            "pastes": ["onPaste(intakeTray, $event)",
                       "onPaste(fupTray[rowKey(it)], $event)"],
        },
        "templates/work_detail.html": {
            "trays": ["fupTray", "fuTray[f.id]"],
            "pastes": ["onPaste(fupTray, $event)", "onDraftPaste(f, $event)"],
        },
        "templates/thread_detail.html": {
            "trays": ["launchTray"],
            "pastes": ["onPaste(launchTray, $event)"],
        },
    }

    @pytest.mark.parametrize("page", sorted(PAGES))
    def test_the_page_loads_the_paste_module(self, page):
        text = pathlib.Path(page).read_text()
        assert '<script src="/static/frshty-paste-images.js"></script>' in text
        assert '.component("paste-tray", window.PasteTray)' in text

    @pytest.mark.parametrize("page", sorted(PAGES))
    def test_every_compose_box_takes_a_paste(self, page):
        text = pathlib.Path(page).read_text()
        for handler in self.PAGES[page]["pastes"]:
            assert f'@paste="{handler}"' in text

    @pytest.mark.parametrize("page", sorted(PAGES))
    def test_every_tray_is_shown_to_the_operator(self, page):
        text = pathlib.Path(page).read_text()
        for tray in self.PAGES[page]["trays"]:
            assert f':tray="{tray}"' in text

    @pytest.mark.parametrize("page", sorted(PAGES))
    def test_every_launch_asks_the_tray_whether_it_may_go(self, page):
        """submitBlock holds a launch while an image is still being read and
        holds the first launch after the board refused one."""
        assert "PasteImages.submitBlock(" in pathlib.Path(page).read_text()

    def test_a_draft_that_carries_no_image_says_so(self):
        page = pathlib.Path("templates/work_detail.html").read_text()
        assert '@paste="onDraftPaste(f, $event)"' in page
        assert "follow-up carries no image" in page

    def test_the_module_removes_a_thumbnail_and_caps_the_count(self):
        module = pathlib.Path("static/frshty-paste-images.js").read_text()
        assert 'class="ln-attach-x"' in module
        assert "@click=\"drop(i)\"" in module
        assert "at most " in module

    def test_the_module_holds_a_launch_after_a_refused_paste(self):
        module = pathlib.Path("static/frshty-paste-images.js").read_text()
        assert "function submitBlock(tray) {" in module
        assert "press again to launch without it" in module
