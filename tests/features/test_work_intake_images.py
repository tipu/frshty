"""The images the operator pastes into the task compose box."""
import base64
import pathlib
from unittest.mock import MagicMock, patch

import pytest

import core.db as db
import core.state as state
from services import work_artifacts

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmM"
    "IQAAAABJRU5ErkJggg==")


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
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


class TestBoardTemplate:
    def test_the_compose_box_takes_a_paste(self):
        page = pathlib.Path("templates/work.html").read_text()
        assert '@paste="onIntakePaste"' in page

    def test_the_thumbnail_carries_a_remove_button(self):
        page = pathlib.Path("templates/work.html").read_text()
        assert 'v-for="(im, i) in intakeImages"' in page
        assert '@click="removeIntakeImage(i)"' in page

    def test_the_launch_request_carries_the_images(self):
        page = pathlib.Path("templates/work.html").read_text()
        assert "images: this.intakeImages.map(im => ({" in page
        assert "this.intakeImages = [];" in page

    def test_the_launch_waits_for_an_image_still_being_read(self):
        page = pathlib.Path("templates/work.html").read_text()
        assert "intakeImagesPending" in page
        assert "a pasted image is still being read" in page
