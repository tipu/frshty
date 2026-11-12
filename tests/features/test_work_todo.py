import pathlib
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.work import router


def _client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestTodoApi:
    def test_add_and_remove_items(self, tmp_state):
        client = _client()
        assert client.get("/api/work/todo").json() == {"items": []}
        first = client.post("/api/work/todo", json={"text": "  buy milk "}).json()["items"]
        assert [it["text"] for it in first] == ["buy milk"]
        both = client.post("/api/work/todo", json={"text": "call home"}).json()["items"]
        assert [it["text"] for it in both] == ["buy milk", "call home"]
        r = client.post(f"/api/work/todo/{both[0]['id']}/delete")
        assert r.status_code == 200
        assert [it["text"] for it in r.json()["items"]] == ["call home"]
        assert [it["text"] for it in client.get("/api/work/todo").json()["items"]] == ["call home"]

    def test_new_items_carry_a_creation_time(self, tmp_state):
        before = datetime.now(timezone.utc)
        item = _client().post("/api/work/todo", json={"text": "buy milk"}).json()["items"][0]
        stamp = datetime.fromisoformat(item["created_at"])
        assert before <= stamp <= datetime.now(timezone.utc) + timedelta(seconds=1)

    def test_empty_text_is_refused(self, tmp_state):
        r = _client().post("/api/work/todo", json={"text": "   "})
        assert r.status_code == 400
        assert _client().get("/api/work/todo").json() == {"items": []}

    def test_unknown_id_is_a_404(self, tmp_state):
        assert _client().post("/api/work/todo/nope/delete").status_code == 404


class TestTodoUi:
    def test_the_todo_card_sits_above_active_threads(self):
        board = pathlib.Path("templates/work.html").read_text()
        assert board.index(">Todo</div>") < board.index("Active threads")
        assert "this.loadTodos()" in board
        assert "removeTodo(t)" in board
        assert "ago(t.created_at)" in board


class TestArtifactFavoritesApi:
    def test_add_dedupe_and_remove(self, tmp_state):
        client = _client()
        assert client.get("/api/work/artifact_favorites").json() == {"items": []}
        first = client.post("/api/work/artifact_favorites",
                            json={"artifact_id": 7, "peer": "peer-b", "title": "#3 report.html"}).json()["items"]
        assert [(f["artifact_id"], f["peer"], f["title"]) for f in first] == [(7, "peer-b", "#3 report.html")]
        again = client.post("/api/work/artifact_favorites",
                            json={"artifact_id": 7, "peer": "peer-b", "title": "x"}).json()["items"]
        assert len(again) == 1
        both = client.post("/api/work/artifact_favorites", json={"artifact_id": 7}).json()["items"]
        assert [(f["artifact_id"], f["peer"], f["title"]) for f in both] == [
            (7, "peer-b", "#3 report.html"), (7, "", "artifact #7")]
        r = client.post(f"/api/work/artifact_favorites/{both[0]['id']}/delete")
        assert r.status_code == 200
        assert [f["peer"] for f in client.get("/api/work/artifact_favorites").json()["items"]] == [""]

    def test_bad_artifact_id_is_refused(self, tmp_state):
        client = _client()
        for bad in ("7", 0, True, None):
            assert client.post("/api/work/artifact_favorites", json={"artifact_id": bad}).status_code == 400
        assert client.post("/api/work/artifact_favorites",
                           json={"artifact_id": 1, "peer": 5}).status_code == 400
        assert client.get("/api/work/artifact_favorites").json() == {"items": []}

    def test_unknown_id_is_a_404(self, tmp_state):
        assert _client().post("/api/work/artifact_favorites/nope/delete").status_code == 404


class TestArtifactFavoritesUi:
    def test_favorites_card_sits_below_todo(self):
        board = pathlib.Path("templates/work.html").read_text()
        assert board.index(">Todo</div>") < board.index(">Favorite artifacts</div>") < board.index("Active threads")
        assert "this.loadFavorites()" in board
        assert '"?peer=" + encodeURIComponent(f.peer)' in board

    def test_artifact_page_has_a_favorite_button(self):
        page = pathlib.Path("templates/work_artifact.html").read_text()
        assert 'id="fav"' in page
        assert "/api/work/artifact_favorites" in page
