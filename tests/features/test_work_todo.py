import pathlib

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
