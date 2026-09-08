"""A WebSocket handshake from a document with no origin of its own is refused.

CORS does not cover WebSocket, so the terminal route is reachable from any
browsing context that knows the board's hostname. /ws/terminal reads the agent
terminal's scrollback and writes keystrokes into it, and resumes the agent
session, so an accepted handshake hands that document the operator's session.
A sandboxed artifact page sends `Origin: null`; a real board page sends its own
hostname, of which three reach this board; a non-browser client sends none.
"""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

import web.reviews as reviews_routes
import web.tickets as tickets_routes

HOST = "personal.frshty.localhost"

ROUTES = [
    ("/ws/terminal/T-1", tickets_routes),
    ("/ws/terminal/work-42", tickets_routes),
    ("/ws/discuss/sess-1", reviews_routes),
]


async def _serve(websocket, key, config):
    await websocket.accept()
    await websocket.send_text("served")
    await websocket.close()


def _client(module):
    app = FastAPI()
    app.include_router(module.router)
    return TestClient(app)


@pytest.mark.parametrize("path,module", ROUTES)
@pytest.mark.parametrize("origin", [
    f"https://{HOST}", "https://personal.frshty.local",
    "https://personal.frshty.danialjaffry.com"])
def test_a_board_page_is_served(path, module, origin):
    with patch.object(module.terminal, "terminal_handler", _serve), \
         patch.object(tickets_routes.work_launch, "resume_session", MagicMock()):
        with _client(module).websocket_connect(
                path, headers={"host": HOST, "origin": origin}) as ws:
            assert ws.receive_text() == "served"


@pytest.mark.parametrize("path,module", ROUTES)
def test_a_client_without_an_origin_is_served(path, module):
    with patch.object(module.terminal, "terminal_handler", _serve), \
         patch.object(tickets_routes.work_launch, "resume_session", MagicMock()):
        with _client(module).websocket_connect(path, headers={"host": HOST}) as ws:
            assert ws.receive_text() == "served"


@pytest.mark.parametrize("origin", ["null", "NULL"])
@pytest.mark.parametrize("path,module", ROUTES)
def test_a_document_with_no_origin_of_its_own_is_refused(path, module, origin):
    served = []

    async def _spy(websocket, key, config):
        served.append(key)
        await _serve(websocket, key, config)

    resume = MagicMock()
    with patch.object(module.terminal, "terminal_handler", _spy), \
         patch.object(tickets_routes.work_launch, "resume_session", resume):
        with pytest.raises(Exception):
            with _client(module).websocket_connect(
                    path, headers={"host": HOST, "origin": origin}) as ws:
                ws.receive_text()
    assert served == []
    resume.assert_not_called()
