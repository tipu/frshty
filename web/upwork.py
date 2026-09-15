"""The /upwork page's data, and the one route that reaches a client.

Reading is scheduled; these routes exist so the operator can look at what was
read and answer it. The reply route is the only place in frshty that writes to
Upwork, it is reached by a person pressing a button, and core/correspondence.py
denies it to a task.

The draft route is the other half of that split, and it is open to a task on
purpose. The reply a room gets is written by a work-board task, and this is
where that task puts it: the text lands in the box on this page and goes no
further. Recording a draft is not sending one, so the gate that closes the
send route to every task leaves this one alone.
"""
from fastapi import APIRouter
from fastapi.responses import JSONResponse

import core.log as log
import core.upwork_client as upwork_client
from features import upwork_inbox
from web.state import _config


router = APIRouter()

MAX_REPLY_CHARS = 4000


@router.get("/api/upwork/data")
def api_upwork_data():
    return upwork_inbox.board(_config)


@router.post("/api/upwork/refresh")
def api_upwork_refresh():
    """Pull the inbox now and give the page back what it holds.

    Only the index is refreshed. Screening and judging are model calls that
    take minutes, and they belong to the scheduled scan rather than to a
    button the operator is waiting on."""
    if not upwork_inbox.configured(_config):
        return JSONResponse({"error": "features.upwork is off"}, status_code=400)
    counts = upwork_inbox.ingest(_config)
    if not counts.get("complete"):
        return JSONResponse({"error": "the Upwork inbox could not be read"},
                            status_code=502)
    return {"status": "ok", "messages": counts["messages"],
            "rooms": counts["rooms"], **upwork_inbox.board(_config)}


@router.post("/api/upwork/rooms/{room_id}/draft")
def api_upwork_draft(room_id: str, body: dict, instance: str = ""):
    """Record the reply a task drafted for one room. This never sends.

    The task names its own instance, because it reaches this server on the
    port rather than on the hostname that picks between instances, and an
    unnamed instance falls back to the primary one. The room decides what the
    request may touch: a room exists only for an instance whose inbox was
    indexed, and a draft for a room that is not there is refused rather than
    ignored. The task was told which room it is answering, so a miss means the
    draft went nowhere, and the agent has to hear that instead of reporting
    the reply as recorded."""
    text = str(body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "text required"}, status_code=400)
    if len(text) > MAX_REPLY_CHARS:
        return JSONResponse({"error": f"text over {MAX_REPLY_CHARS} characters"},
                            status_code=400)
    if not upwork_inbox.record_draft(room_id, text, instance_key=instance):
        return JSONResponse({"error": f"unknown room {room_id}"}, status_code=404)
    # The event is filed under the instance serving the request, which is the
    # one the Host header picked and not necessarily the one named here. So
    # the name is written into the line rather than left to the feed it lands
    # in.
    where = f" of {instance}" if instance else ""
    log.emit("upwork_draft_recorded",
             f"a task drafted the reply to room {room_id}{where}: {text[:120]}",
             links={"detail": "/upwork"},
             meta={"room_id": room_id, "instance": instance, "text": text})
    return {"status": "recorded"}


@router.post("/api/upwork/rooms/{room_id}/reply")
def api_upwork_reply(room_id: str, body: dict):
    """Send one message to a client, as the operator, on the operator's press.

    Whatever Upwork answers is handed back rather than swallowed, because this
    is the only write frshty makes to somebody else's platform and a failure
    here must be visible on the page that asked for it."""
    text = str(body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "text required"}, status_code=400)
    if len(text) > MAX_REPLY_CHARS:
        return JSONResponse({"error": f"text over {MAX_REPLY_CHARS} characters"},
                            status_code=400)
    if not upwork_inbox.configured(_config):
        return JSONResponse({"error": "features.upwork is off"}, status_code=400)
    try:
        upwork_client.send_message(room_id, text, _config)
    except Exception as e:
        log.emit("upwork_reply_failed",
                 f"the reply to room {room_id} was not sent:"
                 f" {type(e).__name__}: {e}",
                 links={"detail": "/upwork"},
                 meta={"room_id": room_id, "error": str(e)[:400]})
        return JSONResponse({"error": str(e)[:400]}, status_code=502)
    upwork_inbox.record_reply(room_id, text)
    log.emit("upwork_reply_sent",
             f"replied in room {room_id}: {text[:120]}",
             links={"detail": "/upwork"},
             meta={"room_id": room_id, "text": text})
    return {"status": "sent"}
