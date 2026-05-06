import json
import urllib.parse
import urllib.request
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import core.log as log
import core.state as state
from web.state import _config


router = APIRouter()


@router.get("/api/slack/data")
def api_slack_data():
    return state.load("slack")


@router.post("/api/slack/send/{reply_id}")
def api_slack_send(reply_id: str, body: dict):
    text = body.get("text", "")
    if not text:
        return JSONResponse({"error": "text required"}, status_code=400)

    sl = state.load("slack")
    replies = sl.get("replies", {})
    ctx = replies.get(reply_id)
    if not ctx:
        return JSONResponse({"error": "reply_id not found"}, status_code=404)

    workspace = ctx["workspace"]
    channel = ctx["channel"]
    thread_ts = ctx.get("thread_ts", "")

    tokens_path = _config.get("slack", {}).get("raw_path", "")
    if tokens_path:
        tokens_file = str(Path(tokens_path).parent.parent / "tokens.json")
        try:
            tokens = json.loads(Path(tokens_file).read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return JSONResponse({"error": "tokens.json not found"}, status_code=500)
        creds = tokens.get(workspace)
        if not creds:
            return JSONResponse({"error": f"no token for workspace {workspace}"}, status_code=400)

        post_data = {"token": creds["token"], "channel": channel, "text": text}
        if thread_ts:
            post_data["thread_ts"] = thread_ts

        req = urllib.request.Request(
            f"https://{workspace}.slack.com/api/chat.postMessage",
            urllib.parse.urlencode(post_data).encode(),
            headers={"Cookie": creds["cookie"].replace(", ", "; ")},
        )
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read())
        if result.get("ok"):
            log.emit("slack_reply_sent", f"Replied in {channel}: {text[:80]}",
                links={"detail": f"{_config['_base_url']}/slack"},
                meta={"channel": channel, "text": text, "reply_id": reply_id})
            return {"status": "sent"}
        return JSONResponse({"error": result.get("error", "unknown")}, status_code=400)

    return JSONResponse({"error": "slack not configured"}, status_code=400)
