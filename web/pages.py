from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse


TEMPLATES_DIR = Path(__file__).parent.parent / "templates"

router = APIRouter()


def _template(name: str) -> HTMLResponse:
    return HTMLResponse((TEMPLATES_DIR / name).read_text())


@router.get("/", response_class=HTMLResponse)
def dashboard():
    return _template("index.html")


@router.get("/global", response_class=HTMLResponse)
def global_feed_page():
    return _template("global.html")


@router.get("/reviews", response_class=HTMLResponse)
def reviews_list():
    return _template("reviews.html")


@router.get("/reviews/{repo}/{pr_id}", response_class=HTMLResponse)
def review_detail(repo: str, pr_id: int):
    return _template("review_detail.html")


@router.get("/reviews/{repo}/{pr_id}/discuss", response_class=HTMLResponse)
def review_discuss(repo: str, pr_id: int):
    return _template("review_discuss.html")


@router.get("/reviews/{repo}/{pr_id}/walkthrough/{idx}", response_class=HTMLResponse)
def review_walkthrough_page(repo: str, pr_id: int, idx: int):
    return _template("review_walkthrough.html")


@router.get("/tickets", response_class=HTMLResponse)
def tickets_page():
    return _template("tickets.html")


@router.get("/tickets/{key}", response_class=HTMLResponse)
def ticket_detail(key: str):
    return _template("ticket_detail.html")


@router.get("/prd", response_class=HTMLResponse)
def prd_page():
    return _template("prd.html")


@router.get("/today", response_class=HTMLResponse)
def today_page():
    return _template("today.html")


@router.get("/slack", response_class=HTMLResponse)
def slack_page():
    return _template("slack.html")


@router.get("/scheduled", response_class=HTMLResponse)
def scheduled_page():
    return _template("scheduled.html")


@router.get("/config", response_class=HTMLResponse)
def config_page():
    return _template("config.html")


@router.get("/timesheet", response_class=HTMLResponse)
def timesheet_page():
    return _template("timesheet.html")


@router.get("/billing", response_class=HTMLResponse)
def billing_page():
    return _template("billing.html")


@router.get("/claude", response_class=HTMLResponse)
def claude_page():
    return _template("claude.html")
