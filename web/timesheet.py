from fastapi import APIRouter
from fastapi.responses import JSONResponse

import features.timesheet as ts
from web.state import _config


router = APIRouter()


@router.get("/api/timesheet")
def api_timesheet(start: str = "", end: str = "", force: bool = False):
    return ts.build_timesheet(_config, start, end, force)


@router.get("/api/timesheet/backtest")
def api_timesheet_backtest(start: str, end: str, force: bool = False):
    result = ts.backtest_timesheet_selection(_config, start, end, force)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@router.post("/api/timesheet/log")
def api_timesheet_log(body: dict):
    ticket = body.get("ticket", "")
    date_str = body.get("date", "")
    time_str = body.get("time", "")
    if not ticket or not date_str or not time_str:
        return JSONResponse({"error": "ticket, date, and time required"}, status_code=400)
    result = ts.log_work(_config, ticket, date_str, time_str)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@router.put("/api/timesheet/worklog")
def api_timesheet_worklog(body: dict):
    ticket = body.get("ticket", "")
    worklog_id = body.get("worklog_id", "")
    time_str = body.get("time", "")
    if not ticket or not worklog_id or not time_str:
        return JSONResponse({"error": "ticket, worklog_id, and time required"}, status_code=400)
    result = ts.update_worklog(_config, ticket, worklog_id, time_str)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result
