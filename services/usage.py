from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.log as log

SKIP_EXACT = frozenset({
    "/api/tickets/list",
    "/api/events",
    "/api/config",
    "/api/status",
    "/api/scheduled",
    "/api/usage/ui",
    "/api/usage/report",
})
SKIP_PREFIXES = ("/static/", "/favicon")

_MAX_NAME_LEN = 200


def is_tracked(path: str) -> bool:
    if path in SKIP_EXACT:
        return False
    return not path.startswith(SKIP_PREFIXES)


def ensure_db() -> None:
    if getattr(db, "_DB_PATH", None) is None:
        migrations = Path(__file__).resolve().parent.parent / "migrations"
        db.init(Path.home() / ".frshty" / "frshty.db", migrations)


def record(kind: str, name: str, instance: str = "", n: int = 1) -> None:
    name = " ".join(name.split())[:_MAX_NAME_LEN]
    if not name or n < 1:
        return
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        db.execute(
            "INSERT INTO usage_counters(instance, kind, name, day, count) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(instance, kind, name, day) "
            "DO UPDATE SET count = count + excluded.count",
            (instance, kind, name, day, n),
        )
    except Exception as e:
        log.emit("usage_record_failed",
                 f"could not record usage {kind} {name}: {type(e).__name__}: {e}",
                 meta={"kind": kind, "name": name})


def aggregates(kind: str) -> list[dict]:
    rows = db.query_all(
        "SELECT name, SUM(count) AS count, MIN(day) AS first_day, "
        "MAX(day) AS last_day, COUNT(DISTINCT day) AS days_used, "
        "GROUP_CONCAT(DISTINCT instance) AS instances "
        "FROM usage_counters WHERE kind = ? "
        "GROUP BY name ORDER BY count DESC",
        (kind,),
    )
    for r in rows:
        r["instances"] = sorted(x for x in (r["instances"] or "").split(",") if x)
    return rows


def tracking_since() -> str | None:
    row = db.query_one("SELECT MIN(day) AS d FROM usage_counters")
    return row["d"] if row else None
