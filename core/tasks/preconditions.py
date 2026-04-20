import re
from pathlib import Path
from typing import Callable

import core.state as state


def _ticket(ctx) -> dict:
    tickets = state.load("tickets")
    return tickets.get(ctx.ticket_key or "", {})


def status_is(*states: str) -> Callable:
    def check(ctx):
        cur = _ticket(ctx).get("status")
        return (cur in states, f"status={cur} not in {states}")
    return check


def status_in(*states: str) -> Callable:
    return status_is(*states)


def auto_pr_true(ctx) -> tuple[bool, str]:
    t = _ticket(ctx)
    if not t:
        return (False, "ticket not found")
    val = t.get("auto_pr")
    if val is None:
        val = bool(ctx.config.get("pr", {}).get("auto_pr"))
    return (bool(val), f"auto_pr={bool(val)}")


def feature_enabled(name: str) -> Callable:
    def check(ctx):
        enabled = bool(ctx.config.get("features", {}).get(name))
        return (enabled, f"features.{name}={enabled}")
    return check


def _ticket_dir(ctx) -> Path:
    ws = ctx.config["workspace"]
    t = _ticket(ctx)
    slug = t.get("slug") or (ctx.ticket_key or "")
    root = Path(ws["root"]) if isinstance(ws["root"], str) else ws["root"]
    return root / ws["tickets_dir"] / slug


def file_exists(rel: str) -> Callable:
    def check(ctx):
        p = _ticket_dir(ctx) / rel
        return (p.exists(), f"{rel} {'exists' if p.exists() else 'missing'}")
    return check


def file_contains(rel: str, pattern: str) -> Callable:
    rx = re.compile(pattern, re.MULTILINE)
    def check(ctx):
        p = _ticket_dir(ctx) / rel
        if not p.exists():
            return (False, f"{rel} missing")
        ok = bool(rx.search(p.read_text(errors="replace")))
        return (ok, f"{rel} {'matches' if ok else 'does not match'} /{pattern}/")
    return check
