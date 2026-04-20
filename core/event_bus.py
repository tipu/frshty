"""Dispatcher that drains undispatched events from the events table into jobs.

Routing is defined in ROUTES, a list of (predicate, handler) pairs. handler
returns a list of (instance_key, task, payload, ticket_key) tuples. The
dispatcher emits one job per returned tuple and marks the event dispatched.
"""
import json
import threading
from typing import Callable

import core.log as log
import core.queue as q


Route = Callable[[dict, dict], list[dict]]
_ROUTES: list[tuple[Callable[[dict], bool], Route]] = []


def register(matcher: Callable[[dict], bool], handler: Route) -> None:
    _ROUTES.append((matcher, handler))


def kind_is(kind: str) -> Callable[[dict], bool]:
    def check(ev: dict) -> bool:
        return ev.get("kind") == kind
    return check


class Dispatcher:
    def __init__(self, registries: dict, poll_interval: float = 1.0):
        self.registries = registries
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        t = threading.Thread(target=self._run, daemon=True, name="event-dispatcher")
        t.start()
        self._thread = t

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                n = self._drain()
            except Exception as e:
                log.emit("dispatcher_error", f"{type(e).__name__}: {e}")
                n = 0
            if n == 0:
                if self._stop.wait(self.poll_interval):
                    return

    def _drain(self) -> int:
        events = q.undispatched_events(limit=50)
        if not events:
            return 0
        for ev in events:
            try:
                payload = json.loads(ev["payload"] or "{}")
            except json.JSONDecodeError:
                payload = {}
            event_obj = {"id": ev["id"], "instance_key": ev["instance_key"],
                         "source": ev["source"], "kind": ev["kind"], "payload": payload}
            handled = False
            for matcher, handler in _ROUTES:
                try:
                    if not matcher(event_obj):
                        continue
                except Exception as e:
                    log.emit("dispatcher_match_error", f"{type(e).__name__}: {e}")
                    continue
                try:
                    jobs = handler(event_obj, self.registries) or []
                except Exception as e:
                    log.emit("dispatcher_handler_error", f"{type(e).__name__}: {e}")
                    jobs = []
                for j in jobs:
                    q.enqueue_job(
                        instance_key=j["instance_key"],
                        task=j["task"],
                        payload=j.get("payload", {}),
                        ticket_key=j.get("ticket_key"),
                        triggering_event_id=ev["id"],
                    )
                handled = True
            reason = "routed" if handled else "unrouted"
            q.mark_dispatched(ev["id"], reason)
        return len(events)
