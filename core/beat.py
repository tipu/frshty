"""Beat thread that fires recurring schedules from the scheduler table.

Runs every `interval` seconds, calls scheduler.fire_due_recurring(), which in
one BEGIN IMMEDIATE transaction: claims every recurring row whose run_at has
passed, enqueues its task into jobs, advances next_run_at. Atomic so a crash
never leaves a schedule in "fired but not advanced" or "advanced but not
enqueued" state.
"""
from __future__ import annotations
import threading

import core.log as log
import core.scheduler as scheduler


class BeatThread:
    def __init__(self, interval: float = 60.0):
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        t = threading.Thread(target=self._run, daemon=True, name="scheduler-beat")
        t.start()
        self._thread = t

    def stop(self, join_timeout: float = 2.0) -> None:
        """Signal the beat thread to stop and wait up to join_timeout seconds
        for it to finish the current fire, so no straggler write lands in a
        database the caller tears down or swaps next."""
        self._stop.set()
        t = self._thread
        if t is not None and t is not threading.current_thread():
            t.join(join_timeout)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                fired = scheduler.fire_due_recurring()
                for f in fired:
                    log.emit("beat_fired",
                             f"[{f['instance_key']}] {f['task']} next={f['next_run_at']}",
                             meta=f)
            except Exception as e:
                try:
                    log.emit("beat_error", f"{type(e).__name__}: {e}")
                except Exception:
                    pass
            if self._stop.wait(self.interval):
                return
