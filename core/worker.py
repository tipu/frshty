import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Callable

import core.db as db
import core.llm as llm
import core.log as log
import core.queue as q
import core.state as state
import core.tasks.registry as registry
from core.job_logs import job_pid_path

ORPHAN_POLL_INTERVAL = 60


def transition_ticket_and_emit(ticket_key: str | None, instance_key: str | None, *,
                               target: str | None = None,
                               mutate: Callable[[dict], dict] | None = None,
                               reason: str = "",
                               job_id: int | None = None,
                               job_status: str = "ok",
                               job_response: dict | None = None,
                               next_events: list[dict] | None = None,
                               advance: bool = True,
                               source: str = "task",
                               **fields) -> dict | None:
    """Move a ticket forward and schedule its next stage in one SQLite
    transaction.

    The ticket write, the job completion and the successor events commit
    together or not at all. A crash between them can no longer leave a
    finished job or a moved ticket with nothing queued to act on it.

    target runs the checked transition of state.transition_ticket with fields
    as co-field updates. mutate runs an unchecked state.update_ticket. With
    neither, no ticket row is written. job_id marks that job finished with
    job_status and job_response. next_events are extra events to insert; each
    needs a "kind" and takes this instance_key unless it names its own.
    advance inserts a ticket_advance event for ticket_key when an
    instance_key is known. Returns the saved ticket, or None when no ticket
    row was written.
    """
    if target is not None and mutate is not None:
        raise ValueError("pass target or mutate, not both")

    def _commit(c) -> None:
        if job_id is not None:
            q.finish_job(c, job_id, job_status, job_response or {})
        for ev in next_events or []:
            q.insert_event(c, source, ev["kind"], ev.get("payload", {}),
                           ev.get("instance_key", instance_key))
        if advance and ticket_key and instance_key:
            q.insert_event(c, source, "ticket_advance", {"ticket_key": ticket_key},
                           instance_key)

    if target is not None:
        return state.transition_ticket(ticket_key, target, reason=reason,
                                       in_tx=_commit, **fields)
    if mutate is not None:
        def _apply(cur: dict) -> dict:
            new = mutate(cur)
            if reason:
                new["_transition_reason"] = reason
            return new
        return state.update_ticket(ticket_key, _apply, in_tx=_commit)
    with db.tx() as c:
        _commit(c)
    return None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_pid(path) -> int | None:
    try:
        s = path.read_text().strip()
        return int(s) if s else None
    except (OSError, ValueError):
        return None


def _job_age_seconds(started_at: str | None) -> float:
    if not started_at:
        return 0.0
    try:
        t = datetime.fromisoformat(started_at)
    except ValueError:
        return 0.0
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - t).total_seconds()


class WorkerPool:
    def __init__(self, registries: dict, size: int = 4, poll_interval: float = 1.0):
        self.registries = registries
        self.size = size
        self.poll_interval = poll_interval
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._live_jobs: set[int] = set()
        self._live_lock = threading.Lock()

    def start(self) -> None:
        self._reconcile_orphans()
        for i in range(self.size):
            t = threading.Thread(target=self._run, args=(i,), daemon=True, name=f"worker-{i}")
            t.start()
            self._threads.append(t)
        sweep = threading.Thread(target=self._orphan_poll_loop, daemon=True, name="orphan-poll")
        sweep.start()
        self._threads.append(sweep)

    def stop(self, join_timeout: float = 2.0) -> None:
        """Signal every thread to stop, then wait up to join_timeout seconds
        per thread for it to finish its current job. Without the join, a
        worker mid-job keeps running after stop() returns and writes its
        completion (mark_done, ticket_advance event) into whatever database
        the module globals point at by then — in the test suite that is the
        NEXT test's freshly initialized DB."""
        self._stop.set()
        for t in self._threads:
            if t is threading.current_thread():
                continue
            t.join(join_timeout)

    def _reconcile_orphans(self) -> None:
        try:
            jobs = q.running_jobs()
        except Exception as e:
            log.emit("orphan_recover_query_error", f"{type(e).__name__}: {e}")
            return
        for job in jobs:
            try:
                self._finalize_orphan(job)
            except Exception as e:
                log.emit("orphan_recover_error",
                         f"job_id={job['id']}: {type(e).__name__}: {e}")

    def _orphan_poll_loop(self) -> None:
        while not self._stop.is_set():
            if self._stop.wait(ORPHAN_POLL_INTERVAL):
                return
            try:
                jobs = q.running_jobs()
            except Exception as e:
                log.emit("orphan_poll_query_error", f"{type(e).__name__}: {e}")
                continue
            for job in jobs:
                with self._live_lock:
                    if job["id"] in self._live_jobs:
                        continue
                try:
                    self._finalize_orphan(job)
                except Exception as e:
                    log.emit("orphan_poll_error",
                             f"job_id={job['id']}: {type(e).__name__}: {e}")

    def _finalize_orphan(self, job: dict) -> None:
        import core.state as state
        instance_key = job["instance_key"]
        reg = self.registries.get(instance_key)
        if not reg:
            return

        pid_path = job_pid_path(instance_key, job["id"])
        pid = _read_pid(pid_path)
        task_def = registry.get_task(job["task"])
        timeout = (task_def or {}).get("timeout", 1800)

        if pid and _pid_alive(pid):
            age = _job_age_seconds(job.get("started_at"))
            if age <= timeout:
                return
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except OSError:
                pass
            for _ in range(20):
                if not _pid_alive(pid):
                    break
                time.sleep(0.1)
            if _pid_alive(pid):
                try:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                except OSError:
                    pass
            log.emit("orphan_timeout_killed",
                     f"job_id={job['id']} task={job['task']} age={age:.0f}s pid={pid}",
                     meta={"ticket": job.get("ticket_key")})

        try:
            pid_path.unlink(missing_ok=True)
        except OSError:
            pass

        state_dir = reg.config.get("_state_dir")
        state_token = state.use(state_dir) if state_dir is not None else None
        log_tokens = log.use(state_dir, instance_key) if state_dir is not None else None
        try:
            payload = job.get("payload")
            if isinstance(payload, str):
                payload = json.loads(payload or "{}")
            elif payload is None:
                payload = {}
            ctx = registry.TaskContext(
                instance_key=instance_key,
                ticket_key=job["ticket_key"],
                task=job["task"],
                payload=payload,
                job_id=job["id"],
                triggering_event_id=job["triggering_event_id"],
                config=reg.config,
                registry=reg,
                now=datetime.now(timezone.utc),
            )
            self._finalize_via_postconditions(ctx, task_def)
        finally:
            if log_tokens is not None:
                log.reset(log_tokens)
            if state_token is not None:
                state.reset(state_token)

    def _finalize_via_postconditions(self, ctx: registry.TaskContext, task_def: dict | None) -> None:
        import core.state as state
        if not task_def:
            q.mark_done(ctx.job_id, "failed",
                        {"reason": "orphaned: unknown task on restart"})
            log.emit("orphan_unknown_task",
                     f"job_id={ctx.job_id} task={ctx.task} marked failed",
                     meta={"ticket": ctx.ticket_key})
            return

        postconds = task_def.get("postconditions") or []
        if not postconds:
            q.mark_done(ctx.job_id, "failed",
                        {"reason": "orphaned: no postconditions to verify completion"})
            log.emit("orphan_no_postconditions",
                     f"job_id={ctx.job_id} task={ctx.task} marked failed",
                     meta={"ticket": ctx.ticket_key})
            return

        for p in postconds:
            try:
                ok, reason = p(ctx)
            except Exception as e:
                q.mark_done(ctx.job_id, "failed",
                            {"reason": f"orphan postcondition errored: {type(e).__name__}: {e}"})
                log.emit("orphan_postcondition_error",
                         f"job_id={ctx.job_id} {type(e).__name__}: {e}",
                         meta={"ticket": ctx.ticket_key})
                return
            if not ok:
                q.mark_done(ctx.job_id, "failed",
                            {"reason": f"orphan postcondition: {reason}"})
                log.emit("orphan_postcondition_failed",
                         f"job_id={ctx.job_id} task={ctx.task}: {reason}",
                         meta={"ticket": ctx.ticket_key})
                return

        on_success = task_def.get("on_success_status")
        target = on_success(ctx, registry.TaskResult("ok")) if callable(on_success) else on_success
        has_target = isinstance(target, str) and bool(target) and bool(ctx.ticket_key)
        try:
            transition_ticket_and_emit(
                ctx.ticket_key, ctx.instance_key,
                target=target if has_target else None,
                job_id=ctx.job_id,
                job_response={"reason": "orphan recovered: postconditions met after restart"},
                advance=ctx.task != "advance_ticket",
            )
        except state.TicketStateError as e:
            q.mark_done(ctx.job_id, "failed",
                        {"reason": f"orphan recovery: transition to {target}: {e}"})
            log.emit("orphan_transition_failed",
                     f"job_id={ctx.job_id} task={ctx.task} target={target}: {e}",
                     meta={"ticket": ctx.ticket_key})
            return
        log.emit("orphan_recovered",
                 f"job_id={ctx.job_id} task={ctx.task} status=ok",
                 meta={"ticket": ctx.ticket_key})

    def _run(self, worker_idx: int) -> None:
        while not self._stop.is_set():
            job = None
            try:
                job = q.claim_next()
            except Exception as e:
                log.emit("worker_claim_error", f"w{worker_idx}: {type(e).__name__}: {e}")
            if not job:
                if self._stop.wait(self.poll_interval):
                    return
                continue
            try:
                self._run_one(job)
            except Exception as e:
                log.emit("worker_run_error", f"w{worker_idx}: job_id={job['id']}: "
                         f"{type(e).__name__}: {e}")

    def _run_one(self, job: dict) -> None:
        import core.state as state
        import core.job_logs as job_logs
        instance_key = job["instance_key"]
        reg = self.registries.get(instance_key)
        if not reg:
            q.mark_done(job["id"], "failed", {"reason": f"unknown instance_key: {instance_key}"})
            return
        with self._live_lock:
            self._live_jobs.add(job["id"])
        state_token = None
        log_tokens = None
        live_token = job_logs.use_live_job(instance_key, job["id"])
        state_dir = reg.config.get("_state_dir")
        if state_dir is not None:
            state_token = state.use(state_dir)
            log_tokens = log.use(state_dir, instance_key)
        try:
            ctx = registry.TaskContext(
                instance_key=instance_key,
                ticket_key=job["ticket_key"],
                task=job["task"],
                payload=job["payload"],
                job_id=job["id"],
                triggering_event_id=job["triggering_event_id"],
                config=reg.config,
                registry=reg,
                now=datetime.now(timezone.utc),
            )
            log.emit("job_started", f"{job['task']} ticket={job['ticket_key']} job_id={job['id']}",
                     meta={"category": "noise"})
            llm.reset_guard_blocked()
            result = registry.run_task(ctx)
            if result.status == "failed" and llm.consume_guard_blocked():
                result = registry.TaskResult(
                    "skipped",
                    f"llm_guard_blocked: {result.reason}" if result.reason else "llm_guard_blocked",
                    artifacts=result.artifacts,
                    next_events=result.next_events,
                )
            response = {"reason": result.reason, "artifacts": result.artifacts}
            next_events = []
            for ev in result.next_events or []:
                if isinstance(ev, dict) and isinstance(ev.get("kind"), str):
                    next_events.append(ev)
                else:
                    log.emit("worker_next_event_error", f"malformed next event: {ev!r}")
            transition_ticket_and_emit(
                job["ticket_key"], instance_key,
                job_id=job["id"], job_status=result.status, job_response=response,
                next_events=next_events,
                advance=result.status == "ok" and job["task"] != "advance_ticket",
            )
            log.emit("job_finished",
                     f"{job['task']} ticket={job['ticket_key']} "
                     f"job_id={job['id']} status={result.status}"
                     f"{(' reason='+result.reason) if result.reason else ''}",
                     meta={"category": "noise"} if result.status == "ok" else None)
        finally:
            log_path = job_logs.job_log_path(instance_key, job["id"])
            if log_path.exists() and log_path.stat().st_size == 0:
                log.emit("empty_job_log_detected", f"job_id={job['id']} - task may have crashed before writing output")
            if log_tokens is not None:
                log.reset(log_tokens)
            if state_token is not None:
                state.reset(state_token)
            job_logs.reset_live_job(live_token)
            with self._live_lock:
                self._live_jobs.discard(job["id"])
