import threading
from datetime import datetime, timezone

import core.log as log
import core.queue as q
import core.tasks.registry as registry


class WorkerPool:
    def __init__(self, registries: dict, size: int = 4, poll_interval: float = 1.0):
        self.registries = registries
        self.size = size
        self.poll_interval = poll_interval
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()

    def start(self) -> None:
        q.sweep_stale(max_age_seconds=0)
        for i in range(self.size):
            t = threading.Thread(target=self._run, args=(i,), daemon=True, name=f"worker-{i}")
            t.start()
            self._threads.append(t)
        sweep = threading.Thread(target=self._sweep_loop, daemon=True, name="stale-sweep")
        sweep.start()
        self._threads.append(sweep)

    def stop(self) -> None:
        self._stop.set()

    def _sweep_loop(self) -> None:
        while not self._stop.is_set():
            if self._stop.wait(60):
                return
            try:
                n = q.sweep_stale()
                if n:
                    log.emit("worker_sweep", f"reset {n} stale jobs")
            except Exception as e:
                log.emit("worker_sweep_error", f"{type(e).__name__}: {e}")

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
            self._run_one(job)

    def _run_one(self, job: dict) -> None:
        import core.state as state
        instance_key = job["instance_key"]
        reg = self.registries.get(instance_key)
        if not reg:
            q.mark_done(job["id"], "failed", {"reason": f"unknown instance_key: {instance_key}"})
            return
        state_token = None
        log_tokens = None
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
            log.emit("job_started", f"{job['task']} ticket={job['ticket_key']} job_id={job['id']}")
            result = registry.run_task(ctx)
            response = {"reason": result.reason, "artifacts": result.artifacts}
            q.mark_done(job["id"], result.status, response)
            log.emit("job_finished",
                     f"{job['task']} ticket={job['ticket_key']} "
                     f"job_id={job['id']} status={result.status}"
                     f"{(' reason='+result.reason) if result.reason else ''}")
            for ev in result.next_events or []:
                try:
                    q.emit_event(source="task", kind=ev["kind"], payload=ev.get("payload", {}),
                                 instance_key=ev.get("instance_key", instance_key))
                except Exception as e:
                    log.emit("worker_next_event_error", f"{type(e).__name__}: {e}")
        finally:
            if log_tokens is not None:
                log.reset(log_tokens)
            if state_token is not None:
                state.reset(state_token)
