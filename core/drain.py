"""The deploy drain. A reload sends DRAIN_SIGNAL to frshty.py. frshty.py
stops claiming jobs, waits up to DRAIN_SECONDS for its live jobs to finish,
and then exits, so a deploy does not kill a model call part-way. A process
still alive EXIT_GRACE seconds after it sent itself SIGTERM exits hard: uvicorn
waits without limit for a request task that never ends."""
import signal

DRAIN_SIGNAL = signal.SIGUSR1
DRAIN_SECONDS = 600
EXIT_GRACE = 30
