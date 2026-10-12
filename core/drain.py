"""The deploy drain. A reload sends DRAIN_SIGNAL to frshty.py. frshty.py
stops claiming jobs, waits up to DRAIN_SECONDS for its live jobs to finish,
and then exits, so a deploy does not kill a model call part-way."""
import signal

DRAIN_SIGNAL = signal.SIGUSR1
DRAIN_SECONDS = 600
