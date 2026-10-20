"""Run a command with an X display of its own.

Usage: task_display.py COMMAND

The script starts one Xvfb, sets DISPLAY to it, and replaces itself with
`/bin/sh -c COMMAND`. Every process COMMAND starts, such as a shell, a
browser or a recorder, inherits that DISPLAY.

Xvfb picks the display number itself through -displayfd. It takes the first
number whose socket it can bind. The instance containers share the host
network, so the abstract X sockets of the host and of every other container
are visible here, and a number in use anywhere on the host is skipped. Two
tasks never get the same display.

Xvfb runs with PR_SET_PDEATHSIG. The kernel sends it SIGTERM when the
process that started it exits. That process becomes COMMAND through exec,
so the display goes away when COMMAND exits, however it exits: a normal
exit, a crash, a tmux kill-session or a SIGKILL.

When Xvfb is not installed or does not start, COMMAND runs without DISPLAY
and the reason goes to stderr.
"""
import ctypes
import os
import select
import shutil
import signal
import subprocess
import sys

SCREEN = "2560x1440x24"
PR_SET_PDEATHSIG = 1
START_TIMEOUT = 10


def _die_with(parent: int):
    def bind():
        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)
        if os.getppid() != parent:
            os._exit(1)
    return bind


def _read_display(fd: int) -> str:
    data = b""
    while not data.endswith(b"\n"):
        ready, _, _ = select.select([fd], [], [], START_TIMEOUT)
        if not ready:
            return ""
        chunk = os.read(fd, 64)
        if not chunk:
            return ""
        data += chunk
    return data.decode().strip()


def start_display() -> str:
    """The DISPLAY value of a new Xvfb bound to this process, or "" when
    no display could start."""
    xvfb = shutil.which("Xvfb")
    if not xvfb:
        print("task_display: Xvfb is not installed; running without DISPLAY", file=sys.stderr)
        return ""
    read_fd, write_fd = os.pipe()
    proc = subprocess.Popen(
        [xvfb, "-displayfd", str(write_fd), "-nolisten", "tcp", "-screen", "0", SCREEN],
        pass_fds=(write_fd,), stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        preexec_fn=_die_with(os.getpid()),
    )
    os.close(write_fd)
    try:
        number = _read_display(read_fd)
    finally:
        os.close(read_fd)
    if number.isdigit():
        return f":{number}"
    proc.kill()
    proc.wait()
    print(f"task_display: Xvfb did not start (exit {proc.returncode}); running without DISPLAY",
          file=sys.stderr)
    return ""


def main(argv: list[str]) -> None:
    if len(argv) != 2:
        raise SystemExit("usage: task_display.py COMMAND")
    display = start_display()
    if display:
        os.environ["DISPLAY"] = display
    else:
        os.environ.pop("DISPLAY", None)
    os.execv("/bin/sh", ["/bin/sh", "-c", argv[1]])


if __name__ == "__main__":
    main(sys.argv)
