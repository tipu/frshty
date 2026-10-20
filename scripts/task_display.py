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

The containers share the host network, so a process outside the task can
reach the display socket. Xvfb therefore accepts only clients that hold a
random cookie. The cookie sits in AUTH_DIR in a file named after the PID of
COMMAND, and XAUTHORITY points COMMAND at it. Each start removes the cookie
files of processes that no longer run.

When Xvfb is not installed or does not start, COMMAND runs without DISPLAY
and the reason goes to stderr.
"""
import ctypes
import os
import secrets
import select
import shutil
import signal
import stat
import struct
import subprocess
import sys

SCREEN = "2560x1440x24"
PR_SET_PDEATHSIG = 1
START_TIMEOUT = 10
AUTH_DIR = "/tmp/frshty-xauth"
FAMILY_WILD = 0xFFFF
COOKIE_NAME = b"MIT-MAGIC-COOKIE-1"


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


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def _prune_cookies():
    for name in os.listdir(AUTH_DIR):
        if name.isdigit() and not _pid_alive(int(name)):
            try:
                os.unlink(os.path.join(AUTH_DIR, name))
            except FileNotFoundError:
                pass


def _auth_entry(cookie: bytes) -> bytes:
    """One Xauthority entry that matches any host and any display number."""
    fields = b"".join(struct.pack(">H", len(f)) + f for f in (b"", b"", COOKIE_NAME, cookie))
    return struct.pack(">H", FAMILY_WILD) + fields


def write_cookie() -> str:
    """The path of a new Xauthority file that holds a random cookie."""
    os.makedirs(AUTH_DIR, mode=0o700, exist_ok=True)
    info = os.lstat(AUTH_DIR)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise OSError(f"{AUTH_DIR} is not a private directory of this user")
    _prune_cookies()
    path = os.path.join(AUTH_DIR, str(os.getpid()))
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(_auth_entry(secrets.token_bytes(16)))
    return path


def start_display(auth: str) -> str:
    """The DISPLAY value of a new Xvfb bound to this process that accepts
    the cookie in `auth`, or "" when no display could start."""
    xvfb = shutil.which("Xvfb")
    if not xvfb:
        print("task_display: Xvfb is not installed; running without DISPLAY", file=sys.stderr)
        return ""
    read_fd, write_fd = os.pipe()
    try:
        proc = subprocess.Popen(
            [xvfb, "-displayfd", str(write_fd), "-auth", auth, "-nolisten", "tcp",
             "-screen", "0", SCREEN],
            pass_fds=(write_fd,), stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            preexec_fn=_die_with(os.getpid()),
        )
    except OSError as e:
        os.close(read_fd)
        os.close(write_fd)
        print(f"task_display: Xvfb did not start ({e}); running without DISPLAY", file=sys.stderr)
        return ""
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
    try:
        auth = write_cookie()
    except OSError as e:
        print(f"task_display: cannot write the X cookie ({e}); running without DISPLAY",
              file=sys.stderr)
        auth = ""
    display = start_display(auth) if auth else ""
    if display:
        os.environ["DISPLAY"] = display
        os.environ["XAUTHORITY"] = auth
    else:
        os.environ.pop("DISPLAY", None)
    os.execv("/bin/sh", ["/bin/sh", "-c", argv[1]])


if __name__ == "__main__":
    main(sys.argv)
