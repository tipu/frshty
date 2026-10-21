"""Run a command with an X display of its own that starts on first use.

Usage: task_display.py COMMAND

The script reserves one X display number, sets DISPLAY to it, and replaces
itself with `/bin/sh -c COMMAND`. Every process COMMAND starts, such as a
shell, a browser or a recorder, inherits that DISPLAY.

No Xvfb runs until a process opens a connection to that display. Most tasks
never open a window, so most tasks never pay for an Xvfb. A small watcher
process holds the display socket. On the first connection it starts Xvfb and
then relays every connection to it, file descriptors included, so MIT-SHM
works through the relay.

The display number is the first number whose abstract socket the watcher
can bind. The instance containers share the host network, so the abstract
X sockets of the host and of every other container are visible here, and a
number in use anywhere on the host is skipped. Xvfb itself picks a second
number through -displayfd the same way. Two tasks never get the same display.

The watcher and Xvfb run with PR_SET_PDEATHSIG. The kernel sends the watcher
SIGTERM when the process that started it exits. That process becomes COMMAND
through exec. Xvfb gets SIGTERM when the watcher exits. The display goes away
when COMMAND exits, however it exits: a normal exit, a crash, a tmux
kill-session or a SIGKILL.

The containers share the host network, so a process outside the task can
reach the display socket. Xvfb therefore accepts only clients that hold a
random cookie. The cookie sits in AUTH_DIR in a file named after the PID of
COMMAND, and XAUTHORITY points COMMAND at it. Each start removes the cookie
files of processes that no longer run.

When Xvfb is not installed, COMMAND runs without DISPLAY. When Xvfb does not
start on first use, the watcher closes the connection. The reason goes to
stderr in both cases.
"""
import ctypes
import os
import secrets
import select
import selectors
import shutil
import signal
import socket
import stat
import struct
import subprocess
import sys
import threading

SCREEN = "2560x1440x24"
PR_SET_PDEATHSIG = 1
START_TIMEOUT = 10
AUTH_DIR = "/tmp/frshty-xauth"
FAMILY_WILD = 0xFFFF
COOKIE_NAME = b"MIT-MAGIC-COOKIE-1"
SOCKET_DIR = "/tmp/.X11-unix"
SOCKET_PREFIX = b"\0/tmp/.X11-unix/X"
DISPLAY_NUMBERS = range(1, 1000)
CHUNK = 65536
MAX_FDS = 253


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


def start_display(xvfb: str, auth: str) -> str:
    """The DISPLAY value of a new Xvfb bound to this process that accepts
    the cookie in `auth`, or "" when no display could start."""
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
        print(f"task_display: Xvfb did not start ({e})", file=sys.stderr)
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
    print(f"task_display: Xvfb did not start (exit {proc.returncode})", file=sys.stderr)
    return ""


def _path_listener(number: int) -> socket.socket | None:
    """A socket that listens on the file of display `number`, which X
    libraries without abstract socket support connect to, or None."""
    path = f"{SOCKET_DIR}/X{number}"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        os.makedirs(SOCKET_DIR, mode=0o1777, exist_ok=True)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        listener.bind(path)
    except OSError as e:
        print(f"task_display: cannot listen on {path} ({e})", file=sys.stderr)
        listener.close()
        return None
    listener.listen(16)
    return listener


def reserve_display() -> tuple[int, list[socket.socket]]:
    """A free display number and the sockets that listen on it."""
    for number in DISPLAY_NUMBERS:
        if os.path.exists(f"/tmp/.X{number}-lock"):
            continue
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(SOCKET_PREFIX + str(number).encode())
        except OSError:
            listener.close()
            continue
        listener.listen(16)
        path_listener = _path_listener(number)
        return number, [listener] + ([path_listener] if path_listener else [])
    raise OSError("no free X display number")


def _relay(src: socket.socket, dst: socket.socket):
    try:
        while True:
            data, fds, flags, _addr = socket.recv_fds(src, CHUNK, MAX_FDS)
            try:
                if not data or flags & socket.MSG_CTRUNC:
                    break
                if fds:
                    data = data[socket.send_fds(dst, [data], fds):]
                dst.sendall(data)
            finally:
                for fd in fds:
                    os.close(fd)
    except OSError:
        pass
    for sock in (src, dst):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def _serve(client: socket.socket, server: socket.socket):
    upstream = threading.Thread(target=_relay, args=(client, server), daemon=True)
    upstream.start()
    _relay(server, client)
    upstream.join()
    client.close()
    server.close()


def watch(listeners: list[socket.socket], xvfb: str, auth: str):
    """Start Xvfb on the first connection to `listeners` and relay every
    connection to it. Xvfb starts from this thread, because PR_SET_PDEATHSIG
    fires when the thread that started it exits."""
    selector = selectors.DefaultSelector()
    for listener in listeners:
        selector.register(listener, selectors.EVENT_READ)
    display = ""
    while True:
        for key, _ in selector.select():
            client, _ = key.fileobj.accept()
            if not display:
                display = start_display(xvfb, auth)
            if not display:
                client.close()
                continue
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                server.connect(SOCKET_PREFIX + display[1:].encode())
            except OSError as e:
                print(f"task_display: cannot reach Xvfb on {display} ({e})", file=sys.stderr)
                client.close()
                server.close()
                continue
            threading.Thread(target=_serve, args=(client, server), daemon=True).start()


def _remove_socket_files(listeners: list[socket.socket]):
    for listener in listeners:
        name = listener.getsockname()
        if isinstance(name, str) and name:
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass


def spawn_watcher(listeners: list[socket.socket], xvfb: str, auth: str):
    """Run `watch` in a child process that exits with this process and
    removes the socket files it listens on."""
    parent = os.getpid()
    if os.fork() == 0:
        try:
            signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
            _die_with(parent)()
            os.setpgid(0, 0)
            devnull = os.open(os.devnull, os.O_RDWR)
            os.dup2(devnull, 0)
            os.dup2(devnull, 1)
            watch(listeners, xvfb, auth)
        except SystemExit:
            pass
        except Exception as e:
            print(f"task_display: the display watcher stopped ({e!r})", file=sys.stderr)
        finally:
            _remove_socket_files(listeners)
            os._exit(1)
    for listener in listeners:
        listener.close()


def main(argv: list[str]) -> None:
    if len(argv) != 2:
        raise SystemExit("usage: task_display.py COMMAND")
    display = ""
    xvfb = shutil.which("Xvfb")
    if not xvfb:
        print("task_display: Xvfb is not installed; running without DISPLAY", file=sys.stderr)
    else:
        try:
            auth = write_cookie()
            number, listeners = reserve_display()
            spawn_watcher(listeners, xvfb, auth)
            display = f":{number}"
        except OSError as e:
            print(f"task_display: cannot set up the display ({e}); running without DISPLAY",
                  file=sys.stderr)
    if display:
        os.environ["DISPLAY"] = display
        os.environ["XAUTHORITY"] = auth
    else:
        os.environ.pop("DISPLAY", None)
    os.execv("/bin/sh", ["/bin/sh", "-c", argv[1]])


if __name__ == "__main__":
    main(sys.argv)
