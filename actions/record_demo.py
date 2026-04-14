import subprocess
import threading
import time
import urllib.request
from pathlib import Path

import core.log as log
import core.terminal as terminal
from core.claude_runner import run_haiku


def handle(payload: dict, trigger: dict, config: dict):
    key = payload["ticket_key"]
    slug = payload.get("slug", "")
    ws = config["workspace"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    docs_dir = ticket_dir / "docs"

    ticket_md = docs_dir / "ticket.md"
    manifest = docs_dir / "change-manifest.md"
    if not ticket_md.exists() and not manifest.exists():
        return

    context = ""
    if ticket_md.exists():
        context += ticket_md.read_text()[:2000]
    if manifest.exists():
        context += "\n\n" + manifest.read_text()[:2000]

    verdict = run_haiku(
        f"Can the changes described below be demoed in a web browser? "
        f"This means there are visible UI changes or user-facing behavior that can be shown. "
        f"Reply with exactly one word: YES or NO.\n\n{context}"
    )
    if not verdict or not verdict.strip().upper().startswith("YES"):
        log.emit("demo_skipped", f"Demo not applicable for {key}",
            meta={"ticket": key, "reason": verdict.strip() if verdict else "no response"})
        return

    t = threading.Thread(target=_record, args=(key, slug, ticket_dir, config), daemon=True)
    t.start()


def _record(key: str, slug: str, ticket_dir: Path, config: dict):
    ws = config["workspace"]
    scripts_dir = ws["root"] / "scripts"
    run_all = scripts_dir / "run_all.sh"
    docs_dir = ticket_dir / "docs"
    demo_path = docs_dir / "demo.webm"
    pid_file = ws["root"] / ".server_pids"

    if not run_all.exists():
        log.emit("demo_error", f"run_all.sh not found for {key}",
            meta={"ticket": key})
        return

    log.emit("demo_starting", f"Starting services for demo recording of {key}",
        meta={"ticket": key})

    proc = subprocess.Popen(
        ["bash", str(run_all), slug],
        cwd=str(ws["root"]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )

    if not _wait_for_services(timeout=180):
        log.emit("demo_error", f"Services failed to start for {key}",
            meta={"ticket": key})
        _kill_services(pid_file, proc)
        return

    log.emit("demo_recording", f"Services up, sending recording command for {key}",
        meta={"ticket": key})

    terminal.send_keys(key,
        f"Record a playwright video demonstrating the functionality described in docs/ticket.md. "
        f"Be verbose about showing the effects of the changes so we can see evidence it worked. "
        f"Save the recording to docs/demo.webm. Use resolution width: 1920, height: 1080. "
        f"The app is running at http://localhost:3000"
    )

    recorded = _poll_file(demo_path, timeout=300)

    _kill_services(pid_file, proc)

    if recorded:
        log.emit("demo_recorded", f"Demo recorded for {key}",
            meta={"ticket": key, "path": str(demo_path)})
    else:
        log.emit("demo_timeout", f"Demo recording timed out for {key}",
            meta={"ticket": key})


def _wait_for_services(timeout: int = 180) -> bool:
    ports = [8000, 8765, 3000]
    deadline = time.time() + timeout
    ready = set()
    while time.time() < deadline:
        for port in ports:
            if port in ready:
                continue
            try:
                urllib.request.urlopen(f"http://localhost:{port}/", timeout=2)
                ready.add(port)
            except Exception:
                pass
        if len(ready) == len(ports):
            return True
        time.sleep(5)
    return False


def _poll_file(path: Path, timeout: int = 300) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and path.stat().st_size > 0:
            time.sleep(2)
            return True
        time.sleep(10)
    return False


def _kill_services(pid_file: Path, proc: subprocess.Popen):
    if pid_file.exists():
        for line in pid_file.read_text().splitlines():
            pid = line.strip()
            if pid:
                subprocess.run(["kill", "-TERM", pid], capture_output=True)
        pid_file.unlink(missing_ok=True)
    proc.terminate()
    proc.wait(timeout=10)
