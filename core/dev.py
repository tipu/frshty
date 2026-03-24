#!/usr/bin/env python3
import os
import signal
import subprocess
import sys

from watchfiles import watch

def main():
    if len(sys.argv) < 2:
        print("Usage: dev.py <config.toml>")
        sys.exit(1)

    config_path = sys.argv[1]
    watch_dir = os.path.dirname(os.path.abspath(__file__))
    proc = None

    def start():
        nonlocal proc
        proc = subprocess.Popen([sys.executable, "frshty.py", config_path])
        print(f"[dev] started pid {proc.pid}")

    def stop():
        nonlocal proc
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=5)
            print(f"[dev] stopped pid {proc.pid}")
        proc = None

    start()
    try:
        for changes in watch(watch_dir, watch_filter=lambda _, p: p.endswith(".py") or p.endswith(".html")):
            changed = [os.path.basename(p) for _, p in changes]
            print(f"[dev] changed: {', '.join(changed)} — restarting")
            stop()
            start()
    except KeyboardInterrupt:
        stop()

if __name__ == "__main__":
    main()
