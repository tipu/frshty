import json
import os
import re
import subprocess
from pathlib import Path


def _env():
    return {**os.environ, "CLAUDE_CODE_ENTRYPOINT": "cli"}


def run_sonnet(prompt: str, worktree: Path | None = None, tools: list[str] | None = None, timeout: int = 600) -> str | None:
    cmd = ["claude", "-p", "-", "--model", "claude-sonnet-4-6"]
    if worktree and worktree.is_dir():
        cmd += ["--dangerously-skip-permissions", "--add-dir", str(worktree)]
        if tools:
            cmd += ["--allowedTools"] + tools
    result = subprocess.run(
        cmd, input=prompt.encode(), capture_output=True, env=_env(), timeout=timeout,
    )
    if result.returncode != 0 or not result.stdout:
        return None
    return result.stdout.decode()


def run_haiku(prompt: str, timeout: int = 120) -> str | None:
    result = subprocess.run(
        ["claude", "-p", "-", "--model", "claude-haiku-4-5-20251001"],
        input=prompt.encode(), capture_output=True, env=_env(), timeout=timeout,
    )
    if result.returncode != 0 or not result.stdout:
        return None
    return result.stdout.decode().strip()


def run_claude_code(prompt: str, cwd: Path, timeout: int = 600) -> str | None:
    result = subprocess.run(
        ["claude", "-p", prompt, "--dangerously-skip-permissions"],
        capture_output=True, text=True, cwd=str(cwd), env=_env(), timeout=timeout,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def extract_json(text: str) -> dict | None:
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    raw = m.group(1) if m else text
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except (json.JSONDecodeError, ValueError):
        pass
    for i in range(len(text) - 1, -1, -1):
        if text[i] == "{":
            try:
                obj = json.loads(text[i:])
                if isinstance(obj, dict):
                    return obj
            except (json.JSONDecodeError, ValueError):
                continue
    return None
