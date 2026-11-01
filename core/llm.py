import base64
import contextvars
import json
import os
import re
import secrets
import signal
import subprocess
import tempfile
import threading
import time
import urllib.request
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import jsonschema

import core.db as _db
import core.lsp_gate as lsp_gate
import core.log as log
import core.redact as redact
import core.state as _state
from core.job_logs import active_live_log_path, active_live_pid_path
from core.paths import review_checkout
from core.state import _instance_key_cv


_LLM_MAX_CONCURRENT = int(os.environ.get(
    "FRSHTY_MAX_CONCURRENT_LLM",
    os.environ.get("FRSHTY_CLAUDE_MAX_CONCURRENT", "15"),
))
_LLM_LIMIT_COOLDOWN_SECONDS = int(os.environ.get("FRSHTY_LLM_LIMIT_COOLDOWN_SECONDS", "1800"))
_DEFAULT_MODEL = "default"
_llm_sem = threading.BoundedSemaphore(max(1, _LLM_MAX_CONCURRENT))

_guard_blocked_cv: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "frshty_llm_guard_blocked", default=False
)


def reset_guard_blocked() -> None:
    _guard_blocked_cv.set(False)


def consume_guard_blocked() -> bool:
    val = _guard_blocked_cv.get()
    if val:
        _guard_blocked_cv.set(False)
    return val


def _flag_guard_blocked() -> None:
    _guard_blocked_cv.set(True)


_last_error_cv: contextvars.ContextVar[str] = contextvars.ContextVar(
    "frshty_llm_last_error", default=""
)


def reset_last_error() -> None:
    _last_error_cv.set("")


def consume_last_error() -> str:
    """The error text of the newest failed `claude -p` run in this context.

    A runner answers None on failure, so a caller that reports the failure
    reads the recorded reason here instead of guessing at it."""
    val = _last_error_cv.get()
    if val:
        _last_error_cv.set("")
    return val


def _cli_error_text(stdout: str, stderr: str, returncode: int) -> str:
    text, envelope = _parse_claude_json_output(stdout)
    detail = _result_error_text(envelope) if envelope else text
    parts = [p for p in (detail.strip(), stderr.strip()) if p]
    return "\n".join(parts) or f"exit code {returncode} with no output"


_providers: dict[str, "LLMProvider"] = {}
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_LLM_LIMIT_PATTERNS = (
    "out of extra usage",
    "hit your org's monthly usage limit",
    "hit your limit",
    "key limit exceeded",
    "requires more credits",
    "monthly usage limit",
    "individual spend limit",
    "/usage-credits",
    "session limit resets",
    "weekly limit resets",
)
_MIN_PLAN_BYTES = int(os.environ.get("FRSHTY_MIN_PLAN_BYTES", "400"))

READ_ONLY_TOOLS = ["Read", "Glob", "Grep", "Bash(git:*)"]
WRITE_TOOLS = ["Write", "Edit", "MultiEdit", "NotebookEdit", "WebFetch", "WebSearch"]
LSP_SHIM_DIR = Path(__file__).resolve().parent.parent / "scripts" / "lsp"


def with_lsp_shim(path: str | None) -> str:
    """`path` with scripts/lsp first, so an agent's TypeScript LSP plugin runs
    the shim that keeps every request on the semantic tsserver, and its
    Python LSP plugin runs the shim that gives pyright the repository of each
    opened file as a workspace folder."""
    return os.pathsep.join(p for p in (str(LSP_SHIM_DIR), path) if p)


def _env():
    return {**os.environ, "CLAUDE_CODE_ENTRYPOINT": "cli"}


def _active_instance_key() -> str:
    k = _instance_key_cv.get()
    if k is not None:
        return k
    return _state._default_instance_key or ""


def _active_job_key() -> str:
    try:
        from core.log import _job_key_cv
        k = _job_key_cv.get()
        return k if k is not None else ""
    except (ImportError, LookupError):
        return ""


def _guard_key() -> str:
    return _active_instance_key() or "__default__"


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _llm_limit_reason(output: str | None) -> str | None:
    if not output:
        return None
    cleaned = _strip_ansi(output).lower()
    for pattern in _LLM_LIMIT_PATTERNS:
        if pattern in cleaned:
            return pattern
    return None


def _clear_guard(key: str) -> None:
    """Drop a stale or expired guard row. A database error is reported and
    swallowed: the row stays, and the next _guard_status re-reads it."""
    try:
        _db.execute(
            "DELETE FROM kv WHERE instance_key=? AND key='llm_guard'", (key,)
        )
    except Exception as e:
        log.emit("llm_guard_clear_failed",
                 f"[{_active_instance_key()}] could not clear the LLM guard: "
                 f"{type(e).__name__}: {e}",
                 meta={"instance_key": _active_instance_key(),
                       "error": f"{type(e).__name__}: {e}"})


def _guard_status() -> tuple[bool, str, int]:
    """Whether a usage-limit cooldown currently blocks new LLM invocations.

    Every database touch fails open, like the invocation bookkeeping around
    it. The guard saves wasted invocations during a vendor cooldown; it is not
    a correctness gate. A database error that propagated from here would kill
    the invocation before the model ran, so one unreadable row would stop
    every Claude call in the process while codex and agy kept running."""
    key = _guard_key()
    try:
        row = _db.query_one(
            "SELECT data FROM kv WHERE instance_key=? AND key='llm_guard'",
            (key,),
        )
    except Exception as e:
        log.emit("llm_guard_read_failed",
                 f"[{_active_instance_key()}] could not read the LLM guard: "
                 f"{type(e).__name__}: {e}; proceeding without the cooldown",
                 meta={"instance_key": _active_instance_key(),
                       "error": f"{type(e).__name__}: {e}"})
        return False, "", 0
    if not row or not row.get("data"):
        return False, "", 0
    try:
        payload = json.loads(row["data"])
        blocked_until = datetime.fromisoformat(payload["blocked_until"])
        reason = payload.get("reason", "recent usage limit response")
    except (json.JSONDecodeError, KeyError, ValueError, TypeError):
        _clear_guard(key)
        return False, "", 0
    now = datetime.now(timezone.utc)
    if blocked_until <= now:
        _clear_guard(key)
        return False, "", 0
    remaining_s = max(1, int((blocked_until - now).total_seconds()))
    return True, reason, remaining_s


def _trip_llm_guard(output: str | None) -> str | None:
    reason = _llm_limit_reason(output)
    if reason is None:
        return None
    key = _guard_key()
    now = datetime.now(timezone.utc)
    blocked_until = now + timedelta(seconds=max(1, _LLM_LIMIT_COOLDOWN_SECONDS))
    payload = json.dumps({
        "blocked_until": blocked_until.isoformat(),
        "reason": reason,
    })
    try:
        _db.execute(
            "INSERT INTO kv(instance_key, key, data, updated_at) "
            "VALUES (?, 'llm_guard', ?, ?) "
            "ON CONFLICT(instance_key, key) DO UPDATE SET "
            "data=excluded.data, updated_at=excluded.updated_at",
            (key, payload, now.isoformat()),
        )
    except Exception as e:
        log.emit("llm_guard_write_failed",
                 f"[{_active_instance_key()}] could not persist the LLM guard: "
                 f"{type(e).__name__}: {e}; later invocations are not blocked",
                 meta={"instance_key": _active_instance_key(),
                       "error": f"{type(e).__name__}: {e}"})
    log.emit(
        "llm_guard_tripped",
        f"[{_active_instance_key()}] blocking new LLM invocations for "
        f"{_LLM_LIMIT_COOLDOWN_SECONDS}s after usage-limit response ({reason})",
        meta={"instance_key": _active_instance_key(), "reason": reason,
              "cooldown_s": _LLM_LIMIT_COOLDOWN_SECONDS},
    )
    _flag_guard_blocked()
    return reason


def _guard_block_output(reason: str, remaining_s: int) -> str:
    return (
        f"[guard] skipped LLM invocation: recent usage-limit response "
        f"({reason}); cooldown {remaining_s}s remaining"
    )


def _model_args(model: str | None) -> list[str]:
    return ["--model", model] if model else []


def _schema_args(schema: dict | None) -> list[str]:
    return ["--json-schema", json.dumps(schema)] if schema else []


def _record_start(function_name: str, model: str, prompt: str,
                  cwd: Path | None = None, tools: list[str] | None = None,
                  timeout: int | None = None) -> str | None:
    inv_id = uuid4().hex[:16]
    started = datetime.now(timezone.utc).isoformat()
    try:
        _db.execute(
            "INSERT INTO claude_invocations(id, instance_key, job_key, function_name, model, "
            "prompt, prompt_length, cwd, tools, timeout_s, started_at, status) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                inv_id,
                _active_instance_key(),
                _active_job_key(),
                function_name,
                model,
                prompt,
                len(prompt),
                str(cwd) if cwd is not None else None,
                json.dumps(tools) if tools else None,
                timeout,
                started,
                "queued",
            ),
        )
        return inv_id
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to record claude start: {e}")
        return None


def _parse_claude_json_output(raw: str) -> tuple[str, dict | None]:
    """Parse `claude -p --output-format json` stdout. Returns (text, usage_dict).
    Falls back to (raw_stripped, None) if the payload isn't a result envelope."""
    text = raw.strip()
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return text, None
    if not isinstance(parsed, dict) or parsed.get("type") != "result":
        return text, None
    return (parsed.get("result") or "").strip(), parsed


def _record_end(inv_id: str | None, started_ms: float, status: str,
                exit_code: int | None, output: str | None,
                usage: dict | None = None) -> None:
    if inv_id is None:
        return
    finished = datetime.now(timezone.utc).isoformat()
    duration_ms = int((time.monotonic() - started_ms) * 1000)
    out_text = output or ""
    cost_usd = None
    input_tokens = output_tokens = cc_tokens = cr_tokens = num_turns = None
    if usage:
        cost_usd = usage.get("total_cost_usd")
        u = usage.get("usage") or {}
        input_tokens = u.get("input_tokens")
        output_tokens = u.get("output_tokens")
        cc_tokens = u.get("cache_creation_input_tokens")
        cr_tokens = u.get("cache_read_input_tokens")
        num_turns = usage.get("num_turns")
    try:
        _db.execute(
            "UPDATE claude_invocations SET finished_at=?, duration_ms=?, status=?, "
            "exit_code=?, output=?, output_length=?, cost_usd=?, input_tokens=?, "
            "output_tokens=?, cache_creation_input_tokens=?, "
            "cache_read_input_tokens=?, num_turns=? WHERE id=?",
            (finished, duration_ms, status, exit_code, out_text, len(out_text),
             cost_usd, input_tokens, output_tokens, cc_tokens, cr_tokens, num_turns,
             inv_id),
        )
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to record claude end: {e}")


MODEL_PRICES = (
    ("claude-fable-5-1", 10.0, 50.0, 0.25),
    ("claude-mythos-5-1", 10.0, 50.0, 0.25),
    ("claude-fable-5", 10.0, 50.0, 1.0),
    ("claude-mythos-5", 10.0, 50.0, 1.0),
    ("claude-opus-5-5", 4.0, 20.0, 0.20),
    ("claude-opus-5", 5.0, 25.0, 0.50),
    ("claude-opus-4-8", 5.0, 25.0, 0.50),
    ("claude-opus-4-7", 5.0, 25.0, 0.50),
    ("claude-opus-4-6", 5.0, 25.0, 0.50),
    ("claude-sonnet-5-5", 2.0, 10.0, 0.20),
    ("claude-sonnet-5", 2.0, 10.0, 0.20),
    ("claude-sonnet-4-6", 3.0, 15.0, 0.30),
    ("claude-haiku-5-5", 0.10, 0.50, 0.01),
    ("claude-haiku-4-5", 1.0, 5.0, 0.10),
)
_HAIKU_LONG_PROMPT = 100_000
_MODEL_DATE = re.compile(r"-\d{8}$")


def message_cost(model: str, usage: dict) -> float | None:
    """The USD cost of one claude message from its usage block, None when the
    model has no price in MODEL_PRICES. MODEL_PRICES holds the first-party
    API price per million tokens of input, output and cache reads. A cache
    write costs 1.25 times the input price for the 5-minute TTL and 2 times
    for the 1-hour TTL. claude-haiku-5-5 costs 5 times more when the prompt
    is longer than 100K tokens, and fast mode doubles every price."""
    base = _MODEL_DATE.sub("", model)
    price = next((p for p in MODEL_PRICES if base == p[0]), None)
    if price is None:
        return None
    _, input_price, output_price, read_price = price
    read = int(usage.get("cache_read_input_tokens") or 0)
    written = int(usage.get("cache_creation_input_tokens") or 0)
    split = usage.get("cache_creation") if isinstance(usage.get("cache_creation"), dict) else {}
    hour = int(split.get("ephemeral_1h_input_tokens") or 0)
    plain = int(usage.get("input_tokens") or 0)
    factor = 2.0 if usage.get("speed") == "fast" else 1.0
    if price[0] == "claude-haiku-5-5" and plain + read + written > _HAIKU_LONG_PROMPT:
        factor *= 5
    return factor * (plain * input_price + int(usage.get("output_tokens") or 0) * output_price
                     + read * read_price + (written - hour) * 1.25 * input_price
                     + hour * 2.0 * input_price) / 1_000_000


def record_session_usage(inv_id: str, *, instance_key: str, function_name: str,
                         model: str, prompt: str, job_key: str, cwd: str,
                         started_at: str, finished_at: str,
                         duration_ms: int | None, status: str, usage: dict) -> None:
    """Write the usage of a session that frshty did not run through this
    module, such as an interactive task agent, to the invocation log. One
    row per session; a later call replaces the totals of the earlier one."""
    try:
        _db.execute(
            "INSERT INTO claude_invocations(id, instance_key, job_key, function_name, model, "
            "prompt, prompt_length, cwd, started_at, finished_at, duration_ms, status, "
            "input_tokens, output_tokens, cache_creation_input_tokens, "
            "cache_read_input_tokens, cost_usd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET model=excluded.model, cost_usd=excluded.cost_usd, "
            "finished_at=excluded.finished_at, duration_ms=excluded.duration_ms, "
            "status=excluded.status, input_tokens=excluded.input_tokens, "
            "output_tokens=excluded.output_tokens, "
            "cache_creation_input_tokens=excluded.cache_creation_input_tokens, "
            "cache_read_input_tokens=excluded.cache_read_input_tokens",
            (inv_id, instance_key, job_key, function_name, model, prompt,
             len(prompt), cwd, started_at, finished_at, duration_ms, status,
             usage.get("input_tokens"), usage.get("output_tokens"),
             usage.get("cache_creation_input_tokens"), usage.get("cache_read_input_tokens"),
             usage.get("cost_usd")),
        )
    except Exception as e:
        log.emit("claude_invocation_log_failed",
                 f"failed to record session usage {inv_id}: {type(e).__name__}: {e}")


class LLMProvider(ABC):
    @abstractmethod
    def thinking(self, prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, **kwargs) -> str | None:
        ...

    @abstractmethod
    def balanced(self, prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 **kwargs) -> str | None:
        ...

    @abstractmethod
    def agentic(self, prompt: str, *, cwd: Path, system_prompt: str | None = None,
                add_dirs: list[Path] | None = None, tools: list[str] | None = None,
                denied_tools: list[str] | None = None, timeout: int = 1200,
                **kwargs) -> str | None:
        ...

    @abstractmethod
    def fast(self, prompt: str, *, timeout: int = 120, **kwargs) -> str | None:
        ...


_SKIP_PERMISSIONS = "--dangerously-skip-permissions"
# A deny list has to name every tool that exists now and every one a release
# adds later, and it cannot name an MCP tool that a settings file introduces.
# `--tools ""` says the opposite: nothing at all, whatever the settings hold.
NO_TOOLS_FLAGS = ("--tools", "", "--strict-mcp-config")
# opencode reads its configuration from OPENCODE_CONFIG_CONTENT before any
# file, so this denies every tool for one invocation without touching the
# operator's opencode.json, which allows every tool to every agent.
NO_TOOLS_OPENCODE_ENV = {
    "OPENCODE_CONFIG_CONTENT": json.dumps({"permission": {"*": {"*": "deny"}}}),
}
ALL_TOOLS_OPENCODE_ENV = {
    "OPENCODE_CONFIG_CONTENT": json.dumps({"permission": {"*": {"*": "allow"}}}),
}
STRUCTURED_ONLY_TOOLS = {"*": False, "StructuredOutput": True}
_LOCAL_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
PUBLISH_DENIED_TOOLS = ("Bash(git push:*)", "Bash(curl:*)", "Bash(gh:*)")
HEADLESS_DENIED_TOOLS = ("ScheduleWakeup",)
NO_PUBLISH_RULE = (
    "Do not push, do not open a pull request, and do not update the ticket in "
    "Jira or any other tracker: frshty does those steps itself."
)
LSP_RULE = (
    "Navigate code with the LSP tool before you use grep. LSP is a deferred "
    "tool: load it once with ToolSearch, query \"select:LSP\", before its first "
    "use. On Python, JavaScript and TypeScript files, use workspaceSymbol with "
    "the symbol name as the query to find the file and line of a symbol, "
    "findReferences to find the callers of a symbol, goToDefinition to find "
    "where a symbol is defined, hover to read its type, and documentSymbol to "
    "list the symbols in a file. Do not grep for a symbol name to find a "
    "position for LSP: workspaceSymbol gives that position. "
    "Use grep only to search for text that is not a symbol, such as a log "
    "message, a config key or a comment, and for file types that have no LSP "
    "server. When ToolSearch finds no LSP tool, an LSP call is refused, or LSP "
    "answers \"No LSP server available\", use grep. "
)


REVIEW_TSSERVER = "/usr/lib/node_modules/typescript/lib/tsserver.js"
REVIEW_TS_PLUGIN_DIR = Path(__file__).resolve().parent.parent / "plugins" / "review-typescript-lsp"
MARKETPLACE_TS_PLUGIN = "typescript-lsp@claude-plugins-official"


def review_checkout_args(cwd) -> list[str]:
    """The flags a Claude run needs when its working directory is a pull
    request checkout, [] for any other directory.

    Claude Code reads .claude/settings.json and .claude/settings.local.json
    of its working directory, and a hook there runs a command at session
    start whatever the permission mode and the tool list say.
    `--setting-sources user` loads the user settings only.

    typescript-language-server runs the tsserver of the workspace when it
    finds node_modules/typescript/lib or .yarn/sdks/typescript/lib there, and
    that tsserver loads the tsconfig plugins of the workspace. The marketplace
    typescript plugin is turned off and plugins/review-typescript-lsp, which
    pins tsserver.path to the global install, takes its place. The server
    falls back to the workspace tsserver when the pinned path does not exist,
    so without the global install the session runs with no TypeScript server
    at all."""
    if not cwd or not review_checkout(cwd):
        return []
    args = ["--setting-sources", "user",
            "--settings", json.dumps({"enabledPlugins": {MARKETPLACE_TS_PLUGIN: False}})]
    if os.path.isfile(REVIEW_TSSERVER):
        args += ["--plugin-dir", str(REVIEW_TS_PLUGIN_DIR)]
    return args


def codex_review_checkout_args(cwd) -> list[str]:
    """The flags a codex run needs when its working directory is a pull
    request checkout, [] for any other directory.

    codex loads .codex/config.toml from its project root down to its working
    directory, and an [mcp_servers] entry there starts a command at session
    start, even under --dangerously-bypass-approvals-and-sandbox and with no
    trust entry for the folder. codex has no flag that skips project config.
    It skips the project layers only when the project root is marked
    untrusted, and the mark must name the root itself: a mark on a parent
    folder does not cover it. The project root is the nearest folder with a
    .git, so every ancestor of the working directory, as given and resolved,
    is marked. The override must be an inline table: codex splits a dotted
    -c key on every dot and keeps the quotes of a quoted path segment."""
    if not cwd or not review_checkout(cwd):
        return []
    dirs = sorted({str(d) for base in (Path(cwd).absolute(), Path(cwd).resolve())
                   for d in (base, *base.parents)})
    table = ", ".join(f'{json.dumps(d, ensure_ascii=False)} = {{ trust_level = "untrusted" }}' for d in dirs)
    return ["-c", f"projects = {{ {table} }}"]


def _with_codex_review_args(cmd: list[str], cwd) -> list[str]:
    if Path(cmd[0]).name != "codex" or "exec" not in cmd:
        return cmd
    at = cmd.index("exec") + 1
    return [*cmd[:at], *codex_review_checkout_args(cwd), *cmd[at:]]


def _denied_tools_args(denied_tools: tuple[str, ...] | list[str] | None) -> list[str]:
    """A `claude -p` run keeps its process alive until a scheduled wakeup
    fires, so every headless run denies HEADLESS_DENIED_TOOLS on top of the
    caller's own list."""
    return ["--disallowedTools", ",".join([*HEADLESS_DENIED_TOOLS, *(denied_tools or ())])]


TICKET_WORKSPACE_RULE = (
    "Ticket workspace rule: the ticket directory is not a git repository. Each "
    "repository is its own git worktree in a subdirectory: workspace/<repo>/ when "
    "that directory exists, otherwise <repo>/. Run git with -C <that subdirectory>, "
    "never in the ticket directory. Credentials are not in any rules file: run "
    "`python3 /app/scripts/secret.py` to list the secrets this instance holds, then "
    "`python3 /app/scripts/secret.py <name>` to print one."
)

_secret_warned: set[tuple[str, float]] = set()


def _warn_rules_file_secrets(path: Path) -> None:
    """Every ticket voice reads the rules file, codex and agy included, so a
    secret written there reaches each of those vendors on every stage."""
    try:
        key = (str(path), path.stat().st_mtime)
        if key in _secret_warned:
            return
        text = path.read_text(errors="replace")
    except OSError:
        return
    _secret_warned.add(key)
    if redact.redact_secrets(text) != text:
        log.emit("rules_file_holds_secret",
                 f"{path} holds a secret value; move it to the instance secrets/ folder "
                 "and read it with scripts/secret.py",
                 meta={"path": str(path)})


def rules_directive(paths: list) -> str:
    """The prompt lines that name the project rules files, followed by
    TICKET_WORKSPACE_RULE. Only ticket stages call this."""
    for p in paths:
        _warn_rules_file_secrets(Path(p))
    if not paths:
        return TICKET_WORKSPACE_RULE
    return ("Read every file listed below before you do anything else. Those files "
            "hold the rules of the project. Follow them for this task.\n"
            + "\n".join(f"- {p}" for p in paths)
            + "\n\n" + TICKET_WORKSPACE_RULE)


class ClaudeProvider(LLMProvider):
    def __init__(self, config: dict | None = None):
        llm_cfg = (config or {}).get("llm", {})
        claude_cfg = llm_cfg.get("claude", {})
        self.bin = claude_cfg.get("bin", "claude")
        self.extra_args = list(claude_cfg.get("args", []))
        self.env_overrides = {
            str(k): str(v)
            for k, v in claude_cfg.get("env", {}).items()
        }
        config_dir = claude_cfg.get("config_dir")
        if config_dir and "CLAUDE_CONFIG_DIR" not in self.env_overrides:
            self.env_overrides["CLAUDE_CONFIG_DIR"] = str(config_dir)

    def _env(self) -> dict[str, str]:
        env = {**_env(), lsp_gate.GATE_ENV: "1"}
        for key, value in self.env_overrides.items():
            env[key] = os.path.expanduser(value)
        env["PATH"] = with_lsp_shim(env.get("PATH"))
        return env

    def _cmd(self, *args: str) -> list[str]:
        return [self.bin, *self.extra_args, *args]

    def thinking(self, prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, session_id: str | None = None,
                 resume: bool = False, model: str | None = None,
                 allowed_tools: list[str] | None = None,
                 denied_tools: tuple[str, ...] | list[str] = PUBLISH_DENIED_TOOLS,
                 **kwargs) -> str | None:
        chosen_model = model or _DEFAULT_MODEL
        cmd = self._cmd(
            "-p", prompt,
            *_model_args(model),
            "--output-format", "stream-json",
            "--include-partial-messages",
            "--verbose",
        )
        if session_id and resume:
            cmd += ["--resume", session_id]
        elif session_id:
            cmd += ["--session-id", session_id]
        if allowed_tools:
            cmd += ["--allowedTools", ",".join(allowed_tools)]
        else:
            cmd += ["--dangerously-skip-permissions"]
        cmd += _denied_tools_args(denied_tools)
        cmd += ["--append-system-prompt", LSP_RULE]
        cmd += review_checkout_args(cwd)
        inv_id = _record_start("run_claude_code", chosen_model, prompt, cwd, allowed_tools, timeout)
        t0 = time.monotonic()
        blocked, reason, remaining_s = _guard_status()
        if blocked:
            _record_end(inv_id, t0, "blocked", None, _guard_block_output(reason, remaining_s))
            log.emit("llm_guard_blocked",
                     f"[{_active_instance_key()}] skipped Claude Code invocation "
                     f"while cooldown active ({remaining_s}s left)",
                     meta={"instance_key": _active_instance_key(),
                           "reason": reason, "remaining_s": remaining_s})
            _flag_guard_blocked()
            return None
        acquire_start = time.monotonic()
        _llm_sem.acquire()
        _mark_running(inv_id, queued_s=time.monotonic() - acquire_start)
        log_path = active_live_log_path()
        log_fh = None
        if log_path:
            try:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                if not log_path.exists():
                    log_path.touch()
                log_fh = open(log_path, "ab", buffering=0)
            except OSError:
                log_fh = None

        try:
            try:
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    cwd=str(cwd) if cwd else None, env=self._env(), text=True, bufsize=1, errors="replace",
                    start_new_session=True,
                )
            except OSError as e:
                if log_fh is not None:
                    try:
                        log_fh.close()
                    except OSError as close_error:
                        log.emit("job_log_close_failed",
                                 f"Failed to close job log: {close_error}")
                return _spawn_failed(inv_id, t0, "run_claude_code", chosen_model, e)
            pid_path = active_live_pid_path()
            if pid_path is not None:
                try:
                    pid_path.parent.mkdir(parents=True, exist_ok=True)
                    pid_path.write_text(str(proc.pid))
                except OSError as e:
                    log.emit("job_pid_write_failed", f"Failed to write pid file: {e}")
            parts: list[str] = []
            non_json: list[str] = []
            result_event: dict | None = None

            def _drain():
                nonlocal result_event
                assert proc.stdout is not None
                for line in proc.stdout:
                    try:
                        evt = json.loads(line)
                    except (json.JSONDecodeError, ValueError):
                        stripped = line.rstrip()
                        if stripped:
                            non_json.append(stripped)
                        continue
                    if evt.get("type") == "result":
                        result_event = evt
                    text = _extract_text(evt)
                    if not text:
                        continue
                    parts.append(text)
                    if log_fh is not None:
                        try:
                            log_fh.write(text.encode("utf-8"))
                        except OSError as e:
                            log.emit("job_log_write_failed", f"Failed to write to job log: {e}")

            reader = threading.Thread(target=_drain, daemon=True)
            reader.start()
            timed_out = False
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            reader.join(timeout=5)
            if log_fh is not None:
                try:
                    if timed_out:
                        log_fh.write(f"\n[TIMEOUT after {timeout}s]\n".encode())
                    elif proc.returncode != 0:
                        log_fh.write(f"\n[EXIT code={proc.returncode}]\n".encode())
                except OSError as e:
                    log.emit("job_log_write_failed", f"Failed to write job status to log: {e}")
                try:
                    log_fh.close()
                except OSError as e:
                    log.emit("job_log_close_failed", f"Failed to close job log: {e}")

            if pid_path is not None:
                try:
                    pid_path.unlink(missing_ok=True)
                except OSError as e:
                    log.emit("job_pid_unlink_failed", f"Failed to remove pid file: {e}")
        finally:
            _llm_sem.release()

        output = "".join(parts)
        if timed_out:
            if non_json:
                output = output + "\n[non-json output]\n" + "\n".join(non_json[-50:])
            _record_end(inv_id, t0, "timeout", None, output, usage=result_event)
            return None
        if proc.returncode != 0:
            result_error = _result_error_text(result_event)
            if result_error and result_error not in output:
                output = output + "\n[result]\n" + result_error
            if non_json:
                output = output + "\n[non-json output]\n" + "\n".join(non_json[-50:])
            _trip_llm_guard(output)
            _record_end(inv_id, t0, "error", proc.returncode, output, usage=result_event)
            return None
        _record_end(inv_id, t0, "success", proc.returncode, output, usage=result_event)
        return output

    def _run_print(self, cmd: list[str], *, prompt: str, function_name: str,
                   model: str, cwd: Path | None, tools: list[str] | None,
                   timeout: int, label: str,
                   schema: dict | None = None) -> tuple[str | None, dict | None]:
        """Shared body of every `claude -p` call: guard, queue, run, record.

        `cwd` is the real process working directory, not just an --add-dir
        grant, so the model explores the checkout it was pointed at.
        Returns (text, result_envelope); text is None on block/timeout/error.
        With `schema` the CLI validates the answer itself and the text is the
        JSON of its `structured_output`; a run without one returns None."""
        inv_id = _record_start(function_name, model, prompt, cwd, tools, timeout)
        t0 = time.monotonic()
        blocked, reason, remaining_s = _guard_status()
        if blocked:
            _record_end(inv_id, t0, "blocked", None, _guard_block_output(reason, remaining_s))
            _last_error_cv.set(_guard_block_output(reason, remaining_s))
            log.emit("llm_guard_blocked",
                     f"[{_active_instance_key()}] skipped {label} invocation "
                     f"while cooldown active ({remaining_s}s left)",
                     meta={"instance_key": _active_instance_key(),
                           "reason": reason, "remaining_s": remaining_s})
            _flag_guard_blocked()
            return None, None
        acquire_start = time.monotonic()
        with _llm_sem:
            _mark_running(inv_id, queued_s=time.monotonic() - acquire_start)
            try:
                result = subprocess.run(
                    cmd, input=prompt.encode(), capture_output=True,
                    cwd=str(cwd) if cwd else None, env=self._env(), timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                _record_end(inv_id, t0, "timeout", None, None)
                _last_error_cv.set(f"{label} timed out after {timeout}s")
                return None, None
            except OSError as e:
                _last_error_cv.set(f"{type(e).__name__}: {e}")
                return _spawn_failed(inv_id, t0, function_name, model, e), None
            raw = result.stdout.decode() if result.stdout else ""
            if result.returncode != 0 or not result.stdout:
                err = result.stderr.decode() if result.stderr else ""
                _last_error_cv.set(_cli_error_text(raw, err, result.returncode))
                if err:
                    raw = raw + "\n[stderr]\n" + err
                _trip_llm_guard(raw)
                _record_end(inv_id, t0, "error", result.returncode, raw)
                return None, None
            text, usage = _parse_claude_json_output(raw)
            if schema is not None:
                structured = (usage or {}).get("structured_output")
                if not isinstance(structured, dict):
                    _record_end(inv_id, t0, "error", result.returncode, text, usage=usage)
                    _last_error_cv.set(f"{label} returned no structured output")
                    _emit_schema_invalid(function_name, model, "no structured_output in the result")
                    return None, usage
                text = json.dumps(structured)
            _record_end(inv_id, t0, "success", result.returncode, text, usage=usage)
            return text, usage

    def balanced(self, prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 model: str | None = None, function_name: str = "run_balanced",
                 schema: dict | None = None, **kwargs) -> str | None:
        chosen_model = model or _DEFAULT_MODEL
        cmd = self._cmd("-p", "-", "--output-format", "json", *_model_args(model),
                        *_schema_args(schema))
        cwd = worktree if worktree and worktree.is_dir() else None
        if cwd:
            cmd += ["--dangerously-skip-permissions", "--add-dir", str(cwd),
                    "--append-system-prompt", LSP_RULE]
            if tools:
                cmd += ["--allowedTools", ",".join(tools)]
            cmd += review_checkout_args(cwd)
        cmd += _denied_tools_args(None)
        text, _ = self._run_print(cmd, prompt=prompt, function_name=function_name,
                                  model=chosen_model, cwd=cwd, tools=tools,
                                  timeout=timeout, label="Claude", schema=schema)
        return text

    def agentic(self, prompt: str, *, cwd: Path, system_prompt: str | None = None,
                add_dirs: list[Path] | None = None, tools: list[str] | None = None,
                denied_tools: list[str] | None = None, timeout: int = 1200,
                model: str | None = None, function_name: str = "run_agentic",
                schema: dict | None = None, **kwargs) -> str | None:
        """Multi-turn Claude Code run rooted in `cwd`.

        balanced() hands the model one prompt and takes the first answer, so a
        prompt that already carries the whole payload comes back in a single
        turn with no tool use. This mode instead starts the model inside the
        checkout and lets it read, grep and run git until it is done, which is
        what an interactive session does. Permission mode `dontAsk` denies
        anything outside `tools` instead of prompting, so the run stays
        read-only without a permission dialog."""
        chosen_model = model or _DEFAULT_MODEL
        cmd = self._cmd("-p", "-", "--output-format", "json", *_model_args(model),
                        "--permission-mode", "dontAsk", *_schema_args(schema))
        cmd += ["--append-system-prompt", f"{system_prompt} {LSP_RULE}" if system_prompt else LSP_RULE]
        if tools:
            cmd += ["--allowedTools", ",".join(tools)]
        cmd += _denied_tools_args(denied_tools)
        for extra in add_dirs or []:
            cmd += ["--add-dir", str(extra)]
        cmd += review_checkout_args(cwd)
        text, usage = self._run_print(cmd, prompt=prompt, function_name=function_name,
                                      model=chosen_model, cwd=cwd, tools=tools,
                                      timeout=timeout, label="Claude agent", schema=schema)
        answer_turns = 2 if schema else 1
        if text is not None and (usage or {}).get("num_turns") == answer_turns:
            log.emit("llm_agentic_single_turn",
                     f"{function_name} answered in one turn without using a tool in {cwd}",
                     meta={"function_name": function_name, "model": chosen_model,
                           "cwd": str(cwd), "prompt_length": len(prompt)})
        return text

    def fast(self, prompt: str, *, timeout: int = 120, schema: dict | None = None,
             **kwargs) -> str | None:
        """Classify or summarize one prompt in a single turn, with no tools.

        Every caller of this tier hands the model the whole payload and takes
        the text back, and several of those payloads are written by other
        people: Slack messages, PR review comments, ticket descriptions. An
        instance configured with --dangerously-skip-permissions would
        otherwise let that text drive Bash. The permission flag is dropped and
        the tool set is emptied outright, which also overrides the user,
        project and local settings files, so a settings file that allows an
        MCP tool cannot put one back."""
        model = _DEFAULT_MODEL
        cmd = [self.bin, *(a for a in self.extra_args if a != _SKIP_PERMISSIONS),
               "-p", "-", "--output-format", "json",
               *NO_TOOLS_FLAGS, *_schema_args(schema)]
        text, _ = self._run_print(cmd, prompt=prompt, function_name="run_haiku",
                                  model=model, cwd=None, tools=None,
                                  timeout=timeout, label="Claude fast", schema=schema)
        return text


class OpenCodeProvider(LLMProvider):
    def __init__(self, config: dict):
        oc = config.get("llm", {}).get("opencode", {})
        self.model_thinking = oc.get("model_thinking", "openrouter/deepseek/deepseek-v4-pro")
        self.model_balanced = oc.get("model_balanced", "openrouter/deepseek/deepseek-v4-flash")
        self.model_fast = oc.get("model_fast", "openrouter/deepseek/deepseek-v4-flash")

    def thinking(self, prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, **kwargs) -> str | None:
        cmd = ["opencode", "run", prompt, "--model", self.model_thinking,
               "--dangerously-skip-permissions"]
        return self._run(cmd, "run_thinking", self.model_thinking, prompt, cwd, timeout)

    def balanced(self, prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 function_name: str = "run_balanced", schema: dict | None = None,
                 **kwargs) -> str | None:
        cwd = worktree if worktree and worktree.is_dir() else None
        if schema:
            return self._run_structured(prompt, schema, function_name, self.model_balanced,
                                        cwd, timeout, tools=STRUCTURED_ONLY_TOOLS)
        cmd = ["opencode", "run", prompt, "--model", self.model_balanced,
               "--dangerously-skip-permissions"]
        return self._run(cmd, function_name, self.model_balanced, prompt, cwd, timeout)

    def agentic(self, prompt: str, *, cwd: Path, system_prompt: str | None = None,
                add_dirs: list[Path] | None = None, tools: list[str] | None = None,
                denied_tools: list[str] | None = None, timeout: int = 1200,
                model: str | None = None, function_name: str = "run_agentic",
                schema: dict | None = None, **kwargs) -> str | None:
        full = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
        if schema:
            return self._run_structured(full, schema, function_name, self.model_thinking,
                                        cwd, timeout, env_overrides=ALL_TOOLS_OPENCODE_ENV)
        cmd = ["opencode", "run", full, "--model", self.model_thinking,
               "--dangerously-skip-permissions"]
        return self._run(cmd, function_name, self.model_thinking, full, cwd, timeout)

    def fast(self, prompt: str, *, timeout: int = 120, schema: dict | None = None,
             **kwargs) -> str | None:
        """One-shot classification with no tools. See ClaudeProvider.fast:
        this tier reads text written by other people.

        `opencode run` has no flag that empties the tool set, and opencode.json
        in this repository allows every tool to every agent, so dropping
        --dangerously-skip-permissions alone leaves the classifier able to act.
        The deny-everything configuration is handed to it in the environment
        instead."""
        if schema:
            return self._run_structured(prompt, schema, "run_fast", self.model_fast, None,
                                        timeout, tools=STRUCTURED_ONLY_TOOLS,
                                        env_overrides=NO_TOOLS_OPENCODE_ENV)
        cmd = ["opencode", "run", prompt, "--model", self.model_fast]
        return self._run(cmd, "run_fast", self.model_fast, prompt, None, timeout,
                         env_overrides=NO_TOOLS_OPENCODE_ENV)

    def _run(self, cmd: list[str], fn_name: str, model: str, prompt: str,
             cwd: Path | None, timeout: int,
             env_overrides: dict[str, str] | None = None) -> str | None:
        inv_id = _record_start(fn_name, model, prompt, cwd, None, timeout)
        t0 = time.monotonic()
        if _opencode_guard_blocks(inv_id, t0, model):
            return None
        acquire_start = time.monotonic()
        with _llm_sem:
            _mark_running(inv_id, queued_s=time.monotonic() - acquire_start)
            try:
                result = subprocess.run(
                    cmd, capture_output=True, cwd=str(cwd) if cwd else None,
                    timeout=timeout, env={**os.environ, **(env_overrides or {})},
                )
            except subprocess.TimeoutExpired:
                _record_end(inv_id, t0, "timeout", None, None)
                return None
            except OSError as e:
                return _spawn_failed(inv_id, t0, fn_name, model, e)
            output = result.stdout.decode().strip() if result.stdout else ""
            if result.returncode != 0 or not result.stdout:
                err = result.stderr.decode() if result.stderr else ""
                if err:
                    output = output + "\n[stderr]\n" + err
                _trip_llm_guard(output)
                _record_end(inv_id, t0, "error", result.returncode, output)
                return None
            _record_end(inv_id, t0, "success", result.returncode, output)
            return output

    def _run_structured(self, prompt: str, schema: dict, fn_name: str, model: str,
                        cwd: Path | None, timeout: int, *, tools: dict | None = None,
                        env_overrides: dict[str, str] | None = None) -> str | None:
        """A schema call through a private `opencode serve` process.

        `opencode run` has no schema flag. The server API takes
        format: json_schema, adds a StructuredOutput tool, and returns the
        checked answer in info.structured. A tool map that denies "*" must
        allow StructuredOutput by name, or the model cannot answer at all.
        The server binds 127.0.0.1 with a one-time password and lives only for
        this call. Returns the JSON of the answer, or None."""
        inv_id = _record_start(fn_name, model, prompt, cwd, None, timeout)
        t0 = time.monotonic()
        if _opencode_guard_blocks(inv_id, t0, model):
            return None
        provider_id, _, model_id = model.partition("/")
        body = {"model": {"providerID": provider_id, "modelID": model_id},
                "format": {"type": "json_schema", "schema": schema},
                "parts": [{"type": "text", "text": prompt}]}
        if tools is not None:
            body["tools"] = tools
        acquire_start = time.monotonic()
        with _llm_sem:
            _mark_running(inv_id, queued_s=time.monotonic() - acquire_start)
            deadline = time.monotonic() + timeout
            password = secrets.token_urlsafe(24)
            with tempfile.TemporaryFile() as server_log:
                try:
                    server = subprocess.Popen(
                        ["opencode", "serve", "--port", "0", "--hostname", "127.0.0.1"],
                        stdin=subprocess.DEVNULL, stdout=server_log, stderr=subprocess.STDOUT,
                        cwd=str(cwd) if cwd else None,
                        env={**os.environ, **(env_overrides or {}),
                             "OPENCODE_SERVER_PASSWORD": password},
                    )
                except OSError as e:
                    return _spawn_failed(inv_id, t0, fn_name, model, e)
                try:
                    url = _opencode_serve_url(server, server_log, deadline)
                    session = _opencode_post(url, "/session", {}, password, deadline)
                    reply = _opencode_post(url, f"/session/{session['id']}/message", body,
                                           password, deadline)
                except (OSError, ValueError, KeyError, TypeError) as e:
                    if time.monotonic() >= deadline:
                        _record_end(inv_id, t0, "timeout", None, None)
                        return None
                    output = (f"{type(e).__name__}: {e}\n[server]\n"
                              + os.pread(server_log.fileno(), 65536, 0).decode(errors="replace"))
                    _trip_llm_guard(output)
                    _record_end(inv_id, t0, "error", None, output)
                    return None
                finally:
                    server.terminate()
                    try:
                        server.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()
            info = reply.get("info") if isinstance(reply, dict) else None
            structured = info.get("structured") if isinstance(info, dict) else None
            if not isinstance(structured, dict):
                error = info.get("error") if isinstance(info, dict) else None
                detail = json.dumps(error) if error else "no structured output in the reply"
                _trip_llm_guard(detail)
                _record_end(inv_id, t0, "error", None, detail)
                _last_error_cv.set(f"opencode {fn_name} returned no structured output")
                _emit_schema_invalid(fn_name, model, detail)
                return None
            text = json.dumps(structured)
            _record_end(inv_id, t0, "success", 0, text)
            return text


def _opencode_guard_blocks(inv_id: str | None, t0: float, model: str) -> bool:
    blocked, reason, remaining_s = _guard_status()
    if not blocked:
        return False
    _record_end(inv_id, t0, "blocked", None, _guard_block_output(reason, remaining_s))
    log.emit("llm_guard_blocked",
             f"[{_active_instance_key()}] skipped {model} invocation "
             f"while cooldown active ({remaining_s}s left)",
             meta={"instance_key": _active_instance_key(),
                   "reason": reason, "remaining_s": remaining_s})
    _flag_guard_blocked()
    return True


def _opencode_serve_url(server: subprocess.Popen, server_log, deadline: float) -> str:
    """The base URL `opencode serve` prints once it listens."""
    while True:
        banner = os.pread(server_log.fileno(), 65536, 0).decode(errors="replace")
        match = re.search(r"listening on (http://\S+)", banner)
        if match:
            return match.group(1)
        if server.poll() is not None:
            raise OSError(f"opencode serve exited with code {server.returncode} before it listened")
        if time.monotonic() >= deadline:
            raise TimeoutError("opencode serve did not start listening")
        time.sleep(0.05)


def _opencode_post(url: str, path: str, body: dict, password: str, deadline: float):
    token = base64.b64encode(f"opencode:{password}".encode()).decode()
    request = urllib.request.Request(
        url + path, data=json.dumps(body).encode(),
        headers={"content-type": "application/json", "authorization": f"Basic {token}"})
    with _LOCAL_OPENER.open(request, timeout=max(deadline - time.monotonic(), 1)) as response:
        return json.load(response)


def strict_object(properties: dict) -> dict:
    """A JSON Schema object that requires every property and allows no other.
    codex --output-schema accepts only this strict form; claude accepts it too."""
    return {"type": "object", "properties": properties,
            "required": list(properties), "additionalProperties": False}


def _emit_schema_invalid(fn_name: str, model: str, detail: str) -> None:
    log.emit("llm_schema_invalid",
             f"[{_active_instance_key()}] {fn_name} ({model}) answered outside its schema: {detail}",
             meta={"instance_key": _active_instance_key(), "function": fn_name,
                   "model": model, "detail": detail[:500]})


def parse_structured(text: str | None, schema: dict, *, fn_name: str,
                     model: str = "") -> dict | None:
    """The answer of a schema call as a dict, or None when there is no answer
    or it does not match `schema`. Claude, codex and opencode validate the
    answer themselves; this check keeps a wrong shape from the caller anyway."""
    if text is None:
        return None
    data = extract_json(text)
    if data is None:
        _emit_schema_invalid(fn_name, model, "no JSON object in the answer")
        return None
    try:
        jsonschema.validate(data, schema)
    except jsonschema.ValidationError as e:
        _emit_schema_invalid(fn_name, model, e.message)
        return None
    return data


def _spawn_failed(inv_id: str | None, started_ms: float, fn_name: str,
                  model: str, error: OSError) -> None:
    """Close out an invocation whose CLI never started, and return None.

    A missing, unreadable or non-executable model binary raises out of the
    spawn itself, before any exit code exists. Every runner here is documented
    to answer None on failure, so an escaping OSError crashed the caller
    instead of degrading, and left the invocation row stuck at 'running'."""
    detail = f"{type(error).__name__}: {error}"
    _record_end(inv_id, started_ms, "error", None, detail)
    log.emit("llm_spawn_failed",
             f"[{_active_instance_key()}] could not start {model} for {fn_name}: {detail}",
             meta={"instance_key": _active_instance_key(), "model": model,
                   "function": fn_name, "error": detail})
    return None


def _mark_running(inv_id: str | None, queued_s: float | None = None) -> None:
    if inv_id is None:
        return
    try:
        _db.execute("UPDATE claude_invocations SET status='running', running_at=? WHERE id=?",
                    (datetime.now(timezone.utc).isoformat(), inv_id))
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to mark running: {e}")
    if queued_s is not None and queued_s >= 30:
        log.emit("llm_sem_wait",
                 f"invocation {inv_id} waited {queued_s:.0f}s for an LLM slot before starting",
                 meta={"invocation_id": inv_id, "queued_s": round(queued_s)})


def _extract_text(evt: dict) -> str:
    if evt.get("type") != "stream_event":
        return ""
    inner = evt.get("event") or {}
    if inner.get("type") != "content_block_delta":
        return ""
    delta = inner.get("delta") or {}
    if delta.get("type") == "text_delta":
        return delta.get("text") or ""
    return ""


def _result_error_text(evt: dict | None) -> str:
    if not evt:
        return ""
    lines: list[str] = []
    result = evt.get("result")
    if isinstance(result, str) and result.strip():
        lines.append(result.strip())
    errors = evt.get("errors")
    if isinstance(errors, list):
        lines.extend(str(e).strip() for e in errors if str(e).strip())
    elif isinstance(errors, str) and errors.strip():
        lines.append(errors.strip())
    return "\n".join(lines)


def _get_provider() -> LLMProvider:
    key = _active_instance_key()
    if key not in _providers:
        _providers[key] = ClaudeProvider()
    return _providers[key]


def configure(config: dict) -> None:
    key = config["job"]["key"]
    llm_cfg = config.get("llm", {})
    provider_name = llm_cfg.get("provider", "claude")
    if provider_name == "opencode":
        _providers[key] = OpenCodeProvider(config)
    else:
        _providers[key] = ClaudeProvider(config)


def run_thinking(prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, **kwargs) -> str | None:
    return _get_provider().thinking(prompt, cwd=cwd, timeout=timeout, **kwargs)


def run_balanced(prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 **kwargs) -> str | None:
    return _get_provider().balanced(prompt, worktree=worktree, tools=tools,
                                    timeout=timeout, **kwargs)


def run_agentic(prompt: str, *, cwd: Path, timeout: int = 1200, **kwargs) -> str | None:
    return _get_provider().agentic(prompt, cwd=cwd, timeout=timeout, **kwargs)


def run_fast(prompt: str, *, timeout: int = 120, **kwargs) -> str | None:
    return _get_provider().fast(prompt, timeout=timeout, **kwargs)


def validate_model_output(text: str | None, *, exit_code: int | None = 0,
                          min_bytes: int = _MIN_PLAN_BYTES) -> tuple[bool, str]:
    """Sanity-gate a model's plan output before it feeds consensus.

    Structural only — no content matching, so a plan that legitimately
    discusses error strings ("permission denied", etc.) is never false-rejected.
    The observed failure captures (PATH error, rate-limit banner, empty/truncated
    output) all fail on exit code or size: a non-zero exit, or output shorter
    than a real plan. Returns (ok, reason)."""
    if exit_code not in (0, None):
        return False, f"exit_code={exit_code}"
    if not text or not text.strip():
        return False, "empty output"
    stripped = text.strip()
    if len(stripped) < min_bytes:
        return False, f"too short ({len(stripped)}B < {min_bytes}B)"
    return True, "ok"


CODEX_EXEC = ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox",
              "--skip-git-repo-check"]
DEEPSEEK_REVIEWER_MODEL = "openrouter/deepseek/deepseek-v4.1-flash"
DEEPSEEK_RUN = ["opencode", "run", "-m", DEEPSEEK_REVIEWER_MODEL]
DEEPSEEK_ENV = {"OPENCODE_DISABLE_CLAUDE_CODE": "1"}
DEEPSEEK_SHELL = f"OPENCODE_DISABLE_CLAUDE_CODE=1 opencode run -m {DEEPSEEK_REVIEWER_MODEL}"


def run_external_model(cmd: list[str], *, fn_name: str, model: str, prompt: str,
                       cwd: Path | None = None, timeout: int = 600,
                       env_extra: dict[str, str] | None = None,
                       last_message_file: Path | None = None,
                       transcript_file: Path | None = None,
                       stdin_text: str | None = None,
                       output_schema: dict | None = None) -> tuple[str | None, int | None]:
    """Run a non-Claude model CLI (codex, agy) under the same invocation
    logging as Claude. Returns (text, exit_code); both are None when the CLI
    times out or cannot be started at all.

    A `codex exec` run whose cwd is a pull request checkout gets the flags of
    codex_review_checkout_args.

    `output_schema` is for `codex exec` only: it is written to a file and
    passed as --output-schema, and the caller reads the answer with
    parse_structured.

    Pass the prompt via `stdin_text` (with a `-` placeholder in `cmd`) instead
    of as an argv element when it can be large: Linux caps a single argv
    element at 128 KB and the spawn fails with E2BIG past that.

    Prefers `last_message_file` (e.g. codex `-o`) when it holds content, else
    falls back to stdout. The full stdout+stderr is written to
    `transcript_file` for debugging — callers must read the returned text, not
    the transcript. Vendor usage limits do NOT trip the shared Claude guard;
    they surface to the caller via validate_model_output instead."""
    if output_schema is not None:
        with tempfile.TemporaryDirectory(prefix="frshty-schema-") as tmp:
            schema_file = Path(tmp) / "schema.json"
            schema_file.write_text(json.dumps(output_schema))
            at = cmd.index("exec") + 1
            return run_external_model(
                [*cmd[:at], "--output-schema", str(schema_file), *cmd[at:]],
                fn_name=fn_name, model=model, prompt=prompt, cwd=cwd, timeout=timeout,
                env_extra=env_extra, last_message_file=last_message_file,
                transcript_file=transcript_file, stdin_text=stdin_text)
    cmd = _with_codex_review_args(cmd, cwd)
    inv_id = _record_start(fn_name, model, prompt, cwd, None, timeout)
    t0 = time.monotonic()
    env = {**os.environ, **(env_extra or {})}
    _mark_running(inv_id)
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE if stdin_text is not None else None,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            cwd=str(cwd) if cwd else None, env=env, start_new_session=True,
        )
    except OSError as e:
        return _spawn_failed(inv_id, t0, fn_name, model, e), None
    try:
        out, err = proc.communicate(
            input=stdin_text.encode() if stdin_text is not None else None, timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            proc.kill()
        proc.communicate()
        _record_end(inv_id, t0, "timeout", None, None)
        return None, None
    stdout = out.decode(errors="replace") if out else ""
    stderr = err.decode(errors="replace") if err else ""
    if transcript_file is not None:
        try:
            transcript_file.parent.mkdir(parents=True, exist_ok=True)
            transcript_file.write_text(stdout + ("\n[stderr]\n" + stderr if stderr else ""))
        except OSError as e:
            log.emit("external_model_transcript_failed",
                     f"failed to write transcript for {model}: {e}")
    text = ""
    if last_message_file is not None:
        try:
            if last_message_file.is_file():
                text = last_message_file.read_text()
        except OSError:
            text = ""
    if not text.strip():
        text = stdout
    status = "success" if proc.returncode == 0 else "error"
    _record_end(inv_id, t0, status, proc.returncode, text)
    return text, proc.returncode


def extract_json(text: str) -> dict | None:
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    raw = m.group(1) if m else text
    try:
        obj = json.loads(raw, strict=False)
        if isinstance(obj, dict):
            return obj
    except (json.JSONDecodeError, ValueError):
        pass
    for i in range(len(text) - 1, -1, -1):
        if text[i] == "{":
            try:
                obj = json.loads(text[i:], strict=False)
                if isinstance(obj, dict):
                    return obj
            except (json.JSONDecodeError, ValueError):
                continue
    for block in reversed(re.findall(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)):
        try:
            obj = json.loads(block, strict=False)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, dict):
            return obj
    decoder = json.JSONDecoder(strict=False)
    found = None
    i = text.find("{")
    while i != -1:
        try:
            obj, end = decoder.raw_decode(text, i)
        except (json.JSONDecodeError, ValueError):
            i = text.find("{", i + 1)
            continue
        if isinstance(obj, dict):
            found = obj
        i = text.find("{", end)
    return found
