# frshty — Fix Plan

## Phase 1 — Security & Critical Fixes

| Task | Why | How |
|---|---|---|
| Rotate bill.com credentials immediately | `.env` with plaintext password is on disk | Change password, revoke API key, then strip `.env` from git tracking (`git rm --cached .env` and update `.gitignore` pattern) |
| Add `.env` to `.gitignore` as `/.env` (it is already) and verify it can't be re-committed | Prevent recurrence | `git check-ignore .env` to confirm; add a pre-commit hook or `core.hooksPath` that blocks `.env` |
| Remove checked-in configs with real server details (`config/*.toml`) | Credential leakage via config files | `git rm --cached config/*.toml` and add `config/*.toml` to `.gitignore` |
| Add `__pycache__/` with leading `**/` to `.gitignore`, then `git rm --cached` all tracked `__pycache__` dirs | Stop tracking bytecode | `git rm -r --cached */__pycache__/` and `git rm -r --cached __pycache__/` |

---

## Phase 2 — Error Handling Overhaul

| Task | How |
|---|---|
| Audit all 185+ `except Exception` clauses using `rg "except Exception" --include "*.py"` | Categorize each as: (a) recoverable → log+continue, (b) fatal → re-raise, (c) business-logic expected → catch specific exception type |
| Introduce a `core/exceptions.py` with domain exception hierarchy | `FrshtyError` base, then `TicketError`, `ReviewError`, `BillingError`, etc. |
| Add structured logging (`import logging` + `logger.exception()`) to every catch clause that currently silences | At minimum log the error message and traceback before continuing |
| Eliminate all bare `except: pass` (in `core/discovery.py`, `manager/staleness.py`) | Replace with specific catch or at least `except Exception: logger.warning(...)` |

---

## Phase 3 — Refactor `frshty.py` (2,244 lines → modular)

1. **Create `routes/` package** with modules per domain:
   - `routes/tickets.py` — ticket endpoints
   - `routes/reviews.py` — PR review endpoints
   - `routes/slack.py` — Slack triage endpoints
   - `routes/timesheet.py` — timesheet/billing endpoints
   - `routes/admin.py` — config, health, logging, admin
   - `routes/events.py` — event aggregation endpoint

2. **Extract helpers** into existing modules:
   - `_trim_to_utf8_boundary` → `core/utils.py` (deduplicate with version in `core/job_logs.py`)
   - HTML template serving → keep in `routes/__init__.py` or a `templates.py`
   - Config creation logic → `core/config.py`

3. **Leave `frshty.py`** as a thin entry point — imports router, calls `uvicorn.run()`, attaches middleware.

---

## Phase 4 — Test Coverage (highest-gap modules first)

| Module | Lines | Target | Approach |
|---|---|---|---|
| `features/billing.py` | 503 | Integration tests | Mock bill.com API, verify invoice creation flow |
| `features/billcom.py` | ~200 | Unit tests | Mock HTTP responses, test XML/JSON parsing |
| `features/timesheet.py` | 766 | Integration tests | Mock Jira API, verify hour logging logic |
| `features/platforms.py` | 685 | Unit + integration | Mock Jira/Linear API, test ticket parsing |
| `features/own_prs.py` | 248 | Unit tests | Mock git/GitHub CLI, test PR creation flow |
| `core/terminal.py` | 260 | Unit tests | Mock subprocess for tmux commands |
| `core/scheduler.py` | 312 | Unit tests | Verify scheduling logic, mock the worker |
| `supervisor.py` | 248 | Integration tests | Test multi-instance lifecycle |
| `mcp_server.py` | 221 | Unit tests | Verify MCP protocol handling |

**Add to CI**: `pytest --cov=features --cov=core --cov=routes --cov-report=term-missing` and enforce minimum coverage per module (start at 40%, ramp to 80%).

---

## Phase 5 — Cleanup & CI Quality

| Task | Details |
|---|---|
| Remove dead scripts from repo root | `explore_site.py`, `explore_detail_pages.py`, `explore_remaining.py`, `test_discover_instances.py`, `test_global_events.py`, `test_global_feeds_match.py`, `tickets-*.png` |
| Remove stale `.claude/worktrees/` | These are copies of the project left behind by old Claude sessions |
| Move planning docs out of `docs/` | Archive to `.archive/` or remove; keep only user-facing docs |
| Deduplicate `_trim_to_utf8_boundary` | Merge into `core/utils.py`, import in both places |
| Fix `test.yml` CI workflow | Use `uv sync` instead of `pip install`, add all deps, add `ruff check`, `mypy src/`, `pytest --cov` |
| Add `pyproject.toml` tool configs | `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]` |
| Add healthcheck endpoint | GET `/health` → returns DB status, worker pool health, uptime |

---

## Phase 6 — Architectural Deep Work

| Issue | Approach |
|---|---|
| **Dual DB layers** (`db.py` vs `state.py`) | Consolidate into `state.py` as the public API, deprecate direct `db.py` usage outside core. Move the runtime kv→tickets migration into a proper `008_migrate_kv_to_tickets.sql` script. |
| **JSON blob state in SQLite** | Add indexed columns to `tickets` table for frequently-queried fields (status, assignee, priority). Keep JSON for extensibility but migrate query paths to column lookups. |
| **No rate limiting** | Add `core/ratelimit.py` with a token-bucket per task type (e.g., 5 Claude calls/min). Wire into scheduler and PR review pipeline. |
| **Filesystem reviews → DB** | Store PR review results in SQLite instead of `~/.frshty/<instance>/reviews/*.json`. Keeps state queryable and backup-friendly. |
| **Type safety** | Add `mypy --strict` config, fix all `Any` types, add return-type annotations to all public functions. Prioritize `frshty.py` and `core/` modules. |
| **Configurable constants** | Move all hardcoded constants (`MAX_LOG_LINES`, `POLL_INTERVAL`, etc.) into a `core/settings.py` module read from env vars with sensible defaults. |

---

### Recommended execution order

```
Phase 1 ──────► Phase 2 ──────► Phase 4 (tests)
   │                                │
   ▼                                ▼
Phase 3 (refactor) ◄──────── Phase 5 (CI)
   │
   ▼
Phase 6 (architecture)
```

Phases 1–3 are blockers — do first. Phase 4 (tests) should happen right after error handling is fixed, so you can run tests without false negatives from swallowed exceptions. Phase 5 (CI) feeds back into testing. Phase 6 is ongoing, low-risk structural work.
