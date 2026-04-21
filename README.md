# frshty (فرشتہ)

**/fəˈrɪʃ.tə/** — a personal dev dashboard that automates the lifecycle of a software engineer's daily work: PR reviews, ticket-to-PR development, Slack triage, timesheet, and billing. Built around AI-assisted workflows using Claude, Codex, and Gemini CLIs running inside a dev container.

Not a product. A single-operator tool shared publicly so others can fork and adapt it.

## Quick start

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```
uv sync
cp config/example.toml config/local.toml    # edit with your values
python frshty.py config/local.toml          # single instance
# OR
python frshty.py --multi config/a.toml config/b.toml --port 7000   # multi-instance supervisor
```

Credentials via env vars referenced in your config (`BB_TOKEN`, `JIRA_TOKEN`, `LINEAR_TOKEN`, `ANTHROPIC_API_KEY`, etc). See `config/example.toml` for the full list.

Docker:

```
cp docker-compose.example.yml docker-compose.yml    # edit volume paths
docker compose up
```

## Product

### What it does for you

You assign a ticket in Jira or Linear. frshty picks it up, creates a git worktree, runs `/confer-technical-plan` to produce a plan and a change manifest, then `/tri-review` to review its own work, loops on the review until it passes, and opens a PR. When CI fails on one of your PRs, it tries to fix it. When reviewers leave comments, it classifies them as actionable or needing a reply and either fixes the code or drafts a response. When someone mentions you in Slack, it summarizes and queues the thread for you. It logs hours against the tickets you worked on that day. At the end of the week or month, it generates an invoice via bill.com.

Everything shows in a single web UI at `http://localhost:<port>`. The event feed is the center of gravity; the other pages are focused views.

### Core workflows

**Ticket pipeline.** Discovered tickets flow through stages: `new → planning → reviewing → pr_ready → pr_created → in_review → merged`. Each stage is a synchronous headless `claude -p` invocation run by a worker:

| Stage | What runs | Artifact produced |
|---|---|---|
| `start_planning` | `/confer-technical-plan docs/` | `docs/change-manifest.md`, `docs/technical-plan.md` |
| `start_reviewing` | `/tri-review` | `docs/tri-review.md` with `VERDICT: PASS` or `VERDICT: FAIL` |
| `fix_review_findings` | Read tri-review, fix blockers, re-run /tri-review | updated `docs/tri-review.md` |
| `mark_ready` | Precondition check only | transitions to `pr_ready` |
| `create_pr` | Pushes branch, opens PR | `prs` attached to ticket state |
| `fix_ci_failures` | CI causality analysis + targeted fix | amended commit |
| `backfill_artifacts` | For PRs opened outside frshty: reverse-generate planning/review docs from the diff | all three docs |

Each stage asserts its artifact via a postcondition before reporting success. A silent claude failure fails the job cleanly; the next scan re-queues.

**PR reviews.** For PRs where you're a reviewer, frshty runs three personas in parallel against the diff:

- **spec**: does it deliver what the ticket asked for?
- **breakage**: will it break something in production? (race conditions, N+1, missing auth, error handling)
- **maintainability**: will you regret this in 3 months? (complexity, DRY, naming, patterns)

The personas' findings are deduped, simplified, and merged into `review.json` + `queued_comments.json`. The `/reviews` page surfaces them; you can submit selected comments back to GitHub/Bitbucket, discuss comments one-by-one in an embedded terminal, or push the whole review.

**Slack triage.** `slack_monitor` reads a slack-proxy-tools JSONL feed, filters mentions and direct messages, and adds them to the Slack page with a Claude-generated summary.

**Timesheet.** Daily at 7pm local, `timesheet_check` walks your day's ticket activity and proposes worklog entries you can edit and submit to Jira. Recurring entries (standups, planned meetings) are defined per-config.

**Billing.** Weekly (Fri 7pm local) or monthly (last day 7pm local), `billing_check` builds an invoice from approved timesheet hours, applies your rate and flat-fee extras, and pushes it to bill.com.

**Multi-instance.** `python frshty.py --multi a.toml b.toml c.toml --port 7000` runs several dashboards from one Python process. State is partitioned by each config's `job.key` in a shared SQLite DB at `~/.frshty/frshty.db`. Each instance gets its own port from its config, its own worktree tree, and its own feature flags.

**Supervisor & MCP.** `supervisor.py` polls all running instances, detects stuck states (stale scans, wedged workers), and can auto-trigger a restart or escalate to you. `mcp_server.py` exposes the same inspection and control surface as MCP tools so Claude Desktop can query tickets, reviews, events, and trigger cycles across all instances.

### The UI

- `/` feed — event stream, attention items, status cards; noise/useful filter
- `/reviews` — open review queue; verdict badges (LLM vs platform) and approval state
- `/reviews/{repo}/{pr_id}` — diff with inline comments, per-comment discussion
- `/tickets` — board view of all ticket stages
- `/tickets/{key}` — ticket detail: docs tabs, jobs timeline, PR diff, embedded terminal
- `/scheduled` — pending PR creations, CI-pending tickets, recurring schedule rows
- `/slack` — mentions and summaries
- `/timesheet`, `/billing` — calendars of hours and invoices
- `/config` — in-browser editor for the instance's TOML

### Nav gating by feature flag

Nav items for Timesheet and Billing hide automatically on instances where the corresponding feature flag is off. The shared `/static/nav-gate.js` fetches `/api/config` and removes disabled routes from the nav.

## Technical

### Process model

One FastAPI app per Python process. `--multi` registers each config as an "instance" in an in-memory registry keyed by `job.key`, and all instances share:

- **SQLite DB** at `~/.frshty/frshty.db` (tables: `kv`, `events`, `jobs`, `scheduler`; all rows carry `instance_key`)
- **Worker pool** (default 4 threads) claiming jobs from `jobs` table, serving every instance's work
- **Beat thread** firing cron ticks (4-min cadence) and recurring scheduler rows
- **Event bus dispatcher** reading undispatched rows from `events` and routing them to tasks

Per-request state access uses a `ContextVar` (`_instance_key_cv`) so handlers, tasks, and workers see the right instance's data without explicit plumbing. The `TaskContext` passed to a task body carries `ctx.instance_key`; everything else is derived via the contextvar overlay.

### State store (`core/state.py`, `core/db.py`)

Legacy API (`state.load("tickets")`, `state.save("tickets", {...})`) wraps a row in the `kv` table: `(instance_key, key) → data JSON`. Anything that used to be a JSON file on disk is now a row. Concurrent workers serialize via SQLite's busy_timeout. `state.use(instance_key)` and `state.reset(token)` provide scoped overrides for per-job context.

### Worker queue (`core/queue.py`, `core/worker.py`)

`claim_next()` is the core primitive. It:

1. Picks the oldest `queued` job
2. **Refuses to claim** if that job's `ticket_key` has another `running` job (per-ticket mutex — prevents two stage jobs for the same ticket from colliding)
3. Atomically flips `queued → running` with `started_at`

`sweep_stale(max_age_seconds=3600)` is called by the worker pool every 60s and on startup to reset jobs that have been `running` longer than the threshold. On startup it's called with `max_age_seconds=0` because no worker can actually be running work yet — any `running` row is from a crashed prior process and should be reset immediately.

The per-ticket mutex means you can run 10 ticket pipelines concurrently without any of them interfering with each other, but a single ticket's stages stay serialized.

### Task registry (`core/tasks/registry.py`)

`@task("name", preconditions=[...], postconditions=[...], timeout=N)` registers a task function. The runner:

1. Runs preconditions. Any fail → task returns `skipped`
2. Runs the body
3. If body returned `ok`, runs postconditions. Any fail → task becomes `failed` (artifacts preserved)

Preconditions are reusable gates like `status_is("reviewing")`, `file_exists("docs/change-manifest.md")`, `feature_enabled("timesheet")`, `has_flag("_ci_failed_pending")`. Postconditions reuse the same callable shape. This lets a stage task assert its expected artifact on the filesystem before reporting success, rather than silently returning ok while nothing happened.

### Event bus (`core/event_bus.py`, `core/tasks/routes.py`)

Events are rows in the `events` table. A dispatcher thread reads undispatched rows and fans them out via registered routes:

- `cron_tick` → one `scan_tickets`/`poll_own_prs`/`poll_reviewer`/`slack_scan` per instance with the right feature flag
- `ui_retry` → re-enqueue a specific task
- `ui_set_state` → set ticket status
- `ui_notes` → archive artifacts + reset ticket to `new`

Dispatch is idempotent; the `dispatched_at` timestamp acts as the lock.

### Scheduler (`core/scheduler.py`, `core/beat.py`)

Two kinds of rows:

- **Recurring** (`cadence = "weekly"|"monthly"|"daily_19pst"`): beat thread re-fires at each `next_run_at` and advances the timestamp by cadence. Examples: `billing_check` weekly, `timesheet_check` at 7pm PT daily
- **Oneshot** (`run_at` timestamp, optional `action`): fires once. Used for scheduled PR creation after the `ticket_dev_complete` event, with optional jitter and business-hours clamping

`_seed_recurring_schedules()` on startup upserts recurring rows when their feature flag is on and **deletes them when the flag is off**, so toggling `[features] timesheet = false` in the toml cleanly removes the daily entry on next restart.

### Claude invocations (`core/claude_runner.py`)

Three headless primitives:

- `run_sonnet(prompt, worktree, tools, timeout)` — `claude -p - --model claude-sonnet-4-6 --add-dir <worktree>` (for PR review)
- `run_haiku(prompt, timeout)` — `claude -p - --model claude-haiku-4-5-20251001` (for triage, classification, 1-word verdicts)
- `run_claude_code(prompt, cwd, timeout)` — `claude -p <prompt> --dangerously-skip-permissions`, cwd-scoped (for ticket pipeline stages)

No tmux, no capture_pane polling. Tasks call these synchronously, wait for the subprocess to return, and postconditions assert the artifact. Claude's keychain/credentials come from whichever process started frshty (launchd on macOS, docker container env on Linux), which is why SSH-spawned ad-hoc invocations won't work for auth — enqueue a task instead.

### Platform abstraction (`features/platforms.py`)

`BitbucketPlatform` and `GitHubPlatform` implement a common interface: `list_review_prs`, `get_pr_info`, `get_pr_diff`, `get_pr_comments`, `post_pr_comment`, `get_pr_checks`, `get_failed_logs`, `create_pr`, `push_branch`, `merge_base`, `merge_pr`, `resolve_comment`. Platform choice comes from `job.platform`. Bitbucket uses the REST API with basic auth; GitHub uses the `gh` CLI.

When a PR state comes back non-OPEN (merged, declined, closed, superseded, deleted), the reviews endpoint rmtrees its review dir so the feed self-cleans.

### Ticket system abstraction (`features/ticket_systems.py`)

`JiraTicketSystem` and `LinearTicketSystem` implement `fetch_tickets()` returning a normalized list of tickets with key, summary, description, status, url, attachments, related, subtasks, parent. Choice comes from `job.ticket_system`.

### Docker (`Dockerfile`, `docker-compose.example.yml`)

The container has Claude Code, Codex, Gemini CLI, `gh`, `git`, and Python 3.12 pre-installed. The repo is bind-mounted to `/app` so code changes don't require a rebuild. Host auth dirs (`~/.claude`, `~/.codex`, `~/.gemini`, `~/.config/gh`) are mounted read-only or read-write depending on tool. The container runs on `--network host` so the server listens on `127.0.0.1:<port>` of the host.

### Directory layout

```
core/                 # orchestration primitives (no business logic)
├── state.py, db.py   # SQLite-backed kv + jobs
├── queue.py, worker.py  # job claim + worker pool
├── event_bus.py      # event → task routing
├── scheduler.py, beat.py  # cron + recurring
├── claude_runner.py  # headless subprocess wrappers
├── tasks/            # @task-registered units of work
└── terminal.py       # tmux wrapper (used only by CI-fix and the operator terminal websocket)

features/             # per-domain logic
├── tickets.py        # discovery, worktree setup, PR creation, CI fix, in-review handling
├── reviewer.py       # PR review with three personas
├── platforms.py      # github / bitbucket clients
├── ticket_systems.py # jira / linear clients
├── slack_monitor.py  # mentions + summaries
├── timesheet.py, billing.py, billcom.py  # time tracking + invoicing

templates/            # Jinja-free static HTML (read fresh per request)
static/               # CSS, JS (nav-gate.js), favicon, vendored libs
config/               # per-instance TOML files
migrations/           # SQL schema migrations
```

## Setup details

### Timezone

Set `FRSHTY_TIMEZONE` in your environment (any IANA zone). Default is `UTC`.

```
FRSHTY_TIMEZONE=America/Los_Angeles
```

Single source of truth for calendar-day semantics: "today", the timesheet fill window, billing fire times (Fri 7pm local, last-day-of-month 7pm local), recurring meeting weekday matching. Stored timestamps in SQLite, logs, and events are always UTC; rendering converts to this zone.

### Slack

Slack functionality requires [slack-proxy-tools](https://github.com/tipu/slack-proxy-tools) to be checked out and running. Follow the setup in that repo first.

### Security

Binds to `127.0.0.1` by default. Endpoints are unauthenticated. Do not expose without adding your own auth layer.

## License

MIT
