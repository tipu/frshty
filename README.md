# frshty (فرشتہ)

**/fəˈrɪʃ.tə/** — a dashboard that runs your engineering day.

You assign yourself a ticket. frshty plans it, builds it, reviews its own work, opens the pull request, fixes the failing CI, answers the reviewer, logs the hours, and sends the invoice. You watch one page and step in when it asks for you.

Not a product for sale. A single-operator tool published so you can fork it and point it at your own work.

## Why it exists

A senior engineer spends most of the day on the same loop. Read the ticket. Plan it. Write it. Review it. Open a PR. Wait. Fix CI. Answer a comment. Log the time. Send the invoice. Every step is mechanical, but every step still needs a human to start it and to remember it.

frshty runs that loop. It keeps the state, starts each step, and asks for you only where judgment is required.

## A day with frshty

**8am.** The `/today` page lists the day in priority order. Tickets waiting to be classified. PR comments the agent could not fix on its own. Your own PRs that no one has reviewed in a day. PRs ready to merge. Hours you have not logged. Each row has one action.

**9am.** You approve a ticket. frshty creates a worktree, produces a plan and a change manifest, implements it, runs the tests, reviews its own diff with three reviewer personas, loops on the findings until they pass, and opens the PR.

**11am.** A teammate's PR lands in your review queue. frshty has already read the diff with three reviewer personas and drafted the comments. You edit them and submit, or you open the embedded terminal and argue with the model first.

**2pm.** CI goes red on your PR. frshty reads the failure, finds the cause, fixes it, and pushes. A reviewer leaves four comments. frshty sorts them into "fix this" and "reply to this", fixes the first group in one commit, and drafts replies for the second.

**7pm.** frshty proposes your timesheet from the tickets you actually touched. You edit and submit it to Jira.

**Friday 7pm.** It builds the invoice from approved hours, applies your rate and your flat fees, and pushes it to bill.com.

## What it does

**Ships tickets.** Discovery pulls your assigned tickets out of Jira or Linear. Each one moves through plan, build, test, prove, review, PR, merge, and post-merge validation. Every stage produces a document you can read: the technical plan, the change manifest, the review verdict, the proof that the fix works. A stage that produces no document fails and retries instead of silently claiming success.

**Reviews other people's PRs.** Three personas read the diff in parallel. Spec asks whether it delivers what the ticket asked for. Breakage asks what it will do to production. Maintainability asks whether you will regret it in three months. The findings are deduped and queued as comments you can edit, discuss one at a time in a terminal, or submit in bulk.

**Keeps your own PRs moving.** It watches CI, fixes red builds, syncs the branch with the base, classifies reviewer comments, fixes the actionable ones in a batch commit, drafts replies to the rest, and resolves the thread on the platform only after a commit actually landed.

**Checks the scope of a branch.** When a ticket is code complete, three independent models answer one question from one identical prompt: does every change on this branch belong to this ticket? A failing verdict holds the PR and the auto-merge until it is fixed. The vote is counted in code, not synthesized by a model.

**Proves a claim before you make it.** Before a drafted reply says "this is already handled", it names a test, runs that test with the branch's source and again with the source reversed, and keeps the claim only if the test passes one way and fails the other.

**Tells you what to work on.** A daily digest ranks everything that needs attention against a `PRIORITIES.md` you write yourself. The plan declares focus. The pipeline does the moving.

**Tracks work that is not a ticket.** The work board is a single global page over every project, including the ones frshty does not manage. Each item is an outcome with a definition of done, not a terminal tab. Sessions become replaceable runs underneath it. The board groups by needs you, agent working, waiting on someone else, failed or stale, and recently done.

**Explains a change.** Reviews and tickets get a generated slide walkthrough of the branch, rebuilt when the code moves.

**Triages Slack.** Mentions and direct messages arrive summarized on one page instead of in your notification tray.

**Logs time and bills for it.** Daily timesheet proposals from real ticket activity, recurring entries for standups, and weekly or monthly invoices pushed to bill.com.

**Runs every client at once.** One process serves several instances. Each client gets its own port, its own worktrees, its own credentials, and its own feature set. State is partitioned per instance, so nothing leaks between them.

## The screens

| Page | What it is for |
|---|---|
| `/today` | The day in priority order, one action per row |
| `/work` | Global outcome board across every project |
| `/` | Live event feed, attention items, status cards |
| `/reviews` | PR review queue, verdicts, queued comments |
| `/reviews/{repo}/{pr}` | Diff, inline comments, per-comment discussion, slide walkthrough |
| `/tickets` | Board of every ticket stage |
| `/tickets/{key}` | Plan and review documents, job timeline, diff, embedded terminal |
| `/wizard` | One card per PR that needs an action: merge, reply, approve, or nudge a reviewer on Slack |
| `/scheduled` | Pending PR creations, CI waits, recurring schedule |
| `/prd` | Product requirements intake and generated tickets |
| `/slack` | Mentions and summaries |
| `/timesheet`, `/billing` | Calendars of hours and invoices |
| `/config` | In-browser editor for the instance config |

## Turn on only what you need

Every capability is a flag in the instance config. A flag that is off costs nothing. Timesheet and billing also drop out of the nav when they are off.

| Flag | Gives you |
|---|---|
| `tickets` | The ticket-to-PR pipeline |
| `review_prs` | Review queue for PRs where you are a reviewer |
| `scope_review` | Consensus scope gate before the PR opens |
| `pr_autofix` | Automatic review and fix cycle on every new PR (GitHub only) |
| `defence` | Test-backed proof before a reply claims something works |
| `presentations` | Slide walkthroughs of a branch |
| `releases` | Group tickets into a release and inspect the whole release |
| `slack` | Mention triage |
| `timesheet` | Daily hour proposals |
| `billing` | Invoices via bill.com |

Separate config blocks turn on the daily manager digest (`[manager]`), the today agent (`[today_agent]`), the advisory PM reviews (`[pm_agent]`), PRD intake (`[prd]`), post-merge browser validation (`[validation]`), and the human approval gate before any ticket starts (`[ticket_approval]`).

## Quick start

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```
uv sync
cp config/example.toml config/local.toml    # edit with your values
python frshty.py config/local.toml          # one instance
python frshty.py --multi config/a.toml config/b.toml --port 7000   # several
```

Open `http://localhost:<port>`.

Credentials come from environment variables named in your config (`BB_TOKEN`, `JIRA_TOKEN`, `LINEAR_TOKEN`, and so on). See `config/example.toml` for the full list. The model CLIs (Claude Code, Codex, and the third consensus voice `agy`) each manage their own auth. Log in once on the host, then mount those auth directories into the container.

Docker:

```
cp docker-compose.example.yml docker-compose.yml    # edit volume paths
docker compose up
```

The image ships Claude Code, Codex, Gemini CLI, Playwright with Chromium, `gh`, `git`, and Python 3.12. The repo is bind-mounted, so a code change does not need a rebuild.

## Setup details

**Timezone.** Set `FRSHTY_TIMEZONE` to any IANA zone. It controls what the server calls "today", when the timesheet fills, and when billing fires. Default is `UTC`. Web pages ignore it and render every timestamp in the viewer's own zone, so two people in two zones each read their own clock on the same page.

**Per-instance Claude account.** If a client uses a separate Claude Code account, point that instance at its own config directory. frshty calls the CLI directly, so a shell alias will not reach it.

```toml
[llm]
provider = "claude"

[llm.claude]
config_dir = "~/.aimyable-claude"
args = ["--dangerously-skip-permissions"]
```

**Slack.** Requires [slack-proxy-tools](https://github.com/tipu/slack-proxy-tools) checked out and running.

**Security.** Binds to `127.0.0.1`. Endpoints are unauthenticated. Do not expose it without putting your own auth in front.

## How it works

One FastAPI process serves every instance. Work is rows in a SQLite database at `~/.frshty/frshty.db`, claimed by a small worker pool.

- **Jobs queue.** A worker claims the oldest queued job, but refuses to claim one whose ticket already has a job running. Different tickets advance in parallel; one ticket's own stages stay in order.
- **Tasks.** Every unit of work declares preconditions and postconditions. Preconditions decide whether it should run. Postconditions assert the artifact on disk before it reports success, so a silent model failure fails the job instead of skipping a stage.
- **Events.** A dispatcher reads the event table and fans rows out to tasks. Dispatch is idempotent.
- **Scheduler.** A beat thread fires cron ticks and recurring rows. Turning a feature off deletes its recurring rows on the next start.
- **Models.** Claude, Codex, and `agy` are invoked headless as subprocesses. No tmux in the pipeline. Tmux is used only for the terminal you drive yourself.
- **Platforms.** GitHub and Bitbucket sit behind one interface, as do Jira and Linear. Choice is per instance.

```text
core/       orchestration primitives: queue, worker, tasks, events, scheduler, model runners
features/   domain logic: tickets, reviewer, own PRs, scope, defence, slack, timesheet, billing
manager/    daily digest and priority ranking
services/   work items, runs, tags, debriefs
prd/        requirements intake and ticket generation
web/        pages and API
templates/  static HTML, read fresh per request
config/     one TOML per instance
```

`supervisor.py` watches every running instance and restarts a dead one. `mcp_server.py` exposes the same inspection and control surface as MCP tools, so Claude Desktop can query tickets, reviews, and events across all instances.

## License

MIT
