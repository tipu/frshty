<!-- Working notes from an architecture audit of this repository.
     The operator-facing report that reconciles these notes with a second
     independent pass is served from the work board as an HTML page. This file
     is the raw evidence: file and line citations for every finding, and the
     commands used to establish them.

     These notes were produced against e1ea312. The report they feed was
     re-verified against d383649 and carries the current line numbers. Where a
     citation here disagrees with the report, the report is current. -->

# frshty architectural audit

Audited checkout: `e1ea312a49b3dc8cd65c327ad4f9fb7084b757ae`.

Scope: the 125 Python files under `core/` (including `core/tasks/`), `features/`, `web/`, `services/`, `manager/`, `pm/`, `prd/`, `actions/`, `scripts/`, and the three requested entrypoints: 39,178 lines. I inventoried definitions and imports across that scope, read implementation slices throughout the production packages, and searched the rest of the repository for callers, registrations, URLs, and configuration references. I did not read every test file or run workflows against the owner's database, repositories, accounts, or agents. Findings below are static architectural findings, not claims of observed production incidents. Line ranges refer to this checkout and are inclusive.

The main opportunity is to give existing behavior a single owner. frshty already has useful shared components—`core.git_util`, `core.branch_sync`, `features.pr_ci`, per-ticket transactions, and `features.timesheet_select`—but callers still implement alternate paths around them. The ticket pipeline and the newer work-item system are distinct products sharing infrastructure; consolidating their infrastructure is more valuable and less disruptive than forcing both into one workflow state machine.

## 1. LAYERING

### The real layers

| Actual layer | Where it lives | What it actually owns |
| --- | --- | --- |
| Process composition and discovery | `frshty.py:75-176`, `core/runtime.py:251-331`, `supervisor.py:217-244`, `core/discovery.py:11-106` | Configuration, instance registration, Git identity/preflight, workers, timers, HTTP serving, external supervision. |
| User and machine entrypoints | `web/*.py`, `mcp_server.py:14-240`, `scripts/work_hook.py:179-270`, `scripts/codex_notify.py:25-65` | HTTP/WebSocket/MCP/hook protocols. Some also own complete workflows. |
| Durable ticket-job execution | `core/queue.py:11-134`, `core/event_bus.py:29-97`, `core/worker.py:45-318`, `core/tasks/registry.py:10-180`, `core/tasks/routes.py:5-77` | Queueing, serialization, dispatch, task lifecycle, status effects, recovery. This is application orchestration despite its `core` location. |
| Ticket workflow | `features/tickets.py`, `features/ticket_states.py`, `core/tasks/tickets.py` | Discovery, dependencies, approval, planning, review, testing, proof, PRs, comments, CI, merging, reopening. Ownership spans all three files. |
| Work-item/session workflow | `services/work_launch.py`, `services/work_store.py`, `services/work_worktree.py`, `services/work_debrief.py` | Proposals, launch/resume, terminal interaction, hook decisions, completion, follow-ups, worktrees, retention. These are not merely storage services. |
| Product-specific application services | `features/reviewer.py`, `features/billing.py`, `features/timesheet.py`, `features/slack_conversations.py`, `manager/`, `pm/`, `prd/` | Review production, billing/calendar rules, Slack intake, prioritization, requirements and release evaluation. |
| External and OS adapters | `features/platforms.py`, `features/ticket_systems.py`, `features/billcom.py`, `core/llm.py`, `core/terminal.py`, `core/git_util.py` | GitHub/Bitbucket/Jira/Linear/Bill.com, CLI execution, tmux, Git, model processes. Several adapters also contain workflow policy. |
| Persistence and read models | `core/db.py`, `core/state.py`, `core/comments.py`, `services/review_store.py`, portions of nearly every feature, `features/ticket_timeline.py` | SQLite rows and JSON blobs, filesystem artifacts, projections for the UI. There is no exclusive persistence boundary. |
| Relatively isolated decision logic | `core/ticket_status.py:4-69`, `features/timesheet_select.py:13-167`, `prd/parser.py:18-76`, `prd/diff.py:14-35` | Transition graph, allocation, parsing, diffing. These are good examples to preserve when extracting more policy. |

### Concrete boundary leaks

1. **Manual PR submission is a second application service inside a route module.** `web/tickets.py:_submit_pr_sync:310-384` validates state, stages and commits, fetches, checks changes, determines the branch, pushes, creates PRs, transitions the ticket, logs, and schedules advancement. It uses `git commit --no-verify` at `341-342`. The automated path is `features/tickets.py:_create_pr:1784-1900`, reached through `core/tasks/tickets.py:create_pr:2480-2499` and `core/scheduler.py:_execute_create_pr:265-292`. Extract one PR-submission service with explicit manual/automatic inputs; do not silently erase their existing policy differences during extraction.

2. **Other routes own workflow decisions and agent execution.** `web/tickets.py:api_restart_ticket:1065-1100` chooses the restart stage; `api_approve_ticket:1268-1295` selects PRD setup versus advancement; `api_merge_ticket:1049-1061` calls the private feature merge helper and saves its returned snapshot. `web/reviews.py:_run_review:29-69` runs and persists a review in a process launched by `api_submit_review:73-92`; `_generate_file_summaries:351-366` invokes an LLM from a GET path; `api_start_discuss:408-464` provisions a worktree and launches a terminal. `web/today.py:api_today_launch:424-482` implements another launch/resume workflow. These should be thin entrypoints into application services, with background work submitted through one managed execution interface.

3. **Persistence reaches upward into release policy and the running application.** `core/state.py:_maybe_fire_release_trigger:313-333` imports `features.releases` and is called by ticket writes. `features/releases.py:maybe_trigger_inspect:281-325` imports `core.runtime`, looks up configuration, evaluates release completion, and enqueues a job. In `update_ticket:462-511`, that callback executes before the enclosing transaction exits; release queries use other connections. In `save_ticket:336-373`, it executes after commit. Thus the same logical transition has different visibility/transaction boundaries depending on the writer. A ticket-write result plus an application-owned release subscriber, or a transactional outbox, is a cleaner boundary.

4. **The task framework depends on private ticket implementation details.** `core/tasks/registry.py:_release_gate_on_failure:105-131` imports `_GATE_OCCUPYING_STATUSES` from `features.tickets` at `123`. `core/tasks/preconditions.py:repo_gate_clear:74-80` imports `_repo_gate_blocked` at `78`. The ticket feature in turn imports `core.tasks.tickets.commit_repo_changes` in `_commit_pr_comment_changes:2055-2066`. Put reusable gate policy and commit execution outside the task entrypoint module.

5. **Work launching imports the ticket pipeline to run repository checks.** `services/work_launch.py:16-18` imports `TEST_RUN_TIMEOUT`, `_NO_LOCAL_PY_VENV_SENTINEL`, `_detect_runner`, and `_run_repo_tests` from the 2,817-line task module; `_gate_tests:900-913` uses them. Importing a general work service consequently pulls in task decorators and ticket-specific dependencies. A `core/repo_tests.py` seam would remove this coupling without changing either workflow.

6. **Platform adapters contain development workflow policy.** `features/platforms.py:_resolve_merge_conflicts:41-113` assembles an agent prompt, invokes an LLM, checks syntax and conflict markers, stages changes, and commits. `_CIMonitorMixin.monitor_ci:194-284` mutates ticket-like state and decides CI timeout/progression. Keep remote API normalization in platform adapters; move conflict repair and CI decisions into application services.

7. **SQL and stored JSON schemas leak throughout product code.** Examples: `features/releases.py:upsert_release:37-55`, `features/validation.py:_record_run:32-41`, `features/slack_conversations.py:_write_message:751-815`, `manager/staleness.py:needs_classification:25-45`, `pm/runner.py:_candidate_summary:16-32`, `prd/orchestrator.py:_create_generated_ticket:70-98`, `web/tickets.py:api_tickets_list:388-434`, `web/observability.py:api_claude_invocations:17-61`, and `web/today.py:api_today_answer:133-170`. These callers know table names, JSON fields, or both. Extract stores by aggregate—tickets, reviews, work items, Slack conversations—rather than a generic repository for every table.

8. **Multi-instance scope has several owners and is not consistently carried into asynchronous work.** HTTP binds config/state/log context in `web/state.py:multi_apply_host:77-93`; workers independently bind state/log/job context in `core/worker.py:_run_one:248-318`. `core/llm.py:_active_instance_key:68-72` falls back to process-global state. `core/consensus_plan.py:_fan_out:83-130` and `features/reviewer.py:_run_personas_for_providers:434-451` submit work to thread pools without passing an instance context or wrapping each submission in a copied context. Provider selection/log attribution therefore relies on the fallback in those threads. Separately, `features/timesheet.py:_init_cache:33-43`, `check:60-62`, and `build_timesheet:282-284` use one process-global cache file selected on first use; `GitHubPlatform._repo_cache` at `features/platforms.py:682` is class-level and keyed only by short repository name. A small explicit instance context and instance-owned caches are higher-value than further directory rearrangement.

9. **There are multiple scheduling/execution owners.** The durable event dispatcher and worker pool coexist with synchronous configured actions (`core/events.py:10-16`, `actions/schedule_pr.py:8-39`), legacy one-shot execution (`core/scheduler.py:136-162`), recurring SQL enqueueing (`165-208`), web-created processes/threads, and a scanner called “debrief” that also suspends sessions, collects artifacts/worktrees, and sweeps stale work (`services/work_debrief.py:_scan_loop:201-241`). `supervisor.py:_detect_problems:95-115` is a separate HTTP health observer; `manager/watchdog.py:scan:556-658` diagnoses product-level stalled work. Keep the latter roles distinct, but make lifecycle ownership explicit in composition.

10. **External integration code crosses the web/service boundary.** `web/slack.py:api_slack_send:23-67` reads credentials and calls Slack directly. `web/wizard.py:_post_slack_bridge:370-389` implements a second HTTP transport. `services/work_debrief.py:_resolve_recipient:253-264` shells into another project, and `_slack_send:267-274` mutates `sys.path` to import its sender. `scripts/work_hook.py:_bind_db:168-176` and `scripts/codex_notify.py:_bind_db:14-22` assign `core.db._DB_PATH` directly. These belong behind explicit integration/bootstrap APIs.

### Import cycles

I built a static module graph from all Python `Import`/`ImportFrom` nodes, including imports inside functions and `core/tasks/__init__.py` registration imports. The following are **all four reciprocal module pairs** in that graph:

| Cycle | Exact import edges |
| --- | --- |
| `core.log → core.state → core.log` | `core/log.py:9,44`; reverse imports in `core/state.py:95,221,246,267,326`. |
| `core.state → features.releases → core.state` | `core/state.py:322`; `features/releases.py:215`. |
| `core.tasks.tickets → features.tickets → core.tasks.tickets` | Task module imports the feature at `core/tasks/tickets.py:1270,1282,1300,1338,1571,1827,2481,2509,2540,2625,2773`; reverse edge at `features/tickets.py:2064`. |
| `features.tickets → features.ticket_states → features.tickets` | `features/tickets.py:1111,1209`; `features/ticket_states.py:29`. |

Longer concrete cycles include:

- `core.runtime → core.tasks → core.tasks.tickets → features.platforms → core.log → core.state → features.releases → core.runtime`. Edges: `core/runtime.py:20`, `core/tasks/__init__.py:4`, `core/tasks/tickets.py:32`, `features/platforms.py:10`, `core/log.py:9`, `core/state.py:322`, `features/releases.py:294`.
- `core.runtime → core.tasks → core.tasks.polls → manager.watchdog → services.work_launch → core.runtime`. Edges: `core/runtime.py:20`, `core/tasks/__init__.py:6`, `core/tasks/polls.py:72`, `manager/watchdog.py:50`, `services/work_launch.py:14`.
- `core.scheduler → features.billing → features.timesheet → features.platforms → core.log → core.state → features.releases → core.runtime → core.scheduler`. Edges: `core/scheduler.py:216`, `features/billing.py:12`, `features/timesheet.py:20`, `features/platforms.py:10`, `core/log.py:9`, `core/state.py:322`, `features/releases.py:294`, `core/runtime.py:19`.

These are dependency cycles, not proof of an import-time exception: deferred imports avoid executing many edges until functions run. Including deferred imports produces one 64-module strongly connected component; enumerating every simple cycle through it would obscure the actionable boundaries. Its membership is:

```text
core.beat, core.branch_sync, core.claude_runner, core.commit_message,
core.consensus_plan, core.consensus_scope, core.deps, core.event_bus,
core.external_log, core.git_util, core.llm, core.log, core.preflight,
core.runtime, core.scheduler, core.state, core.tasks, core.tasks.autonomy,
core.tasks.billing, core.tasks.manager, core.tasks.pm, core.tasks.polls,
core.tasks.prd, core.tasks.preconditions, core.tasks.registry,
core.tasks.releases, core.tasks.routes, core.tasks.slack, core.tasks.tickets,
core.terminal, core.worker, features.acceptance, features.billcom,
features.billing, features.billing_snapshot, features.defence,
features.own_prs, features.peer_reviews, features.platforms,
features.pr_autofix, features.pr_ci, features.presentation,
features.releases, features.reviewer, features.slack_conversations,
features.slack_monitor, features.ticket_states, features.ticket_systems,
features.tickets, features.timesheet, features.validation, manager.planner,
manager.runner, manager.staleness, manager.watchdog, pm.release_runner,
pm.runner, prd.generator, prd.orchestrator, services.ticket_doctor,
services.work_artifacts, services.work_launch, services.work_tags,
services.work_worktree
```

## 2. DUPLICATION

Each item below identifies a specific duplicated concept, its implementation sites, and one proposed owner. Similar domain-specific policies should remain parameters or separate decisions; sharing an implementation does not imply making every failure or worktree behave alike.

### D1. Git process transport and exit-status interpretation

`core/git_util.py:run_git_status:41-58` and `run_git:507-525` already provide the two useful contracts: inspect an expected status versus raise on an unexpected status. `features/platforms.py:_run_git:35-38` implements another raw runner. Direct Git invocations throughout features and web code reproduce executable selection, capture, timeout, and error handling; some inspect only stdout.

For reproducibility, these are the complete literal-`git` `subprocess.run` call spans outside `core/git_util.py` in the audited Python scope (AST scan of the first command-list element; this deliberately excludes commands assembled in variables and Git instructions inside prompts):

```text
core/branch_sync.py:17-18,56-57
core/consensus_plan.py:76-77,175-177,180-182
core/tasks/tickets.py:134-137,491-494,597,604,2163-2164,2165-2168,2176-2179
features/defence.py:118-120,124-126,132-134,235-236,299-300,330-332,340-341,361-362
features/own_prs.py:654-655,692-693,781-782
features/platforms.py:36-38,165-169,542
features/presentation.py:169-171,296-298,301-303,318-320
features/reviewer.py:800-801
features/tickets.py:843-844,848-851,853-856,858-861,869-872,
  1512-1513,1524-1526,1528,1530-1531,1696-1697,1708-1711,
  1713-1714,1716-1719,1804-1805,1808-1811,1825-1827,
  2360,2376-2377,2411-2412,2638
web/reviews.py:444-446
web/tickets.py:151-153,254-255,259-262,340-343,353-355,1204,1221
```

**Owner:** `core/git_util.py`. Migrate by operation, keeping allowed nonzero statuses explicit. Process execution for tmux, dependency installation, and models should retain their own protocol-specific adapters rather than all being routed through a giant generic helper.

### D2. Identical local Git methods on two remote providers

`BitbucketPlatform.push_branch:555-569`, `merge_base:571-576`, and `sync_remote_branch:578-585` are duplicated by `GitHubPlatform.push_branch:1096-1110`, `merge_base:1112-1117`, and `sync_remote_branch:1119-1126`, all in `features/platforms.py`. The provider name is irrelevant to these implementations, including the identity check and fetch-failure behavior.

**Owner:** `services/branch_operations.py`, delegating raw commands to `core.git_util`. Keep thin adapter methods initially. Move `_resolve_merge_conflicts:41-113` there as workflow behavior, rather than importing an LLM into a low-level Git utility.

### D3. GitHub CLI execution and account environment

The transport/account path is implemented in `features/platforms.py:GitHubPlatform._gh_env:641-661`, `_run_gh:663-667`, and `core/preflight.py:_run:15-18`, `gh_active_account:21-27`, `gh_logged_in_accounts:30-45`, `gh_switch_to:48-53`, `gh_token_for:56-63`, `gh_repo_push_ok:66-86`. `GitHubPlatform.ensure_pr_worktree:1080-1094` bypasses `_run_gh` for checkout at `1090-1093`, so its clone gets the configured environment while checkout does not.

**Owner:** `core/github_cli.py`, with `cwd`, timeout, and explicit account/environment support; preflight and the GitHub adapter both consume it. Preserve account-switching as a separate explicit operation.

### D4. Existing-branch worktree provisioning

There are two particularly clear families:

- **PR review/fix checkout:** `features/own_prs.py:_ensure_worktree:764-786`, `features/pr_autofix.py:_ensure_worktree:388-406`, `features/reviewer.py:_ensure_review_worktree:787-805`. Each finds the configured repo, derives a branch slug, validates an existing linked checkout's location, fetches/resets it, or calls `add_or_reuse_worktree`. `features/presentation.py:_ensure_review_worktree:309-327` and `web/reviews.py:api_start_discuss:429-457` repeat the local-repo/branch-worktree path plus provider-clone fallback.
- **Ticket checkout:** `features/tickets.py:_ensure_worktree:820-886`, `_setup_ticket:1506-1544`, and `materialize_prd_ticket:1690-1730` repeat fetch, branch existence/creation, worktree add, dependency setup, and preservation logic. There is already a shared low-level implementation at `core/git_util.py:add_or_reuse_worktree:107-144` and `refresh_worktree_onto_base:536-590`.

**Owner:** `services/worktree_provisioning.py`, using `core.git_util`. Express reset-to-remote, preserve-local-changes, location, and clone fallback as explicit policies. Do not replace ticket preservation with the PR review hard-reset behavior. `services/work_worktree.py:ensure:358-402` should consume this provisioning layer while retaining work-item ownership/GC policy.

### D5. Ticket directory and branch identity construction

- The same `root / tickets_dir / slug` lookup is independently implemented by `core/tasks/preconditions.py:_ticket_dir:41-46`, `core/tasks/tickets.py:_ticket_dir:1259-1264`, and `core/tasks/pm.py:_ticket_worktree:57-70`. The last returns a ticket container, despite calling it a worktree.
- The “normalize text and take seven words” branch suffix appears in `features/tickets.py:_make_slug:2828-2832` and `services/work_worktree.py:branch_name:50-58`. Ticket branch classification/prefixing is separate policy at `features/tickets.py:_make_branch:2835-2848`.
- Ticket recognition is implemented in `features/presentation.py:resolve_ticket_goal:92-94`, `features/timesheet.py:_extract_ticket:765-767`, `features/timesheet.py:_build_candidates:123-128`, `features/reviewer.py:_extract_ticket_from_pr:830-848`, and `services/work_worktree.py:_ticket_dir_from_objective:178-204`. These disagree on minimum digit count, digits in project prefixes, case sensitivity, and whether stored ticket-to-PR links take precedence.

**Owners:** path construction in `core/config.py` beside `ticket_worktree_path:113-119` and `task_worktree_path:156-169`; key recognition/slug mechanics in `core/ticket_identity.py`. Keep “resolve a PR to a stored ticket” in a ticket query service, using recognition only as fallback. PRD synthetic keys (`prd/orchestrator.py:_generate_ticket_key:64-67`) need explicit support rather than another Jira-shaped regex.

### D6. Repository test-runner discovery

`core/tasks/tickets.py:_detect_runner:72-123` and `features/defence.py:detect_runner:78-104` independently discover Python/JavaScript test environments. They already disagree: the task path refuses unconfigured system pytest and understands uv/Poetry; the defence path can use `sys.executable` and has different Pipenv precedence. `services/work_launch.py:_gate_tests:900-913` reuses task-private helpers through the import at `16-18`.

**Owner:** `core/repo_tests.py`, with one environment discovery result and separate “suite”/“named test” command construction. Move `_run_repo_tests:145-173` there too. The two commands should not become identical; the environment choice should.

### D7. Ticket row serialization and mutation

`core/state.py:save_ticket:336-373`, `update_ticket:462-511`, and `_save_tickets_dict:514-554` all implement the same ticket-column list, JSON blob serialization, and SQLite UPSERT. The three paths differ materially: the legacy whole-dictionary writer deletes absent tickets and omits the validation, transition audit, and release callback used by the per-ticket APIs; `save_ticket` audits after commit; `update_ticket` audits in its transaction and calls the release hook inside it. `features/tickets.py:_save_ticket_if_unmoved:1055-1099` adds a second layer of stale-snapshot merge protection around the storage API.

**Owner:** `services/ticket_store.py`, with a single connection-aware row encoder/writer and explicit application commands for transitions/resets. Preserve the old imports with forwarding wrappers during extraction. Remove whole-dictionary ticket writes only after caller migration; never treat the compatibility writer as equivalent today.

### D8. Whole-blob read–modify–write without a shared mutation primitive

The following functions load, change, and replace a named `kv` blob. These are the complete functions in the audited scope containing both direct `state.load` and `state.save` calls, plus the billing entry family whose wrappers hide the same operation:

| Blob or family | Implementation ranges |
| --- | --- |
| Today plan / launch bookkeeping | `core/tasks/autonomy.py:70-114`; `web/today.py:424-482`. |
| Billing state and snapshots | `features/billing.py:474-531`; `features/billing_snapshot.py:16-31`; entry read/write wrappers and mutations at `features/billing.py:43-82`. |
| Own PRs / peer PRs | `features/own_prs.py:57-87`; `features/peer_reviews.py:8-34`. |
| Autofix state | `features/pr_autofix.py:65-113,116-120`. |
| Review state | `features/reviewer.py:197-210,1194-1268`; `web/reviews.py:29-69`. |
| Slack cursors/state | `features/slack_conversations.py:995-1095`; `features/slack_monitor.py:30-287`. |
| Timesheet fill / watchdog state | `features/timesheet.py:173-228`; `manager/watchdog.py:669-681`. |

**Owner:** a connection-aware `update(module, mutate)` primitive in `core/state.py`, with aggregate-specific stores for frequently edited records. There is a concrete overlapping-writer pair in `features/pr_autofix.py:check` versus `_update_record`: a scan and a fix job operate on the same full blob. Restrict the transaction to the final mutation; holding a database write lock across the LLM/network work in these functions would make things worse. Intentional replacement snapshots and merge updates must remain distinguishable.

### D9. Repeated stored-JSON decoding and projection

An exact family is `pm/runner.py:section_findings:215-235`, `post_shipping_findings:238-258`, and `latest_findings:261-281`: different WHERE clauses, the same `findings` JSON decode/fallback and five-field result object. The same field is parsed again in `findings_count:284-298` and `latest_pre_approval_review:301-320`. Ticket-row JSON decoding/defaulting is separately implemented in `pm/runner.py:_candidate_summary:16-32`, `_shipped_tickets_for_section:107-128`, `manager/staleness.py:_load_ticket_data:18-22`, `features/releases.py:list_release_tickets:86-110`, `web/tickets.py:api_tickets_list:388-434`, and `core/state.py:_row_to_ticket:256-276`.

**Owner:** `services/pm_review_store.py` for PM row mapping and `services/ticket_store.py` for ticket-row decoding. `core/db.py:load_json:135-144` is already a generic starting point, but its annotation does not enforce a decoded object type: object/list validation and the corruption-reporting policy must be explicit. A broken findings blob should not silently become an apparently clean review merely because another caller used a different decoder.

### D10. Model-output JSON extraction

`core/llm.py:extract_json:807-824` implements fenced/raw-object extraction. `services/work_debrief.py:_parse_debrief:67-88` uses first/last braces; `services/work_tags.py:_parse_tags:106-113` uses first/last brackets. `features/pr_ci.py:triage_and_fix_pr:89-95` calls the shared extractor and then redundantly tries raw `json.loads` again. Domain schema validation differs and should remain with each caller.

**Owner:** `core/model_output.py`, with explicit object/array extraction and a distinguishable parse failure. Do not mix tolerant model-output parsing with persistence corruption handling from D9.

### D11. LLM command/environment assembly and invocation lifecycle

- Claude binary/config-directory/environment resolution appears in `core/llm.py:ClaudeProvider.__init__:283-294`, `_env:296-300`, and `core/terminal.py:claude_cmd:73-86`. Codex has the analogous configuration path in `core/terminal.py:codex_cmd:89-106`.
- Headless Codex review command, output-file creation, transcript naming, execution, exit-status check, and JSON parsing are independently assembled in `features/reviewer.py:_run_codex_persona:395-431` and `features/pr_autofix.py:_codex_review:155-176`. `core/consensus_plan.py:_codex:92-100` is the third headless `codex exec` command builder, with different reasoning options.
- `services/work_tags.py:_run_claude:96-103` launches a shell command directly, bypassing invocation recording and the provider/guard machinery in `core/llm.py:102-238`. `services/work_debrief.py:_run_claude:60-64` already goes through `llm.run_balanced`.

**Owners:** `core/agent_command.py` for resolved executable/environment, and the existing `core.llm` facade for recorded headless execution, with a Codex-specific method instead of repeated argv construction. Interactive session lifecycle remains in the terminal adapter. Preserve each caller's model, tools, permission flags, and timeout explicitly.

### D12. Work-agent operating prompt

The default autonomous-decision/operator-question/outward-communication/completion policy is repeated in `services/work_store.py:CONTINUE_PROMPT:36-54` and `services/work_launch.py:_start:499-536`; launch adds artifact and attribution instructions and a cross-check block. Updating only one makes resumed/autocontinued turns receive different operating guidance.

**Owner:** `services/work_prompts.py`, producing launch and continuation prompts from shared policy fragments plus task-specific context. Keep product prompts near their workflow rather than collecting all unrelated prompts into one global module.

### D13. CI retry accounting around an already-shared fixer

`features/pr_ci.py:triage_and_fix_pr:41-120` already consolidates causality classification and the fix attempt. Its callers still repeat outcome interpretation, counters, deduplication, and cap decisions in `features/own_prs.py:_check_ci:629-706`, `core/tasks/tickets.py:fix_ci_failures:1570-1673`, and `features/tickets.py:_handle_ci_failure:2788-2825`. The own-PR path leaves `fix_failed`/parse failures for another cycle without updating its counter; the ticket task explicitly spends attempts for several such results (`1623-1663`). Both import the cap from `features.tickets`, not from the shared CI concept.

Comment retry/reclaim has the same remaining ownership problem: `features/own_prs.py:_is_stale:321-330`, `_reclaim_stuck_comments:333-374` versus `features/tickets.py:_unsettled_ticket_comments:898-918`, `_abandoned_ticket_comments:921-942`, `_retry_ticket_reports:1044-1052`. Shared storage already exists in `core/comments.py:mark_comment_error:184-199` and `mark_comment_retryable:202-223`, but each feature decides which failure consumes the budget.

**Owners:** CI outcome/counter policy in `features/pr_ci.py`; comment reclaim/budget policy in `services/comment_processing.py`. Keep resource identity and persistence at the caller boundary. This should not become one generic retry decorator: SQLite lock retries (`core/db.py:87-107`), daemon polling, and development fix iterations have different meanings.

### D14. Timestamp parsing, UTC factories, and calendar-day rules

The tolerant ISO-to-datetime helper is duplicated by `core/codex_session.py:_instant:42-52`, `features/tickets.py:_parse_iso:217-223`, `features/ticket_timeline.py:_dt:144-153`, and `manager/watchdog.py:_parse:123-130`. The ticket version can return a naive datetime, while the other three supply UTC for naive input; the timeline also normalizes offsets to UTC. Related age calculations repeat parse/default logic at `core/worker.py:_job_age_seconds:33-42`, `features/own_prs.py:_is_stale:321-330`, and `services/work_worktree.py:_finished_long_enough:538-550`.

Every one-body `datetime.now(timezone.utc).isoformat()` factory in the audited scope is:

```text
core/queue.py:7-8; core/scheduler.py:30-31
features/billing_snapshot.py:39-40; features/peer_reviews.py:43-44
features/pr_autofix.py:61-62; features/releases.py:27-28
features/validation.py:28-29; manager/runner.py:22-23
pm/runner.py:12-13; prd/orchestrator.py:15-16
services/work_store.py:79-80; services/work_worktree.py:34-35
web/today.py:45-46; web/wizard.py:30-31
```

**Owner:** extend `core/tz.py:33-46` with clearly named UTC serialization/parsing helpers, preserving invalid-input policy at callers. The meaningful issue exceeds duplicate one-liners: `manager/planner.py:build_plan:169-207` and `core/tasks/autonomy.py:_today_iso:36-37` choose UTC calendar dates, while timesheet/billing use local calendar semantics (`features/timesheet.py:60-93,173-228`, `features/billing.py:16-17`). Decide what “today” means once. `core/scheduler.py:_advance_recurring:233-255` also repeats the daily-at-local-hour calculation for `daily_19pst` and `daily_<hour>_local`; retain the legacy alias but share the calculation.

### D15. HTTP/MCP lookup and error boilerplate

Review comment commands repeat `find_review → 404 → index check → mutate JSON → write → event` in `web/reviews.py:api_submit_comment:606-637`, `api_new_comment:641-664`, `api_delete_comment:668-679`, and `api_update_comment:683-697`. This is duplicated artifact transaction logic, not merely several decorators. Instance extraction and missing-instance responses repeat in `web/manager.py:21-29`, `web/prd.py:20-38`, and `web/today.py:424-427`.

MCP repeats `discover/resolve → not-found response → targets → fan_out → json.dumps` in `mcp_server.py:get_tickets:52-58`, `get_events:71-78`, `get_reviews:82-88`, `get_scheduled:92-98`, `trigger_cycle:120-126`, `get_raw_tickets:140-146`, and `get_raw_prs:150-156`; `_resolve:25-30` only shares the first step.

**Owners:** review command/artifact operations in `services/review_store.py`; active-instance dependency and error-to-HTTP mapping in `web/dependencies.py`; the small fan-out wrapper stays in `mcp_server.py`. Do not generate every route from a table just to remove one-line handlers.

### D16. Error-as-empty-result contracts

Concrete repeated best-effort/default paths are `features/presentation.py:resolve_ticket_goal:92-113` (ticket fetch failure → fallback, PR info failure → empty string), `manager/planner.py:_recent_ticket_keys:98-124` (state read failure → empty candidates), `core/event_bus.py:Dispatcher._drain:62-97` (handler failure → no jobs, then dispatched), and `prd/generator.py:generate:48-74` (model failure or invalid output → empty generated ticket list). In the last case, `prd/orchestrator.py:scan:139-231` still reaches the file-hash update at `223-227`, so “no work requested” and “generation failed” are collapsed before retry decisions.

There is also an exact hook-boundary catch pattern: `scripts/work_hook.py:264-268` and `scripts/codex_notify.py:60-64` print a traceback only under `WORK_HOOK_DEBUG`, then return success.

**Owners:** typed failure results at the producing boundaries—`prd/generator.py` for generation, `core/event_bus.py` for routing outcome, the shared model facade for invocation, and `services/work_hook_runtime.py` for hook reporting. Preserve optional enrichment fallbacks; do not apply a blanket “raise everywhere” rewrite. The consolidation opportunity is explicit failure vocabulary consumed by retries, rather than more helpers that return `{}`.

### D17. UTF-8 tail trimming

`core/job_logs.py:trim_to_utf8_boundary:43-71` and `web/observability.py:_trim_to_utf8_boundary:257-289` implement the same trailing-codepoint algorithm. The live endpoint calls only the web copy at `web/observability.py:343`.

**Owner:** the existing `core/job_logs.py` helper. This is a small, clear consolidation; retain a forwarding alias if needed for existing importers.

### D18. Hook database bootstrap

`scripts/work_hook.py:_bind_db:168-176` and `scripts/codex_notify.py:_bind_db:14-22` are identical: import storage, assign `_DB_PATH`, probe `work_runs`, initialize migrations on exception, return `work_store`.

**Owner:** `services/work_hook_runtime.py`, backed by a public `core.db` attach/initialize API. Keep lightweight out-of-process startup and existing migration behavior; importing the server is unnecessary.

### D19. Work launch bookkeeping and session reconciliation

`web/today.py:api_today_launch:424-482`, `_ensure_work_item:408-420`, and helpers at `299-405` assemble session identity/context, inspect a running terminal, launch it, and persist launch/work records. `services/work_launch.py:_start:459-550` and `resume_session:587-687` independently own those operations for work-board sessions. Today keeps a separate KV launch record in addition to its work item.

**Owner:** `services/work_sessions.py`, consumed by both Today and work-board application commands. Preserve deterministic Today identities and its existing scope metadata; a thin wrapper can translate them. The companion shared prompt extraction is D12.

### D20. Slack sending as three integration paths

This is capability duplication rather than text copying: direct cookie/token Slack HTTP at `web/slack.py:23-67`; authenticated bridge HTTP at `web/wizard.py:370-425`; external-project recipient resolution/imported sender at `services/work_debrief.py:253-295`.

**Owner:** `services/slack_delivery.py` exposing draft/recipient resolution/delivery with explicit transport selection. Retain the debrief path's recipient restrictions and the wizard's dry-run behavior. Routes should not each discover credentials or import another project's implementation.

## 3. GOD MODULES

These are all eleven files above 800 lines in the requested scope, plus the nearby 786-line own-PR module. Size alone is not the finding: the responsibility boundaries and proposed moves are listed for each. Keep forwarding imports during the first move so callers can migrate incrementally.

| File / size | Responsibilities visible in the implementation | Proposed split and moves |
| --- | --- | --- |
| `features/tickets.py` — **2,848** | Gate/dependency/ranking policy (`110-413`); ingestion, reopening, attachments and ticket comments (`416-1099`); dispatch/reconciliation (`1102-1495`); worktree/docs setup (`1498-1770`); PR creation (`1773-1900`); PR comment discussion/repair (`1903-2528`); conflict/base sync/merge/CI (`2531-2825`); branch naming (`2828-2848`). | `features/ticket_intake.py` for discovery/materialization; `features/ticket_pipeline.py` for advancement plus existing status handlers; `services/ticket_gates.py` for dependency/repo gates; `services/ticket_prs.py` for submission/merge; `services/ticket_comments.py` for comment lifecycle; shared worktree/identity modules from D4-D5. Leave `check` as an orchestrator over those components. |
| `core/tasks/tickets.py` — **2,817** | Test-runner execution/reporting (`57-214`); proof instructions/scope (`217-364`); session claims (`392-425`); repo snapshots/commits (`434-607`); extensive hook repair/integrity enforcement (`626-1249`); every task stage, prompts, tests, proof, PR bodies, research, validation (`1268-2817`). | `core/repo_tests.py` for `72-173`; `services/commit_repair.py` for commit/repair machinery `434-1249`; `services/ticket_sessions.py` for claims; `features/ticket_prompts.py` for prompt builders; `core/tasks/ticket_planning.py`, `ticket_review.py`, `ticket_testing.py`, `ticket_delivery.py` for thin registered handlers. Move the repair algorithm intact before changing it: hook restoration and code-change restrictions form one safety boundary. |
| `features/slack_conversations.py` — **1,977** | Capture-format adaptation, file rotation/offset recovery (`257-694`); message/conversation SQL and revisions (`719-1095`); evidence/context transcript construction (`1098-1403`); re-proposal eligibility and limits (`1415-1616`); model judgement (`1619-1640`); atomic proposal claim/revalidation and work creation (`1684-1952`). | `features/slack_capture_reader.py`, `services/slack_conversation_store.py`, `features/slack_evidence.py`, `features/slack_proposals.py`; retain `check:1955-1977` as the facade. Keep revision checks and proposal insertion in one transaction even after moving files. |
| `services/work_store.py` — **1,810** | Work/run/event persistence (`163-465`); operator transitions (`468-533`); tmux process/pane control and side questions (`536-733`); transcript/artifact parsing (`736-1016`); detail projection (`1019-1059`); autonomous continuation and user replies (`1062-1266`); stale/recovery sweeps (`1269-1493`); board/thread/archive projections (`1496-1810`). | Keep row operations in `work_store.py`; `core/tmux.py` for terminal transport; `core/claude_session.py` for Claude transcripts alongside existing `core/codex_session.py`; `services/work_lifecycle.py` for transitions/continuation; `services/work_recovery.py` for sweeps; `services/work_queries.py` for board/thread views. Application lifecycle calls storage and terminal adapters; storage should not decide when to send an agent another prompt. |
| `web/tickets.py` — **1,490** | Diff/LLM/PR projections (`41-275`, `388-704`); PR submission (`279-384`); release commands (`707-792`); artifacts/terminal/discussion (`813-1008`); comments and lifecycle commands (`1012-1346`); validation/PM/jobs/settings/doctor (`1350-1490`). | Routers `web/ticket_queries.py`, `web/ticket_commands.py`, `web/ticket_terminals.py`, `web/releases.py`; application behavior to `services/ticket_commands.py` and `services/ticket_prs.py`; projections to `services/ticket_queries.py`. Splitting the router alone would distribute the same ownership problem over four files. |
| `features/reviewer.py` — **1,268** | Persona prompt definitions and provider execution (`19-451`); merging/deduping reviews (`454-578`); validating/explaining issues (`604-784`); worktree/conventions (`787-818`); ticket-to-PR matching/tracking (`830-994`); ticket-context prompt and sibling aggregation (`997-1090`); separate review entrypoints (`214-267`, `1093-1268`); artifact writing (`270-308`). | `features/review_prompts.py`, `features/review_execution.py`, `features/review_findings.py`, `services/review_tracking.py`; use shared worktree provisioning and `services/review_store.py` for artifact writes. Keep `review_pr`, `review_ticket`, and `review_ticket_prs` as distinct application entrypoints with shared execution. |
| `features/platforms.py` — **1,247** | Factory; local Git/conflict repair/identity checks (`22-187`); CI progression (`190-294`); Bitbucket HTTP (`297-627`); GitHub CLI/GraphQL/auth/repo lookup (`630-1247`). | `features/platforms/__init__.py` for the factory, `github.py`, `bitbucket.py`, `contracts.py` for normalized interfaces; local branch operations to `services/branch_operations.py`; CI policy to `features/pr_ci.py`; CLI credentials/transport to `core/github_cli.py`. Preserve the `features.platforms` import surface. |
| `features/ticket_timeline.py` — **1,173** | SQL/log/docs loading (`204-271`); duration/span algorithms (`144-201`); presentation labels/icons (`32-140`); job/event/comment/transition node interpretation (`309-902`); artifact attachment and phase folding (`905-993`); passes, gaps, segments and KPIs (`996-1129`); assembly (`1132-1173`). | `services/ticket_history.py` for source loading, `features/timeline_intervals.py` for time math, `features/timeline_nodes.py` for interpretation, `web/presenters/ticket_timeline.py` for labels/render schema. `build` should accept collected evidence and assemble the view; it should not need to know every storage detail. |
| `services/work_launch.py` — **1,156** | Instance/project/env resolution (`22-147`); guidance/context/prompt assembly (`150-317`); launch/proposal/resume/follow-up (`323-737`); shell/heredoc/Git command parsing (`746-865`); Git outgoing-files/test/attribution checks and commit/push gates (`868-1115`); terminal kickoff (`1122-1156`). | `services/work_context.py`, `services/work_prompts.py`, `services/work_sessions.py`, `core/shell_commands.py`, `services/work_git_gates.py`. Work Git gates consume shared repo-test and Git adapters; the launch facade should not parse shell syntax. |
| `features/timesheet.py` — **1,023** | Global file cache (`24-57`); scheduled/autofill/allocation orchestration (`60-279`); assembled daily view (`282-426`); Jira worklogs (`429-555`, `867-936`, `996-1013`); Git/review/Claude activity collectors (`558-762`); ticket parsing and LLM summaries (`765-864`); config/calendar parsing (`939-1023`). | `services/timesheet_cache.py` with instance-scoped ownership, `features/timesheet_sources.py` for collectors, `services/jira_worklogs.py` for reads/writes, `features/timesheet_summary.py` for prompt/cache coordination. Keep scheduling/build/autofill orchestration in `timesheet.py` and the already-isolated allocator in `timesheet_select.py`. `features/billing.py:12` should import worklog data through the Jira service rather than a private timesheet helper. |
| `core/llm.py` — **824** | Global provider/concurrency selection and usage guard (`20-162`); invocation SQL and usage parsing (`165-238`, `651-691`); Claude streaming/subprocess behavior (`282-564`); OpenCode subprocess behavior (`567-648`); facade (`694-728`); external CLI invocation (`750-804`); output extraction (`807-824`). | `core/llm_providers/claude.py`, `opencode.py`, `external.py`; `services/llm_invocations.py` for persistence; `core/llm_guard.py`; `core/model_output.py`; keep `core.llm` as the facade. Resolve instance/provider context before crossing a thread boundary. |
| `features/own_prs.py` — **786**, near threshold | PR discovery/metadata (`57-122`); thread resolution and comment intake (`125-318`); retry/debounce (`321-413`); repair/commit/push (`416-626`); CI (`629-706`); base freshness (`709-752`); stale alerts and checkout (`755-786`). | Keep polling/ownership decisions in `own_prs.py`; extract `services/pr_comment_processing.py`; reuse D4 worktrees, D13 CI policy, and the existing `core.branch_sync`. A separate base-sync implementation is unnecessary because it is already shared. |

## 4. NAMING AND CONCEPT DRIFT

| Concrete pair | Semantic mismatch and consolidation target |
| --- | --- |
| Config “job” versus queued “job” | `core/config.py:70-71` uses `job.key` to identify an instance; `core/queue.py:20-28` creates execution jobs with numeric IDs; `core/tasks/registry.py:10-19` carries both `instance_key` and `job_id`. `core/log.py:init:16-20` still accepts a `job_key`. Make an instance/project identity distinct from an execution job in public APIs; preserve the TOML spelling as a compatibility alias. |
| “Task” as registered operation versus “task” as work item | `core/tasks/registry.py:task:38-54` registers a task type; `services/work_store.py:create_item:163-176` creates the thing displayed by `/tasks` (`web/work.py:23-25`), while its API is `/api/work/...`. Use explicit `TaskDefinition`, `Job`, `WorkItem`, and `WorkRun` concepts when crossing these boundaries. |
| Three event systems plus log events | `core/events.py:dispatch:10-16` means synchronous configured actions; `core/queue.py:emit_event:11-17` means durable routed events; `core/log.py:emit:51-74` writes operator-visible logs; `services/work_store.py:record_event:333-404` advances work sessions from hook events. These are not interchangeable event buses. Name the public operations after their purpose and specify which ones cause state transitions. |
| `run_claude_code`, `run_haiku`, `run_sonnet` versus configurable provider/tier | `core/claude_runner.py:1-20` aliases those names to `run_thinking`, `run_fast`, and `run_balanced`; `core/llm.py:configure:701-708` can select OpenCode. The table `claude_invocations` also records non-Claude runs via `run_external_model:750-804`. Migrate callers/UI vocabulary to invocation and capability/tier; keep compatibility names while stored data and consumers migrate. |
| “reviewing” versus “in_review” | `core/ticket_status.py:9,14` defines both; `core/tasks/tickets.py:start_reviewing:1432-1446` is internal review, while PR creation transitions to `in_review` (`web/tickets.py:373-378`). Separately `features/pr_autofix.py:280` uses `reviewing` for autofix state. Make state-owner context explicit, especially in event/read-model APIs. |
| “review” spans three unrelated persisted results | PR review artifacts in `features/reviewer.py:_write_review_artifacts:287-308`; requirements/approval findings in `pm/runner.py:run_pre_approval:51-104`; whole-release inspection in `pm/release_runner.py:run_release_inspection:10-66`. Shared model execution makes sense; combining their verdict schemas does not. |
| “worktree” means ticket container, linked checkout, or clone | `core/tasks/pm.py:_ticket_worktree:57-70` returns the container; `core/config.py:ticket_worktree_path:113-119` returns a repo checkout; `features/platforms.py:ensure_pr_worktree:534-553,1080-1094` may create standalone shallow clones; `services/work_worktree.py` records owned linked worktrees. Distinguish container paths, linked worktrees, and fallback clones in the provisioning contract. |
| Internal status, external status, approval, obsolescence | `core/state.py:save_ticket:336-373` stores several independent axes; `web/tickets.py:api_obsolete_ticket:1322-1346` sets `obsolete_at`; `features/ticket_systems.py:PRDTicketSystem.fetch_tickets:23-43` instead checks `status in ('done', 'obsolete')`. `obsolete` is absent from `core/ticket_status.py:4-28`. This is an actual concept mismatch, not a spelling preference. Define one obsolescence predicate used by intake, validation, and display. |
| “Terminal”/“done” depends on the workflow | `core/tasks/autonomy.py:33` treats merged/validation/done as terminal; `core/ticket_status.py:44-48` still permits follow-on/reopen transitions; `services/work_store.py:22-23` treats `needs_ack` and `done` as finished, but `apply_action:500-521` distinguishes acknowledgement and archive behavior. Introduce purpose-specific predicates such as shipped, pipeline-finished, agent-finished, and operator-acknowledged. |
| `daily_19pst` versus configured local timezone | `core/scheduler.py:233-240` implements the old cadence name using `core_tz.local_tz()`, not a fixed PST zone; generic `daily_<hour>_local` repeats it at `242-255`. Treat the old spelling as an alias for a normalized cadence object. |
| Manager versus PM | `manager/runner.py:run_daily_digest:66-120` and `manager/planner.py:build_plan:169-207` prioritize operational work; `pm/runner.py:51-212` evaluates approval/requirements/shipping; `manager/watchdog.py:scan:556-658` creates repair work. They overlap in LLM mechanics, not domain responsibility. Extract invocation/stores without merging the roles into a single “agent manager.” |
| `cron_interval` versus actual cadence | `frshty.py:101` passes `240`; `core/runtime.py:start_events:321-329` logs it as a seconds interval, but `_cron_ticker:109-128` uses only whether it is positive and gets actual timing from `_ticker_sleep_seconds` and per-instance `tick_interval`/quiet-hours settings. Its numeric value is effectively an enable switch, not the interval promised by the API/logging. |

## 5. DEAD OR NEAR DEAD CODE

“No repository caller” is narrower than “no possible external caller.” Decorated FastAPI/MCP/task functions are reachable through registration even if no ordinary Python call names them. I checked those registrations and searched templates/static code before classifying candidates. The commands below were run before this report was included in search results; the report is explicitly excluded in the repeatable versions.

### Definition-only helpers

Command:

```sh
rg -n --hidden -g '!.git' -g '!uv.lock' -g '!CODEX-AUDIT.md' \
  '\b(trigger_resource_recheck|set_event_instance|_log_path|status_in|all_tasks|_hours_since)\b' .
```

Complete matches, reordered by package for readability:

```text
./core/comments.py:323:def trigger_resource_recheck(
./core/queue.py:125:def set_event_instance(event_id: int, instance_key: str) -> None:
./core/external_log.py:74:def _log_path(today: str | None = None) -> Path | None:
./core/tasks/preconditions.py:20:def status_in(*states: str) -> Callable:
./core/tasks/registry.py:61:def all_tasks() -> list[str]:
./features/slack_monitor.py:583:def _hours_since(iso_ts: str) -> float:
```

These functions have no textual reference outside their definition in repository files searched by `rg`, including tests. Their implementation ranges are respectively `323-342`, `125-126`, `74-82`, `20-21`, `61-62`, and `583-588`. None is decorated. `_log_path` is especially clear: `_append:120-134` builds the same path inline and never calls it. Remove the private orphan helpers; retire or document the small public APIs after checking any out-of-repository consumers.

### Duplicated helper left unused

Command:

```sh
rg -n --hidden -g '!.git' -g '!uv.lock' -g '!CODEX-AUDIT.md' \
  '\b(trim_to_utf8_boundary|_trim_to_utf8_boundary)\b' .
```

The production matches are only `core/job_logs.py:43` (definition), `web/observability.py:257` (definition), and `web/observability.py:343` (call to the web copy). The additional matches are two entries in `FIXES.md` and `tests/api/test_live_endpoint.py:3,13`, which imports the web copy. The core implementation is unused in production; D17 wires the existing shared implementation in instead of simply deleting the better-located copy.

### Superseded queue recovery helper

Command:

```sh
rg -n --hidden -g '!.git' -g '!uv.lock' -g '!CODEX-AUDIT.md' '\bsweep_stale\b' .
```

Matches: definition at `core/queue.py:92`; mentions/calls in `tests/core/test_queue_sweep.py:1,32,34,40,46,48,58`; a historical mention in `tests/core/test_worker_orphan_recovery.py:84`. There is **no production caller**. Current startup/recovery is `core/worker.py:WorkerPool.start:55-63`, `_reconcile_orphans:78-89`, `_orphan_poll_loop:91-108`, and `_finalize_orphan:110-173`. `sweep_stale:92-106` is a near-dead prior recovery implementation, not the current recovery mechanism.

### Registered task with no producer and no substantive behavior

Command:

```sh
rg -n --hidden -g '!.git' -g '!uv.lock' -g '!CODEX-AUDIT.md' '\bhandle_slack_message\b' .
```

Only `core/tasks/slack.py:23` (decorator) and `24` (definition) match. Its body at `25` returns the payload timestamp. There is no matching enqueue/route/config producer. Current cron routing uses `slack_scan` and `slack_conversation_scan` (`core/tasks/routes.py:23-26`). This is a registered placeholder, not strictly unreachable: generic manual retry/enqueue tooling could name it. Remove or implement it deliberately instead of presenting it as another supported Slack workflow.

### Unlinked legacy page, not proven unused externally

Command:

```sh
rg -n '\b(tickets2|ticket_detail_v1)\b' web templates static mcp_server.py scripts tests
```

Complete output:

```text
web/pages.py:97:@router.get("/tickets2/{key}", response_class=HTMLResponse)
web/pages.py:98:def ticket_detail_v1(key: str):
web/pages.py:99:    return _template("ticket_detail_v1.html")
```

No first-party navigation or test targets the legacy URL. The template is nevertheless served, so it is not a dead file. Treat `/tickets2/{key}` plus `templates/ticket_detail_v1.html` as a retirement candidate, using existing route-usage data (`web/usage.py:73-91`) before deciding whether to redirect it. Repository search cannot prove that nobody has bookmarked it.

### Orphan supervisor constants and ignored extension parameter

Command:

```sh
rg -n --hidden -g '!.git' -g '!uv.lock' -g '!CODEX-AUDIT.md' \
  '\b(STUCK_THRESHOLD|STALE_CYCLE_THRESHOLD|STATE_FILE)\b' .
```

Complete matches:

```text
./supervisor.py:15:STATE_FILE = STATE_DIR / "supervisor.json"
./supervisor.py:22:STUCK_THRESHOLD = 1800
./supervisor.py:23:STALE_CYCLE_THRESHOLD = 900
```

`_load_state/_save_state:41-60` use SQLite, and `_detect_problems:95-115` never evaluates either threshold. These are unused code-level settings, not active TOML configuration.

For the scheduler:

```sh
rg -n '\b(enqueue_fn|cron_interval|interval)\b' core/runtime.py core/scheduler.py
```

`enqueue_fn` occurs only in `core/scheduler.py:165`, the signature of `fire_due_recurring`. The body directly inserts jobs at `195-198` and never calls the argument. This is a dead extension parameter: remove/deprecate it or honor it with a transaction-aware API. The same grep exposed the `cron_interval` semantic drift described in section 4; that parameter is not wholly unused.

I did **not** establish a wholly dead production module or an unused TOML key with sufficient confidence. Empty `__init__.py` files provide packages; scripts and MCP functions have external entrypoints. `core/runtime.stop_events` has callers in HTTP integration tests, and `core/tz.refresh_from_env` explicitly declares itself a test hook; neither is a strong deletion recommendation. No route should be deleted just because its Python function name is not called directly.

## 6. TOP 10 REFACTORS

Ranked by expected owner value divided by behavior-change risk, not by file size. The scores are judgment calls: value 1–5 measures reduced repeated fixes, debugging effort, and feature friction; risk 1–5 measures how much behavior/contracts must be touched. Size estimates include production code moved or edited, not generated artifacts; they are approximate and overlap where later steps use earlier extractions.

| Rank / value:risk | Change in one sentence | Files touched | Estimated size | Risk and ordering |
| --- | --- | --- | --- | --- |
| **1 — 3:1** | Consolidate the already-duplicated Git adapter methods, UTF-8 helper, and hook bootstrap through existing-compatible facades. | `features/platforms.py:555-585,1096-1126`; new `services/branch_operations.py`; `core/job_logs.py:43-71`; `web/observability.py:257-289,343`; both hook scripts; new `services/work_hook_runtime.py`; public attach seam in `core/db.py`. | **S**, roughly 120–220 lines moved/removed and small forwarding calls. Can be three independent small commits. | **Low.** Preserve return values, commit/push options, hook process behavior, and import compatibility. This is immediately reviewable consolidation. |
| **2 — 5:2** | Make review/fix and ticket callers use one worktree provisioner with explicit preservation/reset policies. | D4 sites in `features/{tickets,own_prs,pr_autofix,reviewer,presentation}.py`, `web/reviews.py`, `services/work_worktree.py`, `core/git_util.py`; new `services/worktree_provisioning.py`; path helpers in `core/config.py`. | **M**, about 250–450 lines replaced/extracted across 9–10 files. | **Low–medium** for extraction, higher if refresh behavior changes. Keep current paths and local-change handling initially; do not migrate or delete existing worktrees as part of it. |
| **3 — 4:2** | Extract repository test discovery/execution and commit-repair machinery from the ticket task entrypoint so both workflows can depend on narrow services. | `core/tasks/tickets.py:72-173,434-1249`; `features/defence.py:78-104`; `services/work_launch.py:16-18,900-913`; `features/tickets.py:2055-2066`; new `core/repo_tests.py`, `services/commit_repair.py`. | **M/L by movement**, about 850–1,000 lines moved, 100–200 lines of interface/caller changes. | **Low–medium** if moved intact. Preserve hook-restoration invariants; resolve Python runner-policy differences as a separate, explicit follow-up. |
| **4 — 5:3** | Make instance scope explicit at thread/process boundaries and scope caches by instance before sharing more execution code. | `web/state.py:59-104`, `core/worker.py:248-318`, `core/llm.py:68-81,694-708`, `core/consensus_plan.py:83-130`, `features/reviewer.py:434-451`, `features/pr_autofix.py:297-312`, `features/timesheet.py:24-52,60-62,282-284`, `features/platforms.py:632-692`; new `core/instance_context.py`. | **M**, approximately 250–450 lines changed; audit all background submission sites. | **Medium.** Correct scope changes which account/cache/log record a call uses. Bind or copy context deliberately, keeping each thread's token lifecycle separate. |
| **5 — 5:3** | Establish one ticket row writer and move release-trigger decisions outside persistence into a committed-event boundary. | `core/state.py:313-373,462-554`, `features/releases.py:281-325`, `core/tasks/routes.py`, `core/queue.py`, `features/tickets.py:1055-1099`; new `services/ticket_store.py`; possibly one outbox migration. | **M/L**, roughly 350–600 lines plus a small migration if using an outbox. | **Medium.** Transaction visibility, concurrent updates, legacy deletion semantics, and release deduplication matter. Extract encoding first; change callback timing as a separate step. |
| **6 — 3:2** | Give review artifacts one read/modify/write owner and share the repeated PM row decoders and model-output parsers. | `services/review_store.py`; `web/reviews.py:606-697`; `features/reviewer.py:270-308`; `pm/runner.py:215-320`; new `services/pm_review_store.py`, `core/model_output.py`; `services/work_{debrief,tags}.py`; `features/pr_ci.py:89-95`. | **M**, about 200–350 lines refactored. | **Low–medium.** Preserve filename/provider suffixes and HTTP results; distinguish corrupt data from empty valid results. Serialize artifact edits rather than merely relocating `write_text`. |
| **7 — 4:3** | Put headless/interactive agent environment resolution and headless Codex/GitHub CLI execution behind shared, account-aware adapters. | `core/llm.py`, `core/terminal.py`, `core/preflight.py`, `features/platforms.py`, `features/reviewer.py:395-431`, `features/pr_autofix.py:155-176`, `core/consensus_plan.py:92-100`, `services/work_tags.py:96-103`; new `core/agent_command.py`, `core/github_cli.py`. | **M**, roughly 200–400 lines. | **Medium.** Environment precedence, model options, permissions, output files and timeout behavior are part of the contract. Keep shared command resolution separate from interactive transport. |
| **8 — 4:3** | Centralize CI and comment retry outcomes so a provider outage, a failed repair, and a clean result consume the intended budgets everywhere. | `features/pr_ci.py`, `features/own_prs.py:321-374,629-706`, `features/tickets.py:898-942,1044-1052,2788-2825`, `core/tasks/tickets.py:1570-1673`, `core/comments.py:184-223`; new `services/comment_processing.py`. | **M**, about 200–350 lines of shared policy and caller adaptation. | **Medium.** This intentionally resolves existing behavior differences; document which outcomes spend attempts before switching callers. SQLite lock retry stays independent. |
| **9 — 4:3** | Split work storage from terminal/transcript/recovery behavior, then route Today launches through the shared session service. | `services/work_store.py:536-1493`, `services/work_launch.py:323-737,1122-1156`, `services/work_debrief.py:201-241`, `web/today.py:299-482`, `core/terminal.py`; new `core/tmux.py`, `core/claude_session.py`, `services/work_{sessions,lifecycle,recovery,queries,prompts}.py`. | **L**, roughly 1,200–1,800 lines moved, 250–400 lines changed; several sequential extractions. | **Medium.** Preserve session IDs, side-question behavior, completion/ack rules, launch/GC locking, and resume environment. Move transport and transcripts first. |
| **10 — 5:4** | Unify ticket commands across HTTP, task handlers, and scheduled execution so manual PR/restart/approve actions share the same application owner. | `web/tickets.py:310-384,1049-1346`, `features/tickets.py:1784-1900,2764-2785`, `features/ticket_states.py`, `core/tasks/tickets.py:2480-2627`, `core/scheduler.py:265-292`; new `services/ticket_commands.py`, `services/ticket_prs.py`; split web routers as in section 3. | **L**, about 600–1,000 lines moved/adapted. | **Medium–high.** Manual submission accepts edited PR bodies and currently has different commit/gate behavior. First expose those differences as explicit inputs; unify policy only deliberately. Build on ranks 2, 3, and 5. |

The remaining god-module splits can follow these seams. Moving the 1,977-line Slack conversation module or 1,173-line timeline module wholesale first would make navigation easier, but it would remove fewer alternate behavior paths than the ranked work above.
