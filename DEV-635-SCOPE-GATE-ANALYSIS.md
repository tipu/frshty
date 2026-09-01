# DEV-635 / PR #19: did the scope-review step run?

Question: did frshty run the final ticket step that checks for unnecessary code and
branch state against main, for
https://bitbucket.org/aimyable/windows-rpa-client-schema/pull-requests/19 ?

Answer: no. The step did not run before the PR was opened. The step also cannot
report the defect that was found. Both facts are bugs. They are separate bugs.

The step in question is `scope_review` (`core/tasks/tickets.py:2123`), the consensus
scope review. It asks two questions: scope fidelity and git integrity
(`core/consensus_scope.py:33`).

## What was PR'd

PR #19 belongs to ticket DEV-635 (`aimyable`, slug `DEV-635-file-explorer-tool`).
Commit `93d4c17` added `FileExplorerOperation`, an enum with no caller, and
`FileExplorerAction.recursive`, a field with no reader. The operator removed both by
hand on 2026-08-26 in commit `3967da2` ("remove unused FileExplorerOperation enum and
recursive field"). The dead code stood in the PR for nine days.

## Timeline, all times UTC

| Time | Event | Source |
|---|---|---|
| 2026-08-18T00:40 | `93d4c17` adds the file-explorer models, including the dead enum and dead field | git log, schema worktree |
| 2026-08-18T00:58 - 01:07 | `start_reviewing` runs `/tri-review` | jobs 319552 |
| 2026-08-18T01:07 - 01:25 | `fix_review_findings` fails: `git commit` returned exit status 1 | job 319578 |
| 2026-08-18T01:31 | ticket moves to `blocked` | ticket_transitions |
| 2026-08-19T20:36 | operator override back to `reviewing`, reason "manual override: review passed but fix_review_findings died on the commit" | ticket_transitions, actor `danial` |
| 2026-08-19T21:59 | ticket reaches `pr_ready` | ticket_transitions |
| 2026-08-20T08:20:36 | `ticket_pr_created`, 5 repos. Transition reason `manual create-pr`. PR #19 opens | log_events, ticket_transitions |
| 2026-08-20T09:00:53 | commit `e895a14` adds the consensus scope review to frshty | git log, frshty repo |
| 2026-08-20T09:01:49 | first `scope_review` job for DEV-635 | job 324746 |
| 2026-08-20 - 2026-08-26 | 19 scope reviews, 0 passes | log_events |
| 2026-08-26T17:38 | operator removes the dead code by hand, `3967da2` | git log |

The gate landed 40 minutes after the PR was opened. It did not exist at PR time.
That part is history and cannot recur. The next three findings are live.

## Bug 1: the manual PR path never consults the gate

`web/tickets.py:310` `_submit_pr_sync` is the code path that opened PR #19. The
transition reason `manual create-pr` is written at `web/tickets.py:375` and appears in
`ticket_transitions` for DEV-635. That function checks two things before it pushes:
`status == "pr_ready"` (`web/tickets.py:323`) and `_is_meaningful_change`
(`web/tickets.py:350`). It never calls `_scope_review_state`.

The gate has three enforcement sites and none of them cover this path:

- `core/scheduler.py:273` guards the scheduled PR. Reached only through `_execute_create_pr`.
- `features/ticket_states.py:388` guards the dispatcher PR. The `_create_pr` call below it is behind `config["pr"]["auto_pr"]`.
- `features/ticket_states.py:457` guards auto-merge. Behind `config["pr"]["auto_merge"]`.

`config/aimyable.toml` sets `auto_pr = false` and `auto_merge = false`. For the
aimyable instance the scope-review gate therefore blocks nothing at all. It is
advisory output only.

The UI has no scope-review state either. `grep scope_review templates/*.html` returns
nothing. The Submit PR button at `templates/tickets.html:161` renders on
`t.status === 'pr_ready'` alone. `templates/today.html:607` and
`templates/ticket_detail.html:1487` post to the same endpoint, so one server-side check
covers all three.

Evidence that this is live: DEV-635 is `in_review` right now with
`scope_review.verdict == "fail"` and five open PRs.

## Bug 2: the scope review cannot report dead code

I ran the check against a case I knew was bad. I checked out the schema repo at
`e374679`, the last commit before the dead-code removal, confirmed the enum and the
field had no caller and no reader anywhere in the ticket workspace, composed the
`SCOPE_DIRECTIVE` prompt byte-for-byte from `core/consensus_scope.py`, and ran it
through `codex exec`.

Result:

```
## 1. Scope fidelity

All changes serve DEV-635:

- The operation enum and import define file-explorer operations
  (src/rpa_schema/models.py:1, src/rpa_schema/models.py:821).
...
No out-of-scope changes were found.

SCOPE VERDICT: PASS
```

The reviewer read the dead enum and called it in scope. The check reports success on a
case that is bad, so its PASS carries no information about dead code.

Two clauses in the prompt cause this. `SCOPE_DIRECTIVE` asks "Does every change in the
diff serve the ticket's purpose?", and a file-explorer enum in a file-explorer ticket
serves the ticket's purpose by topic. The prompt then says "Code quality, style, and
correctness are reviewed elsewhere; they must not affect your verdict"
(`core/consensus_scope.py:52`), which directs every voice to set aside exactly this
class of finding.

This matches the 19 recorded runs. Every one of them failed the ticket overall, and
every one of them passed `windows-rpa-client-schema`.

## Bug 3: tri-review has no repo coverage requirement

`/tri-review` is the step that does look for dead code. Its maintainability persona
lists "dead code" as a target (`features/reviewer.py:69`, and the same persona in
`~/.claude/commands/tri-review.md`).

It never examined the schema repo. `docs/tri-review.md` for DEV-635 names
`django-drf-app`, `saas-dashboard`, `websocket-server`, and `windows-rpa-client`. It
never names `windows-rpa-client-schema`, although that repo had a branch diff at review
time.

The cause is that `start_reviewing` (`core/tasks/tickets.py:1328`) passes a bare prompt,
"Run /tri-review and save the full output to docs/tri-review.md", and the command itself
says only "Identify the diff to review. Use `git diff` ... `git diff main...HEAD`". In a
multi-repo ticket directory that command finds nothing at the root. Which worktrees get
opened is left to the model. It opened four of five.

`scope_review` does not have this problem: `core/consensus_scope.py:125` enumerates every
repo with a branch diff and names each worktree in the prompt.

## Plans

### 1. Gate the manual PR path

In `_submit_pr_sync`, after the `pr_ready` check at `web/tickets.py:323` and before the
push loop, call `_scope_review_state(_config, ticket)`. On `pending` or `fail`, return
409 with the verdict, the reason, and a link to `docs/scope-review.md`. Accept
`data["force"] = true` to override, and record the override in the transition reason so
`ticket_transitions` shows who shipped over a failing verdict.

Expose `scope_review` in the ticket list and detail payloads. Render the verdict next to
the Submit PR button in all three templates, and require a confirm step when the verdict
is `fail`. The server check is the gate; the UI is the explanation.

Regression test: a ticket in `pr_ready` with `scope_review.verdict == "fail"` must get
409 from `POST /api/tickets/{key}/submit-pr`, and 200 with `force: true`.

### 2. Make the scope check able to fail on dead code

Add a third question to `SCOPE_DIRECTIVE`:

> 3. Reachability. For every symbol, field, enum member, and branch the diff adds, name a
> caller or a reader on this branch, in any of the repositories listed above. An addition
> with no caller and no reader is a finding. Cite the grep you ran.

Narrow the exclusion clause at `core/consensus_scope.py:52` so it exempts style and
correctness but not reachability.

The fan-out already passes every ticket worktree as an include dir
(`core/consensus_scope.py:139`), so a voice can grep across repos for callers. That is
what makes the question answerable for a schema-only repo whose consumers live elsewhere.

Verification, and this is the part that matters: re-run the falsification test above
against `e374679` and confirm the verdict flips to FAIL and names
`FileExplorerOperation`. Do not accept the change on a PASS-only test.

### 3. Give tri-review the repo list scope_review already builds

Extract the repo enumeration in `core/consensus_scope.py:125-140` into a shared helper
that returns `[(repo_name, worktree, base_branch)]` for every repo with a branch diff.
Have `start_reviewing` interpolate that list into its prompt the same way
`SCOPE_DIRECTIVE` does, and state that every listed repository must appear in
`docs/tri-review.md`.

Add a postcondition to the `start_reviewing` task that `docs/tri-review.md` names every
repo in the list. `core/tasks/tickets.py` already has postcondition machinery; the
`scope_review` task uses `file_contains` at line 2126. A repo that is silently skipped
then fails the task instead of passing it.

### 4. Do not let a manual override carry a stale verdict forward

DEV-635 went `blocked` because `fix_review_findings` died on a commit, and the operator
moved it back to `reviewing` by hand. `docs/tri-review.md` still read `VERDICT: PASS`
above five unresolved blocking findings, so the ticket walked to `testing` a minute later
on a review that was never completed. The commit-ordering half of this was already fixed
in `fix_review_findings` (see the comment at `core/tasks/tickets.py:1373`), but the
override path was not.

When a manual transition moves a ticket backward into `reviewing`, clear the verdict line
in `docs/tri-review.md` and drop `ts["scope_review"]`. Both then re-run. The rule is that
a verdict is only valid for the state it was computed against, which is the rule
`scope_review` already follows through its fingerprint.

## What I checked

- `~/.frshty/frshty.db`: `tickets`, `jobs`, `ticket_transitions`, `log_events` for `instance_key='aimyable'`, `ticket_key='DEV-635'`.
- `git log` and `git diff` in the DEV-635 schema worktree and in the frshty repo.
- `core/consensus_scope.py`, `core/tasks/tickets.py`, `core/scheduler.py`, `features/tickets.py`, `features/ticket_states.py`, `features/reviewer.py`, `web/tickets.py`, `templates/*.html`.
- One live `codex exec` run of the real `SCOPE_DIRECTIVE` against the known-bad commit `e374679`.
