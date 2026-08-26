# Scope-gate follow-ups

Source: `DEV-635-SCOPE-GATE-ANALYSIS.md` (work item 9059, board artifact 522).

That analysis found three bugs and proposed four plans. This document turns them into
six follow-up work items. Three of the six are new findings that the analysis did not
report. Every number below is re-derivable with `scripts/scope_gate_evidence.sh`.

## Summary

| ID | Follow-up | Source | Severity |
|---|---|---|---|
| FU-1 | Gate every operator ship path on the scope verdict | Bug 1, widened | high |
| FU-2 | Make the scope prompt able to fail on dead code | Bug 2 | high |
| FU-3 | Repair the third voice and require a quorum | new | high |
| FU-4 | Stop the fingerprint rerun loop | new | medium |
| FU-5 | Give tri-review the repo list scope_review already builds | Bug 3 | medium |
| FU-6 | Clear a stale verdict on a backward manual transition | Plan 4 | medium |

Launch FU-1, FU-2 and FU-3 first. FU-1 closes the hole that let PR #19 open. FU-2 makes
the gate able to see the defect that PR #19 carried. FU-3 makes the verdict a consensus
again. FU-4, FU-5 and FU-6 reduce waste and close narrower holes.

## Evidence

Run `scripts/scope_gate_evidence.sh`. It re-derives each number from
`~/.frshty/frshty.db` and from the DEV-635 worktrees. Section 4 carries a known-good
control, `core/scheduler.py:_execute_create_pr`, so a broken check reports a broken
check instead of a clean result. Output on 2026-08-26:

```
== 1. consensus quorum: how many scope verdicts ran on fewer than three voices
   fanouts: 45
   votes per fanout: {2: 32, 3: 13}
   dropped voices: 32
      25  claude: no SCOPE VERDICT line
       7  claude: exit_code=1

== 2. gate outcomes recorded on tickets
   ticket_scope_review_failed       35
   ticket_scope_review_passed       10
   ticket_scope_review_started      49

== 3. scope_review runs for DEV-635
   runs: 19  statuses: ['ok']
   first: 2026-08-20T09:01:49.257281+00:00
   last:  2026-08-26T17:41:44.472874+00:00

== 4. ship paths that do not consult the scope verdict
   core/scheduler.py:_execute_create_pr: consults the gate
   web/tickets.py:_submit_pr_sync: DOES NOT consult the gate
   web/tickets.py:api_merge_ticket: DOES NOT consult the gate

== 5. a base-branch move flips the scope fingerprint
   digest at current base: 20c6b7c10e1d0fd5
   digest at older base:   6f60423599e4c306
   DIFFERENT: a base move flips the fingerprint, so the recorded verdict goes stale
```

## FU-1: gate every operator ship path on the scope verdict

The analysis named one unguarded path. There are two. The merge path is the second, and
it is the more damaging one, because it ends the ticket.

`web/tickets.py:310` `_submit_pr_sync` opens the PR. It checks `status == "pr_ready"`
and `_is_meaningful_change`. It never calls `_scope_review_state`.

`web/tickets.py:1032` `api_merge_ticket` merges every PR on the ticket. It checks
`status == "in_review"` and `ts["prs"]`. It then calls `_tickets_mod._merge` directly.
`_merge` (`features/tickets.py`) contains no scope check either. The dispatcher merge
path at `features/ticket_states.py:458` does check the verdict. The operator merge path
does not. `templates/today.html:125` renders the button that posts to it.

There is a third hole in the same class. `_scope_review_state`
(`features/tickets.py:193`) returns `"disabled"` when `scope_fingerprint` returns an
empty string. `scope_fingerprint` returns an empty string when no worktree yields a
branch diff, and `_branch_diff` returns `None` on any git error: a missing ref, a
`GitCommandError`, a timeout, an `OSError`. All three gates admit `"disabled"`. A git
failure therefore opens the gate instead of holding it. The gate fails open.

Four templates post to `submit-pr`: `templates/tickets.html:477`,
`templates/today.html:607`, `templates/ticket_detail.html:1487`, and
`templates/ticket_detail_v1.html:1015`. `web/pages.py:84` and `web/pages.py:98` route
both detail templates, so both are live. One server-side check covers all four.

Change: add a shared helper that both endpoints call before they act. Return 409 with
the verdict, the reason and the path to `docs/scope-review.md` when the state is
`pending` or `fail`. Split the fail-open case out of `"disabled"`: return a distinct
`"unknown"` state when the fingerprint cannot be derived, and treat `"unknown"` as
blocking at all five call sites. Accept `data["force"] = true` on both endpoints, and
write the override into the transition reason so `ticket_transitions` records who
shipped over a failing verdict. Expose `scope_review` in the ticket list and detail
payloads, and render the verdict beside the Submit PR and Merge buttons.

Verification that can fail: DEV-635 is `in_review` today with
`scope_review.verdict == "fail"` and five open PRs. Post to
`/api/tickets/DEV-635/merge` against a test database seeded from that state and confirm
409. Post again with `force: true` and confirm 200. Add the same pair for
`submit-pr`. Add a third test that stubs `_branch_diff` to return `None` and confirms
both endpoints return 409 rather than proceeding.

Launch draft:

> Close the scope-gate bypass on every operator ship path in frshty. `web/tickets.py:310`
> `_submit_pr_sync` and `web/tickets.py:1032` `api_merge_ticket` both act without calling
> `features/tickets.py:193` `_scope_review_state`, so an operator can open a PR and merge a
> ticket over a failing scope verdict. Add one shared check that both endpoints call, and
> return 409 with the verdict, the reason and the `docs/scope-review.md` path when the state
> is pending or fail. Also fix the fail-open: `_scope_review_state` returns "disabled" when
> `scope_fingerprint` yields an empty string, which happens on any git error inside
> `_branch_diff`, and "disabled" is admitted by all three existing gates
> (`core/scheduler.py:273`, `features/ticket_states.py:388`, `features/ticket_states.py:458`).
> Return a separate "unknown" state for that case and treat it as blocking everywhere.
> Accept `force: true` on both endpoints and record the override in the transition reason.
> Expose `scope_review` in the ticket list and detail payloads and render the verdict next to
> the Submit PR and Merge buttons in `templates/tickets.html`, `templates/today.html`,
> `templates/ticket_detail.html` and `templates/ticket_detail_v1.html`. Add regression tests
> that a fail verdict returns 409 from both endpoints, that `force: true` returns 200, and
> that an underivable fingerprint returns 409.

## FU-2: make the scope prompt able to fail on dead code

The prior job ran the real `SCOPE_DIRECTIVE` against `e374679`, the last commit before
the operator removed the dead code by hand. The reviewer returned `SCOPE VERDICT: PASS`
and called the unused `FileExplorerOperation` enum in scope. A check that cannot report
failure on a known-bad input carries no information on that input.

Two clauses cause this. `core/consensus_scope.py:46` asks whether every change serves
the ticket's purpose, and a file-explorer enum in a file-explorer ticket serves that
purpose by topic. `core/consensus_scope.py:52` then says code quality and correctness
must not affect the verdict, which directs every voice away from this class of finding.

The record matches. All 19 DEV-635 scope reviews failed the ticket overall. Every one
passed `windows-rpa-client-schema`.

Change: add a third question on reachability. For each symbol, field, enum member and
branch the diff adds, the reviewer must name a caller or a reader on the branch, in any
listed repository, and cite the grep. An addition with no caller and no reader is a
finding. Narrow the exclusion clause so it exempts style and correctness but not
reachability. `core/consensus_scope.py:140` already passes every ticket worktree as an
include directory, so a voice can grep across repos. That is what makes the question
answerable for a schema repo whose consumers live in other repos.

Verification that can fail: re-run the falsification test against `e374679` and require
the verdict to flip to FAIL and to name `FileExplorerOperation`. Do not accept the
change on a PASS-only test. Then run the same prompt against `3967da2`, the commit that
removed the dead code, and require PASS. A prompt that fails both commits is a prompt
that only learned to say FAIL.

Launch draft:

> Make the frshty consensus scope review able to fail on unreachable code. Today it cannot.
> Running the real `SCOPE_DIRECTIVE` from `core/consensus_scope.py` against
> `windows-rpa-client-schema` at commit `e374679` returns `SCOPE VERDICT: PASS`, even though
> that commit adds `FileExplorerOperation`, an enum with no caller, and
> `FileExplorerAction.recursive`, a field with no reader. Add a third question to
> `SCOPE_DIRECTIVE` on reachability: for every symbol, field, enum member and branch the diff
> adds, the reviewer must name a caller or a reader on this branch in any listed repository
> and cite the grep it ran; an addition with no caller and no reader is a finding. Narrow the
> exclusion clause at `core/consensus_scope.py:52` so it exempts style and correctness but not
> reachability. Verify by running the new prompt against `e374679` and requiring the verdict to
> flip to FAIL and to name `FileExplorerOperation`, then against `3967da2`, the commit that
> removed both, and requiring PASS. Do not accept the change on a PASS-only test.

## FU-3: repair the third voice and require a quorum

This finding is new. The scope gate is documented as three independent reviewers. It
has run on two for most of its life.

Across all instances there have been 45 fan-outs. 32 of them, 71 percent, decided on
two votes. Every dropped voice was `claude`. `codex` and `agy` were never dropped. The
reasons split 25 to "no SCOPE VERDICT line" and 7 to "exit_code=1".

The vote rule is `fails * 2 >= len(votes)` at `core/consensus_scope.py:165`. With three
votes it takes two FAILs to fail. With two votes one FAIL is enough. So 71 percent of
gate decisions were single-voice vetoes carried by a rule written for three voices. The
drop is recorded in `docs/scope-review.md` and in one `scope_review_fanout_complete`
event. Nothing alerts on it, and the verdict is stored with no record of how many
voices produced it.

The `claude` voice is the only one that does not write to a last-message file. It calls
`run_thinking` at `core/consensus_plan.py`, while `codex` and `agy` go through
`run_external_model` with a `last_message_file` or a transcript file. That difference is
the first thing to check.

Change: find and fix the cause of the missing `SCOPE VERDICT` line from the `claude`
voice. Then set a quorum. Record `len(votes)` on the ticket state next to the verdict.
Emit a distinct warning event when a voice drops. Decide and document what a two-voice
result means; the simplest correct rule is that a verdict from fewer than three voices
is advisory and holds the ticket as `pending` rather than recording a `pass`, so a
single voice can never carry a PASS.

Verification that can fail: after the fix, run the gate ten times on a live ticket and
require zero drops. Assert on the stored vote count, not on the verdict alone.

Launch draft:

> Repair the third voice of the frshty consensus scope review and add a quorum rule. Across 45
> recorded fan-outs, 32 decided on two votes instead of three, and the dropped voice was
> `claude` every time: 25 drops for "no SCOPE VERDICT line" and 7 for "exit_code=1". Re-derive
> this with `scripts/scope_gate_evidence.sh`. The vote rule `fails * 2 >= len(votes)` at
> `core/consensus_scope.py:165` was written for three voices, so with two voices a single FAIL
> decides. Find why the `claude` voice omits the trailing `SCOPE VERDICT:` line; it is the only
> voice that uses `run_thinking` rather than `run_external_model` with a last-message file, see
> `_fan_out` in `core/consensus_plan.py`. Fix it. Then record the vote count on the ticket state
> beside the verdict, emit a distinct warning event when a voice drops, and make a verdict from
> fewer than three voices advisory: hold the ticket as pending instead of recording a pass, so a
> single voice can never carry a PASS. Verify by running the gate ten times on a live ticket and
> requiring zero drops, and assert on the stored vote count, not only on the verdict.

## FU-4: stop the fingerprint rerun loop

This finding is new. The scope review can invalidate its own verdict.

`core/tasks/tickets.py:2142` computes the fingerprint before it calls
`run_scope_review`. `run_scope_review` then runs `git fetch origin <base>` for each repo
at `core/consensus_scope.py:133`, before it derives its own diff. That fetch can move
`origin/<base>`. Moving `origin/<base>` moves the merge-base, which changes
`git diff --raw <merge-base>..HEAD`, which changes the digest.

Section 5 of the evidence script proves the sensitivity on the real DEV-635 schema
worktree: the digest against the current `origin/main` is `20c6b7c1...` and the digest
against an older `origin/main` is `6f604235...`.

So the task records a pre-fetch fingerprint while the dispatcher computes a post-fetch
fingerprint. When the base branch moved during the review, the two disagree,
`_scope_review_state` returns `pending`, and the dispatcher enqueues a fresh review of a
branch that did not change. Each rerun costs three model invocations and about seven
minutes. DEV-635 recorded 19 runs in six days, and several pairs are ten to thirty
minutes apart. The aimyable base branches took merges throughout those days.

Change: fetch first, then compute the fingerprint, then review. Move the fetch out of
`run_scope_review` into a small helper that both `scope_fingerprint` and
`run_scope_review` call, or have the task fetch before it computes the fingerprint. Do
not widen the fingerprint to ignore the base; a real base move does change what the
reviewer must judge, so the review should re-run when the base actually moves, not
because of the order in which frshty read it.

Verification that can fail: a test that moves `origin/<base>` between the task's
fingerprint capture and the review, then asserts the recorded fingerprint equals the
fingerprint `_scope_review_state` computes immediately after. That test must fail
against the current code.

Launch draft:

> Stop the frshty scope review from invalidating its own verdict. `core/tasks/tickets.py:2142`
> captures the branch fingerprint before it calls `run_scope_review`, and `run_scope_review`
> then runs `git fetch origin <base>` at `core/consensus_scope.py:133`. The fetch can move
> `origin/<base>`, which moves the merge-base, which changes the raw branch diff and therefore
> the digest, so the recorded fingerprint is stale the moment the review finishes and the
> dispatcher re-enqueues a full three-voice review of a branch that did not change. Section 5 of
> `scripts/scope_gate_evidence.sh` proves a base move flips the digest on the real DEV-635 schema
> worktree. DEV-635 recorded 19 runs in six days. Fix the ordering: fetch every base branch
> first, then compute the fingerprint, then review. Do not widen the fingerprint to ignore the
> base. Add a test that moves `origin/<base>` between the capture and the review and asserts the
> recorded fingerprint matches what `_scope_review_state` computes right after; confirm that test
> fails against the current code before you fix it.

## FU-5: give tri-review the repo list scope_review already builds

`/tri-review` is the step that does look for dead code. Its maintainability persona
names dead code as a target at `features/reviewer.py:69`. It never examined the schema
repo. `docs/tri-review.md` for DEV-635 names four repos and omits
`windows-rpa-client-schema`, although that repo had a branch diff at review time.

The cause is the prompt. `core/tasks/tickets.py:1332` `start_reviewing` passes only
"Run /tri-review and save the full output to docs/tri-review.md". The command itself
says to use `git diff main...HEAD`, which finds nothing at a multi-repo ticket root.
Which worktrees get opened is left to the model. It opened four of five.

`scope_review` does not have this problem. `core/consensus_scope.py:125` enumerates
every repo with a branch diff and names each worktree in the prompt.

Change: extract that enumeration into a shared helper that returns
`[(repo_name, worktree, base_branch)]` for every repo with a branch diff. Have
`start_reviewing` interpolate the list into its prompt and state that every listed
repository must appear in `docs/tri-review.md`. Add a postcondition that
`docs/tri-review.md` names every repo in the list. `start_reviewing` already carries a
`file_contains` postcondition at `core/tasks/tickets.py:1326`, so the machinery exists.
A silently skipped repo then fails the task instead of passing it.

Verification that can fail: run the new postcondition against the existing DEV-635
`docs/tri-review.md`, which omits `windows-rpa-client-schema`, and require it to fail.

Launch draft:

> Make frshty's `/tri-review` step cover every repository on the ticket branch. For DEV-635 it
> reviewed four of five repos and skipped `windows-rpa-client-schema`, which is the repo that
> carried the dead code into PR #19. The cause is the prompt at `core/tasks/tickets.py:1332`:
> `start_reviewing` passes only "Run /tri-review and save the full output to docs/tri-review.md",
> and the command's own `git diff main...HEAD` finds nothing at a multi-repo ticket root, so the
> model chooses the worktrees. `core/consensus_scope.py:125` already enumerates every repo with a
> branch diff and names each worktree in its prompt. Extract that enumeration into a shared
> helper returning `[(repo_name, worktree, base_branch)]`, interpolate it into the
> `start_reviewing` prompt, and state that every listed repository must appear in
> `docs/tri-review.md`. Add a postcondition that `docs/tri-review.md` names every repo in the
> list, next to the existing `file_contains` postcondition. Verify by running the new
> postcondition against the current DEV-635 `docs/tri-review.md` and requiring it to fail.

## FU-6: clear a stale verdict on a backward manual transition

DEV-635 went to `blocked` because `fix_review_findings` died on a commit. The operator
moved it back to `reviewing` by hand on 2026-08-19T20:36. `docs/tri-review.md` still
read `VERDICT: PASS` above five unresolved blocking findings, so the ticket walked to
`testing` a minute later on a review that never completed.

The commit-ordering half of this is already fixed inside `fix_review_findings`; the
comment at `core/tasks/tickets.py:1373` records it. The override path is not fixed.
`web/tickets.py:1136` `api_set_ticket_status` writes any status with reason "manual
status override" and clears CI fields only. It leaves `docs/tri-review.md` and
`ts["scope_review"]` untouched.

Change: when a manual transition moves a ticket backward into `reviewing`, clear the
verdict line in `docs/tri-review.md` and drop `ts["scope_review"]`. Both then re-run.
The rule is that a verdict is valid only for the state it was computed against.
`scope_review` already follows that rule through its fingerprint. `tri-review` does not
follow it at all, and the override path bypasses it for both.

Verification that can fail: seed a ticket in `blocked` with `VERDICT: PASS` in
`docs/tri-review.md` and a recorded `scope_review` verdict. Post the override to
`reviewing`. Assert the verdict line is gone and `scope_review` is absent from the
state.

Launch draft:

> Stop a manual status override in frshty from carrying a stale review verdict forward.
> `web/tickets.py:1136` `api_set_ticket_status` writes any target status with reason "manual
> status override" and resets only the CI fields. It leaves `docs/tri-review.md` and
> `ts["scope_review"]` untouched. DEV-635 was moved from `blocked` back to `reviewing` by hand on
> 2026-08-19, `docs/tri-review.md` still read `VERDICT: PASS` above five unresolved blocking
> findings, and the ticket advanced to `testing` one minute later on a review that never
> completed. When a manual transition moves a ticket backward into `reviewing`, clear the verdict
> line in `docs/tri-review.md` and drop `ts["scope_review"]` so both re-run. A verdict is valid
> only for the state it was computed against; `scope_review` already enforces that through its
> fingerprint, and `tri-review` does not enforce it at all. Add a test that seeds a blocked ticket
> holding `VERDICT: PASS` and a recorded scope verdict, posts the override to `reviewing`, and
> asserts both are cleared.

## What I checked

- `~/.frshty/frshty.db`: `jobs`, `log_events`, `tickets`, `work_items`, `work_followups`.
- `core/consensus_scope.py`, `core/consensus_plan.py`, `core/scheduler.py`,
  `core/tasks/tickets.py`, `features/tickets.py`, `features/ticket_states.py`,
  `web/tickets.py`, `web/pages.py`, `templates/*.html`, `config/*.toml`.
- `git log` in all seven DEV-635 worktrees.
- One run of `scripts/scope_gate_evidence.sh`, including its known-good control.
