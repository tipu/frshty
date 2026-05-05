# Problem Summary: Duplicate PR Review Fan-Out and Usage Drain

## What happened

Twice on May 4, 2026, frshty's automated PR reviewer launched a large batch of Claude review sessions against the same set of open Bitbucket PRs. Each PR review in frshty fans out into three parallel persona reviews:

- spec
- production-breakage
- maintainability

Because the affected batch contained many PRs, one release of that batch created dozens of top-level Claude sessions in a short window. Once the Claude quota bucket was already low, those sessions immediately failed with:

`You've hit your limit · resets 6pm (America/Los_Angeles)`

The usage loss came from many root review sessions being launched, not from one subagent looping inside a single session.

## How frshty launches these reviews

The launch path was:

1. `poll_reviewer`
2. `features.reviewer.check()`
3. `features.reviewer._track_pending_prs()`
4. `features.reviewer._process_ready_tickets()`
5. `features.reviewer.review_ticket_prs()`
6. `features.reviewer.review_pr()`
7. `features.reviewer._run_all_personas()`
8. `core.claude_runner.run_sonnet()`

Important behavior:

- frshty groups reviewable PRs into `reviews_pending` buckets by ticket
- each ticket waits for a 15 minute quiet period before release
- when released, every PR in that ticket is reviewed
- each PR review starts 3 parallel Claude persona sessions

So one ticket release can multiply into a large burst of Claude sessions.

## The actual bug

There were three separate issues working together.

### 1. `poll_reviewer` jobs could overlap

The queue layer only prevented concurrent jobs when they shared a non-null `ticket_key`.

`poll_reviewer` jobs run with:

- `task = "poll_reviewer"`
- `ticket_key = None`

That meant the queue treated them as non-conflicting, so multiple `poll_reviewer` jobs for the same instance could run at the same time if cron ticks stacked up or workers claimed them close together.

Effect:

- two reviewer polls could load the same `reviews_pending` state
- both could decide the same ticket batch was ready
- both could release the same batch
- both could launch the same PR reviews

This is the core race that allowed duplicate batch fan-out.

### 2. Reviewer state used plain load-mutate-save with no coordination

`features/reviewer.py` stored pending review batches in a JSON blob under `reviews_pending` via:

- `state.load("reviews_pending")`
- mutate in memory
- `state.save("reviews_pending", pending)`

This was designed under the assumption that `poll_reviewer` was effectively single-writer. Once overlapping `poll_reviewer` jobs were possible, that assumption became false.

Effect:

- concurrent reviewer polls raced on the same pending state
- duplicate releases became much easier
- one run could overwrite or ignore another run's changes

### 3. Open PRs were re-tracked even when unchanged

The reviewer pipeline previously tracked open PRs just because they were still reviewable. It did not first ask whether the PR had changed since the last completed review.

That meant a PR could be:

- already reviewed
- have the same branch head
- have the same `updated_on`
- still get placed back into pending state again

Effect:

- even without the overlap race, unchanged open PRs could become eligible for another batch later
- after a failed burst, the same PRs could be queued again
- this is why the problem could happen more than once in the same day

## Why it happened twice today

The combination of the three issues above created the repeat behavior:

1. a ticket batch became eligible after the 15 minute quiet period
2. overlapping `poll_reviewer` jobs could both release it
3. each PR in that batch spawned 3 persona sessions
4. those sessions hit Claude rate limits and failed
5. the failed PRs remained effectively eligible for later review
6. unchanged open PRs could be tracked again on later polls
7. the same overall batch pattern could recur

So the second incident was not a separate root cause. It was the same reviewer scheduling flaw, replaying under the same workload.

## What the evidence showed

The evidence pointed to frshty's automated reviewer, not manual terminal usage:

- root Claude sessions were launched by `sdk-cli` from the frshty repo
- review artifact directories under `~/.frshty/aimyable/reviews/...` were created immediately before the burst
- affected branches/PRs matched frshty's review state and worktree layout
- the burst was dominated by repeated PR review prompts with persona-specific wording
- there was only minimal subagent activity relative to the number of root sessions

This ruled out:

- a one-off manual `claude` command
- the GitHub-only manual review API route
- a subagent recursion loop as the primary usage drain for this event

## Why the quota burn was so large

The review design multiplies work aggressively:

- 1 ticket batch
- N PRs in that batch
- 3 personas per PR

If the batch contains 14 PRs, that is already 42 Claude sessions before any retries, validation passes, or follow-up work.

That architecture is fine only if:

- release happens once
- unchanged PRs are skipped
- failures back off instead of immediately recycling

Those guarantees were missing.

## Fixes applied

Three protections were added.

### 1. Queue-level singleton protection for global poll tasks

File:

- `core/queue.py`

Change:

- `claim_next()` now blocks a queued job when another running job in the same instance has:
  - the same non-null `ticket_key`, or
  - the same task and both jobs have `ticket_key = None`

Effect:

- only one `poll_reviewer` can run per instance at a time
- the duplicate batch-release race is blocked at the queue boundary

### 2. Skip unchanged PRs

File:

- `features/reviewer.py`

Change:

- added `_pr_needs_tracking(review_state, pr)`
- reviewer now compares current PR metadata to saved review metadata
- unchanged already-reviewed PRs are not re-added to `reviews_pending`

Specifically, reviewer checks:

- `last_head_sha`
- `last_updated`
- whether the PR was previously marked reviewed

Effect:

- open PRs no longer get re-batched just because they still exist
- re-reviews only happen when the PR actually changes

### 3. Cooldown after failed review batches

File:

- `features/reviewer.py`

Change:

- failed PRs are preserved in the pending ticket entry
- the ticket enters a cooldown window before retry
- current cooldown is 1 hour

Effect:

- when Claude is already rate-limited, frshty does not immediately hammer the same batch again on the next poll
- failure cost is capped instead of compounding every cycle

## Test coverage added

Regression tests were added for:

- queue claim behavior preventing duplicate global poll task overlap
- reviewer skipping unchanged already-reviewed PRs
- reviewer placing failed ticket batches into cooldown

Verified with targeted tests:

- `tests/core/test_queue_sweep.py`
- `tests/features/test_reviewer.py`

Result:

- `42/42` tests passed

## Operational implication

If frshty is already running in another process, it must be restarted for the new protections to take effect. The code changes only protect future reviewer polls after the updated process is live.

## Bottom line

The problem was not Claude randomly misbehaving on its own. The real issue was frshty's review scheduler:

- overlapping reviewer polls were allowed
- pending review state assumed single-writer access
- unchanged PRs were re-queued
- failed batches had no meaningful backoff

Under a multi-PR ticket batch, that turned one quiet-period release into dozens of persona review sessions and made it possible for the same pattern to repeat later the same day.
