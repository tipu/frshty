# Technical Plan: Per-Ticket Cross-Project Review Batching

## 1. Requirements Summary

### Objective
Shift review scheduling from immediate per-PR execution to delayed, batched, per-ticket review across projects.

### Required Features
- Map each review-eligible PR to a ticket.
- Track pending reviews by ticket instead of by `repo/id`.
- Delay review execution until a ticket has been quiet for 15 minutes.
- Extend the 15-minute window when another PR for the same ticket appears.
- Run one review batch across all PR branches for the ticket after the quiet period.
- Rerun the batch if a later PR arrives for an already-reviewed ticket.

### Optional Features
- None specified.

### Acceptance Criteria
The requirements file lists 6 criteria total:
1. First PR for ticket `FOO` is tracked immediately and not reviewed for 15 minutes.
2. Second PR for `FOO` arriving 5 minutes later extends the timeout.
3. After 15+ minutes of quiet, review runs against both branches.
4. A third PR arriving an hour later reruns review against all 3 branches.
5. Per-project silos are removed; reviews are cross-project per ticket.
6. No performance regression; batching should reduce total review time.

The first 5 are the core behavioral acceptance criteria. The 6th is a performance constraint and should still be preserved.

## 2. Current State Analysis

### Entry Point
- [core/tasks/polls.py](/home/tipu/Documents/dev/frshty/core/tasks/polls.py:12) defines `poll_reviewer`.
- [core/tasks/polls.py](/home/tipu/Documents/dev/frshty/core/tasks/polls.py:13) calls `reviewer.check(ctx.config)`.

### Current Review Flow
- [features/reviewer.py](/home/tipu/Documents/dev/frshty/features/reviewer.py:80) builds the platform adapter and fetches review PRs via `platform.list_review_prs()`.
- [features/reviewer.py](/home/tipu/Documents/dev/frshty/features/reviewer.py:90) iterates PRs one-by-one.
- [features/reviewer.py](/home/tipu/Documents/dev/frshty/features/reviewer.py:116) immediately calls `review_pr(config, platform, pr)` for each PR needing work.
- [features/reviewer.py](/home/tipu/Documents/dev/frshty/features/reviewer.py:138) persists review status under `state.save("reviews", review_state)`.

### Current Review State Shape
- `reviews` state is keyed per PR as `"{repo}/{id}"`.
- Each entry currently stores:
  - `reviewed`
  - `branch`
  - `last_updated`
  - `last_head_sha`

Example:

```python
{
  "repo-a/123": {
    "reviewed": True,
    "branch": "FOO-123-api-change",
    "last_updated": "2026-04-24T10:00:00Z",
    "last_head_sha": "abc123"
  }
}
```

### Current Review Output Contract
- [features/reviewer.py](/home/tipu/Documents/dev/frshty/features/reviewer.py:142) defines `review_pr(config, platform, pr) -> dict | None`.
- The merged review written to disk contains:
  - `verdict`
  - `issues`
  - `author`
  - `source_branch`
  - `destination_branch`
  - `date`
  - `summary`
  - `blocking_summary`
  - `suggestions_summary`
  - `questions_summary`
- Queued comments are written per PR branch to `reviews/<repo>/<branch>/queued_comments.json`.

### Existing Ticket Extraction Logic
- [features/timesheet.py](/home/tipu/Documents/dev/frshty/features/timesheet.py:601) already has `_extract_ticket(text: str) -> str`.
- It uses a simple regex and normalizes matches to uppercase:

```python
def _extract_ticket(text: str) -> str:
    m = re.search(r"[A-Za-z]+-\d+", text)
    return m.group().upper() if m else ""
```

This is the best current candidate to reuse for review batching instead of inventing a separate parser.

### State Layer Constraints
- [core/state.py](/home/tipu/Documents/dev/frshty/core/state.py:72) and [core/state.py](/home/tipu/Documents/dev/frshty/core/state.py:86) already support generic module-level KV blobs through `state.load(module)` and `state.save(module, data)`.
- That means `reviews_pending` can be added without a schema migration.
- There is no generic transactional `update(module, mutate)` helper for KV blobs, so the first implementation will likely remain `load -> mutate -> save`, which is acceptable if `poll_reviewer` is effectively single-writer in practice.

## 3. Architecture Diagram

### Current

```text
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐
│ poll_reviewer   │────▶│ reviewer.    │────▶│ review_pr()  │
│ periodic task   │     │ check()      │     │ per PR       │
└─────────────────┘     └──────────────┘     └──────────────┘
                               │
                               ▼
                        state["reviews"]
                        keyed by repo/id
```

### Proposed

```text
┌─────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│ poll_reviewer   │────▶│ reviewer.check()     │────▶│ review_ticket_batch()│
│ periodic task   │     │ - fetch review PRs   │     │ - review all PRs     │
└─────────────────┘     │ - map to tickets     │     │   for one ticket     │
                        │ - track pending      │     │ - write per-PR output │
                        │ - detect quiet timer │     │ - update reviewed map │
                        └──────────────────────┘     └──────────────────────┘
                               │
                               ▼
                 state["reviews_pending"] keyed by ticket
                 state["reviews"] keyed by repo/id with batch metadata
```

## 4. Proposed Solution

### Design Principles
- Keep `poll_reviewer` unchanged as the task entry point.
- Reuse `review_pr()` as much as possible if batch orchestration can stay thin.
- Add the smallest possible state needed to support quiet-period batching.
- Prefer reusing the existing ticket extractor from `features.timesheet`.

### Proposed State Shape

`state["reviews_pending"]`:

```python
{
  "FOO-123": {
    "tracked_at": "2026-04-24T17:00:00+00:00",
    "last_seen_at": "2026-04-24T17:05:00+00:00",
    "prs": [
      {
        "repo": "repo-a",
        "id": 101,
        "branch": "foo-123-api",
        "url": "https://example.com/pr/101",
        "updated_on": "2026-04-24T17:05:00Z",
        "head_sha": "abc"
      },
      {
        "repo": "repo-b",
        "id": 17,
        "branch": "FOO-123-ui",
        "url": "https://example.com/pr/17",
        "updated_on": "2026-04-24T17:04:00Z",
        "head_sha": "def"
      }
    ]
  }
}
```

`state["reviews"]` remains per PR, but should gain enough metadata to support reruns and auditability:

```python
{
  "repo-a/101": {
    "reviewed": True,
    "branch": "foo-123-api",
    "ticket": "FOO-123",
    "last_updated": "2026-04-24T17:05:00Z",
    "last_head_sha": "abc",
    "last_reviewed_at": "2026-04-24T17:20:00+00:00",
    "batch_size": 2
  }
}
```

### Core Algorithm

#### `reviewer.check(config)`

```python
1. platform = make_platform(config)
2. review_prs = platform.list_review_prs()
3. load review_state = state.load("reviews")
4. load pending = state.load("reviews_pending")
5. dedupe PRs by repo/id
6. for each PR:
   a. decide whether this PR represents new review input
      - never reviewed before
      - head_sha changed
      - or updated_on changed when no head_sha is available
   b. extract ticket from branch/title/description candidate text
   c. upsert pending[ticket]
      - create entry if missing with tracked_at=now
      - update existing PR snapshot if already tracked
      - append new PR if not present
      - set last_seen_at=now whenever a new/revised PR is added
7. for each pending ticket:
   a. if now - last_seen_at < 15 minutes: skip
   b. otherwise run batch review for all tracked PRs on that ticket
   c. on success:
      - update review_state for each reviewed PR
      - remove ticket from pending
   d. on failure:
      - leave ticket in pending for retry
8. save review_state
9. save reviews_pending
```

#### Batch Review Execution

There are two implementation options:

Option A: Minimal-churn path
- Keep `review_pr()` as the execution primitive.
- `reviewer.check()` delays scheduling until the quiet period expires.
- When the ticket is ready, call `review_pr()` for each PR in the batch in one pass.
- This satisfies delayed batching and cross-project coordination, but each PR is still reviewed independently.

Option B: True combined review context
- Add `review_ticket_batch(config, platform, ticket, prs)`.
- Build one combined prompt or orchestration flow covering all PR diffs for the ticket.
- Split the resulting issues back into per-PR artifacts and queued comments.

Recommended first implementation: Option A.

Reason:
- It fully satisfies the scheduling requirement.
- It is much smaller and safer than redesigning the prompt/output pipeline.
- The requirements emphasize batch scheduling and cross-project grouping, not necessarily a single combined LLM judgment artifact.

If product intent later requires one synthesized ticket-level review artifact, that is the point to add Option B.

### Ticket Extraction Strategy

Use the existing `features.timesheet._extract_ticket()` helper first, ideally by moving it to a shared utility if needed.

Extraction order:
1. PR branch name
2. PR title
3. Optional PR description if available from `platform.get_pr_info()`
4. Fallback bucket for unmatched PRs

Fallback behavior:
- If no ticket is extractable, use a stable synthetic key like `__no_ticket__:{repo}/{id}`.
- That preserves the 15-minute delay logic without grouping unrelated PRs together.

This is better than grouping all unmatched PRs under one global `__no_ticket__` bucket, which would incorrectly batch unrelated work.

### Logging Changes

Current logs are per PR:
- `review_started`
- `review_complete`
- `review_comments_queued`
- `review_failed`

Proposed additions:
- `review_ticket_tracked`
- `review_ticket_extended`
- `review_ticket_started`
- `review_ticket_complete`

Per-PR completion logs can remain unchanged after batch execution so downstream UI/event consumers keep working.

## 5. Service Contracts

These contracts keep types explicit while staying aligned with the current code style.

```python
def check(config: dict) -> None:
    """
    Poll review-eligible PRs, track them by ticket, and execute batched reviews
    after 15 minutes of quiet for each ticket.
    """


def _extract_review_ticket(platform, pr: dict) -> str:
    """
    Return the normalized ticket key for a PR.

    Uses branch/title first and may consult platform.get_pr_info() for
    description text when needed. Returns a stable synthetic key for PRs
    with no extractable ticket.
    """


def _pr_needs_tracking(review_state: dict, pr: dict) -> bool:
    """
    Return True when a PR is new or has changed since the last completed review.
    """


def _track_pending_review(pending: dict, ticket: str, pr: dict, tracked_at: str) -> bool:
    """
    Upsert one PR into state['reviews_pending'][ticket].

    Returns True if the pending entry changed in a way that should refresh
    last_seen_at.
    """


def _ticket_ready_for_review(entry: dict, now_iso: str, quiet_minutes: int = 15) -> bool:
    """
    Return True when the ticket has been quiet for the configured delay.
    """


def _review_ticket_batch(config: dict, platform, ticket: str, prs: list[dict]) -> list[dict] | None:
    """
    Execute review for all tracked PRs belonging to one ticket.

    Returns a list of successful per-PR review results:
    [
      {
        "pr": {"repo": "...", "id": 123, "branch": "...", "url": "..."},
        "result": {
          "verdict": "...",
          "issues": [...],
          "summary": "...",
          ...
        }
      }
    ]

    Returns None when the batch fails and should be retried on the next poll.
    """
```

## 6. Files To Modify

| File | Change Type | Description |
|------|-------------|-------------|
| `features/reviewer.py` | Major | Replace immediate per-PR scheduling with ticket-based pending tracking and quiet-period batch execution. |
| `features/timesheet.py` or shared utility module | Minor | Reuse or relocate `_extract_ticket()` so reviewer logic does not duplicate ticket parsing. |
| `core/state.py` | Optional / Minor | No schema change required. Only add a generic KV update helper if concurrent writes become a concern. |
| `tests/features/test_reviewer.py` | Major | Add tests for pending tracking, timeout extension, quiet-period execution, reruns, and no-ticket fallback. |

## 7. Performance Considerations

### Expected Impact
- `platform.list_review_prs()` stays the same.
- State overhead is small: one pending entry per active ticket plus a PR list.
- Review execution should decrease in churn because new PRs for the same ticket are absorbed into a single delayed batch window.

### Complexity
- Tracking pass: `O(n)` for `n` open review PRs.
- Ready-ticket pass: `O(t + p)` where `t` is pending tickets and `p` is PRs across ready tickets.

### Why This Should Reduce Total Time
- The current system may review each PR immediately and then re-review when follow-up PRs arrive.
- The batched system waits through the expected burst window, so it should eliminate unnecessary early reviews for the same ticket.

## 8. Error Handling

| Scenario | Expected Behavior |
|----------|-------------------|
| `list_review_prs()` fails or returns empty | No-op for this poll; existing pending state remains intact. |
| Ticket cannot be extracted | Track under a stable per-PR fallback key and review after 15 minutes. |
| Same PR appears twice in one poll | Deduplicate by `repo/id`. |
| PR changes while still pending | Refresh that PR snapshot and extend `last_seen_at`. |
| Batch review partially fails | Treat batch as failed, keep ticket pending, retry on next poll. |
| Previously reviewed ticket gets a new PR later | Recreate or refresh pending state and rerun review for all currently open PRs on that ticket. |

The partial-failure rule is intentionally simple: no per-PR commit bookkeeping inside the batch on the first pass.

## 9. Boundary Conditions And Test Scenarios

| Scenario | Input | Expected Result |
|----------|-------|-----------------|
| First PR for `FOO-1` | No prior state | Ticket is added to `reviews_pending`; no review runs yet. |
| Second PR for `FOO-1` within 5 minutes | Existing pending ticket with one PR | Second PR is appended; `last_seen_at` moves forward. |
| Same PR updated before timeout | Pending ticket already contains PR | PR snapshot updates and timeout extends. |
| Quiet period expires | Pending ticket older than 15 minutes since `last_seen_at` | Both PRs are reviewed in one batch cycle and ticket is removed from pending. |
| Third PR after prior batch completed | `reviews` already marks earlier PRs reviewed | Ticket is re-added to pending and next batch includes all current open PRs for that ticket. |
| PR with no ticket | Branch/title have no ticket token | PR gets isolated fallback key and still reviews correctly after delay. |
| Different repos, same ticket | `repo-a` and `repo-b` PRs both map to `FOO-1` | One shared pending ticket entry holds both PRs. |

## 10. Test Quality Requirements

All new tests should follow these rules:
- No silent skips or early returns for missing state.
- Use explicit assertions with readable failure messages where helpful.
- Test names should describe the behavior under test, for example:
  - `test_pr_tracked_then_review_delayed`
  - `test_new_pr_extends_ticket_timeout`
  - `test_quiet_period_runs_batch_for_all_ticket_prs`
  - `test_late_pr_reruns_ticket_batch`
  - `test_pr_without_ticket_uses_isolated_fallback_key`

## 11. Recommended Implementation Order

1. Add or reuse shared ticket extraction helper.
2. Refactor `reviewer.check()` to load `reviews_pending`, track PRs by ticket, and gate execution on the 15-minute quiet window.
3. Add a small `_review_ticket_batch()` helper that loops `review_pr()` across ready PRs.
4. Extend `reviews` state entries with ticket and batch metadata.
5. Add focused tests around timing, grouping, reruns, and no-ticket behavior.

## 12. Notes / Non-Goals

- No change is required in `poll_reviewer`; the behavior change belongs in `features/reviewer.py`.
- No state schema migration is required for `reviews_pending`.
- A fully combined multi-PR LLM review artifact is not necessary for the first delivery unless product explicitly wants one ticket-level review output file.
