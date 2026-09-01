#!/usr/bin/env bash
# Re-derives every number quoted in DEV-635-SCOPE-GATE-FOLLOWUPS.md.
set -euo pipefail

DB="${FRSHTY_DB:-$HOME/.frshty/frshty.db}"
REPO="${FRSHTY_REPO:-$HOME/Documents/dev/frshty/frshty}"
WT="${DEV635_SCHEMA_WT:-$HOME/Documents/dev/aimyable/tickets/DEV-635-file-explorer-tool/workspace/windows-rpa-client-schema}"

echo "== 1. consensus quorum: how many scope verdicts ran on fewer than three voices"
python3 - "$DB" <<'PY'
import json, sqlite3, sys
from collections import Counter
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
total = dropped = 0
reasons = Counter()
sizes = Counter()
for r in c.execute("SELECT meta FROM log_events WHERE event='scope_review_fanout_complete'"):
    m = json.loads(r["meta"] or "{}")
    total += 1
    sizes[len(m.get("votes") or {})] += 1
    for name, why in (m.get("dropped") or {}).items():
        dropped += 1
        reasons[(name, str(why)[:40])] += 1
print(f"   fanouts: {total}")
print(f"   votes per fanout: {dict(sorted(sizes.items()))}")
print(f"   dropped voices: {dropped}")
for (name, why), n in reasons.most_common():
    print(f"     {n:3d}  {name}: {why}")
PY

echo
echo "== 2. gate outcomes recorded on tickets"
python3 - "$DB" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
for r in c.execute("SELECT event, COUNT(*) n FROM log_events "
                   "WHERE event LIKE 'ticket_scope_review%' GROUP BY event ORDER BY event"):
    print(f"   {r['event']:32s} {r['n']}")
PY

echo
echo "== 3. scope_review runs for DEV-635"
python3 - "$DB" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.row_factory = sqlite3.Row
rows = c.execute("SELECT id, status, enqueued_at, finished_at FROM jobs "
                 "WHERE instance_key='aimyable' AND ticket_key='DEV-635' "
                 "AND task='scope_review' ORDER BY id").fetchall()
print(f"   runs: {len(rows)}  statuses: {sorted({r['status'] for r in rows})}")
print(f"   first: {rows[0]['enqueued_at']}")
print(f"   last:  {rows[-1]['enqueued_at']}")
PY

echo
echo "== 4. ship paths that do not consult the scope verdict"
cd "$REPO"
# core/scheduler.py:_execute_create_pr is the known-good control. It must print
# "consults the gate". If it does not, this check is broken, not the code.
for target in "core/scheduler.py:_execute_create_pr" "web/tickets.py:_submit_pr_sync" "web/tickets.py:api_merge_ticket"; do
  f="${target%%:*}"; fn="${target##*:}"
  start=$(grep -n "def ${fn}" "$f" | head -1 | cut -d: -f1)
  body=$(sed -n "${start},$((start + 80))p" "$f")
  if printf '%s' "$body" | grep -q "_scope_review_state"; then
    echo "   ${target}: consults the gate"
  else
    echo "   ${target}: DOES NOT consult the gate"
  fi
done

echo
echo "== 5. a base-branch move flips the scope fingerprint"
if [ -d "$WT" ]; then
  mb_now=$(git -C "$WT" merge-base origin/main HEAD)
  mb_old=$(git -C "$WT" merge-base origin/main~5 HEAD)
  d_now=$(git -C "$WT" diff --raw "${mb_now}..HEAD" | sha256sum | cut -c1-16)
  d_old=$(git -C "$WT" diff --raw "${mb_old}..HEAD" | sha256sum | cut -c1-16)
  echo "   digest at current base: ${d_now}"
  echo "   digest at older base:   ${d_old}"
  if [ "$d_now" = "$d_old" ]; then
    echo "   SAME: a base move does not flip the fingerprint"
  else
    echo "   DIFFERENT: a base move flips the fingerprint, so the recorded verdict goes stale"
  fi
else
  echo "   worktree absent, skipped: $WT"
fi
