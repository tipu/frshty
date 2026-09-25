#!/usr/bin/env bash
set -euo pipefail

UNIT="${FRSHTY_UNIT:-frshty.service}"
common_dir="$(git rev-parse --path-format=absolute --git-common-dir)"
CHECKOUT="${FRSHTY_CHECKOUT:-$(dirname "$common_dir")}"

export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"

if [ "$(git -C "$CHECKOUT" rev-parse --abbrev-ref HEAD)" != "main" ]; then
  echo "deploy: $CHECKOUT is not on main" >&2
  exit 1
fi
if [ -n "$(git -C "$CHECKOUT" diff --name-only --diff-filter=U)" ]; then
  echo "deploy: $CHECKOUT holds unmerged files" >&2
  exit 1
fi

git -C "$CHECKOUT" fetch origin main
if ! git -C "$CHECKOUT" merge-tree --write-tree HEAD origin/main >/dev/null; then
  echo "deploy: origin/main does not merge cleanly into $CHECKOUT; resolve it in a throwaway worktree" >&2
  exit 1
fi
clobbered="$(LC_ALL=C comm -12 \
  <(git -C "$CHECKOUT" diff --name-only HEAD origin/main | LC_ALL=C sort) \
  <(git -C "$CHECKOUT" ls-files --others --ignored --exclude-standard | LC_ALL=C sort))"
if [ -n "$clobbered" ]; then
  echo "deploy: origin/main would overwrite untracked files in $CHECKOUT:" >&2
  echo "$clobbered" >&2
  exit 1
fi
if ! git -C "$CHECKOUT" merge-base --is-ancestor origin/main HEAD; then
  git -C "$CHECKOUT" merge --no-edit origin/main
fi
git -C "$CHECKOUT" merge-base --is-ancestor origin/main HEAD

before="$(systemctl --user show "$UNIT" -p ExecMainStartTimestampMonotonic --value)"
systemctl --user restart "$UNIT"
after="$(systemctl --user show "$UNIT" -p ExecMainStartTimestampMonotonic --value)"
if [ -z "$after" ] || [ "$after" = "0" ] || [ "$after" = "$before" ]; then
  echo "deploy: $UNIT did not restart (start stamp $before -> $after)" >&2
  exit 1
fi
systemctl --user is-active --quiet "$UNIT"
echo "deploy: $CHECKOUT at $(git -C "$CHECKOUT" rev-parse --short HEAD), $UNIT restarted"
