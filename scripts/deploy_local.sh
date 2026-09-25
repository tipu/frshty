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
target="$(git -C "$CHECKOUT" rev-parse --verify origin/main^{commit})"
if ! git -C "$CHECKOUT" merge-tree --write-tree HEAD "$target" >/dev/null; then
  echo "deploy: origin/main does not merge cleanly into $CHECKOUT; resolve it in a throwaway worktree" >&2
  exit 1
fi
clobbered="$(awk -v RS='\0' '
  FNR == NR { changed[$0] = 1; next }
  {
    p = $0
    while (1) {
      if (p in changed) { print $0; next }
      i = match(p, /\/[^\/]*$/)
      if (!i) break
      p = substr(p, 1, i - 1)
    }
    for (c in changed) if (index(c, $0 "/") == 1) { print $0; next }
  }' \
  <(git -C "$CHECKOUT" diff -z --name-only HEAD "$target") \
  <(git -C "$CHECKOUT" ls-files -z --others --ignored --exclude-standard))"
if [ -n "$clobbered" ]; then
  echo "deploy: origin/main would overwrite untracked files in $CHECKOUT:" >&2
  echo "$clobbered" >&2
  exit 1
fi
if ! git -C "$CHECKOUT" merge-base --is-ancestor "$target" HEAD; then
  git -C "$CHECKOUT" merge --no-edit "$target"
fi
git -C "$CHECKOUT" merge-base --is-ancestor "$target" HEAD

before="$(systemctl --user show "$UNIT" -p ExecMainStartTimestampMonotonic --value)"
systemctl --user restart "$UNIT"
after="$(systemctl --user show "$UNIT" -p ExecMainStartTimestampMonotonic --value)"
if [ -z "$after" ] || [ "$after" = "0" ] || [ "$after" = "$before" ]; then
  echo "deploy: $UNIT did not restart (start stamp $before -> $after)" >&2
  exit 1
fi
systemctl --user is-active --quiet "$UNIT"
echo "deploy: $CHECKOUT at $(git -C "$CHECKOUT" rev-parse --short HEAD), $UNIT restarted"
