#!/usr/bin/env bash
set -euo pipefail

GVOICE_REF="7d7a74acf10c48cc3d0424d2ff96c24f0fb8a5d1"

instance="${1:?usage: scripts/gvoice_login.sh <instance key>}"
root="${FRSHTY_CONTAINERS:-$HOME/.frshty-containers}"
state="$root/$instance/state"
if [ ! -d "$state" ]; then
  echo "gvoice_login: $state does not exist; name an instance that runs in a container" >&2
  exit 1
fi

prefix="$root/.gvoice-cli"
if [ ! -x "$prefix/bin/gvoice" ]; then
  npm install -g --prefix "$prefix" "github:tipu/google-voice-cli#$GVOICE_REF"
fi

export GVOICE_PROFILE_DIR="$state/gvoice-profile"
export GVOICE_CHROME_CHANNEL="${GVOICE_CHROME_CHANNEL:-chrome}"
mkdir -p "$GVOICE_PROFILE_DIR"
"$prefix/bin/gvoice" login
"$prefix/bin/gvoice" status
echo "gvoice_login: $instance reads texts from $GVOICE_PROFILE_DIR"
