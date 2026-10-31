#!/usr/bin/env bash
# Install a pinned Prisma CLI outside every worktree for scripts/review_recipes/prisma_generate.py.
# Usage: scripts/install_review_prisma.sh <dir> <version>
# Prints the path to pass as --cli.
set -euo pipefail
dir=$1
version=$2
mkdir -p "$dir"
npm install --prefix "$dir" --ignore-scripts --no-fund --no-audit \
    --registry=https://registry.npmjs.org/ "prisma@$version" >&2
cli="$dir/node_modules/prisma/build/index.js"
installed=$(node -p "require('$dir/node_modules/prisma/package.json').version")
if [ "$installed" != "$version" ]; then
    echo "installed prisma $installed, expected $version" >&2
    exit 1
fi
echo "$cli"
