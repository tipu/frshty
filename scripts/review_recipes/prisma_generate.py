"""Generate a Prisma client in a PR review worktree.

Run it from workspace.review_dep_commands after the install entry, because
--ignore-scripts skips the postinstall that runs prisma generate. The working
directory is the review worktree. Run it with an absolute path into frshty's
own install and with `python3 -I`, so the worktree supplies no code:

    python3 -I /app/scripts/review_recipes/prisma_generate.py \
        --cli /path/to/prisma/build/index.js \
        --schema apps/app/prisma/schema.prisma \
        --output apps/app/src/generated/prisma --option moduleFormat=cjs

The PR controls the schema text and nothing else. The operator's pinned CLI
(--cli, installed with scripts/install_review_prisma.sh) runs from a scratch
directory with a frshty config, so the PR prisma.config.ts and node_modules
are never loaded. The script appends its own generator block and selects only
that block with `--generator`, so a PR generator, whose provider is a shell
command, never runs, and the PR names no output path. The minimal environment
keeps every PRISMA_* override out, and PRISMA_SCHEMA_ENGINE_BINARY stops the
CLI from fetching a schema engine that generate does not use.

Exit 0 on success. Exit 1 with the reason on stderr otherwise; frshty logs it
as worktree_dep_command_failed and the review still runs.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

GENERATOR = "frshty_review_client"
TIMEOUT = 300


def worktree_path(wt_path: Path, rel: str) -> Path | None:
    """`wt_path / rel` when `rel` is relative, has no `..` part, and no part
    of it that exists is a symlink. None otherwise."""
    parts = Path(rel).parts
    if not parts or Path(rel).is_absolute() or ".." in parts:
        return None
    path = wt_path
    for part in parts:
        path = path / part
        if path.is_symlink():
            return None
    return path


def generate(cli: str, schema_rel: str, output_rel: str, options: dict,
             wt_path: Path) -> str | None:
    """Generate the client into `wt_path / output_rel` and return None, or
    return the reason it did not."""
    schema = worktree_path(wt_path, schema_rel)
    if schema is None or not schema.is_file():
        return f"schema {schema_rel} is missing or passes through a symlink"
    dest = worktree_path(wt_path, output_rel)
    if dest is None:
        return f"output {output_rel} passes through a symlink"
    try:
        text = schema.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        return f"schema {schema_rel} is unreadable: {e}"
    with tempfile.TemporaryDirectory(prefix="frshty-prisma-") as tmp:
        stage = Path(tmp)
        out = stage / "out"
        lines = "".join(f"  {k} = {json.dumps(v)}\n" for k, v in options.items())
        (stage / "schema.prisma").write_text(
            text + f"\n\ngenerator {GENERATOR} {{\n"
            f"  provider = \"prisma-client\"\n  output = {json.dumps(str(out))}\n{lines}}}\n",
            encoding="utf-8")
        (stage / "prisma.config.mjs").write_text(
            f"export default {{ schema: {json.dumps(str(stage / 'schema.prisma'))} }}\n")
        env = {"PATH": os.environ.get("PATH", ""), "HOME": str(stage),
               "CHECKPOINT_DISABLE": "1", "PRISMA_HIDE_UPDATE_MESSAGE": "1",
               "PRISMA_SCHEMA_ENGINE_BINARY": "/bin/false"}
        argv = ["node", cli, "generate", "--config", str(stage / "prisma.config.mjs"),
                "--generator", GENERATOR]
        try:
            result = subprocess.run(argv, cwd=str(stage), env=env, capture_output=True,
                                    text=True, timeout=TIMEOUT)
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
            return f"{type(e).__name__}: {e}"
        if result.returncode != 0 or not out.is_dir():
            return f"rc={result.returncode}: {(result.stderr or result.stdout or '')[-500:]}"
        try:
            if dest.is_dir():
                shutil.rmtree(dest)
            elif dest.exists():
                dest.unlink()
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(out, dest, symlinks=True)
        except OSError as e:
            return f"copy to {output_rel} failed: {e}"
    return None


def _option(value: str) -> tuple[str, str]:
    key, sep, val = value.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(f"expected key=value, got {value!r}")
    return key, val


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a Prisma client in a review worktree.")
    parser.add_argument("--cli", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--option", type=_option, action="append", default=[])
    args = parser.parse_args(argv)
    reason = generate(args.cli, args.schema, args.output, dict(args.option), Path.cwd())
    if reason:
        print(f"prisma generate failed: {reason}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
