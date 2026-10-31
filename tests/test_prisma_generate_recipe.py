import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "review_recipes" / "prisma_generate.py"
sys.path.insert(0, str(SCRIPT.parent))

import prisma_generate  # noqa: E402

CLI = "/opt/prisma/build/index.js"
SCHEMA = "app/prisma/schema.prisma"
OUTPUT = "app/src/generated/prisma"


def _worktree(tmp_path):
    wt = tmp_path / "wt"
    (wt / "app" / "prisma").mkdir(parents=True)
    (wt / "app" / "prisma" / "schema.prisma").write_text(
        'generator client {\n  provider = "touch /tmp/pwned"\n}\n')
    return wt


def _fake_generate(seen):
    def run(argv, cwd, env, **kw):
        stage = Path(cwd)
        seen.update(argv=argv, env=env, cwd=cwd,
                    schema=(stage / "schema.prisma").read_text(),
                    config=(stage / "prisma.config.mjs").read_text())
        (stage / "out").mkdir()
        (stage / "out" / "client.ts").write_text("export {}\n")
        return subprocess.CompletedProcess(argv, 0, "", "")
    return run


def _generate(wt):
    return prisma_generate.generate(CLI, SCHEMA, OUTPUT, {"moduleFormat": "cjs"}, wt)


def test_runs_only_the_frshty_generator_and_copies_its_output(tmp_path):
    wt = _worktree(tmp_path)
    seen = {}
    with patch("prisma_generate.subprocess.run", side_effect=_fake_generate(seen)):
        assert _generate(wt) is None
    stage = Path(seen["cwd"])
    assert seen["argv"] == [
        "node", CLI, "generate", "--config", str(stage / "prisma.config.mjs"),
        "--generator", prisma_generate.GENERATOR]
    assert set(seen["env"]) == {"PATH", "HOME", "CHECKPOINT_DISABLE",
                                "PRISMA_HIDE_UPDATE_MESSAGE", "PRISMA_SCHEMA_ENGINE_BINARY"}
    assert seen["env"]["HOME"] == str(stage)
    assert not stage.is_relative_to(wt)
    assert seen["schema"].endswith(
        f"generator {prisma_generate.GENERATOR} {{\n"
        f'  provider = "prisma-client"\n  output = "{stage / "out"}"\n'
        f'  moduleFormat = "cjs"\n}}\n')
    assert str(stage / "schema.prisma") in seen["config"]
    assert (wt / OUTPUT / "client.ts").read_text() == "export {}\n"


def test_replaces_a_stale_output(tmp_path):
    wt = _worktree(tmp_path)
    stale = wt / OUTPUT
    stale.mkdir(parents=True)
    (stale / "old.ts").write_text("")
    with patch("prisma_generate.subprocess.run", side_effect=_fake_generate({})):
        assert _generate(wt) is None
    assert sorted(p.name for p in stale.iterdir()) == ["client.ts"]


def test_a_symlinked_output_is_refused(tmp_path):
    wt = _worktree(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("")
    (wt / "app" / "src").symlink_to(outside)
    with patch("prisma_generate.subprocess.run") as run:
        assert "symlink" in _generate(wt)
    assert not run.called
    assert [p.name for p in outside.iterdir()] == ["keep"]


def test_a_symlinked_schema_is_refused(tmp_path):
    wt = _worktree(tmp_path)
    secret = tmp_path / "secret"
    secret.write_text("token")
    schema = wt / SCHEMA
    schema.unlink()
    schema.symlink_to(secret)
    with patch("prisma_generate.subprocess.run") as run:
        assert "symlink" in _generate(wt)
    assert not run.called


def test_a_parent_path_is_refused(tmp_path):
    wt = _worktree(tmp_path)
    with patch("prisma_generate.subprocess.run") as run:
        reason = prisma_generate.generate(CLI, SCHEMA, "../escape", {}, wt)
    assert "symlink" in reason
    assert not run.called


def test_a_failed_generate_keeps_the_output(tmp_path):
    wt = _worktree(tmp_path)
    kept = wt / OUTPUT
    kept.mkdir(parents=True)
    (kept / "client.ts").write_text("old")
    failed = subprocess.CompletedProcess([], 1, "", "Validation Error Count: 1")
    with patch("prisma_generate.subprocess.run", return_value=failed):
        assert _generate(wt) == "rc=1: stderr: Validation Error Count: 1"
    assert (kept / "client.ts").read_text() == "old"


def _fake_node(tmp_path, body):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    node = bin_dir / "node"
    node.write_text("#!/bin/sh\n" + body)
    node.chmod(0o755)
    return str(bin_dir) + ":/usr/bin:/bin"


def _run_script(wt, path, *extra):
    return subprocess.run(
        [sys.executable, "-I", str(SCRIPT), "--cli", CLI, "--schema", SCHEMA,
         "--output", OUTPUT, "--option", "moduleFormat=cjs", *extra],
        cwd=str(wt), capture_output=True, text=True, env={"PATH": path}, timeout=60)


def test_the_script_generates_into_its_working_directory(tmp_path):
    wt = _worktree(tmp_path)
    path = _fake_node(tmp_path, 'mkdir out && echo "$PRISMA_SCHEMA_ENGINE_BINARY" > out/client.ts\n')
    result = _run_script(wt, path)
    assert result.returncode == 0, result.stderr
    assert (wt / OUTPUT / "client.ts").read_text() == "/bin/false\n"


def test_the_script_exits_nonzero_with_the_reason(tmp_path):
    wt = _worktree(tmp_path)
    path = _fake_node(tmp_path, 'echo "Validation Error Count: 1" >&2\nexit 1\n')
    result = _run_script(wt, path)
    assert result.returncode == 1
    assert "prisma generate failed: rc=1: stderr: Validation Error Count: 1" in result.stderr
    assert not (wt / OUTPUT).exists()

