"""Tests for per-repo test-runner detection used by run_tests_and_fix.

Maps repo-root files (package.json scripts, pyproject.toml, go.mod, Cargo.toml)
to the canonical test command. None of these tests touch a real test runner —
each builds a synthetic temp-dir fixture and asserts on what _detect_runner
returns.
"""
import json
import pytest

from core.tasks.tickets import _NO_LOCAL_PY_VENV_SENTINEL, _detect_runner


def _write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


class TestPackageJson:
    def test_npm_test_script_default(self, tmp_path):
        _write(tmp_path / "package.json",
               json.dumps({"scripts": {"test": "jest"}}))
        cmd, env = _detect_runner(tmp_path)
        assert cmd == ["npm", "run", "test"]
        assert env == {"CI": "true"}

    def test_pnpm_preferred_when_lockfile_present(self, tmp_path):
        _write(tmp_path / "package.json",
               json.dumps({"scripts": {"test": "vitest run"}}))
        _write(tmp_path / "pnpm-lock.yaml", "lockfileVersion: '6.0'\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["pnpm", "run", "test"]

    def test_yarn_preferred_when_lockfile_present(self, tmp_path):
        _write(tmp_path / "package.json",
               json.dumps({"scripts": {"test": "jest"}}))
        _write(tmp_path / "yarn.lock", "# yarn lockfile\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["yarn", "run", "test"]

    def test_test_unit_fallback_when_no_test_script(self, tmp_path):
        _write(tmp_path / "package.json",
               json.dumps({"scripts": {"test:unit": "vitest run"}}))
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["npm", "run", "test:unit"]

    def test_package_without_test_scripts_returns_none(self, tmp_path):
        _write(tmp_path / "package.json",
               json.dumps({"scripts": {"build": "tsc"}}))
        assert _detect_runner(tmp_path) is None

    def test_package_with_no_scripts_key_returns_none(self, tmp_path):
        _write(tmp_path / "package.json", json.dumps({"name": "x"}))
        assert _detect_runner(tmp_path) is None

    def test_malformed_package_json_returns_none(self, tmp_path):
        _write(tmp_path / "package.json", "not valid json")
        # Should NOT crash; falls through to other detectors and ultimately None
        result = _detect_runner(tmp_path)
        assert result is None


class TestPython:
    """Python repos must use a per-repo virtualenv solution; system pytest
    is never invoked because it almost certainly lacks the repo's deps."""

    def test_in_project_venv_pytest_preferred(self, tmp_path):
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        venv_bin = tmp_path / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        venv_pytest = venv_bin / "pytest"
        venv_pytest.write_text("#!/usr/bin/env python\n")
        venv_pytest.chmod(0o755)
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == [str(venv_pytest), "-q"]

    def test_pipfile_falls_back_to_pipenv_run(self, tmp_path):
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        _write(tmp_path / "Pipfile", "[packages]\npytest = \"*\"\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["pipenv", "run", "pytest", "-q"]

    def test_uv_lockfile_falls_back_to_uv_run(self, tmp_path):
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        _write(tmp_path / "uv.lock", "# uv lockfile\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["uv", "run", "pytest", "-q"]

    def test_poetry_lockfile_falls_back_to_poetry_run(self, tmp_path):
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        _write(tmp_path / "poetry.lock", "# poetry lockfile\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["poetry", "run", "pytest", "-q"]

    def test_pyproject_with_no_local_solution_returns_sentinel(self, tmp_path):
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == [_NO_LOCAL_PY_VENV_SENTINEL]

    def test_pytest_ini_with_no_local_solution_returns_sentinel(self, tmp_path):
        _write(tmp_path / "pytest.ini", "[pytest]\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == [_NO_LOCAL_PY_VENV_SENTINEL]

    def test_bare_tests_directory_with_no_local_solution_returns_sentinel(self, tmp_path):
        (tmp_path / "tests").mkdir()
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == [_NO_LOCAL_PY_VENV_SENTINEL]

    def test_venv_pytest_takes_priority_over_pipfile(self, tmp_path):
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        _write(tmp_path / "Pipfile", "[packages]\npytest = \"*\"\n")
        venv_bin = tmp_path / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        venv_pytest = venv_bin / "pytest"
        venv_pytest.write_text("#!/usr/bin/env python\n")
        venv_pytest.chmod(0o755)
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == [str(venv_pytest), "-q"]

    def test_unit_subtree_excludes_integration_suite(self, tmp_path):
        """Live-service integration tests have a separate CI environment."""
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        (tmp_path / "tests" / "unit").mkdir(parents=True)
        (tmp_path / "tests" / "integration").mkdir()
        venv_bin = tmp_path / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        venv_pytest = venv_bin / "pytest"
        venv_pytest.write_text("#!/usr/bin/env python\n")
        venv_pytest.chmod(0o755)

        cmd, _ = _detect_runner(tmp_path)
        assert cmd == [str(venv_pytest), "-q", "tests/unit"]


class TestGo:
    def test_go_mod(self, tmp_path):
        _write(tmp_path / "go.mod", "module example.com/x\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["go", "test", "./..."]


class TestRust:
    def test_cargo_toml(self, tmp_path):
        _write(tmp_path / "Cargo.toml", "[package]\nname='x'\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd == ["cargo", "test", "--quiet"]


class TestNoRunner:
    def test_empty_directory_returns_none(self, tmp_path):
        assert _detect_runner(tmp_path) is None

    def test_directory_with_unrelated_files_returns_none(self, tmp_path):
        _write(tmp_path / "README.md", "hello")
        _write(tmp_path / "Dockerfile", "FROM alpine\n")
        assert _detect_runner(tmp_path) is None


class TestPriority:
    """If multiple manifests exist (rare but possible for polyglot repos),
    package.json takes priority because it's most specific to test-runner
    configuration."""

    def test_package_json_beats_pyproject(self, tmp_path):
        _write(tmp_path / "package.json",
               json.dumps({"scripts": {"test": "vitest"}}))
        _write(tmp_path / "pyproject.toml", "[project]\nname='x'\n")
        cmd, _ = _detect_runner(tmp_path)
        assert cmd[0] in ("npm", "pnpm", "yarn")
