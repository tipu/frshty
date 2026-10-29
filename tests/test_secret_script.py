import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "secret.py"


def run(secrets_dir, *args):
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True,
                          env={"FRSHTY_SECRETS_DIR": str(secrets_dir), "PATH": "/usr/bin:/bin"})


@pytest.fixture
def secrets(tmp_path):
    root = tmp_path / "secrets"
    root.mkdir()
    (root / "nehundware-aws").write_text("Hunter2!x\n")
    (root / "teglioncorp-db").write_text("s3cr3t")
    (root / ".hidden").write_text("no")
    return root


def test_lists_names_without_an_argument(secrets):
    out = run(secrets)
    assert out.returncode == 0
    assert out.stdout.split() == ["nehundware-aws", "teglioncorp-db"]


def test_prints_the_named_secret(secrets):
    out = run(secrets, "nehundware-aws")
    assert out.returncode == 0
    assert out.stdout == "Hunter2!x\n"


def test_missing_name_fails_and_lists_the_names(secrets):
    out = run(secrets, "nope")
    assert out.returncode == 1
    assert out.stdout == ""
    assert "no secret named 'nope'" in out.stderr
    assert "teglioncorp-db" in out.stderr


def test_path_outside_the_directory_is_refused(secrets, tmp_path):
    (tmp_path / "outside").write_text("leak")
    out = run(secrets, "../outside")
    assert out.returncode == 1
    assert "leak" not in out.stdout


def test_missing_directory_fails_clearly(tmp_path):
    out = run(tmp_path / "absent")
    assert out.returncode == 1
    assert "no secrets in" in out.stderr
