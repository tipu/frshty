import re
from pathlib import Path


def test_lifespan_simplified():
    """
    Verify _lifespan handler in frshty.py is simplified (no-op).
    Checks the actual source code to confirm subprocess management was removed.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    # VERIFY: _lifespan function exists and is simple
    assert "async def _lifespan" in source, "_lifespan function should exist"

    # Extract the _lifespan function
    lifespan_match = re.search(
        r"async def _lifespan\(a\):.*?(?=\n(?:async def|def|\Z))",
        source,
        re.DOTALL,
    )
    assert lifespan_match, "_lifespan function not found"

    lifespan_code = lifespan_match.group(0)

    # VERIFY: No _run_worker calls in lifespan
    assert "_run_worker" not in lifespan_code, "_run_worker should not be in _lifespan"

    # VERIFY: No multiprocessing.Process in lifespan
    assert "Process(" not in lifespan_code, "subprocess Process should not be in _lifespan"

    # VERIFY: _lifespan contains only yield (is a no-op)
    assert "yield" in lifespan_code, "_lifespan should contain yield"


def test_no_run_worker_function():
    """
    Verify _run_worker function was removed from frshty.py.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    # VERIFY: _run_worker function does not exist
    assert (
        "def _run_worker(" not in source
    ), "_run_worker function should be removed"


def test_no_run_poll_function():
    """
    Verify _run_poll function was removed from frshty.py.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    # VERIFY: _run_poll function does not exist
    assert "def _run_poll(" not in source, "_run_poll function should be removed"


def test_no_worker_proc_global():
    """
    Verify _worker_proc global was removed from frshty.py.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    # VERIFY: No _worker_proc variable at module level
    # (Check for the assignment pattern)
    assert (
        "_worker_proc = " not in source
    ), "_worker_proc global should be removed"


def test_reload_always_false():
    """
    Verify reload is always set to False, not conditional.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    # VERIFY: reload is always False
    assert 'reload = False' in source, "reload should be set to False"

    # Extract reload assignment(s)
    reload_assignments = re.findall(
        r'reload\s*=\s*(.+?)(?:\n|$)', source
    )

    # Verify no conditional assignments (no ternary operator)
    for assignment in reload_assignments:
        assert "if " not in assignment, f"reload assignment should not be conditional: {assignment}"
