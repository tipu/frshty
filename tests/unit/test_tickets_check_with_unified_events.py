from pathlib import Path


def test_check_is_callable():
    """
    Verify tickets.check() exists and is callable.
    This test validates the core auto-healing function exists.
    """
    from features import tickets

    assert hasattr(tickets, "check"), "tickets module should have check() function"
    assert callable(tickets.check), "tickets.check should be callable"


def test_main_loop_function_removed():
    """
    Verify main_loop function was removed from frshty.py.
    Fully event-driven: no polling threads.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    assert "def main_loop(" not in source, "main_loop should be removed (fully event-driven)"


def test_run_cycle_function_removed():
    """
    Verify run_cycle function was removed from frshty.py.
    Fully event-driven: cron_tick emitted by event system, not by run_cycle.
    """
    frshty_path = Path(__file__).parent.parent.parent / "frshty.py"
    source = frshty_path.read_text()

    assert "def run_cycle(" not in source, "run_cycle should be removed (fully event-driven)"
